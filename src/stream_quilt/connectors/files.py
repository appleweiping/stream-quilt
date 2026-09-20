"""Replayable bounded JSONL source and idempotent local output snapshot.

The SQLite journal owns processing authority. This module binds one immutable
local file to its original source positions and materializes a complete output
prefix; it does not coordinate transactions with an external consumer.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import stat
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Any, Literal

from stream_quilt.dataflow import FlowRecord
from stream_quilt.errors import OutputError, ValidationError
from stream_quilt.io import _reject_duplicate_keys, _reject_json_constant
from stream_quilt.partitioned_checkpoint import _hex, _name
from stream_quilt.partitioned_journal import PartitionedFlowJournal
from stream_quilt.partitioned_journal_types import (
    PartitionedFlowRecoveryPoint,
    PartitionedFlowRequest,
    PartitionedOutputCursor,
)
from stream_quilt.recovery import RecoveryConflict, _finite_json_float

_MAX_SOURCE_BYTES = 64 * 1024 * 1024
_MAX_OUTPUT_BYTES = 64 * 1024 * 1024
_MAX_LINE_BYTES = 8 * 1024 * 1024
_MAX_LINES = 1_000_000
_LOCK_WAIT_SECONDS = 10.0
_SOURCE_PREFIX = b"stream-quilt-staged-jsonl-v1\0"
_SINK_FORMAT = "stream-quilt-file-output-v1"
_WINDOWS_DEVICES = {"CON", "PRN", "AUX", "NUL"} | {
    f"{prefix}{digit}" for prefix in ("COM", "LPT") for digit in "123456789¹²³"
}


def _json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


def _relative(value: str) -> tuple[str, ...]:
    if type(value) is not str or not value or "\\" in value or PureWindowsPath(value).drive:
        raise ValidationError("connector path must be a relative slash-separated name")
    if value.startswith("/") or value.endswith("/"):
        raise ValidationError("connector path must be relative and nonempty")
    parts = tuple(value.split("/"))
    if any(
        not part
        or part in {".", ".."}
        or part.endswith((".", " "))
        or ":" in part
        or any(char in '<>"|?*' for char in part)
        or part.split(".", 1)[0].upper() in _WINDOWS_DEVICES
        or any(ord(char) < 32 or ord(char) == 127 for char in part)
        for part in parts
    ):
        raise ValidationError("connector path contains a forbidden component")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValidationError("connector path contains invalid Unicode") from exc
    if len(value.encode("utf-8")) > 1024:
        raise ValidationError("connector path is too long")
    return parts


def _is_link(path: Path) -> bool:
    item = path.lstat()
    return stat.S_ISLNK(item.st_mode) or bool(
        getattr(item, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _directory(path: Path, *, create: bool = False, private: bool = False) -> Path:
    absolute = path.absolute()
    if os.name == "nt" and absolute.drive.startswith("\\\\"):
        raise ValidationError("connector roots must use a local filesystem")
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if create and not current.exists() and not current.is_symlink():
            current.mkdir(mode=0o700)
        try:
            if _is_link(current) or not current.is_dir():
                raise ValidationError("connector directory is not a plain directory")
        except OSError as exc:
            raise ValidationError("cannot inspect connector directory") from exc
    if private and os.name != "nt":
        info = absolute.stat()
        getuid = getattr(os, "getuid", None)
        if not callable(getuid) or info.st_uid != getuid() or info.st_mode & 0o077:
            raise ValidationError("connector directory must be owner-private")
    return absolute


def _case_check(parent: Path, name: str) -> None:
    if os.name != "nt":
        return
    try:
        with os.scandir(parent) as entries:
            for index, entry in enumerate(entries):
                if index >= 10_000:
                    raise ValidationError("connector directory has too many names to check")
                if entry.name.casefold() == name.casefold() and entry.name != name:
                    raise ValidationError("connector path uses a different case alias")
    except OSError as exc:
        raise ValidationError("cannot inspect connector directory names") from exc


def _path(root: Path, relative: str) -> Path:
    parts = _relative(relative)
    base = _directory(root)
    parent = base
    for part in parts[:-1]:
        _case_check(parent, part)
        parent = _directory(parent / part)
    _case_check(parent, parts[-1])
    target = parent / parts[-1]
    try:
        info = target.lstat()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise ValidationError("cannot inspect connector target") from exc
    else:
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or bool(
                getattr(info, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            )
        ):
            raise ValidationError("connector target is not a plain file")
    return target


def _source_digest(relative: str, raw: bytes) -> str:
    name = relative.encode("utf-8")
    return hashlib.sha256(
        _SOURCE_PREFIX + len(name).to_bytes(8, "big") + name + len(raw).to_bytes(8, "big") + raw
    ).hexdigest()


def _record(raw: bytes) -> FlowRecord:
    try:
        document = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ValidationError("invalid keyed JSONL record") from exc
    if type(document) is not dict or set(document) != {"key", "value"}:
        raise ValidationError("keyed JSONL record requires exactly key and value")
    return FlowRecord(document["value"], document["key"])


def _scan(raw: bytes) -> tuple[int, ...]:
    if len(raw) > _MAX_SOURCE_BYTES:
        raise ValidationError("source file exceeds byte limit")
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValidationError("source file must not have a UTF-8 BOM")
    offsets = [0]
    position = 0
    while position < len(raw):
        if len(offsets) > _MAX_LINES:
            raise ValidationError("source file exceeds record limit")
        newline = raw.find(b"\n", position)
        end = len(raw) if newline < 0 else newline + 1
        line = raw[position:end]
        if len(line) > _MAX_LINE_BYTES:
            raise ValidationError("source line exceeds byte limit")
        body = line[:-1] if line.endswith(b"\n") else line
        if line.endswith(b"\n") and body.endswith(b"\r"):
            body = body[:-1]
        if not body or b"\r" in body:
            raise ValidationError("source line is empty or has an invalid terminator")
        _record(body)
        offsets.append(end)
        position = end
    return tuple(offsets)


def _source_bytes(blob: Path) -> bytes:
    try:
        before = blob.lstat()
        if _is_link(blob) or not stat.S_ISREG(before.st_mode):
            raise ValidationError("staged source is not a plain file")
        with blob.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or (
                opened.st_dev,
                opened.st_ino,
            ) != (before.st_dev, before.st_ino):
                raise ValidationError("staged source changed during open")
            raw = handle.read(_MAX_SOURCE_BYTES + 1)
    except OSError as exc:
        raise ValidationError("cannot read staged source") from exc
    if len(raw) > _MAX_SOURCE_BYTES:
        raise ValidationError("staged source exceeds byte limit")
    return raw


def _remove_temp(path: Path | None, primary: BaseException | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        if primary is None:
            raise OutputError("cannot remove connector temporary") from exc
        primary.add_note("connector temporary cleanup also failed")


@dataclass(frozen=True, slots=True)
class StagedJsonlSource:
    """One immutable, bounded local source partition with exact byte offsets."""

    blob_path: Path
    relative_path: str
    source_id: str
    source_digest: str
    byte_offsets: tuple[int, ...]
    _raw: bytes = field(repr=False)

    @classmethod
    def stage(
        cls, input_root: str | Path, relative_path: str, store_root: str | Path, logical_name: str
    ) -> StagedJsonlSource:
        _relative(relative_path)
        _name(logical_name, "logical source name")
        source_id = _name(f"file-v1:{logical_name}", "source ID")
        source = _path(Path(input_root), relative_path)
        raw = _source_bytes(source)
        offsets = _scan(raw)
        private = _directory(Path(store_root), create=True, private=True)
        blobs = _directory(private / "blobs", create=True, private=True)
        digest = _source_digest(relative_path, raw)
        target = _path(blobs, digest)
        if target.exists():
            if _source_bytes(target) != raw:
                raise ValidationError("existing staged source differs from its digest")
        else:
            temporary: Path | None = None
            primary: BaseException | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb", prefix=".source-", suffix=".tmp", dir=blobs, delete=False
                ) as handle:
                    temporary = Path(handle.name)
                    handle.write(raw)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    os.link(temporary, target)
                except FileExistsError as exc:
                    if _source_bytes(target) != raw:
                        raise ValidationError(
                            "existing staged source differs from its digest"
                        ) from exc
                if os.name != "nt":
                    _sync_directory(blobs)
            except OSError as exc:
                primary = OutputError("cannot publish staged source")
                raise primary from exc
            except BaseException as exc:
                primary = exc
                raise
            finally:
                _remove_temp(temporary, primary)
        return cls(target, relative_path, source_id, digest, offsets, raw)

    @classmethod
    def restore(
        cls, store_root: str | Path, relative_path: str, source_id: str, source_digest: str
    ) -> StagedJsonlSource:
        _relative(relative_path)
        _name(source_id, "source ID")
        _hex(source_digest)
        if not source_id.startswith("file-v1:") or not source_id.removeprefix("file-v1:"):
            raise ValidationError("source ID is not a staged file identity")
        private = _directory(Path(store_root), private=True)
        blobs = _directory(private / "blobs", private=True)
        blob = _path(blobs, source_digest)
        raw = _source_bytes(blob)
        if _source_digest(relative_path, raw) != source_digest:
            raise ValidationError("staged source identity or content mismatch")
        return cls(blob, relative_path, source_id, source_digest, _scan(raw), raw)

    @property
    def record_count(self) -> int:
        return len(self.byte_offsets) - 1

    def verify(self) -> None:
        raw = _source_bytes(self.blob_path)
        if raw != self._raw or _source_digest(self.relative_path, self._raw) != self.source_digest:
            raise ValidationError("staged source identity or content mismatch")
        if _scan(self._raw) != self.byte_offsets:
            raise ValidationError("staged source offset table changed")

    def read_batch(self, start_position: int, count: int) -> tuple[FlowRecord, ...]:
        if (
            type(start_position) is not int
            or type(count) is not int
            or not (
                0 <= start_position <= self.record_count
                and 0 <= count <= self.record_count - start_position
            )
        ):
            raise ValidationError("source batch position is outside staged file")
        # _raw was validated and digest-bound at stage/restore. Never reread a
        # mutable file after the run's verification and before journal COMMIT.
        raw = self._raw[
            self.byte_offsets[start_position] : self.byte_offsets[start_position + count]
        ]
        records = []
        for offset in range(start_position, start_position + count):
            a = self.byte_offsets[offset] - self.byte_offsets[start_position]
            b = self.byte_offsets[offset + 1] - self.byte_offsets[start_position]
            line = raw[a:b]
            body = line[:-1] if line.endswith(b"\n") else line
            if line.endswith(b"\n") and body.endswith(b"\r"):
                body = body[:-1]
            records.append(_record(body))
        return tuple(records)


def _request(
    journal_id: str,
    generation: int,
    cause: Literal["wave", "eof"],
    position: int,
    records: tuple[FlowRecord, ...],
) -> PartitionedFlowRequest:
    # The placeholder is excluded from the hash, avoiding a circular ID.
    preliminary = PartitionedFlowRequest(
        journal_id,
        "0" * 32,
        generation,
        cause,
        position,
        records,
    )
    document = preliminary.to_dict()
    del document["request_id"]
    request_id = hashlib.sha256(b"stream-quilt-file-request-v1\0" + _json(document)).hexdigest()[
        :32
    ]
    return PartitionedFlowRequest(
        journal_id,
        request_id,
        generation,
        cause,
        position,
        records,
    )


def run_file_journal(
    source: StagedJsonlSource,
    journal: PartitionedFlowJournal,
    *,
    max_new_inputs: int,
    batch_size: int = 64,
) -> PartitionedFlowRecoveryPoint:
    """Commit up to a bounded number of new records; cap exhaustion is not EOF."""
    if type(source) is not StagedJsonlSource or type(journal) is not PartitionedFlowJournal:
        raise ValidationError("file runner needs a staged source and partitioned journal")
    if type(max_new_inputs) is not int or not 0 <= max_new_inputs <= _MAX_LINES:
        raise ValidationError("max_new_inputs is outside source limit")
    if (
        type(batch_size) is not int
        or not 1 <= batch_size <= journal.latest().checkpoint.limits.max_batch_inputs
    ):
        raise ValidationError("batch size exceeds journal wave limit")
    source.verify()
    point = journal.latest()
    if (point.checkpoint.source_id, point.checkpoint.source_digest) != (
        source.source_id,
        source.source_digest,
    ) or point.checkpoint.next_position > source.record_count:
        raise ValidationError("file source does not match journal checkpoint")
    if point.checkpoint.source_closed or max_new_inputs == 0:
        return point
    processed = 0
    with journal.session() as session:
        while processed < max_new_inputs and point.checkpoint.next_position < source.record_count:
            position = point.checkpoint.next_position
            count = min(batch_size, max_new_inputs - processed, source.record_count - position)
            records = source.read_batch(position, count)
            while True:
                try:
                    request = _request(
                        point.journal_id, point.generation, "wave", position, records
                    )
                    input_size = sum(
                        len(_json({"position": position + index, "record": record.to_dict()}))
                        for index, record in enumerate(records)
                    )
                    if input_size > point.checkpoint.limits.max_input_bytes or (
                        len(records) * journal.flow.limits.max_calls_per_input
                        > point.checkpoint.limits.max_callback_reservation
                    ):
                        raise ValidationError("file wave exceeds worker input admission")
                    break
                except ValidationError:
                    if len(records) == 1:
                        raise
                    records = records[:-1]
            try:
                session.apply(request)
            except Exception:
                receipt = journal.request(request.request_id)
                if receipt is None or (receipt.request_digest, receipt.expected_generation) != (
                    request.digest,
                    request.expected_generation,
                ):
                    raise
                # A lost ACK can coexist with a failed worker session. Reopen on
                # the next call rather than assuming this session can continue.
                return journal.latest()
            new_point = journal.latest()
            if new_point.checkpoint.next_position != position + len(records):
                raise RecoveryConflict("file wave did not publish expected source prefix")
            point = new_point
            processed += len(records)
        if processed < max_new_inputs and point.checkpoint.next_position == source.record_count:
            request = _request(
                point.journal_id, point.generation, "eof", point.checkpoint.next_position, ()
            )
            try:
                session.apply(request)
            except Exception:
                receipt = journal.request(request.request_id)
                if receipt is None or (receipt.request_digest, receipt.expected_generation) != (
                    request.digest,
                    request.expected_generation,
                ):
                    raise
                return journal.latest()
            point = journal.latest()
            if not point.checkpoint.source_closed:
                raise RecoveryConflict("file EOF did not close durable source")
    return point


def _rows(journal: PartitionedFlowJournal, cursor: PartitionedOutputCursor) -> bytes:
    lines: list[bytes] = []
    size = 0
    while cursor.next_sequence < cursor.stop_sequence:
        page = journal.read_outputs(cursor, limit=100)
        if not page.outputs:
            raise ValidationError("journal output cursor made no progress")
        for item in page.outputs:
            output = item.output
            line = (
                _json(
                    {
                        "sequence": output.sequence,
                        "source_position": output.source_position,
                        "output_index": output.output_index,
                        "key": output.record.key,
                        "value": output.record.value,
                    }
                )
                + b"\n"
            )
            size += len(line)
            if size > _MAX_OUTPUT_BYTES:
                raise ValidationError("materialized output exceeds byte limit")
            lines.append(line)
        cursor = page.cursor
    return b"".join(lines)


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def _publication_lock(parent: Path, target_name: str) -> Iterator[None]:
    """Serialize cooperating publishers; keep the lockfile inode across crashes."""
    lock_key = target_name.casefold() if os.name == "nt" else target_name
    lock_name = ".stream-quilt-sink-" + hashlib.sha256(lock_key.encode("utf-8")).hexdigest()
    lock_path = _path(parent, lock_name + ".lock")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise OutputError("cannot open output publication lock") from exc
    locked = False
    primary: BaseException | None = None
    try:
        info = os.fstat(fd)
        path_info = lock_path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or _is_link(lock_path)
            or (info.st_dev, info.st_ino) != (path_info.st_dev, path_info.st_ino)
        ):
            raise ValidationError("output publication lock is not a stable plain file")
        if os.name != "nt":
            getuid = getattr(os, "getuid", None)
            if not callable(getuid) or info.st_uid != getuid() or info.st_mode & 0o077:
                raise ValidationError("output publication lock must be owner-private")
        if info.st_size == 0:
            os.write(fd, b"\0")
        elif info.st_size != 1 or os.read(fd, 1) != b"\0":
            raise ValidationError("output publication lock has invalid contents")
        # On Windows the region lock is mandatory. Keep the marker at byte 0
        # readable by another waiter; lock byte 1 (past EOF) instead.
        os.lseek(fd, 1, os.SEEK_SET)
        module = importlib.import_module("msvcrt" if os.name == "nt" else "fcntl")
        deadline = time.monotonic() + _LOCK_WAIT_SECONDS
        while True:
            try:
                if os.name == "nt":
                    module.locking(fd, module.LK_NBLCK, 1)
                else:
                    module.flock(fd, module.LOCK_EX | module.LOCK_NB)
                locked = True
                break
            except OSError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise OutputError("output publication lock timed out") from exc
                if exc.errno not in {11, 13} and getattr(exc, "winerror", None) not in {33, 36}:
                    raise OutputError("cannot acquire output publication lock") from exc
                time.sleep(min(0.05, remaining))
        yield
    except BaseException as exc:
        primary = exc
        raise
    finally:
        cleanup: OSError | None = None
        try:
            if locked:
                os.lseek(fd, 1, os.SEEK_SET)
                if os.name == "nt":
                    module.locking(fd, module.LK_UNLCK, 1)
                else:
                    module.flock(fd, module.LOCK_UN)
        except OSError as exc:
            cleanup = exc
        try:
            os.close(fd)
        except OSError as exc:
            cleanup = exc if cleanup is None else cleanup
        if cleanup is not None:
            if primary is None:
                raise OutputError("cannot release output publication lock") from cleanup
            primary.add_note("output publication lock cleanup also failed")


def _materialize_file_unlocked(
    journal: PartitionedFlowJournal, target_root: str | Path, relative_filename: str
) -> Path:
    """Atomically replace one bounded, self-describing complete output prefix."""
    if type(journal) is not PartitionedFlowJournal:
        raise ValidationError("file sink requires a partitioned journal")
    root = _directory(Path(target_root), private=True)
    target = _path(root, relative_filename)
    _directory(target.parent, private=True)
    point = journal.latest()
    cursor = journal.output_cursor()
    if cursor.journal_id != point.journal_id:
        raise ValidationError("output cursor disagrees with journal")
    body = _rows(journal, cursor)
    header = {
        "format": _SINK_FORMAT,
        "journal_id": cursor.journal_id,
        "anchor_generation": cursor.anchor_generation,
        "anchor_receipt_digest": cursor.anchor_receipt_digest,
        "stop_sequence": cursor.stop_sequence,
        "source_id": point.checkpoint.source_id,
        "source_digest": point.checkpoint.source_digest,
        "rows_sha256": hashlib.sha256(body).hexdigest(),
    }
    candidate = _json(header) + b"\n" + body
    if len(candidate) > _MAX_OUTPUT_BYTES:
        raise ValidationError("materialized output exceeds byte limit")
    if target.exists():
        try:
            before = target.lstat()
            with target.open("rb") as handle:
                opened = os.fstat(handle.fileno())
                if not stat.S_ISREG(opened.st_mode) or (
                    opened.st_dev,
                    opened.st_ino,
                ) != (before.st_dev, before.st_ino):
                    raise ValidationError("previous output changed during open")
                old = handle.read(_MAX_OUTPUT_BYTES + 1)
        except OSError as exc:
            raise ValidationError("cannot inspect previous output snapshot") from exc
        if len(old) > _MAX_OUTPUT_BYTES:
            raise ValidationError("previous output exceeds byte limit")
        old_header_raw, separator, old_body = old.partition(b"\n")
        if not separator:
            raise ValidationError("previous output has no header")
        try:
            old_header = json.loads(
                old_header_raw.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeError, ValueError, RecursionError) as exc:
            raise ValidationError("previous output has invalid header") from exc
        if (
            type(old_header) is not dict
            or set(old_header) != set(header)
            or old_header.get("format") != _SINK_FORMAT
            or old_header.get("journal_id") != cursor.journal_id
            or old_header.get("source_id") != header["source_id"]
            or old_header.get("source_digest") != header["source_digest"]
            or old_header.get("rows_sha256") != hashlib.sha256(old_body).hexdigest()
            or _json(old_header) != old_header_raw
        ):
            raise ValidationError("previous output identity or digest mismatch")
        previous_cursor = PartitionedOutputCursor(
            old_header["journal_id"],
            old_header["anchor_generation"],
            old_header["anchor_receipt_digest"],
            old_header["stop_sequence"],
            0,
        )
        # Validate even an empty old prefix; _rows otherwise makes no page call.
        journal.read_outputs(previous_cursor, limit=1)
        if _rows(journal, previous_cursor) != old_body:
            raise ValidationError("previous output differs from journal prefix")
        if old_header["stop_sequence"] > cursor.stop_sequence:
            raise ValidationError("previous output is ahead of journal")
        # EOF and zero-output waves can advance the journal anchor without
        # changing the output prefix. Keep the already verified file byte-for-byte.
        if old_header["stop_sequence"] == cursor.stop_sequence and old_body == body:
            return target
    temporary: Path | None = None
    primary: BaseException | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=".output-", suffix=".tmp", dir=target.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(candidate)
            handle.flush()
            os.fsync(handle.fileno())
        _path(root, relative_filename)
        os.replace(temporary, target)
        temporary = None
        if os.name != "nt":
            _sync_directory(target.parent)
    except OSError as exc:
        primary = OutputError("cannot publish output snapshot")
        raise primary from exc
    except BaseException as exc:
        primary = exc
        raise
    finally:
        _remove_temp(temporary, primary)
    return target


def materialize_file(
    journal: PartitionedFlowJournal, target_root: str | Path, relative_filename: str
) -> Path:
    """Publish one prefix under a bounded cross-process per-target lock."""
    if type(journal) is not PartitionedFlowJournal:
        raise ValidationError("file sink requires a partitioned journal")
    root = _directory(Path(target_root), private=True)
    target = _path(root, relative_filename)
    _directory(target.parent, private=True)
    with _publication_lock(target.parent, target.name):
        return _materialize_file_unlocked(journal, root, relative_filename)


__all__ = ["StagedJsonlSource", "materialize_file", "run_file_journal"]
