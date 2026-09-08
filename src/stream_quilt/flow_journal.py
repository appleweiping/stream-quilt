"""Transactional local source/state/output recovery for generalized dataflows.

Callbacks execute outside the database write lock on a detached runtime. A
generation conflict is returned to the caller; callbacks are never auto-retried.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Generic, Protocol, TypeVar

from .branching import GraphOutput
from .dataflow import Dataflow, FlowCheckpoint, FlowRecord, FlowRuntime
from .errors import OutputError, ValidationError
from .io import _reject_duplicate_keys, _reject_json_constant
from .recovery import RecoveryConflict, _count, _finite_json_float, _input_id

_APPLICATION_ID = 0x5351464A
_MAX_DOCUMENT_BYTES = 64 * 1024 * 1024
_MAX_RECORDS = 1_000_000
_MAX_BATCH_INPUTS = 10_000
_MAX_BATCH_OUTPUTS = 100_000


def _encode(value: Any) -> str:
    chunks: list[str] = []
    size = 0
    encoder = json.JSONEncoder(
        sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    try:
        for chunk in encoder.iterencode(value):
            size += len(chunk.encode("utf-8"))
            if size > _MAX_DOCUMENT_BYTES:
                raise ValidationError("flow journal document exceeds 64 MiB")
            chunks.append(chunk)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ValidationError("flow journal requires bounded finite JSON") from exc
    return "".join(chunks)


def _digest(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _decode(payload: Any, digest: Any, size: Any) -> Any:
    if (
        type(payload) is not str
        or type(size) is not int
        or not 0 <= size <= _MAX_DOCUMENT_BYTES
        or type(digest) is not str
        or digest != _digest(payload)
    ):
        raise ValidationError("invalid or corrupted flow journal document")
    try:
        document = json.loads(
            payload,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (ValueError, RecursionError) as exc:
        raise ValidationError("invalid flow journal JSON") from exc
    if _encode(document) != payload:
        raise ValidationError("flow journal document must use canonical JSON")
    return document


@dataclass(frozen=True, slots=True)
class FlowRecoveryPoint:
    """One committed prefix; next_position is a zero-based source record offset."""

    source_id: str
    generation: int
    next_position: int
    checkpoint: FlowCheckpoint

    def __post_init__(self) -> None:
        _validate_point(
            self.source_id, self.generation, self.next_position, self.checkpoint, FlowCheckpoint
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "source_id": self.source_id,
            "generation": self.generation,
            "next_position": self.next_position,
            "checkpoint": self.checkpoint.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class FlowOutput:
    """A detached immutable output and the source input that emitted it."""

    sequence: int
    source_position: int
    record: FlowRecord

    def __post_init__(self) -> None:
        _count(self.sequence, "sequence")
        _count(self.source_position, "source_position")
        if self.sequence >= _MAX_RECORDS or type(self.record) is not FlowRecord:
            raise ValidationError("invalid flow journal output")

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "source_position": self.source_position,
            "record": self.record.to_dict(),
        }


def _validate_point(
    source_id: str,
    generation: int,
    next_position: int,
    checkpoint: FlowCheckpoint,
    checkpoint_type: type[FlowCheckpoint],
) -> None:
    _input_id(source_id)
    _count(generation, "generation")
    _count(next_position, "next_position")
    if type(checkpoint) is not checkpoint_type or checkpoint.processed_inputs != next_position:
        raise ValidationError("flow checkpoint input count must match the source position")
    if checkpoint.emitted_records > _MAX_RECORDS:
        raise ValidationError("flow journal output capacity exceeded")
    if not generation <= next_position <= generation * _MAX_BATCH_INPUTS or (
        generation == 0 and (checkpoint.emitted_records or checkpoint.cells)
    ):
        raise ValidationError("flow journal generation and source prefix are inconsistent")


def _point_document(document: Any, *, kind: str | None = None) -> None:
    fields = {"schema_version", "source_id", "generation", "next_position", "checkpoint"}
    if kind is not None:
        fields.add("kind")
    if (
        type(document) is not dict
        or set(document) != fields
        or document["schema_version"] != "1.0"
        or (kind is not None and document["kind"] != kind)
    ):
        raise ValidationError("invalid flow recovery point fields")


def _stored_record(value: Any) -> FlowRecord:
    if type(value) is not dict or set(value) != {"key", "value"}:
        raise ValidationError("invalid stored FlowRecord fields")
    return FlowRecord(value["value"], value["key"])


class _Spec(Protocol):
    @property
    def identity(self) -> str: ...


class _Point(Protocol):
    @property
    def source_id(self) -> str: ...

    @property
    def generation(self) -> int: ...

    @property
    def next_position(self) -> int: ...

    @property
    def checkpoint(self) -> FlowCheckpoint: ...

    def to_dict(self) -> dict[str, Any]: ...


class _Output(Protocol):
    @property
    def sequence(self) -> int: ...

    @property
    def source_position(self) -> int: ...

    @property
    def record(self) -> FlowRecord: ...

    def to_dict(self) -> dict[str, Any]: ...


class _Runtime(Protocol):
    @property
    def processed_inputs(self) -> int: ...

    def checkpoint(self) -> FlowCheckpoint: ...

    def process(self, record: FlowRecord) -> tuple[FlowRecord | GraphOutput, ...]: ...


_SpecT = TypeVar("_SpecT", bound=_Spec)
_PointT = TypeVar("_PointT", bound=_Point)
_OutputT = TypeVar("_OutputT", bound=_Output)


def _finish_connection(connection: sqlite3.Connection, primary: BaseException | None) -> None:
    failures: list[tuple[str, BaseException]] = []
    for name, operation in (
        ("rollback", connection.rollback if primary is not None else None),
        ("close", connection.close),
    ):
        if operation is not None:
            try:
                operation()
            except BaseException as error:
                failures.append((name, error))
    if failures:
        detail = "flow journal cleanup failed: " + ", ".join(name for name, _ in failures)
        for _, failure in failures:
            if not isinstance(failure, Exception):
                failure.add_note(detail)
                raise failure
        if primary is None or isinstance(primary, GeneratorExit):
            raise OutputError(
                detail + "; inspect latest before replay; a commit may already exist"
            ) from failures[0][1]
        primary.add_note(detail)


class _JournalEngine(Generic[_SpecT, _PointT, _OutputT], ABC):
    """Private shared SQL engine; codec hooks are not a public plugin protocol.

    Stored output count is capped at one million; use a new source/journal for
    another bounded run. This is not broker acknowledgement, replicated storage
    or a transactional wrapper around external callback side effects.
    """

    def __init__(
        self, path: str | Path, flow: _SpecT, source_id: str, *, create: bool = True
    ) -> None:
        self.flow = flow
        self._runtime()
        _input_id(source_id)
        if type(create) is not bool:
            raise ValidationError("create must be boolean")
        self.path = Path(path).absolute()
        self.source_id = source_id
        if not create and not self.path.is_file():
            raise ValidationError("flow journal does not exist")
        try:
            if create:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._transaction(create=create) as connection:
                connection.execute("BEGIN IMMEDIATE")
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                    )
                }
                application_id = connection.execute("PRAGMA application_id").fetchone()[0]
                version = connection.execute("PRAGMA user_version").fetchone()[0]
                if not tables and application_id == 0 and version == 0 and create:
                    connection.execute(self._application_pragma)
                    connection.execute("PRAGMA user_version = 1")
                    connection.execute(
                        "CREATE TABLE flow_head (slot INTEGER PRIMARY KEY CHECK(slot=1), "
                        "payload TEXT NOT NULL, digest TEXT NOT NULL)"
                    )
                    connection.execute(
                        "CREATE TABLE flow_output (seq INTEGER PRIMARY KEY, "
                        "payload TEXT NOT NULL, digest TEXT NOT NULL)"
                    )
                    initial = self._point(0, 0, self._runtime().checkpoint())
                    payload = _encode(initial.to_dict())
                    connection.execute(
                        "INSERT INTO flow_head VALUES (1, ?, ?)", (payload, _digest(payload))
                    )
                elif (
                    tables != {"flow_head", "flow_output"}
                    or application_id != self._application_id
                    or connection.execute("PRAGMA user_version").fetchone() != (1,)
                ):
                    raise ValidationError("unsupported or unrelated flow journal schema")
                self._latest(connection)
        except (sqlite3.Error, OSError) as exc:
            raise OutputError("cannot initialize flow journal") from exc

    _application_id: ClassVar[int]
    _application_pragma: ClassVar[str]

    @abstractmethod
    def _runtime(self, checkpoint: FlowCheckpoint | None = None) -> _Runtime: ...

    @abstractmethod
    def _point(self, generation: int, position: int, checkpoint: FlowCheckpoint) -> _PointT: ...

    @abstractmethod
    def _decode_point(self, document: Any) -> _PointT: ...

    @abstractmethod
    def _output(
        self, sequence: int, position: int, value: FlowRecord | GraphOutput
    ) -> _OutputT: ...

    @abstractmethod
    def _decode_output(self, document: Any) -> _OutputT: ...

    @abstractmethod
    def _validate_output(self, item: _OutputT, previous: _OutputT | None) -> None: ...

    @contextmanager
    def _transaction(self, *, create: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self._connect(create=create)
        primary: BaseException | None = None
        try:
            yield connection
            try:
                connection.commit()
            except BaseException as error:
                detail = "flow journal commit outcome unknown; inspect latest before replay"
                if isinstance(error, Exception):
                    raise OutputError(detail) from error
                error.add_note(detail)
                raise
        except BaseException as error:
            primary = error
            raise
        finally:
            _finish_connection(connection, primary)

    def _connect(self, *, create: bool = False) -> sqlite3.Connection:
        # mode=rw avoids silently recreating a journal removed after construction.
        uri = self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
        connection = sqlite3.connect(uri, uri=True, timeout=10)
        try:
            connection.execute("PRAGMA synchronous=FULL")
        except BaseException as error:
            # The caller's closing() scope has not received this connection yet.
            _finish_connection(connection, error)
            raise
        return connection

    def _latest(self, connection: sqlite3.Connection) -> _PointT:
        if connection.execute("PRAGMA application_id").fetchone() != (
            self._application_id,
        ) or connection.execute("PRAGMA user_version").fetchone() != (1,):
            raise ValidationError("flow journal schema identity changed")
        row = connection.execute(
            "SELECT CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
            "THEN payload END, CASE WHEN typeof(digest)='text' "
            "AND length(CAST(digest AS BLOB))=64 THEN digest END, length(CAST(payload AS BLOB)) "
            "FROM flow_head WHERE slot=1",
            (_MAX_DOCUMENT_BYTES,),
        ).fetchone()
        if row is None or connection.execute("SELECT count(*) FROM flow_head").fetchone() != (1,):
            raise ValidationError("flow journal head is missing or duplicated")
        document = _decode(*row)
        point = self._decode_point(document)
        if point.source_id != self.source_id or point.checkpoint.identity != self.flow.identity:
            raise ValidationError("flow journal source or flow identity mismatch")
        self._runtime(point.checkpoint)
        count, first, last = connection.execute(
            "SELECT count(*), min(seq), max(seq) FROM flow_output"
        ).fetchone()
        if count != point.checkpoint.emitted_records or (
            count and (first != 0 or last != count - 1)
        ):
            raise ValidationError("flow journal output prefix is inconsistent with its head")
        return point

    def latest(self) -> _PointT:
        """Load one consistent verified head/state snapshot."""
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                return self._latest(connection)
        except sqlite3.Error as exc:
            raise OutputError("cannot read flow journal") from exc

    def advance(
        self, records: Iterable[FlowRecord], *, expected_generation: int, max_inputs: int = 1_000
    ) -> _PointT:
        """Process a bounded source slice and publish it atomically, without retry.

        Reads at most max_inputs (no lookahead), including filtered inputs in the
        committed position. Input iteration/callbacks may have external effects
        even if later publication fails. Re-seek a replayable source on conflict.
        """
        _count(expected_generation, "expected_generation")
        if type(max_inputs) is not int or not 1 <= max_inputs <= _MAX_BATCH_INPUTS:
            raise ValidationError("max_inputs must be between 1 and 10000")
        before = self.latest()
        if before.generation != expected_generation:
            raise RecoveryConflict("flow journal generation changed before processing")
        runtime = self._runtime(before.checkpoint)
        payloads: list[tuple[int, str, str]] = []
        encoded_bytes = 0
        iterator = iter(records)
        for _ in range(max_inputs):
            try:
                record = next(iterator)
            except StopIteration:
                break
            source_position = runtime.processed_inputs
            outputs = runtime.process(record)
            if len(payloads) + len(outputs) > _MAX_BATCH_OUTPUTS:
                raise ValidationError("flow transaction output count exceeds 100000")
            for output in outputs:
                item = self._output(
                    before.checkpoint.emitted_records + len(payloads), source_position, output
                )
                payload = _encode(item.to_dict())
                encoded_bytes += len(payload.encode("utf-8"))
                if encoded_bytes > _MAX_DOCUMENT_BYTES:
                    raise ValidationError("flow transaction output bytes exceed 64 MiB")
                payloads.append((item.sequence, payload, _digest(payload)))
        if runtime.processed_inputs == before.next_position:
            # Empty input is a read-only no-op, but must still detect a raced head.
            current = self.latest()
            if current.generation != expected_generation:
                raise RecoveryConflict("flow journal generation changed during empty input")
            return current
        after = self._point(
            _count(before.generation + 1, "generation"),
            runtime.processed_inputs,
            runtime.checkpoint(),
        )
        payload = _encode(after.to_dict())
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current = self._latest(connection)
                if current != before:
                    raise RecoveryConflict(
                        "flow journal changed during processing; no result committed"
                    )
                connection.executemany("INSERT INTO flow_output VALUES (?, ?, ?)", payloads)
                connection.execute(
                    "UPDATE flow_head SET payload=?, digest=? WHERE slot=1",
                    (payload, _digest(payload)),
                )
            return after
        except sqlite3.Error as exc:
            raise OutputError("cannot commit flow transaction; previous prefix retained") from exc

    def outputs(self, *, start: int = 0, limit: int = 1_000) -> Iterator[_OutputT]:
        """Iterate a stable bounded output page; explicitly close to release its read lock."""
        _count(start, "start")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValidationError("output page limit must be between 1 and 10000")
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point = self._latest(connection)
                previous_source = -1
                previous_output: _OutputT | None = None
                read_start = max(0, start - 1)
                rows = connection.execute(
                    "SELECT seq, CASE WHEN typeof(payload)='text' "
                    "AND length(CAST(payload AS BLOB))<=? THEN payload END, "
                    "CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
                    "THEN digest END, length(CAST(payload AS BLOB)) "
                    "FROM flow_output WHERE seq>=? ORDER BY seq LIMIT ?",
                    (_MAX_DOCUMENT_BYTES, read_start, limit + int(start > 0)),
                )
                for expected, (sequence, payload, digest, size) in enumerate(rows, read_start):
                    document = _decode(payload, digest, size)
                    item = self._decode_output(document)
                    if (
                        sequence != expected
                        or item.sequence != sequence
                        or not previous_source <= item.source_position < point.next_position
                    ):
                        raise ValidationError("flow output position/order is invalid")
                    self._validate_output(item, previous_output)
                    if _encode(item.to_dict()) != payload:
                        raise ValidationError("flow output violates its record contract")
                    previous_source = item.source_position
                    previous_output = item
                    if sequence >= start:
                        yield item
        except sqlite3.Error as exc:
            raise OutputError("cannot read flow outputs") from exc


class FlowJournal(_JournalEngine[Dataflow, FlowRecoveryPoint, FlowOutput]):
    """One fixed linear flow/source per SQLite file with atomic CAS publication."""

    _application_id = _APPLICATION_ID
    _application_pragma = "PRAGMA application_id = 1397835338"

    def _runtime(self, checkpoint: FlowCheckpoint | None = None) -> FlowRuntime:
        return (
            FlowRuntime(self.flow)
            if checkpoint is None
            else FlowRuntime.from_checkpoint(self.flow, checkpoint)
        )

    def _point(
        self, generation: int, position: int, checkpoint: FlowCheckpoint
    ) -> FlowRecoveryPoint:
        return FlowRecoveryPoint(self.source_id, generation, position, checkpoint)

    def _decode_point(self, document: Any) -> FlowRecoveryPoint:
        _point_document(document)
        return FlowRecoveryPoint(
            document["source_id"],
            document["generation"],
            document["next_position"],
            FlowCheckpoint.from_dict(document["checkpoint"]),
        )

    def _output(self, sequence: int, position: int, value: FlowRecord | GraphOutput) -> FlowOutput:
        if type(value) is not FlowRecord:
            raise ValidationError("linear journal requires FlowRecord output")
        return FlowOutput(sequence, position, value)

    def _decode_output(self, document: Any) -> FlowOutput:
        if type(document) is not dict or set(document) != {"sequence", "source_position", "record"}:
            raise ValidationError("invalid flow output fields")
        return FlowOutput(
            document["sequence"], document["source_position"], _stored_record(document["record"])
        )

    def _validate_output(self, item: FlowOutput, previous: FlowOutput | None) -> None:
        if item.record.byte_size > self.flow.limits.max_record_bytes:
            raise ValidationError("flow output violates its record contract")


__all__ = ["FlowJournal", "FlowOutput", "FlowRecoveryPoint"]
