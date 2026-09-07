"""Transactional local source/state/output recovery for generalized dataflows.

Callbacks execute outside the database write lock on a detached runtime. A
generation conflict is returned to the caller; callbacks are never auto-retried.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
        _input_id(self.source_id)
        _count(self.generation, "generation")
        _count(self.next_position, "next_position")
        if (
            type(self.checkpoint) is not FlowCheckpoint
            or self.checkpoint.processed_inputs != self.next_position
        ):
            raise ValidationError("flow checkpoint input count must match the source position")
        if self.checkpoint.emitted_records > _MAX_RECORDS:
            raise ValidationError("flow journal output capacity exceeded")
        if not self.generation <= self.next_position <= self.generation * _MAX_BATCH_INPUTS or (
            self.generation == 0 and (self.checkpoint.emitted_records or self.checkpoint.cells)
        ):
            raise ValidationError("flow journal generation and source prefix are inconsistent")

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


class FlowJournal:
    """One fixed flow/source per SQLite file with compare-and-swap publication.

    Stored output count is capped at one million; use a new source/journal for
    another bounded run. This is not broker acknowledgement, replicated storage
    or a transactional wrapper around external callback side effects.
    """

    def __init__(
        self, path: str | Path, flow: Dataflow, source_id: str, *, create: bool = True
    ) -> None:
        FlowRuntime(flow)
        _input_id(source_id)
        if type(create) is not bool:
            raise ValidationError("create must be boolean")
        self.path = Path(path).absolute()
        self.flow = flow
        self.source_id = source_id
        if not create and not self.path.is_file():
            raise ValidationError("flow journal does not exist")
        try:
            if create:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            with closing(self._connect(create=create)) as connection, connection:
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
                    connection.execute("PRAGMA application_id = 1397835338")
                    connection.execute("PRAGMA user_version = 1")
                    connection.execute(
                        "CREATE TABLE flow_head (slot INTEGER PRIMARY KEY CHECK(slot=1), "
                        "payload TEXT NOT NULL, digest TEXT NOT NULL)"
                    )
                    connection.execute(
                        "CREATE TABLE flow_output (seq INTEGER PRIMARY KEY, "
                        "payload TEXT NOT NULL, digest TEXT NOT NULL)"
                    )
                    initial = FlowRecoveryPoint(source_id, 0, 0, FlowRuntime(flow).checkpoint())
                    payload = _encode(initial.to_dict())
                    connection.execute(
                        "INSERT INTO flow_head VALUES (1, ?, ?)", (payload, _digest(payload))
                    )
                elif (
                    tables != {"flow_head", "flow_output"}
                    or application_id != _APPLICATION_ID
                    or connection.execute("PRAGMA user_version").fetchone() != (1,)
                ):
                    raise ValidationError("unsupported or unrelated flow journal schema")
                self._latest(connection)
        except (sqlite3.Error, OSError) as exc:
            raise OutputError("cannot initialize flow journal") from exc

    def _connect(self, *, create: bool = False) -> sqlite3.Connection:
        # mode=rw avoids silently recreating a journal removed after construction.
        uri = self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
        connection = sqlite3.connect(uri, uri=True, timeout=10)
        try:
            connection.execute("PRAGMA synchronous=FULL")
        except BaseException:
            # The caller's closing() scope has not received this connection yet.
            connection.close()
            raise
        return connection

    def _latest(self, connection: sqlite3.Connection) -> FlowRecoveryPoint:
        if connection.execute("PRAGMA application_id").fetchone() != (
            _APPLICATION_ID,
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
        if (
            type(document) is not dict
            or set(document)
            != {"schema_version", "source_id", "generation", "next_position", "checkpoint"}
            or document["schema_version"] != "1.0"
        ):
            raise ValidationError("invalid flow recovery point fields")
        point = FlowRecoveryPoint(
            document["source_id"],
            document["generation"],
            document["next_position"],
            FlowCheckpoint.from_dict(document["checkpoint"]),
        )
        if point.source_id != self.source_id or point.checkpoint.identity != self.flow.identity:
            raise ValidationError("flow journal source or flow identity mismatch")
        FlowRuntime.from_checkpoint(self.flow, point.checkpoint)
        count, first, last = connection.execute(
            "SELECT count(*), min(seq), max(seq) FROM flow_output"
        ).fetchone()
        if count != point.checkpoint.emitted_records or (
            count and (first != 0 or last != count - 1)
        ):
            raise ValidationError("flow journal output prefix is inconsistent with its head")
        return point

    def latest(self) -> FlowRecoveryPoint:
        """Load one consistent verified head/state snapshot."""
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN")
                return self._latest(connection)
        except sqlite3.Error as exc:
            raise OutputError("cannot read flow journal") from exc

    def advance(
        self, records: Iterable[FlowRecord], *, expected_generation: int, max_inputs: int = 1_000
    ) -> FlowRecoveryPoint:
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
        runtime = FlowRuntime.from_checkpoint(self.flow, before.checkpoint)
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
            for record in outputs:
                item = FlowOutput(
                    before.checkpoint.emitted_records + len(payloads), source_position, record
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
        after = FlowRecoveryPoint(
            self.source_id,
            _count(before.generation + 1, "generation"),
            runtime.processed_inputs,
            runtime.checkpoint(),
        )
        payload = _encode(after.to_dict())
        try:
            with closing(self._connect()) as connection, connection:
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

    def outputs(self, *, start: int = 0, limit: int = 1_000) -> Iterator[FlowOutput]:
        """Iterate a stable bounded output page; explicitly close to release its read lock."""
        _count(start, "start")
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValidationError("output page limit must be between 1 and 10000")
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN")
                point = self._latest(connection)
                previous_source = -1
                rows = connection.execute(
                    "SELECT seq, CASE WHEN typeof(payload)='text' "
                    "AND length(CAST(payload AS BLOB))<=? THEN payload END, "
                    "CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
                    "THEN digest END, length(CAST(payload AS BLOB)) "
                    "FROM flow_output WHERE seq>=? ORDER BY seq LIMIT ?",
                    (_MAX_DOCUMENT_BYTES, start, limit),
                )
                for expected, (sequence, payload, digest, size) in enumerate(rows, start):
                    document = _decode(payload, digest, size)
                    if type(document) is not dict or set(document) != {
                        "sequence",
                        "source_position",
                        "record",
                    }:
                        raise ValidationError("invalid flow output fields")
                    value = document["record"]
                    if type(value) is not dict or set(value) != {"key", "value"}:
                        raise ValidationError("invalid stored FlowRecord fields")
                    item = FlowOutput(
                        document["sequence"],
                        document["source_position"],
                        FlowRecord(value["value"], value["key"]),
                    )
                    if (
                        sequence != expected
                        or item.sequence != sequence
                        or not previous_source <= item.source_position < point.next_position
                    ):
                        raise ValidationError("flow output position/order is invalid")
                    if (
                        item.record.byte_size > self.flow.limits.max_record_bytes
                        or _encode(item.to_dict()) != payload
                    ):
                        raise ValidationError("flow output violates its record contract")
                    previous_source = item.source_position
                    yield item
        except sqlite3.Error as exc:
            raise OutputError("cannot read flow outputs") from exc


__all__ = ["FlowJournal", "FlowOutput", "FlowRecoveryPoint"]
