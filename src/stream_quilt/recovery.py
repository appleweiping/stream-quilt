"""SQLite recovery points with atomic source offsets, state, and window outputs.

This is a local transactional sink. External sources/sinks must participate in
their own delivery protocol; committing here does not acknowledge a broker.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Generator, Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_quilt.checkpoint import AlignerCheckpoint
from stream_quilt.errors import OutputError, ValidationError
from stream_quilt.io import _reject_duplicate_keys, _reject_json_constant, event_from_dict
from stream_quilt.limits import (
    MAX_EVENT_FILE_BYTES,
    MAX_EVENTS,
    MAX_EVENTS_PER_WINDOW,
    MAX_OUTPUT_WINDOWS,
)
from stream_quilt.models import (
    AlignedWindow,
    AlignmentConfig,
    Event,
    RetentionPolicy,
    _bounded_tuple,
)


class RecoveryConflict(ValidationError):
    """The expected database generation is stale; no changes were committed."""


@dataclass(frozen=True, slots=True)
class RecoveryPoint:
    generation: int
    input_id: str
    position: int
    checkpoint: AlignerCheckpoint


def _encode(document: Any) -> str:
    # Check before committing; never write a record the bounded reader cannot load.
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), allow_nan=False)
    chunks: list[str] = []
    size = 0
    for chunk in encoder.iterencode(document):
        size += len(chunk.encode("utf-8"))
        if size > MAX_EVENT_FILE_BYTES:
            raise ValidationError("recovery document exceeds the byte limit")
        chunks.append(chunk)
    return "".join(chunks)


def _finite_json_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("JSON number is outside the finite float range")
    return result


def _decode(payload: str, digest: str) -> Any:
    if type(payload) is not str or type(digest) is not str:
        raise ValidationError("recovery records must contain text and a checksum")
    if len(payload.encode("utf-8")) > MAX_EVENT_FILE_BYTES:
        raise ValidationError("recovery document exceeds the byte limit")
    if hashlib.sha256(payload.encode()).hexdigest() != digest:
        raise ValidationError("recovery checksum mismatch")
    try:
        return json.loads(
            payload,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (ValueError, RecursionError) as exc:
        raise ValidationError("invalid recovery JSON") from exc


def _window_document(document: Any, index: int) -> dict[str, Any]:
    """Restore the exact serialized window contract, including derived fields."""
    expected = {
        "index",
        "start_ms",
        "end_ms",
        "complete",
        "missing_streams",
        "modalities",
        "events",
    }
    if not isinstance(document, Mapping) or set(document) != expected:
        raise ValidationError("invalid recovery window fields")
    if type(document["index"]) is not int or document["index"] != index:
        raise ValidationError("invalid recovery window index")
    if type(document["complete"]) is not bool:
        raise ValidationError("recovery window complete must be a boolean")
    for name in ("events", "missing_streams", "modalities"):
        if not isinstance(document[name], list):
            raise ValidationError(f"recovery window {name} must be a JSON array")
    if any(type(value) is not str for value in document["modalities"]):
        raise ValidationError("recovery window modalities must contain strings")
    if len(document["events"]) > MAX_EVENTS_PER_WINDOW:
        raise ValidationError("recovery window exceeds the event limit")
    window = AlignedWindow(
        index=document["index"],
        start_ms=document["start_ms"],
        end_ms=document["end_ms"],
        events=tuple(event_from_dict(event) for event in document["events"]),
        missing_streams=tuple(document["missing_streams"]),
    )
    checked = window.to_dict()
    # JSON comparison preserves scalar types (True is not 1) and rejects
    # silently normalized event fields or inconsistent derived attributes.
    if _encode(document) != _encode(checked):
        raise ValidationError("recovery window does not match its serialized contract")
    return checked


def _stored_document(payload: Any, digest: Any, payload_bytes: Any) -> Any:
    """Decode fields selected through SQLite's pre-materialization guards."""
    if type(payload_bytes) is int and payload_bytes > MAX_EVENT_FILE_BYTES:
        raise ValidationError("recovery document exceeds the byte limit")
    return _decode(payload, digest)


def _count(value: Any, name: str) -> int:
    if type(value) is not int or not 0 <= value <= 2**53 - 1:
        raise ValidationError(f"{name} must be a non-negative safe integer")
    return value


def _input_id(value: Any) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValidationError("input_id must be a lowercase SHA-256 digest")
    return value


class RecoveryStore:
    """One input/configuration per file; concurrent writers use generation CAS.

    FULL synchronous SQLite commits contain the state, next source position and
    newly emitted windows in one transaction. A failed transaction leaves all
    three at the previous generation. Keep the SQLite file as a unit when backing
    it up; hashes detect accidental corruption but do not authenticate a writer.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with closing(self._connect()) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
                objects = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') "
                        "AND name NOT LIKE 'sqlite_%'"
                    )
                }
                if objects and objects != {"sq_meta", "sq_state", "sq_windows"}:
                    raise ValidationError("unsupported or unrelated recovery database schema")
                conn.execute("CREATE TABLE IF NOT EXISTS sq_meta (version INTEGER NOT NULL)")
                rows = conn.execute("SELECT version FROM sq_meta").fetchall()
                if not rows and not objects:
                    conn.execute("INSERT INTO sq_meta VALUES (1)")
                elif rows != [(1,)]:
                    raise ValidationError("unsupported recovery database schema")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sq_state ("
                    "slot INTEGER PRIMARY KEY CHECK (slot = 1), "
                    "payload TEXT NOT NULL, digest TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS sq_windows ("
                    "idx INTEGER PRIMARY KEY, payload TEXT NOT NULL, digest TEXT NOT NULL)"
                )
        except (sqlite3.Error, OSError) as exc:
            raise OutputError(f"cannot initialize recovery database: {exc}") from exc

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    @staticmethod
    def _latest(conn: sqlite3.Connection) -> RecoveryPoint | None:
        # The CASE expressions keep oversized/non-text values inside SQLite;
        # applying a limit only after fetchone() would already allocate them.
        row = conn.execute(
            "SELECT CASE WHEN typeof(payload)='text' "
            "AND length(CAST(payload AS BLOB))<=? THEN payload END, "
            "CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
            "THEN digest END, length(CAST(payload AS BLOB)) "
            "FROM sq_state WHERE slot=1",
            (MAX_EVENT_FILE_BYTES,),
        ).fetchone()
        if row is None:
            return None
        document = _stored_document(*row)
        if not isinstance(document, Mapping) or set(document) != {
            "generation",
            "input_id",
            "position",
            "checkpoint",
        }:
            raise ValidationError("invalid recovery point fields")
        generation = _count(document["generation"], "generation")
        if generation == 0:
            raise ValidationError("stored generation must be positive")
        return RecoveryPoint(
            generation,
            _input_id(document["input_id"]),
            _count(document["position"], "position"),
            AlignerCheckpoint.from_dict(document["checkpoint"]),
        )

    def load(self) -> RecoveryPoint | None:
        """Verify and return the committed point, or None before the first save."""
        try:
            with closing(self._connect()) as conn:
                return self._latest(conn)
        except sqlite3.Error as exc:
            raise ValidationError(f"cannot read recovery database: {exc}") from exc

    def commit(
        self,
        checkpoint: AlignerCheckpoint,
        *,
        input_id: str,
        position: int,
        expected_generation: int,
        windows: Iterable[AlignedWindow] = (),
    ) -> RecoveryPoint:
        """Atomically replace state and add every newly closed window exactly once."""
        if not isinstance(checkpoint, AlignerCheckpoint):
            raise ValidationError("checkpoint must be an AlignerCheckpoint")
        checked = AlignerCheckpoint.from_dict(checkpoint.to_dict())
        input_id = _input_id(input_id)
        position = _count(position, "position")
        expected_generation = _count(expected_generation, "expected_generation")
        generation = _count(expected_generation + 1, "generation")
        output = _bounded_tuple(windows, "windows", AlignedWindow, MAX_OUTPUT_WINDOWS)
        serialized = tuple((window.index, _encode(window.to_dict())) for window in output)
        payload = _encode(
            {
                "generation": generation,
                "input_id": input_id,
                "position": position,
                "checkpoint": checked.to_dict(),
            }
        )
        try:
            with closing(self._connect()) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
                previous = self._latest(conn)
                if (previous.generation if previous else 0) != expected_generation:
                    raise RecoveryConflict("recovery generation changed; reload before committing")
                old_index = previous.checkpoint.next_index if previous else 0
                if checked.next_index < old_index:
                    raise ValidationError("recovery window index cannot move backwards")
                if previous:
                    if previous.input_id != input_id or previous.position > position:
                        raise ValidationError("recovery source identity or offset mismatch")
                    if (
                        previous.checkpoint.config_digest != checked.config_digest
                        or previous.checkpoint.retention != checked.retention
                    ):
                        raise ValidationError("recovery configuration mismatch")
                    if previous.checkpoint.closed:
                        raise ValidationError("cannot commit after the source was flushed")
                if tuple(index for index, _ in serialized) != tuple(
                    range(old_index, checked.next_index)
                ):
                    raise ValidationError("commit must contain every newly closed window in order")
                for index, encoded in serialized:
                    conn.execute(
                        "INSERT INTO sq_windows VALUES (?, ?, ?)",
                        (index, encoded, hashlib.sha256(encoded.encode()).hexdigest()),
                    )
                conn.execute(
                    "INSERT OR REPLACE INTO sq_state VALUES (1, ?, ?)",
                    (payload, hashlib.sha256(payload.encode()).hexdigest()),
                )
        except sqlite3.Error as exc:
            raise OutputError(f"cannot commit recovery transaction: {exc}") from exc
        return RecoveryPoint(generation, input_id, position, checked)

    def window_documents(self) -> tuple[dict[str, Any], ...]:
        """Collect verified output; memory scales with the sum of all output rows.

        Use ``iter_window_documents`` to consume one output row at a time.
        """
        return tuple(self.iter_window_documents())

    def iter_window_documents(self) -> Generator[dict[str, Any], None, None]:
        """Yield validated output rows from one consistent SQLite read transaction.

        Fully exhausting the iterator verifies completeness. Early close only
        validates the consumed prefix. Use ``contextlib.closing`` when stopping
        early: the connection and transaction live until exhaustion or close.
        Memory includes the materialized checkpoint and current output row;
        SQLite's own page cache is outside the Python per-document byte bound.
        """
        try:
            with closing(self._connect()) as conn, conn:
                conn.execute("BEGIN")
                point = self._latest(conn)
                rows = conn.execute(
                    "SELECT idx, CASE WHEN typeof(payload)='text' "
                    "AND length(CAST(payload AS BLOB))<=? THEN payload END, "
                    "CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
                    "THEN digest END, length(CAST(payload AS BLOB)) "
                    "FROM sq_windows ORDER BY idx",
                    (MAX_EVENT_FILE_BYTES,),
                )
                count = 0
                for index, payload, digest, payload_bytes in rows:
                    if index != count or index >= MAX_OUTPUT_WINDOWS:
                        raise ValidationError("recovery windows are missing or out of order")
                    document = _stored_document(payload, digest, payload_bytes)
                    yield _window_document(document, index)
                    count += 1
                expected = point.checkpoint.next_index if point else 0
                if count != expected:
                    raise ValidationError("recovery windows disagree with committed state")
        except sqlite3.Error as exc:
            raise ValidationError(f"cannot read recovery windows: {exc}") from exc


def resume_events(
    events: Iterable[Event],
    config: AlignmentConfig,
    database: str | Path,
    *,
    batch_size: int = 100,
    max_new_events: int | None = None,
    retention: RetentionPolicy | None = None,
) -> RecoveryPoint:
    """Run or resume a finite event source, committing outputs with each offset.

    ``max_new_events`` supports cooperative interruption at a durable boundary.
    The complete finite source is hashed before executing; a changed source is
    rejected even if its first records happen to match. Repeated calls after
    completion only verify the existing state. External side effects are absent.
    """
    from stream_quilt.aligner import WatermarkAligner

    if type(batch_size) is not int or not 1 <= batch_size <= MAX_EVENTS:
        raise ValidationError("batch_size must be between 1 and MAX_EVENTS")
    if max_new_events is not None:
        _count(max_new_events, "max_new_events")
    checked = tuple(
        Event(**event.to_dict()) for event in _bounded_tuple(events, "events", Event, MAX_EVENTS)
    )
    input_id = hashlib.sha256(_encode([event.to_dict() for event in checked]).encode()).hexdigest()
    store = RecoveryStore(database)
    point = store.load()
    if point is None:
        aligner = WatermarkAligner(config, retention=retention)
        position, generation = 0, 0
    else:
        if point.input_id != input_id or point.position > len(checked):
            raise ValidationError("recovery source identity or offset mismatch")
        aligner = WatermarkAligner.from_checkpoint(config, point.checkpoint, retention=retention)
        for _ in store.iter_window_documents():
            pass  # Verify the entire output without accumulating it in memory.
        if point.checkpoint.closed:
            return point
        position, generation = point.position, point.generation
    stop = len(checked) if max_new_events is None else min(len(checked), position + max_new_events)
    pending: list[AlignedWindow] = []
    since_commit = 0
    while position < stop:
        pending.extend(aligner.ingest(checked[position]))
        position += 1
        since_commit += 1
        if since_commit == batch_size and position < stop:
            point = store.commit(
                aligner.checkpoint(),
                input_id=input_id,
                position=position,
                expected_generation=generation,
                windows=pending,
            )
            generation = point.generation
            pending.clear()
            since_commit = 0
    if position == len(checked):
        pending.extend(aligner.flush())
    return store.commit(
        aligner.checkpoint(),
        input_id=input_id,
        position=position,
        expected_generation=generation,
        windows=pending,
    )
