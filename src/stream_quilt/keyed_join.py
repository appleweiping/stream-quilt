"""Bounded incremental keyed joins over explicitly ordered, tagged local arrivals.

This is a single-owner in-memory runtime, not a multi-source DAG scheduler or
transactional external-offset store. Closing a side is an explicit EOF signal.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

from .dataflow import FlowRecord, _count, _key, _snapshot
from .errors import ValidationError
from .io import _reject_duplicate_keys, _reject_json_constant

JoinInsertMode = Literal["first", "last", "product"]
JoinEmitMode = Literal["complete", "final", "running"]
JoinPhase = Literal["open", "draining", "closed"]
_MAX_COUNT = 2**53 - 1
_MAX_DOCUMENT_BYTES = 66 * 1024 * 1024
_CEILINGS = {
    "max_keys": 100_000,
    "max_values_per_side": 10_000,
    "max_values": 1_000_000,
    "max_value_bytes": 8 * 1024 * 1024,
    "max_key_bytes": 16 * 1024 * 1024,
    "max_state_bytes": 64 * 1024 * 1024,
    "max_rows_per_key": 100_000,
    "max_rows_per_batch": 100_000,
    "max_row_bytes": 8 * 1024 * 1024,
    "max_batch_bytes": 64 * 1024 * 1024,
}
_Values = tuple[tuple[str, ...], ...]
_WireCell = tuple[str, _Values]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _sides(value: Any) -> tuple[str, ...]:
    if type(value) is not tuple or not 2 <= len(value) <= 16:
        raise ValidationError("join sides must be an ordered tuple of 2..16 names")
    for side in value:
        _key(side, "join side")
    if len(set(value)) != len(value):
        raise ValidationError("join side names must be unique")
    return value


@dataclass(frozen=True, slots=True)
class JoinLimits:
    """Payload/count ceilings, not Python allocator or execution-time limits."""

    max_keys: int = 10_000
    max_values_per_side: int = 256
    max_values: int = 100_000
    max_value_bytes: int = 1024 * 1024
    max_key_bytes: int = 4 * 1024 * 1024
    max_state_bytes: int = 16 * 1024 * 1024
    max_rows_per_key: int = 1000
    max_rows_per_batch: int = 10_000
    max_row_bytes: int = 1024 * 1024
    max_batch_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        for name, ceiling in _CEILINGS.items():
            _count(getattr(self, name), name, 1, ceiling)
        if self.max_rows_per_key > self.max_rows_per_batch:
            raise ValidationError("one key must fit the configured batch row limit")
        if self.max_row_bytes > self.max_batch_bytes:
            raise ValidationError("one row must fit the configured batch byte limit")

    def to_dict(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in _CEILINGS}


_HARD_LIMITS = JoinLimits(**_CEILINGS)


@dataclass(frozen=True, slots=True)
class KeyedJoin:
    """Explicit local join semantics; changing ordered sides changes identity."""

    join_id: str
    revision: str
    sides: tuple[str, ...]
    insert_mode: JoinInsertMode = "last"
    emit_mode: JoinEmitMode = "complete"
    limits: JoinLimits = field(default_factory=JoinLimits)

    def __post_init__(self) -> None:
        _key(self.join_id, "join_id")
        _key(self.revision, "join revision")
        _sides(self.sides)
        if type(self.insert_mode) is not str or self.insert_mode not in (
            "first",
            "last",
            "product",
        ):
            raise ValidationError("unknown join insertion mode")
        if type(self.emit_mode) is not str or self.emit_mode not in (
            "complete",
            "final",
            "running",
        ):
            raise ValidationError("unknown join emission mode")
        if type(self.limits) is not JoinLimits:
            raise ValidationError("join limits must be JoinLimits")
        self.limits.__post_init__()

    @property
    def identity(self) -> str:
        return hashlib.sha256(
            _json(
                {
                    "kind": "stream-quilt-keyed-join",
                    "version": "1.0",
                    "join_id": self.join_id,
                    "revision": self.revision,
                    "sides": self.sides,
                    "insert_mode": self.insert_mode,
                    "emit_mode": self.emit_mode,
                    "limits": self.limits.to_dict(),
                }
            ).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class JoinRow:
    """A key and ordered side values; absent and present JSON null are distinct."""

    key: str
    sides: tuple[str, ...]
    present: tuple[bool, ...]
    _values: tuple[str, ...] = field(repr=False)

    def __init__(
        self, key: str, sides: tuple[str, ...], present: tuple[bool, ...], values: tuple[Any, ...]
    ) -> None:
        _key(key, "join row key")
        _sides(sides)
        if (
            type(present) is not tuple
            or type(values) is not tuple
            or len(present) != len(sides)
            or len(values) != len(sides)
            or any(type(flag) is not bool for flag in present)
            or not any(present)
        ):
            raise ValidationError("join row requires one presence flag and value per side")
        if any(not flag and value is not None for flag, value in zip(present, values, strict=True)):
            raise ValidationError("absent join values must be represented by null")
        encoded = tuple(_snapshot(value, _HARD_LIMITS.max_value_bytes) for value in values)
        if _row_bytes(present, encoded) > _HARD_LIMITS.max_row_bytes:
            raise ValidationError("join row exceeds the byte ceiling")
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "sides", sides)
        object.__setattr__(self, "present", present)
        object.__setattr__(self, "_values", encoded)

    @property
    def values(self) -> tuple[Any, ...]:
        return tuple(json.loads(value) for value in self._values)

    @property
    def byte_size(self) -> int:
        return _row_bytes(self.present, self._values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "sides": list(self.sides),
            "present": list(self.present),
            "values": list(self.values),
        }

    def to_record(self) -> FlowRecord:
        """Explicit conversion subject to FlowRecord's separate aggregate JSON limits."""
        return FlowRecord({"present": list(self.present), "values": list(self.values)}, self.key)


def _row_overhead(present: tuple[bool, ...]) -> int:
    # Count the complete record VALUE, including presence/array JSON punctuation.
    return len(_json({"present": present, "values": []}).encode("utf-8")) + len(present) - 1


def _row_bytes(present: tuple[bool, ...], values: tuple[str, ...]) -> int:
    return _row_overhead(present) + sum(len(value.encode("utf-8")) for value in values)


@dataclass(frozen=True, slots=True)
class JoinBatch:
    """One fully committed operation, not delivery acknowledgement of its rows."""

    rows: tuple[JoinRow, ...]
    phase: JoinPhase
    pending_keys: int
    drained_keys: int = 0

    def __post_init__(self) -> None:
        if type(self.rows) is not tuple or len(self.rows) > _HARD_LIMITS.max_rows_per_batch:
            raise ValidationError("join batch rows must be a bounded tuple")
        if any(type(row) is not JoinRow for row in self.rows):
            raise ValidationError("join batch rows must be JoinRow values")
        if type(self.phase) is not str or self.phase not in ("open", "draining", "closed"):
            raise ValidationError("invalid join phase")
        _count(self.pending_keys, "pending join keys", 0, _HARD_LIMITS.max_keys)
        _count(self.drained_keys, "drained join keys", 0, _HARD_LIMITS.max_keys)
        if (self.phase == "closed" and self.pending_keys) or (
            self.phase == "draining" and not self.pending_keys
        ):
            raise ValidationError("join phase contradicts pending keys")
        if self.rows and any(row.sides != self.rows[0].sides for row in self.rows):
            raise ValidationError("join batch side order must be uniform")
        byte_size = 0
        for row in self.rows:
            byte_size += row.byte_size
            if byte_size > _HARD_LIMITS.max_batch_bytes:
                raise ValidationError("join batch exceeds the byte ceiling")


@dataclass(frozen=True, slots=True)
class _Cell:
    values: _Values
    byte_size: int
    count: int
    rows: int
    output_bytes: int


def _shape(
    key: Any,
    values: Any,
    side_count: int,
    limits: JoinLimits,
    kind: type[list[Any]] | type[tuple[Any, ...]],
) -> tuple[int, int]:
    _key(key, "join state key")
    if type(values) is not kind or len(values) != side_count:
        raise ValidationError("join state requires an array for every side")
    count = 0
    size = len(_json({"key": key, "values": []}).encode("utf-8")) + side_count - 1
    for side in values:
        if type(side) is not kind or len(side) > limits.max_values_per_side:
            raise ValidationError("join side state exceeds its value-count limit")
        count += len(side)
        size += 2 + max(0, len(side) - 1)
        for value in side:
            if type(value) is not str or len(value) > limits.max_value_bytes:
                raise ValidationError("join state values must be bounded encoded JSON strings")
            try:
                if len(value.encode("utf-8")) > limits.max_value_bytes:
                    raise ValidationError("join state value exceeds its UTF-8 byte limit")
                size += len(_json(value).encode("utf-8"))
            except UnicodeError as exc:
                raise ValidationError("join state JSON must contain valid Unicode") from exc
            if size > limits.max_key_bytes:
                raise ValidationError("join key exceeds its encoded cell byte limit")
    if not count:
        raise ValidationError("empty join key state must be removed")
    if size > limits.max_key_bytes:
        raise ValidationError("join key exceeds its encoded cell byte limit")
    return count, size


def _cell(key: str, values: _Values, limits: JoinLimits) -> _Cell:
    count, size = _shape(key, values, len(values), limits, tuple)
    present = tuple(bool(side) for side in values)
    lengths = tuple(max(1, len(side)) for side in values)
    rows = 1
    for length in lengths:
        if rows > limits.max_rows_per_key // length:
            raise ValidationError("join Cartesian product exceeds the per-key row limit")
        rows *= length
    overhead = _row_overhead(present)
    maximum = overhead
    output_bytes = overhead * rows
    for length, side in zip(lengths, values, strict=True):
        sizes = tuple(len(value.encode("utf-8")) for value in side) or (4,)
        maximum += max(sizes)
        output_bytes += (rows // length) * sum(sizes)
    if maximum > limits.max_row_bytes or output_bytes > limits.max_batch_bytes:
        raise ValidationError("join projected output exceeds a row or batch byte limit")
    return _Cell(values, size, count, rows, output_bytes)


def _rows(key: str, cell: _Cell, sides: tuple[str, ...]) -> Iterator[JoinRow]:
    present = tuple(bool(values) for values in cell.values)
    for values in itertools.product(*(values or ("null",) for values in cell.values)):
        # Only admitted immutable canonical strings reach this private constructor.
        row = object.__new__(JoinRow)
        object.__setattr__(row, "key", key)
        object.__setattr__(row, "sides", sides)
        object.__setattr__(row, "present", present)
        object.__setattr__(row, "_values", values)
        yield row


@dataclass(frozen=True, slots=True)
class JoinCheckpoint:
    """Canonical local scheduling state; no source offsets or authenticated identity."""

    identity: str
    sides: tuple[str, ...]
    closed_sides: tuple[bool, ...]
    phase: JoinPhase
    processed_inputs: tuple[int, ...]
    emitted_rows: int
    cells: tuple[_WireCell, ...]

    def __post_init__(self) -> None:
        _sides(self.sides)
        if (
            type(self.identity) is not str
            or len(self.identity) != 64
            or any(char not in "0123456789abcdef" for char in self.identity)
        ):
            raise ValidationError("join checkpoint identity must be a SHA-256 hex digest")
        if (
            type(self.closed_sides) is not tuple
            or len(self.closed_sides) != len(self.sides)
            or any(type(flag) is not bool for flag in self.closed_sides)
            or type(self.processed_inputs) is not tuple
            or len(self.processed_inputs) != len(self.sides)
        ):
            raise ValidationError("invalid join checkpoint side metadata")
        for count in self.processed_inputs:
            _count(count, "processed side inputs", 0, _MAX_COUNT)
        _count(sum(self.processed_inputs), "total processed inputs", 0, _MAX_COUNT)
        _count(self.emitted_rows, "emitted join rows", 0, _MAX_COUNT)
        if self.emitted_rows > sum(self.processed_inputs) * _HARD_LIMITS.max_rows_per_key:
            raise ValidationError("join output count is impossible for admitted inputs")
        if type(self.cells) is not tuple or len(self.cells) > _HARD_LIMITS.max_keys:
            raise ValidationError("join checkpoint cells must be a bounded tuple")
        if type(self.phase) is not str or self.phase not in ("open", "draining", "closed"):
            raise ValidationError("invalid join checkpoint phase")
        if (self.phase == "open") == all(self.closed_sides):
            raise ValidationError("join phase contradicts side EOF state")
        if (self.phase == "closed" and self.cells) or (self.phase == "draining" and not self.cells):
            raise ValidationError("join phase contradicts retained state")
        total_count = total_bytes = 0
        side_counts = [0] * len(self.sides)
        previous: str | None = None
        # Shape and cumulative byte/count admission precede nested JSON parsing.
        for cell in self.cells:
            if type(cell) is not tuple or len(cell) != 2:
                raise ValidationError("invalid join checkpoint cell")
            key, values = cell
            count, size = _shape(key, values, len(self.sides), _HARD_LIMITS, tuple)
            if previous is not None and key <= previous:
                raise ValidationError("join cells must have unique sorted keys")
            previous = key
            total_count += count
            total_bytes += size
            if total_count > _HARD_LIMITS.max_values or total_bytes > _HARD_LIMITS.max_state_bytes:
                raise ValidationError("join checkpoint exceeds total state limits")
            for index, side in enumerate(values):
                side_counts[index] += len(side)
        if any(
            count > admitted
            for count, admitted in zip(side_counts, self.processed_inputs, strict=True)
        ):
            raise ValidationError("join retained values exceed side input counts")
        for key, values in self.cells:
            for side in values:
                for encoded in side:
                    try:
                        value = json.loads(
                            encoded,
                            object_pairs_hook=_reject_duplicate_keys,
                            parse_constant=_reject_json_constant,
                        )
                        if _snapshot(value, _HARD_LIMITS.max_value_bytes) != encoded:
                            raise ValidationError("join state JSON must be canonical")
                    except (ValueError, RecursionError) as exc:
                        raise ValidationError("invalid encoded join state JSON") from exc
            _cell(key, values, _HARD_LIMITS)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "stream-quilt-join-checkpoint",
            "version": "1.0",
            "identity": self.identity,
            "sides": list(self.sides),
            "closed_sides": list(self.closed_sides),
            "phase": self.phase,
            "processed_inputs": list(self.processed_inputs),
            "emitted_rows": self.emitted_rows,
            "cells": [
                {"key": key, "values": [list(side) for side in values]}
                for key, values in self.cells
            ],
        }

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, document: Any) -> JoinCheckpoint:
        fields = {
            "kind",
            "version",
            "identity",
            "sides",
            "closed_sides",
            "phase",
            "processed_inputs",
            "emitted_rows",
            "cells",
        }
        if (
            type(document) is not dict
            or set(document) != fields
            or document["kind"] != "stream-quilt-join-checkpoint"
            or document["version"] != "1.0"
        ):
            raise ValidationError("invalid join checkpoint document fields/version")
        for name in ("sides", "closed_sides", "processed_inputs"):
            if type(document[name]) is not list or not 2 <= len(document[name]) <= 16:
                raise ValidationError("invalid join checkpoint metadata arrays")
        cells = document["cells"]
        if type(cells) is not list or len(cells) > _HARD_LIMITS.max_keys:
            raise ValidationError("invalid join checkpoint cell array")
        count = size = 0
        for cell in cells:
            if type(cell) is not dict or set(cell) != {"key", "values"}:
                raise ValidationError("invalid join checkpoint cell fields")
            cell_count, cell_size = _shape(
                cell["key"], cell["values"], len(document["sides"]), _HARD_LIMITS, list
            )
            count += cell_count
            size += cell_size
            if count > _HARD_LIMITS.max_values or size > _HARD_LIMITS.max_state_bytes:
                raise ValidationError("join checkpoint exceeds total state limits")
        return cls(
            document["identity"],
            tuple(document["sides"]),
            tuple(document["closed_sides"]),
            document["phase"],
            tuple(document["processed_inputs"]),
            document["emitted_rows"],
            tuple((cell["key"], tuple(tuple(side) for side in cell["values"])) for cell in cells),
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> JoinCheckpoint:
        if type(payload) not in (str, bytes) or len(payload) > _MAX_DOCUMENT_BYTES:
            raise ValidationError("join checkpoint JSON exceeds its input byte limit")
        try:
            raw = payload.encode("utf-8") if isinstance(payload, str) else payload
            if len(raw) > _MAX_DOCUMENT_BYTES:
                raise ValidationError("join checkpoint JSON exceeds its UTF-8 byte limit")
            document = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (ValueError, RecursionError) as exc:
            raise ValidationError("invalid join checkpoint JSON") from exc
        return cls.from_dict(document)


@dataclass(frozen=True, slots=True)
class _State:
    cells: dict[str, _Cell]
    closed: tuple[bool, ...]
    counts: tuple[int, ...]
    emitted: int = 0
    byte_size: int = 0
    values: int = 0
    phase: JoinPhase = "open"


class JoinRuntime:
    """One deterministic local arrival order with atomic process/close/drain operations."""

    def __init__(self, join: KeyedJoin) -> None:
        if type(join) is not KeyedJoin:
            raise ValidationError("join must be KeyedJoin")
        join.__post_init__()
        self._join = join
        self._state = _State({}, (False,) * len(join.sides), (0,) * len(join.sides))
        self._busy = False

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if self._busy:
            raise ValidationError("join runtime does not allow reentrant operations")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    @property
    def join(self) -> KeyedJoin:
        return self._join

    @property
    def phase(self) -> JoinPhase:
        return self._state.phase

    @property
    def closed_sides(self) -> tuple[bool, ...]:
        return self._state.closed

    def process(self, side: str, record: FlowRecord) -> JoinBatch:
        with self._operation():
            after, batch = _stage_process(self.join, self._state, side, record)
            self._state = after
            return batch

    def close(self, side: str) -> JoinBatch:
        """Signal this side\'s actual EOF; repeating the same signal is a no-op."""
        with self._operation():
            after, batch = _stage_close(self.join, self._state, side)
            self._state = after
            return batch

    def drain(self, *, max_keys: int = 100) -> JoinBatch:
        """Commit a bounded sorted-key final batch; no key is partially emitted."""
        with self._operation():
            after, batch = _stage_drain(self.join, self._state, max_keys)
            self._state = after
            return batch

    def run(
        self, source: Iterator[tuple[str, FlowRecord]], *, max_inputs: int = 1000
    ) -> Iterator[JoinBatch]:
        """Pull at most N tagged inputs; source ownership and side EOF stay explicit."""
        _count(max_inputs, "join run max_inputs", 1, 100_000)
        for _ in range(max_inputs):
            try:
                item = next(source)
            except StopIteration:
                return
            if type(item) is not tuple or len(item) != 2:
                raise ValidationError("join source must yield (side, FlowRecord) tuples")
            yield self.process(item[0], item[1])

    def checkpoint(self) -> JoinCheckpoint:
        with self._operation():
            state = self._state
            return JoinCheckpoint(
                self.join.identity,
                self.join.sides,
                state.closed,
                state.phase,
                state.counts,
                state.emitted,
                tuple((key, state.cells[key].values) for key in sorted(state.cells)),
            )

    @classmethod
    def from_checkpoint(cls, join: KeyedJoin, checkpoint: JoinCheckpoint) -> JoinRuntime:
        runtime = cls(join)
        if type(checkpoint) is not JoinCheckpoint:
            raise ValidationError("join restore requires JoinCheckpoint")
        checkpoint.__post_init__()
        if checkpoint.identity != join.identity or checkpoint.sides != join.sides:
            raise ValidationError("join checkpoint configuration identity mismatch")
        if checkpoint.phase == "draining" and join.emit_mode != "final":
            raise ValidationError("only final joins can retain draining state")
        if checkpoint.phase == "open" and join.emit_mode == "final" and checkpoint.emitted_rows:
            raise ValidationError("final joins cannot emit rows before all sides reach EOF")
        cells: dict[str, _Cell] = {}
        size = count = 0
        for key, values in checkpoint.cells:
            if join.insert_mode != "product" and any(len(side) > 1 for side in values):
                raise ValidationError("first/last join state can retain at most one value per side")
            if join.emit_mode == "complete" and all(values):
                raise ValidationError("complete join state cannot retain a complete key")
            cell = _cell(key, values, join.limits)
            size += cell.byte_size
            count += cell.count
            if (
                len(cells) == join.limits.max_keys
                or size > join.limits.max_state_bytes
                or count > join.limits.max_values
            ):
                raise ValidationError("join checkpoint exceeds configured retained state limits")
            cells[key] = cell
        if (
            checkpoint.emitted_rows
            > sum(checkpoint.processed_inputs) * join.limits.max_rows_per_key
        ):
            raise ValidationError("join checkpoint output count violates configured limits")
        inputs = sum(checkpoint.processed_inputs)
        outputs = checkpoint.emitted_rows
        product = join.insert_mode == "product"
        retained = tuple(
            sum(len(cell.values[index]) for cell in cells.values())
            for index in range(len(join.sides))
        )
        if product and join.emit_mode != "complete" and inputs > join.limits.max_values:
            raise ValidationError("join counters exceed the lifetime product retention limit")
        if checkpoint.phase == "open" and join.emit_mode != "complete":
            if product and retained != checkpoint.processed_inputs:
                raise ValidationError("product join counters contradict retained inputs")
            if not product and any(
                admitted and not saved
                for admitted, saved in zip(checkpoint.processed_inputs, retained, strict=True)
            ):
                raise ValidationError("first/last join counters require retained side state")
        if join.emit_mode == "running" and (
            outputs < inputs or (not product and outputs != inputs)
        ):
            raise ValidationError("running join counters contradict per-arrival emission")
        consumed = tuple(
            admitted - saved
            for admitted, saved in zip(checkpoint.processed_inputs, retained, strict=True)
        )
        if join.emit_mode == "complete" and outputs > min(consumed) * (
            join.limits.max_rows_per_key if product else 1
        ):
            raise ValidationError("complete join counters exceed possible matching cycles")
        if join.emit_mode == "final" and not product and outputs > inputs - len(cells):
            raise ValidationError("final join counters exceed inputs available to drained keys")
        if join.emit_mode == "final":
            if checkpoint.phase == "closed" and inputs and not outputs:
                raise ValidationError("final join counters require nonempty EOF emission")
            if outputs > (join.limits.max_keys - len(cells)) * (
                join.limits.max_rows_per_key if product else 1
            ):
                raise ValidationError("final join counters exceed possible drained keys")
            if product and (
                outputs < max(consumed) or outputs > sum(consumed) * join.limits.max_rows_per_key
            ):
                raise ValidationError("final product counters contradict consumed values")
        runtime._state = _State(
            cells,
            checkpoint.closed_sides,
            checkpoint.processed_inputs,
            checkpoint.emitted_rows,
            size,
            count,
            checkpoint.phase,
        )
        return runtime


def _stage_process(
    join: KeyedJoin,
    before: _State,
    side: str,
    record: FlowRecord,
    admit: Callable[[_State, int, int], None] | None = None,
) -> tuple[_State, JoinBatch]:
    if type(side) is not str or side not in join.sides:
        raise ValidationError("unknown join input side")
    index = join.sides.index(side)
    if before.phase != "open" or before.closed[index]:
        raise ValidationError("join side has already reached EOF")
    if type(record) is not FlowRecord or record.key is None:
        raise ValidationError("join inputs must be keyed FlowRecord values")
    key = _key(record.key, "join input key")
    limits = join.limits
    encoded = _snapshot(record.value, limits.max_value_bytes)
    counts = list(before.counts)
    counts[index] += 1
    _count(sum(counts), "total processed inputs", 0, _MAX_COUNT)
    previous = before.cells.get(key)
    sides = list(previous.values if previous else ((),) * len(join.sides))
    if join.insert_mode == "product":
        if len(sides[index]) == limits.max_values_per_side:
            raise ValidationError("join side exceeds its retained value-count limit")
        sides[index] = (*sides[index], encoded)
    elif join.insert_mode == "last" or not sides[index]:
        sides[index] = (encoded,)
    proposed = _cell(key, tuple(sides), limits)
    complete = join.emit_mode == "complete" and all(sides)
    emit = complete or join.emit_mode == "running"
    emitted = before.emitted + (proposed.rows if emit else 0)
    _count(emitted, "emitted join rows", 0, _MAX_COUNT)
    size = before.byte_size - (previous.byte_size if previous else 0)
    values = before.values - (previous.count if previous else 0)
    if not complete:
        size += proposed.byte_size
        values += proposed.count
    keys = len(before.cells) + (0 if previous else 1) - int(complete)
    if keys > limits.max_keys or size > limits.max_state_bytes or values > limits.max_values:
        raise ValidationError("join retained state exceeds its aggregate limits")
    cells = dict(before.cells)
    if complete:
        cells.pop(key, None)
    else:
        cells[key] = proposed
    after = _State(cells, before.closed, tuple(counts), emitted, size, values)
    if admit is not None:
        admit(after, proposed.rows if emit else 0, proposed.output_bytes if emit else 0)
    rows = tuple(_rows(key, proposed, join.sides)) if emit else ()
    batch = JoinBatch(rows, "open", len(cells))
    return after, batch


def _stage_close(join: KeyedJoin, before: _State, side: str) -> tuple[_State, JoinBatch]:
    if type(side) is not str or side not in join.sides:
        raise ValidationError("unknown join input side")
    index = join.sides.index(side)
    if before.closed[index]:
        return before, JoinBatch((), before.phase, len(before.cells))
    closed = list(before.closed)
    closed[index] = True
    phase: JoinPhase = "open"
    cells, size, values = before.cells, before.byte_size, before.values
    if all(closed):
        if join.emit_mode == "final" and cells:
            phase = "draining"
        else:
            phase, cells, size, values = "closed", {}, 0, 0
    batch = JoinBatch((), phase, len(cells))
    after = _State(cells, tuple(closed), before.counts, before.emitted, size, values, phase)
    return after, batch


def _stage_drain(
    join: KeyedJoin,
    before: _State,
    max_keys: int,
    admit: Callable[[_State, int, int], None] | None = None,
) -> tuple[_State, JoinBatch]:
    _count(max_keys, "drain max_keys", 1, _HARD_LIMITS.max_keys)
    if before.phase == "open":
        raise ValidationError("all join sides must close before final draining")
    if before.phase == "closed":
        return before, JoinBatch((), "closed", 0)
    selected: list[str] = []
    row_count = byte_count = 0
    limits = join.limits
    for key in sorted(before.cells):
        cell = before.cells[key]
        if (
            len(selected) == max_keys
            or row_count + cell.rows > limits.max_rows_per_batch
            or byte_count + cell.output_bytes > limits.max_batch_bytes
        ):
            break
        selected.append(key)
        row_count += cell.rows
        byte_count += cell.output_bytes
    emitted = before.emitted + row_count
    _count(emitted, "emitted join rows", 0, _MAX_COUNT)
    cells = dict(before.cells)
    size, values = before.byte_size, before.values
    for key in selected:
        cell = cells.pop(key)
        size -= cell.byte_size
        values -= cell.count
    phase: JoinPhase = "draining" if cells else "closed"
    after = _State(cells, before.closed, before.counts, emitted, size, values, phase)
    if admit is not None:
        admit(after, row_count, byte_count)
    rows = tuple(row for key in selected for row in _rows(key, before.cells[key], join.sides))
    batch = JoinBatch(rows, phase, len(cells), len(selected))
    return after, batch


__all__ = ["JoinBatch", "JoinCheckpoint", "JoinLimits", "JoinRow", "JoinRuntime", "KeyedJoin"]
