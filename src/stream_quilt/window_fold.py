"""Arrival-ordered, explicitly watermarked local folds with atomic bounded drain.

This is not a timestamp-order buffer, wall clock, durable journal or distributed
operator. Application callbacks and delivery remain outside state atomicity.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, Literal, cast

from .dataflow import _MAX_COUNT, FlowRecord, _count, _key, _no_awaitable, _snapshot, _sync
from .errors import ValidationError
from .flow_journal import _encode
from .io import _reject_duplicate_keys, _reject_json_constant
from .recovery import _finite_json_float

_MAX_WIRE = 66 * 1024 * 1024
_CEILINGS = {
    "max_windows_per_input": 1024,
    "max_keys": 100_000,
    "max_windows": 100_000,
    "max_input_bytes": 8 * 1024 * 1024,
    "max_state_value_bytes": 8 * 1024 * 1024,
    "max_state_bytes": 64 * 1024 * 1024,
    "max_row_bytes": 8 * 1024 * 1024,
    "max_rows_per_batch": 100_000,
    "max_batch_bytes": 64 * 1024 * 1024,
    "max_inputs": _MAX_COUNT,
}
_COUNTERS = (
    "processed_inputs",
    "late_drops",
    "gap_inputs",
    "membership_updates",
    "created_windows",
    "emitted_windows",
    "finalized_memberships",
)
_CELL_FIELDS = {"key", "index", "start", "end", "input_count", "state"}
WindowPhase = Literal["open", "draining", "closed"]
WindowOutcome = Literal["folded", "late_dropped", "gap"]


def _tick(value: Any, label: str) -> int:
    return _count(value, label, -_MAX_COUNT, _MAX_COUNT)


def _name(value: Any, label: str) -> str:
    if type(value) is not str or len(value) > 1024:
        raise ValidationError(f"{label} must be a bounded canonical string")
    return _key(value, label)


def _shape(value: Any, fields: set[str]) -> None:
    if (
        type(value) is not dict
        or len(value) != len(fields)
        or any(type(key) is not str for key in value)
        or set(value) != fields
    ):
        raise ValidationError("invalid window document fields")


def _wire(value: Any, maximum: int = _MAX_WIRE) -> str:
    return _encode(value, max_bytes=maximum, label="window fold")


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hex(value: Any) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValidationError("window digest must be lowercase SHA-256 hex")


def _text(value: Any, maximum: int) -> str:
    if type(value) is not str or len(value) > maximum:
        raise ValidationError("window JSON text exceeds its byte bound")
    try:
        if len(value.encode("utf-8")) > maximum:
            raise ValidationError("window JSON text exceeds its UTF-8 byte bound")
    except UnicodeError as exc:
        raise ValidationError("window JSON requires Unicode scalar values") from exc
    return value


def _integer(token: str) -> int:
    # Reject large tokens before int() can allocate their arbitrary-precision value.
    if len(token) > 17:
        raise ValidationError("window JSON integer exceeds the interoperable range")
    return _tick(int(token), "JSON integer")


def _load(text: str) -> Any:
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
            parse_int=_integer,
        )
    except (ValueError, RecursionError) as exc:
        raise ValidationError("invalid window JSON") from exc


def _checked_value(encoded: Any, maximum: int) -> str:
    text = _text(encoded, maximum)
    if _snapshot(_load(text), maximum) != text:
        raise ValidationError("window state requires canonical JSON")
    return text


@dataclass(frozen=True, slots=True)
class WindowFoldLimits:
    """Lowerable wire/work limits, not callback-time or process-RSS quotas."""

    max_windows_per_input: int = 64
    max_keys: int = 10_000
    max_windows: int = 10_000
    max_input_bytes: int = 1024 * 1024
    max_state_value_bytes: int = 1024 * 1024
    max_state_bytes: int = 16 * 1024 * 1024
    max_row_bytes: int = 1024 * 1024
    max_rows_per_batch: int = 1000
    max_batch_bytes: int = 16 * 1024 * 1024
    max_inputs: int = 1_000_000_000

    def __post_init__(self) -> None:
        for name, ceiling in _CEILINGS.items():
            _count(getattr(self, name), name, 1, ceiling)
        if self.max_keys > self.max_windows:
            raise ValidationError("window key limit cannot exceed window limit")
        if self.max_row_bytes > self.max_batch_bytes:
            raise ValidationError("one window row must fit a drain batch")

    def to_dict(self) -> dict[str, int]:
        self.__post_init__()
        return {name: getattr(self, name) for name in _CEILINGS}


@dataclass(frozen=True, slots=True)
class WindowFold:
    """One original arrival-ordered fixed-window fold; callbacks are revisioned."""

    fold_id: str
    revision: str
    width: int = field(kw_only=True)
    initial: Callable[[], Any] = field(kw_only=True, repr=False)
    fold: Callable[[Any, Any], Any] = field(kw_only=True, repr=False)
    hop: int | None = field(default=None, kw_only=True)
    origin: int = field(default=0, kw_only=True)
    tick_unit: str = field(default="tick", kw_only=True)
    finalize: Callable[[Any], Any] | None = field(default=None, kw_only=True, repr=False)
    late_policy: Literal["reject", "drop"] = field(default="reject", kw_only=True)
    limits: WindowFoldLimits = field(default_factory=WindowFoldLimits, kw_only=True)

    def __post_init__(self) -> None:
        _name(self.fold_id, "fold_id")
        _name(self.revision, "fold revision")
        _name(self.tick_unit, "tick unit")
        _count(self.width, "window width", 1, _MAX_COUNT)
        if self.hop is None:
            object.__setattr__(self, "hop", self.width)
        _count(self.hop, "window hop", 1, _MAX_COUNT)
        _tick(self.origin, "window origin")
        if type(self.late_policy) is not str or self.late_policy not in ("reject", "drop"):
            raise ValidationError("window late policy must be reject or drop")
        if type(self.limits) is not WindowFoldLimits:
            raise ValidationError("window limits must be WindowFoldLimits")
        self.limits.__post_init__()
        _sync(self.initial, "window initializer")
        _sync(self.fold, "window folder")
        if self.finalize is not None:
            _sync(self.finalize, "window finalizer")

    def _configuration(self) -> dict[str, Any]:
        self.__post_init__()
        return {
            "fold_id": self.fold_id,
            "revision": self.revision,
            "width": self.width,
            "hop": self.hop,
            "origin": self.origin,
            "tick_unit": self.tick_unit,
            "order": "arrival",
            "late_policy": self.late_policy,
            "finalizer": self.finalize is not None,
            "limits": self.limits.to_dict(),
        }

    @property
    def identity(self) -> str:
        return _digest(_wire(self._configuration()))


class WindowFoldExecutionError(ValidationError):
    """A trusted callback failed before publication of this complete operation."""

    def __init__(self, phase: str, key: str, index: int) -> None:
        self.phase, self.key, self.index = phase, key, index
        super().__init__(f"window {index} key {key!r} {phase} callback failed")


@dataclass(frozen=True, slots=True)
class WindowStatus:
    phase: WindowPhase
    watermark: int | None
    finished: bool
    retained_windows: int
    pending_windows: int

    def __post_init__(self) -> None:
        if self.watermark is not None:
            _tick(self.watermark, "watermark")
        if type(self.finished) is not bool:
            raise ValidationError("window finished flag must be boolean")
        _count(self.retained_windows, "retained windows", 0, _CEILINGS["max_windows"])
        _count(self.pending_windows, "pending windows", 0, self.retained_windows)
        expected = "draining" if self.pending_windows else "closed" if self.finished else "open"
        if (
            type(self.phase) is not str
            or self.phase != expected
            or (self.finished and self.pending_windows != self.retained_windows)
        ):
            raise ValidationError("window phase contradicts retained eligibility/EOF")

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class WindowProcessResult:
    outcome: WindowOutcome
    memberships: int
    status: WindowStatus

    def __post_init__(self) -> None:
        if type(self.outcome) is not str or self.outcome not in ("folded", "late_dropped", "gap"):
            raise ValidationError("invalid window input outcome")
        _count(self.memberships, "input memberships", 0, _CEILINGS["max_windows_per_input"])
        if (self.outcome == "folded") != bool(self.memberships):
            raise ValidationError("input outcome contradicts window membership")
        if type(self.status) is not WindowStatus:
            raise ValidationError("window result requires WindowStatus")
        self.status.__post_init__()


def _row_metadata(
    key: str, index: int, start: int, end: int, tick_unit: str, count: int
) -> dict[str, Any]:
    return {
        "key": key,
        "index": index,
        "start": start,
        "end": end,
        "tick_unit": tick_unit,
        "input_count": count,
    }


def _row_size(metadata: dict[str, Any], encoded: str) -> int:
    # Replace the canonical null placeholder by already-admitted JSON value text.
    return (
        len(_wire({**metadata, "value": None}).encode("utf-8")) - 4 + len(encoded.encode("utf-8"))
    )


@dataclass(frozen=True, slots=True, init=False)
class WindowRow:
    """One complete closed-window output; JSON reads return independent values."""

    key: str
    index: int
    start: int
    end: int
    tick_unit: str
    input_count: int
    _json: str = field(repr=False)

    def __init__(
        self,
        key: str,
        index: int,
        start: int,
        end: int,
        tick_unit: str,
        input_count: int,
        value: Any,
    ) -> None:
        _name(key, "window row key")
        _name(tick_unit, "tick unit")
        for label, tick in (("index", index), ("start", start), ("end", end)):
            _tick(tick, label)
        if start >= end:
            raise ValidationError("window row start must precede end")
        _count(input_count, "window input count", 1, _MAX_COUNT)
        encoded = _snapshot(value, _CEILINGS["max_row_bytes"])
        metadata = _row_metadata(key, index, start, end, tick_unit, input_count)
        if _row_size(metadata, encoded) > _CEILINGS["max_row_bytes"]:
            raise ValidationError("window row exceeds the hard byte limit")
        for name, item in metadata.items():
            object.__setattr__(self, name, item)
        object.__setattr__(self, "_json", encoded)

    @property
    def value(self) -> Any:
        return json.loads(self._json)

    @property
    def byte_size(self) -> int:
        return _row_size(self._metadata(), self._json)

    def _metadata(self) -> dict[str, Any]:
        return _row_metadata(
            self.key, self.index, self.start, self.end, self.tick_unit, self.input_count
        )

    def to_dict(self) -> dict[str, Any]:
        return {**self._metadata(), "value": self.value}

    def to_record(self) -> FlowRecord:
        document = self.to_dict()
        document.pop("key")
        return FlowRecord(document, self.key)


@dataclass(frozen=True, slots=True)
class WindowBatch:
    """One atomic drain result, not durable delivery acknowledgement."""

    rows: tuple[WindowRow, ...]
    status: WindowStatus

    def __post_init__(self) -> None:
        if type(self.rows) is not tuple or len(self.rows) > _CEILINGS["max_rows_per_batch"]:
            raise ValidationError("window batch requires bounded tuple rows")
        if any(type(row) is not WindowRow for row in self.rows):
            raise ValidationError("window batch rows must be WindowRow")
        if type(self.status) is not WindowStatus:
            raise ValidationError("window batch requires WindowStatus")
        self.status.__post_init__()
        if sum(row.byte_size for row in self.rows) > _CEILINGS["max_batch_bytes"]:
            raise ValidationError("window batch exceeds the hard byte limit")

    @property
    def drained_windows(self) -> int:
        return len(self.rows)


@dataclass(frozen=True, slots=True)
class _Cell:
    key: str
    index: int
    start: int
    end: int
    input_count: int
    encoded: str
    byte_size: int

    def wire(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "index": self.index,
            "start": self.start,
            "end": self.end,
            "input_count": self.input_count,
            "state": self.encoded,
        }


@dataclass(frozen=True, slots=True)
class _State:
    cells: dict[tuple[int, str], _Cell] = field(default_factory=dict)
    watermark: int | None = None
    finished: bool = False
    processed_inputs: int = 0
    late_drops: int = 0
    gap_inputs: int = 0
    membership_updates: int = 0
    created_windows: int = 0
    emitted_windows: int = 0
    finalized_memberships: int = 0
    byte_size: int = 0


def _eligible(state: _State, cell: _Cell) -> bool:
    return state.finished or (state.watermark is not None and cell.end <= state.watermark)


def _status(state: _State) -> WindowStatus:
    pending = sum(_eligible(state, cell) for cell in state.cells.values())
    phase: WindowPhase = "draining" if pending else "closed" if state.finished else "open"
    return WindowStatus(phase, state.watermark, state.finished, len(state.cells), pending)


def _geometry(spec: WindowFold, index: int) -> tuple[int, int]:
    _tick(index, "window index")
    start = spec.origin + index * cast(int, spec.hop)
    return _tick(start, "window start"), _tick(start + spec.width, "window end")


def _cell(spec: WindowFold, key: str, index: int, count: int, encoded: str) -> _Cell:
    start, end = _geometry(spec, index)
    _count(count, "window input count", 1, _MAX_COUNT)
    cell = _Cell(key, index, start, end, count, encoded, 0)
    size = len(_wire(cell.wire(), spec.limits.max_state_bytes).encode("utf-8"))
    if spec.finalize is None:
        metadata = _row_metadata(key, index, start, end, spec.tick_unit, count)
        if _row_size(metadata, encoded) > spec.limits.max_row_bytes:
            raise ValidationError("identity-finalized state cannot fit one output row")
    return replace(cell, byte_size=size)


def _invoke(function: Callable[..., Any], phase: str, key: str, index: int, *args: Any) -> Any:
    try:
        result = function(*args)
        _no_awaitable(result)
        return result
    except Exception as exc:
        raise WindowFoldExecutionError(phase, key, index) from exc


class WindowFoldRuntime:
    """Single-owner explicit progress; each public operation publishes once."""

    def __init__(self, spec: WindowFold) -> None:
        if type(spec) is not WindowFold:
            raise ValidationError("window runtime requires WindowFold")
        spec.__post_init__()
        self._spec = spec
        self._state = _State()
        self._busy = False

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if self._busy:
            raise ValidationError("window runtime does not allow reentrant operations")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    @property
    def spec(self) -> WindowFold:
        return self._spec

    @property
    def status(self) -> WindowStatus:
        return _status(self._state)

    def process(self, timestamp: int, record: FlowRecord) -> WindowProcessResult:
        with self._operation():
            before, spec = self._state, self.spec
            if _status(before).phase != "open":
                raise ValidationError("window input requires open, non-draining state")
            _tick(timestamp, "input timestamp")
            if type(record) is not FlowRecord or record.key is None:
                raise ValidationError("window inputs require keyed FlowRecord")
            key = _name(record.key, "input key")
            encoded = _checked_value(record._json, spec.limits.max_input_bytes)
            late = before.watermark is not None and timestamp < before.watermark
            if late and spec.late_policy == "reject":
                raise ValidationError("window input precedes the watermark")
            processed = _count(
                before.processed_inputs + 1, "processed inputs", 0, spec.limits.max_inputs
            )
            if late:
                after = replace(
                    before, processed_inputs=processed, late_drops=before.late_drops + 1
                )
                result = WindowProcessResult("late_dropped", 0, _status(after))
            else:
                after, result = self._fold(before, timestamp, key, encoded, processed)
            self._state = after
            return result

    def _fold(
        self, before: _State, timestamp: int, key: str, encoded: str, processed: int
    ) -> tuple[_State, WindowProcessResult]:
        spec, limits = self.spec, self.spec.limits
        hop = cast(int, spec.hop)
        first = (timestamp - spec.origin - spec.width) // hop + 1
        last = (timestamp - spec.origin) // hop
        count = max(0, last - first + 1)
        _count(count, "input window membership", 0, limits.max_windows_per_input)
        if not count:
            after = replace(before, processed_inputs=processed, gap_inputs=before.gap_inputs + 1)
            return after, WindowProcessResult("gap", 0, _status(after))
        for index in range(first, last + 1):
            _geometry(spec, index)
        identities = tuple((index, key) for index in range(first, last + 1))
        new = sum(identity not in before.cells for identity in identities)
        _count(len(before.cells) + new, "retained windows", 0, limits.max_windows)
        keys = {cell.key for cell in before.cells.values()}
        _count(len(keys) + (key not in keys), "retained window keys", 0, limits.max_keys)
        updates = _count(before.membership_updates + count, "membership updates", 0, _MAX_COUNT)
        created = _count(before.created_windows + new, "created windows", 0, _MAX_COUNT)
        for identity in identities:
            previous = before.cells.get(identity)
            _count(
                (previous.input_count if previous else 0) + 1, "window input count", 1, _MAX_COUNT
            )
        cells = dict(before.cells)
        # Remove all touched old costs first; a later shrinking sibling must not
        # make an otherwise-admissible final transaction fail a prefix budget.
        size = before.byte_size - sum(
            before.cells[identity].byte_size for identity in identities if identity in before.cells
        )
        for index, _key_value in identities:
            previous = before.cells.get((index, key))
            if previous is None:
                initial = _invoke(spec.initial, "initial", key, index)
                state = json.loads(_snapshot(initial, limits.max_state_value_bytes))
            else:
                state = json.loads(previous.encoded)
            value = _invoke(spec.fold, "fold", key, index, state, json.loads(encoded))
            saved = _snapshot(value, limits.max_state_value_bytes)
            proposed = _cell(spec, key, index, (previous.input_count if previous else 0) + 1, saved)
            size += proposed.byte_size
            if size > limits.max_state_bytes:
                raise ValidationError("window aggregate cell-wire byte limit exceeded")
            cells[index, key] = proposed
        after = replace(
            before,
            cells=cells,
            processed_inputs=processed,
            membership_updates=updates,
            created_windows=created,
            byte_size=size,
        )
        return after, WindowProcessResult("folded", count, _status(after))

    def advance_watermark(self, timestamp: int) -> WindowStatus:
        with self._operation():
            _tick(timestamp, "watermark")
            before = self._state
            if _status(before).phase != "open":
                raise ValidationError("watermark advancement requires open state")
            if before.watermark is not None and timestamp < before.watermark:
                raise ValidationError("watermark must not regress")
            after = replace(before, watermark=timestamp)
            result = _status(after)
            self._state = after
            return result

    def finish(self) -> WindowStatus:
        with self._operation():
            after = replace(self._state, finished=True)
            result = _status(after)
            self._state = after
            return result

    def drain(self, *, max_windows: int = 100) -> WindowBatch:
        with self._operation():
            _count(max_windows, "drain max_windows", 1, _CEILINGS["max_windows"])
            before, limits = self._state, self.spec.limits
            cap = min(
                max_windows,
                limits.max_rows_per_batch,
                limits.max_batch_bytes // limits.max_row_bytes,
            )
            selected = sorted(
                identity for identity, cell in before.cells.items() if _eligible(before, cell)
            )[:cap]
            emitted = _count(
                before.emitted_windows + len(selected), "emitted windows", 0, _MAX_COUNT
            )
            consumed = sum(before.cells[identity].input_count for identity in selected)
            finalized = _count(
                before.finalized_memberships + consumed, "finalized memberships", 0, _MAX_COUNT
            )
            rows: list[WindowRow] = []
            for identity in selected:
                cell = before.cells[identity]
                value = json.loads(cell.encoded)
                if self.spec.finalize is not None:
                    value = _invoke(self.spec.finalize, "finalize", cell.key, cell.index, value)
                row = WindowRow(
                    cell.key,
                    cell.index,
                    cell.start,
                    cell.end,
                    self.spec.tick_unit,
                    cell.input_count,
                    value,
                )
                if row.byte_size > limits.max_row_bytes:
                    raise ValidationError("window finalizer output exceeds row byte limit")
                rows.append(row)
            if sum(row.byte_size for row in rows) > limits.max_batch_bytes:
                raise ValidationError("window drain exceeds aggregate row byte limit")
            cells = dict(before.cells)
            size = before.byte_size
            for identity in selected:
                size -= cells.pop(identity).byte_size
            after = replace(
                before,
                cells=cells,
                byte_size=size,
                emitted_windows=emitted,
                finalized_memberships=finalized,
            )
            result = WindowBatch(tuple(rows), _status(after))
            self._state = after
            return result

    def checkpoint(self) -> WindowCheckpoint:
        with self._operation():
            body = _body(self.spec, self._state)
            return WindowCheckpoint({"body": body, "sha256": _digest(_wire(body))})

    @classmethod
    def from_checkpoint(cls, spec: WindowFold, checkpoint: WindowCheckpoint) -> WindowFoldRuntime:
        runtime = cls(spec)
        if type(checkpoint) is not WindowCheckpoint:
            raise ValidationError("window restore requires WindowCheckpoint")
        _, state, _ = _validate_document(checkpoint.to_dict(), spec)
        runtime._state = state
        return runtime


def _unavailable(*args: Any) -> Any:
    raise AssertionError("checkpoint validation must not execute callbacks")


def _read_spec(document: Any) -> WindowFold:
    _shape(
        document,
        {
            "fold_id",
            "revision",
            "width",
            "hop",
            "origin",
            "tick_unit",
            "order",
            "late_policy",
            "finalizer",
            "limits",
        },
    )
    if (
        type(document["order"]) is not str
        or document["order"] != "arrival"
        or type(document["finalizer"]) is not bool
    ):
        raise ValidationError("invalid window fold ordering/finalizer mode")
    if document["hop"] is None:
        raise ValidationError("checkpoint hop must be normalized")
    _shape(document["limits"], set(_CEILINGS))
    return WindowFold(
        document["fold_id"],
        document["revision"],
        width=document["width"],
        hop=document["hop"],
        origin=document["origin"],
        tick_unit=document["tick_unit"],
        initial=_unavailable,
        fold=_unavailable,
        finalize=_unavailable if document["finalizer"] else None,
        late_policy=document["late_policy"],
        limits=WindowFoldLimits(**document["limits"]),
    )


def _body(spec: WindowFold, state: _State) -> dict[str, Any]:
    return {
        "kind": "stream-quilt-window-checkpoint",
        "version": "1.0",
        "configuration": spec._configuration(),
        "identity": spec.identity,
        "watermark": state.watermark,
        "finished": state.finished,
        "phase": _status(state).phase,
        "counters": {name: getattr(state, name) for name in _COUNTERS},
        "cells": [state.cells[identity].wire() for identity in sorted(state.cells)],
    }


def _validate_document(
    document: Any, supplied: WindowFold | None = None
) -> tuple[WindowFold, _State, str]:
    _shape(document, {"body", "sha256"})
    _hex(document["sha256"])
    body = document["body"]
    _shape(
        body,
        {
            "kind",
            "version",
            "configuration",
            "identity",
            "watermark",
            "finished",
            "phase",
            "counters",
            "cells",
        },
    )
    if (
        type(body["kind"]) is not str
        or type(body["version"]) is not str
        or body["kind"] != "stream-quilt-window-checkpoint"
        or body["version"] != "1.0"
    ):
        raise ValidationError("unsupported window checkpoint kind/version")
    spec = _read_spec(body["configuration"])
    _hex(body["identity"])
    if body["identity"] != spec.identity:
        raise ValidationError("window checkpoint configuration digest mismatch")
    if supplied is not None:
        if supplied.identity != spec.identity:
            raise ValidationError("window configuration/revision identity mismatch")
        spec = supplied
    if body["watermark"] is not None:
        _tick(body["watermark"], "checkpoint watermark")
    if type(body["finished"]) is not bool:
        raise ValidationError("window EOF must be boolean")
    _shape(body["counters"], set(_COUNTERS))
    for name, value in body["counters"].items():
        _count(value, name, 0, _MAX_COUNT)
    raw_cells = body["cells"]
    if type(raw_cells) is not list or len(raw_cells) > spec.limits.max_windows:
        raise ValidationError("window checkpoint requires bounded cell array")
    previous: tuple[int, str] | None = None
    total = 0
    keys: set[str] = set()
    cells: dict[tuple[int, str], _Cell] = {}
    # Admit every cell's metadata/text/aggregate wire before parsing any state.
    for raw in raw_cells:
        _shape(raw, _CELL_FIELDS)
        _name(raw["key"], "checkpoint key")
        start, end = _geometry(spec, raw["index"])
        _tick(raw["start"], "checkpoint start")
        _tick(raw["end"], "checkpoint end")
        if (raw["start"], raw["end"]) != (start, end):
            raise ValidationError("window checkpoint redundant geometry mismatch")
        identity = raw["index"], raw["key"]
        if previous is not None and identity <= previous:
            raise ValidationError("window cells require unique sorted identities")
        previous = identity
        _count(raw["input_count"], "retained input count", 1, _MAX_COUNT)
        _text(raw["state"], spec.limits.max_state_value_bytes)
        size = len(_wire(raw, spec.limits.max_state_bytes - total).encode("utf-8"))
        total += size
        keys.add(raw["key"])
        if len(keys) > spec.limits.max_keys:
            raise ValidationError("window checkpoint key capacity exceeded")
        cells[identity] = _Cell(
            raw["key"], raw["index"], start, end, raw["input_count"], raw["state"], size
        )
    encoded_body = _wire(body)
    if document["sha256"] != _digest(encoded_body):
        raise ValidationError("window checkpoint checksum mismatch")
    state = _State(
        cells=cells,
        watermark=body["watermark"],
        finished=body["finished"],
        byte_size=total,
        **body["counters"],
    )
    _check_counters(spec, state)
    if type(body["phase"]) is not str or body["phase"] != _status(state).phase:
        raise ValidationError("window checkpoint phase contradicts eligibility/EOF")
    for identity, cell in cells.items():
        encoded = _checked_value(cell.encoded, spec.limits.max_state_value_bytes)
        cells[identity] = _cell(spec, cell.key, cell.index, cell.input_count, encoded)
    return spec, state, _wire(document)


def _check_counters(spec: WindowFold, state: _State) -> None:
    p, late, gaps = state.processed_inputs, state.late_drops, state.gap_inputs
    if p > spec.limits.max_inputs or late + gaps > p:
        raise ValidationError("window input counters are inconsistent")
    if (spec.late_policy == "reject" and late) or (spec.width >= cast(int, spec.hop) and gaps):
        raise ValidationError("window mode contradicts drop/gap counters")
    folded = p - late - gaps
    quotient, remainder = divmod(spec.width, cast(int, spec.hop))
    low = max(1, quotient)
    high = min(spec.limits.max_windows_per_input, max(1, quotient + bool(remainder)))
    updates, created = state.membership_updates, state.created_windows
    emitted, finalized = state.emitted_windows, state.finalized_memberships
    if not folded * low <= updates <= folded * high:
        raise ValidationError("window membership counters contradict geometry/fanout")
    if (
        created != len(state.cells) + emitted
        or created > updates
        or (created == 0) != (updates == 0)
    ):
        raise ValidationError("window creation/emission counters are inconsistent")
    retained = sum(cell.input_count for cell in state.cells.values())
    if (
        updates != retained + finalized
        or not emitted <= finalized <= emitted * folded
        or any(cell.input_count > folded for cell in state.cells.values())
    ):
        raise ValidationError("window contribution/finalization counters are inconsistent")


@dataclass(frozen=True, slots=True, init=False)
class WindowCheckpoint:
    """Strict owned canonical wire; checksums are integrity, not authentication."""

    _json: str = field(repr=False)

    def __init__(self, document: Any) -> None:
        _, _, encoded = _validate_document(document)
        object.__setattr__(self, "_json", encoded)

    def to_dict(self) -> dict[str, Any]:
        text = _text(self._json, _MAX_WIRE)
        document = _load(text)
        _, _, encoded = _validate_document(document)
        if encoded != text:
            raise ValidationError("window checkpoint JSON must be canonical")
        return cast(dict[str, Any], document)

    def to_json(self) -> str:
        self.to_dict()
        return self._json

    @classmethod
    def from_dict(cls, document: Any) -> WindowCheckpoint:
        return cls(document)

    @classmethod
    def from_json(cls, payload: str | bytes) -> WindowCheckpoint:
        if type(payload) not in (str, bytes) or len(payload) > _MAX_WIRE:
            raise ValidationError("window checkpoint input exceeds its byte bound")
        try:
            text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        except UnicodeError as exc:
            raise ValidationError("window checkpoint must be UTF-8") from exc
        text = _text(text, _MAX_WIRE)
        checkpoint = cls(_load(text))
        if checkpoint._json != text:
            raise ValidationError("window checkpoint JSON must be canonical")
        return checkpoint


__all__ = [
    "WindowBatch",
    "WindowCheckpoint",
    "WindowFold",
    "WindowFoldExecutionError",
    "WindowFoldLimits",
    "WindowFoldRuntime",
    "WindowProcessResult",
    "WindowRow",
    "WindowStatus",
]
