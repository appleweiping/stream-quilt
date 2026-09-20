"""Bounded timestamp-ordered local window folds with explicit source progress.

The release boundary is strictly ``timestamp < watermark``.  This module
does not provide a source offset transaction or external output delivery.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, cast

from .dataflow import _MAX_COUNT, FlowRecord, _count
from .errors import ValidationError
from .window_fold import (
    WindowBatch,
    WindowCheckpoint,
    WindowFold,
    WindowOutcome,
    WindowStatus,
    _body,
    _checked_value,
    _digest,
    _hex,
    _load,
    _shape,
    _stage_advance,
    _stage_drain,
    _stage_finish,
    _stage_process,
    _State,
    _status,
    _text,
    _tick,
    _validate_document,
    _wire,
)

_MAX_BUFFER_RECORDS = 1000
_MAX_BUFFER_BYTES = 16 * 1024 * 1024
_MAX_CHECKPOINT_BYTES = 96 * 1024 * 1024
_KIND = "stream-quilt-ordered-window-checkpoint"


@dataclass(frozen=True, slots=True)
class OrderedWindowLimits:
    max_buffer_records: int = _MAX_BUFFER_RECORDS
    max_buffer_bytes: int = _MAX_BUFFER_BYTES

    def __post_init__(self) -> None:
        _count(self.max_buffer_records, "buffer record limit", 1, _MAX_BUFFER_RECORDS)
        _count(self.max_buffer_bytes, "buffer byte limit", 1, _MAX_BUFFER_BYTES)

    def to_dict(self) -> dict[str, int]:
        self.__post_init__()
        return {
            "max_buffer_records": self.max_buffer_records,
            "max_buffer_bytes": self.max_buffer_bytes,
        }


@dataclass(frozen=True, slots=True)
class OrderedWindowStatus:
    next_position: int
    buffered_records: int
    buffered_bytes: int
    window: WindowStatus

    def __post_init__(self) -> None:
        _count(self.next_position, "next source position", 0, _MAX_COUNT)
        _count(self.buffered_records, "buffered records", 0, _MAX_BUFFER_RECORDS)
        _count(self.buffered_bytes, "buffered bytes", 0, _MAX_BUFFER_BYTES)
        if type(self.window) is not WindowStatus:
            raise ValidationError("ordered status requires WindowStatus")
        self.window.__post_init__()


@dataclass(frozen=True, slots=True)
class OrderedWindowAdmission:
    position: int
    status: OrderedWindowStatus

    def __post_init__(self) -> None:
        _count(self.position, "admitted position", 0, _MAX_COUNT)
        if type(self.status) is not OrderedWindowStatus:
            raise ValidationError("ordered admission requires status")
        self.status.__post_init__()


@dataclass(frozen=True, slots=True)
class OrderedWindowEffect:
    position: int
    timestamp: int
    outcome: WindowOutcome
    memberships: int

    def __post_init__(self) -> None:
        _count(self.position, "released position", 0, _MAX_COUNT)
        _tick(self.timestamp, "released timestamp")
        if type(self.outcome) is not str or self.outcome not in ("folded", "gap"):
            raise ValidationError("invalid ordered release outcome")
        _count(self.memberships, "release memberships", 0, 1024)
        if (self.outcome == "folded") != bool(self.memberships):
            raise ValidationError("ordered release outcome contradicts membership")


@dataclass(frozen=True, slots=True)
class OrderedWindowRelease:
    effects: tuple[OrderedWindowEffect, ...]
    status: OrderedWindowStatus

    def __post_init__(self) -> None:
        if (
            type(self.effects) is not tuple
            or len(self.effects) > _MAX_BUFFER_RECORDS
            or any(type(item) is not OrderedWindowEffect for item in self.effects)
            or type(self.status) is not OrderedWindowStatus
        ):
            raise ValidationError("invalid ordered release")
        for effect in self.effects:
            effect.__post_init__()
        self.status.__post_init__()


@dataclass(frozen=True, slots=True)
class _Pending:
    position: int
    timestamp: int
    record: FlowRecord
    byte_size: int

    def wire(self) -> dict[str, Any]:
        return {
            "position": self.position,
            "timestamp": self.timestamp,
            "key": self.record.key,
            "value": self.record.value,
        }


def _entry_size(raw: dict[str, Any], maximum: int) -> int:
    return len(_wire(raw, maximum).encode("utf-8"))


def _status_for(
    state: _State, position: int, buffer: tuple[_Pending, ...], size: int
) -> OrderedWindowStatus:
    return OrderedWindowStatus(position, len(buffer), size, _status(state))


def _read_document(
    document: Any,
    spec: WindowFold | None = None,
    limits: OrderedWindowLimits | None = None,
) -> tuple[WindowFold, OrderedWindowLimits, _State, int, tuple[_Pending, ...], int, str]:
    _shape(document, {"body", "sha256"})
    _hex(document["sha256"])
    body = document["body"]
    _shape(
        body, {"kind", "version", "fold_identity", "limits", "next_position", "window", "buffer"}
    )
    if (
        type(body["kind"]) is not str
        or body["kind"] != _KIND
        or type(body["version"]) is not str
        or body["version"] != "1.0"
    ):
        raise ValidationError("unsupported ordered window checkpoint kind/version")
    _hex(body["fold_identity"])
    _shape(body["limits"], {"max_buffer_records", "max_buffer_bytes"})
    stored_limits = OrderedWindowLimits(**body["limits"])
    if limits is not None and limits != stored_limits:
        raise ValidationError("ordered window buffer limits mismatch")
    position = _count(body["next_position"], "next source position", 0, _MAX_COUNT)
    stored_spec, state, _ = _validate_document(body["window"], spec)
    if stored_spec.late_policy != "reject" or body["fold_identity"] != stored_spec.identity:
        raise ValidationError("ordered window fold identity or late policy mismatch")
    raw_buffer = body["buffer"]
    if type(raw_buffer) is not list or len(raw_buffer) > stored_limits.max_buffer_records:
        raise ValidationError("ordered buffer exceeds record limit")
    buffer: list[_Pending] = []
    size = 0
    previous = -1
    for raw in raw_buffer:
        _shape(raw, {"position", "timestamp", "key", "value"})
        item_position = _count(raw["position"], "buffered position", 0, _MAX_COUNT)
        if item_position <= previous or item_position >= position:
            raise ValidationError("buffer positions must be unique ordered source prefix entries")
        previous = item_position
        timestamp = _tick(raw["timestamp"], "buffered timestamp")
        if state.watermark is not None and timestamp < state.watermark:
            raise ValidationError("buffered timestamp precedes saved watermark")
        if type(raw["key"]) is not str:
            raise ValidationError("ordered buffer requires keyed records")
        record = FlowRecord(raw["value"], raw["key"])
        _checked_value(record._json, stored_spec.limits.max_input_bytes)
        item_size = _entry_size(raw, stored_limits.max_buffer_bytes - size)
        size += item_size
        buffer.append(_Pending(item_position, timestamp, record, item_size))
    if state.finished and buffer:
        raise ValidationError("finished ordered window cannot retain buffered records")
    if state.processed_inputs + len(buffer) != position:
        raise ValidationError("ordered source prefix contradicts processed input count")
    if position > stored_spec.limits.max_inputs:
        raise ValidationError("ordered source prefix exceeds fold input limit")
    encoded_body = _wire(body, _MAX_CHECKPOINT_BYTES)
    if document["sha256"] != _digest(encoded_body):
        raise ValidationError("ordered checkpoint checksum mismatch")
    encoded = _wire(document, _MAX_CHECKPOINT_BYTES)
    return stored_spec, stored_limits, state, position, tuple(buffer), size, encoded


@dataclass(frozen=True, slots=True, init=False)
class OrderedWindowCheckpoint:
    """Strict owned JSON checkpoint; checksum is not authentication."""

    _json: str = field(repr=False)

    def __init__(self, document: Any) -> None:
        *_, encoded = _read_document(document)
        object.__setattr__(self, "_json", encoded)

    def to_dict(self) -> dict[str, Any]:
        text = _text(self._json, _MAX_CHECKPOINT_BYTES)
        document = _load(text)
        *_, encoded = _read_document(document)
        if text != encoded:
            raise ValidationError("ordered checkpoint JSON must be canonical")
        return cast(dict[str, Any], document)

    def to_json(self) -> str:
        self.to_dict()
        return self._json

    @classmethod
    def from_dict(cls, document: Any) -> OrderedWindowCheckpoint:
        return cls(document)

    @classmethod
    def from_json(cls, payload: str | bytes) -> OrderedWindowCheckpoint:
        if type(payload) not in (str, bytes) or len(payload) > _MAX_CHECKPOINT_BYTES:
            raise ValidationError("ordered checkpoint input exceeds its byte bound")
        try:
            text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        except UnicodeError as exc:
            raise ValidationError("ordered checkpoint must be UTF-8") from exc
        document = cls(_load(_text(text, _MAX_CHECKPOINT_BYTES)))
        if document._json != text:
            raise ValidationError("ordered checkpoint JSON must be canonical")
        return document


class OrderedWindowFoldRuntime:
    """Single-owner timestamp-order buffer over the existing staged fold core."""

    def __init__(self, spec: WindowFold, *, limits: OrderedWindowLimits | None = None) -> None:
        if type(spec) is not WindowFold or spec.late_policy != "reject":
            raise ValidationError("ordered runtime requires reject-late WindowFold")
        spec.__post_init__()
        if limits is None:
            limits = OrderedWindowLimits()
        if type(limits) is not OrderedWindowLimits:
            raise ValidationError("ordered runtime requires OrderedWindowLimits")
        limits.__post_init__()
        self._spec = spec
        self._limits = limits
        self._state = _State()
        self._buffer: tuple[_Pending, ...] = ()
        self._buffer_bytes = 0
        self._next_position = 0
        self._busy = False

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if self._busy:
            raise ValidationError("ordered runtime does not allow reentrant operations")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    @property
    def spec(self) -> WindowFold:
        return self._spec

    @property
    def limits(self) -> OrderedWindowLimits:
        return self._limits

    @property
    def status(self) -> OrderedWindowStatus:
        return _status_for(self._state, self._next_position, self._buffer, self._buffer_bytes)

    def _check_prefix(self, position: int) -> None:
        _count(position, "source next_position", 0, _MAX_COUNT)
        if position != self._next_position:
            raise ValidationError("ordered source position does not match admitted prefix")

    def process(self, position: int, timestamp: int, record: FlowRecord) -> OrderedWindowAdmission:
        with self._operation():
            self._check_prefix(position)
            _tick(timestamp, "input timestamp")
            if _status(self._state).phase != "open":
                raise ValidationError("ordered input requires open, non-draining state")
            if self._state.watermark is not None and timestamp < self._state.watermark:
                raise ValidationError("ordered input precedes the watermark")
            if type(record) is not FlowRecord or record.key is None:
                raise ValidationError("ordered input requires keyed FlowRecord")
            encoded = _checked_value(record._json, self.spec.limits.max_input_bytes)
            owned = FlowRecord(json.loads(encoded), record.key)
            next_position = _count(position + 1, "next source position", 0, _MAX_COUNT)
            if next_position > self.spec.limits.max_inputs:
                raise ValidationError("ordered source prefix exceeds fold input limit")
            if len(self._buffer) >= self.limits.max_buffer_records:
                raise ValidationError("ordered buffer record limit exceeded")
            raw = {
                "position": position,
                "timestamp": timestamp,
                "key": owned.key,
                "value": owned.value,
            }
            size = _entry_size(raw, self.limits.max_buffer_bytes - self._buffer_bytes)
            next_bytes = self._buffer_bytes + size
            candidate = (*self._buffer, _Pending(position, timestamp, owned, size))
            status = _status_for(self._state, next_position, candidate, next_bytes)
            result = OrderedWindowAdmission(position, status)
            self._buffer, self._buffer_bytes, self._next_position = (
                candidate,
                next_bytes,
                next_position,
            )
            return result

    def _release(
        self, selected: tuple[_Pending, ...], remaining: tuple[_Pending, ...], watermark: int | None
    ) -> OrderedWindowRelease:
        candidate_state = self._state
        effects: list[OrderedWindowEffect] = []
        for item in sorted(selected, key=lambda value: (value.timestamp, value.position)):
            candidate_state, fold_result = _stage_process(
                self.spec, candidate_state, item.timestamp, item.record
            )
            effects.append(
                OrderedWindowEffect(
                    item.position, item.timestamp, fold_result.outcome, fold_result.memberships
                )
            )
        if watermark is None:
            candidate_state, _ = _stage_finish(candidate_state)
        else:
            candidate_state, _ = _stage_advance(candidate_state, watermark)
        remaining_size = sum(item.byte_size for item in remaining)
        release = OrderedWindowRelease(
            tuple(effects),
            _status_for(candidate_state, self._next_position, remaining, remaining_size),
        )
        self._state, self._buffer, self._buffer_bytes = candidate_state, remaining, remaining_size
        return release

    def advance_watermark(self, watermark: int, *, next_position: int) -> OrderedWindowRelease:
        with self._operation():
            self._check_prefix(next_position)
            _tick(watermark, "watermark")
            if _status(self._state).phase != "open":
                raise ValidationError("watermark advancement requires open state")
            if self._state.watermark is not None and watermark < self._state.watermark:
                raise ValidationError("watermark must not regress")
            selected = tuple(item for item in self._buffer if item.timestamp < watermark)
            remaining = tuple(item for item in self._buffer if item.timestamp >= watermark)
            return self._release(selected, remaining, watermark)

    def finish(self, *, next_position: int) -> OrderedWindowRelease:
        with self._operation():
            self._check_prefix(next_position)
            if self._state.finished:
                return OrderedWindowRelease((), self.status)
            if self._buffer and _status(self._state).phase != "open":
                raise ValidationError("drain eligible windows before finishing buffered inputs")
            return self._release(self._buffer, (), None)

    def drain(self, *, max_windows: int = 100) -> WindowBatch:
        with self._operation():
            state, result = _stage_drain(self.spec, self._state, max_windows=max_windows)
            self._state = state
            return result

    def checkpoint(self) -> OrderedWindowCheckpoint:
        with self._operation():
            window_body = _body(self.spec, self._state)
            window = WindowCheckpoint({"body": window_body, "sha256": _digest(_wire(window_body))})
            body = {
                "kind": _KIND,
                "version": "1.0",
                "fold_identity": self.spec.identity,
                "limits": self.limits.to_dict(),
                "next_position": self._next_position,
                "window": window.to_dict(),
                "buffer": [item.wire() for item in self._buffer],
            }
            return OrderedWindowCheckpoint(
                {"body": body, "sha256": _digest(_wire(body, _MAX_CHECKPOINT_BYTES))}
            )

    @classmethod
    def from_checkpoint(
        cls,
        spec: WindowFold,
        checkpoint: OrderedWindowCheckpoint,
        *,
        limits: OrderedWindowLimits | None = None,
    ) -> OrderedWindowFoldRuntime:
        runtime = cls(spec, limits=limits)
        if type(checkpoint) is not OrderedWindowCheckpoint:
            raise ValidationError("ordered restore requires OrderedWindowCheckpoint")
        _, _, state, position, buffer, size, _ = _read_document(
            checkpoint.to_dict(), spec, runtime.limits
        )
        runtime._state, runtime._next_position = state, position
        runtime._buffer, runtime._buffer_bytes = buffer, size
        return runtime


__all__ = [
    "OrderedWindowAdmission",
    "OrderedWindowCheckpoint",
    "OrderedWindowEffect",
    "OrderedWindowFoldRuntime",
    "OrderedWindowLimits",
    "OrderedWindowRelease",
    "OrderedWindowStatus",
]
