"""Deterministic cross-stream event joins over an alignment result."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from stream_quilt.errors import ValidationError
from stream_quilt.limits import MAX_JOIN_COMPARISONS, MAX_RESULT_GAPS
from stream_quilt.models import AlignmentResult, Event


def _stream(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")
    return value.strip()


def _nonnegative(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be a finite non-negative number")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValidationError(f"{name} must be a finite non-negative number")
    return number


@dataclass(frozen=True, slots=True)
class JoinedPair:
    """One pair of events whose normalized starts are within the join tolerance."""

    left_event_id: str
    right_event_id: str
    delta_ms: float
    window_indexes: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        left = _stream(self.left_event_id, "join left_event_id")
        right = _stream(self.right_event_id, "join right_event_id")
        if left == right:
            raise ValidationError("join pair must contain two different event IDs")
        object.__setattr__(self, "left_event_id", left)
        object.__setattr__(self, "right_event_id", right)
        object.__setattr__(self, "delta_ms", _nonnegative(self.delta_ms, "join delta_ms"))
        indexes: list[int] = []
        for index in self.window_indexes:
            if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                raise ValidationError("join window_indexes must contain non-negative integers")
            if index in indexes:
                raise ValidationError("join window_indexes must not contain duplicates")
            indexes.append(index)
        object.__setattr__(self, "window_indexes", tuple(sorted(indexes)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_event_id": self.left_event_id,
            "right_event_id": self.right_event_id,
            "delta_ms": self.delta_ms,
            "window_indexes": list(self.window_indexes),
        }


@dataclass(frozen=True, slots=True)
class StreamJoin:
    """All matching pairs between two streams in one aligned result."""

    left_stream: str
    right_stream: str
    max_delta_ms: float | None
    left_event_count: int
    right_event_count: int
    comparisons: int
    pairs: tuple[JoinedPair, ...] = ()

    def __post_init__(self) -> None:
        left = _stream(self.left_stream, "join left_stream")
        right = _stream(self.right_stream, "join right_stream")
        if left == right:
            raise ValidationError("join streams must be different")
        object.__setattr__(self, "left_stream", left)
        object.__setattr__(self, "right_stream", right)
        if self.max_delta_ms is not None:
            object.__setattr__(
                self, "max_delta_ms", _nonnegative(self.max_delta_ms, "join max_delta_ms")
            )
        for name in ("left_event_count", "right_event_count", "comparisons"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValidationError(f"join {name} must be a non-negative integer")
        if self.comparisons > MAX_JOIN_COMPARISONS:
            raise ValidationError(f"join comparisons exceeds {MAX_JOIN_COMPARISONS}")
        pairs = tuple(self.pairs)
        if len(pairs) > MAX_RESULT_GAPS:
            raise ValidationError(f"join pairs exceeds {MAX_RESULT_GAPS}")
        if any(not isinstance(pair, JoinedPair) for pair in pairs):
            raise ValidationError("join pairs must contain JoinedPair objects")
        keys = {(pair.left_event_id, pair.right_event_id) for pair in pairs}
        if len(keys) != len(pairs):
            raise ValidationError("join pairs must not contain duplicate event pairs")
        object.__setattr__(
            self,
            "pairs",
            tuple(
                sorted(
                    pairs,
                    key=lambda pair: (
                        pair.delta_ms,
                        pair.left_event_id,
                        pair.right_event_id,
                    ),
                )
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_stream": self.left_stream,
            "right_stream": self.right_stream,
            "max_delta_ms": self.max_delta_ms,
            "left_event_count": self.left_event_count,
            "right_event_count": self.right_event_count,
            "comparisons": self.comparisons,
            "pair_count": len(self.pairs),
            "pairs": [pair.to_dict() for pair in self.pairs],
        }


def join_streams(
    result: AlignmentResult,
    left_stream: str,
    right_stream: str,
    *,
    max_delta_ms: float | None = None,
) -> StreamJoin:
    """Join distinct streams by normalized start time.

    Events are deduplicated by ID even when overlapping windows contain the
    same event. ``max_delta_ms`` is an inclusive absolute tolerance; ``None``
    means every cross-stream pair is returned. The explicit comparison budget
    prevents an accidental quadratic join over an unbounded result.
    """

    if not isinstance(result, AlignmentResult):
        raise ValidationError("result must be an AlignmentResult")
    left_name = _stream(left_stream, "left_stream")
    right_name = _stream(right_stream, "right_stream")
    if left_name == right_name:
        raise ValidationError("join streams must be different")
    tolerance = None if max_delta_ms is None else _nonnegative(max_delta_ms, "max_delta_ms")
    by_stream: dict[str, dict[str, Event]] = {left_name: {}, right_name: {}}
    windows_by_event: dict[str, set[int]] = {}
    for window in result.windows:
        for event in window.events:
            if event.stream in by_stream:
                by_stream[event.stream][event.id] = event
                windows_by_event.setdefault(event.id, set()).add(window.index)
    left_events = tuple(
        sorted(by_stream[left_name].values(), key=lambda event: (event.timestamp_ms, event.id))
    )
    right_events = tuple(
        sorted(by_stream[right_name].values(), key=lambda event: (event.timestamp_ms, event.id))
    )
    comparisons = len(left_events) * len(right_events)
    if comparisons > MAX_JOIN_COMPARISONS:
        raise ValidationError(
            f"join would require {comparisons} comparisons, exceeding {MAX_JOIN_COMPARISONS}"
        )
    pairs: list[JoinedPair] = []
    for left in left_events:
        for right in right_events:
            delta = abs(left.timestamp_ms - right.timestamp_ms)
            if tolerance is not None and delta > tolerance:
                continue
            pairs.append(
                JoinedPair(
                    left_event_id=left.id,
                    right_event_id=right.id,
                    delta_ms=delta,
                    window_indexes=tuple(
                        sorted(windows_by_event[left.id] & windows_by_event[right.id])
                    ),
                )
            )
    return StreamJoin(
        left_stream=left_name,
        right_stream=right_name,
        max_delta_ms=tolerance,
        left_event_count=len(left_events),
        right_event_count=len(right_events),
        comparisons=comparisons,
        pairs=tuple(pairs),
    )


__all__ = ["JoinedPair", "StreamJoin", "join_streams"]
