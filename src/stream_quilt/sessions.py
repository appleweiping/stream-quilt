"""Event-time session windows for sparse multimodal streams.

Fixed windows are ideal for synchronized model input.  Session windows are
useful for bursty streams such as speech or user interactions: adjacent events
belong to one session while the gap from the previous event's end stays within
an explicit threshold.  The operation is offline and deterministic; it does
not read wall-clock time or mutate input events.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from stream_quilt.errors import ValidationError
from stream_quilt.models import Event


@dataclass(frozen=True, slots=True)
class SessionWindow:
    """One stream-local half-open session interval."""

    index: int
    stream: str
    start_ms: float
    end_ms: float
    event_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "stream": self.stream,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "event_ids": list(self.event_ids),
            "event_count": len(self.event_ids),
        }


@dataclass(frozen=True, slots=True)
class SessionResult:
    """Stable collection of session windows and input accounting."""

    gap_ms: float
    input_event_count: int
    streams: tuple[str, ...]
    windows: tuple[SessionWindow, ...]

    @property
    def session_count(self) -> int:
        return len(self.windows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "kind": "stream-quilt-sessions",
            "gap_ms": self.gap_ms,
            "input_event_count": self.input_event_count,
            "streams": list(self.streams),
            "session_count": self.session_count,
            "windows": [window.to_dict() for window in self.windows],
        }


def sessionize(events: Iterable[Event], *, gap_ms: float) -> SessionResult:
    """Group each stream into sessions separated by more than ``gap_ms``.

    The gap is measured from the previous event's exclusive end to the next
    event's start, so overlapping events naturally stay in one session.  Zero
    duration events use their representable next instant, matching the rest of
    Stream Quilt's interval semantics.
    """

    if isinstance(gap_ms, bool) or not isinstance(gap_ms, (int, float)):
        raise ValidationError("gap_ms must be a finite non-negative number")
    gap = float(gap_ms)
    if not math.isfinite(gap) or gap < 0:
        raise ValidationError("gap_ms must be a finite non-negative number")
    supplied = list(events)
    if any(not isinstance(event, Event) for event in supplied):
        raise ValidationError("events must contain Event records")
    if len({event.id for event in supplied}) != len(supplied):
        raise ValidationError("events must have unique IDs")
    grouped: dict[str, list[Event]] = defaultdict(list)
    for event in supplied:
        grouped[event.stream].append(event)
    windows: list[SessionWindow] = []
    next_index = 0
    for stream in sorted(grouped):
        ordered = sorted(grouped[stream], key=lambda event: (event.timestamp_ms, event.id))
        current_ids: list[str] = []
        start = end = 0.0
        for event in ordered:
            event_end = (
                event.end_ms if event.duration_ms else math.nextafter(event.timestamp_ms, math.inf)
            )
            if not current_ids:
                start, end = event.timestamp_ms, event_end
                current_ids = [event.id]
                continue
            if event.timestamp_ms - end > gap:
                windows.append(SessionWindow(next_index, stream, start, end, tuple(current_ids)))
                next_index += 1
                start, end = event.timestamp_ms, event_end
                current_ids = [event.id]
            else:
                end = max(end, event_end)
                current_ids.append(event.id)
        if current_ids:
            windows.append(SessionWindow(next_index, stream, start, end, tuple(current_ids)))
            next_index += 1
    return SessionResult(
        gap_ms=gap,
        input_event_count=len(supplied),
        streams=tuple(sorted(grouped)),
        windows=tuple(windows),
    )


__all__ = ["SessionResult", "SessionWindow", "sessionize"]
