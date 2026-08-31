"""Immutable event, configuration, window, and diagnostic models."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

LatePolicy = Literal["reject", "drop", "accept"]


@dataclass(frozen=True, slots=True)
class Event:
    """One timestamped observation before clock normalization."""

    id: str
    stream: str
    modality: str
    timestamp_ms: float
    duration_ms: float = 0.0
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def end_ms(self) -> float:
        return self.timestamp_ms + self.duration_ms

    def shifted(self, offset_ms: float) -> Event:
        """Return an event on the shared timeline."""

        return Event(
            id=self.id,
            stream=self.stream,
            modality=self.modality,
            timestamp_ms=self.timestamp_ms + offset_ms,
            duration_ms=self.duration_ms,
            data=deepcopy(self.data),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AlignmentConfig:
    """Windowing, watermark, clock, and completeness policy."""

    window_ms: float
    hop_ms: float
    allowed_lateness_ms: float = 0.0
    origin_ms: float = 0.0
    required_streams: tuple[str, ...] = ()
    offsets_ms: dict[str, float] = field(default_factory=dict)
    expected_cadence_ms: dict[str, float] = field(default_factory=dict)
    gap_factor: float = 1.5
    late_policy: LatePolicy = "reject"
    max_events_per_window: int = 10_000
    max_output_windows: int = 10_000


@dataclass(frozen=True, slots=True)
class AlignedWindow:
    """Events overlapping one half-open timeline interval."""

    index: int
    start_ms: float
    end_ms: float
    events: tuple[Event, ...]
    missing_streams: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.missing_streams

    @property
    def modalities(self) -> tuple[str, ...]:
        return tuple(sorted({event.modality for event in self.events}))

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "complete": self.complete,
            "missing_streams": list(self.missing_streams),
            "modalities": list(self.modalities),
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True, slots=True)
class Gap:
    """A cadence violation between consecutive events on one stream."""

    stream: str
    start_ms: float
    end_ms: float
    observed_ms: float
    expected_ms: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AlignmentResult:
    """Offline alignment output with dropped-event and gap diagnostics."""

    windows: tuple[AlignedWindow, ...]
    gaps: tuple[Gap, ...]
    dropped_event_ids: tuple[str, ...] = ()
    accepted_late_event_ids: tuple[str, ...] = ()
    unassigned_event_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "windows": [window.to_dict() for window in self.windows],
            "gaps": [gap.to_dict() for gap in self.gaps],
            "dropped_event_ids": list(self.dropped_event_ids),
            "accepted_late_event_ids": list(self.accepted_late_event_ids),
            "unassigned_event_ids": list(self.unassigned_event_ids),
        }
