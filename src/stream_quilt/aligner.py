"""Offline alignment and bounded out-of-order streaming with watermarks."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import replace
from itertools import pairwise

from stream_quilt.errors import LateEventError, ValidationError
from stream_quilt.limits import MAX_EVENTS
from stream_quilt.models import AlignedWindow, AlignmentConfig, AlignmentResult, Event, Gap


class WatermarkAligner:
    """Incrementally close windows when every required stream has advanced.

    Event time is normalized by the configured per-stream offset before any
    watermark or window calculation. The class is deterministic and performs
    no wall-clock reads.
    """

    def __init__(self, config: AlignmentConfig) -> None:
        _validate_config(config)
        self.config = replace(
            config,
            required_streams=tuple(config.required_streams),
            offsets_ms=dict(config.offsets_ms),
            expected_cadence_ms=dict(config.expected_cadence_ms),
        )
        self._buffer: list[Event] = []
        self._seen_ids: set[str] = set()
        self._seen_order: list[str] = []
        self._assigned_ids: set[str] = set()
        self._max_seen: dict[str, float] = {}
        self._next_start = config.origin_ms
        self._next_index = 0
        self._dropped: list[str] = []
        self._accepted_late: list[str] = []
        self._closed = False

    @property
    def watermark_ms(self) -> float | None:
        """Current event-time watermark, or ``None`` until it is defined."""

        return self._watermark_for(self._max_seen)

    def _watermark_for(self, max_seen: Mapping[str, float]) -> float | None:
        participants = self.config.required_streams
        if not participants or any(stream not in max_seen for stream in participants):
            return None
        return min(max_seen[stream] for stream in participants) - self.config.allowed_lateness_ms

    @property
    def dropped_event_ids(self) -> tuple[str, ...]:
        return tuple(self._dropped)

    @property
    def accepted_late_event_ids(self) -> tuple[str, ...]:
        """Late events retained for still-open windows under the accept policy."""

        return tuple(self._accepted_late)

    @property
    def unassigned_event_ids(self) -> tuple[str, ...]:
        """Accepted event IDs that did not overlap any emitted window."""

        dropped = set(self._dropped)
        return tuple(
            event_id
            for event_id in self._seen_order
            if event_id not in dropped and event_id not in self._assigned_ids
        )

    def ingest(self, event: Event) -> tuple[AlignedWindow, ...]:
        """Ingest one event in arrival order and return newly closed windows."""

        if self._closed:
            raise ValidationError("cannot ingest after flush")
        _validate_event(event)
        if event.id in self._seen_ids:
            raise ValidationError(f"duplicate event id {event.id!r}")
        if len(self._seen_ids) >= MAX_EVENTS:
            raise ValidationError(f"alignment exceeds the {MAX_EVENTS}-event limit")
        normalized = event.shifted(self.config.offsets_ms.get(event.stream, 0.0))
        _validate_event(normalized)
        fully_obsolete = _event_ends_at_or_before(normalized, self._next_start)
        closed_horizon = self._closed_horizon()
        late = fully_obsolete or normalized.timestamp_ms < closed_horizon
        if late:
            if self.config.late_policy == "reject":
                raise LateEventError(
                    f"event {event.id!r} overlaps or precedes closed output ending at "
                    f"{max(closed_horizon, self._next_start):g} ms"
                )
            if self.config.late_policy == "drop" or fully_obsolete:
                self._seen_ids.add(event.id)
                self._seen_order.append(event.id)
                self._dropped.append(event.id)
                return ()
        candidate_buffer = [*self._buffer, normalized]
        candidate_max_seen = dict(self._max_seen)
        candidate_max_seen[event.stream] = max(
            normalized.timestamp_ms, candidate_max_seen.get(event.stream, -math.inf)
        )
        watermark = self._watermark_for(candidate_max_seen)
        windows = () if watermark is None else self._preview_ready(watermark, candidate_buffer)

        self._seen_ids.add(event.id)
        self._seen_order.append(event.id)
        if late:
            self._accepted_late.append(event.id)
        self._buffer = candidate_buffer
        self._max_seen = candidate_max_seen
        self._commit_windows(windows)
        return windows

    def _closed_horizon(self) -> float:
        if self._next_index == 0:
            return -math.inf
        previous_start = self.config.origin_ms + (self._next_index - 1) * self.config.hop_ms
        return previous_start + self.config.window_ms

    def flush(self) -> tuple[AlignedWindow, ...]:
        """Close the fixed window grid through the final buffered event horizon."""

        if self._closed:
            return ()
        if not self._buffer:
            self._closed = True
            return ()
        horizon = max(_event_horizon(event) for event in self._buffer)
        windows = self._preview_flush(horizon, self._buffer)
        self._commit_windows(windows)
        self._buffer.clear()
        self._closed = True
        return windows

    def _preview_flush(self, horizon: float, buffer: list[Event]) -> tuple[AlignedWindow, ...]:
        forbidden_start = (
            self.config.origin_ms + self.config.max_output_windows * self.config.hop_ms
        )
        if forbidden_start < horizon:
            raise ValidationError(
                f"alignment would exceed max_output_windows ({self.config.max_output_windows})"
            )
        windows: list[AlignedWindow] = []
        index = self._next_index
        start = self._next_start
        while start < horizon:
            windows.append(self._build_window(index, start, buffer))
            index += 1
            start = self.config.origin_ms + index * self.config.hop_ms
        return tuple(windows)

    def _preview_ready(self, watermark: float, buffer: list[Event]) -> tuple[AlignedWindow, ...]:
        forbidden_start = (
            self.config.origin_ms + self.config.max_output_windows * self.config.hop_ms
        )
        if forbidden_start + self.config.window_ms <= watermark:
            raise ValidationError(
                f"alignment would exceed max_output_windows ({self.config.max_output_windows})"
            )
        windows: list[AlignedWindow] = []
        index = self._next_index
        start = self._next_start
        while start + self.config.window_ms <= watermark:
            windows.append(self._build_window(index, start, buffer))
            index += 1
            start = self.config.origin_ms + index * self.config.hop_ms
        return tuple(windows)

    def _build_window(self, index: int, start: float, buffer: list[Event]) -> AlignedWindow:
        end = start + self.config.window_ms
        events = tuple(
            sorted(
                (event for event in buffer if _overlaps(event, start, end)),
                key=lambda event: (event.timestamp_ms, event.stream, event.id),
            )
        )
        if len(events) > self.config.max_events_per_window:
            raise ValidationError(
                f"window {index} exceeds max_events_per_window "
                f"({len(events)} > {self.config.max_events_per_window})"
            )
        present = {event.stream for event in events}
        missing = tuple(stream for stream in self.config.required_streams if stream not in present)
        return AlignedWindow(
            index=index,
            start_ms=start,
            end_ms=end,
            events=events,
            missing_streams=missing,
        )

    def _commit_windows(self, windows: tuple[AlignedWindow, ...]) -> None:
        for window in windows:
            self._assigned_ids.update(event.id for event in window.events)
        self._next_index += len(windows)
        self._next_start = self.config.origin_ms + self._next_index * self.config.hop_ms
        self._buffer = [
            event for event in self._buffer if not _event_ends_at_or_before(event, self._next_start)
        ]


def align_events(events: Iterable[Event], config: AlignmentConfig) -> AlignmentResult:
    """Align a finite collection by normalized event time.

    Sorting makes offline output independent of input arrival order. Use
    :class:`WatermarkAligner` directly to test live arrival behavior.
    """

    materialized = _bounded_events(events)
    aligner = WatermarkAligner(config)
    effective_config = aligner.config
    for event in materialized:
        _validate_event(event)
    ordered = sorted(
        materialized,
        key=lambda event: (
            event.timestamp_ms + effective_config.offsets_ms.get(event.stream, 0.0),
            event.stream,
            event.id,
        ),
    )
    windows: list[AlignedWindow] = []
    for event in ordered:
        windows.extend(aligner.ingest(event))
    windows.extend(aligner.flush())
    normalized = tuple(
        event.shifted(effective_config.offsets_ms.get(event.stream, 0.0)) for event in materialized
    )
    return AlignmentResult(
        windows=tuple(windows),
        gaps=detect_gaps(normalized, effective_config),
        dropped_event_ids=aligner.dropped_event_ids,
        accepted_late_event_ids=aligner.accepted_late_event_ids,
        unassigned_event_ids=aligner.unassigned_event_ids,
    )


def detect_gaps(events: Iterable[Event], config: AlignmentConfig) -> tuple[Gap, ...]:
    """Find start-to-start cadence gaps on configured streams."""

    _validate_config(config)
    grouped: dict[str, list[Event]] = defaultdict(list)
    for event in _bounded_events(events):
        _validate_event(event)
        if event.stream in config.expected_cadence_ms:
            grouped[event.stream].append(event)
    gaps: list[Gap] = []
    for stream in sorted(grouped):
        expected = config.expected_cadence_ms[stream]
        ordered = sorted(grouped[stream], key=lambda event: (event.timestamp_ms, event.id))
        for previous, current in pairwise(ordered):
            observed = current.timestamp_ms - previous.timestamp_ms
            if observed > expected * config.gap_factor:
                gaps.append(
                    Gap(
                        stream=stream,
                        start_ms=previous.timestamp_ms + expected,
                        end_ms=current.timestamp_ms,
                        observed_ms=observed,
                        expected_ms=expected,
                    )
                )
    return tuple(gaps)


def _bounded_events(events: Iterable[Event]) -> list[Event]:
    try:
        iterator = iter(events)
    except TypeError as exc:
        raise ValidationError("events must be iterable") from exc
    materialized: list[Event] = []
    for event in iterator:
        if len(materialized) == MAX_EVENTS:
            raise ValidationError(f"events exceeds the {MAX_EVENTS}-event limit")
        if not isinstance(event, Event):
            raise ValidationError("events must contain Event records")
        materialized.append(event)
    return materialized


def _overlaps(event: Event, start: float, end: float) -> bool:
    if event.duration_ms == 0:
        return start <= event.timestamp_ms < end
    return event.timestamp_ms < end and event.end_ms > start


def _event_horizon(event: Event) -> float:
    if event.duration_ms == 0:
        return math.nextafter(event.timestamp_ms, math.inf)
    return event.end_ms


def _event_ends_at_or_before(event: Event, boundary: float) -> bool:
    return _event_horizon(event) <= boundary


def _validate_config(config: AlignmentConfig) -> None:
    if not isinstance(config, AlignmentConfig):
        raise ValidationError("config must be an AlignmentConfig")
    AlignmentConfig(
        window_ms=config.window_ms,
        hop_ms=config.hop_ms,
        allowed_lateness_ms=config.allowed_lateness_ms,
        origin_ms=config.origin_ms,
        required_streams=config.required_streams,
        offsets_ms=config.offsets_ms,
        expected_cadence_ms=config.expected_cadence_ms,
        gap_factor=config.gap_factor,
        late_policy=config.late_policy,
        max_events_per_window=config.max_events_per_window,
        max_output_windows=config.max_output_windows,
    )


def _validate_event(event: Event) -> None:
    if not isinstance(event, Event):
        raise ValidationError("event must be an Event")
    Event(
        id=event.id,
        stream=event.stream,
        modality=event.modality,
        timestamp_ms=event.timestamp_ms,
        duration_ms=event.duration_ms,
        data=event.data,
    )
