"""Offline alignment and bounded out-of-order streaming with watermarks."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from itertools import pairwise
from typing import NamedTuple

from stream_quilt.checkpoint import AlignerCheckpoint, TrackedCheckpoint, config_digest
from stream_quilt.errors import LateEventError, ValidationError
from stream_quilt.interval_index import WindowIntervalIndex, event_horizon, overlaps
from stream_quilt.limits import MAX_EVENTS
from stream_quilt.models import (
    AlignedWindow,
    AlignmentConfig,
    AlignmentResult,
    Event,
    Gap,
    RetentionPolicy,
)

_TRACKED = 0
_DROPPED = 1
_ACCEPTED_LATE = 2


class _Tracked(NamedTuple):
    """Identity bookkeeping for one ingested event."""

    event_id: str
    horizon_ms: float
    status: int


class WatermarkAligner:
    """Incrementally close windows when every required stream has advanced.

    Event time is normalized by the configured per-stream offset before any
    watermark or window calculation. The class is deterministic and performs
    no wall-clock reads.

    ``retention`` bounds the per-event identity state a long-running aligner keeps. The
    default retains every record, which reproduces the historical behavior.
    """

    def __init__(
        self, config: AlignmentConfig, *, retention: RetentionPolicy | None = None
    ) -> None:
        self.config = _validated_config(config)
        self._index = WindowIntervalIndex(
            origin_ms=self.config.origin_ms,
            window_ms=self.config.window_ms,
            hop_ms=self.config.hop_ms,
            max_windows=self.config.max_output_windows,
        )
        self.retention = _validated_retention(self.config, retention)
        self._seen_ids: set[str] = set()
        self._seen_order: deque[_Tracked] = deque()
        self._assigned_ids: set[str] = set()
        self._max_seen: dict[str, float] = {}
        self._next_start = self.config.origin_ms
        self._next_index = 0
        self._dropped: list[str] = []
        self._accepted_late: list[str] = []
        self._unassigned: list[str] = []
        self._released = 0
        self._released_reported = 0
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

        return (
            *self._unassigned,
            *(
                record.event_id
                for record in self._seen_order
                if record.status != _DROPPED and record.event_id not in self._assigned_ids
            ),
        )

    @property
    def retained_event_count(self) -> int:
        """Event identities still held, including IDs kept only for reporting."""

        return len(self._seen_ids) + self._released_reported

    @property
    def released_event_count(self) -> int:
        """Event identities released by the retention policy."""

        return self._released

    def checkpoint(self) -> AlignerCheckpoint:
        """Return a portable snapshot that can be restored after process failure."""

        live_events = tuple(entry.event for entry in self._index._live.values())
        return AlignerCheckpoint(
            config_digest=config_digest(self.config),
            next_index=self._next_index,
            next_start=self._next_start,
            max_seen=dict(self._max_seen),
            live_events=live_events,
            seen_order=tuple(
                TrackedCheckpoint(item.event_id, item.horizon_ms, item.status)
                for item in self._seen_order
            ),
            assigned_event_ids=tuple(sorted(self._assigned_ids)),
            dropped_event_ids=tuple(self._dropped),
            accepted_late_event_ids=tuple(self._accepted_late),
            unassigned_event_ids=tuple(self._unassigned),
            released_count=self._released,
            released_reported_count=self._released_reported,
            closed=self._closed,
            retention=self.retention,
        )

    @classmethod
    def from_checkpoint(
        cls,
        config: AlignmentConfig,
        checkpoint: AlignerCheckpoint,
        *,
        retention: RetentionPolicy | None = None,
    ) -> WatermarkAligner:
        """Restore a checkpoint after verifying its configuration identity."""

        if not isinstance(checkpoint, AlignerCheckpoint):
            raise ValidationError("checkpoint must be an AlignerCheckpoint")
        # Snapshot again: callers can bypass frozen dataclasses with object.__setattr__.
        checkpoint = AlignerCheckpoint.from_dict(checkpoint.to_dict())
        restored = cls(config, retention=checkpoint.retention if retention is None else retention)
        if checkpoint.config_digest != config_digest(restored.config):
            raise ValidationError("checkpoint was created with a different alignment configuration")
        if restored.retention != checkpoint.retention:
            raise ValidationError("checkpoint was created with a different retention policy")
        if checkpoint.next_index > restored.config.max_output_windows:
            raise ValidationError("checkpoint exceeds configured window limit")
        expected_start = restored.config.origin_ms + checkpoint.next_index * restored.config.hop_ms
        if checkpoint.next_start != expected_start:
            raise ValidationError("checkpoint next_start does not match its window index")
        tracked = {item.event_id: item for item in checkpoint.seen_order}
        for event in checkpoint.live_events:
            if event_horizon(event) != tracked[event.id].horizon_ms:
                raise ValidationError(
                    "checkpoint live event horizon disagrees with identity record"
                )
            restored._index._insert_validated(event)
        restored._index.prune(checkpoint.next_index)
        restored._next_index = checkpoint.next_index
        restored._next_start = checkpoint.next_start
        restored._max_seen = dict(checkpoint.max_seen)
        restored._seen_order = deque(
            _Tracked(item.event_id, item.horizon_ms, item.status) for item in checkpoint.seen_order
        )
        restored._seen_ids = {item.event_id for item in checkpoint.seen_order}
        restored._assigned_ids = set(checkpoint.assigned_event_ids)
        restored._dropped = list(checkpoint.dropped_event_ids)
        restored._accepted_late = list(checkpoint.accepted_late_event_ids)
        restored._unassigned = list(checkpoint.unassigned_event_ids)
        restored._released = checkpoint.released_count
        restored._released_reported = checkpoint.released_reported_count
        restored._closed = checkpoint.closed
        return restored

    def ingest(self, event: Event) -> tuple[AlignedWindow, ...]:
        """Ingest one event in arrival order and return newly closed windows."""

        if self._closed:
            raise ValidationError("cannot ingest after flush")
        checked = _validated_event(event)
        if checked.id in self._seen_ids:
            raise ValidationError(f"duplicate event id {checked.id!r}")
        if len(self._seen_ids) >= MAX_EVENTS:
            raise ValidationError(f"alignment exceeds the {MAX_EVENTS}-event limit")
        if self.retained_event_count >= self.retention.max_tracked_events:
            raise ValidationError(
                "retained event identities reached max_tracked_events "
                f"({self.retention.max_tracked_events}); widen the retention policy rather "
                "than losing reported IDs"
            )
        normalized = _shift_validated_event(checked, _stream_offset(self.config, checked))
        fully_obsolete = _event_ends_at_or_before(normalized, self._next_start)
        closed_horizon = self._closed_horizon()
        late = fully_obsolete or normalized.timestamp_ms < closed_horizon
        if late:
            if self.config.late_policy == "reject":
                raise LateEventError(
                    f"event {checked.id!r} overlaps or precedes closed output ending at "
                    f"{max(closed_horizon, self._next_start):g} ms"
                )
            if self.config.late_policy == "drop" or fully_obsolete:
                self._track(checked.id, normalized, _DROPPED)
                self._dropped.append(checked.id)
                return ()
        candidate_max_seen = dict(self._max_seen)
        candidate_max_seen[checked.stream] = max(
            normalized.timestamp_ms, candidate_max_seen.get(checked.stream, -math.inf)
        )
        watermark = self._watermark_for(candidate_max_seen)
        windows = () if watermark is None else self._preview_ready(watermark, normalized)

        self._track(checked.id, normalized, _ACCEPTED_LATE if late else _TRACKED)
        if late:
            self._accepted_late.append(checked.id)
        self._index._insert_validated(normalized)
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
        if not self._index:
            self._closed = True
            return ()
        horizon = self._index.horizon()
        windows = self._preview_flush(horizon)
        self._commit_windows(windows)
        self._index.clear()
        self._closed = True
        return windows

    def _preview_flush(self, horizon: float) -> tuple[AlignedWindow, ...]:
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
            windows.append(self._build_window(index, start, ()))
            index += 1
            start = self.config.origin_ms + index * self.config.hop_ms
        return tuple(windows)

    def _preview_ready(self, watermark: float, pending: Event) -> tuple[AlignedWindow, ...]:
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
            windows.append(self._build_window(index, start, (pending,)))
            index += 1
            start = self.config.origin_ms + index * self.config.hop_ms
        return tuple(windows)

    def _build_window(self, index: int, start: float, pending: tuple[Event, ...]) -> AlignedWindow:
        end = start + self.config.window_ms
        # The index holds committed events only; the arrival still being previewed is
        # merged in here so that a rejected preview leaves no trace in the index.
        candidates = [
            # ``AlignedWindow`` snapshots every nested event as it validates the
            # result. Avoid taking a second public-query snapshot on this hot path.
            *self._index._matching(index),
            *(event for event in pending if overlaps(event, start, end)),
        ]
        events = tuple(
            sorted(candidates, key=lambda event: (event.timestamp_ms, event.stream, event.id))
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
        # ``horizon <= _next_start`` and "last covered window index < _next_index" are the
        # same predicate, so the index expires exactly what the buffer scan used to drop.
        self._index.prune(self._next_index)
        self._release()

    def _track(self, event_id: str, normalized: Event, status: int) -> None:
        self._seen_ids.add(event_id)
        self._seen_order.append(_Tracked(event_id, event_horizon(normalized), status))

    def _release(self) -> None:
        """Forget identity records that the retention policy no longer requires.

        The frontier is derived from the watermark. A ready batch always stops with
        ``_next_start + window_ms > watermark``, and ``horizon_ms >= window_ms`` is
        enforced when the policy is accepted, so the frontier is strictly behind
        ``_next_start``: every released event had already expired from the interval index
        and no future window can contain it.

        Records are released in arrival order, and each one is classified before it is
        forgotten, so ``dropped_event_ids``, ``accepted_late_event_ids``, and
        ``unassigned_event_ids`` still report exactly what they would report with every
        record retained.
        """

        watermark = self.watermark_ms
        if watermark is None:
            return
        frontier = watermark - self.retention.horizon_ms
        while self._seen_order and self._seen_order[0].horizon_ms <= frontier:
            record = self._seen_order.popleft()
            self._seen_ids.discard(record.event_id)
            assigned = record.event_id in self._assigned_ids
            self._assigned_ids.discard(record.event_id)
            if record.status != _TRACKED:
                self._released_reported += 1
            elif not assigned:
                self._unassigned.append(record.event_id)
                self._released_reported += 1
            self._released += 1


def align_events(events: Iterable[Event], config: AlignmentConfig) -> AlignmentResult:
    """Align a finite collection by normalized event time.

    Sorting makes offline output independent of input arrival order. Use
    :class:`WatermarkAligner` directly to test live arrival behavior.
    """

    materialized = [_validated_event(event) for event in _bounded_events(events)]
    aligner = WatermarkAligner(config)
    effective_config = aligner.config
    ordered = sorted(
        materialized,
        key=lambda event: (
            event.timestamp_ms + _stream_offset(effective_config, event),
            event.stream,
            event.id,
        ),
    )
    windows: list[AlignedWindow] = []
    for event in ordered:
        windows.extend(aligner.ingest(event))
    windows.extend(aligner.flush())
    normalized = tuple(
        _shift_validated_event(event, _stream_offset(effective_config, event))
        for event in materialized
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

    checked_config = _validated_config(config)
    grouped: dict[str, list[Event]] = defaultdict(list)
    for event in _bounded_events(events):
        checked = _validated_event(event)
        if checked.stream in checked_config.expected_cadence_ms:
            grouped[checked.stream].append(checked)
    gaps: list[Gap] = []
    for stream in sorted(grouped):
        expected = checked_config.expected_cadence_ms[stream]
        ordered = sorted(grouped[stream], key=lambda event: (event.timestamp_ms, event.id))
        for previous, current in pairwise(ordered):
            observed = current.timestamp_ms - previous.timestamp_ms
            if observed > expected * checked_config.gap_factor:
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


def _stream_offset(config: AlignmentConfig, event: Event) -> float:
    """Return the offset added to one observed event timestamp.

    A stream without a ``clock_drifts`` entry takes exactly the constant ``offsets_ms``
    lookup the aligner has always used, so an unused drift correction cannot change a
    single floating-point result. A stream with one takes the affine correction instead;
    the two mappings are refused for the same stream, so the choice is never ambiguous.
    """

    drift = config.clock_drifts.get(event.stream)
    if drift is None:
        return config.offsets_ms.get(event.stream, 0.0)
    return drift.offset_at(event.timestamp_ms)


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


def _event_ends_at_or_before(event: Event, boundary: float) -> bool:
    return event_horizon(event) <= boundary


def _validated_retention(
    config: AlignmentConfig, retention: RetentionPolicy | None
) -> RetentionPolicy:
    """Accept only a retention policy that cannot release a still-reachable event."""

    if retention is None:
        return RetentionPolicy()
    if not isinstance(retention, RetentionPolicy):
        raise ValidationError("retention must be a RetentionPolicy")
    checked = RetentionPolicy(
        horizon_ms=retention.horizon_ms,
        max_tracked_events=retention.max_tracked_events,
    )
    if checked.horizon_ms < config.window_ms:
        raise ValidationError(
            f"retention horizon_ms ({checked.horizon_ms:g}) must be at least window_ms "
            f"({config.window_ms:g}); a shorter horizon could release an event that a "
            "still-open window can legitimately include"
        )
    return checked


def _validated_config(config: AlignmentConfig) -> AlignmentConfig:
    if not isinstance(config, AlignmentConfig):
        raise ValidationError("config must be an AlignmentConfig")
    return AlignmentConfig(
        window_ms=config.window_ms,
        hop_ms=config.hop_ms,
        allowed_lateness_ms=config.allowed_lateness_ms,
        origin_ms=config.origin_ms,
        required_streams=config.required_streams,
        offsets_ms=config.offsets_ms,
        clock_drifts=config.clock_drifts,
        expected_cadence_ms=config.expected_cadence_ms,
        gap_factor=config.gap_factor,
        late_policy=config.late_policy,
        max_events_per_window=config.max_events_per_window,
        max_output_windows=config.max_output_windows,
    )


def _validate_config(config: AlignmentConfig) -> None:
    """Compatibility validator used by the strict JSON adapter."""

    _validated_config(config)


def _validated_event(event: Event) -> Event:
    if not isinstance(event, Event):
        raise ValidationError("event must be an Event")
    return Event(
        id=event.id,
        stream=event.stream,
        modality=event.modality,
        timestamp_ms=event.timestamp_ms,
        duration_ms=event.duration_ms,
        data=event.data,
    )


def _shift_validated_event(event: Event, offset_ms: float) -> Event:
    """Shift an internal event that was snapshotted at the current boundary."""

    return Event(
        id=event.id,
        stream=event.stream,
        modality=event.modality,
        timestamp_ms=event.timestamp_ms + offset_ms,
        duration_ms=event.duration_ms,
        data=event.data,
    )
