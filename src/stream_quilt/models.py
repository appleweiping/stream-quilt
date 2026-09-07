"""Defensively immutable event, configuration, window, and diagnostic models."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from types import MappingProxyType
from typing import Any, Literal, TypeVar

from stream_quilt.errors import ValidationError
from stream_quilt.limits import (
    MAX_DRIFT_RATE_PPM,
    MAX_EVENT_DATA_BYTES,
    MAX_EVENTS,
    MAX_EVENTS_PER_WINDOW,
    MAX_JSON_DEPTH,
    MAX_JSON_NODES,
    MAX_MAPPING_ENTRIES,
    MAX_OUTPUT_WINDOWS,
    MAX_RESULT_GAPS,
    MAX_STREAMS,
    MAX_TEXT_LENGTH,
)

LatePolicy = Literal["reject", "drop", "accept"]
_T = TypeVar("_T")
_MAX_SAFE_INTEGER = 2**53 - 1


def _label(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a non-empty string")
    text = value.strip()
    if len(text) > MAX_TEXT_LENGTH:
        raise ValidationError(f"{name} exceeds the {MAX_TEXT_LENGTH}-character limit")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValidationError(f"{name} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValidationError(f"{name} must not contain control characters")
    return text


def _finite(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be a number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValidationError(f"{name} must be finite") from exc
    if not math.isfinite(number):
        raise ValidationError(f"{name} must be finite")
    if positive and number <= 0:
        raise ValidationError(f"{name} must be greater than zero")
    if not positive and number < 0:
        raise ValidationError(f"{name} must be zero or greater")
    return number


def _any_finite(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be a number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValidationError(f"{name} must be finite") from exc
    if not math.isfinite(number):
        raise ValidationError(f"{name} must be finite")
    return number


def _positive_int(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValidationError(f"{name} must be an integer from 1 to {maximum}")
    return value


def _drift_rate_ppm(value: Any) -> float:
    number = _any_finite(value, "clock drift rate_ppm")
    if abs(number) > MAX_DRIFT_RATE_PPM:
        raise ValidationError(
            f"clock drift rate_ppm must be within +/-{MAX_DRIFT_RATE_PPM:g} ppm; "
            f"{number:g} ppm describes a clock too far from nominal to be a real rate"
        )
    return number


def _retention_horizon(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("horizon_ms must be a number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValidationError("horizon_ms must be a real number") from exc
    if math.isnan(number) or number < 0:
        raise ValidationError("horizon_ms must be zero or greater")
    return number


def _bounded_tuple(value: Any, name: str, item_type: type[_T], maximum: int) -> tuple[_T, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Iterable):
        raise ValidationError(f"{name} must be an iterable")
    result: list[_T] = []
    for item in value:
        if len(result) == maximum:
            raise ValidationError(f"{name} exceeds the {maximum}-item limit")
        if not isinstance(item, item_type):
            raise ValidationError(f"{name} entries must be {item_type.__name__} instances")
        result.append(item)
    return tuple(result)


def _label_tuple(value: Any, name: str, maximum: int) -> tuple[str, ...]:
    labels = tuple(
        _label(item, f"{name} entry") for item in _bounded_tuple(value, name, str, maximum)
    )
    if len(labels) != len(set(labels)):
        raise ValidationError(f"{name} must not contain duplicates")
    return labels


def _number_mapping(value: Any, name: str, *, positive: bool) -> Mapping[str, float]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{name} must be a mapping")
    if len(value) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"{name} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
    result: dict[str, float] = {}
    for index, (raw_key, raw_number) in enumerate(value.items()):
        if index == MAX_MAPPING_ENTRIES:
            raise ValidationError(f"{name} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
        key = _label(raw_key, f"{name} key")
        if key in result:
            raise ValidationError(f"{name} contains duplicate key {key!r}")
        result[key] = (
            _finite(raw_number, f"{name}.{key}", positive=True)
            if positive
            else _any_finite(raw_number, f"{name}.{key}")
        )
    return MappingProxyType(result)


def _drift_mapping(value: Any, name: str) -> Mapping[str, ClockDrift]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{name} must be a mapping")
    if len(value) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"{name} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
    result: dict[str, ClockDrift] = {}
    for index, (raw_key, raw_drift) in enumerate(value.items()):
        if index == MAX_MAPPING_ENTRIES:
            raise ValidationError(f"{name} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
        key = _label(raw_key, f"{name} key")
        if key in result:
            raise ValidationError(f"{name} contains duplicate key {key!r}")
        if not isinstance(raw_drift, ClockDrift):
            raise ValidationError(f"{name}.{key} must be a ClockDrift")
        result[key] = ClockDrift(
            rate_ppm=raw_drift.rate_ppm,
            offset_ms=raw_drift.offset_ms,
            epoch_ms=raw_drift.epoch_ms,
        )
    return MappingProxyType(result)


def _freeze_event_data(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError("event data must be a mapping with string keys")
    if len(value) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"event data exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
    budget = [0]
    active: set[int] = set()
    frozen = _freeze_json(value, depth=0, budget=budget, active=active, path="event data")
    if not isinstance(frozen, Mapping):  # pragma: no cover - guarded above
        raise AssertionError("event data root must remain a mapping")
    try:
        encoded = json.dumps(
            _thaw_json(frozen),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError) as exc:
        raise ValidationError("event data must contain finite JSON values") from exc
    if len(encoded) > MAX_EVENT_DATA_BYTES:
        raise ValidationError(f"event data exceeds the {MAX_EVENT_DATA_BYTES}-byte limit")
    return frozen


def _freeze_json(
    value: Any,
    *,
    depth: int,
    budget: list[int],
    active: set[int],
    path: str,
) -> Any:
    if depth > MAX_JSON_DEPTH:
        raise ValidationError(f"{path} exceeds the maximum JSON depth of {MAX_JSON_DEPTH}")
    budget[0] += 1
    if budget[0] > MAX_JSON_NODES:
        raise ValidationError(f"event data exceeds the {MAX_JSON_NODES}-value limit")
    if value is None or isinstance(value, (str, bool)):
        if isinstance(value, str):
            try:
                value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValidationError(f"{path} contains invalid Unicode") from exc
        return value
    if isinstance(value, int):
        if abs(value) > _MAX_SAFE_INTEGER:
            raise ValidationError(f"{path} integer is outside the interoperable JSON range")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError(f"{path} must contain finite JSON values")
        return value
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise ValidationError(f"{path} must not contain reference cycles")
        active.add(identity)
        try:
            result: dict[str, Any] = {}
            for index, (key, child) in enumerate(value.items()):
                if index == MAX_MAPPING_ENTRIES:
                    raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
                if not isinstance(key, str):
                    raise ValidationError(f"{path} object keys must be strings")
                if len(key) > MAX_TEXT_LENGTH:
                    raise ValidationError(
                        f"{path} key exceeds the {MAX_TEXT_LENGTH}-character limit"
                    )
                try:
                    key.encode("utf-8")
                except UnicodeEncodeError as exc:
                    raise ValidationError(f"{path} contains an invalid Unicode key") from exc
                result[key] = _freeze_json(
                    child,
                    depth=depth + 1,
                    budget=budget,
                    active=active,
                    path=f"{path}.{key}",
                )
            return MappingProxyType(result)
        finally:
            active.remove(identity)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        identity = id(value)
        if identity in active:
            raise ValidationError(f"{path} must not contain reference cycles")
        active.add(identity)
        try:
            return tuple(
                _freeze_json(
                    child,
                    depth=depth + 1,
                    budget=budget,
                    active=active,
                    path=f"{path}[{index}]",
                )
                for index, child in enumerate(value)
            )
        finally:
            active.remove(identity)
    raise ValidationError(f"{path} contains a non-JSON value of type {type(value).__name__}")


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(child) for child in value]
    return value


@dataclass(frozen=True, slots=True)
class Event:
    """One timestamped observation before clock normalization."""

    id: str
    stream: str
    modality: str
    timestamp_ms: float
    duration_ms: float = 0.0
    data: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _label(self.id, "event id"))
        object.__setattr__(self, "stream", _label(self.stream, "event stream"))
        object.__setattr__(self, "modality", _label(self.modality, "event modality"))
        object.__setattr__(
            self, "timestamp_ms", _any_finite(self.timestamp_ms, "event timestamp_ms")
        )
        object.__setattr__(self, "duration_ms", _finite(self.duration_ms, "event duration_ms"))
        if not math.isfinite(self.timestamp_ms + self.duration_ms):
            raise ValidationError("event end time must be finite")
        object.__setattr__(self, "data", _freeze_event_data(self.data))

    @property
    def end_ms(self) -> float:
        timestamp = _any_finite(self.timestamp_ms, "event timestamp_ms")
        duration = _finite(self.duration_ms, "event duration_ms")
        end = timestamp + duration
        if not math.isfinite(end):
            raise ValidationError("event end time must be finite")
        return end

    def shifted(self, offset_ms: float) -> Event:
        """Return an event on the shared timeline."""

        checked = _snapshot_event(self)
        offset = _any_finite(offset_ms, "offset_ms")
        shifted = checked.timestamp_ms + offset
        if not math.isfinite(shifted):
            raise ValidationError("shifted event timestamp must be finite")
        return Event(
            id=checked.id,
            stream=checked.stream,
            modality=checked.modality,
            timestamp_ms=shifted,
            duration_ms=checked.duration_ms,
            data=checked.data,
        )

    def to_dict(self) -> dict[str, Any]:
        return _event_to_dict(_snapshot_event(self))


@dataclass(frozen=True, slots=True)
class ClockDrift:
    """Affine clock correction for one stream: a constant offset plus a rate.

    The correction added to an observed timestamp ``t`` is
    ``offset_ms + rate_ppm * 1e-6 * (t - epoch_ms)``. ``offset_ms`` is therefore the offset
    that applies exactly at ``epoch_ms``, and ``rate_ppm`` is how fast that offset grows,
    in parts per million of observed elapsed time. ``rate_ppm = 0`` reproduces the constant
    offset that ``offsets_ms`` already expresses, which is why a stream may be listed in
    one mapping or the other but not both.

    ``rate_ppm`` is bounded by ``MAX_DRIFT_RATE_PPM``. The bound keeps ``1 + rate``
    positive, so the correction is strictly increasing and can never reorder a stream
    against itself.

    Durations are not scaled, matching the existing constant-offset semantics. The residual
    error that leaves on an event of length ``d`` is ``|rate| * d``: at the bound that is
    one part in a hundred, and below a millisecond for any event shorter than 100 seconds.
    """

    rate_ppm: float
    offset_ms: float = 0.0
    epoch_ms: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "rate_ppm", _drift_rate_ppm(self.rate_ppm))
        object.__setattr__(self, "offset_ms", _any_finite(self.offset_ms, "clock drift offset_ms"))
        object.__setattr__(self, "epoch_ms", _any_finite(self.epoch_ms, "clock drift epoch_ms"))

    def offset_at(self, observed_ms: float) -> float:
        """Return the offset to add to one observed timestamp on this clock."""

        checked = _snapshot_clock_drift(self)
        observed = _any_finite(observed_ms, "observed_ms")
        offset = checked.offset_ms + checked.rate_ppm * 1e-6 * (observed - checked.epoch_ms)
        if not math.isfinite(offset):
            raise ValidationError("clock drift correction must remain finite")
        return offset

    def to_dict(self) -> dict[str, Any]:
        checked = _snapshot_clock_drift(self)
        return {
            "rate_ppm": checked.rate_ppm,
            "offset_ms": checked.offset_ms,
            "epoch_ms": checked.epoch_ms,
        }


@dataclass(frozen=True, slots=True)
class AlignmentConfig:
    """Windowing, watermark, clock, and completeness policy."""

    window_ms: float
    hop_ms: float
    allowed_lateness_ms: float = 0.0
    origin_ms: float = 0.0
    required_streams: tuple[str, ...] = ()
    offsets_ms: Mapping[str, float] = field(default_factory=dict)
    clock_drifts: Mapping[str, ClockDrift] = field(default_factory=dict)
    expected_cadence_ms: Mapping[str, float] = field(default_factory=dict)
    gap_factor: float = 1.5
    late_policy: LatePolicy = "reject"
    max_events_per_window: int = 10_000
    max_output_windows: int = 10_000

    def __post_init__(self) -> None:
        object.__setattr__(self, "window_ms", _finite(self.window_ms, "window_ms", positive=True))
        object.__setattr__(self, "hop_ms", _finite(self.hop_ms, "hop_ms", positive=True))
        object.__setattr__(
            self,
            "allowed_lateness_ms",
            _finite(self.allowed_lateness_ms, "allowed_lateness_ms"),
        )
        object.__setattr__(self, "origin_ms", _any_finite(self.origin_ms, "origin_ms"))
        object.__setattr__(
            self,
            "required_streams",
            _label_tuple(self.required_streams, "required_streams", MAX_STREAMS),
        )
        object.__setattr__(
            self,
            "offsets_ms",
            _number_mapping(self.offsets_ms, "offsets_ms", positive=False),
        )
        object.__setattr__(
            self,
            "clock_drifts",
            _drift_mapping(self.clock_drifts, "clock_drifts"),
        )
        conflicting = sorted(set(self.offsets_ms) & set(self.clock_drifts))
        if conflicting:
            raise ValidationError(
                f"stream(s) {', '.join(conflicting)} appear in both offsets_ms and "
                "clock_drifts; a drift correction already carries its own offset, so "
                "listing both would silently double-correct the clock"
            )
        object.__setattr__(
            self,
            "expected_cadence_ms",
            _number_mapping(self.expected_cadence_ms, "expected_cadence_ms", positive=True),
        )
        object.__setattr__(
            self, "gap_factor", _finite(self.gap_factor, "gap_factor", positive=True)
        )
        if self.gap_factor <= 1:
            raise ValidationError("gap_factor must be greater than 1")
        if not isinstance(self.late_policy, str) or self.late_policy not in {
            "reject",
            "drop",
            "accept",
        }:
            raise ValidationError("late_policy must be reject, drop, or accept")
        object.__setattr__(
            self,
            "max_events_per_window",
            _positive_int(
                self.max_events_per_window,
                "max_events_per_window",
                MAX_EVENTS_PER_WINDOW,
            ),
        )
        object.__setattr__(
            self,
            "max_output_windows",
            _positive_int(self.max_output_windows, "max_output_windows", MAX_OUTPUT_WINDOWS),
        )
        values = (
            self.origin_ms + self.hop_ms,
            self.origin_ms + self.window_ms,
            self.origin_ms + (self.max_output_windows - 1) * self.hop_ms,
            self.origin_ms + self.max_output_windows * self.hop_ms,
        )
        if not all(math.isfinite(value) for value in values):
            raise ValidationError("configured window range must remain finite")
        first_start, first_end, last_start, final_start = values
        last_end = last_start + self.window_ms
        final_end = final_start + self.window_ms
        if not all(math.isfinite(value) for value in (last_end, final_end)):
            raise ValidationError("configured window range must remain finite")
        if (
            first_start <= self.origin_ms
            or first_end <= self.origin_ms
            or final_start <= last_start
            or last_end <= last_start
            or final_end <= final_start
        ):
            raise ValidationError("window and hop increments must be representable at this origin")


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Explicit bound on the per-event identity state a live aligner keeps.

    ``horizon_ms`` is measured back from the current watermark: an event's identity
    record is released once its horizon is at least that far behind. The default keeps
    every record, which is the historical behavior and the right choice for finite
    datasets. ``max_tracked_events`` is a hard ceiling on retained records; reaching it
    raises rather than quietly forgetting a reported ID.

    The policy governs bookkeeping only. Buffered events are expired by the window grid,
    which is always stricter, so no configuration of this policy can release an event a
    future window could still include.
    """

    horizon_ms: float = math.inf
    max_tracked_events: int = MAX_EVENTS

    def __post_init__(self) -> None:
        object.__setattr__(self, "horizon_ms", _retention_horizon(self.horizon_ms))
        object.__setattr__(
            self,
            "max_tracked_events",
            _positive_int(self.max_tracked_events, "max_tracked_events", MAX_EVENTS),
        )


@dataclass(frozen=True, slots=True)
class AlignedWindow:
    """Events overlapping one half-open timeline interval."""

    index: int
    start_ms: float
    end_ms: float
    events: tuple[Event, ...]
    missing_streams: tuple[str, ...]

    def __post_init__(self) -> None:
        if isinstance(self.index, bool) or not isinstance(self.index, int) or self.index < 0:
            raise ValidationError("window index must be a non-negative integer")
        start = _any_finite(self.start_ms, "window start_ms")
        end = _any_finite(self.end_ms, "window end_ms")
        if end <= start:
            raise ValidationError("window end_ms must be greater than start_ms")
        raw_events = _bounded_tuple(self.events, "window events", Event, MAX_EVENTS_PER_WINDOW)
        events = tuple(_snapshot_event(event) for event in raw_events)
        if len({event.id for event in events}) != len(events):
            raise ValidationError("window events must not contain duplicate event ids")
        missing = _label_tuple(self.missing_streams, "missing_streams", MAX_STREAMS)
        if any(not _overlaps_bounds(event, start, end) for event in events):
            raise ValidationError("every window event must overlap the half-open window bounds")
        present = {event.stream for event in events}
        if present & set(missing):
            raise ValidationError("missing_streams cannot name a stream present in window events")
        object.__setattr__(self, "start_ms", start)
        object.__setattr__(self, "end_ms", end)
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "missing_streams", missing)

    @property
    def complete(self) -> bool:
        return not _snapshot_window(self).missing_streams

    @property
    def modalities(self) -> tuple[str, ...]:
        checked = _snapshot_window(self)
        return tuple(sorted({event.modality for event in checked.events}))

    def to_dict(self) -> dict[str, Any]:
        return _window_to_dict(_snapshot_window(self))


@dataclass(frozen=True, slots=True)
class Gap:
    """A cadence violation between consecutive events on one stream."""

    stream: str
    start_ms: float
    end_ms: float
    observed_ms: float
    expected_ms: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "stream", _label(self.stream, "gap stream"))
        object.__setattr__(self, "start_ms", _any_finite(self.start_ms, "gap start_ms"))
        object.__setattr__(self, "end_ms", _any_finite(self.end_ms, "gap end_ms"))
        object.__setattr__(
            self, "observed_ms", _finite(self.observed_ms, "gap observed_ms", positive=True)
        )
        object.__setattr__(
            self, "expected_ms", _finite(self.expected_ms, "gap expected_ms", positive=True)
        )
        if self.end_ms <= self.start_ms:
            raise ValidationError("gap end_ms must be greater than start_ms")
        if self.observed_ms <= self.expected_ms:
            raise ValidationError("gap observed_ms must be greater than expected_ms")
        implied_observed = self.end_ms - self.start_ms + self.expected_ms
        if not math.isfinite(implied_observed):
            raise ValidationError(
                "gap boundaries must satisfy observed_ms = end_ms - start_ms + expected_ms"
            )
        rounding_tolerance = max(
            1e-9,
            sum(
                math.ulp(value)
                for value in (
                    self.start_ms,
                    self.end_ms,
                    self.expected_ms,
                    implied_observed,
                )
            ),
        )
        if not math.isclose(
            self.observed_ms,
            implied_observed,
            rel_tol=0.0,
            abs_tol=rounding_tolerance,
        ):
            raise ValidationError(
                "gap boundaries must satisfy observed_ms = end_ms - start_ms + expected_ms"
            )

    def to_dict(self) -> dict[str, Any]:
        return _gap_to_dict(_snapshot_gap(self))


@dataclass(frozen=True, slots=True)
class AlignmentResult:
    """Offline alignment output with dropped-event and gap diagnostics."""

    windows: tuple[AlignedWindow, ...]
    gaps: tuple[Gap, ...]
    dropped_event_ids: tuple[str, ...] = ()
    accepted_late_event_ids: tuple[str, ...] = ()
    unassigned_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        raw_windows = _bounded_tuple(self.windows, "windows", AlignedWindow, MAX_OUTPUT_WINDOWS)
        raw_gaps = _bounded_tuple(self.gaps, "gaps", Gap, MAX_RESULT_GAPS)
        window_list: list[AlignedWindow] = []
        event_by_id: dict[str, Event] = {}
        for raw_window in raw_windows:
            window = _snapshot_window(raw_window)
            if window_list and (
                window.index <= window_list[-1].index or window.start_ms <= window_list[-1].start_ms
            ):
                raise ValidationError("windows must be ordered by increasing index and start_ms")
            for event in window.events:
                existing = event_by_id.get(event.id)
                if existing is None:
                    if len(event_by_id) == MAX_EVENTS:
                        raise ValidationError(
                            f"window events exceed the {MAX_EVENTS}-distinct-event limit"
                        )
                    event_by_id[event.id] = event
                elif existing != event:
                    raise ValidationError(
                        f"event id {event.id!r} must have the same event snapshot in every window"
                    )
            window_list.append(window)
        windows = tuple(window_list)
        gaps = tuple(_snapshot_gap(gap) for gap in raw_gaps)
        if len({window.index for window in windows}) != len(windows):
            raise ValidationError("windows must not contain duplicate indexes")
        if any(
            later.index <= earlier.index or later.start_ms <= earlier.start_ms
            for earlier, later in pairwise(windows)
        ):
            raise ValidationError("windows must be ordered by increasing index and start_ms")
        dropped = _label_tuple(self.dropped_event_ids, "dropped_event_ids", MAX_EVENTS)
        accepted = _label_tuple(self.accepted_late_event_ids, "accepted_late_event_ids", MAX_EVENTS)
        unassigned = _label_tuple(self.unassigned_event_ids, "unassigned_event_ids", MAX_EVENTS)
        dropped_set = set(dropped)
        accepted_set = set(accepted)
        unassigned_set = set(unassigned)
        if dropped_set & (accepted_set | unassigned_set) or accepted_set & unassigned_set:
            raise ValidationError(
                "dropped, accepted-late, and unassigned event ids must be disjoint"
            )
        window_event_ids = set(event_by_id)
        if window_event_ids & (dropped_set | unassigned_set):
            raise ValidationError("dropped or unassigned event ids cannot appear in a window")
        if not accepted_set <= window_event_ids:
            raise ValidationError("every accepted-late event id must appear in a window")
        object.__setattr__(self, "windows", windows)
        object.__setattr__(self, "gaps", gaps)
        object.__setattr__(self, "dropped_event_ids", dropped)
        object.__setattr__(self, "accepted_late_event_ids", accepted)
        object.__setattr__(self, "unassigned_event_ids", unassigned)

    def to_dict(self) -> dict[str, Any]:
        return _result_to_dict(_snapshot_result(self))


def _overlaps_bounds(event: Event, start: float, end: float) -> bool:
    if event.duration_ms == 0:
        return start <= event.timestamp_ms < end
    return event.timestamp_ms < end and event.timestamp_ms + event.duration_ms > start


def _snapshot_event(value: Event) -> Event:
    """Revalidate and detach an event at a public model boundary."""

    return Event(
        id=value.id,
        stream=value.stream,
        modality=value.modality,
        timestamp_ms=value.timestamp_ms,
        duration_ms=value.duration_ms,
        data=value.data,
    )


def _snapshot_clock_drift(value: ClockDrift) -> ClockDrift:
    """Revalidate a clock correction before a public operation."""

    return ClockDrift(
        rate_ppm=value.rate_ppm,
        offset_ms=value.offset_ms,
        epoch_ms=value.epoch_ms,
    )


def _snapshot_window(value: AlignedWindow) -> AlignedWindow:
    """Revalidate and detach a window, including every nested event."""

    return AlignedWindow(
        index=value.index,
        start_ms=value.start_ms,
        end_ms=value.end_ms,
        events=value.events,
        missing_streams=value.missing_streams,
    )


def _snapshot_gap(value: Gap) -> Gap:
    """Revalidate and detach a gap diagnostic."""

    return Gap(
        stream=value.stream,
        start_ms=value.start_ms,
        end_ms=value.end_ms,
        observed_ms=value.observed_ms,
        expected_ms=value.expected_ms,
    )


def _snapshot_result(value: AlignmentResult) -> AlignmentResult:
    """Revalidate and detach a complete result before publication."""

    return AlignmentResult(
        windows=value.windows,
        gaps=value.gaps,
        dropped_event_ids=value.dropped_event_ids,
        accepted_late_event_ids=value.accepted_late_event_ids,
        unassigned_event_ids=value.unassigned_event_ids,
    )


def _event_to_dict(value: Event) -> dict[str, Any]:
    return {
        "id": value.id,
        "stream": value.stream,
        "modality": value.modality,
        "timestamp_ms": value.timestamp_ms,
        "duration_ms": value.duration_ms,
        "data": _thaw_json(value.data),
    }


def _window_to_dict(value: AlignedWindow) -> dict[str, Any]:
    return {
        "index": value.index,
        "start_ms": value.start_ms,
        "end_ms": value.end_ms,
        "complete": not value.missing_streams,
        "missing_streams": list(value.missing_streams),
        "modalities": sorted({event.modality for event in value.events}),
        "events": [_event_to_dict(event) for event in value.events],
    }


def _gap_to_dict(value: Gap) -> dict[str, Any]:
    return {
        "stream": value.stream,
        "start_ms": value.start_ms,
        "end_ms": value.end_ms,
        "observed_ms": value.observed_ms,
        "expected_ms": value.expected_ms,
    }


def _result_to_dict(value: AlignmentResult) -> dict[str, Any]:
    return {
        "windows": [_window_to_dict(window) for window in value.windows],
        "gaps": [_gap_to_dict(gap) for gap in value.gaps],
        "dropped_event_ids": list(value.dropped_event_ids),
        "accepted_late_event_ids": list(value.accepted_late_event_ids),
        "unassigned_event_ids": list(value.unassigned_event_ids),
    }
