"""Strict JSON and JSONL parsing for configurations and events."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TypeGuard

from stream_quilt.errors import ValidationError
from stream_quilt.limits import (
    MAX_CONFIG_BYTES,
    MAX_EVENT_FILE_BYTES,
    MAX_EVENTS,
    MAX_MAPPING_ENTRIES,
    MAX_STREAMS,
    MAX_TEXT_LENGTH,
)
from stream_quilt.models import AlignmentConfig, ClockDrift, Event, LatePolicy

_CONFIG_FIELDS = {
    "window_ms",
    "hop_ms",
    "allowed_lateness_ms",
    "origin_ms",
    "required_streams",
    "offsets_ms",
    "clock_drifts",
    "expected_cadence_ms",
    "gap_factor",
    "late_policy",
    "max_events_per_window",
    "max_output_windows",
}
_EVENT_FIELDS = {"id", "stream", "modality", "timestamp_ms", "duration_ms", "data"}
_DRIFT_FIELDS = {"rate_ppm", "offset_ms", "epoch_ms"}


def load_config(path: str | Path) -> AlignmentConfig:
    """Load and validate a JSON configuration."""

    source = Path(path)
    try:
        payload = json.loads(
            _read_text_limited(source, MAX_CONFIG_BYTES, "config"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"cannot read config {source}: {exc}") from exc
    except (json.JSONDecodeError, RecursionError) as exc:
        if isinstance(exc, RecursionError):
            raise ValidationError(f"invalid JSON in {source}: nesting is too deep") from exc
        raise ValidationError(
            f"invalid JSON in {source} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    except _StrictJsonError as exc:
        raise ValidationError(f"invalid JSON in {source}: {exc}") from exc
    return config_from_dict(payload)


def load_events(path: str | Path) -> tuple[Event, ...]:
    """Load newline-delimited event objects, preserving arrival order."""

    source = Path(path)
    try:
        lines = _read_text_limited(source, MAX_EVENT_FILE_BYTES, "event").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"cannot read events {source}: {exc}") from exc
    events: list[Event] = []
    seen: set[str] = set()
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        if len(events) == MAX_EVENTS:
            raise ValidationError(f"event file exceeds the {MAX_EVENTS}-record limit")
        try:
            payload = json.loads(
                line,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (json.JSONDecodeError, RecursionError) as exc:
            if isinstance(exc, RecursionError):
                raise ValidationError(
                    f"invalid JSON in {source} at line {line_number}: nesting is too deep"
                ) from exc
            raise ValidationError(
                f"invalid JSON in {source} at line {line_number}, column {exc.colno}: {exc.msg}"
            ) from exc
        except _StrictJsonError as exc:
            raise ValidationError(f"invalid JSON in {source} at line {line_number}: {exc}") from exc
        event = event_from_dict(payload, path=f"line {line_number}")
        if event.id in seen:
            raise ValidationError(f"duplicate event id {event.id!r} at line {line_number}")
        seen.add(event.id)
        events.append(event)
    return tuple(events)


def config_from_dict(payload: Any) -> AlignmentConfig:
    """Parse and validate an alignment configuration."""

    item = _mapping(payload, "$config")
    _reject_unknown(item, _CONFIG_FIELDS, "$config")
    required = _string_sequence(item.get("required_streams", []), "required_streams")
    if len(required) != len(set(required)):
        raise ValidationError("required_streams must not contain duplicates")
    offsets = _number_mapping(item.get("offsets_ms", {}), "offsets_ms", positive=False)
    drifts = _drift_mapping(item.get("clock_drifts", {}), "clock_drifts")
    cadence = _number_mapping(
        item.get("expected_cadence_ms", {}), "expected_cadence_ms", positive=True
    )
    late_policy = item.get("late_policy", "reject")
    if not _is_late_policy(late_policy):
        raise ValidationError("late_policy must be one of: reject, drop, accept")
    max_events = item.get("max_events_per_window", 10_000)
    if isinstance(max_events, bool) or not isinstance(max_events, int) or max_events < 1:
        raise ValidationError("max_events_per_window must be a positive integer")
    max_windows = item.get("max_output_windows", 10_000)
    if isinstance(max_windows, bool) or not isinstance(max_windows, int) or max_windows < 1:
        raise ValidationError("max_output_windows must be a positive integer")
    config = AlignmentConfig(
        window_ms=_positive(item.get("window_ms"), "window_ms"),
        hop_ms=_positive(item.get("hop_ms"), "hop_ms"),
        allowed_lateness_ms=_nonnegative(
            item.get("allowed_lateness_ms", 0.0), "allowed_lateness_ms"
        ),
        origin_ms=_finite(item.get("origin_ms", 0.0), "origin_ms"),
        required_streams=required,
        offsets_ms=offsets,
        clock_drifts=drifts,
        expected_cadence_ms=cadence,
        gap_factor=_positive(item.get("gap_factor", 1.5), "gap_factor"),
        late_policy=late_policy,
        max_events_per_window=max_events,
        max_output_windows=max_windows,
    )
    from stream_quilt.aligner import _validate_config

    _validate_config(config)
    return config


def event_from_dict(payload: Any, *, path: str = "$event") -> Event:
    """Parse one event object without applying clock correction."""

    item = _mapping(payload, path)
    _reject_unknown(item, _EVENT_FIELDS, path)
    data = item.get("data", {})
    if not isinstance(data, Mapping):
        raise ValidationError(f"{path}.data must be an object with string keys")
    return Event(
        id=_text(item.get("id"), f"{path}.id"),
        stream=_text(item.get("stream"), f"{path}.stream"),
        modality=_text(item.get("modality"), f"{path}.modality"),
        timestamp_ms=_finite(item.get("timestamp_ms"), f"{path}.timestamp_ms"),
        duration_ms=_nonnegative(item.get("duration_ms", 0.0), f"{path}.duration_ms"),
        data=data,
    )


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{path} must be an object with string keys")
    if len(value) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
    result: dict[str, Any] = {}
    for index, (key, item) in enumerate(value.items()):
        if index == MAX_MAPPING_ENTRIES:
            raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
        if not isinstance(key, str):
            raise ValidationError(f"{path} must be an object with string keys")
        result[key] = item
    return result


def _reject_unknown(item: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(item) - allowed)
    if unknown:
        raise ValidationError(f"{path} contains unknown field(s): {', '.join(unknown)}")


def _is_late_policy(value: Any) -> TypeGuard[LatePolicy]:
    return isinstance(value, str) and value in {"reject", "drop", "accept"}


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{path} must be a non-empty string")
    text = value.strip()
    if len(text) > MAX_TEXT_LENGTH:
        raise ValidationError(f"{path} exceeds the {MAX_TEXT_LENGTH}-character limit")
    try:
        text.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValidationError(f"{path} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValidationError(f"{path} must not contain control characters")
    return text


def _finite(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{path} must be a number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ValidationError(f"{path} must be finite") from exc
    if number != number or number in {float("inf"), float("-inf")}:
        raise ValidationError(f"{path} must be finite")
    return number


def _positive(value: Any, path: str) -> float:
    number = _finite(value, path)
    if number <= 0:
        raise ValidationError(f"{path} must be greater than zero")
    return number


def _nonnegative(value: Any, path: str) -> float:
    number = _finite(value, path)
    if number < 0:
        raise ValidationError(f"{path} must be zero or greater")
    return number


def _string_sequence(value: Any, path: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValidationError(f"{path} must be an array")
    if len(value) > MAX_STREAMS:
        raise ValidationError(f"{path} exceeds the {MAX_STREAMS}-item limit")
    result: list[str] = []
    for index, item in enumerate(value):
        if index == MAX_STREAMS:
            raise ValidationError(f"{path} exceeds the {MAX_STREAMS}-item limit")
        result.append(_text(item, f"{path}[{index}]"))
    return tuple(result)


def _number_mapping(value: Any, path: str, *, positive: bool) -> dict[str, float]:
    item = _mapping(value, path)
    if len(item) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
    parser = _positive if positive else _finite
    result: dict[str, float] = {}
    for key, number in item.items():
        normalized_key = _text(key, f"{path} key")
        if normalized_key in result:
            raise ValidationError(
                f"{path} contains duplicate key after normalization: {normalized_key!r}"
            )
        result[normalized_key] = parser(number, f"{path}.{key}")
    return result


def _drift_mapping(value: Any, path: str) -> dict[str, ClockDrift]:
    item = _mapping(value, path)
    if len(item) > MAX_MAPPING_ENTRIES:
        raise ValidationError(f"{path} exceeds the {MAX_MAPPING_ENTRIES}-entry limit")
    result: dict[str, ClockDrift] = {}
    for key, raw in item.items():
        normalized_key = _text(key, f"{path} key")
        if normalized_key in result:
            raise ValidationError(
                f"{path} contains duplicate key after normalization: {normalized_key!r}"
            )
        entry = _mapping(raw, f"{path}.{key}")
        _reject_unknown(entry, _DRIFT_FIELDS, f"{path}.{key}")
        result[normalized_key] = ClockDrift(
            rate_ppm=_finite(entry.get("rate_ppm"), f"{path}.{key}.rate_ppm"),
            offset_ms=_finite(entry.get("offset_ms", 0.0), f"{path}.{key}.offset_ms"),
            epoch_ms=_finite(entry.get("epoch_ms", 0.0), f"{path}.{key}.epoch_ms"),
        )
    return result


class _StrictJsonError(ValueError):
    """A strict-JSON violation not represented by JSONDecodeError."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _StrictJsonError(f"duplicate object key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise _StrictJsonError(f"non-finite number {value} is not permitted")


def _read_text_limited(source: Path, maximum: int, label: str) -> str:
    with source.open("rb") as handle:
        raw = handle.read(maximum + 1)
    if len(raw) > maximum:
        raise ValidationError(f"{label} file exceeds the {maximum}-byte limit")
    return raw.decode("utf-8")
