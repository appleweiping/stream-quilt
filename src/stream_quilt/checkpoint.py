"""Portable checkpoints for :class:`WatermarkAligner` recovery.

Checkpoints contain only validated, normalized state and a configuration digest.
They allow a process to resume without replaying the whole input, while a
configuration mismatch fails closed instead of silently changing window
semantics.  The checkpoint is an operational snapshot, not a proof that a
source event was authentic.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any

from stream_quilt.errors import ValidationError
from stream_quilt.limits import MAX_EVENTS, MAX_OUTPUT_WINDOWS
from stream_quilt.models import (
    AlignmentConfig,
    Event,
    RetentionPolicy,
    _any_finite,
    _bounded_tuple,
    _label,
    _label_tuple,
    _number_mapping,
)


def config_digest(config: AlignmentConfig) -> str:
    """Return the stable SHA-256 identity of all alignment semantics."""

    if not isinstance(config, AlignmentConfig):
        raise ValidationError("config must be an AlignmentConfig")
    document = {
        "window_ms": config.window_ms,
        "hop_ms": config.hop_ms,
        "allowed_lateness_ms": config.allowed_lateness_ms,
        "origin_ms": config.origin_ms,
        "required_streams": list(config.required_streams),
        "offsets_ms": dict(sorted(config.offsets_ms.items())),
        "clock_drifts": {
            key: config.clock_drifts[key].to_dict() for key in sorted(config.clock_drifts)
        },
        "expected_cadence_ms": dict(sorted(config.expected_cadence_ms.items())),
        "gap_factor": config.gap_factor,
        "late_policy": config.late_policy,
        "max_events_per_window": config.max_events_per_window,
        "max_output_windows": config.max_output_windows,
    }
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class TrackedCheckpoint:
    event_id: str
    horizon_ms: float
    status: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _label(self.event_id, "tracked event_id"))
        # A point at the largest float has an infinite exclusive horizon.
        horizon = self.horizon_ms
        if not (type(horizon) is float and horizon == math.inf):
            horizon = _any_finite(horizon, "tracked horizon_ms")
        object.__setattr__(self, "horizon_ms", horizon)
        if type(self.status) is not int or self.status not in (0, 1, 2):
            raise ValidationError("tracked status must be 0, 1, or 2")

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "horizon_ms": None if self.horizon_ms == math.inf else self.horizon_ms,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class AlignerCheckpoint:
    """A complete immutable snapshot of a live or flushed aligner."""

    config_digest: str
    next_index: int
    next_start: float
    max_seen: Mapping[str, float]
    live_events: tuple[Event, ...]
    seen_order: tuple[TrackedCheckpoint, ...]
    assigned_event_ids: tuple[str, ...]
    dropped_event_ids: tuple[str, ...]
    accepted_late_event_ids: tuple[str, ...]
    unassigned_event_ids: tuple[str, ...]
    released_count: int
    released_reported_count: int
    closed: bool
    retention: RetentionPolicy = field(default_factory=RetentionPolicy)

    def __post_init__(self) -> None:
        if (
            type(self.config_digest) is not str
            or len(self.config_digest) != 64
            or any(char not in "0123456789abcdef" for char in self.config_digest)
        ):
            raise ValidationError("checkpoint config_digest must be a SHA-256 hex digest")
        if type(self.next_index) is not int or not 0 <= self.next_index <= MAX_OUTPUT_WINDOWS:
            raise ValidationError("checkpoint next_index is outside the window limit")
        object.__setattr__(
            self, "next_start", _any_finite(self.next_start, "checkpoint next_start")
        )
        object.__setattr__(
            self, "max_seen", _number_mapping(self.max_seen, "max_seen", positive=False)
        )
        live = _bounded_tuple(self.live_events, "live_events", Event, MAX_EVENTS)
        live = tuple(Event(**event.to_dict()) for event in live)
        tracked = _bounded_tuple(self.seen_order, "seen_order", TrackedCheckpoint, MAX_EVENTS)
        tracked = tuple(
            TrackedCheckpoint(item.event_id, item.horizon_ms, item.status) for item in tracked
        )
        object.__setattr__(self, "live_events", live)
        object.__setattr__(self, "seen_order", tracked)
        if len({event.id for event in live}) != len(live):
            raise ValidationError("checkpoint live_events must have unique IDs")
        if len({item.event_id for item in tracked}) != len(tracked):
            raise ValidationError("checkpoint seen_order must have unique IDs")
        for name in (
            "assigned_event_ids",
            "dropped_event_ids",
            "accepted_late_event_ids",
            "unassigned_event_ids",
        ):
            values = _label_tuple(getattr(self, name), name, MAX_EVENTS)
            object.__setattr__(self, name, values)
        for name in ("released_count", "released_reported_count"):
            value = getattr(self, name)
            if type(value) is not int or not 0 <= value <= 2**53 - 1:
                raise ValidationError(f"checkpoint {name} must be a non-negative safe integer")
        if not isinstance(self.closed, bool):
            raise ValidationError("checkpoint closed must be a boolean")
        if not isinstance(self.retention, RetentionPolicy):
            raise ValidationError("checkpoint retention must be a RetentionPolicy")
        object.__setattr__(
            self,
            "retention",
            RetentionPolicy(self.retention.horizon_ms, self.retention.max_tracked_events),
        )
        if self.released_reported_count > self.released_count:
            raise ValidationError("reported release count exceeds released count")
        if len(tracked) + self.released_reported_count > self.retention.max_tracked_events:
            raise ValidationError("checkpoint exceeds retention max_tracked_events")
        ids = {item.event_id: item for item in tracked}
        if not set(self.assigned_event_ids) <= ids.keys():
            raise ValidationError("assigned IDs must be retained")
        for event in live:
            if event.id not in ids or ids[event.id].status == 1:
                raise ValidationError("live event must have a non-dropped identity record")
            if (
                event.stream not in self.max_seen
                or self.max_seen[event.stream] < event.timestamp_ms
            ):
                raise ValidationError("live event is ahead of its stream watermark state")
        if self.closed and live:
            raise ValidationError("closed checkpoint must not retain live events")

    @classmethod
    def from_dict(cls, document: Any) -> AlignerCheckpoint:
        """Read strict version 1.1 JSON data; no permissive coercion or pickle."""
        from stream_quilt.io import event_from_dict

        if not isinstance(document, Mapping):
            raise ValidationError("checkpoint must be an object")
        expected = {item.name for item in fields(cls)} | {"schema_version", "kind"}
        if set(document) != expected:
            raise ValidationError("checkpoint fields are missing or unknown")
        if (
            document["schema_version"] != "1.1"
            or document["kind"] != "stream-quilt-aligner-checkpoint"
        ):
            raise ValidationError("unsupported checkpoint schema or kind")
        values = dict(document)
        del values["schema_version"], values["kind"]
        for name in ("live_events", "seen_order"):
            if not isinstance(values[name], list) or len(values[name]) > MAX_EVENTS:
                raise ValidationError(f"checkpoint {name} must be a bounded JSON array")
        values["live_events"] = tuple(event_from_dict(item) for item in values["live_events"])
        tracked = []
        for item in values["seen_order"]:
            if not isinstance(item, Mapping) or set(item) != {"event_id", "horizon_ms", "status"}:
                raise ValidationError("invalid tracked checkpoint fields")
            horizon = (
                math.inf
                if item["horizon_ms"] is None
                else _any_finite(item["horizon_ms"], "tracked horizon_ms")
            )
            tracked.append(TrackedCheckpoint(item["event_id"], horizon, item["status"]))
        values["seen_order"] = tuple(tracked)
        retention = values["retention"]
        if not isinstance(retention, Mapping) or set(retention) != {
            "horizon_ms",
            "max_tracked_events",
        }:
            raise ValidationError("invalid checkpoint retention fields")
        values["retention"] = RetentionPolicy(
            math.inf
            if retention["horizon_ms"] is None
            else _any_finite(retention["horizon_ms"], "retention horizon_ms"),
            retention["max_tracked_events"],
        )
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.1",
            "kind": "stream-quilt-aligner-checkpoint",
            "config_digest": self.config_digest,
            "next_index": self.next_index,
            "next_start": self.next_start,
            "max_seen": dict(sorted(self.max_seen.items())),
            "live_events": [event.to_dict() for event in self.live_events],
            "seen_order": [item.to_dict() for item in self.seen_order],
            "assigned_event_ids": list(self.assigned_event_ids),
            "dropped_event_ids": list(self.dropped_event_ids),
            "accepted_late_event_ids": list(self.accepted_late_event_ids),
            "unassigned_event_ids": list(self.unassigned_event_ids),
            "released_count": self.released_count,
            "released_reported_count": self.released_reported_count,
            "closed": self.closed,
            "retention": {
                "horizon_ms": None
                if self.retention.horizon_ms == math.inf
                else self.retention.horizon_ms,
                "max_tracked_events": self.retention.max_tracked_events,
            },
        }


__all__ = ["AlignerCheckpoint", "TrackedCheckpoint", "config_digest"]
