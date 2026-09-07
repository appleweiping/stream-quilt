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
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from stream_quilt.errors import ValidationError
from stream_quilt.models import AlignmentConfig, Event


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "horizon_ms": self.horizon_ms,
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

    def __post_init__(self) -> None:
        if not isinstance(self.config_digest, str) or len(self.config_digest) != 64:
            raise ValidationError("checkpoint config_digest must be a SHA-256 hex digest")
        try:
            int(self.config_digest, 16)
        except ValueError as error:
            raise ValidationError(
                "checkpoint config_digest must be a SHA-256 hex digest"
            ) from error
        if type(self.next_index) is not int or self.next_index < 0:
            raise ValidationError("checkpoint next_index must be non-negative")
        if not isinstance(self.next_start, (int, float)):
            raise ValidationError("checkpoint next_start must be numeric")
        if not isinstance(self.max_seen, Mapping):
            raise ValidationError("checkpoint max_seen must be a mapping")
        max_seen = {str(key): float(value) for key, value in self.max_seen.items()}
        object.__setattr__(self, "max_seen", MappingProxyType(max_seen))
        if any(not isinstance(event, Event) for event in self.live_events):
            raise ValidationError("checkpoint live_events must contain Event records")
        if any(not isinstance(item, TrackedCheckpoint) for item in self.seen_order):
            raise ValidationError("checkpoint seen_order must contain TrackedCheckpoint records")
        for name in (
            "assigned_event_ids",
            "dropped_event_ids",
            "accepted_late_event_ids",
            "unassigned_event_ids",
        ):
            values = tuple(getattr(self, name))
            if any(not isinstance(value, str) or not value for value in values):
                raise ValidationError(f"checkpoint {name} must contain non-empty strings")
            if len(values) != len(set(values)):
                raise ValidationError(f"checkpoint {name} must not contain duplicates")
            object.__setattr__(self, name, values)
        for name in ("released_count", "released_reported_count"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValidationError(f"checkpoint {name} must be non-negative")
        if not isinstance(self.closed, bool):
            raise ValidationError("checkpoint closed must be a boolean")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
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
        }


__all__ = ["AlignerCheckpoint", "TrackedCheckpoint", "config_digest"]
