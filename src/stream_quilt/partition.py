"""Deterministic event partitioning for parallel stream workers."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from stream_quilt.errors import ValidationError
from stream_quilt.models import Event


@dataclass(frozen=True, slots=True)
class PartitionedEvents:
    """Stable partition assignment with per-partition event order."""

    partition_count: int
    partitions: tuple[tuple[Event, ...], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "kind": "stream-quilt-partitions",
            "partition_count": self.partition_count,
            "partitions": [[event.to_dict() for event in events] for events in self.partitions],
            "event_count": sum(len(events) for events in self.partitions),
        }

    def partition_for(self, event_id: str) -> int:
        """Return the deterministic partition for an event identity."""

        if not isinstance(event_id, str) or not event_id:
            raise ValidationError("event_id must be a non-empty string")
        return (
            int.from_bytes(hashlib.sha256(event_id.encode()).digest()[:8], "big")
            % self.partition_count
        )


def partition_events(events: Iterable[Event], partition_count: int) -> PartitionedEvents:
    """Assign unique event IDs to partitions using SHA-256, not process-random hash()."""

    if (
        isinstance(partition_count, bool)
        or not isinstance(partition_count, int)
        or not 1 <= partition_count <= 1024
    ):
        raise ValidationError("partition_count must be an integer from 1 to 1024")
    supplied = list(events)
    if any(not isinstance(event, Event) for event in supplied):
        raise ValidationError("events must contain Event records")
    if len({event.id for event in supplied}) != len(supplied):
        raise ValidationError("events must have unique IDs")
    buckets: dict[int, list[Event]] = defaultdict(list)
    for event in supplied:
        partition = (
            int.from_bytes(hashlib.sha256(event.id.encode()).digest()[:8], "big") % partition_count
        )
        buckets[partition].append(event)
    partitions = tuple(
        tuple(sorted(buckets[index], key=lambda item: (item.timestamp_ms, item.stream, item.id)))
        for index in range(partition_count)
    )
    return PartitionedEvents(partition_count, partitions)


__all__ = ["PartitionedEvents", "partition_events"]
