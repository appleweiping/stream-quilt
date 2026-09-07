from __future__ import annotations

import pytest

from stream_quilt import Event, partition_events
from stream_quilt.errors import ValidationError


def _event(event_id: str, timestamp: float) -> Event:
    return Event(event_id, "camera", "video", timestamp, 0, {})


def test_partitioning_is_stable_and_complete() -> None:
    events = [_event("b", 2), _event("a", 1), _event("c", 3)]
    first = partition_events(events, 3)
    second = partition_events(list(reversed(events)), 3)
    assert first == second
    assert sum(len(partition) for partition in first.partitions) == 3
    assert all(
        first.partition_for(event.id) == index
        for index, partition in enumerate(first.partitions)
        for event in partition
    )
    assert first.to_dict()["event_count"] == 3


def test_partitioning_rejects_bad_inputs() -> None:
    with pytest.raises(ValidationError):
        partition_events([], 0)
    event = _event("same", 0)
    with pytest.raises(ValidationError):
        partition_events([event, event], 2)
    with pytest.raises(ValidationError):
        partition_events([object()], 2)  # type: ignore[list-item]
    with pytest.raises(ValidationError):
        partition_events([], True)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        partition_events([], 1025)
    with pytest.raises(ValidationError):
        partition_events([], 2).partition_for("")
