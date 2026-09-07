from __future__ import annotations

import pytest

from stream_quilt import Event, sessionize
from stream_quilt.errors import ValidationError


def _event(event_id: str, stream: str, timestamp: float, duration: float = 0) -> Event:
    return Event(event_id, stream, "text", timestamp, duration, {})


def test_sessionize_uses_event_end_and_stable_stream_order() -> None:
    result = sessionize(
        [
            _event("b2", "b", 2),
            _event("a2", "a", 2),
            _event("a1", "a", 0, 1),
            _event("b1", "b", 0),
            _event("a3", "a", 5),
        ],
        gap_ms=2,
    )
    assert result.streams == ("a", "b")
    assert [(window.stream, window.event_ids) for window in result.windows] == [
        ("a", ("a1", "a2")),
        ("a", ("a3",)),
        ("b", ("b1", "b2")),
    ]
    assert result.to_dict()["session_count"] == 3


def test_sessionize_rejects_invalid_gap_and_duplicate_ids() -> None:
    with pytest.raises(ValidationError):
        sessionize([], gap_ms=True)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        sessionize([], gap_ms=-1)
    event = _event("same", "a", 0)
    with pytest.raises(ValidationError):
        sessionize([event, event], gap_ms=1)
    with pytest.raises(ValidationError):
        sessionize([object()], gap_ms=1)  # type: ignore[list-item]
