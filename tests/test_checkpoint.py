from __future__ import annotations

from dataclasses import replace

import pytest

from stream_quilt import AlignmentConfig, ClockDrift, Event, WatermarkAligner, config_digest
from stream_quilt.errors import ValidationError


def _event(event_id: str, stream: str, timestamp: float) -> Event:
    return Event(event_id, stream, "text", timestamp, 0, {})


def test_checkpoint_round_trip_preserves_live_processing() -> None:
    config = AlignmentConfig(
        window_ms=10,
        hop_ms=10,
        required_streams=("a", "b"),
        allowed_lateness_ms=1,
    )
    first = WatermarkAligner(config)
    first.ingest(_event("a0", "a", 0))
    checkpoint = first.checkpoint()
    restored = WatermarkAligner.from_checkpoint(config, checkpoint)
    assert restored.checkpoint().to_dict() == checkpoint.to_dict()
    assert restored.ingest(_event("b0", "b", 0)) == first.ingest(_event("b0", "b", 0))
    assert restored.flush() == first.flush()


def test_checkpoint_rejects_configuration_mismatch() -> None:
    config = AlignmentConfig(window_ms=10, hop_ms=10)
    checkpoint = WatermarkAligner(config).checkpoint()
    changed = AlignmentConfig(window_ms=20, hop_ms=10)
    with pytest.raises(ValidationError):
        WatermarkAligner.from_checkpoint(changed, checkpoint)


def test_checkpoint_validates_snapshot_fields_and_drift_digest() -> None:
    config = AlignmentConfig(
        window_ms=10,
        hop_ms=10,
        clock_drifts={"a": ClockDrift(rate_ppm=2, offset_ms=1, epoch_ms=0)},
    )
    assert len(config_digest(config)) == 64
    with pytest.raises(ValidationError):
        config_digest(object())  # type: ignore[arg-type]
    checkpoint = WatermarkAligner(config).checkpoint()
    for changes in (
        {"config_digest": "bad"},
        {"config_digest": "z" * 64},
        {"next_index": -1},
        {"next_start": "bad"},
        {"max_seen": []},
        {"live_events": (object(),)},
        {"seen_order": (object(),)},
        {"assigned_event_ids": ("",)},
        {"assigned_event_ids": ("a", "a")},
        {"released_count": -1},
        {"closed": 1},
    ):
        with pytest.raises(ValidationError):
            replace(checkpoint, **changes)
