from __future__ import annotations

import itertools

import pytest

from stream_quilt.aligner import WatermarkAligner, align_events, detect_gaps
from stream_quilt.errors import LateEventError, ValidationError
from stream_quilt.models import AlignmentConfig, Event


def config(**overrides):
    values = {
        "window_ms": 100,
        "hop_ms": 100,
        "allowed_lateness_ms": 0,
        "origin_ms": 0,
        "required_streams": (),
        "offsets_ms": {},
        "expected_cadence_ms": {},
        "gap_factor": 1.5,
        "late_policy": "reject",
        "max_events_per_window": 100,
        "max_output_windows": 100,
    }
    values.update(overrides)
    return AlignmentConfig(**values)


def event(event_id, timestamp, *, stream="a", modality="video", duration=0):
    return Event(event_id, stream, modality, timestamp, duration)


def test_event_at_origin_is_not_late():
    result = align_events([event("e", 0)], config())
    assert result.windows[0].events[0].id == "e"


def test_half_open_boundary_places_point_in_next_window():
    result = align_events([event("e", 100)], config())
    assert [window.index for window in result.windows] == [0, 1]
    assert result.windows[0].events == ()
    assert result.windows[1].events[0].id == "e"


def test_point_at_large_epoch_origin_emits_one_window():
    origin = 1_700_000_000_000.0
    result = align_events([event("e", origin)], config(origin_ms=origin))
    assert len(result.windows) == 1
    assert result.windows[0].events[0].id == "e"


def test_point_just_before_limit_boundary_does_not_add_a_window():
    result = align_events(
        [event("e", 199.9999999995)],
        config(max_output_windows=2),
    )
    assert len(result.windows) == 2


def test_duration_overlaps_multiple_windows():
    result = align_events([event("long", 50, duration=180)], config(window_ms=100, hop_ms=50))
    assert [window.index for window in result.windows if window.events] == [0, 1, 2, 3, 4]


def test_duration_ending_at_boundary_does_not_enter_next_window():
    result = align_events([event("clip", 20, duration=80)], config())
    assert len(result.windows) == 1


def test_required_streams_control_completeness():
    result = align_events(
        [event("a0", 0, stream="a"), event("b0", 150, stream="b")],
        config(required_streams=("a", "b")),
    )
    assert result.windows[0].missing_streams == ("b",)
    assert result.windows[1].missing_streams == ("a",)


def test_offsets_are_applied_before_windowing():
    result = align_events(
        [event("e", 110, stream="camera")],
        config(offsets_ms={"camera": -20}),
    )
    assert result.windows[0].events[0].timestamp_ms == 90


def test_offline_alignment_is_independent_of_input_order():
    events = [event("a", 10), event("b", 120), event("c", 75)]
    expected = align_events(events, config()).to_dict()
    for permutation in itertools.permutations(events):
        assert align_events(permutation, config()).to_dict() == expected


def test_watermark_waits_for_all_required_streams():
    aligner = WatermarkAligner(config(required_streams=("a", "b")))
    assert aligner.ingest(event("a0", 200, stream="a")) == ()
    assert aligner.watermark_ms is None
    windows = aligner.ingest(event("b0", 200, stream="b"))
    assert len(windows) == 2
    assert aligner.watermark_ms == 200


def test_empty_required_streams_defer_closure_until_flush():
    aligner = WatermarkAligner(config())
    assert aligner.ingest(event("a0", 200, stream="a")) == ()
    assert aligner.watermark_ms is None
    assert aligner.ingest(event("b0", 50, stream="b")) == ()
    windows = aligner.flush()
    assert any(item.id == "b0" for window in windows for item in window.events)


def test_explicit_single_required_stream_advances_watermark():
    aligner = WatermarkAligner(config(required_streams=("a",)))
    assert len(aligner.ingest(event("a0", 200, stream="a"))) == 2


def test_allowed_lateness_delays_window_close():
    aligner = WatermarkAligner(config(required_streams=("a", "b"), allowed_lateness_ms=50))
    aligner.ingest(event("a0", 140, stream="a"))
    assert aligner.ingest(event("b0", 140, stream="b")) == ()
    assert len(aligner.ingest(event("a1", 160, stream="a"))) == 0
    assert len(aligner.ingest(event("b1", 160, stream="b"))) == 1


def _close_first_window(late_policy):
    aligner = WatermarkAligner(config(required_streams=("a", "b"), late_policy=late_policy))
    aligner.ingest(event("a0", 200, stream="a"))
    aligner.ingest(event("b0", 200, stream="b"))
    return aligner


def test_late_event_reject_policy():
    aligner = _close_first_window("reject")
    with pytest.raises(LateEventError, match="closed output"):
        aligner.ingest(event("late", 50, stream="a"))


def test_late_event_drop_policy_records_id():
    aligner = _close_first_window("drop")
    assert aligner.ingest(event("late", 50, stream="a")) == ()
    assert aligner.dropped_event_ids == ("late",)


def test_late_duration_that_overlaps_open_window_is_accepted():
    aligner = _close_first_window("accept")
    assert aligner.ingest(event("overlap", 150, stream="a", duration=100)) == ()
    assert aligner.accepted_late_event_ids == ("overlap",)
    remaining = aligner.flush()
    assert any(item.id == "overlap" for window in remaining for item in window.events)


def test_accept_policy_reports_fully_obsolete_event_as_dropped():
    aligner = _close_first_window("accept")
    assert aligner.ingest(event("late", 50, stream="a")) == ()
    assert aligner.dropped_event_ids == ("late",)
    assert all(item.id != "late" for window in aligner.flush() for item in window.events)


def test_overlapping_window_partial_lateness_is_rejected():
    aligner = WatermarkAligner(
        config(window_ms=100, hop_ms=50, required_streams=("a",), late_policy="reject")
    )
    aligner.ingest(event("advance", 200, stream="a"))
    with pytest.raises(LateEventError, match="closed output"):
        aligner.ingest(event("partial", 170, stream="a"))


def test_overlapping_window_partial_lateness_can_be_accepted():
    aligner = WatermarkAligner(
        config(window_ms=100, hop_ms=50, required_streams=("a",), late_policy="accept")
    )
    aligner.ingest(event("advance", 200, stream="a"))
    assert aligner.ingest(event("partial", 170, stream="a")) == ()
    assert aligner.accepted_late_event_ids == ("partial",)
    assert any(item.id == "partial" for window in aligner.flush() for item in window.events)


def test_duplicate_event_is_rejected():
    aligner = WatermarkAligner(config())
    aligner.ingest(event("same", 10))
    with pytest.raises(ValidationError, match="duplicate event id"):
        aligner.ingest(event("same", 20))


def test_ingest_after_flush_is_rejected():
    aligner = WatermarkAligner(config())
    aligner.flush()
    with pytest.raises(ValidationError, match="after flush"):
        aligner.ingest(event("e", 0))


def test_flush_is_idempotent():
    aligner = WatermarkAligner(config())
    aligner.ingest(event("e", 0))
    assert aligner.flush()
    assert aligner.flush() == ()


def test_empty_flush_returns_no_windows():
    assert WatermarkAligner(config()).flush() == ()


def test_event_limit_is_enforced():
    aligner = WatermarkAligner(config(max_events_per_window=1))
    aligner.ingest(event("a", 10))
    aligner.ingest(event("b", 20))
    with pytest.raises(ValidationError, match="max_events_per_window"):
        aligner.flush()


def test_output_window_limit_blocks_far_future_flush():
    aligner = WatermarkAligner(config(max_output_windows=2))
    aligner.ingest(event("future", 1_000))
    with pytest.raises(ValidationError, match="max_output_windows"):
        aligner.flush()


def test_output_window_limit_blocks_far_future_watermark():
    aligner = WatermarkAligner(config(required_streams=("a",), max_output_windows=2))
    with pytest.raises(ValidationError, match="max_output_windows"):
        aligner.ingest(event("future", 1_000))


def test_ready_window_failure_does_not_commit_trigger_event_or_watermark():
    aligner = WatermarkAligner(config(required_streams=("r",), max_events_per_window=1))
    aligner.ingest(event("long", 100, stream="x", duration=150))
    aligner.ingest(event("overlap", 150, stream="x"))
    with pytest.raises(ValidationError, match="max_events_per_window"):
        aligner.ingest(event("trigger", 300, stream="r"))
    assert aligner.watermark_ms is None
    assert aligner.ingest(event("trigger", 50, stream="r")) == ()
    assert aligner.watermark_ms == 50


def test_event_in_gapped_window_grid_is_reported_unassigned():
    result = align_events([event("gap", 120)], config(window_ms=100, hop_ms=150))
    assert result.unassigned_event_ids == ("gap",)
    assert all(item.id != "gap" for window in result.windows for item in window.events)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Event("", "a", "video", 0),
        lambda: Event("e", "", "video", 0),
        lambda: Event("e", "a", "", 0),
        lambda: Event("e", "a", "video", float("inf")),
        lambda: Event("e", "a", "video", 0, -1),
        lambda: Event("e", "a", "video", 1e308, 1e308),
        lambda: Event("e", "a", "video", 0, data=[]),
        lambda: Event("e", "a", "video", 0, data={"score": float("nan")}),
        lambda: Event("bad\ud800id", "a", "video", 0),
        lambda: Event("bad\x00id", "a", "video", 0),
    ],
)
def test_direct_event_validation(factory):
    with pytest.raises(ValidationError):
        factory()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: config(window_ms=0),
        lambda: config(hop_ms=-1),
        lambda: config(allowed_lateness_ms=-1),
        lambda: config(late_policy="wait"),
        lambda: config(max_events_per_window=0),
        lambda: config(max_output_windows=0),
        lambda: config(origin_ms=float("inf")),
        lambda: config(allowed_lateness_ms=float("nan")),
        lambda: config(offsets_ms={"a": float("inf")}),
        lambda: config(expected_cadence_ms={"a": 0}),
        lambda: config(gap_factor=1),
        lambda: config(required_streams=("a", "a")),
        lambda: config(max_output_windows=10**400),
    ],
)
def test_direct_config_validation(factory):
    with pytest.raises(ValidationError):
        factory()


def test_gap_detection_finds_start_to_start_violation():
    events = [event("a", 0), event("b", 100), event("c", 300)]
    gaps = detect_gaps(events, config(expected_cadence_ms={"a": 100}, gap_factor=1.5))
    assert len(gaps) == 1
    assert gaps[0].to_dict() == {
        "stream": "a",
        "start_ms": 200,
        "end_ms": 300,
        "observed_ms": 200,
        "expected_ms": 100,
    }


def test_aligner_snapshots_config_mappings():
    offsets = {"a": 10.0}
    aligner = WatermarkAligner(config(offsets_ms=offsets))
    offsets["a"] = 999.0
    aligner.ingest(event("e", 0))
    assert aligner.flush()[0].events[0].timestamp_ms == 10.0


def test_aligner_snapshots_nested_event_data():
    data = {"labels": ["original"]}
    aligner = WatermarkAligner(config())
    aligner.ingest(Event("e", "a", "video", 0, data=data))
    data["labels"].append("changed")
    assert aligner.flush()[0].events[0].data == {"labels": ("original",)}


def test_gap_at_exact_threshold_is_not_reported():
    events = [event("a", 0), event("b", 150)]
    assert detect_gaps(events, config(expected_cadence_ms={"a": 100}, gap_factor=1.5)) == ()


def test_unconfigured_stream_does_not_produce_gap():
    events = [event("a", 0, stream="other"), event("b", 1_000, stream="other")]
    assert detect_gaps(events, config(expected_cadence_ms={"a": 100})) == ()
