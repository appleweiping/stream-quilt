from __future__ import annotations

import pytest

from stream_quilt.aligner import WatermarkAligner, align_events
from stream_quilt.errors import ValidationError
from stream_quilt.interval_index import (
    WindowIntervalIndex,
    _decompose,
    event_horizon,
    overlaps,
)
from stream_quilt.models import AlignmentConfig, Event, RetentionPolicy


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


def build_index(setup, events):
    index = WindowIntervalIndex(
        origin_ms=setup.origin_ms,
        window_ms=setup.window_ms,
        hop_ms=setup.hop_ms,
        max_windows=setup.max_output_windows,
    )
    for item in events:
        index.insert(item)
    return index


def scan(events, setup, index):
    start = setup.origin_ms + index * setup.hop_ms
    end = start + setup.window_ms
    return sorted(item.id for item in events if overlaps(item, start, end))


# --- interval index -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("window_ms", "hop_ms", "origin_ms"),
    [
        (100.0, 100.0, 0.0),
        (100.0, 50.0, 0.0),
        (100.0, 150.0, 0.0),
        (100.0, 25.0, -60.0),
        (0.5, 0.1, 0.1),
        (7.0, 3.0, 1_700_000_000_000.0),
    ],
)
def test_index_membership_matches_a_full_scan(window_ms, hop_ms, origin_ms):
    setup = config(window_ms=window_ms, hop_ms=hop_ms, origin_ms=origin_ms, max_output_windows=40)
    events = [
        event("point-origin", origin_ms),
        event("point-boundary", origin_ms + hop_ms),
        event("point-inside", origin_ms + window_ms / 3),
        event("point-late", origin_ms + 11 * hop_ms + window_ms / 7),
        event("span-short", origin_ms + window_ms / 4, duration=window_ms / 2),
        event("span-exact", origin_ms, duration=window_ms),
        event("span-long", origin_ms + hop_ms / 2, duration=window_ms * 13),
        event("span-tail", origin_ms + 9 * hop_ms, duration=hop_ms * 4),
    ]
    index = build_index(setup, events)
    for window in range(setup.max_output_windows):
        assert sorted(item.id for item in index.overlapping(window)) == scan(events, setup, window)


def test_index_returns_a_long_event_exactly_once_per_window():
    setup = config(window_ms=100, hop_ms=50, max_output_windows=64)
    long_event = event("span", 25, duration=1_400)
    index = build_index(setup, [long_event])
    for window in range(setup.max_output_windows):
        matched = index.overlapping(window)
        assert len(matched) == len({item.id for item in matched})
        assert bool(matched) == bool(scan([long_event], setup, window))


def test_index_skips_events_that_no_window_can_cover():
    # hop_ms > window_ms leaves uncovered intervals; nothing is indexed for them.
    setup = config(window_ms=100, hop_ms=150, max_output_windows=10)
    index = build_index(setup, [event("gap", 120)])
    assert len(index) == 1
    assert all(index.overlapping(window) == () for window in range(10))


def test_index_ignores_events_beyond_the_buildable_grid():
    setup = config(window_ms=100, hop_ms=100, max_output_windows=3)
    index = build_index(setup, [event("far", 10_000)])
    assert all(index.overlapping(window) == () for window in range(3))
    # The far event is still live: it expires no earlier than the end of the grid.
    index.prune(3)
    assert len(index) == 1


def test_prune_expires_only_events_behind_the_open_frontier():
    setup = config(window_ms=100, hop_ms=100, max_output_windows=10)
    events = [event("w0", 10), event("w1", 110), event("span", 50, duration=250)]
    index = build_index(setup, events)
    index.prune(1)
    assert sorted(item.id for item in index.overlapping(1)) == ["span", "w1"]
    index.prune(3)
    assert len(index) == 0


def test_index_horizon_reports_the_furthest_live_event():
    setup = config()
    index = build_index(setup, [event("a", 10), event("b", 20, duration=45)])
    assert index.horizon() == 65
    index.clear()
    assert len(index) == 0


@pytest.mark.parametrize(
    ("first", "last"),
    [(0, 0), (0, 1), (1, 2), (0, 3), (3, 12), (5, 5), (0, 999), (7, 1_000_000)],
)
def test_decomposition_covers_a_range_exactly_and_logarithmically(first, last):
    span = last - first + 1
    nodes = list(_decompose(first, last))
    covered = []
    for level, block in nodes:
        covered.extend(range(block << level, (block + 1) << level))
    assert sorted(covered) == list(range(first, last + 1))
    assert len(covered) == len(set(covered))
    # A per-window bucket layout would need `span` entries for one long event.
    assert len(nodes) <= 2 * span.bit_length()


def test_empty_decomposition_yields_no_nodes():
    assert list(_decompose(4, 3)) == []


def test_event_horizon_matches_the_overlap_predicate():
    point = event("point", 100)
    span = event("span", 100, duration=50)
    assert event_horizon(point) > 100
    assert not overlaps(point, 100.0000001, 200)
    assert event_horizon(span) == 150


def test_window_queries_touch_only_matching_events():
    # Nothing can close until the required stream arrives, so the buffer grows to
    # `count` events and one arrival then closes `count` windows in a single batch.
    count = 200
    aligner = WatermarkAligner(
        config(required_streams=("r",), max_events_per_window=4, max_output_windows=400)
    )
    for index in range(count):
        assert aligner.ingest(event(f"x{index}", index * 100, stream="x")) == ()
    buffered = len(aligner._index)
    assert buffered == count
    windows = aligner.ingest(event("trigger", count * 100, stream="r"))
    assert len(windows) == count
    assert sum(len(window.events) for window in windows) == count

    # A builder that filters the buffer inspects every buffered event for every
    # window it emits. The index inspects only the events it returns.
    linear_scan_cost = len(windows) * buffered
    assert linear_scan_cost == 40_000
    assert aligner._index.events_examined == count


def test_indexed_alignment_agrees_with_a_full_scan_of_every_window():
    setup = config(window_ms=100, hop_ms=40, max_output_windows=60)
    events = [
        event("a", 0),
        event("b", 100, stream="b"),
        event("c", 39.999999, stream="c"),
        event("long", 20, duration=520),
        event("tail", 500, duration=10),
    ]
    result = align_events(events, setup)
    for window in result.windows:
        expected = sorted(
            item.id for item in events if overlaps(item, window.start_ms, window.end_ms)
        )
        assert sorted(item.id for item in window.events) == expected


# --- retention ------------------------------------------------------------------------


def drain(aligner, events):
    windows = []
    for item in events:
        windows.extend(aligner.ingest(item))
    return windows


def test_default_retention_keeps_every_identity():
    aligner = WatermarkAligner(config(required_streams=("a",), max_output_windows=1_000))
    drain(aligner, [event(f"a{step}", step * 100, stream="a") for step in range(500)])
    assert aligner.released_event_count == 0
    assert aligner.retained_event_count == 500


def test_retention_bounds_identity_state_over_a_long_run():
    aligner = WatermarkAligner(
        config(required_streams=("a",), max_output_windows=1_000),
        retention=RetentionPolicy(horizon_ms=200),
    )
    drain(aligner, [event(f"a{step}", step * 100, stream="a") for step in range(500)])
    assert aligner.released_event_count >= 495
    assert aligner.retained_event_count <= 5


def test_retention_does_not_change_reported_output():
    def run(retention):
        aligner = WatermarkAligner(
            config(window_ms=100, hop_ms=50, required_streams=("a",), late_policy="accept"),
            retention=retention,
        )
        windows = []
        for step in range(60):
            windows.extend(aligner.ingest(event(f"a{step}", step * 40, stream="a")))
            if step % 7 == 3:
                windows.extend(
                    aligner.ingest(event(f"x{step}", step * 40 - 30, stream="x", duration=60))
                )
        windows.extend(aligner.flush())
        return (
            tuple(window.to_dict() for window in windows),
            aligner.dropped_event_ids,
            aligner.accepted_late_event_ids,
            aligner.unassigned_event_ids,
        )

    assert run(RetentionPolicy(horizon_ms=100)) == run(None)


@pytest.mark.parametrize(("duration", "released"), [(100.0, 2), (100.5, 1)])
def test_release_boundary_is_inclusive(duration, released):
    # The frontier is `watermark - horizon_ms`. An event whose horizon lands exactly on
    # it is released; one epsilon past it is kept, and keeps everything behind it.
    aligner = WatermarkAligner(
        config(required_streams=("a",)), retention=RetentionPolicy(horizon_ms=100)
    )
    drain(
        aligner,
        [
            event("a0", 0, stream="a"),
            event("edge", 0, stream="x", duration=duration),
            event("a2", 200, stream="a"),
        ],
    )
    assert aligner.watermark_ms == 200
    assert aligner.released_event_count == released


def test_released_late_event_keeps_its_reported_id():
    aligner = WatermarkAligner(
        config(required_streams=("a",), late_policy="accept"),
        retention=RetentionPolicy(horizon_ms=100),
    )
    drain(aligner, [event("a0", 0, stream="a"), event("a3", 300, stream="a")])
    assert aligner.ingest(event("late", 250, stream="x", duration=100)) == ()
    assert aligner.accepted_late_event_ids == ("late",)
    assert aligner.retained_event_count == 2
    drain(aligner, [event("a6", 600, stream="a")])
    # The identity record is gone, but the reported ID survives it.
    assert aligner.released_event_count == 3
    assert aligner.accepted_late_event_ids == ("late",)
    assert aligner.retained_event_count == 2


def test_released_dropped_event_keeps_its_reported_id():
    aligner = WatermarkAligner(
        config(required_streams=("a",), late_policy="drop"),
        retention=RetentionPolicy(horizon_ms=100),
    )
    drain(aligner, [event("a0", 0, stream="a"), event("a3", 300, stream="a")])
    assert aligner.ingest(event("stale", 10, stream="x")) == ()
    assert aligner.dropped_event_ids == ("stale",)
    drain(aligner, [event("a6", 600, stream="a")])
    assert aligner.released_event_count == 3
    assert aligner.dropped_event_ids == ("stale",)


def test_released_unassigned_event_keeps_its_reported_id():
    setup = config(window_ms=100, hop_ms=150, required_streams=("a",))
    arrivals = [
        event("gap", 120, stream="x"),
        event("a0", 0, stream="a"),
        event("a5", 750, stream="a"),
    ]
    plain = WatermarkAligner(setup)
    drain(plain, arrivals)
    plain.flush()
    bounded = WatermarkAligner(setup, retention=RetentionPolicy(horizon_ms=100))
    drain(bounded, arrivals)
    assert bounded.released_event_count == 2
    bounded.flush()
    # `gap` fell in an uncovered interval; its classification was finalized at release.
    assert bounded.unassigned_event_ids == plain.unassigned_event_ids == ("gap",)


def test_retention_never_releases_an_event_a_future_window_could_include():
    aligner = WatermarkAligner(
        config(window_ms=100, hop_ms=50, required_streams=("a",), max_output_windows=200),
        retention=RetentionPolicy(horizon_ms=100),
    )
    aligner.ingest(event("span", 0, stream="x", duration=900))
    for step in range(1, 60):
        aligner.ingest(event(f"a{step}", step * 50, stream="a"))
        # Every event any future window could still return must still be tracked.
        for window_index in range(aligner._next_index, 200):
            for item in aligner._index.overlapping(window_index):
                assert item.id in aligner._seen_ids


def test_retention_ceiling_fails_loudly_instead_of_forgetting_ids():
    aligner = WatermarkAligner(
        config(required_streams=("a",), late_policy="drop", max_output_windows=1_000),
        retention=RetentionPolicy(horizon_ms=100, max_tracked_events=3),
    )
    drain(
        aligner,
        [
            event("a0", 0, stream="a"),
            event("a5", 500, stream="a"),
            event("d1", 10, stream="x"),
            event("a9", 900, stream="a"),
            event("d2", 10, stream="y"),
        ],
    )
    with pytest.raises(ValidationError, match="max_tracked_events"):
        aligner.ingest(event("a13", 1_300, stream="a"))
    assert aligner.dropped_event_ids == ("d1", "d2")


def test_retention_horizon_shorter_than_the_window_is_refused():
    with pytest.raises(ValidationError, match="at least window_ms"):
        WatermarkAligner(config(window_ms=100), retention=RetentionPolicy(horizon_ms=99.9))
    accepted = WatermarkAligner(config(window_ms=100), retention=RetentionPolicy(horizon_ms=100))
    assert accepted.retention.horizon_ms == 100


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RetentionPolicy(horizon_ms=-1),
        lambda: RetentionPolicy(horizon_ms=float("nan")),
        lambda: RetentionPolicy(horizon_ms="200"),
        lambda: RetentionPolicy(horizon_ms=True),
        lambda: RetentionPolicy(horizon_ms=10**400),
        lambda: RetentionPolicy(max_tracked_events=0),
        lambda: RetentionPolicy(max_tracked_events=10**400),
        lambda: WatermarkAligner(config(), retention="soon"),
    ],
)
def test_retention_validation(factory):
    with pytest.raises(ValidationError):
        factory()
