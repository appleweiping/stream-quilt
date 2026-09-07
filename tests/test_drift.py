from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace

import pytest

from stream_quilt import (
    AlignmentConfig,
    ClockDrift,
    DriftEstimate,
    Event,
    OffsetEstimate,
    WatermarkAligner,
    align_events,
    estimate_drift,
    estimate_offset,
)
from stream_quilt.clock import MIN_DRIFT_ANCHORS
from stream_quilt.errors import ValidationError
from stream_quilt.io import config_from_dict
from stream_quilt.limits import MAX_DRIFT_ANCHORS, MAX_DRIFT_RATE_PPM, MAX_MAPPING_ENTRIES

SPAN = tuple(float(step * 100_000) for step in range(9))


def anchors(rate_ppm, offset_ms, instants=SPAN):
    """Matched ``(reference, observed)`` anchors for a clock with a known affine error."""

    return [(observed + offset_ms + rate_ppm * 1e-6 * observed, observed) for observed in instants]


def least_squares_ppm(pairs):
    """Slope of the non-robust fit the estimator deliberately does not use."""

    samples = [(observed, reference - observed) for reference, observed in pairs]
    mean_x = sum(observed for observed, _ in samples) / len(samples)
    mean_y = sum(offset for _, offset in samples) / len(samples)
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in samples)
    variance = sum((x - mean_x) ** 2 for x, _ in samples)
    return covariance / variance * 1e6


def estimate(**overrides):
    values = {
        "rate_ppm": 250.0,
        "offset_ms": 40.0,
        "epoch_ms": 400_000.0,
        "anchors_used": 9,
        "pairs_used": 36,
        "median_absolute_residual_ms": 0.5,
        "max_residual_ms": 2.0,
    }
    values.update(overrides)
    return DriftEstimate(**values)


def offset_estimate(**overrides):
    values = {
        "offset_ms": 10.0,
        "anchors_used": 3,
        "median_absolute_deviation_ms": 0.5,
        "max_residual_ms": 2.0,
    }
    values.update(overrides)
    return OffsetEstimate(**values)


# --- fitting --------------------------------------------------------------------------


def test_clean_linear_drift_is_recovered_within_tolerance():
    fit = estimate_drift(anchors(250.0, 40.0))
    assert (fit.anchors_used, fit.pairs_used) == (9, 36)
    assert fit.rate_ppm == pytest.approx(250.0, abs=1e-9)
    correction = fit.as_correction()
    for observed in SPAN:
        assert correction.offset_at(observed) == pytest.approx(40.0 + 250e-6 * observed, abs=1e-6)
    assert fit.max_residual_ms < 1e-6


def test_drift_fit_survives_bounded_anchor_jitter():
    jitter = (0.3, -0.4, 0.1, 0.5, -0.2, 0.4, -0.5, 0.2, -0.1)
    noisy = [
        (reference + shake, observed)
        for (reference, observed), shake in zip(anchors(120.0, 25.0), jitter, strict=True)
    ]
    fit = estimate_drift(noisy)
    assert fit.rate_ppm == pytest.approx(120.0, abs=1.0)
    assert fit.as_correction().offset_at(0.0) == pytest.approx(25.0, abs=0.5)
    assert fit.max_residual_ms <= max(abs(shake) for shake in jitter) * 2


def test_a_single_bad_anchor_does_not_swing_the_fit():
    clean = anchors(250.0, 40.0)
    corrupted = list(clean)
    corrupted[3] = (corrupted[3][0] + 500.0, corrupted[3][1])
    reference_fit = estimate_drift(clean)
    fit = estimate_drift(corrupted)

    assert fit.rate_ppm == reference_fit.rate_ppm
    assert fit.offset_ms == reference_fit.offset_ms
    # The outlier is reported rather than absorbed: the median residual stays at zero while
    # the maximum names the full size of the mismatch.
    assert fit.median_absolute_residual_ms == 0.0
    assert fit.max_residual_ms == pytest.approx(500.0)
    # A least-squares fit over the same anchors moves; that is the failure mode avoided.
    assert abs(least_squares_ppm(corrupted) - least_squares_ppm(clean)) > 1.0


def test_reordered_anchors_produce_an_identical_fit():
    corrupted = anchors(250.0, 40.0)
    corrupted[3] = (corrupted[3][0] + 500.0, corrupted[3][1])
    baseline = estimate_drift(corrupted)
    orderings = (
        list(reversed(corrupted)),
        corrupted[4:] + corrupted[:4],
        sorted(corrupted, key=lambda pair: pair[0]),
        sorted(corrupted, key=lambda pair: -pair[1]),
        [corrupted[position] for position in (7, 2, 0, 5, 8, 1, 4, 6, 3)],
        tuple(corrupted),
        iter(list(corrupted)),
    )
    for ordering in orderings:
        candidate = estimate_drift(ordering)
        assert candidate == baseline
        assert candidate.to_dict() == baseline.to_dict()


def test_drift_fit_reports_every_field_an_operator_needs():
    payload = estimate_drift(anchors(250.0, 40.0)).to_dict()
    assert set(payload) == {
        "rate_ppm",
        "offset_ms",
        "epoch_ms",
        "anchors_used",
        "pairs_used",
        "median_absolute_residual_ms",
        "max_residual_ms",
    }
    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
    assert estimate(rate_ppm=249.99999994).to_dict()["rate_ppm"] == 250.0


# --- identifiability ------------------------------------------------------------------


@pytest.mark.parametrize("count", [0, 1, 2, 3, 4])
def test_too_few_anchors_are_refused(count):
    with pytest.raises(ValidationError, match=f"at least {MIN_DRIFT_ANCHORS} matched clock"):
        estimate_drift(anchors(250.0, 40.0, SPAN[:count]))


def test_two_anchor_refusal_names_the_constant_offset_fallback():
    with pytest.raises(ValidationError, match="estimate_offset"):
        estimate_drift(anchors(250.0, 40.0, SPAN[:2]))
    fallback = estimate_offset(anchors(250.0, 40.0, SPAN[:2]))
    assert fallback.anchors_used == 2


def test_anchors_at_a_single_instant_fix_no_rate():
    with pytest.raises(ValidationError, match="two distinct observed timestamps"):
        estimate_drift([(40.0 + position, 1_000.0) for position in range(MIN_DRIFT_ANCHORS)])


def test_clustered_anchors_are_refused_as_unidentifiable():
    clustered = (0.0, 0.0, 0.0, 100_000.0, 200_000.0)
    with pytest.raises(ValidationError, match="not identifiable"):
        estimate_drift(anchors(250.0, 40.0, clustered))


def test_repeated_instants_are_accepted_when_the_pair_majority_survives():
    repeated = (0.0, 0.0, 100_000.0, 200_000.0, 300_000.0, 400_000.0)
    fit = estimate_drift(anchors(250.0, 40.0, repeated))
    assert fit.anchors_used == 6
    assert fit.pairs_used == 14
    assert fit.rate_ppm == pytest.approx(250.0, abs=1e-9)


def test_anchor_count_ceiling_is_enforced():
    too_many = [(float(step) + 1.0, float(step)) for step in range(MAX_DRIFT_ANCHORS + 1)]
    with pytest.raises(ValidationError, match=f"at most {MAX_DRIFT_ANCHORS} clock anchors"):
        estimate_drift(too_many)


def test_anchor_generators_stop_at_the_resource_ceiling(monkeypatch):
    consumed = {"offset": 0, "drift": 0}

    def anchors_forever(kind):
        index = 0
        while True:
            consumed[kind] += 1
            yield (float(index + 1), float(index))
            index += 1

    monkeypatch.setattr("stream_quilt.clock.MAX_EVENTS", 2)
    with pytest.raises(ValidationError, match="at most 2 clock anchors"):
        estimate_offset(anchors_forever("offset"))
    assert consumed["offset"] == 3

    monkeypatch.setattr("stream_quilt.clock.MAX_DRIFT_ANCHORS", 5)
    with pytest.raises(ValidationError, match="at most 5 clock anchors"):
        estimate_drift(anchors_forever("drift"))
    assert consumed["drift"] == 6


def test_anchor_pair_validation_never_trusts_len_or_consumes_past_three_items():
    class MisleadingPair:
        def __init__(self):
            self.consumed = 0

        def __len__(self):
            raise AssertionError("anchor validation must not trust __len__")

        def __iter__(self):
            while True:
                self.consumed += 1
                yield self.consumed

    pair = MisleadingPair()
    with pytest.raises(ValidationError, match="two timestamps"):
        estimate_offset([pair])  # type: ignore[list-item]
    assert pair.consumed == 3


@pytest.mark.parametrize(
    "bad", [(1.0,), (1.0, "x"), (1.0, float("nan")), 7, (1.0, True), (10**400, 0.0)]
)
def test_drift_rejects_malformed_anchors(bad):
    pairs = anchors(250.0, 40.0)
    pairs[2] = bad
    with pytest.raises(ValidationError, match="clock anchor 2"):
        estimate_drift(pairs)


def test_constant_offset_refuses_a_non_finite_median():
    with pytest.raises(ValidationError, match="non-finite diagnostics"):
        estimate_offset([(1.7e308, 0.0), (1.7e308, 0.0)])


# --- plausibility ---------------------------------------------------------------------


def test_half_speed_clock_is_refused_as_implausible():
    with pytest.raises(ValidationError, match="plausibility bound"):
        estimate_drift([(2.0 * observed, observed) for observed in SPAN])


def test_rate_at_the_bound_is_accepted_and_beyond_it_is_refused():
    assert estimate_drift(anchors(MAX_DRIFT_RATE_PPM, 0.0)).rate_ppm == MAX_DRIFT_RATE_PPM
    with pytest.raises(ValidationError, match="plausibility bound"):
        estimate_drift(anchors(MAX_DRIFT_RATE_PPM * 1.01, 0.0))


# --- the estimate as a model ----------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"rate_ppm": float("inf")}, "rate_ppm must be finite"),
        ({"rate_ppm": 10**400}, "rate_ppm must be finite"),
        ({"rate_ppm": "fast"}, "rate_ppm must be a number"),
        ({"rate_ppm": MAX_DRIFT_RATE_PPM + 1.0}, "plausibility bound"),
        ({"offset_ms": float("nan")}, "offset_ms must be finite"),
        ({"epoch_ms": None}, "epoch_ms must be a number"),
        ({"anchors_used": MIN_DRIFT_ANCHORS - 1}, "anchors_used must be an integer"),
        ({"anchors_used": True}, "anchors_used must be an integer"),
        ({"pairs_used": 0}, "pairs_used must be an integer"),
        ({"median_absolute_residual_ms": -1.0}, "residuals must be non-negative"),
        ({"max_residual_ms": 0.1}, "residuals must be non-negative"),
        ({"max_residual_ms": float("inf")}, "max_residual_ms must be finite"),
        ({"median_absolute_residual_ms": float("nan")}, "residual_ms must be finite"),
    ],
)
def test_drift_estimate_validates_direct_construction(overrides, message):
    with pytest.raises(ValidationError, match=message):
        estimate(**overrides)


def test_drift_estimate_revalidates_replacement():
    with pytest.raises(ValidationError, match="plausibility bound"):
        replace(estimate(), rate_ppm=MAX_DRIFT_RATE_PPM * 2)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"offset_ms": float("inf")}, "offset_ms must be finite"),
        ({"anchors_used": 0}, "anchors_used must be an integer"),
        ({"anchors_used": True}, "anchors_used must be an integer"),
        ({"median_absolute_deviation_ms": -1}, "residuals must be non-negative"),
        ({"max_residual_ms": 0.1}, "residuals must be non-negative"),
        ({"max_residual_ms": float("nan")}, "max_residual_ms must be finite"),
    ],
)
def test_offset_estimate_validates_direct_construction(overrides, message):
    with pytest.raises(ValidationError, match=message):
        offset_estimate(**overrides)


def test_offset_estimate_revalidates_replace_and_serializes_all_evidence():
    result = offset_estimate()
    assert result.to_dict() == {
        "offset_ms": 10.0,
        "anchors_used": 3,
        "median_absolute_deviation_ms": 0.5,
        "max_residual_ms": 2.0,
    }
    with pytest.raises(ValidationError, match="residuals"):
        replace(result, max_residual_ms=0.1)
    object.__setattr__(result, "anchors_used", 0)
    with pytest.raises(ValidationError, match="anchors_used"):
        result.to_dict()


@pytest.mark.parametrize(
    "overrides",
    [
        {"anchors_used": MAX_DRIFT_ANCHORS + 1},
        {"anchors_used": 5, "pairs_used": 5},
        {"anchors_used": 5, "pairs_used": 11},
    ],
)
def test_drift_estimate_rejects_impossible_evidence_counts(overrides):
    with pytest.raises(ValidationError, match=r"anchors_used|pairs_used"):
        estimate(**overrides)


def test_drift_estimate_public_operations_revalidate_tampered_evidence():
    result = estimate()
    object.__setattr__(result, "pairs_used", 1)
    with pytest.raises(ValidationError, match="pairs_used"):
        result.to_dict()
    with pytest.raises(ValidationError, match="pairs_used"):
        result.as_correction()


# --- the correction as a model --------------------------------------------------------


def test_zero_rate_correction_is_exactly_a_constant_offset():
    drift = ClockDrift(rate_ppm=0.0, offset_ms=-40.0, epoch_ms=1234.5)
    assert drift.offset_at(0.0) == -40.0
    assert drift.offset_at(9_999_999.0) == -40.0


def test_clock_drift_is_immutable_and_revalidates_replace():
    correction = ClockDrift(rate_ppm=10, offset_ms=2, epoch_ms=3)
    with pytest.raises(FrozenInstanceError):
        correction.rate_ppm = 20  # type: ignore[misc]
    with pytest.raises(ValidationError, match="must be within"):
        replace(correction, rate_ppm=MAX_DRIFT_RATE_PPM + 1)
    object.__setattr__(correction, "rate_ppm", MAX_DRIFT_RATE_PPM + 1)
    with pytest.raises(ValidationError, match="must be within"):
        correction.offset_at(10)
    with pytest.raises(ValidationError, match="must be within"):
        correction.to_dict()


def test_correction_grows_with_observed_time():
    drift = ClockDrift(rate_ppm=500.0, offset_ms=10.0, epoch_ms=0.0)
    assert drift.offset_at(0.0) == 10.0
    assert drift.offset_at(1_000_000.0) == pytest.approx(510.0)
    assert drift.to_dict() == {"rate_ppm": 500.0, "offset_ms": 10.0, "epoch_ms": 0.0}


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"rate_ppm": float("inf")}, "clock drift rate_ppm must be finite"),
        ({"rate_ppm": "fast"}, "clock drift rate_ppm must be a number"),
        ({"rate_ppm": -MAX_DRIFT_RATE_PPM - 1.0}, "must be within"),
        ({"offset_ms": float("nan")}, "clock drift offset_ms must be finite"),
        ({"epoch_ms": None}, "clock drift epoch_ms must be a number"),
    ],
)
def test_clock_drift_validates_direct_construction(overrides, message):
    values = {"rate_ppm": 120.0, "offset_ms": 0.0, "epoch_ms": 0.0}
    values.update(overrides)
    with pytest.raises(ValidationError, match=message):
        ClockDrift(**values)


def test_correction_rejects_a_non_finite_observation():
    with pytest.raises(ValidationError, match="observed_ms must be finite"):
        ClockDrift(rate_ppm=1.0).offset_at(float("inf"))


def test_correction_refuses_to_overflow():
    drift = ClockDrift(rate_ppm=MAX_DRIFT_RATE_PPM, offset_ms=1.797e308)
    with pytest.raises(ValidationError, match="must remain finite"):
        drift.offset_at(1.797e308)


@pytest.mark.parametrize("rate_ppm", [MAX_DRIFT_RATE_PPM, -MAX_DRIFT_RATE_PPM])
def test_correction_never_reorders_a_stream_against_itself(rate_ppm):
    drift = ClockDrift(rate_ppm=rate_ppm, offset_ms=0.0, epoch_ms=500_000.0)
    instants = [0.0, 1.0, 2.5, 1_000.0, 500_000.0, 1_000_000.0, 5_000_000.0]
    corrected = [observed + drift.offset_at(observed) for observed in instants]
    assert corrected == sorted(corrected)
    assert len(set(corrected)) == len(instants)


# --- configuration --------------------------------------------------------------------


def test_a_stream_may_not_carry_both_a_constant_offset_and_a_drift():
    with pytest.raises(ValidationError, match="both offsets_ms and clock_drifts"):
        AlignmentConfig(
            1000.0,
            1000.0,
            offsets_ms={"camera": 10.0},
            clock_drifts={"camera": ClockDrift(rate_ppm=5.0)},
        )


@pytest.mark.parametrize(
    ("drifts", "message"),
    [
        ([], "clock_drifts must be a mapping"),
        ({"camera": 5.0}, "must be a ClockDrift"),
        ({"camera": ClockDrift(1.0), " camera ": ClockDrift(2.0)}, "duplicate key"),
        ({"": ClockDrift(1.0)}, "clock_drifts key must be a non-empty string"),
    ],
)
def test_config_rejects_malformed_clock_drifts(drifts, message):
    with pytest.raises(ValidationError, match=message):
        AlignmentConfig(1000.0, 1000.0, clock_drifts=drifts)


def test_config_enforces_the_clock_drift_entry_limit():
    shared = ClockDrift(rate_ppm=1.0)
    crowded = {f"stream-{index}": shared for index in range(MAX_MAPPING_ENTRIES + 1)}
    with pytest.raises(ValidationError, match="entry limit"):
        AlignmentConfig(1000.0, 1000.0, clock_drifts=crowded)


def drift_payload(**overrides):
    payload = {
        "window_ms": 1000,
        "hop_ms": 1000,
        "required_streams": ["camera"],
        "clock_drifts": {"camera": {"rate_ppm": 120.5, "offset_ms": -40, "epoch_ms": 1000}},
    }
    payload.update(overrides)
    return payload


def test_config_json_parses_clock_drifts():
    drift = config_from_dict(drift_payload()).clock_drifts["camera"]
    assert drift.to_dict() == {"rate_ppm": 120.5, "offset_ms": -40.0, "epoch_ms": 1000.0}


def test_config_json_defaults_the_drift_offset_and_epoch():
    config = config_from_dict(drift_payload(clock_drifts={"camera": {"rate_ppm": 7}}))
    assert config.clock_drifts["camera"].to_dict() == {
        "rate_ppm": 7.0,
        "offset_ms": 0.0,
        "epoch_ms": 0.0,
    }


@pytest.mark.parametrize(
    ("drifts", "message"),
    [
        ({"camera": {"rate_ppm": 1, "rate": 2}}, "unknown field"),
        ({"camera": {}}, "rate_ppm must be a number"),
        ({"camera": 5}, "must be an object"),
        ({"camera": {"rate_ppm": 1e9}}, "must be within"),
        ({"camera": {"rate_ppm": "fast"}}, "rate_ppm must be a number"),
        ({"camera": {"rate_ppm": 1, "offset_ms": None}}, "offset_ms must be a number"),
        ({"camera": {"rate_ppm": 1, "epoch_ms": float("inf")}}, "epoch_ms must be finite"),
        ({"camera": {"rate_ppm": 1}, " camera ": {"rate_ppm": 2}}, "duplicate key"),
    ],
)
def test_config_json_rejects_malformed_clock_drifts(drifts, message):
    with pytest.raises(ValidationError, match=message):
        config_from_dict(drift_payload(clock_drifts=drifts))


def test_config_json_rejects_a_non_object_clock_drifts_field():
    with pytest.raises(ValidationError, match="clock_drifts must be an object"):
        config_from_dict(drift_payload(clock_drifts=[]))


def test_config_json_enforces_the_clock_drift_entry_limit():
    crowded = {f"s{index}": {"rate_ppm": 1} for index in range(MAX_MAPPING_ENTRIES + 1)}
    with pytest.raises(ValidationError, match="entry limit"):
        config_from_dict(drift_payload(clock_drifts=crowded))


# --- alignment ------------------------------------------------------------------------

CAPTURE = {
    "window_ms": 1000.0,
    "hop_ms": 1000.0,
    "required_streams": ("camera", "microphone"),
    "max_output_windows": 10_000,
}
CAMERA_OBSERVED = (400.0, 1_000_400.0, 2_000_400.0, 3_000_400.0)
MICROPHONE_REFERENCE = (400.0, 1_000_700.0, 2_001_000.5, 3_001_300.0)
CAPTURE_ANCHORS = tuple(
    (observed * 1.0003, observed)
    for observed in (0.0, 750_000.0, 1_500_000.0, 2_250_000.0, 3_000_000.0, 3_750_000.0)
)


def capture_events():
    return tuple(
        [
            Event(f"c{index}", "camera", "video", observed)
            for index, observed in enumerate(CAMERA_OBSERVED)
        ]
        + [
            Event(f"m{index}", "microphone", "audio", observed)
            for index, observed in enumerate(MICROPHONE_REFERENCE)
        ]
    )


def occupied(result):
    return [(window.index, window.complete) for window in result.windows if window.events]


def test_absent_and_empty_clock_drifts_align_identically():
    events = capture_events()
    without = align_events(events, AlignmentConfig(**CAPTURE)).to_dict()
    empty = align_events(events, AlignmentConfig(**CAPTURE, clock_drifts={})).to_dict()
    assert json.dumps(without, sort_keys=True) == json.dumps(empty, sort_keys=True)


def test_zero_rate_drift_reproduces_the_constant_offset_byte_for_byte():
    events = capture_events()
    constant = AlignmentConfig(**CAPTURE, offsets_ms={"camera": -40.0})
    affine = AlignmentConfig(
        **CAPTURE, clock_drifts={"camera": ClockDrift(rate_ppm=0.0, offset_ms=-40.0)}
    )
    left = json.dumps(align_events(events, constant).to_dict(), sort_keys=True)
    right = json.dumps(align_events(events, affine).to_dict(), sort_keys=True)
    assert left == right


def test_affine_correction_reunites_pairs_no_constant_offset_can():
    events = capture_events()
    fitted_offset = estimate_offset(CAPTURE_ANCHORS)
    fitted_drift = estimate_drift(CAPTURE_ANCHORS)
    assert fitted_drift.rate_ppm == pytest.approx(300.0, abs=1e-6)

    uncorrected = occupied(align_events(events, AlignmentConfig(**CAPTURE)))
    constant = occupied(
        align_events(
            events, AlignmentConfig(**CAPTURE, offsets_ms={"camera": fitted_offset.offset_ms})
        )
    )
    corrected = occupied(
        align_events(
            events,
            AlignmentConfig(**CAPTURE, clock_drifts={"camera": fitted_drift.as_correction()}),
        )
    )

    # A constant offset cannot hold both ends of a 50-minute capture: the later pairs land
    # in separate, incomplete windows however the constant is chosen.
    assert uncorrected == [
        (0, True),
        (1000, True),
        (2000, False),
        (2001, False),
        (3000, False),
        (3001, False),
    ]
    assert constant == uncorrected
    assert corrected == [(0, True), (1000, True), (2001, True), (3001, True)]


def test_watermark_replay_applies_the_same_correction():
    config = AlignmentConfig(
        **CAPTURE, clock_drifts={"camera": estimate_drift(CAPTURE_ANCHORS).as_correction()}
    )
    aligner = WatermarkAligner(config)
    windows = []
    for event in sorted(capture_events(), key=lambda item: item.timestamp_ms):
        windows.extend(aligner.ingest(event))
    windows.extend(aligner.flush())
    assert [(window.index, window.complete) for window in windows if window.events] == [
        (0, True),
        (1000, True),
        (2001, True),
        (3001, True),
    ]
    assert aligner.config.clock_drifts["camera"].rate_ppm == pytest.approx(300.0, abs=1e-6)
