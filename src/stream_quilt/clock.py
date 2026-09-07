"""Robust constant-offset and affine clock-drift estimation from matched anchors."""

from __future__ import annotations

import math
import statistics
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from stream_quilt.errors import ValidationError
from stream_quilt.limits import MAX_DRIFT_ANCHORS, MAX_DRIFT_RATE_PPM, MAX_EVENTS
from stream_quilt.models import ClockDrift

MIN_DRIFT_ANCHORS = 5
"""Smallest anchor count at which the robustness claim of a drift fit means anything.

One bad anchor takes part in ``n - 1`` of the ``n * (n - 1) / 2`` pairwise slopes, a
fraction ``2 / n``. The median of the slope multiset is decided by the clean majority only
while that fraction stays below one half, which is ``n > 4``. Two anchors are the
degenerate case: one slope, no residual, and no way to tell a mismatched pair from a real
rate.

The count alone is sufficient only when the anchors sit at distinct observed instants;
:func:`estimate_drift` also checks the general condition on the pairs it can actually use.
"""


@dataclass(frozen=True, slots=True)
class OffsetEstimate:
    """Offset to add to an observed clock and residual diagnostics."""

    offset_ms: float
    anchors_used: int
    median_absolute_deviation_ms: float
    max_residual_ms: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "offset_ms", _finite(self.offset_ms, "offset estimate offset_ms"))
        object.__setattr__(
            self,
            "anchors_used",
            _integer_between(self.anchors_used, "anchors_used", 1, MAX_EVENTS),
        )
        median_residual = _finite(
            self.median_absolute_deviation_ms,
            "offset estimate median_absolute_deviation_ms",
        )
        max_residual = _finite(self.max_residual_ms, "offset estimate max_residual_ms")
        if median_residual < 0 or max_residual < median_residual:
            raise ValidationError(
                "offset estimate residuals must be non-negative with max_residual_ms at or "
                "above median_absolute_deviation_ms"
            )
        object.__setattr__(self, "median_absolute_deviation_ms", median_residual)
        object.__setattr__(self, "max_residual_ms", max_residual)

    def to_dict(self) -> dict[str, Any]:
        checked = _snapshot_offset_estimate(self)
        return {
            "offset_ms": checked.offset_ms,
            "anchors_used": checked.anchors_used,
            "median_absolute_deviation_ms": checked.median_absolute_deviation_ms,
            "max_residual_ms": checked.max_residual_ms,
        }


@dataclass(frozen=True, slots=True)
class DriftEstimate:
    """A fitted affine clock correction and the evidence for judging it.

    ``rate_ppm`` and ``offset_ms`` describe the same correction as :class:`ClockDrift`:
    ``offset_ms`` is the offset that applies at ``epoch_ms``, and ``rate_ppm`` is how fast
    that offset grows, in parts per million of observed elapsed time. Residuals are
    measured against exactly the correction :meth:`as_correction` produces, so an operator
    reading them is reading the error of the correction that will actually be applied.

    ``anchors_used`` and ``pairs_used`` say how much evidence stands behind the fit:
    ``pairs_used`` is the number of pairwise slopes the median was taken over, and one bad
    anchor can move that median only if it touches at least half of them.
    """

    rate_ppm: float
    offset_ms: float
    epoch_ms: float
    anchors_used: int
    pairs_used: int
    median_absolute_residual_ms: float
    max_residual_ms: float

    def __post_init__(self) -> None:
        object.__setattr__(self, "rate_ppm", _plausible_rate_ppm(self.rate_ppm))
        object.__setattr__(self, "offset_ms", _finite(self.offset_ms, "drift estimate offset_ms"))
        object.__setattr__(self, "epoch_ms", _finite(self.epoch_ms, "drift estimate epoch_ms"))
        anchors_used = _integer_between(
            self.anchors_used,
            "anchors_used",
            MIN_DRIFT_ANCHORS,
            MAX_DRIFT_ANCHORS,
        )
        max_pairs = anchors_used * (anchors_used - 1) // 2
        pairs_used = _integer_between(self.pairs_used, "pairs_used", 1, max_pairs)
        if pairs_used <= anchors_used:
            raise ValidationError(
                "drift estimate pairs_used must exceed anchors_used to support an identifiable fit"
            )
        object.__setattr__(self, "anchors_used", anchors_used)
        object.__setattr__(self, "pairs_used", pairs_used)
        median_residual = _finite(
            self.median_absolute_residual_ms, "drift estimate median_absolute_residual_ms"
        )
        max_residual = _finite(self.max_residual_ms, "drift estimate max_residual_ms")
        if median_residual < 0 or max_residual < median_residual:
            raise ValidationError(
                "drift estimate residuals must be non-negative with max_residual_ms at or "
                "above median_absolute_residual_ms"
            )
        object.__setattr__(self, "median_absolute_residual_ms", median_residual)
        object.__setattr__(self, "max_residual_ms", max_residual)

    def as_correction(self) -> ClockDrift:
        """Return the correction to install for one stream in ``clock_drifts``."""

        checked = _snapshot_drift_estimate(self)
        return ClockDrift(
            rate_ppm=checked.rate_ppm,
            offset_ms=checked.offset_ms,
            epoch_ms=checked.epoch_ms,
        )

    def to_dict(self) -> dict[str, Any]:
        checked = _snapshot_drift_estimate(self)
        return {
            "rate_ppm": round(checked.rate_ppm, 6),
            "offset_ms": round(checked.offset_ms, 6),
            "epoch_ms": round(checked.epoch_ms, 6),
            "anchors_used": checked.anchors_used,
            "pairs_used": checked.pairs_used,
            "median_absolute_residual_ms": round(checked.median_absolute_residual_ms, 6),
            "max_residual_ms": round(checked.max_residual_ms, 6),
        }


def estimate_offset(anchors: Iterable[tuple[float, float]]) -> OffsetEstimate:
    """Estimate ``reference - observed`` with a median robust to outliers.

    Each tuple is ``(reference_timestamp_ms, observed_timestamp_ms)``. The estimate is a
    constant, so it cannot describe a clock whose rate is wrong. Use :func:`estimate_drift`
    when the residuals grow with time instead of scattering around zero.
    """

    pairs = _bounded_anchors(anchors, MAX_EVENTS, "offset estimation")
    if not pairs:
        raise ValidationError("at least one clock anchor is required")
    offsets = [offset for _, offset in _anchor_samples(pairs)]
    estimate = statistics.median(offsets)
    residuals = [abs(value - estimate) for value in offsets]
    if not math.isfinite(estimate) or any(not math.isfinite(value) for value in residuals):
        raise ValidationError("clock anchors produce non-finite diagnostics")
    return OffsetEstimate(
        offset_ms=estimate,
        anchors_used=len(offsets),
        median_absolute_deviation_ms=statistics.median(residuals),
        max_residual_ms=max(residuals),
    )


def estimate_drift(anchors: Iterable[tuple[float, float]]) -> DriftEstimate:
    """Fit ``reference - observed`` as a line in observed time, robustly.

    Each tuple is ``(reference_timestamp_ms, observed_timestamp_ms)``. A constant offset
    cannot correct a clock that runs fast or slow: the error it leaves grows without bound
    over a long capture. This estimates an offset *and* a rate.

    **Method.** Theil-Sen. The rate is the median of the slopes of every anchor pair with
    distinct observed timestamps, and the offset is the median of the anchor offsets once
    that rate is removed. Both stages are medians, so the estimator keeps a breakdown point
    near 29% of the anchors instead of the zero of a least-squares fit, where one badly
    matched pair moves the line by an unbounded amount. Nothing iterates to convergence, so
    there is no starting point, tolerance, or iteration cap to tune.

    **Determinism.** Anchors are validated, reduced to ``(observed, offset)`` samples, and
    sorted before anything is computed, so any permutation of the same anchors produces the
    identical sequence of floating-point operations and the identical result. Residuals are
    computed with the same arithmetic :meth:`ClockDrift.offset_at` uses, so the reported
    error is the error of the applied correction.

    **Assumptions.** The rate is constant over the anchor span; a clock stepped mid-capture
    is two clocks and has to be fitted in two pieces. Anchors are matched observations of
    one instant rather than merely nearby events. The correction is a pure timeline map:
    durations are not scaled, exactly as constant offsets do not scale them.

    **Identifiability.** Fewer than :data:`MIN_DRIFT_ANCHORS` anchors are refused, as are
    anchor sets in which one anchor takes part in half or more of the usable pairs. In both
    cases a single bad anchor can decide the fit and no residual would reveal it. Refusing
    is deliberate: reporting a confident-looking two-point line is the failure this guards
    against. Fall back to :func:`estimate_offset`, which needs one anchor and claims only a
    constant.

    **Plausibility.** A fit outside ``MAX_DRIFT_RATE_PPM`` is refused rather than reported.
    """

    pairs = _bounded_anchors(anchors, MAX_DRIFT_ANCHORS, "drift estimation")
    if len(pairs) < MIN_DRIFT_ANCHORS:
        raise ValidationError(
            f"drift estimation requires at least {MIN_DRIFT_ANCHORS} matched clock anchors "
            f"(got {len(pairs)}); below that one bad anchor decides the median slope and no "
            "residual reveals it, so use estimate_offset() for a constant offset instead"
        )
    samples = sorted(_anchor_samples(pairs))
    usable_pairs, most_shared = _pair_census(samples)
    if usable_pairs == 0:
        raise ValidationError(
            "clock anchors must cover at least two distinct observed timestamps; anchors at "
            "a single instant fix an offset but no rate"
        )
    if 2 * most_shared >= usable_pairs:
        raise ValidationError(
            f"clock anchors are not identifiable: one anchor takes part in {most_shared} of "
            f"the {usable_pairs} usable anchor pairs, so a single bad anchor can decide the "
            "median slope; spread the anchors over more distinct observed instants"
        )
    slopes = [
        (later_offset - earlier_offset) / (later_observed - earlier_observed)
        for index, (earlier_observed, earlier_offset) in enumerate(samples)
        for later_observed, later_offset in samples[index + 1 :]
        if later_observed != earlier_observed
    ]
    rate_ppm = _plausible_rate_ppm(statistics.median(slopes) * 1e6)
    epoch_ms = statistics.median([observed for observed, _ in samples])
    rate = rate_ppm * 1e-6
    detrended = [offset - rate * (observed - epoch_ms) for observed, offset in samples]
    offset_ms = statistics.median(detrended)
    residuals = [abs(value - offset_ms) for value in detrended]
    # Every field is re-checked by DriftEstimate, so an offset or residual that overflowed
    # to a non-finite value is refused there rather than reported as a fit.
    return DriftEstimate(
        rate_ppm=rate_ppm,
        offset_ms=offset_ms,
        epoch_ms=epoch_ms,
        anchors_used=len(samples),
        pairs_used=len(slopes),
        median_absolute_residual_ms=statistics.median(residuals),
        max_residual_ms=max(residuals),
    )


def _anchor_samples(pairs: list[Any]) -> list[tuple[float, float]]:
    """Validate matched anchors and reduce them to ``(observed_ms, offset_ms)`` samples."""

    samples: list[tuple[float, float]] = []
    for index, pair in enumerate(pairs):
        try:
            iterator = iter(pair)
        except TypeError as exc:
            raise ValidationError(f"clock anchor {index} must contain two timestamps") from exc
        values: list[Any] = []
        for _ in range(3):
            try:
                values.append(next(iterator))
            except StopIteration:
                break
        if len(values) != 2:
            raise ValidationError(f"clock anchor {index} must contain two timestamps")
        reference, observed = values
        for label, value in (("reference", reference), ("observed", observed)):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValidationError(f"clock anchor {index} {label} must be numeric")
            try:
                finite = math.isfinite(value)
            except (OverflowError, TypeError):
                finite = False
            if not finite:
                raise ValidationError(f"clock anchor {index} {label} must be finite")
        offset = float(reference) - float(observed)
        if not math.isfinite(offset):
            raise ValidationError(f"clock anchor {index} produces a non-finite offset")
        samples.append((float(observed), offset))
    return samples


def _bounded_anchors(anchors: Iterable[Any], maximum: int, operation: str) -> list[Any]:
    """Materialize no more than the documented amount of caller-owned input."""

    if isinstance(anchors, (str, bytes, bytearray)):
        raise ValidationError("clock anchors must be an iterable of timestamp pairs")
    try:
        iterator = iter(anchors)
    except TypeError as exc:
        raise ValidationError("clock anchors must be iterable") from exc
    pairs: list[Any] = []
    for pair in iterator:
        if len(pairs) == maximum:
            detail = (
                "; the pairwise slope set grows quadratically"
                if maximum == MAX_DRIFT_ANCHORS
                else ""
            )
            raise ValidationError(f"{operation} accepts at most {maximum} clock anchors{detail}")
        pairs.append(pair)
    return pairs


def _pair_census(samples: list[tuple[float, float]]) -> tuple[int, int]:
    """Return the usable pair count and the pairs touching the most connected anchor.

    Two anchors observed at the same instant yield no slope, so that pair is unusable.
    Every usable pair touching an anchor at instant ``v`` joins it to one of the
    ``len(samples) - multiplicity(v)`` anchors elsewhere, so the worst case over anchors is
    set by the least repeated instant.
    """

    total = len(samples)
    multiplicity = Counter(observed for observed, _ in samples)
    tied = sum(count * (count - 1) // 2 for count in multiplicity.values())
    return total * (total - 1) // 2 - tied, total - min(multiplicity.values())


def _plausible_rate_ppm(value: Any) -> float:
    number = _finite(value, "drift estimate rate_ppm")
    if abs(number) > MAX_DRIFT_RATE_PPM:
        raise ValidationError(
            f"estimated drift rate {number:g} ppm is outside the +/-{MAX_DRIFT_RATE_PPM:g} "
            "ppm plausibility bound; a clock that far from nominal is far more likely bad "
            "anchor data than a real rate, so the fit is refused rather than reported"
        )
    return number


def _finite(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be a number")
    try:
        finite = math.isfinite(value)
    except (OverflowError, TypeError):
        finite = False
    if not finite:
        raise ValidationError(f"{name} must be finite")
    return float(value)


def _integer_between(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValidationError(f"estimate {name} must be an integer from {minimum} to {maximum}")
    return value


def _snapshot_offset_estimate(value: OffsetEstimate) -> OffsetEstimate:
    """Revalidate offset evidence before publication."""

    return OffsetEstimate(
        offset_ms=value.offset_ms,
        anchors_used=value.anchors_used,
        median_absolute_deviation_ms=value.median_absolute_deviation_ms,
        max_residual_ms=value.max_residual_ms,
    )


def _snapshot_drift_estimate(value: DriftEstimate) -> DriftEstimate:
    """Revalidate drift evidence before publication or correction creation."""

    return DriftEstimate(
        rate_ppm=value.rate_ppm,
        offset_ms=value.offset_ms,
        epoch_ms=value.epoch_ms,
        anchors_used=value.anchors_used,
        pairs_used=value.pairs_used,
        median_absolute_residual_ms=value.median_absolute_residual_ms,
        max_residual_ms=value.max_residual_ms,
    )
