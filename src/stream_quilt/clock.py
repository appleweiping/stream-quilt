"""Robust constant clock-offset estimation from matched anchors."""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable
from dataclasses import dataclass

from stream_quilt.errors import ValidationError


@dataclass(frozen=True, slots=True)
class OffsetEstimate:
    """Offset to add to an observed clock and residual diagnostics."""

    offset_ms: float
    anchors_used: int
    median_absolute_deviation_ms: float
    max_residual_ms: float


def estimate_offset(anchors: Iterable[tuple[float, float]]) -> OffsetEstimate:
    """Estimate ``reference - observed`` with a median robust to outliers.

    Each tuple is ``(reference_timestamp_ms, observed_timestamp_ms)``.
    """

    pairs = list(anchors)
    if not pairs:
        raise ValidationError("at least one clock anchor is required")
    offsets: list[float] = []
    for index, pair in enumerate(pairs):
        try:
            pair_length = len(pair)
        except TypeError as exc:
            raise ValidationError(f"clock anchor {index} must contain two timestamps") from exc
        if pair_length != 2:
            raise ValidationError(f"clock anchor {index} must contain two timestamps")
        reference, observed = pair
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
        offsets.append(offset)
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
