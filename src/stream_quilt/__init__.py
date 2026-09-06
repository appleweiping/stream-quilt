"""Stream Quilt public API."""

from stream_quilt.aligner import WatermarkAligner, align_events
from stream_quilt.benchmark import AlignmentBenchmark, ModeBenchmark, benchmark_alignment
from stream_quilt.clock import DriftEstimate, OffsetEstimate, estimate_drift, estimate_offset
from stream_quilt.cloudevents import cloudevent_from_dict, load_cloudevents
from stream_quilt.errors import LateEventError, OutputError, StreamQuiltError, ValidationError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events
from stream_quilt.models import (
    AlignedWindow,
    AlignmentConfig,
    AlignmentResult,
    ClockDrift,
    Event,
    Gap,
    RetentionPolicy,
)

__all__ = [
    "AlignedWindow",
    "AlignmentBenchmark",
    "AlignmentConfig",
    "AlignmentResult",
    "ClockDrift",
    "DriftEstimate",
    "Event",
    "Gap",
    "LateEventError",
    "ModeBenchmark",
    "OffsetEstimate",
    "OutputError",
    "RetentionPolicy",
    "StreamQuiltError",
    "ValidationError",
    "WatermarkAligner",
    "align_events",
    "benchmark_alignment",
    "cloudevent_from_dict",
    "config_from_dict",
    "estimate_drift",
    "estimate_offset",
    "event_from_dict",
    "load_cloudevents",
    "load_config",
    "load_events",
]

__version__ = "0.2.0"
