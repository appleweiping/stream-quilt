"""Stream Quilt public API."""

from stream_quilt.aligner import WatermarkAligner, align_events
from stream_quilt.benchmark import AlignmentBenchmark, ModeBenchmark, benchmark_alignment
from stream_quilt.checkpoint import AlignerCheckpoint, TrackedCheckpoint, config_digest
from stream_quilt.clock import DriftEstimate, OffsetEstimate, estimate_drift, estimate_offset
from stream_quilt.cloudevents import cloudevent_from_dict, load_cloudevents
from stream_quilt.errors import LateEventError, OutputError, StreamQuiltError, ValidationError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events
from stream_quilt.joins import JoinedPair, StreamJoin, join_streams
from stream_quilt.models import (
    AlignedWindow,
    AlignmentConfig,
    AlignmentResult,
    ClockDrift,
    Event,
    Gap,
    RetentionPolicy,
)
from stream_quilt.partition import PartitionedEvents, partition_events
from stream_quilt.recovery import RecoveryConflict, RecoveryPoint, RecoveryStore, resume_events
from stream_quilt.sessions import SessionResult, SessionWindow, sessionize

__all__ = [
    "AlignedWindow",
    "AlignerCheckpoint",
    "AlignmentBenchmark",
    "AlignmentConfig",
    "AlignmentResult",
    "ClockDrift",
    "DriftEstimate",
    "Event",
    "Gap",
    "JoinedPair",
    "LateEventError",
    "ModeBenchmark",
    "OffsetEstimate",
    "OutputError",
    "PartitionedEvents",
    "RecoveryConflict",
    "RecoveryPoint",
    "RecoveryStore",
    "RetentionPolicy",
    "SessionResult",
    "SessionWindow",
    "StreamJoin",
    "StreamQuiltError",
    "TrackedCheckpoint",
    "ValidationError",
    "WatermarkAligner",
    "align_events",
    "benchmark_alignment",
    "cloudevent_from_dict",
    "config_digest",
    "config_from_dict",
    "estimate_drift",
    "estimate_offset",
    "event_from_dict",
    "join_streams",
    "load_cloudevents",
    "load_config",
    "load_events",
    "partition_events",
    "resume_events",
    "sessionize",
]

__version__ = "0.5.0"
