"""Stream Quilt public API."""

from stream_quilt.aligner import WatermarkAligner, align_events
from stream_quilt.clock import OffsetEstimate, estimate_offset
from stream_quilt.errors import LateEventError, OutputError, StreamQuiltError, ValidationError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events
from stream_quilt.models import AlignedWindow, AlignmentConfig, AlignmentResult, Event, Gap

__all__ = [
    "AlignedWindow",
    "AlignmentConfig",
    "AlignmentResult",
    "Event",
    "Gap",
    "LateEventError",
    "OffsetEstimate",
    "OutputError",
    "StreamQuiltError",
    "ValidationError",
    "WatermarkAligner",
    "align_events",
    "config_from_dict",
    "estimate_offset",
    "event_from_dict",
    "load_config",
    "load_events",
]

__version__ = "0.1.0"
