"""Expected failures exposed by the public API and CLI."""


class StreamQuiltError(Exception):
    """Base class for recoverable Stream Quilt failures."""


class ValidationError(StreamQuiltError):
    """Raised for malformed configuration or events."""


class LateEventError(StreamQuiltError):
    """Raised when configured to reject data behind the current watermark."""


class OutputError(StreamQuiltError):
    """Raised when a report bundle cannot be written."""
