"""Reproducible offline-versus-watermark alignment benchmark."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_quilt.aligner import WatermarkAligner, align_events, detect_gaps
from stream_quilt.errors import OutputError
from stream_quilt.limits import (
    MAX_BENCHMARK_EVENT_VISITS,
    MAX_EVENTS,
    MAX_OUTPUT_WINDOWS,
    MAX_STREAMS,
)
from stream_quilt.models import AlignedWindow, AlignmentConfig, AlignmentResult, Event


@dataclass(frozen=True, slots=True)
class ModeBenchmark:
    """Runtime and deterministic output identity for one execution mode."""

    mode: str
    repeats: int
    median_runtime_ms: float
    p95_runtime_ms: float
    median_events_per_second: float
    output_sha256: str
    windows: int

    def __post_init__(self) -> None:
        _validate_mode(self)

    def to_dict(self) -> dict[str, Any]:
        _validate_mode(self)
        return {
            "mode": self.mode,
            "repeats": self.repeats,
            "median_runtime_ms": round(self.median_runtime_ms, 6),
            "p95_runtime_ms": round(self.p95_runtime_ms, 6),
            "median_events_per_second": round(self.median_events_per_second, 3),
            "output_sha256": self.output_sha256,
            "windows": self.windows,
        }


@dataclass(frozen=True, slots=True)
class AlignmentBenchmark:
    """Machine-readable semantic and performance comparison."""

    event_count: int
    stream_count: int
    warmups: int
    equivalent_outputs: bool
    modes: tuple[ModeBenchmark, ...]

    def __post_init__(self) -> None:
        if isinstance(self.modes, (str, bytes, bytearray)):
            raise ValueError("modes must be an iterable of ModeBenchmark records")
        try:
            mode_list: list[Any] = []
            for mode in self.modes:
                if len(mode_list) == MAX_STREAMS:
                    raise ValueError(f"modes exceeds the {MAX_STREAMS}-item limit")
                mode_list.append(mode)
        except TypeError as exc:
            raise ValueError("modes must be iterable") from exc
        modes = tuple(mode_list)
        object.__setattr__(self, "modes", modes)
        _validate_benchmark(self)

    def to_dict(self) -> dict[str, Any]:
        _validate_benchmark(self)
        return {
            "schema_version": 1,
            "workload": {
                "generator": "round-robin-monotonic-v1",
                "event_count": self.event_count,
                "stream_count": self.stream_count,
            },
            "protocol": {
                "clock": "perf_counter_ns",
                "warmups": self.warmups,
                "runtime_scope": "alignment only; generation and serialization excluded",
                "semantic_check": "SHA-256 of canonical AlignmentResult JSON",
            },
            "environment": {
                "python": platform.python_version(),
                "implementation": platform.python_implementation(),
                "platform": platform.platform(),
                "executable_bits": 64 if sys.maxsize > 2**32 else 32,
            },
            "equivalent_outputs": self.equivalent_outputs,
            "modes": [mode.to_dict() for mode in self.modes],
        }


def benchmark_alignment(
    *,
    event_count: int = 3_000,
    stream_count: int = 3,
    repeats: int = 5,
    warmups: int = 1,
    clock_ns: Callable[[], int] = time.perf_counter_ns,
) -> AlignmentBenchmark:
    """Compare finite offline sorting with append-only watermark replay."""

    for label, value, minimum in (
        ("event_count", event_count, 1),
        ("stream_count", stream_count, 1),
        ("repeats", repeats, 1),
        ("warmups", warmups, 0),
    ):
        maximum = (
            10_000
            if label in {"repeats", "warmups"}
            else MAX_EVENTS
            if label == "event_count"
            else MAX_STREAMS
        )
        _bounded_count(value, label, minimum=minimum, maximum=maximum)
    if stream_count > event_count:
        raise ValueError("stream_count must not exceed event_count")
    work = event_count * (repeats + warmups) * 2
    if work > MAX_BENCHMARK_EVENT_VISITS:
        raise ValueError(
            f"benchmark work exceeds the {MAX_BENCHMARK_EVENT_VISITS}-event-visit limit"
        )
    events, config = _workload(event_count, stream_count)
    runners: tuple[tuple[str, Callable[[], AlignmentResult]], ...] = (
        ("offline-sort", lambda: align_events(events, config)),
        ("watermark-replay", lambda: _replay(events, config)),
    )
    modes: list[ModeBenchmark] = []
    digests: list[str] = []
    for name, runner in runners:
        for _ in range(warmups):
            runner()
        durations: list[float] = []
        results: list[AlignmentResult] = []
        for _ in range(repeats):
            started = clock_ns()
            result = runner()
            ended = clock_ns()
            elapsed = (ended - started) / 1_000_000
            if elapsed < 0 or not math.isfinite(elapsed):
                raise ValueError("benchmark clock must be monotonic and finite")
            durations.append(elapsed)
            results.append(result)
        result_digests = {_digest(result) for result in results}
        if len(result_digests) != 1:
            raise AssertionError(f"mode {name} produced non-deterministic output")
        digest = next(iter(result_digests))
        digests.append(digest)
        median = statistics.median(durations)
        throughput = event_count / (max(median, 1e-9) / 1_000)
        modes.append(
            ModeBenchmark(
                mode=name,
                repeats=repeats,
                median_runtime_ms=median,
                p95_runtime_ms=_nearest_rank(durations, 0.95),
                median_events_per_second=throughput,
                output_sha256=digest,
                windows=len(results[0].windows),
            )
        )
    return AlignmentBenchmark(
        event_count=event_count,
        stream_count=stream_count,
        warmups=warmups,
        equivalent_outputs=len(set(digests)) == 1,
        modes=tuple(modes),
    )


def write_benchmark(result: AlignmentBenchmark, path: str | Path) -> Path:
    """Write benchmark evidence as strict stable-key JSON."""

    target = Path(path)
    try:
        document = json.dumps(result.to_dict(), indent=2, sort_keys=True, allow_nan=False) + "\n"
        _atomic_write(target, document)
    except (OSError, TypeError, ValueError) as exc:
        raise OutputError(f"cannot write benchmark result to {target}: {exc}") from exc
    return target


def _workload(event_count: int, stream_count: int) -> tuple[tuple[Event, ...], AlignmentConfig]:
    streams = tuple(f"stream-{index:02}" for index in range(stream_count))
    events = tuple(
        Event(
            id=f"event-{index:08}",
            stream=streams[index % stream_count],
            modality=("video", "audio", "text")[index % 3],
            timestamp_ms=(index // stream_count) * 10.0 + (index % stream_count) / stream_count,
            data={"sequence": index},
        )
        for index in range(event_count)
    )
    return events, AlignmentConfig(
        window_ms=100.0,
        hop_ms=100.0,
        allowed_lateness_ms=0.0,
        required_streams=streams,
        max_events_per_window=max(1_000, event_count),
        max_output_windows=max(100, event_count),
    )


def _replay(events: tuple[Event, ...], config: AlignmentConfig) -> AlignmentResult:
    aligner = WatermarkAligner(config)
    windows: list[AlignedWindow] = []
    for event in events:
        windows.extend(aligner.ingest(event))
    windows.extend(aligner.flush())
    return AlignmentResult(
        windows=tuple(windows),
        gaps=detect_gaps(events, aligner.config),
        dropped_event_ids=aligner.dropped_event_ids,
        accepted_late_event_ids=aligner.accepted_late_event_ids,
        unassigned_event_ids=aligner.unassigned_event_ids,
    )


def _digest(result: AlignmentResult) -> str:
    payload = json.dumps(
        result.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def _validate_mode(value: ModeBenchmark) -> None:
    if not isinstance(value, ModeBenchmark):
        raise ValueError("modes must contain ModeBenchmark records")
    _safe_text(value.mode, "mode")
    _bounded_count(value.repeats, "repeats", minimum=1, maximum=10_000)
    _bounded_count(value.windows, "windows", minimum=0, maximum=MAX_OUTPUT_WINDOWS)
    for label, number in (
        ("median_runtime_ms", value.median_runtime_ms),
        ("p95_runtime_ms", value.p95_runtime_ms),
        ("median_events_per_second", value.median_events_per_second),
    ):
        _finite(number, label)
    if value.p95_runtime_ms < value.median_runtime_ms:
        raise ValueError("p95_runtime_ms must be >= median_runtime_ms")
    if not isinstance(value.output_sha256, str) or len(value.output_sha256) != 64:
        raise ValueError("output_sha256 must be a 64-character lowercase hexadecimal digest")
    if any(character not in "0123456789abcdef" for character in value.output_sha256):
        raise ValueError("output_sha256 must be a 64-character lowercase hexadecimal digest")


def _validate_benchmark(value: AlignmentBenchmark) -> None:
    if not isinstance(value, AlignmentBenchmark):
        raise ValueError("result must be an AlignmentBenchmark")
    _bounded_count(value.event_count, "event_count", minimum=1, maximum=MAX_EVENTS)
    _bounded_count(value.stream_count, "stream_count", minimum=1, maximum=MAX_STREAMS)
    _bounded_count(value.warmups, "warmups", minimum=0, maximum=10_000)
    if value.stream_count > value.event_count:
        raise ValueError("stream_count must not exceed event_count")
    if not isinstance(value.equivalent_outputs, bool):
        raise ValueError("equivalent_outputs must be a boolean")
    if not isinstance(value.modes, tuple) or not value.modes:
        raise ValueError("modes must be a non-empty tuple")
    for mode in value.modes:
        _validate_mode(mode)
    if len({mode.mode for mode in value.modes}) != len(value.modes):
        raise ValueError("benchmark mode names must be unique")
    if len({mode.repeats for mode in value.modes}) != 1:
        raise ValueError("all benchmark modes must use the same repeat count")
    equal_digests = len({mode.output_sha256 for mode in value.modes}) == 1
    if value.equivalent_outputs != equal_digests:
        raise ValueError("equivalent_outputs is inconsistent with output digests")


def _bounded_count(value: Any, label: str, *, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{label} must be an integer from {minimum} to {maximum}")


def _finite(value: Any, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    try:
        valid = math.isfinite(value)
    except (OverflowError, TypeError):
        valid = False
    if not valid or value < 0:
        raise ValueError(f"{label} must be finite and zero or greater")


def _safe_text(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{label} must contain valid Unicode scalar values") from exc
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{label} must not contain control characters")


def _atomic_write(target: Path, document: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", newline="\n", dir=target.parent, delete=False
        ) as handle:
            handle.write(document)
            handle.flush()
            temporary = Path(handle.name)
        temporary.replace(target)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
