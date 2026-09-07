"""CLI for validation, offline alignment, arrival replay, and demos."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from stream_quilt import __version__
from stream_quilt.aligner import WatermarkAligner, align_events, detect_gaps
from stream_quilt.benchmark import benchmark_alignment, write_benchmark
from stream_quilt.cloudevents import load_cloudevents
from stream_quilt.demo import demo_config_payload, demo_event_payloads
from stream_quilt.errors import OutputError, StreamQuiltError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events
from stream_quilt.joins import join_streams
from stream_quilt.models import AlignedWindow, AlignmentConfig, AlignmentResult, Event
from stream_quilt.recovery import resume_events
from stream_quilt.report import write_report_bundle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stream-quilt",
        description="Align multimodal event streams with explicit clocks and watermarks.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate", help="validate config JSON and event JSONL")
    validate.add_argument("config", type=Path)
    validate.add_argument("events", type=Path)
    validate.add_argument("--input-format", choices=("native", "cloudevents"), default="native")

    align = commands.add_parser("align", help="offline alignment independent of arrival order")
    _add_run_arguments(align)

    replay = commands.add_parser("replay", help="replay JSONL in arrival order through watermarks")
    _add_run_arguments(replay)

    resume = commands.add_parser("resume", help="resume file replay with atomic SQLite outputs")
    resume.add_argument("config", type=Path)
    resume.add_argument("events", type=Path)
    resume.add_argument("--database", required=True, type=Path)
    resume.add_argument("--batch-size", type=int, default=100)
    resume.add_argument("--max-new-events", type=int)
    resume.add_argument("--input-format", choices=("native", "cloudevents"), default="native")

    join = commands.add_parser(
        "join", help="pair events from two streams after deterministic offline alignment"
    )
    join.add_argument("config", type=Path)
    join.add_argument("events", type=Path)
    join.add_argument("left_stream")
    join.add_argument("right_stream")
    join.add_argument("--max-delta-ms", type=float, default=None)
    join.add_argument("--output", type=Path, default=Path("join.json"))
    join.add_argument("--input-format", choices=("native", "cloudevents"), default="native")

    demo = commands.add_parser("demo", help="run a built-in three-stream example")
    demo.add_argument("--output", type=Path, default=Path("demo-output"))
    demo.add_argument("--write-input", action="store_true")

    benchmark = commands.add_parser(
        "benchmark", help="compare offline sorting and watermark replay on a generated workload"
    )
    benchmark.add_argument("--events", type=int, default=3_000)
    benchmark.add_argument("--streams", type=int, default=3)
    benchmark.add_argument("--repeats", type=int, default=5)
    benchmark.add_argument("--warmups", type=int, default=1)
    benchmark.add_argument("--output", type=Path, default=Path("benchmark.json"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            config = load_config(args.config)
            events = _load_events(args.events, args.input_format)
            align_events(events, config)
            print(
                f"valid: {len(events)} events, window={config.window_ms:g} ms, "
                f"hop={config.hop_ms:g} ms"
            )
            return 0
        if args.command in {"align", "replay"}:
            config = load_config(args.config)
            events = _load_events(args.events, args.input_format)
            result = (
                align_events(events, config) if args.command == "align" else _replay(events, config)
            )
            paths = write_report_bundle(result, config, args.output)
            _print_summary(result, paths)
            return 0
        if args.command == "resume":
            point = resume_events(
                _load_events(args.events, args.input_format),
                load_config(args.config),
                args.database,
                batch_size=args.batch_size,
                max_new_events=args.max_new_events,
            )
            print(
                json.dumps(
                    {
                        "generation": point.generation,
                        "position": point.position,
                        "windows": point.checkpoint.next_index,
                        "closed": point.checkpoint.closed,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "join":
            config = load_config(args.config)
            events = _load_events(args.events, args.input_format)
            result = align_events(events, config)
            joined = join_streams(
                result,
                args.left_stream,
                args.right_stream,
                max_delta_ms=args.max_delta_ms,
            )
            try:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(
                    json.dumps(joined.to_dict(), indent=2, allow_nan=False) + "\n",
                    encoding="utf-8",
                    newline="\n",
                )
            except OSError as exc:
                raise OutputError(f"cannot write join output to {args.output}: {exc}") from exc
            print(
                f"joined {len(joined.pairs)} pairs from {args.left_stream} and {args.right_stream}"
            )
            print(f"  {'result':<20} {args.output}")
            return 0
        if args.command == "benchmark":
            benchmark = benchmark_alignment(
                event_count=args.events,
                stream_count=args.streams,
                repeats=args.repeats,
                warmups=args.warmups,
            )
            path = write_benchmark(benchmark, args.output)
            print(
                f"benchmarked {benchmark.event_count} events across "
                f"{benchmark.stream_count} streams "
                f"(equivalent outputs: {str(benchmark.equivalent_outputs).lower()})"
            )
            for mode in benchmark.modes:
                print(
                    f"  {mode.mode:<20} median={mode.median_runtime_ms:.3f} ms "
                    f"throughput={mode.median_events_per_second:.0f} events/s"
                )
            print(f"  {'result':<20} {path}")
            return 0
        if args.command == "demo":
            config = config_from_dict(demo_config_payload())
            events = tuple(event_from_dict(item) for item in demo_event_payloads())
            result = _replay(events, config)
            paths = write_report_bundle(result, config, args.output)
            if args.write_input:
                try:
                    args.output.mkdir(parents=True, exist_ok=True)
                    config_path = args.output / "config.json"
                    event_path = args.output / "events.jsonl"
                    config_path.write_text(
                        json.dumps(demo_config_payload(), indent=2) + "\n",
                        encoding="utf-8",
                        newline="\n",
                    )
                    event_path.write_text(
                        "\n".join(json.dumps(item) for item in demo_event_payloads()) + "\n",
                        encoding="utf-8",
                        newline="\n",
                    )
                except OSError as exc:
                    raise OutputError(f"cannot write demo inputs to {args.output}: {exc}") from exc
                paths.update({"config": config_path, "events": event_path})
            _print_summary(result, paths)
            return 0
    except StreamQuiltError as exc:
        print(f"stream-quilt: error: {exc}", file=sys.stderr)
        return 2
    return 2


def _add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("config", type=Path)
    parser.add_argument("events", type=Path)
    parser.add_argument("--output", type=Path, default=Path("stream-quilt-output"))
    parser.add_argument("--input-format", choices=("native", "cloudevents"), default="native")


def _load_events(path: Path, input_format: str) -> tuple[Event, ...]:
    return load_cloudevents(path) if input_format == "cloudevents" else load_events(path)


def _replay(events: tuple[Event, ...], config: AlignmentConfig) -> AlignmentResult:
    aligner = WatermarkAligner(config)
    windows: list[AlignedWindow] = []
    for event in events:
        windows.extend(aligner.ingest(event))
    windows.extend(aligner.flush())
    normalized = tuple(event.shifted(_stream_offset(aligner.config, event)) for event in events)
    return AlignmentResult(
        windows=tuple(windows),
        gaps=detect_gaps(normalized, aligner.config),
        dropped_event_ids=aligner.dropped_event_ids,
        accepted_late_event_ids=aligner.accepted_late_event_ids,
        unassigned_event_ids=aligner.unassigned_event_ids,
    )


def _stream_offset(config: AlignmentConfig, event: Event) -> float:
    drift = config.clock_drifts.get(event.stream)
    if drift is not None:
        return drift.offset_at(event.timestamp_ms)
    return config.offsets_ms.get(event.stream, 0.0)


def _print_summary(result: AlignmentResult, paths: dict[str, Path]) -> None:
    event_ids = {event.id for window in result.windows for event in window.events}
    incomplete = sum(not window.complete for window in result.windows)
    print(
        f"aligned {len(event_ids)} events into {len(result.windows)} windows "
        f"({incomplete} incomplete, {len(result.gaps)} gaps, "
        f"{len(result.dropped_event_ids)} dropped, "
        f"{len(result.accepted_late_event_ids)} accepted late, "
        f"{len(result.unassigned_event_ids)} unassigned)"
    )
    for label, path in paths.items():
        print(f"  {label:<12} {path}")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
