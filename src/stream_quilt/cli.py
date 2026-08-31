"""CLI for validation, offline alignment, arrival replay, and demos."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from stream_quilt.aligner import WatermarkAligner, align_events, detect_gaps
from stream_quilt.demo import demo_config_payload, demo_event_payloads
from stream_quilt.errors import OutputError, StreamQuiltError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events
from stream_quilt.models import AlignmentConfig, AlignmentResult, Event
from stream_quilt.report import write_report_bundle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="stream-quilt",
        description="Align multimodal event streams with explicit clocks and watermarks.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate", help="validate config JSON and event JSONL")
    validate.add_argument("config", type=Path)
    validate.add_argument("events", type=Path)

    align = commands.add_parser("align", help="offline alignment independent of arrival order")
    _add_run_arguments(align)

    replay = commands.add_parser("replay", help="replay JSONL in arrival order through watermarks")
    _add_run_arguments(replay)

    demo = commands.add_parser("demo", help="run a built-in three-stream example")
    demo.add_argument("--output", type=Path, default=Path("demo-output"))
    demo.add_argument("--write-input", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            config = load_config(args.config)
            events = load_events(args.events)
            align_events(events, config)
            print(
                f"valid: {len(events)} events, window={config.window_ms:g} ms, "
                f"hop={config.hop_ms:g} ms"
            )
            return 0
        if args.command in {"align", "replay"}:
            config = load_config(args.config)
            events = load_events(args.events)
            result = (
                align_events(events, config) if args.command == "align" else _replay(events, config)
            )
            paths = write_report_bundle(result, config, args.output)
            _print_summary(result, paths)
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


def _replay(events: tuple[Event, ...], config: AlignmentConfig) -> AlignmentResult:
    aligner = WatermarkAligner(config)
    windows = []
    for event in events:
        windows.extend(aligner.ingest(event))
    windows.extend(aligner.flush())
    normalized = tuple(
        event.shifted(aligner.config.offsets_ms.get(event.stream, 0.0)) for event in events
    )
    return AlignmentResult(
        windows=tuple(windows),
        gaps=detect_gaps(normalized, aligner.config),
        dropped_event_ids=aligner.dropped_event_ids,
        accepted_late_event_ids=aligner.accepted_late_event_ids,
        unassigned_event_ids=aligner.unassigned_event_ids,
    )


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
