from __future__ import annotations

import json
from pathlib import Path

import pytest

from stream_quilt.aligner import align_events
from stream_quilt.cli import main
from stream_quilt.errors import ValidationError
from stream_quilt.joins import JoinedPair, StreamJoin, join_streams
from stream_quilt.models import AlignmentConfig, Event


def _config() -> AlignmentConfig:
    return AlignmentConfig(window_ms=100, hop_ms=50, max_output_windows=20)


def test_join_deduplicates_events_repeated_by_overlapping_windows() -> None:
    result = align_events(
        [
            Event("left", "camera", "video", 10, 150),
            Event("right", "microphone", "audio", 20),
        ],
        _config(),
    )

    joined = join_streams(result, "camera", "microphone", max_delta_ms=10)

    assert joined.left_event_count == 1
    assert joined.right_event_count == 1
    assert joined.comparisons == 1
    assert len(joined.pairs) == 1
    assert joined.pairs[0].delta_ms == 10
    assert joined.pairs[0].window_indexes == (0,)


def test_join_tolerance_is_inclusive_and_none_returns_all_pairs() -> None:
    result = align_events(
        [
            Event("l1", "left", "video", 0),
            Event("l2", "left", "video", 80),
            Event("r1", "right", "audio", 10),
        ],
        AlignmentConfig(window_ms=100, hop_ms=100),
    )

    limited = join_streams(result, "left", "right", max_delta_ms=10)
    unbounded = join_streams(result, "left", "right")
    assert [pair.left_event_id for pair in limited.pairs] == ["l1"]
    assert len(unbounded.pairs) == 2
    assert unbounded.to_dict()["max_delta_ms"] is None


def test_join_models_are_sorted_and_serializable() -> None:
    pair = JoinedPair("right", "left", 2, (3, 1))
    assert pair.window_indexes == (1, 3)
    joined = StreamJoin("left", "right", 5, 1, 1, 1, (pair,))
    assert joined.to_dict()["pairs"][0]["delta_ms"] == 2


@pytest.mark.parametrize(
    ("left", "right", "tolerance"),
    [("left", "left", 1), ("", "right", 1), ("left", "right", -1)],
)
def test_join_rejects_invalid_arguments(left: str, right: str, tolerance: float) -> None:
    result = align_events([Event("e", "left", "video", 0)], AlignmentConfig(100, 100))
    with pytest.raises(ValidationError):
        join_streams(result, left, right, max_delta_ms=tolerance)


def test_join_rejects_non_alignment_result() -> None:
    with pytest.raises(ValidationError, match="AlignmentResult"):
        join_streams("bad", "left", "right")  # type: ignore[arg-type]


def test_cli_join_writes_pair_report(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "config.json"
    events_path = tmp_path / "events.jsonl"
    output_path = tmp_path / "join.json"
    config_path.write_text(json.dumps({"window_ms": 100, "hop_ms": 100}), encoding="utf-8")
    events_path.write_text(
        "\n".join(
            json.dumps(item)
            for item in (
                {"id": "camera-1", "stream": "camera", "modality": "video", "timestamp_ms": 10},
                {
                    "id": "mic-1",
                    "stream": "microphone",
                    "modality": "audio",
                    "timestamp_ms": 20,
                },
            )
        )
        + "\n",
        encoding="utf-8",
    )

    assert (
        main(
            [
                "join",
                str(config_path),
                str(events_path),
                "camera",
                "microphone",
                "--max-delta-ms",
                "10",
                "--output",
                str(output_path),
            ]
        )
        == 0
    )
    assert json.loads(output_path.read_text(encoding="utf-8"))["pair_count"] == 1
    assert "joined 1 pairs" in capsys.readouterr().out
