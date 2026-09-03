from __future__ import annotations

import json
from pathlib import Path

import pytest

from stream_quilt import __version__
from stream_quilt.aligner import align_events
from stream_quilt.cli import main
from stream_quilt.demo import demo_config_payload, demo_event_payloads
from stream_quilt.io import config_from_dict, event_from_dict
from stream_quilt.report import render_html, write_report_bundle


def test_cli_reports_package_version(capsys):
    with pytest.raises(SystemExit) as error:
        main(["--version"])
    assert error.value.code == 0
    assert capsys.readouterr().out == f"stream-quilt {__version__}\n"


@pytest.fixture
def demo_result():
    config = config_from_dict(demo_config_payload())
    events = tuple(event_from_dict(item) for item in demo_event_payloads())
    return config, align_events(events, config)


def test_report_bundle_contains_alignment_data(tmp_path, demo_result):
    config, result = demo_result
    paths = write_report_bundle(result, config, tmp_path)
    document = json.loads(paths["alignment"].read_text(encoding="utf-8"))
    assert len(document["windows"]) == 5
    assert document["gaps"][0]["stream"] == "camera"
    assert paths["report"].stat().st_size > 2_000


def test_report_escapes_stream_and_event_ids():
    config = config_from_dict({"window_ms": 100, "hop_ms": 100})
    event = event_from_dict(
        {
            "id": "<img src=x>",
            "stream": "<script>",
            "modality": "text",
            "timestamp_ms": 0,
        }
    )
    report = render_html(align_events([event], config), config)
    assert "<script>" not in report
    assert "&lt;script&gt;" in report


def test_report_escapes_custom_modality():
    config = config_from_dict({"window_ms": 100, "hop_ms": 100})
    event = event_from_dict(
        {
            "id": "event",
            "stream": "stream",
            "modality": "</td><script>alert(1)</script>",
            "timestamp_ms": 0,
        }
    )
    report = render_html(align_events([event], config), config)
    assert "</td><script>" not in report
    assert "&lt;/td&gt;&lt;script&gt;alert(1)&lt;/script&gt;" in report


def _write_inputs(tmp_path):
    config_path = tmp_path / "config.json"
    event_path = tmp_path / "events.jsonl"
    config_path.write_text(json.dumps(demo_config_payload()), encoding="utf-8")
    event_path.write_text(
        "\n".join(json.dumps(item) for item in demo_event_payloads()) + "\n",
        encoding="utf-8",
    )
    return config_path, event_path


def test_cli_validate(tmp_path, capsys):
    config_path, event_path = _write_inputs(tmp_path)
    assert main(["validate", str(config_path), str(event_path)]) == 0
    assert "valid: 11 events" in capsys.readouterr().out


def test_cli_validate_checks_offset_overflow(tmp_path, capsys):
    config_path = tmp_path / "config.json"
    event_path = tmp_path / "events.jsonl"
    config_path.write_text(
        json.dumps(
            {
                "window_ms": 100,
                "hop_ms": 100,
                "offsets_ms": {"camera": 1e308},
            }
        ),
        encoding="utf-8",
    )
    event_path.write_text(
        json.dumps(
            {
                "id": "e",
                "stream": "camera",
                "modality": "video",
                "timestamp_ms": 1e308,
            }
        ),
        encoding="utf-8",
    )
    assert main(["validate", str(config_path), str(event_path)]) == 2
    assert "must be finite" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["align", "replay"])
def test_cli_run_writes_bundle(tmp_path, capsys, command):
    config_path, event_path = _write_inputs(tmp_path)
    output = tmp_path / "output"
    assert main([command, str(config_path), str(event_path), "--output", str(output)]) == 0
    assert (output / "alignment.json").exists()
    assert (output / "timeline.html").exists()
    assert "aligned 11 events" in capsys.readouterr().out


def test_cli_demo_writes_reproducible_inputs(tmp_path):
    output = tmp_path / "demo"
    assert main(["demo", "--output", str(output), "--write-input"]) == 0
    assert json.loads((output / "config.json").read_text())["window_ms"] == 1_000
    assert len((output / "events.jsonl").read_text().splitlines()) == 11


def test_checked_in_demo_bundle_matches_generator(tmp_path):
    generated = tmp_path / "demo"
    assert main(["demo", "--output", str(generated), "--write-input"]) == 0
    checked_in = Path(__file__).parents[1] / "examples" / "demo-output"
    for name in ("alignment.json", "timeline.html", "config.json", "events.jsonl"):
        assert (generated / name).read_bytes() == (checked_in / name).read_bytes()


def test_cli_invalid_input_returns_two(tmp_path, capsys):
    config_path, event_path = _write_inputs(tmp_path)
    config_path.write_text("[]", encoding="utf-8")
    assert main(["validate", str(config_path), str(event_path)]) == 2
    assert "stream-quilt: error" in capsys.readouterr().err


def test_cli_output_target_file_returns_two(tmp_path, capsys):
    config_path, event_path = _write_inputs(tmp_path)
    output = tmp_path / "already-a-file"
    output.write_text("keep", encoding="utf-8")
    assert main(["align", str(config_path), str(event_path), "--output", str(output)]) == 2
    assert "cannot write report bundle" in capsys.readouterr().err
    assert output.read_text(encoding="utf-8") == "keep"
