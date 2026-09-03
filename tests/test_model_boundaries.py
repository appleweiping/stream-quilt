from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from stream_quilt.aligner import WatermarkAligner, align_events
from stream_quilt.cloudevents import load_cloudevents
from stream_quilt.errors import ValidationError
from stream_quilt.io import load_config, load_events
from stream_quilt.limits import MAX_JSON_DEPTH, MAX_STREAMS, MAX_TEXT_LENGTH
from stream_quilt.models import (
    AlignedWindow,
    AlignmentConfig,
    AlignmentResult,
    Event,
    Gap,
)


def test_event_deep_freezes_data_and_to_dict_detaches_it() -> None:
    source = {"nested": {"labels": ["first"]}}
    event = Event("e", "s", "video", 0, data=source)
    source["nested"]["labels"].append("changed")
    assert event.data["nested"]["labels"] == ("first",)
    with pytest.raises(TypeError):
        event.data["new"] = True  # type: ignore[index]
    with pytest.raises(TypeError):
        event.data["nested"]["new"] = True  # type: ignore[index]
    detached = event.to_dict()
    detached["data"]["nested"]["labels"].append("detached")
    assert event.data["nested"]["labels"] == ("first",)
    with pytest.raises(FrozenInstanceError):
        event.id = "changed"  # type: ignore[misc]


def test_event_rejects_cycles_depth_huge_integer_and_oversize_data(monkeypatch) -> None:
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValidationError, match="cycles"):
        Event("e", "s", "m", 0, data=cyclic)
    nested: object = None
    for _ in range(MAX_JSON_DEPTH + 1):
        nested = [nested]
    with pytest.raises(ValidationError, match="depth"):
        Event("e", "s", "m", 0, data={"nested": nested})
    with pytest.raises(ValidationError, match="interoperable"):
        Event("e", "s", "m", 0, data={"value": 2**53})
    with pytest.raises(ValidationError, match="must be a number"):
        Event("e", "s", "m", True)
    with pytest.raises(ValidationError, match="must be finite"):
        Event("e", "s", "m", 10**400)
    with pytest.raises(ValidationError, match="non-JSON"):
        Event("e", "s", "m", 0, data={"value": object()})
    with pytest.raises(ValidationError, match="object keys"):
        Event("e", "s", "m", 0, data={"nested": {1: "bad"}})
    with pytest.raises(ValidationError, match="invalid Unicode"):
        Event("e", "s", "m", 0, data={"value": "bad\ud800"})
    cyclic_list: list[object] = []
    cyclic_list.append(cyclic_list)
    with pytest.raises(ValidationError, match="cycles"):
        Event("e", "s", "m", 0, data={"items": cyclic_list})
    monkeypatch.setattr("stream_quilt.models.MAX_JSON_NODES", 2)
    with pytest.raises(ValidationError, match="value limit"):
        Event("e", "s", "m", 0, data={"items": [1, 2]})
    monkeypatch.setattr("stream_quilt.models.MAX_JSON_NODES", 100_000)
    monkeypatch.setattr("stream_quilt.models.MAX_EVENT_DATA_BYTES", 10)
    with pytest.raises(ValidationError, match="byte limit"):
        Event("e", "s", "m", 0, data={"value": "too long"})


def test_config_snapshots_mappings_and_rechecks_replace() -> None:
    offsets = {"camera": 1.0}
    required = ["camera"]
    config = AlignmentConfig(100, 100, required_streams=required, offsets_ms=offsets)  # type: ignore[arg-type]
    offsets["camera"] = 9
    required.append("late")
    assert config.offsets_ms == {"camera": 1.0}
    assert config.required_streams == ("camera",)
    with pytest.raises(TypeError):
        config.offsets_ms["camera"] = 2  # type: ignore[index]
    with pytest.raises(ValidationError):
        replace(config, hop_ms=0)
    with pytest.raises(ValidationError, match="must be a mapping"):
        AlignmentConfig(100, 100, offsets_ms=[])  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="must be an iterable"):
        AlignmentConfig(100, 100, required_streams="camera")  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="window range"):
        AlignmentConfig(1e308, 1e308, max_output_windows=1)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Event("x" * (MAX_TEXT_LENGTH + 1), "s", "m", 0),
        lambda: AlignmentConfig(100, 100, required_streams=("s", "s")),
        lambda: AlignmentConfig(
            100, 100, required_streams=tuple(f"s{index}" for index in range(MAX_STREAMS + 1))
        ),
        lambda: AlignedWindow(True, 0, 1, (), ()),
        lambda: AlignedWindow(0, 1, 1, (), ()),
        lambda: AlignedWindow(
            0,
            0,
            1,
            (Event("e", "s", "m", 0), Event("e", "s", "m", 0)),
            (),
        ),
        lambda: Gap("s", 0, 2, 1, 1),
        lambda: Gap("s", 2, 1, 3, 1),
        lambda: AlignmentResult((), (), ("e",), ("e",), ()),
        lambda: AlignmentResult(
            (AlignedWindow(0, 0, 1, (), ()), AlignedWindow(0, 1, 2, (), ())), ()
        ),
    ],
)
def test_public_models_reject_invalid_direct_construction(factory) -> None:
    with pytest.raises(ValidationError):
        factory()


def test_window_and_result_snapshot_mutable_collections() -> None:
    event = Event("e", "s", "m", 0)
    events = [event]
    missing = ["other"]
    window = AlignedWindow(0, 0, 100, events, missing)  # type: ignore[arg-type]
    events.clear()
    missing.clear()
    result = AlignmentResult([window], [], unassigned_event_ids=["u"])  # type: ignore[arg-type]
    assert window.events == (event,)
    assert window.missing_streams == ("other",)
    assert result.windows == (window,)
    assert result.unassigned_event_ids == ("u",)


def test_shift_rejects_nonfinite_offset_and_preserves_data() -> None:
    event = Event("e", "s", "m", 1, data={"items": [1]})
    shifted = event.shifted(2)
    assert shifted.timestamp_ms == 3
    assert shifted.data == event.data
    with pytest.raises(ValidationError, match="offset_ms"):
        event.shifted(float("inf"))


def test_in_memory_alignment_event_limit(monkeypatch) -> None:
    monkeypatch.setattr("stream_quilt.aligner.MAX_EVENTS", 1)
    config = AlignmentConfig(100, 100)
    events = [Event("a", "s", "m", 0), Event("b", "s", "m", 1)]
    with pytest.raises(ValidationError, match="event limit"):
        align_events(events, config)
    aligner = WatermarkAligner(config)
    aligner.ingest(events[0])
    with pytest.raises(ValidationError, match="event limit"):
        aligner.ingest(events[1])
    with pytest.raises(ValidationError, match="iterable"):
        align_events(None, config)  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="Event records"):
        align_events(["bad"], config)  # type: ignore[list-item]
    with pytest.raises(ValidationError, match="AlignmentConfig"):
        WatermarkAligner("bad")  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="Event"):
        WatermarkAligner(config).ingest("bad")  # type: ignore[arg-type]


def test_input_files_have_preparse_byte_limits(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.json"
    events_path = tmp_path / "events.jsonl"
    cloud_path = tmp_path / "cloud.jsonl"
    config_path.write_text(" " * 11, encoding="utf-8")
    events_path.write_text(" " * 11, encoding="utf-8")
    cloud_path.write_text(" " * 11, encoding="utf-8")
    monkeypatch.setattr("stream_quilt.io.MAX_CONFIG_BYTES", 10)
    monkeypatch.setattr("stream_quilt.io.MAX_EVENT_FILE_BYTES", 10)
    monkeypatch.setattr("stream_quilt.cloudevents.MAX_EVENT_FILE_BYTES", 10)
    with pytest.raises(ValidationError, match="byte limit"):
        load_config(config_path)
    with pytest.raises(ValidationError, match="byte limit"):
        load_events(events_path)
    with pytest.raises(ValidationError, match="byte limit"):
        load_cloudevents(cloud_path)


def test_parser_collection_and_record_limits(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("stream_quilt.io.MAX_EVENTS", 1)
    path = tmp_path / "events.jsonl"
    line = '{"id":"e%s","stream":"s","modality":"m","timestamp_ms":0}'
    path.write_text((line % 1) + "\n" + (line % 2) + "\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="record limit"):
        load_events(path)

    monkeypatch.setattr("stream_quilt.io.MAX_STREAMS", 1)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"window_ms":1,"hop_ms":1,"required_streams":["a","b"]}', encoding="utf-8"
    )
    with pytest.raises(ValidationError, match="item limit"):
        load_config(config_path)
