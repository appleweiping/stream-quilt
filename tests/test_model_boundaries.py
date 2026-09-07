from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import FrozenInstanceError, replace

import pytest

from stream_quilt.aligner import WatermarkAligner, align_events
from stream_quilt.cloudevents import cloudevent_from_dict, load_cloudevents
from stream_quilt.errors import ValidationError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events
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


def test_window_and_result_deep_snapshot_nested_records() -> None:
    source_event = Event("e", "camera", "video", 10, data={"value": "original"})
    source_window = AlignedWindow(0, 0, 100, (source_event,), ())
    source_gap = Gap("camera", 20, 30, 20, 10)
    result = AlignmentResult((source_window,), (source_gap,))

    object.__setattr__(source_event, "timestamp_ms", 90)
    object.__setattr__(source_event, "data", {"value": "changed"})
    object.__setattr__(source_window, "start_ms", 5)
    object.__setattr__(source_gap, "stream", "changed")

    assert result.windows[0].start_ms == 0
    assert result.windows[0].events[0].timestamp_ms == 10
    assert result.windows[0].events[0].data == {"value": "original"}
    assert result.gaps[0].stream == "camera"


def test_output_serialization_revalidates_tampered_nested_records() -> None:
    event = Event("e", "camera", "video", 10)
    window = AlignedWindow(0, 0, 100, (event,), ())
    gap = Gap("camera", 20, 30, 20, 10)
    result = AlignmentResult((window,), (gap,))

    object.__setattr__(result.windows[0], "events", (Event("outside", "camera", "video", 100),))
    with pytest.raises(ValidationError, match="overlap"):
        result.to_dict()

    object.__setattr__(gap, "observed_ms", 30)
    with pytest.raises(ValidationError, match="boundaries"):
        gap.to_dict()

    object.__setattr__(event, "duration_ms", -1)
    with pytest.raises(ValidationError, match="duration_ms"):
        event.to_dict()


def test_public_properties_and_operations_revalidate_tampered_records() -> None:
    event = Event("e", "camera", "video", 10)
    object.__setattr__(event, "duration_ms", -1)
    with pytest.raises(ValidationError, match="duration_ms"):
        _ = event.end_ms
    with pytest.raises(ValidationError, match="duration_ms"):
        event.shifted(1)

    window = AlignedWindow(0, 0, 100, (Event("nested", "camera", "video", 10),), ())
    object.__setattr__(window.events[0], "duration_ms", -1)
    with pytest.raises(ValidationError, match="duration_ms"):
        _ = window.complete
    with pytest.raises(ValidationError, match="duration_ms"):
        _ = window.modalities


def test_window_rejects_events_outside_bounds_and_present_missing_streams() -> None:
    before = Event("before", "camera", "video", -1)
    at_end = Event("end", "camera", "video", 100)
    with pytest.raises(ValidationError, match="overlap"):
        AlignedWindow(0, 0, 100, (before,), ())
    with pytest.raises(ValidationError, match="overlap"):
        AlignedWindow(0, 0, 100, (at_end,), ())
    with pytest.raises(ValidationError, match="missing_streams"):
        AlignedWindow(0, 0, 100, (Event("inside", "camera", "video", 10),), ("camera",))


def test_gap_rejects_diagnostics_inconsistent_with_its_boundaries() -> None:
    with pytest.raises(ValidationError, match="boundaries"):
        Gap("camera", start_ms=10, end_ms=20, observed_ms=25, expected_ms=5)
    assert Gap("camera", start_ms=10, end_ms=20, observed_ms=15, expected_ms=5)


def test_gap_accepts_relationship_at_large_timestamp_precision() -> None:
    previous = 1e15
    current = previous + 0.25
    expected = 0.1
    observed = current - previous
    gap = Gap(
        "camera",
        start_ms=previous + expected,
        end_ms=current,
        observed_ms=observed,
        expected_ms=expected,
    )
    assert gap.observed_ms == observed


def test_gap_rejects_large_absolute_contradictions_and_boundary_overflow() -> None:
    with pytest.raises(ValidationError, match="boundaries"):
        Gap(
            "camera",
            start_ms=0,
            end_ms=1e12,
            observed_ms=1e12 + 0.5,
            expected_ms=1,
        )
    with pytest.raises(ValidationError, match="boundaries"):
        Gap(
            "camera",
            start_ms=-1e308,
            end_ms=1e308,
            observed_ms=1e308,
            expected_ms=1,
        )


def test_result_rejects_conflicting_event_associations() -> None:
    event = Event("e", "camera", "video", 10)
    window = AlignedWindow(0, 0, 100, (event,), ())
    with pytest.raises(ValidationError, match="disjoint"):
        AlignmentResult((window,), (), accepted_late_event_ids=("e",), unassigned_event_ids=("e",))
    with pytest.raises(ValidationError, match="cannot appear"):
        AlignmentResult((window,), (), dropped_event_ids=("e",))
    with pytest.raises(ValidationError, match="cannot appear"):
        AlignmentResult((window,), (), unassigned_event_ids=("e",))
    with pytest.raises(ValidationError, match="must appear"):
        AlignmentResult((window,), (), accepted_late_event_ids=("missing",))


def test_result_rejects_reordered_windows_and_conflicting_event_snapshots() -> None:
    first = Event("e", "camera", "video", 10, data={"value": 1})
    changed = Event("e", "camera", "video", 110, data={"value": 2})
    first_window = AlignedWindow(0, 0, 100, (first,), ())
    changed_window = AlignedWindow(1, 100, 200, (changed,), ())
    with pytest.raises(ValidationError, match="same event snapshot"):
        AlignmentResult((first_window, changed_window), ())
    with pytest.raises(ValidationError, match="ordered"):
        AlignmentResult((changed_window, first_window), ())


def test_result_bounds_its_distinct_event_index_before_materializing(monkeypatch) -> None:
    first = AlignedWindow(0, 0, 100, (Event("a", "camera", "video", 10),), ())
    second = AlignedWindow(1, 100, 200, (Event("b", "camera", "video", 110),), ())
    monkeypatch.setattr("stream_quilt.models.MAX_EVENTS", 1)
    with pytest.raises(ValidationError, match="distinct-event limit"):
        AlignmentResult((first, second), ())


def test_event_and_output_models_revalidate_replacement() -> None:
    event = Event("e", "camera", "video", 10)
    window = AlignedWindow(0, 0, 100, (event,), ())
    gap = Gap("camera", 20, 30, 20, 10)
    result = AlignmentResult((window,), (gap,))
    with pytest.raises(ValidationError, match="duration_ms"):
        replace(event, duration_ms=-1)
    with pytest.raises(ValidationError, match="overlap"):
        replace(window, events=(Event("outside", "camera", "video", 100),))
    with pytest.raises(ValidationError, match="boundaries"):
        replace(gap, observed_ms=30)
    with pytest.raises(ValidationError, match="must appear"):
        replace(result, accepted_late_event_ids=("missing",))


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


class _UnboundedMapping(Mapping[str, object]):
    """A hostile Mapping whose reported length hides an unbounded iterator."""

    def __init__(self, prefix: dict[str, object]) -> None:
        self.prefix = prefix

    def __getitem__(self, key: str) -> object:
        if key in self.prefix:
            return self.prefix[key]
        return ""

    def __iter__(self) -> Iterator[str]:
        yield from self.prefix
        index = 0
        while True:
            yield f"extra{index}"
            index += 1

    def __len__(self) -> int:
        return len(self.prefix)


class _RepeatingMapping(Mapping[str, object]):
    """A hostile Mapping that repeats one key forever while claiming to be empty."""

    def __getitem__(self, key: str) -> object:
        return "value"

    def __iter__(self) -> Iterator[str]:
        while True:
            yield "same"

    def __len__(self) -> int:
        return 0


class _UnboundedSequence(Sequence[str]):
    """A hostile Sequence whose index iteration never raises IndexError."""

    def __init__(self) -> None:
        self.consumed = 0

    def __getitem__(self, index):
        self.consumed += 1
        return f"stream{index}"

    def __len__(self) -> int:
        return 0


def test_public_mapping_boundaries_stop_hostile_iterators(monkeypatch) -> None:
    monkeypatch.setattr("stream_quilt.models.MAX_MAPPING_ENTRIES", 2)
    with pytest.raises(ValidationError, match="entry limit"):
        Event("e", "s", "m", 0, data=_UnboundedMapping({}))
    nested = {
        "id": "e",
        "stream": "s",
        "modality": "m",
        "timestamp_ms": 0,
        "data": _UnboundedMapping({}),
    }
    with pytest.raises(ValidationError, match="entry limit"):
        event_from_dict(nested)

    monkeypatch.setattr("stream_quilt.io.MAX_MAPPING_ENTRIES", 4)
    native = _UnboundedMapping({"id": "e", "stream": "s", "modality": "m", "timestamp_ms": 0})
    with pytest.raises(ValidationError, match="entry limit"):
        event_from_dict(native)

    monkeypatch.setattr("stream_quilt.cloudevents.MAX_MAPPING_ENTRIES", 5)
    cloud = _UnboundedMapping(
        {
            "specversion": "1.0",
            "id": "e",
            "source": "/s",
            "type": "t",
            "time": "2026-01-01T00:00:00Z",
        }
    )
    with pytest.raises(ValidationError, match="attribute limit"):
        cloudevent_from_dict(cloud)


def test_mapping_limits_count_consumed_items_even_when_keys_repeat(monkeypatch) -> None:
    monkeypatch.setattr("stream_quilt.models.MAX_MAPPING_ENTRIES", 2)
    with pytest.raises(ValidationError, match="entry limit"):
        Event("e", "s", "m", 0, data=_RepeatingMapping())
    monkeypatch.setattr("stream_quilt.io.MAX_MAPPING_ENTRIES", 2)
    with pytest.raises(ValidationError, match="entry limit"):
        event_from_dict(_RepeatingMapping())
    monkeypatch.setattr("stream_quilt.cloudevents.MAX_MAPPING_ENTRIES", 2)
    with pytest.raises(ValidationError, match="attribute limit"):
        cloudevent_from_dict(_RepeatingMapping())


def test_config_sequence_stops_when_a_sequence_lies_about_its_length(monkeypatch) -> None:
    sequence = _UnboundedSequence()
    monkeypatch.setattr("stream_quilt.io.MAX_STREAMS", 2)
    with pytest.raises(ValidationError, match="item limit"):
        config_from_dict({"window_ms": 1, "hop_ms": 1, "required_streams": sequence})
    assert sequence.consumed == 3


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
