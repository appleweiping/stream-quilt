from __future__ import annotations

import json

import pytest

from stream_quilt.clock import estimate_offset
from stream_quilt.demo import demo_config_payload, demo_event_payloads
from stream_quilt.errors import ValidationError
from stream_quilt.io import config_from_dict, event_from_dict, load_config, load_events


def test_demo_config_is_valid():
    config = config_from_dict(demo_config_payload())
    assert config.window_ms == 1_000
    assert config.required_streams == ("camera", "microphone", "transcript")


def test_config_allows_offsets_for_optional_streams():
    payload = demo_config_payload()
    payload["offsets_ms"]["optional-sensor"] = -12
    assert config_from_dict(payload).offsets_ms["optional-sensor"] == -12


def test_config_rejects_normalized_duplicate_mapping_key():
    payload = demo_config_payload()
    payload["offsets_ms"] = {"camera": 0, " camera ": 1}
    with pytest.raises(ValidationError, match="duplicate key after normalization"):
        config_from_dict(payload)


def test_config_rejects_unknown_field():
    payload = demo_config_payload()
    payload["widow_ms"] = 20
    with pytest.raises(ValidationError, match=r"unknown field.*widow_ms"):
        config_from_dict(payload)


@pytest.mark.parametrize("field", ["window_ms", "hop_ms", "gap_factor"])
@pytest.mark.parametrize("value", [0, -1, True, float("inf"), float("nan")])
def test_config_rejects_invalid_positive_numbers(field, value):
    payload = demo_config_payload()
    payload[field] = value
    with pytest.raises(ValidationError, match=field):
        config_from_dict(payload)


@pytest.mark.parametrize("value", [-1, True, float("inf")])
def test_config_rejects_invalid_lateness(value):
    payload = demo_config_payload()
    payload["allowed_lateness_ms"] = value
    with pytest.raises(ValidationError, match="allowed_lateness_ms"):
        config_from_dict(payload)


def test_config_rejects_duplicate_required_stream():
    payload = demo_config_payload()
    payload["required_streams"].append("camera")
    with pytest.raises(ValidationError, match="must not contain duplicates"):
        config_from_dict(payload)


@pytest.mark.parametrize("value", ["wait", None, 3, []])
def test_config_rejects_invalid_late_policy(value):
    payload = demo_config_payload()
    payload["late_policy"] = value
    with pytest.raises(ValidationError, match="late_policy"):
        config_from_dict(payload)


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_config_rejects_invalid_event_limit(value):
    payload = demo_config_payload()
    payload["max_events_per_window"] = value
    with pytest.raises(ValidationError, match="max_events_per_window"):
        config_from_dict(payload)


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_config_rejects_invalid_output_window_limit(value):
    payload = demo_config_payload()
    payload["max_output_windows"] = value
    with pytest.raises(ValidationError, match="max_output_windows"):
        config_from_dict(payload)


def test_config_parser_rejects_nonfinite_combined_window_range():
    payload = demo_config_payload()
    payload["origin_ms"] = 1e308
    payload["hop_ms"] = 1e308
    with pytest.raises(ValidationError, match="window range"):
        config_from_dict(payload)


def test_config_parser_wraps_huge_integer_as_validation_error():
    payload = demo_config_payload()
    payload["origin_ms"] = 10**400
    with pytest.raises(ValidationError, match="origin_ms"):
        config_from_dict(payload)


def test_config_parser_wraps_huge_window_count_float_product():
    payload = demo_config_payload()
    payload["hop_ms"] = 100.0
    payload["max_output_windows"] = 10**400
    with pytest.raises(ValidationError, match="window range"):
        config_from_dict(payload)


def test_config_rejects_grid_increment_below_origin_precision():
    payload = demo_config_payload()
    payload["origin_ms"] = 1_700_000_000_000.0
    payload["window_ms"] = 1e-9
    payload["hop_ms"] = 1e-9
    with pytest.raises(ValidationError, match="increments must be representable"):
        config_from_dict(payload)


def test_event_parser_trims_identifiers_and_copies_data():
    payload = {
        "id": " e1 ",
        "stream": " camera ",
        "modality": " video ",
        "timestamp_ms": 12,
        "data": {"labels": ["x"]},
    }
    event = event_from_dict(payload)
    payload["data"]["labels"].append("changed")
    assert (event.id, event.stream, event.modality) == ("e1", "camera", "video")
    assert event.data == {"labels": ["x"]}


def test_event_parser_rejects_nonfinite_nested_data():
    payload = demo_event_payloads()[0]
    payload["data"] = {"score": float("nan")}
    with pytest.raises(ValidationError, match="finite JSON"):
        event_from_dict(payload)


@pytest.mark.parametrize("field", ["id", "stream", "modality"])
def test_event_requires_nonempty_text(field):
    payload = demo_event_payloads()[0]
    payload[field] = " "
    with pytest.raises(ValidationError, match=field):
        event_from_dict(payload)


@pytest.mark.parametrize("value", ["bad\ud800label", "bad\x00label"])
def test_event_rejects_unsafe_label_text(value):
    payload = demo_event_payloads()[0]
    payload["modality"] = value
    with pytest.raises(ValidationError):
        event_from_dict(payload)


@pytest.mark.parametrize("value", [-1, True, float("nan")])
def test_event_rejects_invalid_duration(value):
    payload = demo_event_payloads()[0]
    payload["duration_ms"] = value
    with pytest.raises(ValidationError, match="duration_ms"):
        event_from_dict(payload)


def test_event_rejects_non_object_data():
    payload = demo_event_payloads()[0]
    payload["data"] = []
    with pytest.raises(ValidationError, match="data must be an object"):
        event_from_dict(payload)


def test_load_events_preserves_arrival_order_and_ignores_blank_lines(tmp_path):
    path = tmp_path / "events.jsonl"
    payloads = demo_event_payloads()[:2]
    path.write_text("\n" + "\n\n".join(json.dumps(item) for item in payloads), encoding="utf-8")
    assert [event.id for event in load_events(path)] == ["v0", "a0"]


def test_load_events_rejects_duplicate_ids(tmp_path):
    path = tmp_path / "events.jsonl"
    item = demo_event_payloads()[0]
    path.write_text(json.dumps(item) + "\n" + json.dumps(item), encoding="utf-8")
    with pytest.raises(ValidationError, match="duplicate event id"):
        load_events(path)


def test_load_events_reports_line_for_invalid_json(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text("\n{bad", encoding="utf-8")
    with pytest.raises(ValidationError, match="line 2"):
        load_events(path)


def test_load_events_rejects_duplicate_json_key(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text(
        '{"id":"a","id":"b","stream":"s","modality":"text","timestamp_ms":0}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="duplicate object key"):
        load_events(path)


def test_load_events_rejects_json_nan(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text(
        '{"id":"a","stream":"s","modality":"text","timestamp_ms":0,"data":{"score":NaN}}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="non-finite number"):
        load_events(path)


def test_load_config_round_trip(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(demo_config_payload()), encoding="utf-8")
    assert load_config(path).hop_ms == 500


def test_load_config_reports_missing_file(tmp_path):
    with pytest.raises(ValidationError, match="cannot read config"):
        load_config(tmp_path / "missing.json")


def test_clock_offset_single_anchor():
    estimate = estimate_offset([(1_000, 970)])
    assert estimate.offset_ms == 30
    assert estimate.median_absolute_deviation_ms == 0


def test_clock_offset_uses_median_against_outlier():
    estimate = estimate_offset([(1_000, 980), (2_000, 1_979), (3_000, 2_500)])
    assert estimate.offset_ms == 21
    assert estimate.max_residual_ms == 479


def test_clock_offset_even_anchor_count():
    assert estimate_offset([(100, 90), (200, 188)]).offset_ms == 11


def test_clock_offset_requires_anchor():
    with pytest.raises(ValidationError, match="at least one"):
        estimate_offset([])


def test_clock_offset_rejects_overflowing_derived_difference():
    with pytest.raises(ValidationError, match="non-finite offset"):
        estimate_offset([(1e308, -1e308)])


@pytest.mark.parametrize("pair", [(1,), (1, "x"), (1, float("inf"))])
def test_clock_offset_rejects_malformed_anchor(pair):
    with pytest.raises(ValidationError, match="clock anchor"):
        estimate_offset([pair])
