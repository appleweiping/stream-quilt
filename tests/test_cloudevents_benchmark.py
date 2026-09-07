from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path

import pytest

from stream_quilt.benchmark import (
    AlignmentBenchmark,
    ModeBenchmark,
    benchmark_alignment,
    write_benchmark,
)
from stream_quilt.cli import main
from stream_quilt.cloudevents import cloudevent_from_dict, load_cloudevents
from stream_quilt.errors import OutputError, ValidationError
from stream_quilt.limits import MAX_STREAMS


def cloud_event(**overrides):
    payload = {
        "specversion": "1.0",
        "id": "frame-7",
        "source": "/camera/front",
        "type": "com.example.video.frame",
        "time": "2026-01-02T03:04:05.250Z",
        "stream": "front-camera",
        "modality": "video",
        "durationms": 40,
        "subject": "lane",
        "data": {"objects": 3},
        "traceparent": "00-abc-def-01",
    }
    payload.update(overrides)
    return payload


def test_cloudevent_maps_identity_time_extensions_and_payload():
    event = cloudevent_from_dict(cloud_event())
    assert event.id == "ce:%2Fcamera%2Ffront:frame-7"
    assert event.stream == "front-camera"
    assert event.modality == "video"
    assert event.duration_ms == 40
    assert event.timestamp_ms == 1_767_323_045_250
    assert event.data["payload"] == {"objects": 3}
    assert event.data["extensions"] == {
        "durationms": 40,
        "modality": "video",
        "stream": "front-camera",
        "traceparent": "00-abc-def-01",
    }


def test_long_valid_source_and_id_use_a_stable_bounded_identity() -> None:
    source = "/" + "s" * 1_023
    event_id = "é" * 1_024
    first = cloudevent_from_dict(cloud_event(source=source, id=event_id))
    second = cloudevent_from_dict(cloud_event(source=source, id=event_id))
    changed = cloudevent_from_dict(cloud_event(source=source, id="è" + event_id[1:]))

    assert first.id == second.id
    assert first.id.startswith("ce-sha256:")
    assert len(first.id) < 1_024
    assert first.id != changed.id
    assert first.data["cloudevent"]["source"] == source
    assert first.data["cloudevent"]["id"] == event_id


def test_cloudevent_defaults_stream_and_modality_to_core_attributes():
    payload = cloud_event()
    for key in ("stream", "modality", "durationms"):
        payload.pop(key)
    event = cloudevent_from_dict(payload)
    assert event.stream == "/camera/front"
    assert event.modality == "com.example.video.frame"
    assert event.duration_ms == 0


def test_cloudevent_null_attributes_are_unset_but_null_data_is_payload():
    event = cloudevent_from_dict(
        cloud_event(
            stream=None,
            modality=None,
            durationms=None,
            subject=None,
            traceparent=None,
            data=None,
            data_base64=None,
        )
    )
    assert event.stream == "/camera/front"
    assert event.modality == "com.example.video.frame"
    assert event.duration_ms == 0
    assert event.data["payload"] is None
    assert "subject" not in event.data["cloudevent"]
    assert "traceparent" not in event.data.get("extensions", {})
    assert "payload_base64" not in event.data


def test_cloudevent_validates_media_type_and_absolute_dataschema():
    event = cloudevent_from_dict(
        cloud_event(
            datacontenttype='application/json; charset="utf-8"',
            dataschema="urn:example:schema:v1",
        )
    )
    assert event.data["cloudevent"]["dataschema"] == "urn:example:schema:v1"
    for override, message in (
        ({"datacontenttype": "not a media type"}, "media type"),
        ({"datacontenttype": "text/plain; bad"}, "media type"),
        ({"dataschema": "1bad:value"}, "absolute URI"),
        ({"dataschema": "https://schema.example/v1#fragment"}, "absolute URI"),
    ):
        with pytest.raises(ValidationError, match=message):
            cloudevent_from_dict(cloud_event(**override))


@pytest.mark.parametrize("missing", ["specversion", "id", "source", "type", "time"])
def test_cloudevent_requires_core_identity_and_alignment_time(missing):
    payload = cloud_event()
    payload.pop(missing)
    with pytest.raises(ValidationError, match="missing required"):
        cloudevent_from_dict(payload)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"specversion": "0.3"}, "specversion"),
        ({"time": "2026-01-02T03:04:05"}, "supported RFC 3339 subset"),
        ({"time": "not-a-time"}, "RFC 3339"),
        ({"durationms": -1}, "durationms"),
        ({"durationms": True}, "durationms"),
        ({"data_base64": "@@@", "data": None}, "both data"),
    ],
)
def test_cloudevent_rejects_unsupported_or_ambiguous_values(overrides, message):
    with pytest.raises(ValidationError, match=message):
        cloudevent_from_dict(cloud_event(**overrides))


def test_cloudevent_accepts_and_preserves_valid_base64():
    payload = cloud_event(data_base64="aGVsbG8=")
    payload.pop("data")
    event = cloudevent_from_dict(payload)
    assert event.data["payload_base64"] == "aGVsbG8="


def test_cloudevent_integer_extensions_use_the_normative_int32_range():
    low = cloudevent_from_dict(cloud_event(sequence=-(2**31)))
    high = cloudevent_from_dict(cloud_event(sequence=2**31 - 1, durationms=2**31 - 1))
    assert low.data["extensions"]["sequence"] == -(2**31)
    assert high.data["extensions"]["sequence"] == 2**31 - 1
    assert high.duration_ms == float(2**31 - 1)
    for payload in (
        cloud_event(sequence=-(2**31) - 1),
        cloud_event(sequence=2**31),
        cloud_event(durationms=2**31),
    ):
        with pytest.raises(ValidationError, match=r"CloudEvents Integer|Integer range"):
            cloudevent_from_dict(payload)


def test_cloudevent_string_extensions_allow_empty_but_reject_forbidden_unicode():
    event = cloudevent_from_dict(cloud_event(optionalnote="", **{"1note": "starts-with-digit"}))
    assert event.data["extensions"]["optionalnote"] == ""
    assert event.data["extensions"]["1note"] == "starts-with-digit"
    for value in ("bad\u0085", "bad\ufdd0", "bad\ufffe", "bad\U0001ffff"):
        with pytest.raises(ValidationError, match="forbidden by CloudEvents String"):
            cloudevent_from_dict(cloud_event(optionalnote=value))


def test_cloudevent_adapter_never_silently_trims_context_or_mapping_labels():
    for payload in (
        cloud_event(source=" /camera/front"),
        cloud_event(time=" 2026-01-02T03:04:05Z"),
        cloud_event(stream=" camera"),
        cloud_event(modality="video "),
    ):
        with pytest.raises(ValidationError):
            cloudevent_from_dict(payload)


@pytest.mark.parametrize(
    "source",
    [
        "/relative/path",
        "relative/path?query=a:b?c#fragment/ok?yes",
        "https://user:pass@[2001:db8::1]:443/path?x=1#frame",
        "https://[2001:db8::1]:/path",
        "https://[v1.fe80]:80/path",
        "https://[V1.fe80]:80/path",
        "urn:example:sensor:front",
        "#fragment-only",
    ],
)
def test_cloudevent_accepts_valid_rfc3986_uri_references(source):
    event = cloudevent_from_dict(cloud_event(source=source, stream="front"))
    assert event.data["cloudevent"]["source"] == source


@pytest.mark.parametrize(
    "source",
    [
        "https://example.test/a#one#two",
        "1bad:value/path",
        "https://[not-an-ipv6]/path",
        "https://[fe80::1%25eth0]/path",
        "https://host:port/path",
        "https://user@@host/path",
        "/bad/%escape",
        "/raw/[bracket]",
    ],
)
def test_cloudevent_rejects_invalid_rfc3986_uri_references(source):
    with pytest.raises(ValidationError, match="URI-reference"):
        cloudevent_from_dict(cloud_event(source=source))


def test_cloudevent_rejects_invalid_base64_without_data():
    payload = cloud_event(data_base64="@@@")
    payload.pop("data")
    with pytest.raises(ValidationError, match="valid base64"):
        cloudevent_from_dict(payload)


def test_cloudevent_rejects_base64_before_unbounded_decode(monkeypatch):
    monkeypatch.setattr("stream_quilt.cloudevents._MAX_BASE64_DECODED_BYTES", 3)
    payload = cloud_event(data_base64="aGVsbG8=")
    payload.pop("data")
    with pytest.raises(ValidationError, match="decoded limit"):
        cloudevent_from_dict(payload)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"time": "2026-01-02 03:04:05Z"}, "supported RFC 3339 subset"),
        ({"time": "2026-01-02T03:04:05.1234Z"}, "supported RFC 3339 subset"),
        ({"source": "source with spaces"}, "URI-reference"),
        ({"bad_name": "x"}, "attribute name"),
        ({"floatvalue": 1.25}, "CloudEvents String"),
        ({"dataschema": "/relative"}, "absolute URI"),
        ({"dataschema": "http://["}, "absolute URI"),
        ({"time": "2026-02-30T00:00:00Z"}, "RFC 3339"),
    ],
)
def test_cloudevent_enforces_documented_interoperable_subset(overrides, message):
    with pytest.raises(ValidationError, match=message):
        cloudevent_from_dict(cloud_event(**overrides))


@pytest.mark.parametrize(
    "payload",
    [
        [],
        cloud_event(time=None),
        cloud_event(time="2026-99-99T00:00:00Z"),
        cloud_event(source="bad%escape"),
        cloud_event(subject=[]),
        cloud_event(nestedextension={"x": 1}),
        cloud_event(hugeinteger=2**53),
        cloud_event(data={"score": float("nan")}),
    ],
)
def test_cloudevent_rejects_additional_malformed_envelopes(payload):
    with pytest.raises(ValidationError):
        cloudevent_from_dict(payload)


def test_cloudevent_rejects_non_string_and_decoded_oversize_base64(monkeypatch):
    payload = cloud_event(data_base64=3)
    payload.pop("data")
    with pytest.raises(ValidationError, match="base64 string"):
        cloudevent_from_dict(payload)
    monkeypatch.setattr("stream_quilt.cloudevents._MAX_BASE64_DECODED_BYTES", 4)
    payload = cloud_event(data_base64="MTIzNDU=")
    payload.pop("data")
    with pytest.raises(ValidationError, match="decoded limit"):
        cloudevent_from_dict(payload)


def test_cloudevent_loader_preserves_order_and_rejects_duplicate_identity(tmp_path):
    path = tmp_path / "events.jsonl"
    first = cloud_event(id="a")
    second = cloud_event(id="b")
    path.write_text(f"{json.dumps(first)}\n\n{json.dumps(second)}\n", encoding="utf-8")
    assert [event.data["cloudevent"]["id"] for event in load_cloudevents(path)] == ["a", "b"]
    path.write_text(f"{json.dumps(first)}\n{json.dumps(first)}\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="duplicate CloudEvent"):
        load_cloudevents(path)


def test_cloudevent_loader_rejects_duplicate_json_keys_and_missing_file(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text(
        '{"specversion":"1.0","id":"a","id":"b","source":"/s","type":"t","time":"2026-01-01T00:00:00Z"}',
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="duplicate"):
        load_cloudevents(path)
    with pytest.raises(ValidationError, match="cannot read"):
        load_cloudevents(tmp_path / "missing")


def test_cloudevent_loader_record_limit(tmp_path, monkeypatch):
    monkeypatch.setattr("stream_quilt.cloudevents.MAX_EVENTS", 1)
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps(cloud_event(id="a")) + "\n" + json.dumps(cloud_event(id="b")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="record limit"):
        load_cloudevents(path)


def test_benchmark_modes_are_semantically_equivalent_with_fake_clock():
    ticks = iter(range(0, 100_000_000, 1_000_000))
    result = benchmark_alignment(
        event_count=60,
        stream_count=3,
        repeats=2,
        warmups=0,
        clock_ns=lambda: next(ticks),
    )
    payload = result.to_dict()
    assert payload["schema_version"] == 1
    assert payload["equivalent_outputs"] is True
    assert [mode["mode"] for mode in payload["modes"]] == [
        "offline-sort",
        "watermark-replay",
    ]
    assert payload["modes"][0]["output_sha256"] == payload["modes"][1]["output_sha256"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"event_count": 0},
        {"stream_count": 0},
        {"repeats": 0},
        {"warmups": -1},
        {"event_count": True},
    ],
)
def test_benchmark_rejects_invalid_protocol(kwargs):
    with pytest.raises(ValueError):
        benchmark_alignment(**kwargs)


def test_benchmark_rejects_non_monotonic_clock():
    ticks = iter([2, 1])
    with pytest.raises(ValueError, match="monotonic"):
        benchmark_alignment(
            event_count=3,
            stream_count=1,
            repeats=1,
            warmups=0,
            clock_ns=lambda: next(ticks),
        )


def test_benchmark_runs_warmup_and_enforces_total_work():
    ticks = iter([0, 1_000_000, 2_000_000, 3_000_000])
    result = benchmark_alignment(
        event_count=3,
        stream_count=1,
        repeats=1,
        warmups=1,
        clock_ns=lambda: next(ticks),
    )
    assert result.warmups == 1
    with pytest.raises(ValueError, match="benchmark work"):
        benchmark_alignment(event_count=100_000, repeats=251, warmups=0)


def test_benchmark_writer_and_cli(tmp_path, capsys):
    result = benchmark_alignment(event_count=30, repeats=1, warmups=0)
    output = tmp_path / "nested" / "benchmark.json"
    assert write_benchmark(result, output) == output
    assert json.loads(output.read_text())["equivalent_outputs"] is True
    assert (
        main(
            [
                "benchmark",
                "--events",
                "30",
                "--repeats",
                "1",
                "--warmups",
                "0",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert "equivalent outputs: true" in capsys.readouterr().out
    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(OutputError, match="cannot write"):
        write_benchmark(result, directory)


def test_benchmark_atomic_write_cleans_staging_file_on_replace_failure(tmp_path, monkeypatch):
    result = benchmark_alignment(event_count=30, repeats=1, warmups=0)
    before = set(tmp_path.iterdir())

    def fail_replace(_self, _target):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OutputError, match="simulated replace failure"):
        write_benchmark(result, tmp_path / "benchmark.json")
    assert set(tmp_path.iterdir()) == before


def test_cli_aligns_cloudevents(tmp_path):
    origin = 1_767_323_045_000
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"window_ms": 1_000, "hop_ms": 1_000, "origin_ms": origin}),
        encoding="utf-8",
    )
    events = tmp_path / "events.jsonl"
    events.write_text(json.dumps(cloud_event()) + "\n", encoding="utf-8")
    output = tmp_path / "output"
    assert (
        main(
            [
                "align",
                str(config),
                str(events),
                "--input-format",
                "cloudevents",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    event = json.loads((output / "alignment.json").read_text())["windows"][0]["events"][0]
    assert event["data"]["cloudevent"]["id"] == "frame-7"


def test_generated_workload_runtime_guard():
    started = time.perf_counter()
    result = benchmark_alignment(event_count=600, repeats=1, warmups=0)
    assert time.perf_counter() - started < 5.0
    assert result.equivalent_outputs is True


def test_checked_in_cloudevents_example_is_directly_runnable(tmp_path):
    root = Path(__file__).parents[1]
    assert (
        main(
            [
                "align",
                str(root / "examples" / "cloudevents-config.json"),
                str(root / "examples" / "cloudevents.jsonl"),
                "--input-format",
                "cloudevents",
                "--output",
                str(tmp_path / "output"),
            ]
        )
        == 0
    )


def test_checked_in_reference_benchmark_has_current_functional_digest():
    reference_path = Path(__file__).parents[1] / "benchmarks" / "results" / "windows-python314.json"
    reference = json.loads(reference_path.read_text(encoding="utf-8"))
    ticks = iter(range(0, 100_000_000, 1_000_000))
    current = benchmark_alignment(
        event_count=3_000,
        stream_count=3,
        repeats=1,
        warmups=0,
        clock_ns=lambda: next(ticks),
    ).to_dict()
    assert reference["workload"] == current["workload"]
    assert reference["equivalent_outputs"] is True
    for saved, regenerated in zip(reference["modes"], current["modes"], strict=True):
        for key in ("mode", "output_sha256", "windows"):
            assert saved[key] == regenerated[key]
    assert reference["environment"]["implementation"] == "CPython"
    assert reference["environment"]["python"].startswith("3.14.")


def test_public_benchmark_records_reject_nonfinite_huge_and_inconsistent_values(tmp_path):
    digest = "a" * 64
    good = ModeBenchmark("offline", 1, 1, 1, 100, digest, 2)
    with pytest.raises(ValueError, match="p95"):
        ModeBenchmark("offline", 1, 2, 1, 100, digest, 2)
    with pytest.raises(ValueError, match="finite"):
        ModeBenchmark("offline", 1, float("inf"), float("inf"), 100, digest, 2)
    with pytest.raises(ValueError, match="repeats"):
        ModeBenchmark("offline", 10**100, 1, 1, 100, digest, 2)
    with pytest.raises(ValueError, match="finite"):
        ModeBenchmark("offline", 1, 10**400, 10**400, 100, digest, 2)
    with pytest.raises(ValueError, match="Unicode"):
        ModeBenchmark("bad\ud800", 1, 1, 1, 100, digest, 2)
    with pytest.raises(ValueError, match="inconsistent"):
        AlignmentBenchmark(3, 1, 0, True, (good, ModeBenchmark("live", 1, 1, 1, 100, "b" * 64, 2)))


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ModeBenchmark("", 1, 1, 1, 1, "a" * 64, 1),
        lambda: ModeBenchmark("m", 1, 1, 1, True, "a" * 64, 1),
        lambda: ModeBenchmark("m", 1, 1, 1, 1, "short", 1),
        lambda: ModeBenchmark("m", 1, 1, 1, 1, "z" * 64, 1),
        lambda: ModeBenchmark("bad\x00name", 1, 1, 1, 1, "a" * 64, 1),
    ],
)
def test_public_mode_benchmark_rejects_malformed_records(factory):
    with pytest.raises(ValueError):
        factory()


def test_public_alignment_benchmark_rejects_bad_collection_shapes():
    one = ModeBenchmark("a", 1, 1, 1, 1, "a" * 64, 1)
    two_repeats = ModeBenchmark("b", 2, 1, 1, 1, "b" * 64, 1)
    for factory in (
        lambda: AlignmentBenchmark(1, 1, 0, True, ()),
        lambda: AlignmentBenchmark(1, 1, 0, True, (one, one)),
        lambda: AlignmentBenchmark(1, 1, 0, False, (one, two_repeats)),
        lambda: AlignmentBenchmark(1, 1, 0, True, ("bad",)),
        lambda: AlignmentBenchmark(1, 2, 0, True, (one,)),
        lambda: AlignmentBenchmark(1, 1, 0, 1, (one,)),
    ):
        with pytest.raises(ValueError):
            factory()


def test_public_benchmark_records_snapshot_nested_modes_and_revalidate_replace():
    source = ModeBenchmark("offline", 1, 1, 1, 100, "a" * 64, 2)
    result = AlignmentBenchmark(3, 1, 0, True, (source,))
    object.__setattr__(source, "mode", "changed")
    assert result.modes[0].mode == "offline"
    with pytest.raises(ValueError, match="p95"):
        replace(result.modes[0], median_runtime_ms=2)
    object.__setattr__(result, "modes", result.modes * (MAX_STREAMS + 1))
    with pytest.raises(ValueError, match="item limit"):
        result.to_dict()


def test_benchmark_rejects_more_streams_than_events():
    with pytest.raises(ValueError, match="must not exceed"):
        benchmark_alignment(event_count=1, stream_count=2)
