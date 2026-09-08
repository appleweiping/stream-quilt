"""Independent wire, routing and pre-materialization checkpoint admission."""

import hashlib
import json
from dataclasses import replace

import pytest

from stream_quilt import (
    Dataflow,
    FlowCheckpoint,
    FlowLimits,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    ValidationError,
)
from stream_quilt._local_worker import _copy_record
from stream_quilt.partitioned_checkpoint import (
    LocalWorkerLimits,
    PartitionedFlowCheckpoint,
    _flow,
    _load,
    partition_for,
)
from stream_quilt.partitioned_flow import PartitionedFlowBatch, PartitionedFlowOutput


def point(limits=None):
    flow = Dataflow("wire", "1", (FlowStep("map", "map", str),))
    return PartitionedFlowCheckpoint(
        flow.identity,
        "source",
        "a" * 64,
        2,
        limits or LocalWorkerLimits(),
        0,
        0,
        False,
        tuple(FlowRuntime(flow).checkpoint() for _ in range(2)),
    )


@pytest.mark.parametrize("key", ["a", "b", "é", "汉字", "a:b", "😀"])
@pytest.mark.parametrize("workers", [1, 2, 3, 8])
def test_routing_matches_independent_fixed_byte_profile(key, workers):
    digest = hashlib.sha256(b"stream-quilt-key-route-v1\0" + key.encode()).digest()
    expected = sum(byte << (8 * (7 - i)) for i, byte in enumerate(digest[:8])) % workers
    assert partition_for(key, workers) == expected


@pytest.mark.parametrize("key", ["a\x00b", " a", "a ", "", "x" * 1025, "\ud800"])
def test_invalid_keys_are_rejected_not_normalized(key):
    with pytest.raises(ValidationError):
        partition_for(key, 2)


def test_canonical_wire_roundtrip_and_value_snapshot():
    original = point()
    wire = original.to_dict()
    restored = PartitionedFlowCheckpoint.from_json(original.to_json().encode())
    assert restored == original
    wire["shards"][0]["processed_inputs"] = 9
    assert original.next_position == 0
    assert original.to_json() == json.dumps(
        original.to_dict(), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_closed", 1),
        ("workers", True),
        ("workers", 0),
        ("waves", True),
        ("waves", 1),
        ("next_position", 1),
        ("source_id", " source"),
        ("source_digest", "X" * 64),
        ("flow_identity", []),
        ("routing", "other"),
        ("version", "2.0"),
    ],
)
def test_strict_outer_wire_fields(field, value):
    wire = point().to_dict()
    wire[field] = value
    with pytest.raises(ValidationError):
        PartitionedFlowCheckpoint.from_dict(wire)


@pytest.mark.parametrize("value", [True, 0, -1, 10**1000, float("nan"), float("inf"), "1"])
@pytest.mark.parametrize("field", ["wave_timeout", "cleanup_timeout", "max_batch_inputs"])
def test_limits_reject_bad_scalar_before_numeric_conversion(field, value):
    with pytest.raises(ValidationError):
        replace(LocalWorkerLimits(), **{field: value})


@pytest.mark.parametrize(
    "raw",
    [
        '{"a":1,"a":2}',
        '{"a":NaN}',
        '{"a":Infinity}',
        '{"a":1e999}',
        '{ "a":1}',
        '"\\ud800"',
        b'"\xed\xa0\x80"',
        "[" * 2000,
    ],
)
def test_wire_rejects_noncanonical_nonfinite_or_malformed_json(raw):
    with pytest.raises(ValidationError):
        _load(raw, 4096)


def test_all_shard_budgets_checked_before_any_state_materialization(monkeypatch):
    original = point(replace(LocalWorkerLimits(), max_state_cells=1))
    wire = original.to_dict()
    wire["next_position"] = 2
    wire["waves"] = 1
    for index, shard in enumerate(wire["shards"]):
        shard["processed_inputs"] = 1
        key = next(str(i) for i in range(100) if partition_for(str(i), 2) == index)
        shard["cells"] = [{"step": "map", "key": key, "encoded_value": "{}"}]
    monkeypatch.setattr(FlowCheckpoint, "__post_init__", lambda _: pytest.fail("parsed state"))
    with pytest.raises(ValidationError, match="aggregate"):
        PartitionedFlowCheckpoint.from_dict(wire)


def test_forged_public_shard_rejected_without_iterating_cells():
    class NeverIterate:
        def __iter__(self):
            pytest.fail("iterated unbounded forged cells")

    original = point()
    object.__setattr__(original.shards[0], "cells", NeverIterate())
    with pytest.raises(ValidationError):
        original.to_dict()


def test_state_key_route_unused_state_and_aggregate_counts_rejected():
    original = point()
    key = next(str(i) for i in range(100) if partition_for(str(i), 2) == 1)
    shard = replace(original.shards[0], processed_inputs=1, cells=(("map", key, "1"),))
    with pytest.raises(ValidationError, match="different shard"):
        replace(original, next_position=1, waves=1, shards=(shard, original.shards[1]))
    shard = replace(original.shards[0], emitted_records=1)
    with pytest.raises(ValidationError, match="unused shard"):
        replace(original, shards=(shard, original.shards[1]))
    with pytest.raises(ValidationError, match="summed shard"):
        replace(original, next_position=1, waves=1)


@pytest.mark.parametrize("raw", [False, '"x"' * 3, '{"x":1,"x":2}', '"\\ud800"'])
def test_forged_flow_record_is_revalidated_without_callback(raw):
    record = FlowRecord(1, "key")
    object.__setattr__(record, "_json", raw)
    with pytest.raises(ValidationError):
        _copy_record(record)


def test_oversized_record_rejected_before_json_parse(monkeypatch):
    record = FlowRecord(1, "key")
    object.__setattr__(record, "_json", "x" * (8 * 1024 * 1024 + 1))
    monkeypatch.setattr(json, "loads", lambda *a, **k: pytest.fail("parsed oversized record"))
    with pytest.raises(ValidationError):
        _copy_record(record)


def test_output_and_batch_strict_order_range_and_no_aliased_payload():
    original = point()
    index = partition_for("a", 2)
    shards = list(original.shards)
    shards[index] = replace(shards[index], processed_inputs=1, emitted_records=1)
    after = replace(original, next_position=1, waves=1, shards=tuple(shards))
    item = PartitionedFlowOutput(0, 0, 0, FlowRecord({"x": [1]}, "a"))
    batch = PartitionedFlowBatch(0, (item,), after)
    exposed = batch.outputs[0].record.value
    exposed["x"].append(2)
    assert item.record.value == {"x": [1]}
    for bad in (
        replace(item, sequence=1),
        replace(item, source_position=1),
        replace(item, output_index=1),
    ):
        with pytest.raises(ValidationError):
            PartitionedFlowBatch(0, (bad,), after)
    with pytest.raises(ValidationError):
        PartitionedFlowOutput(True, 0, 0, item.record)


@pytest.mark.parametrize("cells", [(("x",),), (("x", "a", False),), (("x", "a", "1"),) * 100_001])
def test_raw_public_cells_are_admitted_before_wire_comprehension(cells):
    original = point()
    object.__setattr__(original.shards[0], "cells", cells)
    with pytest.raises(ValidationError):
        original.to_dict()


@pytest.mark.parametrize("cells", [False, [{"step": "x", "key": "a", "encoded_value": False}]])
def test_raw_imported_shard_cell_shape_is_strict(cells):
    wire = point().to_dict()
    wire["shards"][0]["cells"] = cells
    with pytest.raises(ValidationError):
        PartitionedFlowCheckpoint.from_dict(wire)


def test_encoded_state_aggregate_wire_budget_and_utf8_admission():
    original = point(replace(LocalWorkerLimits(), max_state_bytes=80))
    wire = original.to_dict()
    wire["shards"][0]["cells"] = [{"step": "x", "key": "a", "encoded_value": '"' + "中" * 30 + '"'}]
    with pytest.raises(ValidationError):
        PartitionedFlowCheckpoint.from_dict(wire)
    with pytest.raises(ValidationError):
        _load('"中中中"', 6)


@pytest.mark.parametrize(
    "change",
    [
        {"limits": None},
        {"source_closed": 1},
        {"shards": []},
        {"shards": (False, False)},
        {"next_position": 257, "waves": 1},
    ],
)
def test_exact_constructor_does_not_accept_impossible_outer_shape(change):
    with pytest.raises(ValidationError):
        replace(point(), **change)


def test_shard_identity_and_configured_aggregate_limits_checked_before_restore():
    original = point()
    with pytest.raises(ValidationError, match="identity"):
        replace(
            original, shards=(replace(original.shards[0], identity="b" * 64), original.shards[1])
        )
    with pytest.raises(ValidationError, match="flow mismatch"):
        original.validate_for(Dataflow("other", "1", (FlowStep("map", "map", str),)))
    malformed = replace(original.shards[0], processed_inputs=1, emitted_records=1001)
    after = replace(original, next_position=1, waves=1, shards=(malformed, original.shards[1]))
    with pytest.raises(ValidationError, match="counters"):
        after.validate_for(Dataflow("wire", "1", (FlowStep("map", "map", str),)))
    wire = original.to_dict()
    wire["shards"] = []
    with pytest.raises(ValidationError):
        PartitionedFlowCheckpoint.from_dict(wire)


def test_closed_or_invalid_shape_batch_is_not_an_execution_certificate():
    with pytest.raises(ValidationError):
        PartitionedFlowBatch(0, (), None)
    with pytest.raises(ValidationError):
        PartitionedFlowBatch(0, [], point())
    with pytest.raises(ValidationError):
        PartitionedFlowBatch(0, (False,), point())
    with pytest.raises(ValidationError):
        PartitionedFlowOutput(0, 0, 0, FlowRecord(1))
    original = point(replace(LocalWorkerLimits(), max_batch_inputs=1))
    shards = (replace(original.shards[0], processed_inputs=2), original.shards[1])
    after = replace(original, next_position=2, waves=2, shards=shards)
    with pytest.raises(ValidationError, match="batch input"):
        PartitionedFlowBatch(0, (), after)


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_calls_per_input", float("nan")),
        ("max_records_per_input", 10**1000),
    ],
    ids=["nonfinite-call-budget", "oversized-expansion-budget"],
)
def test_forged_flow_limits_rejected_before_reservation_or_startup(field, value):
    limits = FlowLimits()
    object.__setattr__(limits, field, value)
    flow = Dataflow("forged-limits", "1", (FlowStep("map", "map", str),), limits)
    with pytest.raises(ValidationError):
        _flow(flow)
