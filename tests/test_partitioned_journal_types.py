"""Strict SQPJ shapes and independent canonical-wire / resource admission cases."""

import copy
import hashlib
import json
from dataclasses import FrozenInstanceError, replace

import pytest

from stream_quilt import Dataflow, FlowRecord, FlowRuntime, FlowStep, ValidationError
from stream_quilt.partitioned_checkpoint import LocalWorkerLimits, PartitionedFlowCheckpoint
from stream_quilt.partitioned_flow import PartitionedFlowOutput
from stream_quilt.partitioned_journal_types import (
    PartitionedFlowReceipt,
    PartitionedFlowRecoveryPoint,
    PartitionedFlowRequest,
    PartitionedJournalOutput,
    PartitionedOutputCursor,
    PartitionedOutputPage,
)


def head():
    flow = Dataflow("wire", "1", (FlowStep("str", "map", str),))
    cp = PartitionedFlowCheckpoint(
        flow.identity,
        "source",
        "a" * 64,
        1,
        LocalWorkerLimits(),
        0,
        0,
        False,
        (FlowRuntime(flow).checkpoint(),),
    )
    return PartitionedFlowRecoveryPoint("1" * 32, 0, cp)


def receipt():
    return PartitionedFlowReceipt(
        "1" * 32,
        "2" * 32,
        "3" * 64,
        "committed",
        0,
        "wave",
        (0, 0, False, 0),
        (2, 1, False, 3),
        "4" * 64,
        "5" * 64,
    )


def test_independent_canonical_utf8_request_digest_and_no_mutation():
    value = {"日本語": [True, None, -1, 0.5, 'é\\"']}
    record = FlowRecord(value, "clé")
    item = PartitionedFlowRequest("1" * 32, "2" * 32, 0, "wave", 0, (record,))
    expected = {
        "kind": "stream-quilt-partitioned-request",
        "version": "1.0",
        "journal_id": "1" * 32,
        "request_id": "2" * 32,
        "expected_generation": 0,
        "cause": "wave",
        "start_position": 0,
        "records": [
            {
                "key": "clé",
                "encoded_value": json.dumps(
                    value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
                ),
            }
        ],
    }
    wire = json.dumps(expected, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert item.to_json() == wire
    assert item.digest == hashlib.sha256(wire.encode()).hexdigest()
    original = copy.deepcopy(expected)
    restored = PartitionedFlowRequest.from_dict(expected)
    assert expected == original and restored == item
    expected["records"][0]["key"] = "other"
    value["日本語"].append(10)
    assert restored == item and record.value != value
    with pytest.raises(FrozenInstanceError):
        item.start_position = 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_id", "g" * 32),
        ("journal_id", "a" * 31),
        ("request_id", 1),
        ("expected_generation", True),
        ("expected_generation", -1),
        ("expected_generation", 1_000_001),
        ("expected_generation", 10**1000),
        ("cause", []),
        ("cause", "drain"),
        ("start_position", False),
        ("start_position", float("nan")),
        ("start_position", 1_000_001),
        ("records", []),
        ("records", ()),
        ("records", (FlowRecord(1),)),
        ("records", (FlowRecord(1, "a"),) * 257),
    ],
)
def test_strict_request_constructor_rejects(field, value):
    item = PartitionedFlowRequest("1" * 32, "2" * 32, 0, "wave", 0, (FlowRecord(1, "a"),))
    with pytest.raises(ValidationError):
        replace(item, **{field: value})


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(extra=0),
        lambda d: d.update(kind="stream-quilt-multi-request"),
        lambda d: d.update(version="2.0"),
        lambda d: d.update(records={}),
        lambda d: d["records"][0].update(value=1),
        lambda d: d["records"][0].update(encoded_value="1.0 "),
        lambda d: d["records"][0].update(encoded_value='{"x":1,"x":2}'),
        lambda d: d["records"][0].update(encoded_value="NaN"),
        lambda d: d["records"][0].update(encoded_value="1e999"),
        lambda d: d["records"][0].update(key=None),
        lambda d: d["records"][0].update(key=" x "),
    ],
)
def test_request_import_rejects_unknown_and_noncanonical_inner_values(mutate):
    document = PartitionedFlowRequest(
        "1" * 32, "2" * 32, 0, "wave", 0, (FlowRecord(1, "a"),)
    ).to_dict()
    mutate(document)
    with pytest.raises(ValidationError):
        PartitionedFlowRequest.from_dict(document)


def test_aggregate_request_admission_precedes_inner_decoding(monkeypatch):
    import stream_quilt.partitioned_journal_types as module

    document = PartitionedFlowRequest(
        "1" * 32, "2" * 32, 0, "wave", 0, (FlowRecord("é" * 100, "a"),)
    ).to_dict()
    monkeypatch.setattr(module, "_REQUEST_BYTES", 150)
    monkeypatch.setattr(module, "_load", lambda *args: pytest.fail("decoded before wire admission"))
    with pytest.raises(ValidationError, match="bytes"):
        PartitionedFlowRequest.from_dict(document)


@pytest.mark.parametrize("value", ["{", "{} ", b"\xff", '{"a":1,"a":2}', "NaN"])
def test_json_ingress_is_strict(value):
    for cls in (
        PartitionedFlowRequest,
        PartitionedFlowReceipt,
        PartitionedFlowRecoveryPoint,
        PartitionedOutputCursor,
    ):
        with pytest.raises(ValidationError):
            cls.from_json(value)


def test_head_receipt_cursor_output_roundtrips():
    point = head()
    saved = receipt()
    cursor = PartitionedOutputCursor("1" * 32, 1, "2" * 64, 3, 2)
    assert PartitionedFlowRecoveryPoint.from_json(point.to_json()) == point
    assert PartitionedFlowReceipt.from_json(saved.to_json()) == saved
    assert PartitionedOutputCursor.from_json(cursor.to_json()) == cursor
    output = PartitionedJournalOutput(1, PartitionedFlowOutput(1, 0, 1, FlowRecord(None, "a")))
    assert PartitionedJournalOutput.from_dict(output.to_dict()) == output
    assert json.loads(PartitionedOutputPage((output,), cursor).to_json())["outputs"] == [
        output.to_dict()
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "pending"),
        ("status", []),
        ("cause", []),
        ("cause", "drain"),
        ("expected_generation", 1),
        ("expected_generation", True),
        ("before", [0, 0, False, 0]),
        ("before", (0, 0, 0, 0)),
        ("before", (0, 0, False, 1)),
        ("after", (257, 1, False, 3)),
        ("after", (1, 0, False, 3)),
        ("after", (2, 1, True, 3)),
        ("after", (2, 1, False, 100_001)),
        ("after", (2, 1, False, -1)),
        ("before_head_digest", "x" * 64),
        ("request_digest", True),
    ],
)
def test_receipt_necessary_counter_invariants(field, value):
    with pytest.raises(ValidationError):
        replace(receipt(), **{field: value})


def test_eof_and_noop_progress_rules():
    base = receipt()
    eof = replace(base, cause="eof", after=(0, 0, True, 0))
    assert eof.after_generation == 1 and eof.output_start == eof.output_stop == 0
    no_op = replace(
        eof,
        status="no_op",
        expected_generation=1,
        before=eof.after,
        before_head_digest=eof.after_head_digest,
    )
    assert no_op.after_generation == 1
    for change in ({"after": (1, 1, True, 0)}, {"cause": "wave"}, {"before_head_digest": "f" * 64}):
        with pytest.raises(ValidationError):
            replace(no_op, **change)
    with pytest.raises(ValidationError):
        replace(eof, after=(1, 1, True, 0))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda x: x.update(extra=1),
        lambda x: x.update(checkpoint=None),
        lambda x: x.update(generation=1),
    ],
)
def test_head_shape_and_identity_counter_reject(mutate):
    wire = head().to_dict()
    mutate(wire)
    with pytest.raises(ValidationError):
        PartitionedFlowRecoveryPoint.from_dict(wire)


def test_page_and_cursor_constructor_bounds():
    cursor = PartitionedOutputCursor("1" * 32, 1, "2" * 64, 1, 1)
    output = PartitionedJournalOutput(1, PartitionedFlowOutput(0, 0, 0, FlowRecord(1, "a")))
    for changes in ({"next_sequence": 2}, {"anchor_generation": 0}, {"stop_sequence": True}):
        with pytest.raises(ValidationError):
            replace(cursor, **changes)
    for outputs in ([output], (output,) * 1001, ("not output",), (replace(output, generation=2),)):
        with pytest.raises(ValidationError):
            PartitionedOutputPage(outputs, cursor)


@pytest.mark.parametrize(
    "changes", [{"cause": "drain"}, {"records": [{"key": "a", "encoded_value": 1}]}]
)
def test_request_wire_does_not_coerce_invalid_types(changes):
    wire = PartitionedFlowRequest("1" * 32, "2" * 32, 0, "wave", 0, (FlowRecord(1, "a"),)).to_dict()
    wire.update(changes)
    with pytest.raises(ValidationError):
        PartitionedFlowRequest.from_dict(wire)


def test_receipt_progress_wire_and_eof_output_cannot_be_forged():
    wire = receipt().to_dict()
    wire["before"] = [0, 0, False]
    with pytest.raises(ValidationError):
        PartitionedFlowReceipt.from_dict(wire)
    committed = receipt()
    with pytest.raises(ValidationError, match="EOF"):
        replace(
            committed,
            expected_generation=1,
            cause="eof",
            before=(2, 1, False, 3),
            after=(2, 1, True, 4),
        )


def test_forged_encoded_record_and_wrong_nested_types_are_readmitted():
    record = FlowRecord(1, "a")
    object.__setattr__(record, "_json", 1)
    with pytest.raises(ValidationError):
        PartitionedFlowRequest("1" * 32, "2" * 32, 0, "wave", 0, (record,))
    with pytest.raises(ValidationError):
        PartitionedFlowRecoveryPoint("1" * 32, 0, None)
    with pytest.raises(ValidationError):
        PartitionedJournalOutput(1, None)
    with pytest.raises(ValidationError):
        PartitionedOutputPage((), {})


def test_aggregate_page_wire_is_admitted_before_return(monkeypatch):
    import stream_quilt.partitioned_journal_types as module

    cursor = PartitionedOutputCursor("1" * 32, 1, "2" * 64, 2, 2)
    rows = tuple(
        PartitionedJournalOutput(1, PartitionedFlowOutput(i, 0, i, FlowRecord("é" * 20, "a")))
        for i in range(2)
    )
    first_size = len(
        json.dumps(rows[0].to_dict(), ensure_ascii=False, separators=(",", ":")).encode()
    )
    monkeypatch.setattr(module, "_BATCH_BYTES", first_size)
    with pytest.raises(ValidationError, match="page wire budget"):
        PartitionedOutputPage(rows, cursor)
