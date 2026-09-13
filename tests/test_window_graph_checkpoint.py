"""Closed wire, aggregate-before-nested admission and progress/counter histories."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace

import pytest

import stream_quilt.window_fold as window_module
import stream_quilt.window_graph_checkpoint as module
from stream_quilt import (
    FlowEdge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    StateUpdate,
    ValidationError,
    WindowFold,
    WindowFoldLimits,
    WindowGraphCheckpoint,
    WindowGraphDataflow,
    WindowGraphInput,
    WindowGraphLimits,
    WindowGraphRuntime,
)


def canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def rehash(document):
    body = document["body"]
    body["identity"] = digest(body["configuration"])
    nested = body["window"]
    nested["body"]["identity"] = digest(nested["body"]["configuration"])
    nested["sha256"] = digest(nested["body"])
    document["sha256"] = digest(body)
    return document


def make_runtime(*, filtered=False, maximum=1_000_000_000):
    prefix = (
        FlowStep("input", "filter", lambda value: False)
        if filtered
        else FlowStep(
            "input", "stateful_map", lambda value, state: StateUpdate(value, value), lambda: None
        )
    )
    fold = WindowFold(
        "window",
        "v1",
        width=10,
        initial=lambda: None,
        fold=lambda state, value: value,
        limits=replace(WindowFoldLimits(), max_row_bytes=512, max_batch_bytes=2048),
    )
    flow = WindowGraphDataflow(
        "checkpoint",
        "v1",
        (prefix, FlowWindow("window", fold)),
        (FlowEdge("input", "window"),),
        entry="input",
        limits=WindowGraphLimits(max_source_inputs=maximum),
    )
    runtime = WindowGraphRuntime(flow)
    return flow, runtime


def populated():
    flow, runtime = make_runtime()
    for index, key in enumerate(("a", 'é"\\')):
        runtime.process(
            WindowGraphInput(index, 1, FlowRecord({"quoted": '"\\', "nested": [1.0, None]}, key))
        )
    return flow, runtime, runtime.checkpoint().to_dict()


def test_exact_wire_equation_includes_escaped_state_and_both_configurations():
    flow, runtime, document = populated()
    body = document["body"]
    ordinary, windows = body["ordinary_cells"], body["window"]["body"]["cells"]
    assert body["configuration"]["nodes"][1]["fold"] == body["window"]["body"]["configuration"]
    empty = deepcopy(document)
    empty["body"]["ordinary_cells"] = []
    empty["body"]["window"]["body"]["cells"] = []
    sizes = [len(canonical(cell).encode()) for cell in [*ordinary, *windows]]
    equation = len(canonical(empty).encode()) + sum(sizes) + len(ordinary) - 1 + len(windows) - 1
    assert len(canonical(document).encode()) == equation
    assert runtime._state.cell_bytes == sum(sizes)
    assert len(canonical(empty).encode()) <= module._header_reservation(flow)
    assert sum(sizes) > sum(len(cell["state"].encode()) for cell in [*ordinary, *windows])
    point = WindowGraphCheckpoint.from_json(canonical(document).encode())
    assert point.to_json() == canonical(document)
    assert WindowGraphRuntime.from_checkpoint(flow, point).checkpoint() == point


@pytest.mark.parametrize(
    "section",
    [
        "envelope",
        "body",
        "configuration",
        "counter",
        "node",
        "edge",
        "cell",
        "window",
        "window_body",
        "window_cell",
    ],
)
def test_unknown_fields_reject_in_every_closed_shape(section):
    _, _, document = populated()
    body = document["body"]
    targets = {
        "envelope": document,
        "body": body,
        "configuration": body["configuration"],
        "counter": body["counters"],
        "node": body["configuration"]["nodes"][0],
        "edge": body["configuration"]["edges"][0],
        "cell": body["ordinary_cells"][0],
        "window": body["window"],
        "window_body": body["window"]["body"],
        "window_cell": body["window"]["body"]["cells"][0],
    }
    targets[section]["extra"] = None
    rehash(document)
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(document)


@pytest.mark.parametrize("value", [True, -1, 2**53, "1", 1.0, None])
@pytest.mark.parametrize(
    "slot",
    [
        "next_position",
        "operation_sequence",
        "watermark_advances",
        "drain_operations",
        "emitted_records",
        "edge",
    ],
)
def test_exact_interoperable_counters(value, slot):
    _, _, document = populated()
    body = document["body"]
    if slot == "edge":
        body["edge_counts"][0] = value
    elif slot in body["counters"]:
        body["counters"][slot] = value
    else:
        body[slot] = value
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(rehash(document))


@pytest.mark.parametrize(
    "change",
    [
        "sequence",
        "source_eof",
        "frontier_count",
        "input_edge",
        "ordinary_node",
        "ordinary_sort",
        "window_sort",
        "geometry",
        "phase",
        "drains",
        "outputs",
    ],
)
def test_rehashed_impossible_metadata_and_counts_reject_before_state_decode(monkeypatch, change):
    _, _, document = populated()
    body, window = document["body"], document["body"]["window"]["body"]
    if change == "sequence":
        body["operation_sequence"] += 1
    elif change == "source_eof":
        body["source_finished"] = True
    elif change == "frontier_count":
        body["counters"]["watermark_advances"] = 1
        body["operation_sequence"] += 1
    elif change == "input_edge":
        body["edge_counts"][0] += 1
    elif change == "ordinary_node":
        body["ordinary_cells"][0]["step_id"] = "window"
    elif change == "ordinary_sort":
        body["ordinary_cells"].reverse()
    elif change == "window_sort":
        window["cells"].reverse()
    elif change == "geometry":
        window["cells"][0]["end"] += 1
    elif change == "phase":
        window["phase"] = "closed"
    elif change == "drains":
        body["counters"]["drain_operations"] = 1
        body["operation_sequence"] += 1
    else:
        body["counters"]["emitted_records"] = 1
    rehash(document)

    def forbidden(*args):
        raise AssertionError("nested state decoder ran before metadata admission")

    monkeypatch.setattr(module, "_checked_value", forbidden)
    monkeypatch.setattr(window_module, "_checked_value", forbidden)
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(document)


@pytest.mark.parametrize(
    "slot", ["aggregate_bytes", "aggregate_cells", "ordinary_payload", "window_payload"]
)
def test_combined_capacity_is_checked_before_either_nested_decoder(monkeypatch, slot):
    _, _, document = populated()
    body = document["body"]
    if slot == "aggregate_bytes":
        body["configuration"]["limits"]["max_state_bytes"] = 1
    elif slot == "aggregate_cells":
        body["configuration"]["limits"]["max_state_cells"] = 1
    elif slot == "ordinary_payload":
        body["configuration"]["limits"]["graph"]["operator_limits"]["max_state_value_bytes"] = 1
    else:
        body["configuration"]["nodes"][1]["fold"]["limits"]["max_state_value_bytes"] = 1
        body["window"]["body"]["configuration"]["limits"]["max_state_value_bytes"] = 1
    rehash(document)

    def forbidden(*args):
        raise AssertionError("nested decoding preceded aggregate admission")

    monkeypatch.setattr(module, "_checked_value", forbidden)
    monkeypatch.setattr(window_module, "_checked_value", forbidden)
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(document)


@pytest.mark.parametrize("where", ["ordinary", "window"])
@pytest.mark.parametrize(
    "encoded",
    [
        "NaN",
        "Infinity",
        "-Infinity",
        "01",
        "[1,]",
        '{"x":1,"x":2}',
        '{"z":1, "a":2}',
        "9007199254740992",
        '"\\ud800"',
    ],
)
def test_nested_text_is_strict_finite_canonical_json(where, encoded):
    _, _, document = populated()
    cells = (
        document["body"]["ordinary_cells"]
        if where == "ordinary"
        else document["body"]["window"]["body"]["cells"]
    )
    cells[0]["state"] = encoded
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(rehash(document))


@pytest.mark.parametrize(
    "payload", [b"\xff", b"{}", "[]", "null", '{"body":{},"body":{},"sha256":"x"}']
)
def test_outer_wire_rejects_malformed_shapes_encoding_and_duplicates(payload):
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint.from_json(payload)


def test_outer_byte_limit_precedes_json_parse(monkeypatch):
    monkeypatch.setattr(module, "_MAX_WIRE", 20)
    monkeypatch.setattr(
        module, "_load", lambda *args: pytest.fail("must not parse oversized document")
    )
    for payload in (b"x" * 21, "é" * 11):
        with pytest.raises(ValidationError):
            WindowGraphCheckpoint.from_json(payload)


def test_noop_frontiers_remain_available_at_lifetime_ceiling():
    maximum = 2**53 - 1
    flow, runtime = make_runtime(filtered=True, maximum=maximum)
    document = runtime.checkpoint().to_dict()
    body = document["body"]
    body.update(next_position=maximum - 1, operation_sequence=maximum)
    body["counters"]["watermark_advances"] = 1
    body["window"]["body"]["watermark"] = 10
    runtime = WindowGraphRuntime.from_checkpoint(flow, WindowGraphCheckpoint(rehash(document)))
    before = runtime.checkpoint()
    assert runtime.advance_watermark(10, next_position=maximum - 1).operation_sequence == maximum
    assert runtime.drain().operation_sequence == maximum
    assert runtime.checkpoint() == before
    for operation in (
        lambda: runtime.advance_watermark(11, next_position=maximum - 1),
        lambda: runtime.finish(next_position=maximum - 1),
        lambda: runtime.process(WindowGraphInput(maximum - 1, 10, FlowRecord(1))),
    ):
        with pytest.raises(ValidationError):
            operation()
        assert runtime.checkpoint() == before
    body["counters"]["watermark_advances"] = maximum
    body["next_position"] = 0
    runtime = WindowGraphRuntime.from_checkpoint(flow, WindowGraphCheckpoint(rehash(document)))
    assert runtime.advance_watermark(10, next_position=0).operation_sequence == maximum
    assert runtime.drain().operation_sequence == maximum


def test_repeated_finish_at_operation_ceiling_and_filtered_input_at_window_ceiling():
    maximum = 2**53 - 1
    flow, runtime = make_runtime(filtered=True, maximum=maximum)
    document = runtime.checkpoint().to_dict()
    body = document["body"]
    body.update(next_position=maximum - 1, operation_sequence=maximum, source_finished=True)
    body["window"]["body"].update(finished=True, phase="closed")
    runtime = WindowGraphRuntime.from_checkpoint(flow, WindowGraphCheckpoint(rehash(document)))
    assert runtime.finish(next_position=maximum - 1).operation_sequence == maximum
    assert runtime.drain().outputs == ()
    with pytest.raises(ValidationError):
        runtime.finish(next_position=0)
    # A real one-input window limit, followed by a filtered record, still advances I.
    fold = WindowFold(
        "window",
        "v1",
        width=10,
        initial=lambda: 0,
        fold=lambda state, value: value,
        limits=replace(WindowFoldLimits(), max_inputs=1, max_row_bytes=512),
    )
    flow = WindowGraphDataflow(
        "filtered",
        "v1",
        (FlowStep("input", "filter", lambda value: value > 0), FlowWindow("window", fold)),
        (FlowEdge("input", "window"),),
        entry="input",
    )
    runtime = WindowGraphRuntime(flow)
    runtime.process(WindowGraphInput(0, 0, FlowRecord(1, "a")))
    assert runtime.process(WindowGraphInput(1, 0, FlowRecord(0, "a"))).next_position == 2
    assert (
        runtime.checkpoint().to_dict()["body"]["window"]["body"]["counters"]["processed_inputs"]
        == 1
    )


def test_unknown_or_changed_configuration_and_forged_frozen_objects_reject():
    flow, runtime, document = populated()
    point = runtime.checkpoint()
    with pytest.raises(ValidationError):
        WindowGraphRuntime.from_checkpoint(replace(flow, revision="changed"), point)
    object.__setattr__(point, "_json", "{}")
    with pytest.raises(ValidationError):
        WindowGraphRuntime.from_checkpoint(flow, point)
    document["body"]["configuration"]["policies"]["drain_order"] = "terminal-major"
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(rehash(document))


@pytest.mark.parametrize("field", ["kind", "version"])
@pytest.mark.parametrize("text_subclass", [False, True])
def test_nested_header_rejects_nonexact_types_without_invoking_comparison(field, text_subclass):
    _, runtime = make_runtime()
    document = runtime.checkpoint().to_dict()
    calls = []

    class Hostile:
        def __eq__(self, other):
            calls.append("comparison")
            raise AssertionError("untrusted comparison before exact-type rejection")

        __ne__ = __eq__

    class HostileText(str):
        def __eq__(self, other):
            calls.append("comparison")
            raise AssertionError("string subclass comparison before exact-type rejection")

        __ne__ = __eq__

    document["body"]["window"]["body"][field] = HostileText("bad") if text_subclass else Hostile()
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint.from_dict(document)
    assert calls == []
