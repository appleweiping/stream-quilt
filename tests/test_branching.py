from __future__ import annotations

import hashlib
import itertools
import json
import runpy
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from stream_quilt import (
    Dataflow,
    FlowBranch,
    FlowCheckpoint,
    FlowEdge,
    FlowExecutionError,
    FlowJournal,
    FlowLimits,
    FlowMerge,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    GraphCheckpoint,
    GraphDataflow,
    GraphLimits,
    GraphOutput,
    GraphRuntime,
    StateUpdate,
    ValidationError,
)


def identity(value):
    return value


def add(value, state):
    return StateUpdate(state + value, state + value)


def fanout(*, limits=None, right=identity):
    return GraphDataflow(
        "fork",
        "1",
        (
            FlowStep("input", "map", identity),
            FlowStep("left", "map", identity),
            FlowStep("right", "map", right),
        ),
        (FlowEdge("input", "left"), FlowEdge("input", "right")),
        "input",
        GraphLimits() if limits is None else limits,
    )


def records(outputs):
    return [(output.step_id, output.record.key, output.record.value) for output in outputs]


def diamond():
    return GraphDataflow(
        "diamond",
        "1",
        (
            FlowMerge("merge"),
            FlowStep("left", "map", lambda value: value * 10),
            FlowStep("sum", "stateful_map", add, lambda: 0),
            FlowStep("right", "map", lambda value: value + 100),
            FlowStep("input", "flat_map", lambda value: (value, value + 1)),
        ),
        (
            FlowEdge("right", "merge"),
            FlowEdge("input", "right"),
            FlowEdge("input", "left"),
            FlowEdge("left", "merge"),
            FlowEdge("merge", "sum"),
        ),
        "input",
    )


def test_diamond_manual_oracle_edge_order_not_execution_order():
    flow = diamond()
    assert flow.execution_order == ("input", "left", "right", "merge", "sum")
    runtime = GraphRuntime(flow)
    # Right executes second, but is the first declared incoming merge edge.
    assert records(runtime.process(FlowRecord(1, "a"))) == [
        ("sum", "a", 101),
        ("sum", "a", 203),
        ("sum", "a", 213),
        ("sum", "a", 233),
    ]
    assert records(runtime.process(FlowRecord(2, "b"))) == [
        ("sum", "b", 102),
        ("sum", "b", 205),
        ("sum", "b", 225),
        ("sum", "b", 255),
    ]
    assert records(runtime.process(FlowRecord(0, "a")))[-1] == ("sum", "a", 444)
    assert runtime.processed_inputs == 3 and runtime.emitted_records == 12
    assert runtime.checkpoint().cells == (("sum", "a", "444"), ("sum", "b", "255"))


def test_conditional_branch_predicate_once_and_merge_groups_routes():
    calls = []

    def even(value):
        calls.append(value)
        return value % 2 == 0

    flow = GraphDataflow(
        "routes",
        "1",
        (
            FlowStep("items", "flat_map", identity),
            FlowBranch("even", even),
            FlowMerge("result"),
        ),
        (
            FlowEdge("items", "even"),
            FlowEdge("even", "result", False),
            FlowEdge("even", "result", True),
        ),
        "items",
    )
    runtime = GraphRuntime(flow)
    assert records(runtime.process(FlowRecord([4, 1, 2, 3], "a"))) == [
        ("result", "a", 1),
        ("result", "a", 3),
        ("result", "a", 4),
        ("result", "a", 2),
    ]
    assert calls == [4, 1, 2, 3]
    assert records(runtime.process(FlowRecord([6], "b"))) == [("result", "b", 6)]
    assert calls == [4, 1, 2, 3, 6]
    assert runtime.process(FlowRecord([])) == ()


def test_branch_can_fan_out_one_route_without_retesting_predicate():
    calls = []
    flow = GraphDataflow(
        "branch-fanout",
        "1",
        (
            FlowBranch("route", lambda value: calls.append(value) or value > 0),
            FlowStep("a", "map", identity),
            FlowStep("b", "map", identity),
            FlowStep("discard", "filter", lambda _: False),
        ),
        (
            FlowEdge("route", "a", True),
            FlowEdge("route", "b", True),
            FlowEdge("route", "discard", False),
        ),
        "route",
    )
    runtime = GraphRuntime(flow)
    assert records(runtime.process(FlowRecord(1))) == [("a", None, 1), ("b", None, 1)]
    assert runtime.process(FlowRecord(0)) == ()
    assert calls == [1, 0]


def test_sibling_state_is_isolated_and_downstream_state_is_shared():
    flow = replace(
        diamond(),
        nodes=(
            FlowStep("input", "map", identity),
            FlowStep("left", "stateful_map", add, lambda: 0),
            FlowStep("right", "stateful_map", add, lambda: 10),
            FlowMerge("merge"),
            FlowStep("sum", "stateful_map", add, lambda: 0),
        ),
    )
    runtime = GraphRuntime(flow)
    assert records(runtime.process(FlowRecord(2, "a"))) == [("sum", "a", 12), ("sum", "a", 14)]
    assert records(runtime.process(FlowRecord(3, "a"))) == [("sum", "a", 29), ("sum", "a", 34)]
    assert runtime.checkpoint().cells == (
        ("left", "a", "5"),
        ("right", "a", "15"),
        ("sum", "a", "34"),
    )


def test_later_sibling_failure_publishes_no_outputs_or_state():
    calls = []

    def reject(value):
        calls.append(("right", value))
        if value == 5:
            raise RuntimeError("private details")
        return value

    def count(value, state):
        calls.append(("left", value))
        return add(value, state)

    flow = replace(
        fanout(right=reject),
        nodes=(
            FlowStep("input", "map", identity),
            FlowStep("left", "stateful_map", count, lambda: 0),
            FlowStep("right", "map", reject),
        ),
    )
    runtime = GraphRuntime(flow)
    runtime.process(FlowRecord(2, "a"))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError, match="right") as error:
        runtime.process(FlowRecord(5, "a"))
    assert "private" not in str(error.value)
    assert runtime.checkpoint() == before
    assert calls[-2:] == [("left", 5), ("right", 5)]  # external effects aren't rolled back
    assert records(runtime.process(FlowRecord(1, "a"))) == [("left", "a", 3), ("right", "a", 1)]


def test_input_and_sibling_callback_mutations_are_isolated():
    def mutate(value):
        value["items"].append(2)
        return value

    flow = replace(
        fanout(),
        nodes=(
            FlowStep("input", "map", identity),
            FlowStep("left", "map", mutate),
            FlowStep("right", "map", identity),
        ),
    )
    original = {"items": [1]}
    record = FlowRecord(original, "a")
    original["items"].append(99)
    output = GraphRuntime(flow).process(record)
    assert records(output) == [("left", "a", {"items": [1, 2]}), ("right", "a", {"items": [1]})]
    output[0].record.value["items"].append(100)
    assert record.value == {"items": [1]} and output[0].record.value == {"items": [1, 2]}
    with pytest.raises(FrozenInstanceError):
        output[0].step_id = "changed"
    assert output[1].to_dict() == {"step_id": "right", "key": "a", "value": {"items": [1]}}


@pytest.mark.parametrize("name", ["max_work_records", "max_work_bytes"])
def test_exact_aggregate_fanout_boundary(name):
    # Input + root emission + two edge deliveries + two terminal emissions = 6.
    rejected = GraphRuntime(fanout(limits=replace(GraphLimits(), **{name: 5})))
    before = rejected.checkpoint()
    with pytest.raises(FlowExecutionError, match="right"):
        rejected.process(FlowRecord(1))
    assert rejected.checkpoint() == before
    accepted = GraphRuntime(fanout(limits=replace(GraphLimits(), **{name: 6})))
    assert len(accepted.process(FlowRecord(1))) == 2


def test_utf8_budget_counts_delivery_copies_and_rejects_input_before_callbacks():
    # Each JSON "é" is four UTF-8 bytes, not three characters.
    assert (
        len(GraphRuntime(fanout(limits=GraphLimits(max_work_bytes=24))).process(FlowRecord("é")))
        == 2
    )
    runtime = GraphRuntime(fanout(limits=GraphLimits(max_work_bytes=23)))
    with pytest.raises(FlowExecutionError, match="right"):
        runtime.process(FlowRecord("é"))
    runtime = GraphRuntime(fanout(limits=GraphLimits(max_work_bytes=1)))
    with pytest.raises(ValidationError, match="aggregate"):
        runtime.process(FlowRecord(10))
    assert runtime.processed_inputs == 0


def test_fanout_edge_work_charged_before_accepting_downstream_callbacks():
    calls = []
    flow = replace(
        fanout(limits=GraphLimits(max_work_records=3)),
        nodes=(
            FlowStep("input", "map", identity),
            FlowStep("left", "map", lambda value: calls.append(value)),
            FlowStep("right", "map", identity),
        ),
    )
    runtime = GraphRuntime(flow)
    with pytest.raises(FlowExecutionError, match="input"):
        runtime.process(FlowRecord(1))
    assert calls == [] and runtime.emitted_records == 0


def test_infinite_expansion_closed_by_global_not_per_stage_budget():
    closed = []

    def expand(value):
        try:
            yield from itertools.repeat(value)
        finally:
            closed.append(True)

    flow = replace(
        fanout(limits=GraphLimits(max_work_records=4)),
        nodes=(FlowStep("input", "flat_map", expand), *fanout().nodes[1:]),
    )
    runtime = GraphRuntime(flow)
    with pytest.raises(FlowExecutionError, match="input"):
        runtime.process(FlowRecord(1))
    assert closed == [True] and runtime.checkpoint().cells == ()


@pytest.mark.parametrize("limit", ["max_state_keys", "max_state_bytes", "max_calls_per_input"])
def test_state_and_callback_budgets_shared_across_siblings(limit):
    limits = GraphLimits(
        operator_limits=replace(FlowLimits(), **{limit: 2 if limit == "max_calls_per_input" else 1})
    )
    flow = replace(
        fanout(limits=limits),
        nodes=(
            FlowStep("input", "map", identity),
            FlowStep("left", "stateful_map", add, lambda: 0),
            FlowStep("right", "stateful_map", add, lambda: 0),
        ),
    )
    runtime = GraphRuntime(flow)
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1, "a"))
    assert runtime.checkpoint().cells == () and runtime.processed_inputs == 0


@pytest.mark.parametrize("operation", ["process", "checkpoint", "run"])
def test_reentrancy_and_checkpoint_during_transaction_rejected(operation):
    def callback(value):
        if operation == "process":
            runtime.process(FlowRecord(value))
        elif operation == "checkpoint":
            runtime.checkpoint()
        else:
            list(runtime.run([FlowRecord(value)]))
        return value

    runtime = GraphRuntime(fanout(right=callback))
    with pytest.raises(FlowExecutionError, match="right"):
        runtime.process(FlowRecord(1))
    assert runtime.processed_inputs == 0


def test_reentrant_run_does_not_touch_source_iterator():
    pulled = []

    def source():
        pulled.append(True)
        yield FlowRecord(1)

    def callback(value):
        list(runtime.run(source()))
        return value

    runtime = GraphRuntime(fanout(right=callback))
    with pytest.raises(FlowExecutionError, match="right"):
        runtime.process(FlowRecord(1))
    assert pulled == []


def test_branch_async_function_or_result_rejected_and_coroutine_closed():
    async def async_predicate(value):
        return bool(value)

    with pytest.raises(ValidationError, match="synchronous"):
        FlowBranch("branch", async_predicate)
    coroutine = async_predicate(1)
    flow = GraphDataflow(
        "async",
        "1",
        (FlowBranch("b", lambda _: coroutine), FlowMerge("m")),
        (FlowEdge("b", "m", True), FlowEdge("b", "m", False)),
        "b",
    )
    runtime = GraphRuntime(flow)
    with pytest.raises(FlowExecutionError, match="b"):
        runtime.process(FlowRecord(1))
    assert coroutine.cr_frame is None and runtime.processed_inputs == 0


@pytest.mark.parametrize("result", [1, None, "yes", []])
def test_branch_requires_real_bool(result):
    flow = GraphDataflow(
        "bool",
        "1",
        (FlowBranch("b", lambda _: result), FlowMerge("m")),
        (FlowEdge("b", "m", True), FlowEdge("b", "m", False)),
        "b",
    )
    runtime = GraphRuntime(flow)
    with pytest.raises(FlowExecutionError, match="b"):
        runtime.process(FlowRecord(1))
    assert runtime.emitted_records == 0


def test_control_exception_rolls_back_shared_state_and_releases_busy_guard():
    def stop(value):
        if value:
            raise SystemExit(7)
        return value

    runtime = GraphRuntime(fanout(right=stop))
    before = runtime.checkpoint()
    with pytest.raises(SystemExit):
        runtime.process(FlowRecord(1))
    assert runtime.checkpoint() == before
    assert len(runtime.process(FlowRecord(0))) == 2


def test_checkpoint_json_restart_and_independent_seeded_oracle():
    flow = diamond()
    runtime = GraphRuntime(flow)
    totals = {"a": 0, "b": 0, "c": 0}
    for index in range(60):
        key, value = ("a", "b", "c")[index % 3], (index * 7) % 11
        expected = []
        for addition in (value + 100, value + 101, value * 10, (value + 1) * 10):
            totals[key] += addition
            expected.append(("sum", key, totals[key]))
        assert records(runtime.process(FlowRecord(value, key))) == expected
        if index % 7 == 0:
            document = json.loads(json.dumps(runtime.checkpoint().to_dict()))
            runtime = GraphRuntime.from_checkpoint(flow, GraphCheckpoint.from_dict(document))
            document["cells"][0]["value"] = 999999
    assert runtime.processed_inputs == 60 and runtime.emitted_records == 240
    assert runtime.checkpoint().cells == tuple(
        ("sum", key, str(value)) for key, value in totals.items()
    )


@pytest.mark.parametrize("change", ["revision", "flow_id", "edges", "nodes", "limits"])
def test_checkpoint_binds_topology_order_limits_and_revision(change):
    flow = diamond()
    changed = {
        "revision": "2",
        "flow_id": "other",
        "edges": tuple(reversed(flow.edges)),
        "nodes": tuple(reversed(flow.nodes)),
        "limits": GraphLimits(max_work_records=12345),
    }[change]
    with pytest.raises(ValidationError, match="identity"):
        GraphRuntime.from_checkpoint(
            replace(flow, **{change: changed}), GraphRuntime(flow).checkpoint()
        )


def test_callback_code_not_serialized_revision_is_explicit():
    original = fanout()
    assert original.identity == fanout(right=lambda value: value * 9).identity
    document = original.to_dict()
    assert (
        original.identity
        == hashlib.sha256(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert "function" not in json.dumps(document)
    document["edges"].clear()
    assert len(original.edges) == 2


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "bad"},
        {"schema_version": "2.0"},
        {"extra": 1},
        {"processed_inputs": True},
        {"cells": [{}]},
    ],
)
def test_graph_checkpoint_strict_format(change):
    document = GraphRuntime(fanout()).checkpoint().to_dict()
    document.update(change)
    with pytest.raises(ValidationError):
        GraphCheckpoint.from_dict(document)


def test_graph_checkpoint_unknown_cells_and_malformed_document():
    flow = diamond()
    checkpoint = GraphRuntime(flow).checkpoint()
    for cells in ((("left", "a", "1"),), (("missing", "a", "1"),)):
        with pytest.raises(ValidationError, match="incompatible"):
            GraphRuntime.from_checkpoint(flow, replace(checkpoint, cells=cells))
    with pytest.raises(ValidationError):
        GraphCheckpoint.from_dict([])
    with pytest.raises(ValidationError):
        GraphCheckpoint.from_dict(FlowCheckpoint(checkpoint.identity, 0, 0, ()).to_dict())
    with pytest.raises(ValidationError):
        FlowCheckpoint.from_dict(checkpoint.to_dict())
    with pytest.raises(ValidationError):
        GraphRuntime.from_checkpoint(flow, FlowCheckpoint(checkpoint.identity, 0, 0, ()))


@pytest.mark.parametrize("limit", ["max_state_keys", "max_state_bytes", "max_state_value_bytes"])
def test_restoration_enforces_graph_state_limits(limit):
    flow = replace(
        diamond(), limits=GraphLimits(operator_limits=replace(FlowLimits(), **{limit: 1}))
    )
    cells = (
        (("sum", "a", "10"),)
        if limit == "max_state_value_bytes"
        else (("sum", "a", "1"), ("sum", "b", "2"))
    )
    checkpoint = replace(GraphRuntime(flow).checkpoint(), cells=cells)
    with pytest.raises(ValidationError):
        GraphRuntime.from_checkpoint(flow, checkpoint)


@pytest.mark.parametrize("counter", ["processed_inputs", "emitted_records"])
def test_counter_overflow_cannot_commit(counter):
    flow = diamond()
    checkpoint = replace(GraphRuntime(flow).checkpoint(), **{counter: 2**53 - 1})
    runtime = GraphRuntime.from_checkpoint(flow, checkpoint)
    with pytest.raises(ValidationError, match=counter):
        runtime.process(FlowRecord(1, "a"))
    assert runtime.checkpoint() == checkpoint


def test_pull_consumption_and_empty_source():
    pulled = []

    def source():
        for value in range(4):
            pulled.append(value)
            yield FlowRecord(value)

    runtime = GraphRuntime(fanout())
    iterator = source()
    output = runtime.run(iterator, max_inputs=1)
    assert next(output).record.value == 0 and pulled == [0]
    assert len(list(output)) == 1 and pulled == [0]
    output = runtime.run(iterator)
    assert next(output).record.value == 1
    output.close()
    assert pulled == [0, 1] and runtime.emitted_records == 4
    assert next(iterator).value == 2
    assert list(runtime.run(())) == []
    with pytest.raises(ValidationError):
        list(runtime.run((), max_inputs=True))


@pytest.mark.parametrize(
    "change",
    [
        {"nodes": ()},
        {"nodes": []},
        {"nodes": (None,)},
        {"nodes": (FlowStep("x", "drop_key"),) * 65},
        {"nodes": (FlowStep("input", "drop_key"),) * 2},
        {"entry": "missing"},
        {"edges": []},
        {"edges": (None,)},
        {"edges": (FlowEdge("input", "left"),) * 257},
        {"edges": (FlowEdge("missing", "left"),)},
        {"edges": (FlowEdge("input", "left"), FlowEdge("input", "left"))},
        {"edges": (FlowEdge("input", "left"), FlowEdge("left", "input"))},
        {"edges": (FlowEdge("input", "left"),)},
        {
            "edges": (
                FlowEdge("input", "left"),
                FlowEdge("input", "right"),
                FlowEdge("left", "right"),
            )
        },
        {"edges": (FlowEdge("input", "left", True), FlowEdge("input", "right"))},
        {"limits": {}},
    ],
)
def test_invalid_graphs_rejected_before_callbacks(change):
    with pytest.raises(ValidationError):
        replace(fanout(), **change)


def test_merge_and_branch_shape_validation():
    with pytest.raises(ValidationError, match="merge"):
        GraphDataflow("bad", "1", (FlowMerge("only"),), (), "only")
    for routes in ((None, None), (True, True), (False, False), (True, None)):
        with pytest.raises(ValidationError, match="branch"):
            GraphDataflow(
                "bad",
                "1",
                (FlowBranch("b", bool), FlowStep("a", "drop_key"), FlowStep("c", "drop_key")),
                (FlowEdge("b", "a", routes[0]), FlowEdge("b", "c", routes[1])),
                "b",
            )
    with pytest.raises(ValidationError, match="route"):
        FlowEdge("a", "b", 1)


@pytest.mark.parametrize(
    "options",
    [
        {"operator_limits": {}},
        {"max_work_records": True},
        {"max_work_records": 0},
        {"max_work_bytes": 2**1000},
    ],
)
def test_invalid_graph_limits(options):
    with pytest.raises(ValidationError):
        GraphLimits(**options)


def test_new_and_old_api_boundaries_and_no_journal_side_effect(tmp_path):
    with pytest.raises(ValidationError):
        GraphRuntime(None)
    runtime = GraphRuntime(fanout())
    with pytest.raises(ValidationError):
        runtime.process({})
    with pytest.raises(ValidationError):
        GraphOutput("output", {})
    with pytest.raises(ValidationError):
        FlowRuntime(runtime.flow)
    with pytest.raises(ValidationError):
        FlowJournal(tmp_path / "absent" / "store.db", runtime.flow, "source")
    assert not (tmp_path / "absent").exists()
    limited = GraphRuntime(
        fanout(limits=GraphLimits(operator_limits=FlowLimits(max_record_bytes=1)))
    )
    with pytest.raises(ValidationError):
        limited.process(FlowRecord(10))
    with pytest.raises(AttributeError):
        runtime.flow = fanout()


def test_linear_identity_and_checkpoint_document_remain_byte_compatible():
    flow = Dataflow("legacy", "1", (FlowStep("sum", "stateful_map", add, lambda: 0),))
    # Independent declaration and frozen digest of the published linear format.
    declaration = {
        "flow_id": "legacy",
        "revision": "1",
        "steps": [["sum", "stateful_map"]],
        "limits": FlowLimits().to_dict(),
    }
    expected = hashlib.sha256(
        json.dumps(declaration, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert expected == "a9247d01968bc0fb48edb85ec83729c7485ee771b69d939310890d8595c918ce"
    runtime = FlowRuntime(flow)
    runtime.process(FlowRecord(2, "a"))
    assert runtime.checkpoint().to_dict() == {
        "kind": "stream-quilt-dataflow-checkpoint",
        "schema_version": "1.0",
        "identity": expected,
        "processed_inputs": 1,
        "emitted_records": 1,
        "cells": [{"step": "sum", "key": "a", "value": 2}],
    }


def test_offline_graph_example(capsys):
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "branching_totals.py"), run_name="__main__"
    )
    report = json.loads(capsys.readouterr().out)
    assert report["processed_inputs"] == 2
    assert report["outputs"] == [3, 8, 7, 15]


def test_maximum_node_count_and_exact_chain_work_bound():
    nodes = tuple(FlowStep(f"n{index}", "map", lambda value: value + 1) for index in range(64))
    edges = tuple(FlowEdge(f"n{index}", f"n{index + 1}") for index in range(63))
    flow = GraphDataflow("chain", "1", nodes, edges, "n0", GraphLimits(max_work_records=128))
    assert records(GraphRuntime(flow).process(FlowRecord(0))) == [("n63", None, 64)]
    limited = replace(flow, limits=GraphLimits(max_work_records=127))
    with pytest.raises(FlowExecutionError, match="n63"):
        GraphRuntime(limited).process(FlowRecord(0))


def test_branch_callback_cannot_spoof_error_step_identity():
    def fail(_):
        raise FlowExecutionError("unrelated")

    flow = GraphDataflow(
        "error",
        "1",
        (FlowBranch("actual", fail), FlowMerge("merged")),
        (FlowEdge("actual", "merged", True), FlowEdge("actual", "merged", False)),
        "actual",
    )
    with pytest.raises(FlowExecutionError) as error:
        GraphRuntime(flow).process(FlowRecord(1))
    assert error.value.step_id == "actual"


@pytest.mark.parametrize("runtime_type", ["linear", "graph"])
@pytest.mark.parametrize("stage", ["copy", "second_update"])
def test_commit_allocation_failure_leaves_old_index_untouched(runtime_type, stage):
    steps = (
        FlowStep("left", "stateful_map", add, lambda: 0),
        FlowStep("right", "stateful_map", add, lambda: 0),
    )
    runtime = (
        FlowRuntime(Dataflow("allocation", "1", steps))
        if runtime_type == "linear"
        else GraphRuntime(
            GraphDataflow("allocation", "1", steps, (FlowEdge("left", "right"),), "left")
        )
    )
    runtime.process(FlowRecord(1, "a"))
    before = runtime.checkpoint()
    candidates = []

    class NextIndex(dict):
        writes = 0

        def __setitem__(self, key, value):
            self.writes += 1
            if self.writes == 2:
                raise MemoryError("injected allocation failure after one staged update")
            super().__setitem__(key, value)

    class RetainedIndex(NextIndex):
        def copy(self):
            if stage == "copy":
                raise MemoryError("injected index copy failure")
            candidate = NextIndex(self)
            candidates.append(candidate)
            return candidate

    retained = RetainedIndex(runtime._state)
    runtime._state = retained
    with pytest.raises(MemoryError):
        runtime.process(FlowRecord(2, "a"))
    assert runtime._state is retained and runtime.checkpoint() == before
    if stage == "second_update":
        assert candidates[0][("left", "a")] == "3"
        assert retained[("left", "a")] == "1"


@pytest.mark.parametrize("runtime_type", ["linear", "graph"])
@pytest.mark.parametrize("primary", [KeyboardInterrupt, SystemExit])
def test_consumer_control_exception_survives_generator_cleanup_error(
    runtime_type, primary, monkeypatch
):
    import stream_quilt.dataflow as module

    closed = []

    def expand(_):
        try:
            yield 17
            yield 18
        finally:
            closed.append(True)
            raise OSError("secondary cleanup error")

    steps = (
        FlowStep("state", "stateful_map", add, lambda: 0),
        FlowStep("expand", "flat_map", expand),
    )
    runtime = (
        FlowRuntime(Dataflow("interrupt", "1", steps))
        if runtime_type == "linear"
        else GraphRuntime(
            GraphDataflow("interrupt", "1", steps, (FlowEdge("state", "expand"),), "state")
        )
    )
    before = runtime.checkpoint()
    record = FlowRecord(1, "a")
    snapshot = module._snapshot

    def interrupted(value, maximum):
        if value == 17:
            raise primary("primary consumer interruption")
        return snapshot(value, maximum)

    monkeypatch.setattr(module, "_snapshot", interrupted)
    with pytest.raises(primary, match="primary consumer"):
        runtime.process(record)
    assert closed == [True] and runtime.checkpoint() == before


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit])
def test_genuine_generator_cleanup_interrupt_is_not_suppressed(interrupt):
    def expand(_):
        try:
            yield 1
            yield 2
            yield 3
        finally:
            raise interrupt("cleanup interruption")

    flow = GraphDataflow(
        "cleanup",
        "1",
        (FlowStep("expand", "flat_map", expand),),
        (),
        "expand",
        GraphLimits(operator_limits=FlowLimits(max_records_per_input=1)),
    )
    runtime = GraphRuntime(flow)
    with pytest.raises(interrupt, match="cleanup interruption"):
        runtime.process(FlowRecord(0))
    assert runtime.processed_inputs == 0
