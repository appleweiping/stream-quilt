"""Actual prefix/window/suffix transactions and ownership/admission boundaries."""

import itertools
import json
from dataclasses import replace

import pytest

import stream_quilt.window_graph as module
from stream_quilt import (
    FlowEdge,
    FlowExecutionError,
    FlowLimits,
    FlowRecord,
    FlowStep,
    FlowWindow,
    GraphDataflow,
    GraphLimits,
    GraphRuntime,
    StateFlatUpdate,
    StateUpdate,
    ValidationError,
    WindowFold,
    WindowFoldExecutionError,
    WindowFoldLimits,
    WindowGraphBatch,
    WindowGraphCheckpoint,
    WindowGraphDataflow,
    WindowGraphInput,
    WindowGraphLimits,
    WindowGraphRuntime,
)


def identity(value):
    return value


def fold_spec(**changes):
    values = {
        "width": 10,
        "initial": lambda: 0,
        "fold": lambda state, value: state + value,
        "limits": replace(WindowFoldLimits(), max_row_bytes=512, max_batch_bytes=2048),
    }
    values.update(changes)
    return WindowFold("window", "v1", **values)


def linear_flow(*, spec=None, prefix=None, suffix=None, limits=None):
    nodes = (
        prefix or FlowStep("input", "map", identity),
        FlowWindow("window", spec or fold_spec()),
    )
    if suffix is not None:
        nodes += (suffix,)
    return WindowGraphDataflow(
        "windows",
        "v1",
        nodes,
        tuple(FlowEdge(left.step_id, right.step_id) for left, right in itertools.pairwise(nodes)),
        entry=nodes[0].step_id,
        limits=limits or WindowGraphLimits(),
    )


def put(runtime, timestamp, value=1, key="a"):
    return runtime.process(
        WindowGraphInput(runtime.next_position, timestamp, FlowRecord(value, key))
    )


def restart(runtime, flow):
    point = runtime.checkpoint()
    other = WindowGraphRuntime.from_checkpoint(
        flow, WindowGraphCheckpoint.from_json(point.to_json().encode())
    )
    assert other.checkpoint() == point
    return other


def test_missing_public_window_graph_api_is_now_functional():
    flow = linear_flow()
    runtime = WindowGraphRuntime(flow)
    assert isinstance(runtime.process(WindowGraphInput(0, 3, FlowRecord(7, "a"))), WindowGraphBatch)
    runtime = restart(runtime, flow)
    assert runtime.advance_watermark(10, next_position=1).status.phase == "draining"
    assert runtime.drain().outputs[0].to_dict() == {
        "step_id": "window",
        "key": "a",
        "value": {
            "index": 0,
            "start": 0,
            "end": 10,
            "tick_unit": "tick",
            "input_count": 1,
            "value": 7,
        },
    }
    assert runtime.status.phase == "open"
    runtime.finish(next_position=1)
    assert restart(runtime, flow).status.phase == "closed"


@pytest.mark.parametrize("policy", ["reject", "drop"])
def test_late_policy_is_window_local_with_whole_prefix_rollback(policy):
    prefix = FlowStep(
        "prefix", "stateful_map", lambda value, state: StateUpdate(state + 1, value), lambda: 0
    )
    flow = linear_flow(spec=fold_spec(late_policy=policy), prefix=prefix)
    runtime = WindowGraphRuntime(flow)
    runtime.advance_watermark(5, next_position=0)
    before = runtime.checkpoint()
    if policy == "reject":
        with pytest.raises(FlowExecutionError) as raised:
            put(runtime, 4)
        assert raised.value.step_id == "window"
        assert runtime.checkpoint() == before
    else:
        result = put(runtime, 4)
        assert result.late_dropped_inputs == 1
        body = runtime.checkpoint().to_dict()["body"]
        assert body["ordinary_cells"] == [{"step_id": "prefix", "key": "a", "state": "1"}]
        assert body["window"]["body"]["cells"] == []
        assert body["next_position"] == 1
    restart(runtime, flow)


def test_filtered_late_source_never_reaches_window_and_empty_is_not_eof():
    flow = linear_flow(prefix=FlowStep("filter", "filter", lambda value: False))
    runtime = WindowGraphRuntime(flow)
    runtime.advance_watermark(10, next_position=0)
    result = put(runtime, -10, key=None)
    assert result.folded_inputs == result.late_dropped_inputs == result.gap_inputs == 0
    assert result.next_position == 1
    assert runtime.status.phase == "open"
    assert (
        runtime.checkpoint().to_dict()["body"]["window"]["body"]["counters"]["processed_inputs"]
        == 0
    )


@pytest.mark.parametrize("policy", ["reject", "drop"])
@pytest.mark.parametrize("value,key", [(1, None), ("x" * 20, "a")])
def test_window_record_admission_precedes_late_policy(policy, value, key):
    spec = fold_spec(
        late_policy=policy, limits=replace(WindowFoldLimits(), max_input_bytes=5, max_row_bytes=512)
    )
    runtime = WindowGraphRuntime(linear_flow(spec=spec))
    runtime.advance_watermark(10, next_position=0)
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        put(runtime, 0, value, key)
    assert runtime.checkpoint() == before


def test_later_expanded_membership_failure_rolls_back_prefix_and_earlier_fold():
    calls = []

    def folder(state, value):
        calls.append(value)
        if value == 9:
            raise ValueError("second expanded record")
        return state + value

    prefix = FlowStep(
        "prefix",
        "stateful_flat_map",
        lambda value, state: StateFlatUpdate(state + 1, [value, 9]),
        lambda: 0,
    )
    runtime = WindowGraphRuntime(linear_flow(spec=fold_spec(fold=folder), prefix=prefix))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        put(runtime, 2, 4)
    assert calls == [4, 9]
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("location", ["finalizer", "suffix"])
def test_later_drain_failure_retains_every_window_and_downstream_state(location):
    enabled = {"fail": True}

    def finalize(value):
        if location == "finalizer" and enabled["fail"] and value == 2:
            raise ValueError("late finalizer")
        return value

    def suffix(value, state):
        if location == "suffix" and enabled["fail"] and value["value"] == 2:
            raise ValueError("late suffix")
        return StateUpdate(state + value["value"], state + value["value"])

    flow = linear_flow(
        spec=fold_spec(finalize=finalize),
        suffix=FlowStep("total", "stateful_map", suffix, lambda: 0),
    )
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0, 1)
    put(runtime, 10, 2)
    runtime.finish(next_position=2)
    before = runtime.checkpoint()
    with pytest.raises(
        WindowFoldExecutionError if location == "finalizer" else FlowExecutionError
    ) as error:
        runtime.drain(max_windows=2)
    if location == "suffix":
        assert error.value.step_id == "total"
    assert runtime.checkpoint() == before
    enabled["fail"] = False
    assert [output.record.value for output in runtime.drain(max_windows=2).outputs] == [1, 3]
    restart(runtime, flow)


@pytest.mark.parametrize(
    "which",
    [
        "status",
        "next_position",
        "operation_sequence",
        "checkpoint",
        "process",
        "advance",
        "finish",
        "drain",
    ],
)
def test_callback_reentry_including_public_reads_is_rejected(which):
    runtime = None

    def folder(state, value):
        if which in ("status", "next_position", "operation_sequence"):
            getattr(runtime, which)
        elif which == "process":
            runtime.process(WindowGraphInput(0, 0, FlowRecord(1, "a")))
        elif which == "advance":
            runtime.advance_watermark(0, next_position=0)
        elif which == "finish":
            runtime.finish(next_position=0)
        else:
            getattr(runtime, which)()
        return state + value

    runtime = WindowGraphRuntime(linear_flow(spec=fold_spec(fold=folder)))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        put(runtime, 0)
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("control", [KeyboardInterrupt("primary"), SystemExit("primary")])
@pytest.mark.parametrize("location", ["fold", "finalizer", "suffix"])
def test_control_identity_and_whole_transaction_ownership(control, location):
    def fail(*args):
        raise control

    spec = fold_spec(
        **(
            {"fold": fail}
            if location == "fold"
            else {"finalize": fail}
            if location == "finalizer"
            else {}
        )
    )
    runtime = WindowGraphRuntime(
        linear_flow(
            spec=spec, suffix=FlowStep("sink", "map", fail) if location == "suffix" else None
        )
    )
    if location != "fold":
        put(runtime, 0)
        runtime.finish(next_position=1)
    before = runtime.checkpoint()
    with pytest.raises(type(control)) as caught:
        put(runtime, 0) if location == "fold" else runtime.drain()
    assert caught.value is control
    assert runtime.checkpoint() == before


def test_native_suffix_generator_preserves_primary_control_when_close_also_controls(monkeypatch):
    primary, cleanup = KeyboardInterrupt("original"), SystemExit("cleanup")

    def outputs(value):
        try:
            yield 999
        finally:
            raise cleanup

    original_charge = module._Work.charge

    def charge(work, record):
        if record.value == 999:
            raise primary
        original_charge(work, record)

    flow = linear_flow(suffix=FlowStep("out", "flat_map", outputs))
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0)
    runtime.finish(next_position=1)
    before = runtime.checkpoint()
    with monkeypatch.context() as patch:
        patch.setattr(module._Work, "charge", charge)
        with pytest.raises(KeyboardInterrupt) as caught:
            runtime.drain()
    assert caught.value is primary
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("field,limit", [("max_state_cells", 1), ("max_state_bytes", 1)])
def test_aggregate_state_admission_prevents_new_window_callbacks(field, limit):
    called = []
    prefix = FlowStep(
        "prefix", "stateful_map", lambda value, state: StateUpdate(1, value), lambda: 0
    )
    spec = fold_spec(initial=lambda: called.append("initial") or 0)
    runtime = WindowGraphRuntime(
        linear_flow(spec=spec, prefix=prefix, limits=replace(WindowGraphLimits(), **{field: limit}))
    )
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        put(runtime, 0)
    assert called == []
    assert runtime.checkpoint() == before


def test_shared_callback_budget_checks_before_the_excess_folder():
    called = []
    spec = fold_spec(
        initial=lambda: called.append("initial") or 0,
        fold=lambda state, value: called.append("fold") or value,
    )
    limits = WindowGraphLimits(
        graph=GraphLimits(operator_limits=replace(FlowLimits(), max_calls_per_input=2))
    )
    runtime = WindowGraphRuntime(linear_flow(spec=spec, limits=limits))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        put(runtime, 0)
    assert called == ["initial"]
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("slot", ["_State", "WindowGraphBatch"])
def test_return_and_state_allocation_precede_publication(monkeypatch, slot):
    runtime = WindowGraphRuntime(linear_flow())
    before = runtime.checkpoint()

    def fail(*args, **kwargs):
        raise MemoryError("allocation boundary")

    with monkeypatch.context() as patch:
        patch.setattr(module, slot, fail)
        with pytest.raises(MemoryError):
            put(runtime, 0)
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("operation", ["advance", "finish", "drain"])
def test_progress_and_drain_return_allocation_precede_publication(monkeypatch, operation):
    runtime = WindowGraphRuntime(linear_flow())
    put(runtime, 0)
    if operation == "drain":
        runtime.finish(next_position=1)
    before = runtime.checkpoint()

    def fail(*args, **kwargs):
        raise MemoryError("batch allocation")

    with monkeypatch.context() as patch:
        patch.setattr(module, "WindowGraphBatch", fail)
        with pytest.raises(MemoryError):
            if operation == "advance":
                runtime.advance_watermark(10, next_position=1)
            elif operation == "finish":
                runtime.finish(next_position=1)
            else:
                runtime.drain()
    assert runtime.checkpoint() == before


def test_invalid_profile_and_old_runtime_acceptance_stay_separate():
    flow = linear_flow()
    with pytest.raises(ValidationError):
        GraphRuntime(flow)
    with pytest.raises(ValidationError):
        GraphDataflow(flow.flow_id, flow.revision, flow.nodes, flow.edges, flow.entry)
    with pytest.raises(ValidationError):
        WindowGraphRuntime(GraphDataflow("old", "v1", (FlowStep("m", "map", identity),), (), "m"))
    bypass = FlowStep("bypass", "map", identity)
    with pytest.raises(ValidationError, match="dominate"):
        replace(flow, nodes=(*flow.nodes, bypass), edges=(*flow.edges, FlowEdge("input", "bypass")))
    with pytest.raises(ValidationError):
        replace(flow, nodes=list(flow.nodes))
    with pytest.raises(ValidationError):
        replace(flow, entry="window")
    with pytest.raises(ValidationError):
        FlowWindow("wrong", fold_spec())


def test_batch_and_checkpoint_reads_do_not_alias_retained_state():
    spec = fold_spec(initial=list, fold=lambda state, value: [*state, value])
    flow = linear_flow(spec=spec)
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0, {"x": [1]})
    saved = runtime.checkpoint()
    document = saved.to_dict()
    document["body"]["window"]["body"]["cells"][0]["state"] = "null"
    assert runtime.checkpoint() == saved
    runtime.finish(next_position=1)
    batch = runtime.drain()
    value = batch.outputs[0].record.value
    value["value"][0]["x"].append(99)
    assert batch.outputs[0].record.value["value"] == [{"x": [1]}]
    assert json.loads(saved.to_json()) != document
