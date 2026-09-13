"""Shared work/callback/cell quotas, exact geometry and real branch progression."""

import itertools
from dataclasses import replace

import pytest

from stream_quilt import (
    FlowBranch,
    FlowEdge,
    FlowExecutionError,
    FlowLimits,
    FlowMerge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    GraphLimits,
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


def spec_for(**kwargs):
    options = {
        "width": 10,
        "initial": lambda: 0,
        "fold": lambda state, value: state + value,
        "limits": replace(WindowFoldLimits(), max_row_bytes=512, max_batch_bytes=2048),
    }
    options.update(kwargs)
    return WindowFold("window", "v1", **options)


def flow_for(spec=None, limits=None, suffix=None):
    nodes = (
        FlowStep("input", "map", lambda value: value),
        FlowWindow("window", spec or spec_for()),
    )
    if suffix is not None:
        nodes += (suffix,)
    return WindowGraphDataflow(
        "limits",
        "v1",
        nodes,
        tuple(FlowEdge(a.step_id, b.step_id) for a, b in itertools.pairwise(nodes)),
        entry="input",
        limits=limits or WindowGraphLimits(),
    )


def put(runtime, timestamp, value=1):
    return runtime.process(
        WindowGraphInput(runtime.next_position, timestamp, FlowRecord(value, "a"))
    )


@pytest.mark.parametrize("field", ["max_state_cells", "max_state_bytes", "max_source_inputs"])
@pytest.mark.parametrize("value", [0, -1, True, 1.0, "1", None, 2**54])
def test_new_limit_fields_are_bounded_exact_positive_integers(field, value):
    with pytest.raises(ValidationError):
        WindowGraphLimits(**{field: value})


@pytest.mark.parametrize("value", [None, {}, GraphLimits().to_dict()])
def test_nested_graph_limit_type_is_exact(value):
    with pytest.raises(ValidationError):
        WindowGraphLimits(graph=value)


@pytest.mark.parametrize("field,value", [("max_record_bytes", 511), ("max_batch_bytes", 511)])
def test_one_maximum_window_row_requires_explicit_node_reservation(field, value):
    limits = WindowGraphLimits(
        graph=GraphLimits(operator_limits=replace(FlowLimits(), **{field: value}))
    )
    with pytest.raises(ValidationError, match="reservation"):
        flow_for(limits=limits)


@pytest.mark.parametrize("field,value", [("max_work_records", 1), ("max_work_bytes", 1023)])
def test_one_maximum_window_row_including_fanout_must_fit(field, value):
    limits = WindowGraphLimits(graph=replace(GraphLimits(), **{field: value}))
    with pytest.raises(ValidationError, match="reservation"):
        flow_for(limits=limits, suffix=FlowStep("suffix", "map", lambda value: value))


@pytest.mark.parametrize("quota", ["work", "node_bytes"])
def test_drain_reserves_future_rows_before_any_finalizer(quota):
    calls = []
    spec = spec_for(finalize=lambda value: calls.append(value) or value)
    graph = (
        GraphLimits(max_work_records=3)
        if quota == "work"
        else GraphLimits(operator_limits=replace(FlowLimits(), max_batch_bytes=768))
    )
    flow = flow_for(
        spec, WindowGraphLimits(graph=graph), FlowStep("suffix", "map", lambda value: value)
    )
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0, 1)
    put(runtime, 10, 2)
    runtime.finish(next_position=2)
    result = runtime.drain(max_windows=100)
    assert result.drained_windows == 1
    assert calls == [1]
    assert result.status.pending_windows == 1
    assert runtime.drain().drained_windows == 1
    assert calls == [1, 2]
    WindowGraphRuntime.from_checkpoint(flow, runtime.checkpoint())


def test_shared_callback_quota_is_not_reset_per_drained_window():
    calls = []
    spec = spec_for(finalize=lambda value: calls.append(value) or value)
    limits = WindowGraphLimits(
        graph=GraphLimits(operator_limits=replace(FlowLimits(), max_calls_per_input=3))
    )
    flow = flow_for(spec, limits, FlowStep("suffix", "map", lambda value: value))
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0, 1)
    put(runtime, 10, 2)
    runtime.finish(next_position=2)
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.drain(max_windows=2)
    assert runtime.checkpoint() == before
    assert calls == [1, 2]
    assert runtime.drain(max_windows=1).drained_windows == 1
    assert runtime.drain(max_windows=1).drained_windows == 1


def test_shared_work_quota_is_not_reset_per_drained_window_and_retry_can_reduce_batch():
    # Prefix costs 3 records. One suffix row costs 4; two selected rows cannot
    # finish under 7 even though immediate reservations permit selecting two.
    spec = spec_for()
    suffix = FlowStep("suffix", "flat_map", lambda value: [value, value])
    flow = flow_for(spec, WindowGraphLimits(graph=GraphLimits(max_work_records=7)), suffix)
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0, 1)
    put(runtime, 10, 2)
    runtime.finish(next_position=2)
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.drain(max_windows=2)
    assert runtime.checkpoint() == before
    assert len(runtime.drain(max_windows=1).outputs) == 2
    assert len(runtime.drain(max_windows=1).outputs) == 2


def test_growing_window_uses_all_touched_old_cell_reservations():
    phase, calls = [0], [0]

    def folder(state, value):
        calls[0] += 1
        return "x" * (100 if calls[0] % 2 == phase[0] else 1)

    spec = spec_for(width=10, hop=5, fold=folder)
    probe = WindowGraphRuntime(flow_for(spec))
    put(probe, 0)
    size = probe._state.cell_bytes
    spec = replace(spec, limits=replace(spec.limits, max_state_bytes=size))
    flow = flow_for(spec, replace(WindowGraphLimits(), max_state_bytes=size))
    runtime = WindowGraphRuntime(flow)
    calls[0] = 0
    put(runtime, 0)
    phase[0] = 1
    put(runtime, 1)
    assert runtime._state.cell_bytes == size
    WindowGraphRuntime.from_checkpoint(flow, runtime.checkpoint())


def test_selected_retirements_can_fund_downstream_state_without_deleting_live_windows():
    suffix = FlowStep(
        "suffix",
        "stateful_map",
        lambda value, state: StateUpdate(value["value"], value["value"]),
        lambda: None,
    )
    spec = spec_for(fold=lambda state, value: value)
    probe = WindowGraphRuntime(flow_for(spec, suffix=suffix))
    put(probe, 0, "x" * 200)
    total = probe._state.cell_bytes
    flow = flow_for(spec, replace(WindowGraphLimits(), max_state_bytes=total), suffix)
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0, "x" * 200)
    runtime.finish(next_position=1)
    assert runtime.drain().outputs[0].record.value == "x" * 200
    assert runtime._state.cell_bytes <= total
    WindowGraphRuntime.from_checkpoint(flow, runtime.checkpoint())


def test_identity_window_row_record_adapter_failure_retains_selected_state():
    from stream_quilt.limits import MAX_JSON_DEPTH

    value = 1
    for _ in range(MAX_JSON_DEPTH - 1):
        value = [value]
    runtime = WindowGraphRuntime(flow_for(spec_for(fold=lambda state, value: value)))
    put(runtime, 0, value)
    runtime.finish(next_position=1)
    before = runtime.checkpoint()
    with pytest.raises(ValidationError, match="depth"):
        runtime.drain()
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("timestamp", [True, 1.0, "1", None, -(2**53), 2**53])
def test_source_timestamp_admission_precedes_prefix_callbacks(timestamp):
    calls = []
    flow = flow_for()
    flow = replace(
        flow,
        nodes=(FlowStep("input", "map", lambda value: calls.append(value) or value), flow.nodes[1]),
    )
    runtime = WindowGraphRuntime(flow)
    before = runtime.checkpoint()
    with pytest.raises(ValidationError):
        runtime.process(WindowGraphInput(0, timestamp, FlowRecord(1, "a")))
    assert calls == []
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("max_windows", [True, 0, -1, 1.0, "1", 100001])
def test_empty_drain_still_admits_exact_requested_scalar(max_windows):
    runtime = WindowGraphRuntime(flow_for())
    before = runtime.checkpoint()
    with pytest.raises(ValidationError):
        runtime.drain(max_windows=max_windows)
    assert runtime.checkpoint() == before


def test_position_eof_and_draining_barriers_precede_callbacks():
    calls = []
    spec = spec_for(fold=lambda state, value: calls.append(value) or value)
    flow = flow_for(spec)
    runtime = WindowGraphRuntime(flow)
    put(runtime, 0)
    runtime.advance_watermark(10, next_position=1)
    before = runtime.checkpoint()
    for operation in (
        lambda: put(runtime, 10),
        lambda: runtime.advance_watermark(10, next_position=1),
        lambda: runtime.finish(next_position=0),
    ):
        with pytest.raises(ValidationError):
            operation()
        assert runtime.checkpoint() == before
    assert calls == [1]
    runtime.finish(next_position=1)
    runtime.drain()
    with pytest.raises(ValidationError):
        put(runtime, 10)
    assert WindowGraphRuntime.from_checkpoint(flow, runtime.checkpoint()).status.phase == "closed"


def test_real_prefix_and_suffix_branches_restore_with_exact_edge_counts():
    fold = spec_for(initial=list, fold=lambda state, value: [*state, value])
    nodes = (
        FlowBranch("input", lambda value: value > 0),
        FlowStep("positive", "map", lambda value: value),
        FlowStep("negative", "map", lambda value: value),
        FlowMerge("merge"),
        FlowWindow("window", fold),
        FlowBranch("classify", lambda row: sum(row["value"]) >= 0),
        FlowStep("nonnegative", "map", lambda row: row["value"]),
        FlowStep("negative_sum", "map", lambda row: row["value"]),
    )
    edges = (
        FlowEdge("input", "positive", True),
        FlowEdge("input", "negative", False),
        FlowEdge("positive", "merge"),
        FlowEdge("negative", "merge"),
        FlowEdge("merge", "window"),
        FlowEdge("window", "classify"),
        FlowEdge("classify", "nonnegative", True),
        FlowEdge("classify", "negative_sum", False),
    )
    flow = WindowGraphDataflow("branches", "v1", nodes, edges, entry="input")
    runtime = WindowGraphRuntime(flow)
    for timestamp, value in [(2, 3), (1, -4), (11, 5)]:
        put(runtime, timestamp, value)
        runtime = WindowGraphRuntime.from_checkpoint(
            flow, WindowGraphCheckpoint.from_json(runtime.checkpoint().to_json())
        )
    runtime.finish(next_position=3)
    result = runtime.drain()
    assert [(output.step_id, output.record.value) for output in result.outputs] == [
        ("negative_sum", [3, -4]),
        ("nonnegative", [5]),
    ]
    assert runtime.checkpoint().to_dict()["body"]["edge_counts"] == [2, 1, 2, 1, 3, 2, 1, 1]
    WindowGraphRuntime.from_checkpoint(flow, runtime.checkpoint())
