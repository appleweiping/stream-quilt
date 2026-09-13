"""Public boundary admission and isolated lifetime/output allocation failures."""

import hashlib
import importlib.util
import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

import stream_quilt.window_graph as module
from stream_quilt import (
    FlowEdge,
    FlowExecutionError,
    FlowRecord,
    FlowStep,
    FlowWindow,
    ValidationError,
    WindowFold,
    WindowFoldLimits,
    WindowGraphBatch,
    WindowGraphCheckpoint,
    WindowGraphDataflow,
    WindowGraphInput,
    WindowGraphRuntime,
)


def flow_for(suffix=None, finalize=None):
    fold = WindowFold(
        "window",
        "v1",
        width=10,
        initial=lambda: 0,
        fold=lambda state, value: state + value,
        finalize=finalize,
        limits=replace(WindowFoldLimits(), max_row_bytes=512),
    )
    nodes = (FlowStep("input", "map", lambda value: value), FlowWindow("window", fold))
    edges = (FlowEdge("input", "window"),)
    if suffix:
        nodes += (suffix,)
        edges += (FlowEdge("window", suffix.step_id),)
    return WindowGraphDataflow("boundaries", "v1", nodes, edges, entry="input")


def populated(suffix=None, finalize=None):
    flow = flow_for(suffix, finalize)
    runtime = WindowGraphRuntime(flow)
    runtime.process(WindowGraphInput(0, 1, FlowRecord(2, "a")))
    runtime.finish(next_position=1)
    return flow, runtime


@pytest.mark.parametrize(
    "change",
    [
        {"nodes": ()},
        {"nodes": (object(), object())},
        {"edges": []},
        {"edges": (object(),)},
        {"limits": {}},
        {"flow_id": True},
        {"entry": "absent"},
    ],
)
def test_configuration_rejects_wrong_exact_types_and_missing_entry(change):
    with pytest.raises(ValidationError):
        replace(flow_for(), **change)


def test_window_and_source_require_exact_owned_types():
    flow = flow_for()
    with pytest.raises(ValidationError):
        FlowWindow("window", {})
    with pytest.raises(ValidationError):
        WindowGraphInput(0, 0, {"key": "a", "value": 1})
    runtime = WindowGraphRuntime(flow)
    with pytest.raises(ValidationError):
        runtime.process(FlowRecord(1, "a"))
    with pytest.raises(ValidationError):
        WindowGraphRuntime.from_checkpoint(flow, {})


@pytest.mark.parametrize("slot", ["outputs", "output", "status", "drained"])
def test_batch_exact_container_and_metadata_bounds(slot):
    runtime = WindowGraphRuntime(flow_for())
    result = runtime.drain()
    values = {"outputs": (), "operation_sequence": 0, "next_position": 0, "status": result.status}
    if slot == "outputs":
        values["outputs"] = []
    elif slot == "output":
        values["outputs"] = (FlowRecord(1),)
    elif slot == "status":
        values["status"] = {}
    else:
        values["drained_windows"] = 100001
    with pytest.raises(ValidationError):
        WindowGraphBatch(**values)


@pytest.mark.parametrize("slot", ["drains", "emitted_windows"])
def test_isolated_saturated_drain_counters_fail_before_finalizer(slot):
    # Fault-inject one counter to isolate its local admission boundary. This is
    # deliberately not offered as a reachable checkpoint or historical oracle.
    calls = []
    _, runtime = populated(finalize=lambda value: calls.append(value) or value)
    if slot == "drains":
        runtime._state = replace(runtime._state, drains=2**53 - 1)
    else:
        runtime._state = replace(
            runtime._state, window=replace(runtime._state.window, emitted_windows=2**53 - 1)
        )
    before = runtime._state
    with pytest.raises(ValidationError):
        runtime.drain()
    assert runtime._state is before
    assert calls == []


@pytest.mark.parametrize("keep", [False, True])
def test_terminal_lifetime_admission_counts_actual_output_not_selected_rows(keep):
    _, runtime = populated(FlowStep("suffix", "filter", lambda row: keep))
    # As above, isolate the local output counter. Valid full-history checkpoint
    # invariants are covered separately; do not manufacture billions of arrivals.
    runtime._state = replace(runtime._state, emitted=2**53 - 1)
    before = runtime._state
    if keep:
        with pytest.raises(ValidationError, match="terminal emissions"):
            runtime.drain()
        assert runtime._state is before
    else:
        result = runtime.drain()
        assert result.outputs == ()
        assert result.drained_windows == 1
        assert result.status.phase == "closed"
        assert runtime._state.emitted == 2**53 - 1


def test_transaction_commit_allocation_failure_retains_entire_pending_batch(monkeypatch):
    _, runtime = populated(FlowStep("suffix", "map", lambda row: row))
    before = runtime.checkpoint()

    def fail(transaction):
        raise MemoryError("copying proposed ordinary state")

    with monkeypatch.context() as patch:
        patch.setattr(module._FlowTransaction, "commit", fail)
        with pytest.raises(MemoryError):
            runtime.drain()
    assert runtime.checkpoint() == before


def test_awaitable_suffix_is_closed_without_publishing_selected_window():
    returned = []

    async def misplaced():
        return 1

    def callback(row):
        result = misplaced()
        returned.append(result)
        return result

    _, runtime = populated(FlowStep("suffix", "map", callback))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.drain()
    assert runtime.checkpoint() == before
    assert len(returned) == 1
    assert returned[0].cr_frame is None


@pytest.mark.parametrize("slot", ["kind", "version", "operator", "nodes", "edges", "normalization"])
def test_rehashed_configuration_corruption_is_not_a_callback_loader(slot):
    flow, runtime = populated()
    document = deepcopy(runtime.checkpoint().to_dict())
    config = document["body"]["configuration"]
    if slot in ("kind", "version"):
        config[slot] = "unsupported"
    elif slot == "operator":
        config["nodes"][0]["operator"] = "import_python"
    elif slot in ("nodes", "edges"):
        config[slot] = None
    else:
        config["nodes"][1]["fold"]["hop"] = None
    encoded = json.dumps(config, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    document["body"]["identity"] = hashlib.sha256(encoded.encode()).hexdigest()
    encoded = json.dumps(
        document["body"], sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    document["sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    with pytest.raises(ValidationError):
        WindowGraphCheckpoint(document)
    assert WindowGraphRuntime.from_checkpoint(flow, runtime.checkpoint()).status.phase == "draining"


def test_checkpoint_forged_noncanonical_text_and_wrong_checksums_reject():
    _, runtime = populated()
    document = runtime.checkpoint().to_dict()
    document["sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="checksum"):
        WindowGraphCheckpoint(document)
    point = runtime.checkpoint()
    spaced = json.dumps(point.to_dict(), sort_keys=True, ensure_ascii=False)
    with pytest.raises(ValidationError, match="canonical"):
        WindowGraphCheckpoint.from_json(spaced)
    object.__setattr__(point, "_json", spaced)
    with pytest.raises(ValidationError, match="canonical"):
        point.to_dict()


def test_real_pipeline_example_checks_every_field_and_cleans_owned_files():
    path = Path(__file__).resolve().parents[1] / "examples" / "window_graph.py"
    spec = importlib.util.spec_from_file_location("window_graph_example", path)
    assert spec is not None and spec.loader is not None
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    report = example.run_example()
    assert report["checkpoint_file_restarts"] == 4
    assert report["operations"] == 14
    assert len(report["outputs"]) == 8
