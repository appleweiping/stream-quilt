"""Prerequisite: native output cleanup must not replace an original control."""

import pytest

import stream_quilt.dataflow as module
from stream_quilt import (
    Dataflow,
    FlowExecutionError,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    GraphDataflow,
    GraphRuntime,
    StateFlatUpdate,
)


def runtime_for(step, graph):
    if graph:
        return GraphRuntime(GraphDataflow("cleanup", "v1", (step,), (), step.step_id))
    return FlowRuntime(Dataflow("cleanup", "v1", (step,)))


@pytest.mark.parametrize("graph", [False, True])
@pytest.mark.parametrize("operator", ["flat_map", "stateful_flat_map"])
@pytest.mark.parametrize("primary_type", [ValueError, KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("cleanup_type", [ValueError, KeyboardInterrupt, SystemExit])
def test_original_exception_priority_across_owned_native_output_cleanup(
    monkeypatch, graph, operator, primary_type, cleanup_type
):
    primary, cleanup = primary_type("original"), cleanup_type("cleanup")
    calls, owners = [], []

    def values():
        try:
            calls.append("entered")
            yield "emission-boundary"
        finally:
            calls.append("closed")
            raise cleanup

    def callback(value, state=None):
        iterator = values()
        owners.append(iterator)
        return StateFlatUpdate(1, iterator) if operator == "stateful_flat_map" else iterator

    step = FlowStep(
        "expand", operator, callback, (lambda: 0) if operator == "stateful_flat_map" else None
    )
    runtime = runtime_for(step, graph)
    record, before = FlowRecord(1, "a"), runtime.checkpoint()
    snapshot = module._snapshot

    def fail_emission(value, maximum):
        if value == "emission-boundary":
            raise primary
        return snapshot(value, maximum)

    expected = (
        cleanup
        if isinstance(primary, Exception) and not isinstance(cleanup, Exception)
        else primary
    )
    expected_type = FlowExecutionError if isinstance(expected, Exception) else type(expected)
    with monkeypatch.context() as patch:
        patch.setattr(module, "_snapshot", fail_emission)
        with pytest.raises(BaseException) as raised:
            runtime.process(record)
    assert isinstance(raised.value, expected_type)
    if not isinstance(expected, Exception):
        assert raised.value is expected
    if expected is primary:
        assert "output generator cleanup also failed" in str(primary.__notes__)
    assert calls == ["entered", "closed"]
    assert owners[0].gi_frame is None
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("cleanup_type", [ValueError, KeyboardInterrupt, SystemExit])
def test_fresh_cleanup_exception_propagates_without_primary(cleanup_type):
    cleanup = cleanup_type("fresh cleanup")
    calls = []

    def values():
        try:
            yield 1
        finally:
            calls.append("closed")
            raise cleanup

    owner = values()
    with pytest.raises(cleanup_type) as raised, module._flat_values(owner) as open_iterator:
        assert next(open_iterator()) == 1
    assert raised.value is cleanup
    assert calls == ["closed"]
    assert owner.gi_frame is None


@pytest.mark.parametrize("graph", [False, True])
@pytest.mark.parametrize("primary_type", [ValueError, KeyboardInterrupt, SystemExit])
def test_failed_state_admission_closes_unentered_generator_without_running_its_body(
    monkeypatch, graph, primary_type
):
    primary, owners, calls = primary_type("proposal"), [], []

    def values():
        try:
            calls.append("entered")
            yield 1
        finally:
            calls.append("closed")
            raise SystemExit("must never run")

    def callback(value, state):
        owner = values()
        owners.append(owner)
        return StateFlatUpdate("state-boundary", owner)

    runtime = runtime_for(FlowStep("expand", "stateful_flat_map", callback, lambda: 0), graph)
    before, record, original = runtime.checkpoint(), FlowRecord(1, "a"), module._snapshot

    def fail_state(value, maximum):
        if value == "state-boundary":
            raise primary
        return original(value, maximum)

    with monkeypatch.context() as patch:
        patch.setattr(module, "_snapshot", fail_state)
        with pytest.raises(
            FlowExecutionError if isinstance(primary, Exception) else primary_type
        ) as error:
            runtime.process(record)
    if not isinstance(primary, Exception):
        assert error.value is primary
    assert calls == []
    assert owners[0].gi_frame is None
    assert runtime.checkpoint() == before
