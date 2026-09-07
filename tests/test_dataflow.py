from __future__ import annotations

import itertools
import json
import math
import runpy
from dataclasses import replace
from pathlib import Path

import pytest

from stream_quilt.dataflow import (
    Dataflow,
    FlowCheckpoint,
    FlowExecutionError,
    FlowLimits,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    StateUpdate,
)
from stream_quilt.errors import ValidationError


def running_sum(value, state):
    return StateUpdate(state + value, state + value)


def sums(*extra, limits=None):
    return Dataflow(
        "totals",
        "v1",
        (FlowStep("sum", "stateful_map", running_sum, lambda: 0), *extra),
        FlowLimits() if limits is None else limits,
    )


def values(records):
    return [(record.key, record.value) for record in records]


def test_real_operator_sequence_preserves_order_and_input_isolation():
    flow = Dataflow(
        "orders",
        "1",
        (
            FlowStep("words", "flat_map", lambda value: value.split()),
            FlowStep("long", "filter", lambda value: len(value) > 1),
            FlowStep("upper", "map", str.upper),
            FlowStep("key", "key_by", lambda value: value[0]),
        ),
    )
    runtime = FlowRuntime(flow)
    result = runtime.process(FlowRecord("a bee ant be"))
    assert values(result) == [("B", "BEE"), ("A", "ANT"), ("B", "BE")]
    assert runtime.processed_inputs == 1 and runtime.emitted_records == 3
    assert runtime.checkpoint().cells == ()
    assert values(
        FlowRuntime(Dataflow("unkey", "1", (FlowStep("clear", "drop_key"),))).process(result[0])
    ) == [(None, "BEE")]


def test_independent_keys_and_repeated_key_in_one_expansion():
    flow = Dataflow(
        "duplicates",
        "1",
        (
            FlowStep("twice", "flat_map", lambda value: (value, value)),
            FlowStep("sum", "stateful_map", running_sum, lambda: 0),
        ),
    )
    runtime = FlowRuntime(flow)
    assert values(runtime.process(FlowRecord(3, "a"))) == [("a", 3), ("a", 6)]
    assert values(runtime.process(FlowRecord(10, "b"))) == [("b", 10), ("b", 20)]
    assert values(runtime.process(FlowRecord(1, "a"))) == [("a", 7), ("a", 8)]


def test_snapshot_restart_matches_manual_running_totals():
    flow = sums()
    runtime = FlowRuntime(flow)
    assert values(runtime.process(FlowRecord(2, "a"))) == [("a", 2)]
    snapshot = runtime.checkpoint()
    document = json.loads(json.dumps(snapshot.to_dict(), allow_nan=False))
    restored = FlowRuntime.from_checkpoint(flow, FlowCheckpoint.from_dict(document))
    document["cells"][0]["value"] = 999
    assert values(restored.process(FlowRecord(3, "a"))) == [("a", 5)]
    assert values(restored.process(FlowRecord(7, "b"))) == [("b", 7)]
    assert restored.processed_inputs == 3
    assert snapshot.cells[0][2] == "2"
    with pytest.raises(ValidationError, match="identity"):
        FlowRuntime.from_checkpoint(replace(flow, revision="v2"), snapshot)


def test_failure_after_state_update_rolls_back_entire_input():
    def reject_large(value):
        if value > 5:
            raise RuntimeError("secret output must not escape")
        return value

    runtime = FlowRuntime(sums(FlowStep("guard", "map", reject_large)))
    runtime.process(FlowRecord(3, "a"))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError) as error:
        runtime.process(FlowRecord(4, "a"))
    assert error.value.step_id == "guard" and "secret" not in str(error.value)
    assert runtime.checkpoint() == before
    assert values(runtime.process(FlowRecord(2, "a"))) == [("a", 5)]


def test_rejected_input_does_not_leave_new_keys_or_initializer_mutation():
    shared = []

    def append(value, state):
        state.append(value)
        return StateUpdate(state, state)

    def invalid(value):
        return math.inf

    flow = Dataflow(
        "lists",
        "1",
        (
            FlowStep("append", "stateful_map", append, lambda: shared),
            FlowStep("bad", "map", invalid),
        ),
    )
    runtime = FlowRuntime(flow)
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1, "a"))
    assert shared == [] and runtime.checkpoint().cells == ()


def test_suppressed_output_and_explicit_state_deletion():
    def update(value, state):
        if value == "clear":
            return StateUpdate(None, emit=False, retain=False)
        return StateUpdate(value, state, emit=value != 0)

    runtime = FlowRuntime(
        Dataflow("delete", "1", (FlowStep("state", "stateful_map", update, lambda: None),))
    )
    assert runtime.process(FlowRecord(0, "a")) == ()
    assert values(runtime.process(FlowRecord(1, "a"))) == [("a", 0)]
    assert runtime.process(FlowRecord("clear", "a")) == ()
    assert runtime.checkpoint().cells == ()
    assert values(runtime.process(FlowRecord(2, "a"))) == [("a", None)]


def test_record_and_return_values_are_defensive_copies():
    data = {"x": [1]}
    record = FlowRecord(data, "key")
    data["x"].append(2)
    record.value["x"].append(3)
    assert record.value == {"x": [1]}
    runtime = FlowRuntime(Dataflow("copy", "1", (FlowStep("same", "map", lambda value: value),)))
    result = runtime.process(record)[0]
    result.value["x"].append(4)
    assert result.value == {"x": [1]} and record.value == {"x": [1]}


def test_pull_boundary_and_early_close_do_not_consume_extra_source():
    pulled = []

    def source():
        for index in range(5):
            pulled.append(index)
            yield FlowRecord(index)

    runtime = FlowRuntime(
        Dataflow("pull", "1", (FlowStep("repeat", "flat_map", lambda value: (value, value)),))
    )
    iterator = source()
    outputs = runtime.run(iterator, max_inputs=2)
    assert next(outputs).value == 0 and pulled == [0]
    assert next(outputs).value == 0 and pulled == [0]
    assert values(outputs) == [(None, 1), (None, 1)]
    assert pulled == [0, 1]
    assert next(iterator).value == 2
    outputs = runtime.run(iterator)
    assert next(outputs).value == 3
    outputs.close()
    assert pulled == [0, 1, 2, 3]
    assert values(runtime.run(())) == []


@pytest.mark.parametrize(
    "limit,value", [("max_state_keys", 1), ("max_state_bytes", 2), ("max_state_value_bytes", 1)]
)
def test_state_limit_failure_is_atomic(limit, value):
    runtime = FlowRuntime(sums(limits=replace(FlowLimits(), **{limit: value})))
    runtime.process(FlowRecord(1, "a"))
    before = runtime.checkpoint()
    record = FlowRecord(20, "b")
    with pytest.raises(FlowExecutionError):
        runtime.process(record)
    assert runtime.checkpoint() == before


@pytest.mark.parametrize(
    "limits",
    [
        FlowLimits(max_records_per_input=2),
        FlowLimits(max_batch_bytes=2),
        FlowLimits(max_calls_per_input=1),
    ],
)
def test_expansion_and_work_budgets_rollback(limits):
    flow = Dataflow(
        "budget",
        "1",
        (
            FlowStep("many", "flat_map", lambda value: (value, value, value)),
            FlowStep("copy", "map", lambda value: value),
        ),
        limits,
    )
    runtime = FlowRuntime(flow)
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1))
    assert runtime.checkpoint().processed_inputs == 0


def test_infinite_flat_map_is_closed_at_expansion_boundary():
    closed = []

    def expand(value):
        try:
            yield from itertools.repeat(value)
        finally:
            closed.append(True)

    runtime = FlowRuntime(
        Dataflow(
            "infinite",
            "1",
            (FlowStep("many", "flat_map", expand),),
            FlowLimits(max_records_per_input=2),
        )
    )
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1))
    assert closed == [True] and runtime.processed_inputs == 0


@pytest.mark.parametrize(
    "step",
    [
        FlowStep("predicate", "filter", lambda value: 1),
        FlowStep("expand", "flat_map", lambda value: "abc"),
        FlowStep("expand", "flat_map", lambda value: {}),
        FlowStep("expand", "flat_map", lambda value: 42),
        FlowStep("key", "key_by", lambda value: 1),
        FlowStep("state", "stateful_map", lambda value, state: {}, lambda: 0),
    ],
)
def test_callback_result_contracts(step):
    runtime = FlowRuntime(Dataflow("contracts", "1", (step,)))
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1, "a"))
    assert runtime.processed_inputs == 0


@pytest.mark.parametrize("operation", ["process", "checkpoint"])
def test_keyed_operator_requires_key_and_reentrancy_is_rejected(operation):
    runtime = FlowRuntime(sums())
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1))

    def callback(value):
        if operation == "process":
            return runtime.process(FlowRecord(value))
        return runtime.checkpoint()

    runtime = FlowRuntime(Dataflow("reentrant", "1", (FlowStep("call", "map", callback),)))
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1))
    assert runtime.processed_inputs == 0


def test_base_exception_rolls_back_and_does_not_poison_runtime():
    def exit_task(value):
        if value:
            raise SystemExit(7)
        return value

    runtime = FlowRuntime(Dataflow("exit", "1", (FlowStep("exit", "map", exit_task),)))
    with pytest.raises(SystemExit):
        runtime.process(FlowRecord(1))
    assert values(runtime.process(FlowRecord(0))) == [(None, 0)]


def test_sync_contract_rejects_async_function_and_closes_returned_coroutine():
    async def value():
        return 1

    with pytest.raises(ValidationError):
        FlowStep("bad", "map", value)
    runtime = FlowRuntime(
        Dataflow("async-result", "1", (FlowStep("bad", "map", lambda _: value()),))
    )
    with pytest.raises(FlowExecutionError):
        runtime.process(FlowRecord(1))


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "wrong"},
        {"schema_version": "2"},
        {"extra": 1},
        {"cells": "bad"},
        {"cells": [{}]},
        {"processed_inputs": True},
        {"emitted_records": -1},
        {"identity": "x"},
    ],
)
def test_strict_checkpoint_document(change):
    document = FlowRuntime(sums()).checkpoint().to_dict()
    document.update(change)
    with pytest.raises(ValidationError):
        FlowCheckpoint.from_dict(document)


def test_checkpoint_rejects_duplicate_unknown_and_noncanonical_cells():
    runtime = FlowRuntime(sums())
    runtime.process(FlowRecord(1, "a"))
    snapshot = runtime.checkpoint()
    with pytest.raises(ValidationError, match="duplicate"):
        replace(snapshot, cells=snapshot.cells * 2)
    for encoded in ("NaN", "1e999", "1 ", '{"a":1,"a":2}', "\ud800", 1):
        with pytest.raises(ValidationError):
            replace(snapshot, cells=(("sum", "a", encoded),))
    with pytest.raises(ValidationError, match="incompatible"):
        FlowRuntime.from_checkpoint(sums(), replace(snapshot, cells=(("missing", "a", "1"),)))
    with pytest.raises(ValidationError):
        FlowCheckpoint.from_dict([])


@pytest.mark.parametrize("value", [math.inf, math.nan, 2**53, {1: "bad"}, b"bytes", "\ud800"])
def test_non_json_records(value):
    with pytest.raises(ValidationError):
        FlowRecord(value)


@pytest.mark.parametrize("key", ["", " padded", "line\nbreak", 1, "\ud800"])
def test_invalid_key(key):
    with pytest.raises(ValidationError):
        FlowRecord(1, key)


@pytest.mark.parametrize(
    "kwargs",
    [{"max_calls_per_input": 0}, {"max_state_keys": True}, {"max_batch_bytes": 100_000_000}],
)
def test_invalid_limits(kwargs):
    with pytest.raises(ValidationError):
        FlowLimits(**kwargs)


def test_configuration_and_input_validation():
    with pytest.raises(ValidationError):
        FlowStep("bad", [])
    with pytest.raises(ValidationError):
        FlowStep("bad", "drop_key", lambda value: value)
    with pytest.raises(ValidationError):
        FlowStep("bad", "map", lambda value: value, lambda: 0)
    with pytest.raises(ValidationError):
        Dataflow("empty", "1", ())
    step = FlowStep("x", "drop_key")
    with pytest.raises(ValidationError):
        Dataflow("duplicate", "1", (step, step))
    with pytest.raises(ValidationError):
        Dataflow("bad", "1", (None,))
    with pytest.raises(ValidationError):
        Dataflow("bad", "1", (step,), {})
    with pytest.raises(ValidationError):
        FlowRuntime(None)
    with pytest.raises(ValidationError):
        FlowRuntime.from_checkpoint(sums(), {})
    runtime = FlowRuntime(sums(limits=FlowLimits(max_record_bytes=1)))
    with pytest.raises(ValidationError):
        runtime.process(FlowRecord(20))
    with pytest.raises(ValidationError):
        runtime.process(20)
    with pytest.raises(ValidationError):
        list(runtime.run((), max_inputs=0))
    with pytest.raises(ValidationError):
        StateUpdate(1, emit=1)


@pytest.mark.parametrize("limit", ["max_state_keys", "max_state_bytes", "max_state_value_bytes"])
def test_restored_snapshot_cannot_bypass_configured_state_limits(limit):
    flow = sums(limits=replace(FlowLimits(), **{limit: 1}))
    cells = (
        (("sum", "a", "10"),)
        if limit == "max_state_value_bytes"
        else (("sum", "a", "1"), ("sum", "b", "2"))
    )
    checkpoint = replace(FlowRuntime(flow).checkpoint(), cells=cells)
    with pytest.raises(ValidationError):
        FlowRuntime.from_checkpoint(flow, checkpoint)


def test_output_byte_limit_uses_utf8_and_rolls_back():
    flow = sums(
        FlowStep("unicode", "map", lambda value: "é"), limits=FlowLimits(max_record_bytes=3)
    )
    runtime = FlowRuntime(flow)
    with pytest.raises(FlowExecutionError, match="unicode"):
        runtime.process(FlowRecord(1, "a"))
    assert runtime.checkpoint().cells == ()


def test_counter_exhaustion_does_not_commit_proposals():
    flow = sums()
    checkpoint = replace(FlowRuntime(flow).checkpoint(), processed_inputs=2**53 - 1)
    runtime = FlowRuntime.from_checkpoint(flow, checkpoint)
    with pytest.raises(ValidationError, match="processed_inputs"):
        runtime.process(FlowRecord(1, "a"))
    checkpoint = replace(checkpoint, processed_inputs=0, emitted_records=2**53 - 1)
    runtime = FlowRuntime.from_checkpoint(flow, checkpoint)
    with pytest.raises(ValidationError, match="emitted_records"):
        runtime.process(FlowRecord(1, "a"))
    assert runtime.checkpoint() == checkpoint


def test_executable_keyed_example(capsys):
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "keyed_totals.py"), run_name="__main__"
    )
    result = json.loads(capsys.readouterr().out)
    assert result["processed_inputs"] == 2
    assert result["results"][-2:] == [{"word": "blue", "count": 2}, {"word": "green", "count": 1}]


@pytest.mark.parametrize(
    "encoded",
    ["x" * (8 * 1024 * 1024 + 1), "é" * (4 * 1024 * 1024 + 1)],
    ids=["ascii-byte-limit", "multibyte-limit"],
)
def test_checkpoint_value_byte_limit_precedes_parsing(encoded):
    checkpoint = FlowRuntime(sums()).checkpoint()
    with pytest.raises(ValidationError, match="value byte limit"):
        replace(checkpoint, cells=(("sum", "a", encoded),))
