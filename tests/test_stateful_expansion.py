from __future__ import annotations

import itertools
import random
import sys
from dataclasses import replace

import pytest

from stream_quilt import (
    Dataflow,
    FlowEdge,
    FlowExecutionError,
    FlowJournal,
    FlowLimits,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    GraphDataflow,
    GraphJournal,
    GraphLimits,
    GraphRuntime,
    StateFlatUpdate,
    StateUpdate,
    ValidationError,
)


def build(callback, *, initial=lambda: 0, graph=False, limits=None, trailing=()):
    nodes = (FlowStep("expand", "stateful_flat_map", callback, initial), *trailing)
    if graph:
        return GraphDataflow(
            "expansion",
            "v1",
            nodes,
            tuple(FlowEdge(a.step_id, b.step_id) for a, b in itertools.pairwise(nodes)),
            entry="expand",
            limits=GraphLimits(operator_limits=limits or FlowLimits()),
        )
    return Dataflow("expansion", "v1", nodes, limits or FlowLimits())


def runtime(flow, checkpoint=None):
    cls = GraphRuntime if isinstance(flow, GraphDataflow) else FlowRuntime
    return cls(flow) if checkpoint is None else cls.from_checkpoint(flow, checkpoint)


def values(output):
    return [item.record.value if hasattr(item, "record") else item.value for item in output]


def test_running_keyed_expansion_and_checkpoint_restore():
    flow = Dataflow(
        "paired-running-totals",
        "v1",
        (
            FlowStep(
                "expand",
                "stateful_flat_map",
                lambda value, state: StateFlatUpdate(state + value, (state, state + value)),
                lambda: 0,
            ),
        ),
    )
    runtime = FlowRuntime(flow)
    assert [item.value for item in runtime.process(FlowRecord(2, "a"))] == [0, 2]
    restored = FlowRuntime.from_checkpoint(flow, runtime.checkpoint())
    assert [item.value for item in restored.process(FlowRecord(3, "a"))] == [2, 5]
    assert restored.checkpoint().cells == (("expand", "a", "5"),)


@pytest.mark.parametrize("graph", [False, True])
def test_seeded_keyed_expansion_removal_and_repeated_snapshot_oracle(graph):
    def callback(value, state):
        if value < 0:
            return StateFlatUpdate(None, (), retain=False)
        return StateFlatUpdate(state + value, (state, state + value, 2 * (state + value)))

    flow = build(callback, graph=graph)
    active = runtime(flow)
    expected = {}
    rng = random.Random(61517)
    emitted = 0
    for position in range(90):
        key = str(rng.randrange(4))
        value = rng.randrange(-2, 8)
        before = expected.get(key, 0)
        if value < 0:
            expected.pop(key, None)
            want = []
        else:
            expected[key] = before + value
            want = [before, before + value, 2 * (before + value)]
        assert values(active.process(FlowRecord(value, key))) == want
        emitted += len(want)
        point = active.checkpoint()
        assert point.processed_inputs == position + 1 and point.emitted_records == emitted
        assert point.cells == tuple(
            ("expand", key, str(value)) for key, value in sorted(expected.items())
        )
        active = runtime(flow, point)


@pytest.mark.parametrize("graph", [False, True])
def test_state_snapshots_precede_generator_mutation_and_output_values_are_isolated(graph):
    proposed = {"count": 1}
    output = {"value": 1}

    def callback(value, state):
        def expansion():
            yield output
            proposed["count"] = 1000
            output["value"] = 2
            yield output

        return StateFlatUpdate(proposed, expansion())

    active = runtime(build(callback, graph=graph))
    assert values(active.process(FlowRecord(0, "a"))) == [{"value": 1}, {"value": 2}]
    assert active.checkpoint().cells == (("expand", "a", '{"count":1}'),)


@pytest.mark.parametrize("graph", [False, True])
def test_state_is_snapshotted_before_custom_iterable_entry(graph):
    proposed = {"count": 1}

    class Items:
        def __iter__(self):
            proposed["count"] = 99
            return iter((1, 2))

    active = runtime(build(lambda v, s: StateFlatUpdate(proposed, Items()), graph=graph))
    assert values(active.process(FlowRecord(0, "a"))) == [1, 2]
    assert active.checkpoint().cells == (("expand", "a", '{"count":1}'),)


@pytest.mark.parametrize("graph", [False, True])
def test_repeated_key_within_one_input_sees_pending_state_and_deletion(graph):
    steps = (
        FlowStep("repeat", "flat_map", lambda value: (2, 3, -1, 4)),
        FlowStep(
            "expand",
            "stateful_flat_map",
            lambda value, state: StateFlatUpdate(
                state + value, (state, state + value), retain=value >= 0
            ),
            lambda: 0,
        ),
    )
    flow = (
        GraphDataflow("repeated", "v1", steps, (FlowEdge("repeat", "expand"),), entry="repeat")
        if graph
        else Dataflow("repeated", "v1", steps)
    )
    active = runtime(flow)
    assert values(active.process(FlowRecord(0, "a"))) == [0, 2, 2, 5, 5, 4, 0, 4]
    assert active.checkpoint().cells == (("expand", "a", "4"),)


def test_late_graph_sibling_failure_rolls_back_expansion_and_output():
    flow = GraphDataflow(
        "siblings",
        "v1",
        (
            FlowStep("entry", "map", lambda value: value),
            FlowStep(
                "expand",
                "stateful_flat_map",
                lambda v, s: StateFlatUpdate(s + v, (v, v)),
                lambda: 0,
            ),
            FlowStep("reject", "map", lambda value: 1 / 0),
        ),
        (FlowEdge("entry", "expand"), FlowEdge("entry", "reject")),
        entry="entry",
    )
    active = runtime(flow)
    before = active.checkpoint()
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(2, "a"))
    assert active.checkpoint() == before


@pytest.mark.parametrize("graph", [False, True])
def test_zero_outputs_can_store_json_null_and_delete_reinitializes(graph):
    initialized = []

    def initial():
        initialized.append(True)
        return None

    def callback(value, state):
        assert state is None
        return StateFlatUpdate(None, (), retain=value != "delete")

    active = runtime(build(callback, initial=initial, graph=graph))
    for value in ("store", "store", "delete", "store"):
        assert active.process(FlowRecord(value, "a")) == ()
    assert initialized == [True, True]
    assert active.checkpoint().cells == (("expand", "a", "null"),)
    assert active.checkpoint().processed_inputs == 4


@pytest.mark.parametrize("graph", [False, True])
@pytest.mark.parametrize("failure", ["generator", "value", "state", "expansion", "downstream"])
def test_all_internal_state_and_counters_roll_back_on_failure(graph, failure):
    closed = []

    def callback(value, state):
        def expansion():
            try:
                yield 1
                if failure == "generator":
                    raise ValueError("bad stream")
                if failure == "value":
                    yield object()
                yield 2
                yield 3
            finally:
                closed.append(True)

        return StateFlatUpdate(object() if failure == "state" else state + value, expansion())

    trailing = (FlowStep("reject", "map", lambda value: 1 / 0),) if failure == "downstream" else ()
    limits = FlowLimits(max_records_per_input=2) if failure == "expansion" else FlowLimits()
    active = runtime(build(callback, graph=graph, limits=limits, trailing=trailing))
    before = active.checkpoint()
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(5, "a"))
    assert active.checkpoint() == before
    if failure != "state":
        assert closed == [True]


@pytest.mark.parametrize("graph", [False, True])
def test_primed_generator_closes_even_if_proposed_state_fails_before_first_pull(graph):
    closed = []

    def callback(value, state):
        def expansion():
            try:
                yield "already-consumed-by-callback"
                yield 1
            finally:
                closed.append(True)

        iterator = expansion()
        next(iterator)
        return StateFlatUpdate(object(), iterator)

    active = runtime(build(callback, graph=graph))
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert closed == [True] and active.checkpoint().cells == ()


@pytest.mark.parametrize("graph", [False, True])
@pytest.mark.parametrize(
    "settings",
    [
        {"max_record_bytes": 1},
        {"max_batch_bytes": 2},
        {"max_state_value_bytes": 1},
        {"max_state_bytes": 1},
        {"max_calls_per_input": 1},
    ],
)
def test_each_shared_budget_rolls_back(graph, settings):
    active = runtime(
        build(
            lambda v, s: StateFlatUpdate(12, (12, 12)), graph=graph, limits=FlowLimits(**settings)
        )
    )
    before = active.checkpoint()
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert active.checkpoint() == before


@pytest.mark.parametrize("graph", [False, True])
def test_retained_key_limit_and_failed_second_key_are_atomic(graph):
    active = runtime(
        build(
            lambda v, s: StateFlatUpdate(v, (v,)), graph=graph, limits=FlowLimits(max_state_keys=1)
        )
    )
    active.process(FlowRecord(1, "a"))
    before = active.checkpoint()
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(2, "b"))
    assert active.checkpoint() == before


@pytest.mark.parametrize("output", [None, 1, "not-an-iterable-batch", b"no", {}])
def test_invalid_expansion_contract(output):
    active = runtime(build(lambda v, s: StateFlatUpdate(1, output)))
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert active.checkpoint().cells == ()


@pytest.mark.parametrize("where", ["outputs", "yield", "return"])
def test_misplaced_native_coroutines_closed_without_executing(where):
    ran = []
    acquired = []

    async def misplaced():
        ran.append(True)

    def callback(v, s):
        bad = misplaced()
        acquired.append(bad)

        def expansion():
            if where == "yield":
                yield bad
            return bad

        return StateFlatUpdate(1, bad if where == "outputs" else expansion())

    active = runtime(build(callback))
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert not ran and active.checkpoint().cells == ()
    with pytest.raises(RuntimeError, match="cannot reuse already awaited coroutine"):
        acquired[0].send(None)


@pytest.mark.parametrize("graph", [False, True])
def test_control_exception_survives_generator_ordinary_cleanup_failure(graph):
    def callback(v, s):
        def expansion():
            try:
                yield object()
            finally:
                raise ValueError("cleanup failure")

        return StateFlatUpdate(1, expansion())

    active = runtime(build(callback, graph=graph))
    # A downstream emit hook is shared by graph accounting and representation.
    from stream_quilt import dataflow

    original = dataflow._snapshot

    def interrupt(value, maximum):
        if type(value) is object:
            raise KeyboardInterrupt("stop-now")
        return original(value, maximum)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(dataflow, "_snapshot", interrupt)
        with pytest.raises(KeyboardInterrupt, match="stop-now") as error:
            active.process(FlowRecord(1, "a"))
    assert "cleanup also failed" in str(error.value.__notes__)
    assert active.checkpoint().cells == ()


@pytest.mark.parametrize("graph", [False, True])
def test_real_sqlite_reopen_persists_expanded_source_positions(tmp_path, graph):
    flow = build(lambda v, s: StateFlatUpdate(s + v, (s, s + v)), graph=graph)
    cls = GraphJournal if graph else FlowJournal
    path = tmp_path / "expansion.sqlite"
    source = "1" * 64
    first = cls(path, flow, source).advance([FlowRecord(2, "a")], expected_generation=0)
    reopened = cls(path, flow, source, create=False)
    last = reopened.advance([FlowRecord(3, "a")], expected_generation=first.generation)
    assert [item.record.value for item in reopened.outputs()] == [0, 2, 2, 5]
    assert [item.source_position for item in reopened.outputs()] == [0, 0, 1, 1]
    assert last.next_position == 2 and last.checkpoint.cells == (("expand", "a", "5"),)


def test_kind_identity_prevents_restoring_stateful_map_as_expansion():
    flow = build(lambda v, s: StateFlatUpdate(1, (1,)))
    active = runtime(flow)
    active.process(FlowRecord(1, "a"))
    other = replace(
        flow, steps=(FlowStep("expand", "stateful_map", lambda v, s: StateUpdate(1, 1), lambda: 0),)
    )
    with pytest.raises(ValidationError):
        runtime(other, active.checkpoint())


def test_missing_key_wrong_update_and_invalid_retain():
    with pytest.raises(ValidationError):
        StateFlatUpdate(1, (), retain=1)
    for record, callback in (
        (FlowRecord(1), lambda v, s: StateFlatUpdate(1, ())),
        (FlowRecord(1, "a"), lambda v, s: StateUpdate(1, 1)),
    ):
        active = runtime(build(callback))
        with pytest.raises(FlowExecutionError):
            active.process(record)
        assert active.checkpoint().processed_inputs == 0


def test_custom_iterator_resource_remains_caller_owned():
    class Items:
        closed = False

        def __iter__(self):
            return iter((1, 2))

        def close(self):
            self.closed = True

    items = Items()
    assert values(
        runtime(build(lambda v, s: StateFlatUpdate(1, items))).process(FlowRecord(1, "a"))
    ) == [1, 2]
    assert not items.closed
    items.close()
    assert items.closed


@pytest.mark.parametrize("graph", [False, True])
def test_custom_iterable_entry_failure_rolls_back_prepared_state(graph):
    class Items:
        def __iter__(self):
            raise ValueError("cannot start")

    active = runtime(build(lambda v, s: StateFlatUpdate(12, Items()), graph=graph))
    before = active.checkpoint()
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert active.checkpoint() == before


def test_custom_iterable_returned_generator_is_closed_on_overflow():
    closed = []

    class Items:
        def __iter__(self):
            try:
                yield from itertools.repeat(1)
            finally:
                closed.append(True)

    active = runtime(
        build(lambda v, s: StateFlatUpdate(12, Items()), limits=FlowLimits(max_records_per_input=1))
    )
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert closed == [True] and active.checkpoint().cells == ()


def test_new_cleanup_control_exception_propagates_and_runtime_becomes_reusable():
    def callback(v, s):
        if v == 0:
            return StateFlatUpdate(0, ())

        def expansion():
            try:
                yield from itertools.repeat(1)
            finally:
                raise KeyboardInterrupt("cleanup stop")

        return StateFlatUpdate(12, expansion())

    active = runtime(build(callback, limits=FlowLimits(max_records_per_input=1)))
    with pytest.raises(KeyboardInterrupt, match="cleanup stop"):
        active.process(FlowRecord(1, "a"))
    assert active.checkpoint().processed_inputs == 0
    assert active.process(FlowRecord(0, "a")) == ()
    assert active.checkpoint().processed_inputs == 1


def test_custom_awaitable_rejected_without_running_its_protocol():
    called = []

    class Waiting:
        def __await__(self):
            called.append(True)
            yield None

    active = runtime(build(lambda v, s: StateFlatUpdate(1, Waiting())))
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    assert called == [] and active.checkpoint().cells == ()


def test_early_run_close_commits_complete_input_without_pulling_next_source():
    pulled = []

    def source():
        for value in (2, 3):
            pulled.append(value)
            yield FlowRecord(value, "a")

    active = runtime(build(lambda v, s: StateFlatUpdate(s + v, (s, s + v))))
    inputs = source()
    outputs = active.run(inputs, max_inputs=1)
    assert next(outputs).value == 0
    outputs.close()
    assert pulled == [2]
    assert active.checkpoint().cells == (("expand", "a", "2"),)
    assert active.checkpoint().emitted_records == 2
    assert values(active.run(inputs, max_inputs=1)) == [2, 5]
    assert pulled == [2, 3]


@pytest.mark.skipif(
    sys.version_info < (3, 13), reason="generator.close return values require 3.13+"
)
def test_native_coroutine_returned_during_generator_close_is_closed():
    acquired = []

    async def misplaced():
        pytest.fail("misplaced coroutine body must never run")

    def expansion():
        try:
            yield from itertools.repeat(1)
        except GeneratorExit:
            bad = misplaced()
            acquired.append(bad)
            return bad

    active = runtime(
        build(
            lambda v, s: StateFlatUpdate(1, expansion()), limits=FlowLimits(max_records_per_input=1)
        )
    )
    with pytest.raises(FlowExecutionError):
        active.process(FlowRecord(1, "a"))
    with pytest.raises(RuntimeError, match="cannot reuse already awaited coroutine"):
        acquired[0].send(None)
    assert active.checkpoint().processed_inputs == 0
