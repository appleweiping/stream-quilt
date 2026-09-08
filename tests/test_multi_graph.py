"""Independent multi-source graph contracts; local positions are not broker acks."""

import copy
import itertools
import random
from dataclasses import replace

import pytest

from stream_quilt import (
    FlowBranch,
    FlowEdge,
    FlowExecutionError,
    FlowMerge,
    FlowRecord,
    FlowStep,
    KeyedJoin,
    MultiGraphCheckpoint,
    StateUpdate,
    ValidationError,
)
from stream_quilt.multi_graph import (
    FlowEntry,
    FlowJoin,
    GraphInput,
    JoinEdge,
    MultiGraphDataflow,
    MultiGraphRuntime,
)


def graph(insert="last", emit="complete"):
    return MultiGraphDataflow(
        "orders",
        "1",
        (
            FlowStep("left", "map", lambda x: x),
            FlowStep("right", "map", lambda x: x),
            FlowJoin("join", KeyedJoin("join", "1", ("l", "r"), insert, emit)),
        ),
        (JoinEdge("left", "join", "l"), JoinEdge("right", "join", "r")),
        (FlowEntry("a", "left"), FlowEntry("b", "right")),
    )


def test_two_sources_complete_and_eof_noop():
    runtime = MultiGraphRuntime(graph())
    assert runtime.process(GraphInput("a", 0, FlowRecord(3, "k"))).outputs == ()
    result = runtime.process(GraphInput("b", 0, FlowRecord(None, "k")))
    assert [out.record.value for out in result.outputs] == [
        {"present": [True, True], "values": [3, None]}
    ]
    runtime.close("a", next_position=1)
    closed = runtime.close("b", next_position=1)
    assert closed.phase == "closed"
    before = runtime.checkpoint()
    assert runtime.close("b", next_position=1).operation_sequence == closed.operation_sequence
    assert runtime.drain().operation_sequence == closed.operation_sequence
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("insert", ["first", "last", "product"])
def test_final_requires_explicit_close_and_bounded_drain(insert):
    runtime = MultiGraphRuntime(graph(insert, "final"))
    runtime.process(GraphInput("a", 0, FlowRecord(1, "b")))
    runtime.process(GraphInput("a", 1, FlowRecord(2, "a")))
    assert runtime.drain().outputs == ()
    runtime.close("a", next_position=2)
    assert runtime.close("b", next_position=0).ready_joins == ("join",)
    first = runtime.drain(max_keys=1)
    assert [out.record.key for out in first.outputs] == ["a"]
    assert first.phase == "draining"
    assert runtime.drain(max_keys=1).phase == "closed"


def test_join_then_downstream_failure_rolls_back_everything():
    base = graph()
    fail = True

    def callback(value):
        if fail:
            raise RuntimeError("private callback payload")
        return value

    flow = MultiGraphDataflow(
        base.flow_id,
        base.revision,
        (*base.nodes, FlowStep("sink", "map", callback)),
        (*base.edges, FlowEdge("join", "sink")),
        base.entries,
    )
    runtime = MultiGraphRuntime(flow)
    runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    before = runtime.checkpoint()
    with pytest.raises(Exception, match="sink"):
        runtime.process(GraphInput("b", 0, FlowRecord(2, "k")))
    assert runtime.checkpoint() == before
    fail = False
    assert len(runtime.process(GraphInput("b", 0, FlowRecord(2, "k"))).outputs) == 1


def restart(runtime):
    checkpoint = runtime.checkpoint()
    decoded = MultiGraphCheckpoint.from_json(checkpoint.to_json())
    assert decoded == checkpoint
    return MultiGraphRuntime.from_checkpoint(runtime.flow, decoded)


class Oracle:
    """Direct list-based reference, independent of JoinRuntime and wire machinery."""

    def __init__(self, insert, emit):
        self.insert, self.emit = insert, emit
        self.keys = {}

    def rows(self, key):
        left, right = self.keys[key]
        return [
            (key, {"present": [bool(left), bool(right)], "values": [a, b]})
            for a in left or [None]
            for b in right or [None]
        ]

    def process(self, side, key, value):
        lists = self.keys.setdefault(key, [[], []])
        if self.insert == "product":
            lists[side].append(copy.deepcopy(value))
        elif self.insert == "last" or not lists[side]:
            lists[side][:] = [copy.deepcopy(value)]
        if self.emit == "running":
            return self.rows(key)
        if self.emit == "complete" and all(lists):
            result = self.rows(key)
            del self.keys[key]
            return result
        return []


@pytest.mark.parametrize(
    "insert,emit",
    tuple(itertools.product(["first", "last", "product"], ["complete", "final", "running"])),
)
@pytest.mark.parametrize("seed", [13, 991, 3201])
def test_independent_modes_seeded_every_operation_restart(insert, emit, seed):
    runtime = MultiGraphRuntime(graph(insert, emit))
    oracle = Oracle(insert, emit)
    rng = random.Random(seed)
    positions = [0, 0]
    for _ in range(35):
        side = rng.randrange(2)
        key = rng.choice(["c", "a", "世界", "b"])
        value = rng.choice([None, {"标签": [1, 2]}, -3, ""])
        expected = oracle.process(side, key, value)
        result = runtime.process(
            GraphInput(("a", "b")[side], positions[side], FlowRecord(value, key))
        )
        positions[side] += 1
        assert [(o.record.key, o.record.value) for o in result.outputs] == expected
        runtime = restart(runtime)
    for source, position in zip(("a", "b"), positions, strict=True):
        assert runtime.close(source, next_position=position).outputs == ()
        runtime = restart(runtime)
    if emit == "final":
        expected = [row for key in sorted(oracle.keys) for row in oracle.rows(key)]
        actual = []
        while runtime.phase != "closed":
            actual.extend((o.record.key, o.record.value) for o in runtime.drain(max_keys=1).outputs)
            runtime = restart(runtime)
        assert actual == expected
    assert runtime.phase == "closed"
    before = runtime.checkpoint()
    assert runtime.drain().outputs == ()
    assert runtime.checkpoint() == before


def nested_graph():
    base = graph(emit="final")
    return MultiGraphDataflow(
        "nested",
        "1",
        (
            *base.nodes,
            FlowStep("third", "map", lambda x: x),
            FlowJoin("second", KeyedJoin("second", "1", ("joined", "third"), emit_mode="final")),
        ),
        (*base.edges, JoinEdge("join", "second", "joined"), JoinEdge("third", "second", "third")),
        (*base.entries, FlowEntry("c", "third")),
    )


def test_nested_final_rows_precede_eof_and_unrelated_source_can_remain_open():
    runtime = MultiGraphRuntime(nested_graph())
    for p, key in enumerate(("b", "a")):
        runtime.process(GraphInput("a", p, FlowRecord(p, key)))
    runtime.process(GraphInput("c", 0, FlowRecord("C", "a")))
    runtime.close("a", next_position=2)
    ready = runtime.close("b", next_position=0)
    assert ready.phase == "open" and ready.ready_joins == ("join",)
    assert runtime.drain(max_keys=1).outputs == ()
    runtime = restart(runtime)
    second = dict(runtime.checkpoint().joins)["second"]
    assert second.closed_sides == (False, False)
    assert second.processed_inputs == (1, 1)
    runtime.close("c", next_position=1)
    assert runtime.drain(max_keys=1).ready_joins == ("second",)
    runtime = restart(runtime)
    second = dict(runtime.checkpoint().joins)["second"]
    assert second.closed_sides == (True, True) and second.processed_inputs == (2, 1)
    result = runtime.drain()
    assert [o.record.key for o in result.outputs] == ["a", "b"]
    assert result.outputs[0].record.value == {
        "present": [True, True],
        "values": [{"present": [True, False], "values": [1, None]}, "C"],
    }
    assert result.phase == "closed"
    restart(runtime)


def test_merge_waits_for_all_producers_and_empty_route_closes():
    flow = MultiGraphDataflow(
        "routes",
        "1",
        (
            FlowBranch("branch", lambda _: True),
            FlowStep("other", "map", lambda x: x),
            FlowMerge("merged"),
            FlowJoin("join", KeyedJoin("join", "1", ("yes", "no"), emit_mode="final")),
        ),
        (
            FlowEdge("branch", "merged", True),
            FlowEdge("other", "merged"),
            JoinEdge("branch", "join", "no", False),
            JoinEdge("merged", "join", "yes"),
        ),
        (FlowEntry("a", "branch"), FlowEntry("b", "other")),
    )
    runtime = MultiGraphRuntime(flow)
    runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    runtime.close("a", next_position=1)
    runtime = restart(runtime)
    assert dict(runtime.checkpoint().joins)["join"].closed_sides == (False, True)
    runtime.process(GraphInput("b", 0, FlowRecord(2, "k")))
    runtime.close("b", next_position=1)
    assert runtime.drain().outputs[0].record.value == {
        "present": [True, False],
        "values": [2, None],
    }


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_multiple_join_and_ordinary_state_then_sibling_failure_roll_back(failure):
    fail = True

    def sink(value):
        if fail:
            raise failure("private payload")
        return value

    flow = MultiGraphDataflow(
        "rollback",
        "1",
        (
            FlowStep(
                "a", "stateful_map", lambda value, state: StateUpdate(state + 1, value), lambda: 0
            ),
            FlowStep("b", "map", lambda x: x),
            FlowJoin("j1", KeyedJoin("j1", "1", ("a", "b"), emit_mode="running")),
            FlowJoin("j2", KeyedJoin("j2", "1", ("a", "b"), emit_mode="running")),
            FlowStep("sink", "map", sink),
        ),
        (
            JoinEdge("a", "j1", "a"),
            JoinEdge("b", "j1", "b"),
            JoinEdge("a", "j2", "a"),
            JoinEdge("b", "j2", "b"),
            FlowEdge("j2", "sink"),
        ),
        (FlowEntry("left", "a"), FlowEntry("right", "b")),
    )
    runtime = MultiGraphRuntime(flow)
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError if failure is RuntimeError else failure):
        runtime.process(GraphInput("left", 0, FlowRecord(4, "k")))
    assert runtime.checkpoint() == before
    fail = False
    result = runtime.process(GraphInput("left", 0, FlowRecord(4, "k")))
    assert [o.step_id for o in result.outputs] == ["j1", "sink"]
    assert runtime.checkpoint().cells == (("a", "k", "1"),)
    restart(runtime)


def test_final_drain_callback_failure_preserves_draining_state_and_eof():
    base = graph(emit="final")
    fail = True

    def callback(value):
        if fail:
            raise RuntimeError("failed")
        return value

    flow = replace(
        base,
        nodes=(*base.nodes, FlowStep("sink", "map", callback)),
        edges=(*base.edges, FlowEdge("join", "sink")),
    )
    runtime = MultiGraphRuntime(flow)
    runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    runtime.close("a", next_position=1)
    runtime.close("b", next_position=0)
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.drain()
    assert runtime.checkpoint() == before
    fail = False
    assert runtime.drain().phase == "closed"


@pytest.mark.parametrize("action", ["process", "close", "drain", "checkpoint", "run"])
def test_reentrant_operations_are_rejected_and_state_rolled_back(action):
    runtime = None

    def callback(value):
        if action == "process":
            runtime.process(GraphInput("a", 0, FlowRecord(1)))
        elif action == "close":
            runtime.close("a", next_position=0)
        elif action == "drain":
            runtime.drain()
        elif action == "checkpoint":
            runtime.checkpoint()
        else:
            next(runtime.run([GraphInput("a", 0, FlowRecord(1))]))
        return value

    flow = MultiGraphDataflow(
        "reentry", "1", (FlowStep("a", "map", callback),), (), (FlowEntry("a", "a"),)
    )
    runtime = MultiGraphRuntime(flow)
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.process(GraphInput("a", 0, FlowRecord(1)))
    assert runtime.checkpoint() == before


def test_borrowed_iterable_no_lookahead_no_cleanup_no_implicit_eof():
    pulls = []

    def source():
        try:
            for i in range(3):
                pulls.append(i)
                yield GraphInput("a", i, FlowRecord(i, "k"))
        finally:
            pulls.append("closed")

    runtime = MultiGraphRuntime(graph())
    iterator = source()
    assert len(list(runtime.run(iterator, max_inputs=1))) == 1
    assert pulls == [0]
    assert len(list(runtime.run(iterator))) == 2
    assert runtime.phase == "open"
    assert pulls == [0, 1, 2, "closed"]


@pytest.mark.parametrize("source,position", [("a", -1), ("a", 1), ("unknown", 0), ("a", True)])
def test_exact_source_positions_before_any_callback(source, position):
    runtime = MultiGraphRuntime(graph())
    before = runtime.checkpoint()
    with pytest.raises(ValidationError):
        runtime.process(GraphInput(source, position, FlowRecord(1, "k")))
    with pytest.raises(ValidationError):
        runtime.close(source, next_position=position)
    assert runtime.checkpoint() == before


def test_closed_source_rejects_record_and_wrong_position_repeated_close():
    runtime = MultiGraphRuntime(graph())
    runtime.close("a", next_position=0)
    before = runtime.checkpoint()
    with pytest.raises(ValidationError):
        runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    with pytest.raises(ValidationError):
        runtime.close("a", next_position=1)
    assert runtime.checkpoint() == before
