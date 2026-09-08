"""Adversarial configuration, allocation, retention and checkpoint boundaries."""

import copy
import json
import runpy
from dataclasses import replace
from pathlib import Path

import pytest

import stream_quilt.keyed_join as joins
import stream_quilt.multi_checkpoint as wire
import stream_quilt.multi_graph as multi
from stream_quilt import (
    FlowBranch,
    FlowEdge,
    FlowEntry,
    FlowExecutionError,
    FlowJoin,
    FlowLimits,
    FlowMerge,
    FlowRecord,
    FlowStep,
    GraphCheckpoint,
    GraphInput,
    GraphLimits,
    JoinEdge,
    KeyedJoin,
    MultiGraphBatch,
    MultiGraphCheckpoint,
    MultiGraphDataflow,
    MultiGraphLimits,
    MultiGraphRuntime,
    StateUpdate,
    ValidationError,
)


def flow(*, emit="running", limits=None):
    return MultiGraphDataflow(
        "bounds",
        "1",
        (
            FlowStep("left", "map", lambda x: x),
            FlowStep("right", "map", lambda x: x),
            FlowJoin("join", KeyedJoin("join", "1", ("l", "r"), "product", emit)),
        ),
        (JoinEdge("left", "join", "l"), JoinEdge("right", "join", "r")),
        (FlowEntry("a", "left"), FlowEntry("b", "right")),
        limits or MultiGraphLimits(),
    )


def populated(*, emit="running"):
    runtime = MultiGraphRuntime(flow(emit=emit))
    runtime.process(GraphInput("a", 0, FlowRecord({"世界": [None]}, "k")))
    return runtime


def test_product_preflight_accounts_for_owed_fanout_before_second_materialization(monkeypatch):
    base = flow()
    configured = replace(
        base,
        nodes=(
            FlowStep("left", "flat_map", lambda x: [x, x]),
            *base.nodes[1:],
            FlowStep("sink1", "map", lambda x: x),
            FlowStep("sink2", "map", lambda x: x),
        ),
        edges=(*base.edges, FlowEdge("join", "sink1"), FlowEdge("join", "sink2")),
        limits=MultiGraphLimits(graph=GraphLimits(max_work_records=13)),
    )
    runtime = MultiGraphRuntime(configured)
    before = runtime.checkpoint()
    materialized = []
    original = joins._rows

    def rows(key, cell, sides):
        materialized.append(cell.rows)
        return original(key, cell, sides)

    monkeypatch.setattr(joins, "_rows", rows)
    with pytest.raises(FlowExecutionError):
        runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    assert materialized == [1]
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("budget", ["records", "bytes"])
def test_drain_budget_rejects_before_any_rows_and_preserves_eof(monkeypatch, budget):
    runtime = populated(emit="final")
    runtime.close("a", next_position=1)
    runtime.close("b", next_position=0)
    # Restore under changed identity is intentionally impossible; use a graph
    # admitted from the outset with enough input work but a fanout-heavy drain.
    base = flow(emit="final")
    tail = tuple(FlowStep(f"tail{i}", "map", lambda x: x) for i in range(5))
    gl = GraphLimits(max_work_records=5) if budget == "records" else GraphLimits(max_work_bytes=100)
    configured = replace(
        base,
        nodes=(*base.nodes, *tail),
        edges=(*base.edges, *(FlowEdge("join", n.step_id) for n in tail)),
        limits=MultiGraphLimits(graph=gl),
    )
    runtime = MultiGraphRuntime(configured)
    runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    runtime.close("a", next_position=1)
    runtime.close("b", next_position=0)
    before = runtime.checkpoint()
    monkeypatch.setattr(
        joins, "_rows", lambda *_: pytest.fail("rows materialized before admission")
    )
    with pytest.raises(ValidationError):
        runtime.drain()
    assert runtime.checkpoint() == before


def test_ordinary_and_join_state_share_one_global_cell_limit(monkeypatch):
    base = flow(emit="final")
    configured = replace(
        base,
        nodes=(
            FlowStep("left", "stateful_map", lambda x, s: StateUpdate(s + 1, x), lambda: 0),
            *base.nodes[1:],
        ),
        limits=MultiGraphLimits(max_state_cells=1),
    )
    runtime = MultiGraphRuntime(configured)
    before = runtime.checkpoint()
    monkeypatch.setattr(joins, "_rows", lambda *_: pytest.fail("rows should not be materialized"))
    with pytest.raises(FlowExecutionError):
        runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    assert runtime.checkpoint() == before


@pytest.mark.parametrize(
    "name,limit", [("max_state_cells", 1), ("max_join_values", 1), ("max_state_bytes", 65)]
)
def test_global_retention_rejection_preserves_positions(name, limit):
    runtime = MultiGraphRuntime(flow(emit="final", limits=MultiGraphLimits(**{name: limit})))
    runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.process(GraphInput("a", 1, FlowRecord(2, "other")))
    assert runtime.checkpoint() == before


def test_wire_charge_matches_independent_compact_utf8_cell_encoding():
    base = flow(emit="final")
    configured = replace(
        base,
        nodes=(
            FlowStep("left", "stateful_map", lambda x, s: StateUpdate(x, x), lambda: None),
            *base.nodes[1:],
        ),
    )
    runtime = MultiGraphRuntime(configured)
    runtime.process(GraphInput("a", 0, FlowRecord({"特殊": '"\n'}, "世界")))
    cp = runtime.checkpoint().to_dict()
    objects = cp["cells"] + [
        {"step": entry["step"], "cell": cell}
        for entry in cp["joins"]
        for cell in entry["checkpoint"]["cells"]
    ]
    expected = sum(
        len(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()) for obj in objects
    )
    assert runtime._state.wire_bytes == expected
    restored = MultiGraphRuntime.from_checkpoint(configured, runtime.checkpoint())
    assert restored._state.wire_bytes == expected


@pytest.mark.parametrize("where", ["return", "outer_state"])
@pytest.mark.parametrize("error", [MemoryError, KeyboardInterrupt])
def test_allocation_before_outer_swap(monkeypatch, where, error):
    runtime = populated()
    before = runtime.checkpoint()

    def reject(*args, **kwargs):
        raise error("allocation failed")

    monkeypatch.setattr(multi, "MultiGraphBatch" if where == "return" else "_State", reject)
    with pytest.raises(error):
        runtime.process(GraphInput("b", 0, FlowRecord(2, "k")))
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("size", [1, 2, 3])
def test_join_combined_record_contract_is_not_bypassed(size):
    runtime = MultiGraphRuntime(
        flow(
            limits=MultiGraphLimits(
                graph=GraphLimits(operator_limits=FlowLimits(max_record_bytes=size))
            )
        )
    )
    before = runtime.checkpoint()
    with pytest.raises(FlowExecutionError):
        runtime.process(GraphInput("a", 0, FlowRecord(1, "k")))
    assert runtime.checkpoint() == before


@pytest.mark.parametrize(
    "mutation",
    [
        lambda f: replace(f, nodes=()),
        lambda f: replace(f, nodes=[*f.nodes]),
        lambda f: replace(f, nodes=(*f.nodes, f.nodes[0])),
        lambda f: replace(f, nodes=(*f.nodes, object())),
        lambda f: replace(f, entries=()),
        lambda f: replace(f, entries=[*f.entries]),
        lambda f: replace(f, entries=(object(),)),
        lambda f: replace(f, entries=(FlowEntry("a", "join"),)),
        lambda f: replace(f, entries=(FlowEntry("a", "left"), FlowEntry("a", "right"))),
        lambda f: replace(f, entries=(FlowEntry("a", "left"), FlowEntry("b", "left"))),
        lambda f: replace(f, edges=[*f.edges]),
        lambda f: replace(f, edges=(*f.edges, f.edges[0])),
        lambda f: replace(f, edges=(object(),)),
        lambda f: replace(f, edges=(JoinEdge("unknown", "join", "l"), f.edges[1])),
        lambda f: replace(f, edges=(FlowEdge("left", "join"), f.edges[1])),
        lambda f: replace(f, edges=(*f.edges, JoinEdge("join", "left", "l"))),
        lambda f: replace(f, edges=(JoinEdge("left", "join", "missing"), f.edges[1])),
        lambda f: replace(f, edges=(JoinEdge("left", "join", "r"), f.edges[1])),
        lambda f: replace(f, nodes=(*f.nodes, FlowStep("unused", "map", lambda x: x))),
        lambda f: replace(f, edges=(*f.edges, FlowEdge("join", "left"))),
        lambda f: replace(f, edges=(JoinEdge("left", "join", "l", True), f.edges[1])),
        lambda f: replace(f, nodes=(FlowBranch("left", lambda _: True), *f.nodes[1:])),
        lambda f: replace(
            f, nodes=(*f.nodes, FlowMerge("merge")), edges=(*f.edges, FlowEdge("join", "merge"))
        ),
        lambda f: replace(f, limits=GraphLimits()),
    ],
)
def test_topology_rejects_invalid_declarations_before_callbacks(mutation):
    with pytest.raises(ValidationError):
        mutation(flow())


@pytest.mark.parametrize(
    "factory",
    [
        lambda: FlowEntry(" a", "s"),
        lambda: FlowJoin("x", object()),
        lambda: FlowJoin("x", KeyedJoin("y", "1", ("a", "b"))),
        lambda: JoinEdge("x", "y", "a", 1),
        lambda: GraphInput("a", 0, {}),
        lambda: MultiGraphLimits(graph={}),
        lambda: MultiGraphLimits(max_join_values=True),
        lambda: MultiGraphLimits(max_state_cells=0),
        lambda: MultiGraphLimits(max_state_bytes=10**1000),
        lambda: MultiGraphRuntime(object()),
        lambda: MultiGraphBatch((), 0, "bad", ()),
        lambda: MultiGraphBatch([], 0, "open", ()),
        lambda: MultiGraphBatch(({},), 0, "open", ()),
        lambda: MultiGraphBatch((), -1, "open", ()),
        lambda: MultiGraphBatch((), 0, "open", []),
        lambda: MultiGraphBatch((), 0, "open", ("j", "j")),
        lambda: MultiGraphBatch((), 0, "draining", ()),
        lambda: MultiGraphBatch((), 0, "closed", ("j",)),
    ],
)
def test_strict_public_constructors(factory):
    with pytest.raises(ValidationError):
        factory()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(extra=1),
        lambda d: d.update(kind="stream-quilt-graph-checkpoint"),
        lambda d: d.update(version="2.0"),
        lambda d: d.update(identity="z" * 64),
        lambda d: d.update(sources=[]),
        lambda d: d.update(edge_counts=[True, 0]),
        lambda d: d.update(cells=[{"step": "left", "key": "k", "value": "1", "extra": 1}]),
        lambda d: d["sources"][0].update(extra=1),
        lambda d: d["sources"][0].update(next_position=True),
        lambda d: d["sources"][1].update(source_id="a"),
        lambda d: d["joins"][0].update(extra=1),
        lambda d: d["joins"][0]["checkpoint"].update(extra=1),
        lambda d: d["joins"][0]["checkpoint"].update(sides=["l", "l"]),
        lambda d: d["joins"][0]["checkpoint"].update(closed_sides=[False]),
        lambda d: d["joins"][0]["checkpoint"].update(phase="bogus"),
        lambda d: d["joins"][0]["checkpoint"].update(processed_inputs=[1, True]),
        lambda d: d["joins"][0]["checkpoint"]["cells"][0].update(values=[["NaN"], []]),
        lambda d: d["joins"][0]["checkpoint"]["cells"][0].update(values=[["{ }"], []]),
    ],
)
def test_strict_checkpoint_import_does_not_mutate_input(mutation):
    document = populated().checkpoint().to_dict()
    mutation(document)
    before = copy.deepcopy(document)
    with pytest.raises(ValidationError):
        MultiGraphCheckpoint.from_dict(document)
    assert document == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("identity", "x" * 64),
        ("operation_sequence", True),
        ("emitted_records", -1),
        ("sources", ()),
        ("sources", (("a", 0, False), ("a", 0, False))),
        ("sources", (("a", True, False),)),
        ("edge_counts", (False,)),
        ("cells", (("x", "a", "NaN"),)),
        ("cells", (("x", "a", "1"), ("x", "a", "1"))),
        ("joins", (("x", object()),)),
    ],
)
def test_direct_checkpoint_constructor_is_strict(field, value):
    with pytest.raises(ValidationError):
        replace(populated().checkpoint(), **{field: value})


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d["edge_counts"].__setitem__(0, 2),
        lambda d: d["sources"][0].update(closed=True),
        lambda d: d["sources"][0].update(source_id="other"),
        lambda d: d["joins"][0].update(step="unknown"),
        lambda d: d["joins"].clear(),
        lambda d: d.update(edge_counts=[1]),
        lambda d: d.update(operation_sequence=100),
        lambda d: d.update(emitted_records=10**9),
        lambda d: d.update(cells=[{"step": "left", "key": "k", "value": "1"}]),
    ],
)
def test_restore_rejects_impossible_graph_specific_state(mutation):
    runtime = populated()
    document = runtime.checkpoint().to_dict()
    mutation(document)
    with pytest.raises(ValidationError):
        cp = MultiGraphCheckpoint.from_dict(document)
        MultiGraphRuntime.from_checkpoint(runtime.flow, cp)


def test_checkpoint_global_admission_precedes_any_nested_json_parse(monkeypatch):
    runtime = populated()
    document = runtime.checkpoint().to_dict()
    monkeypatch.setattr(wire, "_MAX_STATE_BYTES", 1)
    monkeypatch.setattr(joins.json, "loads", lambda *_a, **_k: pytest.fail("nested JSON parsed"))
    with pytest.raises(ValidationError, match="aggregate"):
        MultiGraphCheckpoint.from_dict(document)


@pytest.mark.parametrize("payload", [b"\xff", "{", '{"a":1,"a":2}', "NaN", "\ud800", 3])
def test_invalid_json_envelopes(payload):
    with pytest.raises(ValidationError):
        MultiGraphCheckpoint.from_json(payload)


def test_old_checkpoint_loader_does_not_accept_new_wire():
    with pytest.raises(ValidationError):
        GraphCheckpoint.from_dict(populated().checkpoint().to_dict())


def test_limits_revision_port_order_identity_and_restored_payload_ownership():
    runtime = populated()
    cp = runtime.checkpoint()
    for changed in (
        replace(runtime.flow, revision="2"),
        replace(runtime.flow, edges=tuple(reversed(runtime.flow.edges))),
        replace(runtime.flow, entries=tuple(reversed(runtime.flow.entries))),
        replace(runtime.flow, limits=MultiGraphLimits(max_state_cells=99)),
    ):
        with pytest.raises(ValidationError, match="identity"):
            MultiGraphRuntime.from_checkpoint(changed, cp)
    document = cp.to_dict()
    loaded = MultiGraphCheckpoint.from_dict(document)
    document["joins"][0]["checkpoint"]["cells"].clear()
    restored = MultiGraphRuntime.from_checkpoint(runtime.flow, loaded)
    assert restored.checkpoint() == cp


@pytest.mark.parametrize(
    "field,value",
    [
        ("sources", (("a",),)),
        ("edge_counts", []),
        ("cells", []),
        ("cells", ((),)),
        ("joins", []),
        ("joins", ((),)),
        ("cells", (("left", "k", "{ }"),)),
        ("cells", (("left", "k", 1),)),
        ("cells", (("left", "k", '"\ud800"'),)),
        ("sources", (("a", 0, False), ("b", 0, False))),
    ],
)
def test_additional_live_checkpoint_shape_and_canonical_boundaries(field, value):
    with pytest.raises(ValidationError):
        replace(populated().checkpoint(), **{field: value})


@pytest.mark.parametrize("field,value", [("sides", ()), ("cells", []), ("cells", ((),))])
def test_defensively_rechecks_directly_forged_nested_frozen_object(field, value):
    cp = populated().checkpoint()
    nested = copy.copy(cp.joins[0][1])
    object.__setattr__(nested, field, value)
    with pytest.raises(ValidationError):
        replace(cp, joins=(("join", nested),))


def test_encoded_value_utf8_admission_uses_bytes_not_characters(monkeypatch):
    cp = populated().checkpoint()
    monkeypatch.setattr(wire, "_HARD_LIMITS", replace(wire._HARD_LIMITS, max_value_bytes=4))
    with pytest.raises(ValidationError, match="UTF-8"):
        replace(cp, cells=(("left", "k", '"世"'),))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d["joins"][0]["checkpoint"].update(cells={}),
        lambda d: d["joins"][0]["checkpoint"].update(cells=[{}]),
        lambda d: d["joins"][0]["checkpoint"].update(sides=["l", "r"], closed_sides=[False, 0]),
    ],
)
def test_nested_admission_rejects_shape_before_materialization(monkeypatch, mutation):
    document = populated().checkpoint().to_dict()
    mutation(document)
    monkeypatch.setattr(joins.json, "loads", lambda *_a, **_k: pytest.fail("nested parse reached"))
    with pytest.raises(ValidationError):
        MultiGraphCheckpoint.from_dict(document)


def test_json_document_export_and_utf8_import_limits(monkeypatch):
    cp = populated().checkpoint()
    monkeypatch.setattr(wire, "_MAX_DOCUMENT_BYTES", 16)
    with pytest.raises(ValidationError, match="byte limit"):
        cp.to_json()
    with pytest.raises(ValidationError, match="byte limit"):
        MultiGraphCheckpoint.from_json(" " * 17)
    with pytest.raises(ValidationError):
        MultiGraphCheckpoint.from_json("世" * 8)


@pytest.mark.parametrize(
    "field,value", [("max_state_keys", 1), ("max_state_bytes", 1), ("max_state_value_bytes", 1)]
)
def test_restore_checks_ordinary_state_config_even_with_forged_identity(field, value):
    configured = MultiGraphDataflow(
        "state",
        "1",
        (FlowStep("state", "stateful_map", lambda x, _: StateUpdate(x, x), lambda: None),),
        (),
        (FlowEntry("a", "state"),),
    )
    runtime = MultiGraphRuntime(configured)
    for position in range(2):
        runtime.process(GraphInput("a", position, FlowRecord("value", str(position))))
    changed = replace(
        configured,
        limits=MultiGraphLimits(
            graph=GraphLimits(operator_limits=replace(FlowLimits(), **{field: value}))
        ),
    )
    cp = replace(runtime.checkpoint(), identity=changed.identity)
    with pytest.raises(ValidationError):
        MultiGraphRuntime.from_checkpoint(changed, cp)


def test_restore_checks_global_state_even_with_forged_identity():
    runtime = populated()
    changed = replace(runtime.flow, limits=MultiGraphLimits(max_state_bytes=1))
    cp = replace(runtime.checkpoint(), identity=changed.identity)
    with pytest.raises(ValidationError):
        MultiGraphRuntime.from_checkpoint(changed, cp)


def test_join_eof_consistency_is_checked_after_valid_shape():
    runtime = populated()
    cp = runtime.checkpoint()
    cp = replace(cp, sources=(("a", 1, True), ("b", 0, False)), operation_sequence=2)
    with pytest.raises(ValidationError, match="EOF"):
        MultiGraphRuntime.from_checkpoint(runtime.flow, cp)


def test_empty_graph_and_zero_input_eof_history_are_valid_but_fabricated_drain_is_not():
    configured = MultiGraphDataflow(
        "empty", "1", (FlowStep("a", "map", lambda x: x),), (), (FlowEntry("source", "a"),)
    )
    runtime = MultiGraphRuntime(configured)
    with pytest.raises(ValidationError):
        runtime.process({})
    with pytest.raises(ValidationError):
        MultiGraphRuntime.from_checkpoint(configured, object())
    runtime.close("source", next_position=0)
    cp = runtime.checkpoint()
    assert MultiGraphRuntime.from_checkpoint(configured, cp).phase == "closed"
    with pytest.raises(ValidationError, match="history"):
        MultiGraphRuntime.from_checkpoint(configured, replace(cp, operation_sequence=2))


def test_ordinary_node_cannot_accept_two_producers():
    configured = flow()
    with pytest.raises(ValidationError, match="ordinary"):
        replace(
            configured,
            nodes=(*configured.nodes, FlowStep("sink", "map", lambda x: x)),
            edges=(*configured.edges, FlowEdge("left", "sink"), FlowEdge("right", "sink")),
        )


def test_declaration_count_caps_are_preflighted():
    base = flow()
    with pytest.raises(ValidationError, match="nodes"):
        replace(base, nodes=tuple(FlowStep(str(i), "map", lambda x: x) for i in range(65)))
    with pytest.raises(ValidationError, match="edges"):
        replace(base, edges=base.edges * 129)
    with pytest.raises(ValidationError, match="entries"):
        replace(base, entries=tuple(FlowEntry(str(i), "left") for i in range(17)))
    with pytest.raises(ValidationError, match="16 joins"):
        replace(
            base,
            nodes=tuple(FlowJoin(str(i), KeyedJoin(str(i), "1", ("l", "r"))) for i in range(17)),
        )


def test_executable_offline_example_has_expected_matches_and_local_positions(capsys):
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "multi_source_orders.py"), run_name="__main__"
    )
    document = json.loads(capsys.readouterr().out)
    assert document["phase"] == "closed"
    assert [o["value"]["matched"] for o in document["outputs"] if o["step_id"] == "audit"] == [
        True,
        False,
    ]
    assert [s["next_position"] for s in document["sources"]] == [2, 1]
