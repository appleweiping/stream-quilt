"""Independent list oracle, strict requests, corruption scope and resource ceilings."""

import hashlib
import itertools
import json
import random
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from test_multi_journal import graph, request

import stream_quilt.multi_journal as module
import stream_quilt.multi_journal_types as wire
from stream_quilt import (
    FlowEdge,
    FlowEntry,
    FlowJoin,
    FlowRecord,
    FlowStep,
    GraphInput,
    JoinEdge,
    KeyedJoin,
    MultiGraphDataflow,
    StateUpdate,
    ValidationError,
)
from stream_quilt.multi_journal import (
    GraphDrain,
    GraphEOF,
    MultiGraphJournal,
    MultiGraphOutputCursor,
    MultiGraphRequest,
)


@pytest.mark.parametrize(
    "insert,emit",
    tuple(itertools.product(("first", "last", "product"), ("complete", "final", "running"))),
)
def test_nine_modes_seeded_list_oracle_and_sqlite_restarts(tmp_path, insert, emit):
    flow = MultiGraphDataflow(
        "oracle",
        "1",
        (
            FlowStep("a", "map", lambda x: x),
            FlowStep("b", "map", lambda x: x),
            FlowJoin("join", KeyedJoin("join", "1", ("left", "right"), insert, emit)),
        ),
        (JoinEdge("a", "join", "left"), JoinEdge("b", "join", "right")),
        (FlowEntry("a", "a"), FlowEntry("b", "b")),
    )
    commitments = {"a": "a" * 64, "b": "b" * 64}
    journal = MultiGraphJournal(tmp_path / "j", flow, commitments)
    rng = random.Random(941)
    positions = {"a": 0, "b": 0}
    cells = {}
    expected = []

    def rows(key):
        left, right = cells[key]
        return [
            (key, {"present": [bool(left), bool(right)], "values": [a, b]})
            for a in left or [None]
            for b in right or [None]
        ]

    for batch_index in range(6):
        commands = [GraphDrain()]
        for _ in range(4):
            source = rng.choice(("a", "b"))
            side = source == "b"
            key = rng.choice(("k", "界", "other"))
            value = rng.choice((None, 2, "λ"))
            commands.append(GraphInput(source, positions[source], FlowRecord(value, key)))
            positions[source] += 1
            values = cells.setdefault(key, [[], []])[side]
            if insert == "product":
                values.append(value)
            elif insert == "last" or not values:
                values[:] = [value]
            if emit == "running" or (emit == "complete" and all(cells[key])):
                expected.extend(rows(key))
                if emit == "complete":
                    del cells[key]
        journal.apply(request(journal, tuple(commands), batch_index + 1))
        journal = MultiGraphJournal(journal.path, flow, commitments, create=False)
        assert journal.latest().checkpoint.sources == tuple(
            (s, positions[s], False) for s in ("a", "b")
        )
    journal.apply(
        request(
            journal,
            (
                GraphEOF("a", positions["a"]),
                GraphDrain(),
                GraphEOF("b", positions["b"]),
                GraphEOF("b", positions["b"]),
            ),
            7,
        )
    )
    if emit == "final":
        for index, key in enumerate(sorted(cells)):
            expected.extend(rows(key))
            journal.apply(request(journal, (GraphDrain(1),), 8 + index))
    assert journal.apply(request(journal, (GraphDrain(),), 50)).status == "no_op"
    cursor = journal.output_cursor()
    actual = []
    while cursor.next_sequence < cursor.stop_sequence:
        cursor = MultiGraphOutputCursor.from_json(cursor.to_json())
        page = journal.read_outputs(cursor, limit=3)
        actual.extend((item.record.key, item.record.value) for item in page.outputs)
        cursor = page.cursor
    assert actual == expected


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_id", "a" * 31),
        ("request_id", "A" * 32),
        ("expected_generation", True),
        ("expected_generation", 10**1000),
        ("commands", []),
        ("commands", (None,)),
        ("commands", (GraphDrain(),) * 1001),
    ],
)
def test_request_constructor_rejects_malformed_contracts(field, value):
    data = {
        "journal_id": "a" * 32,
        "request_id": "b" * 32,
        "expected_generation": 0,
        "commands": (),
    }
    data[field] = value
    with pytest.raises(ValidationError):
        MultiGraphRequest(**data)


@pytest.mark.parametrize(
    "encoded", ["1e0", "NaN", "Infinity", '{"x":1,"x":2}', "[1,]", "1e999", '"\\ud800"']
)
def test_request_inner_json_rejects_noncanonical_or_invalid(encoded):
    value = MultiGraphRequest("a" * 32, "b" * 32, 0, (GraphInput("a", 0, FlowRecord(1)),)).to_dict()
    value["commands"][0]["record"]["encoded_value"] = encoded
    with pytest.raises(ValidationError):
        MultiGraphRequest.from_dict(value)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(unknown=True),
        lambda v: v.update(version="2.0"),
        lambda v: v["commands"][0].update(cause="guess"),
        lambda v: v["commands"][0].update(position=False),
        lambda v: v["commands"][0]["record"].update(extra=True),
        lambda v: v["commands"][0]["record"].update(encoded_value=[]),
    ],
)
def test_request_fixed_shape_and_field_types(mutation):
    value = MultiGraphRequest("a" * 32, "b" * 32, 0, (GraphInput("a", 0, FlowRecord(1)),)).to_dict()
    mutation(value)
    with pytest.raises(ValidationError):
        MultiGraphRequest.from_dict(value)


def test_request_aggregate_utf8_admission_precedes_nested_value_parse(monkeypatch):
    value = MultiGraphRequest(
        "a" * 32, "b" * 32, 0, (GraphInput("a", 0, FlowRecord("界" * 80)),)
    ).to_dict()
    monkeypatch.setattr(wire, "_REQUEST_BYTES", 300)
    monkeypatch.setattr(wire, "_loaded", lambda *args: pytest.fail("nested parsing was reached"))
    with pytest.raises(ValidationError, match="bytes"):
        MultiGraphRequest.from_dict(value)


def test_request_freezes_payload_and_roundtrip_has_no_aliases():
    source = {"arr": [None, {"界": 3}]}
    item = MultiGraphRequest("a" * 32, "b" * 32, 0, (GraphInput("a", 0, FlowRecord(source, "k")),))
    serialized = item.to_json()
    source["arr"].append(9)
    copy = MultiGraphRequest.from_json(serialized.encode())
    assert item == copy
    assert (
        json.loads(serialized)["commands"][0]["record"]["encoded_value"]
        == '{"arr":[null,{"界":3}]}'
    )
    with pytest.raises(AttributeError):
        item.commands = ()


def rewrite(journal, table, seq, mutate):
    with closing(sqlite3.connect(journal.path)) as db, db:
        value = json.loads(
            db.execute(f"SELECT payload FROM {table} WHERE seq=?", (seq,)).fetchone()[0]
        )
        mutate(value)
        payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        db.execute(
            f"UPDATE {table} SET payload=?,digest=? WHERE seq=?",
            (payload, hashlib.sha256(payload.encode()).hexdigest(), seq),
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("cause", "guess"),
        ("source_id", "missing"),
        ("position", 4),
        ("output_start", 1),
        ("command_index", 2),
        ("input_digest", "bad"),
        ("generation", 2),
        ("sequence", 2),
    ],
)
def test_rehashed_impossible_operation_metadata_is_rejected(tmp_path, field, value):
    journal = MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})
    pending = request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),))
    journal.apply(pending)
    rewrite(journal, "multi_operation", 1, lambda v: v.update({field: value}))
    # latest validates the current checkpoint/adjacent receipt, not all historical row contents.
    assert journal.latest().generation == 1
    with pytest.raises(ValidationError):
        journal.request(pending.request_id)


def test_plausible_rehashed_input_digest_is_not_authenticated_history(tmp_path):
    journal = MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})
    journal.apply(request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),)))
    rewrite(journal, "multi_operation", 1, lambda v: v.update(input_digest="e" * 64))
    assert journal.operation(1).input_digest == "e" * 64


def test_output_byte_budget_short_page_progress_and_oversized_candidate(tmp_path):
    journal = MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})
    journal.apply(request(journal, tuple(GraphInput("a", i, FlowRecord(i, "k")) for i in range(3))))
    with closing(sqlite3.connect(journal.path)) as db, db:
        size = db.execute(
            "SELECT length(CAST(payload AS BLOB)) FROM multi_output WHERE seq=0"
        ).fetchone()[0]
    cursor = journal.output_cursor()
    with pytest.raises(ValidationError, match="page byte"):
        journal.read_outputs(cursor, max_bytes=size - 1)
    page = journal.read_outputs(cursor, max_bytes=size)
    assert len(page.outputs) == 1 and page.cursor.next_sequence == 1
    assert len(journal.read_outputs(page.cursor).outputs) == 2


def test_metadata_count_admission_produces_conservative_progress(tmp_path, monkeypatch):
    journal = MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})
    for index in range(3):
        journal.apply(
            request(journal, (GraphInput("a", index, FlowRecord(index, "k")),), index + 1)
        )
    monkeypatch.setattr(module, "_PAGE_METADATA_ROWS", 2)
    page = journal.read_outputs(journal.output_cursor())
    assert len(page.outputs) == 2 and page.cursor.next_sequence == 2
    assert len(journal.read_outputs(page.cursor).outputs) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_id", "f" * 32),
        ("anchor_receipt_digest", "e" * 64),
        ("anchor_generation", 2),
        ("stop_sequence", 0),
    ],
)
def test_cursor_cannot_cross_identity_generation_or_stop(tmp_path, field, value):
    journal = MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})
    journal.apply(request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),)))
    cursor = replace(journal.output_cursor(), **{field: value})
    with pytest.raises(ValidationError):
        journal.read_outputs(cursor)


def test_source_positions_are_not_globally_monotonic_and_fanout_order_is_stable(tmp_path):
    base = graph("running")
    flow = MultiGraphDataflow(
        base.flow_id,
        base.revision,
        (
            *base.nodes,
            FlowStep("z", "map", lambda v: ["z", v]),
            FlowStep("y", "map", lambda v: ["y", v]),
        ),
        (*base.edges, FlowEdge("join", "z"), FlowEdge("join", "y")),
        base.entries,
    )
    journal = MultiGraphJournal(tmp_path / "j", flow, {"a": "a" * 64, "b": "b" * 64})
    commands = (
        *(GraphInput("a", i, FlowRecord(i, "k")) for i in range(4)),
        GraphInput("b", 0, FlowRecord(None, "k")),
    )
    journal.apply(request(journal, commands))
    cursor = journal.output_cursor()
    rows = []
    while cursor.next_sequence < cursor.stop_sequence:
        page = journal.read_outputs(cursor, limit=1)
        rows.extend(page.outputs)
        cursor = page.cursor
    ranks = {n: i for i, n in enumerate(flow.execution_order)}
    assert all(ranks[rows[i].step_id] < ranks[rows[i + 1].step_id] for i in range(0, len(rows), 2))
    assert journal.operation(5).position == 0


@pytest.mark.parametrize("failed_step", ["middle", "sink"])
def test_nested_final_drain_and_ordinary_state_roll_back_as_one_durable_request(
    tmp_path, failed_step
):
    enabled = False

    def gate(step):
        def callback(value):
            if enabled and step == failed_step:
                raise RuntimeError("requested rollback")
            return value

        return callback

    flow = MultiGraphDataflow(
        "nested",
        "1",
        (
            FlowStep("a", "map", lambda x: x),
            FlowStep("b", "map", lambda x: x),
            FlowStep("c", "map", lambda x: x),
            FlowJoin("first", KeyedJoin("first", "1", ("a", "b"), "last", "final")),
            FlowStep(
                "count",
                "stateful_map",
                lambda value, state: StateUpdate(state + 1, value),
                lambda: 0,
            ),
            FlowStep("middle", "map", gate("middle")),
            FlowJoin("last", KeyedJoin("last", "1", ("joined", "extra"), "last", "final")),
            FlowStep("sink", "map", gate("sink")),
        ),
        (
            JoinEdge("a", "first", "a"),
            JoinEdge("b", "first", "b"),
            FlowEdge("first", "count"),
            FlowEdge("count", "middle"),
            JoinEdge("middle", "last", "joined"),
            JoinEdge("c", "last", "extra"),
            FlowEdge("last", "sink"),
        ),
        (FlowEntry("a", "a"), FlowEntry("b", "b"), FlowEntry("c", "c")),
    )
    journal = MultiGraphJournal(tmp_path / "nested", flow, {s: s * 64 for s in ("a", "b", "c")})
    journal.apply(
        request(
            journal,
            (
                *(GraphInput(s, 0, FlowRecord(i, "k")) for i, s in enumerate(("a", "b", "c"))),
                *(GraphEOF(s, 1) for s in ("a", "b", "c")),
            ),
        )
    )
    if failed_step == "sink":
        first = journal.apply(request(journal, (GraphDrain(1),), 2))
        assert first.output_stop == 0  # Effective first drain is not a no-op.
        assert journal.latest().checkpoint.cells == (("count", "k", "1"),)
    before = journal.latest()
    pending = request(journal, (GraphDrain(1),), 3)
    enabled = True
    with pytest.raises(ValidationError, match=failed_step):
        journal.apply(pending)
    assert journal.latest() == before and journal.request(pending.request_id) is None
    enabled = False
    journal.apply(pending)
    if failed_step == "middle":
        journal.apply(request(journal, (GraphDrain(1),), 4))
    page = journal.read_outputs(journal.output_cursor())
    assert len(page.outputs) == 1
    assert journal.latest().checkpoint.sources == (("a", 1, True), ("b", 1, True), ("c", 1, True))
    assert journal.latest().checkpoint.cells == (("count", "k", "1"),)
