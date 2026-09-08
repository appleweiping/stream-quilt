from __future__ import annotations

import hashlib
import json
import runpy
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

import stream_quilt.flow_journal as engine
from stream_quilt import (
    Dataflow,
    FlowBranch,
    FlowEdge,
    FlowExecutionError,
    FlowJournal,
    FlowLimits,
    FlowMerge,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    GraphDataflow,
    GraphJournal,
    GraphJournalOutput,
    GraphLimits,
    GraphRecoveryPoint,
    GraphRuntime,
    OutputError,
    RecoveryConflict,
    StateUpdate,
    ValidationError,
)

SOURCE = hashlib.sha256(b"graph-source-content-and-order-v1").hexdigest()


def graph(*, callback=None, revision="v1", limits=None):
    return GraphDataflow(
        "two-sums",
        revision,
        (
            FlowStep("root", "map", lambda value: value),
            FlowStep(
                "left",
                "stateful_map",
                lambda value, state: StateUpdate(state + value, state + value),
                lambda: 0,
            ),
            FlowStep(
                "right",
                "stateful_map",
                callback
                or (lambda value, state: StateUpdate(state + value * 10, state + value * 10)),
                lambda: 0,
            ),
        ),
        (FlowEdge("root", "right"), FlowEdge("root", "left")),
        entry="root",
        limits=limits or GraphLimits(),
    )


def records(*values):
    return [FlowRecord(value, "a") for value in values]


def snapshot(journal):
    return [
        (item.sequence, item.source_position, item.step_id, item.record.key, item.record.value)
        for item in journal.outputs()
    ]


def rewrite(path, table, sequence, mutation):
    with closing(sqlite3.connect(path)) as connection, connection:
        where = "slot=1" if table == "flow_head" else f"seq={sequence}"
        payload = connection.execute(f"SELECT payload FROM {table} WHERE {where}").fetchone()[0]
        document = json.loads(payload)
        mutation(document)
        payload = engine._encode(document)
        connection.execute(
            f"UPDATE {table} SET payload=?,digest=? WHERE {where}",
            (payload, engine._digest(payload)),
        )


def test_actual_sqlite_prefix_reopen_state_and_terminal_order_manual_oracle(tmp_path):
    path = tmp_path / "nested" / "run.sqlite"
    journal = GraphJournal(path, graph(), SOURCE)
    first = journal.advance([FlowRecord(2, "a"), FlowRecord(3, "b")], expected_generation=0)
    assert (first.generation, first.next_position, first.checkpoint.emitted_records) == (1, 2, 4)
    reopened = GraphJournal(path, graph(), SOURCE, create=False)
    after = reopened.advance([FlowRecord(4, "a")], expected_generation=1)
    assert after.checkpoint.cells == (
        ("left", "a", "6"),
        ("left", "b", "3"),
        ("right", "a", "60"),
        ("right", "b", "30"),
    )
    assert snapshot(reopened) == [
        (0, 0, "left", "a", 2),
        (1, 0, "right", "a", 20),
        (2, 1, "left", "b", 3),
        (3, 1, "right", "b", 30),
        (4, 2, "left", "a", 6),
        (5, 2, "right", "a", 60),
    ]
    with closing(sqlite3.connect(path)) as database:
        assert database.execute("PRAGMA application_id").fetchone() == (0x5351474A,)
        assert database.execute("PRAGMA user_version").fetchone() == (1,)
        payload, digest = database.execute("SELECT payload,digest FROM flow_head").fetchone()
        assert hashlib.sha256(payload.encode()).hexdigest() == digest
        document = json.loads(payload)
        assert document["kind"] == "stream-quilt-graph-recovery-point"
        assert document["checkpoint"]["kind"] == "stream-quilt-graph-checkpoint"
        assert document["next_position"] == 3
        assert database.execute(
            "SELECT count(*),min(seq),max(seq) FROM flow_output"
        ).fetchone() == (6, 0, 5)
    assert [item.sequence for item in reopened.outputs(start=3, limit=2)] == [3, 4]
    assert list(reopened.outputs(start=99)) == []


def test_diamond_merge_order_and_recovery_use_independent_running_sum(tmp_path):
    spec = GraphDataflow(
        "diamond",
        "1",
        (
            FlowStep("input", "map", lambda value: value),
            FlowStep("double", "map", lambda value: value * 2),
            FlowStep("offset", "map", lambda value: value + 10),
            FlowMerge("merge"),
            FlowStep(
                "sum",
                "stateful_map",
                lambda value, state: StateUpdate(state + value, state + value),
                lambda: 0,
            ),
        ),
        (
            FlowEdge("input", "double"),
            FlowEdge("input", "offset"),
            FlowEdge("offset", "merge"),
            FlowEdge("double", "merge"),
            FlowEdge("merge", "sum"),
        ),
        entry="input",
    )
    path = tmp_path / "diamond.sqlite"
    first = GraphJournal(path, spec, SOURCE).advance(records(1), expected_generation=0)
    after = GraphJournal(path, spec, SOURCE, create=False).advance(
        records(2), expected_generation=first.generation
    )
    assert after.checkpoint.cells == (("sum", "a", "29"),)
    assert [
        (item.source_position, item.record.value)
        for item in GraphJournal(path, spec, SOURCE).outputs()
    ] == [(0, 11), (0, 13), (1, 25), (1, 29)]


def test_conditional_filtered_inputs_advance_source_offset_without_output(tmp_path):
    spec = GraphDataflow(
        "filter",
        "1",
        (
            FlowBranch("route", lambda value: value > 0),
            FlowStep("keep", "map", lambda value: value),
            FlowStep("drop", "filter", lambda value: False),
        ),
        (FlowEdge("route", "keep", True), FlowEdge("route", "drop", False)),
        entry="route",
    )
    journal = GraphJournal(tmp_path / "filtered.sqlite", spec, SOURCE)
    point = journal.advance(records(0, 2, -1, 3), expected_generation=0)
    assert point.next_position == 4 and point.checkpoint.emitted_records == 2
    assert [
        (item.source_position, item.step_id, item.record.value) for item in journal.outputs()
    ] == [(1, "keep", 2), (3, "keep", 3)]
    assert journal.advance([], expected_generation=1) == point


def test_late_sibling_failure_rolls_back_whole_batch_and_source_is_not_rewound(tmp_path):
    seen = []

    def callback(value, state):
        seen.append(value)
        if value == 9:
            raise RuntimeError("late sibling")
        return StateUpdate(state + value * 10, state + value * 10)

    journal = GraphJournal(tmp_path / "run.sqlite", graph(callback=callback), SOURCE)
    before = journal.advance(records(1), expected_generation=0)
    source = iter(records(2, 9, 4))
    with pytest.raises(FlowExecutionError) as caught:
        journal.advance(source, expected_generation=1)
    assert caught.value.step_id == "right"
    assert journal.latest() == before and len(snapshot(journal)) == 2
    assert next(source).value == 4 and seen == [1, 2, 9]


def test_no_lookahead_source_failure_and_empty_source_cas(tmp_path):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)

    def source():
        yield FlowRecord(2, "a")
        raise RuntimeError("next pull")

    iterator = source()
    before = journal.advance(iterator, expected_generation=0, max_inputs=1)
    with pytest.raises(RuntimeError, match="next pull"):
        journal.advance(iterator, expected_generation=1)
    assert journal.latest() == before

    def empty_race():
        journal.advance(records(1), expected_generation=1)
        return
        yield  # pragma: no cover

    with pytest.raises(RecoveryConflict, match="empty input"):
        journal.advance(empty_race(), expected_generation=1)


def test_real_two_writer_barrier_one_atomic_winner_no_retry(tmp_path):
    barrier = threading.Barrier(2)
    calls = []
    lock = threading.Lock()

    def callback(value, state):
        with lock:
            calls.append(value)
        barrier.wait(timeout=10)
        return StateUpdate(state + value, state + value)

    path = tmp_path / "race.sqlite"
    spec = graph(callback=callback)
    journals = [GraphJournal(path, spec, SOURCE), GraphJournal(path, spec, SOURCE)]

    def advance(pair):
        journal, value = pair
        try:
            return journal.advance(records(value), expected_generation=0)
        except RecoveryConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(advance, zip(journals, (2, 3), strict=True)))
    winners = [item for item in results if item is not None]
    assert len(winners) == 1 and sorted(calls) == [2, 3]
    values = snapshot(journals[0])
    assert len(values) == 2 and values[0][-1] == values[1][-1]
    assert journals[0].latest().next_position == 1
    pulled = []

    def source():
        pulled.append(1)
        yield FlowRecord(1, "a")

    with pytest.raises(RecoveryConflict, match="before processing"):
        journals[0].advance(source(), expected_generation=0)
    assert not pulled


def test_reentrant_callback_can_commit_competitor_but_outer_conflicts(tmp_path):
    journal = None
    calls = []

    def callback(value, state):
        calls.append(value)
        if value == 9:
            journal.advance(records(2), expected_generation=0)
        return StateUpdate(state + value, state + value)

    journal = GraphJournal(tmp_path / "run.sqlite", graph(callback=callback), SOURCE)
    with pytest.raises(RecoveryConflict, match="during processing"):
        journal.advance(records(9), expected_generation=0)
    assert calls == [9, 2] and journal.latest().next_position == 1
    assert [row[-1] for row in snapshot(journal)] == [2, 2]


@pytest.mark.parametrize("fault", ["insert", "head", "commit"])
def test_sql_failure_preserves_previous_head_all_sibling_outputs_and_closes(
    tmp_path, monkeypatch, fault
):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    before = journal.advance(records(1), expected_generation=0)
    connections = []

    class Broken(sqlite3.Connection):
        def executemany(self, sql, values):
            values = list(values)
            if fault == "insert":
                self.execute(sql, values[0])
                raise sqlite3.OperationalError("after first sibling")
            return super().executemany(sql, values)

        def execute(self, sql, *args):
            result = super().execute(sql, *args)
            if fault == "head" and sql.startswith("UPDATE flow_head"):
                raise sqlite3.OperationalError("after head update")
            return result

        def commit(self):
            if (
                fault == "commit"
                and self.in_transaction
                and self.execute("SELECT count(*) FROM flow_output").fetchone()[0] > 2
            ):
                raise sqlite3.OperationalError("before commit")
            return super().commit()

    def connect(**kwargs):
        connection = sqlite3.connect(journal.path, factory=Broken)
        connections.append(connection)
        return connection

    with monkeypatch.context() as scoped:
        scoped.setattr(journal, "_connect", connect)
        with pytest.raises(OutputError, match=r"previous prefix retained|inspect latest"):
            journal.advance(records(2, 3), expected_generation=1)
    assert journal.latest() == before and len(snapshot(journal)) == 2
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def test_process_death_after_output_insert_and_after_head_update_rolls_back(tmp_path):
    path = tmp_path / "run.sqlite"
    journal = GraphJournal(path, graph(), SOURCE)
    before = journal.advance(records(1), expected_generation=0)
    script = """
import os, sqlite3, sys
from stream_quilt import FlowEdge,FlowStep,FlowRecord,GraphDataflow,GraphJournal,StateUpdate
spec=GraphDataflow('two-sums','v1',(
 FlowStep('root','map',lambda v:v),
 FlowStep('left','stateful_map',lambda v,s:StateUpdate(s+v,s+v),lambda:0),
 FlowStep('right','stateful_map',lambda v,s:StateUpdate(s+v*10,s+v*10),lambda:0),
),(FlowEdge('root','right'),FlowEdge('root','left')),entry='root')
j=GraphJournal(sys.argv[1],spec,sys.argv[2],create=False)
class Crash(sqlite3.Connection):
 def executemany(self,sql,rows):
  super().executemany(sql,rows)
  if sys.argv[3]=='insert': os._exit(23)
 def execute(self,sql,*args):
  result=super().execute(sql,*args)
  if sys.argv[3]=='head' and sql.startswith('UPDATE flow_head'): os._exit(24)
  return result
j._connect=lambda **kwargs:sqlite3.connect(j.path,factory=Crash)
j.advance([FlowRecord(9,'a')],expected_generation=1)
"""
    for phase, code in (("insert", 23), ("head", 24)):
        result = subprocess.run(
            [sys.executable, "-c", script, str(path), SOURCE, phase],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == code, result.stderr
        assert journal.latest() == before and len(snapshot(journal)) == 2
    after = GraphJournal(path, graph(), SOURCE, create=False).advance(
        records(2), expected_generation=1
    )
    assert after.next_position == 2 and [row[-1] for row in snapshot(journal)] == [1, 10, 3, 30]


def test_stable_output_page_under_wal_and_early_close(tmp_path, monkeypatch):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    with closing(sqlite3.connect(journal.path)) as database:
        assert database.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    journal.advance(records(1, 2), expected_generation=0)
    page = journal.outputs()
    assert next(page).record.value == 1
    journal.advance(records(3), expected_generation=1)
    assert [item.record.value for item in page] == [10, 3, 30]
    connections = []
    connect = journal._connect

    def tracked(**kwargs):
        connection = connect(**kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(journal, "_connect", tracked)
    page = journal.outputs()
    next(page)
    page.close()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(extra=True),
        lambda d: d.update(kind="linear"),
        lambda d: d.update(schema_version="2.0"),
        lambda d: d.update(generation=True),
        lambda d: d.update(generation=0),
        lambda d: d.update(next_position=9),
        lambda d: d["checkpoint"].update(kind="stream-quilt-dataflow-checkpoint"),
        lambda d: d["checkpoint"].update(cells=[{"step": "root", "key": "a", "value": 1}]),
    ],
)
def test_rehashed_head_corruption_rejected(tmp_path, mutation):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    journal.advance(records(1), expected_generation=0)
    rewrite(journal.path, "flow_head", 0, mutation)
    with pytest.raises(ValidationError):
        journal.latest()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(extra=True),
        lambda d: d.update(kind="linear"),
        lambda d: d.update(sequence=True),
        lambda d: d.update(source_position=1),
        lambda d: d.update(step_id="root"),
        lambda d: d.update(step_id="missing"),
        lambda d: d.update(record=[]),
        lambda d: d["record"].update(extra=True),
        lambda d: d["record"].update(value="x" * 101),
    ],
)
def test_rehashed_output_corruption_rejected(tmp_path, mutation):
    spec = graph(limits=GraphLimits(operator_limits=FlowLimits(max_record_bytes=100)))
    journal = GraphJournal(tmp_path / "run.sqlite", spec, SOURCE)
    journal.advance(records(1), expected_generation=0)
    rewrite(journal.path, "flow_output", 0, mutation)
    with pytest.raises(ValidationError):
        list(journal.outputs())


def test_rehashed_terminal_order_and_source_prefix_are_checked(tmp_path):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    journal.advance(records(1), expected_generation=0)
    rewrite(journal.path, "flow_output", 0, lambda d: d.update(step_id="right"))
    rewrite(journal.path, "flow_output", 1, lambda d: d.update(step_id="left"))
    page = journal.outputs()
    assert next(page).step_id == "right"
    with pytest.raises(ValidationError, match="order"):
        next(page)
    with pytest.raises(ValidationError, match="order"):
        list(journal.outputs(start=1, limit=1))


@pytest.mark.parametrize("kind", ["linear", "graph"])
def test_source_order_corruption_across_single_row_page_boundary(tmp_path, kind):
    if kind == "graph":
        journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
        boundary = 2
    else:
        journal = FlowJournal(
            tmp_path / "run.sqlite",
            Dataflow("linear", "1", (FlowStep("map", "map", lambda value: value),)),
            SOURCE,
        )
        boundary = 1
    journal.advance(records(1, 2, 3), expected_generation=0)
    rewrite(
        journal.path,
        "flow_output",
        boundary - 1,
        lambda document: document.update(source_position=2),
    )
    with pytest.raises(ValidationError, match="position/order"):
        list(journal.outputs(start=boundary, limit=1))


def test_terminal_order_uses_topological_execution_not_raw_declaration_order(tmp_path):
    spec = GraphDataflow(
        "nontrivial-order",
        "1",
        (
            FlowStep("root", "map", lambda value: value),
            FlowStep("deep", "map", lambda value: value * 2),
            FlowStep("shallow", "map", lambda value: value + 10),
            FlowStep("middle", "map", lambda value: value),
        ),
        (FlowEdge("root", "middle"), FlowEdge("middle", "deep"), FlowEdge("root", "shallow")),
        entry="root",
    )
    journal = GraphJournal(tmp_path / "order.sqlite", spec, SOURCE)
    journal.advance(records(3), expected_generation=0)
    assert [(item.step_id, item.record.value) for item in journal.outputs()] == [
        ("shallow", 13),
        ("deep", 6),
    ]
    assert [item.step_id for item in journal.outputs(start=1, limit=1)] == ["deep"]


@pytest.mark.parametrize("error", [sqlite3.OperationalError, KeyboardInterrupt])
def test_commit_then_acknowledgement_failure_reports_unknown_outcome_and_keeps_committed_prefix(
    tmp_path, monkeypatch, error
):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)

    class LostAcknowledgement(sqlite3.Connection):
        def commit(self):
            rows = self.execute("SELECT count(*) FROM flow_output").fetchone()[0]
            super().commit()
            if rows:
                raise error("after actual commit")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            journal,
            "_connect",
            lambda **kwargs: sqlite3.connect(journal.path, factory=LostAcknowledgement),
        )
        with pytest.raises(OutputError if error is sqlite3.OperationalError else error) as caught:
            journal.advance(records(2), expected_generation=0)
        diagnostic = (
            str(caught.value) if error is sqlite3.OperationalError else caught.value.__notes__[0]
        )
        assert "inspect latest before replay" in diagnostic
    assert journal.latest().next_position == 1
    assert [item.record.value for item in journal.outputs()] == [2, 20]


def test_sql_guard_rejects_oversized_row_before_python_materialization(tmp_path, monkeypatch):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    journal.advance(records(1), expected_generation=0)
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute("UPDATE flow_output SET payload=? WHERE seq=0", ("x" * 4096,))
    monkeypatch.setattr(engine, "_MAX_DOCUMENT_BYTES", 2048)
    connect = journal._connect

    def guarded(**kwargs):
        connection = connect(**kwargs)

        def text_factory(value):
            assert len(value) <= 2048, "oversized payload escaped SQL guard"
            return value.decode()

        connection.text_factory = text_factory
        return connection

    monkeypatch.setattr(journal, "_connect", guarded)
    with pytest.raises(ValidationError, match="document"):
        list(journal.outputs())


@pytest.mark.parametrize("limit", ["batch_outputs", "document", "history"])
def test_shared_journal_limits_abort_all_siblings_and_source_batch(tmp_path, monkeypatch, limit):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    before = journal.latest()
    if limit == "batch_outputs":
        monkeypatch.setattr(engine, "_MAX_BATCH_OUTPUTS", 1)
    elif limit == "history":
        monkeypatch.setattr(engine, "_MAX_RECORDS", 1)
    else:
        monkeypatch.setattr(engine, "_MAX_DOCUMENT_BYTES", 1024)
    with pytest.raises(ValidationError):
        journal.advance(records(1, 2, 3, 4, 5), expected_generation=0)
    assert journal.latest() == before and snapshot(journal) == []


def test_wrong_source_graph_revision_limits_or_topology_refuse_reopen(tmp_path):
    path = tmp_path / "run.sqlite"
    spec = graph()
    GraphJournal(path, spec, SOURCE)
    variants = [
        graph(revision="v2"),
        graph(limits=GraphLimits(max_work_records=50)),
        replace(spec, edges=tuple(reversed(spec.edges))),
    ]
    for wrong in variants:
        with pytest.raises(ValidationError, match="identity"):
            GraphJournal(path, wrong, SOURCE, create=False)
    with pytest.raises(ValidationError, match="identity"):
        GraphJournal(path, spec, "b" * 64)


def test_linear_and_graph_databases_never_cross_open_or_create_wrong_type(tmp_path):
    linear = Dataflow("linear", "1", (FlowStep("identity", "map", lambda v: v),))
    paths = [tmp_path / "linear.sqlite", tmp_path / "graph.sqlite"]
    FlowJournal(paths[0], linear, SOURCE)
    GraphJournal(paths[1], graph(), SOURCE)
    before = [path.read_bytes() for path in paths]
    with pytest.raises(ValidationError, match="schema"):
        GraphJournal(paths[0], graph(), SOURCE)
    with pytest.raises(ValidationError, match="schema"):
        FlowJournal(paths[1], linear, SOURCE)
    assert [path.read_bytes() for path in paths] == before
    for constructor, spec in ((GraphJournal, linear), (FlowJournal, graph())):
        path = tmp_path / "wrong" / "run.sqlite"
        with pytest.raises(ValidationError):
            constructor(path, spec, SOURCE)
        assert not path.parent.exists()


def test_unrelated_missing_removed_and_schema_changed_database_refused(tmp_path):
    path = tmp_path / "run.sqlite"
    with pytest.raises(ValidationError, match="does not exist"):
        GraphJournal(path, graph(), SOURCE, create=False)
    with closing(sqlite3.connect(path)) as database, database:
        database.execute("CREATE TABLE unrelated (value)")
    before = path.read_bytes()
    with pytest.raises(ValidationError, match="schema"):
        GraphJournal(path, graph(), SOURCE)
    assert path.read_bytes() == before
    other = tmp_path / "other.sqlite"
    journal = GraphJournal(other, graph(), SOURCE)
    with closing(sqlite3.connect(other)) as database, database:
        database.execute("PRAGMA application_id=0")
    with pytest.raises(ValidationError, match="identity"):
        journal.latest()
    other.unlink()
    with pytest.raises(OutputError):
        journal.latest()
    assert not other.exists()


def test_strict_models_generation_zero_and_public_options(tmp_path):
    checkpoint = GraphRuntime(graph()).checkpoint()
    for point in [
        replace(checkpoint, processed_inputs=1),
        replace(checkpoint, cells=(("left", "a", "9"),)),
    ]:
        with pytest.raises(ValidationError):
            GraphRecoveryPoint(SOURCE, 0, 0, point)
    linear = FlowRuntime(
        Dataflow("linear", "1", (FlowStep("identity", "map", lambda v: v),))
    ).checkpoint()
    with pytest.raises(ValidationError):
        GraphRecoveryPoint(SOURCE, 0, 0, linear)
    for args in [
        (True, 0, "left", FlowRecord(1)),
        (0, -1, "left", FlowRecord(1)),
        (0, 0, "", FlowRecord(1)),
        (0, 0, "left", {}),
    ]:
        with pytest.raises(ValidationError):
            GraphJournalOutput(*args)
    with pytest.raises(ValidationError):
        GraphJournal(tmp_path / "bad.sqlite", graph(), SOURCE, create=1)
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    for value in [True, -1, 0, 10001]:
        with pytest.raises(ValidationError):
            journal.advance([], expected_generation=0, max_inputs=value)
        with pytest.raises(ValidationError):
            list(journal.outputs(limit=value))
    with pytest.raises(ValidationError):
        journal._runtime(linear)
    with pytest.raises(ValidationError):
        journal._point(0, 0, linear)
    with pytest.raises(ValidationError):
        journal._output(0, 0, FlowRecord(1))


def test_frozen_linear_wire_format_is_byte_identical_after_shared_engine_extraction(tmp_path):
    path = tmp_path / "linear.sqlite"
    spec = Dataflow("compat", "v1", (FlowStep("identity", "map", lambda value: value),))
    journal = FlowJournal(path, spec, "a" * 64)
    journal.advance([FlowRecord(1, "a")], expected_generation=0)
    with closing(sqlite3.connect(path)) as database:
        assert database.execute("PRAGMA application_id").fetchone() == (1397835338,)
        assert database.execute("SELECT payload FROM flow_head").fetchone()[0] == (
            '{"checkpoint":{"cells":[],"emitted_records":1,"identity":"0dec5de48f456a305212d836ccdfbbd73314fe9c76a38a6c5cddb8269ac48f2f",'
            '"kind":"stream-quilt-dataflow-checkpoint","processed_inputs":1,"schema_version":"1.0"},'
            '"generation":1,"next_position":1,"schema_version":"1.0","source_id":"'
            + "a" * 64
            + '"}'
        )
        assert (
            database.execute("SELECT payload FROM flow_output").fetchone()[0]
            == '{"record":{"key":"a","value":1},"sequence":0,"source_position":0}'
        )


def test_offline_graph_recovery_example(capsys):
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "durable_branching_totals.py"),
        run_name="__main__",
    )
    result = json.loads(capsys.readouterr().out)
    assert result["next_position"] == 3
    assert [row["record"]["value"] for row in result["outputs"]] == [2, 20, 5, 50, 9, 90]


@pytest.mark.parametrize("control", [KeyboardInterrupt, SystemExit])
def test_sql_operation_control_survives_rollback_and_close_ordinary_failures(
    tmp_path, monkeypatch, control
):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    seen = []

    class Broken(sqlite3.Connection):
        def execute(self, sql, *args):
            if sql == "BEGIN":
                raise control("operation")
            return super().execute(sql, *args)

        def rollback(self):
            seen.append("rollback")
            raise sqlite3.OperationalError("rollback")

        def close(self):
            super().close()
            seen.append("close")
            raise sqlite3.OperationalError("close")

    connection = sqlite3.connect(journal.path, factory=Broken)
    monkeypatch.setattr(journal, "_connect", lambda **kwargs: connection)
    with pytest.raises(control, match="operation") as caught:
        journal.latest()
    assert seen == ["rollback", "close"]
    assert "rollback, close" in caught.value.__notes__[0]


def test_rollback_control_not_masked_by_later_close_error(tmp_path, monkeypatch):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    closed = []

    class Broken(sqlite3.Connection):
        def execute(self, sql, *args):
            raise sqlite3.OperationalError("operation")

        def rollback(self):
            raise KeyboardInterrupt("rollback")

        def close(self):
            super().close()
            closed.append(True)
            raise sqlite3.OperationalError("close")

    monkeypatch.setattr(
        journal, "_connect", lambda **kwargs: sqlite3.connect(journal.path, factory=Broken)
    )
    with pytest.raises(KeyboardInterrupt, match="rollback"):
        journal.latest()
    assert closed == [True]


@pytest.mark.parametrize("operation", ["latest", "page_close"])
def test_success_or_early_page_close_reports_connection_cleanup_failure(
    tmp_path, monkeypatch, operation
):
    journal = GraphJournal(tmp_path / "run.sqlite", graph(), SOURCE)
    journal.advance(records(1), expected_generation=0)
    closed = []

    class Broken(sqlite3.Connection):
        def close(self):
            super().close()
            closed.append(True)
            raise sqlite3.OperationalError("close")

    monkeypatch.setattr(
        journal, "_connect", lambda **kwargs: sqlite3.connect(journal.path, factory=Broken)
    )
    with pytest.raises(OutputError, match="cleanup failed"):
        if operation == "latest":
            journal.latest()
        else:
            page = journal.outputs()
            next(page)
            page.close()
    assert closed == [True]


def test_linear_codec_still_rejects_graph_raw_outputs(tmp_path):
    from stream_quilt import GraphOutput

    journal = FlowJournal(
        tmp_path / "linear.sqlite",
        Dataflow("linear", "1", (FlowStep("id", "map", lambda v: v),)),
        SOURCE,
    )
    with pytest.raises(ValidationError):
        journal._output(0, 0, GraphOutput("id", FlowRecord(1)))
