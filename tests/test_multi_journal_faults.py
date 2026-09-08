"""Real SQLite writers, publication-ack loss, and bounded failure injection."""

import json
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace

import pytest
from test_multi_journal import graph, request

import stream_quilt.multi_journal as module
from stream_quilt import FlowRecord, GraphInput, OutputError, RecoveryConflict, ValidationError
from stream_quilt.multi_journal import GraphDrain, GraphEOF, MultiGraphJournal


def make(tmp_path, emit="running", callback=lambda x: x):
    return MultiGraphJournal(tmp_path / "j", graph(emit, callback), {"a": "a" * 64, "b": "b" * 64})


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_actual_commit_before_raise_is_resolved_by_persistent_receipt(tmp_path, monkeypatch, error):
    calls = []
    journal = make(tmp_path, callback=lambda x: calls.append(x) or x)
    pending = request(journal, (GraphInput("a", 0, FlowRecord(4, "k")),))

    class LostAck(sqlite3.Connection):
        def commit(self):
            written = self.total_changes > 0
            super().commit()
            if written:
                raise error("response lost after real commit")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=LostAck)
        )
        with pytest.raises(OutputError if error is RuntimeError else error):
            journal.apply(pending)
    assert journal.latest().generation == 1
    confirmed = journal.request(pending.request_id)
    assert confirmed is not None and confirmed.output_stop == 1
    assert journal.apply(pending) == confirmed
    assert calls == [4]
    assert len(journal.read_outputs(journal.output_cursor()).outputs) == 1


@pytest.mark.parametrize("same_id", [False, True])
def test_real_two_writer_cas_and_same_id_publication_not_callback_once(tmp_path, same_id):
    rendezvous = threading.Barrier(2)
    calls = []
    lock = threading.Lock()

    def callback(value):
        with lock:
            calls.append(value)
        rendezvous.wait(timeout=30)
        return value

    journal = make(tmp_path, callback=callback)
    peer = MultiGraphJournal(
        journal.path, journal.flow, dict(journal.source_commitments), create=False
    )
    first = request(journal, (GraphInput("a", 0, FlowRecord(7, "k")),))
    second = first if same_id else replace(first, request_id="2" * 32)

    def apply(instance, pending):
        try:
            return instance.apply(pending)
        except RecoveryConflict as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(apply, journal, first)
        right = pool.submit(apply, peer, second)
        outcomes = [left.result(timeout=60), right.result(timeout=60)]
    assert calls == [7, 7]
    assert journal.latest().generation == 1
    if same_id:
        assert outcomes[0] == outcomes[1]
    else:
        assert sum(isinstance(result, RecoveryConflict) for result in outcomes) == 1
    with closing(sqlite3.connect(journal.path)) as db, db:
        assert db.execute("SELECT count(*) FROM multi_commit").fetchone() == (1,)
        assert db.execute("SELECT count(*) FROM multi_output").fetchone() == (1,)


@pytest.mark.parametrize("stage", ["receipt", "operation", "output", "head", "commit"])
def test_sql_failure_after_each_staged_write_keeps_old_prefix(tmp_path, monkeypatch, stage):
    journal = make(tmp_path)
    before = journal.latest()
    pending = request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),))

    class Broken(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if (stage == "receipt" and sql.startswith("INSERT INTO multi_commit")) or (
                stage == "head" and sql.startswith("UPDATE multi_head")
            ):
                raise sqlite3.OperationalError("injected after SQL write")
            return result

        def executemany(self, sql, parameters):
            result = super().executemany(sql, parameters)
            if (stage == "operation" and sql.startswith("INSERT INTO multi_operation")) or (
                stage == "output" and sql.startswith("INSERT INTO multi_output")
            ):
                raise sqlite3.OperationalError("injected after batch insert")
            return result

        def commit(self):
            if stage == "commit" and self.total_changes:
                raise sqlite3.OperationalError("commit not reached")
            return super().commit()

    with monkeypatch.context() as scoped:
        scoped.setattr(
            journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=Broken)
        )
        with pytest.raises(OutputError):
            journal.apply(pending)
    assert journal.latest() == before
    assert journal.request(pending.request_id) is None


@pytest.mark.parametrize("stage", ["receipt", "operation", "output", "head"])
def test_real_process_death_before_commit_rolls_back_all_four_tables(tmp_path, stage):
    journal = make(tmp_path)
    before = journal.latest()
    script = r"""
import os, sqlite3, sys
from stream_quilt import (
    FlowEntry, FlowJoin, FlowRecord, FlowStep, GraphInput, JoinEdge, KeyedJoin, MultiGraphDataflow,
)
from stream_quilt.multi_journal import MultiGraphJournal, MultiGraphRequest
flow=MultiGraphDataflow(
    "orders","1",(FlowStep("left","map",lambda x:x),FlowStep("right","map",lambda x:x),
    FlowJoin("join",KeyedJoin("join","1",("l","r"),"last","running"))),
    (JoinEdge("left","join","l"),JoinEdge("right","join","r")),
    (FlowEntry("a","left"),FlowEntry("b","right")),
)
j=MultiGraphJournal(sys.argv[1],flow,{"a":"a"*64,"b":"b"*64},create=False)
p=j.latest()
class Crash(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        value=super().execute(sql,parameters)
        if ((sys.argv[2]=="receipt" and sql.startswith("INSERT INTO multi_commit")) or
                (sys.argv[2]=="head" and sql.startswith("UPDATE multi_head"))):
            os._exit(43)
        return value
    def executemany(self,sql,parameters):
        value=super().executemany(sql,parameters)
        if ((sys.argv[2]=="operation" and sql.startswith("INSERT INTO multi_operation")) or
                (sys.argv[2]=="output" and sql.startswith("INSERT INTO multi_output"))):
            os._exit(43)
        return value
j._connect=lambda **kw:sqlite3.connect(j.path,factory=Crash)
j.apply(MultiGraphRequest(p.journal_id,"1"*32,0,(GraphInput("a",0,FlowRecord(3,"k")),)))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(journal.path), stage],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 43, result.stderr
    assert journal.latest() == before
    with closing(sqlite3.connect(journal.path)) as db, db:
        assert [
            db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("multi_commit", "multi_operation", "multi_output")
        ] == [0, 0, 0]


def test_mixed_noops_have_command_indices_but_no_fabricated_operations(tmp_path):
    journal = make(tmp_path, "final")
    receipt = journal.apply(
        request(
            journal,
            (
                GraphDrain(),
                GraphInput("a", 0, FlowRecord(1, "k")),
                GraphEOF("a", 1),
                GraphEOF("a", 1),
                GraphEOF("b", 0),
                GraphDrain(1),
                GraphDrain(1),
            ),
        )
    )
    assert receipt.command_count == 7 and receipt.after_operation == 4
    operations = [journal.operation(i) for i in range(1, 5)]
    assert [op.command_index for op in operations] == [1, 2, 4, 5]
    assert [op.cause for op in operations] == ["process", "eof", "eof", "drain"]
    assert operations[-1].source_id is None and operations[-1].position is None
    assert operations[-1].join_id == "join"


def test_noop_is_no_sql_write_and_result_allocation_precedes_race_check(tmp_path, monkeypatch):
    journal = make(tmp_path)
    pending = request(journal, (GraphDrain(),))
    changes = []

    class Tracked(sqlite3.Connection):
        def close(self):
            changes.append(self.total_changes)
            return super().close()

    with monkeypatch.context() as scoped:
        scoped.setattr(
            journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=Tracked)
        )
        assert journal.apply(pending).status == "no_op"
    assert changes == [0, 0]
    real_encode = module._encoded
    raced = False

    def encode(value, maximum):
        nonlocal raced
        result = real_encode(value, maximum)
        if (
            value.get("kind") == "stream-quilt-multi-receipt"
            and value["status"] == "no_op"
            and not raced
        ):
            raced = True
            journal.apply(request(journal, (GraphEOF("a", 0),), 2))
        return result

    monkeypatch.setattr(module, "_encoded", encode)
    with pytest.raises(RecoveryConflict, match="no-op"):
        journal.apply(pending)
    assert journal.request(pending.request_id) is None


@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_primary_errors_survive_ordinary_cleanup_and_all_resources_attempted(
    tmp_path, monkeypatch, error
):
    journal = make(tmp_path)
    calls = []

    class Broken(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql == "BEGIN":
                raise error("primary")
            return super().execute(sql, parameters)

        def rollback(self):
            calls.append("rollback")
            super().rollback()
            raise RuntimeError("rollback cleanup")

        def close(self):
            calls.append("close")
            super().close()
            raise RuntimeError("close cleanup")

    monkeypatch.setattr(
        journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=Broken)
    )
    with pytest.raises(error, match="primary"):
        journal.latest()
    assert calls == ["rollback", "close"]


@pytest.mark.parametrize(
    "table,cap",
    [
        ("multi_head", "_HEAD_BYTES"),
        ("multi_commit", "_RECEIPT_BYTES"),
        ("multi_operation", "_OPERATION_BYTES"),
        ("multi_output", "_OUTPUT_BYTES"),
    ],
)
def test_sql_utf8_size_guard_precedes_text_materialization(tmp_path, monkeypatch, table, cap):
    journal = make(tmp_path)
    pending = request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),))
    journal.apply(pending)
    cursor = journal.output_cursor()
    oversized = "界" * 600
    with closing(sqlite3.connect(journal.path)) as db, db:
        db.execute(f"UPDATE {table} SET payload=?", (oversized,))
    monkeypatch.setattr(module, cap, 1024)
    real_connect = journal._connect

    def guarded(**kw):
        connection = real_connect(**kw)

        def text(raw):
            assert raw != oversized.encode(), "oversized SQL text reached Python"
            return raw.decode()

        connection.text_factory = text
        return connection

    monkeypatch.setattr(journal, "_connect", guarded)
    with pytest.raises(ValidationError):
        if table == "multi_operation":
            journal.request(pending.request_id)
        elif table == "multi_output":
            journal.read_outputs(cursor)
        else:
            journal.latest()


def test_full_head_utf8_envelope_is_counted_without_double_encoding(tmp_path, monkeypatch):
    journal = make(tmp_path, "final")
    pending = request(journal, (GraphInput("a", 0, FlowRecord("界" * 100, "k")),))
    captured = []
    real_encode = module._encoded

    def encoded(value, maximum):
        payload = real_encode(value, maximum)
        if value.get("kind") == "stream-quilt-multi-recovery-point":
            assert type(value["checkpoint"]) is dict
            assert type(json.loads(payload)["checkpoint"]) is dict
            captured.append(len(payload.encode()))
        return payload

    monkeypatch.setattr(module, "_encoded", encoded)
    receipt = journal.apply(pending)
    assert receipt.status == "committed" and captured
    before = journal.latest()
    with closing(sqlite3.connect(journal.path)) as db, db:
        current_size = db.execute(
            "SELECT length(CAST(payload AS BLOB)) FROM multi_head"
        ).fetchone()[0]
    monkeypatch.setattr(module, "_HEAD_BYTES", current_size + 10)
    with pytest.raises(ValidationError, match="bytes"):
        journal.apply(request(journal, (GraphInput("a", 1, FlowRecord("界" * 300, "other")),), 2))
    assert journal.latest() == before


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
def test_first_control_identity_survives_later_cleanup_controls(tmp_path, monkeypatch, kind):
    journal = make(tmp_path)
    primary = kind("primary")
    cleanup = SystemExit("later") if kind is KeyboardInterrupt else KeyboardInterrupt("later")
    seen = []

    class Broken(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql == "BEGIN":
                raise primary
            return super().execute(sql, parameters)

        def rollback(self):
            seen.append("rollback")
            super().rollback()
            raise cleanup

        def close(self):
            seen.append("close")
            super().close()
            raise SystemExit("last")

    monkeypatch.setattr(
        journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=Broken)
    )
    caught = None
    try:
        journal.latest()
    except BaseException as error:
        caught = error
    assert caught is primary
    assert seen == ["rollback", "close"]


@pytest.mark.parametrize("missing", ["all", "tail"])
def test_output_query_truncation_is_not_a_successful_short_page(tmp_path, monkeypatch, missing):
    journal = make(tmp_path)
    journal.apply(request(journal, tuple(GraphInput("a", i, FlowRecord(i, "k")) for i in range(2))))
    cursor = journal.output_cursor()

    class Truncated(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if sql == module._SELECT_OUTPUTS:
                return iter([] if missing == "all" else result.fetchmany(1))
            return result

    monkeypatch.setattr(
        journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=Truncated)
    )
    with pytest.raises(ValidationError, match="incomplete"):
        journal.read_outputs(cursor)


def test_successful_commit_then_close_failure_still_has_one_receipt(tmp_path, monkeypatch):
    journal = make(tmp_path)
    pending = request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),))

    class CloseFails(sqlite3.Connection):
        def close(self):
            written = self.total_changes
            super().close()
            if written:
                raise RuntimeError("post-publication close failed")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=CloseFails)
        )
        with pytest.raises(OutputError, match="cleanup failed"):
            journal.apply(pending)
    assert journal.latest().generation == 1
    assert journal.apply(pending) == journal.request(pending.request_id)


def test_lookup_none_while_writer_in_flight_is_only_a_snapshot(tmp_path):
    entered, release = threading.Event(), threading.Event()

    def callback(value):
        entered.set()
        if not release.wait(timeout=30):
            raise RuntimeError("test writer release timeout")
        return value

    journal = make(tmp_path, callback=callback)
    pending = request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(journal.apply, pending)
        try:
            assert entered.wait(timeout=30)
            assert journal.request(pending.request_id) is None
        finally:
            release.set()
        receipt = future.result(timeout=60)
    assert journal.request(pending.request_id) == receipt
