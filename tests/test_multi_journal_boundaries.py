"""Configuration, wire, capacity and recovery failures without expensive oversized fixtures."""

import json
import runpy
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest
from test_multi_journal import graph, request
from test_multi_journal_contracts import rewrite

import stream_quilt.multi_journal as module
import stream_quilt.multi_journal_types as wire
from stream_quilt import FlowRecord, GraphInput, MultiGraphRuntime, OutputError, ValidationError
from stream_quilt.multi_journal import (
    GraphDrain,
    GraphEOF,
    MultiGraphJournal,
    MultiGraphJournalOutput,
    MultiGraphOperation,
    MultiGraphOutputCursor,
    MultiGraphOutputPage,
    MultiGraphReceipt,
    MultiGraphRecoveryPoint,
    MultiGraphRequest,
)


def make(tmp_path):
    return MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})


@pytest.mark.parametrize(
    "commitments,create",
    [
        ([], True),
        ({}, True),
        ({"a": "a" * 64}, True),
        ({"a": "a" * 64, "b": "b" * 64}, 1),
        ({"a": "a" * 63, "b": "b" * 64}, True),
    ],
)
def test_invalid_configuration_does_not_touch_path(tmp_path, commitments, create):
    path = tmp_path / "absent" / "j"
    with pytest.raises(ValidationError):
        MultiGraphJournal(path, graph(), commitments, create=create)
    assert not path.parent.exists()


def test_hostile_mapping_iteration_is_bounded(tmp_path):
    class Infinite(Mapping):
        def __len__(self):
            return 2

        def __iter__(self):
            for i in range(18):
                yield str(i)
            pytest.fail("mapping iteration exceeded admission bound")

        def __getitem__(self, key):
            return "a" * 64

    with pytest.raises(ValidationError):
        MultiGraphJournal(tmp_path / "j", graph(), Infinite())
    assert not (tmp_path / "j").exists()


def test_missing_journal_never_recreated_and_path_bound_before_cwd_change(tmp_path, monkeypatch):
    with pytest.raises(ValidationError, match="does not exist"):
        MultiGraphJournal(
            tmp_path / "missing", graph(), {"a": "a" * 64, "b": "b" * 64}, create=False
        )
    monkeypatch.chdir(tmp_path)
    journal = MultiGraphJournal("relative.sqlite", graph(), {"a": "a" * 64, "b": "b" * 64})
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert journal.latest().generation == 0
    journal.path.unlink()
    with pytest.raises(OutputError):
        journal.latest()
    assert not journal.path.exists() and not (elsewhere / "relative.sqlite").exists()


@pytest.mark.parametrize(
    "sql",
    [
        "PRAGMA application_id=1",
        "PRAGMA user_version=2",
        "CREATE VIEW unexpected AS SELECT 1",
        "ALTER TABLE multi_head ADD COLUMN extra INTEGER",
        "DELETE FROM multi_head",
        "DELETE FROM multi_output",
        "DELETE FROM multi_commit",
    ],
)
def test_schema_or_prefix_corruption_is_rejected(tmp_path, sql):
    journal = make(tmp_path)
    journal.apply(request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),)))
    with closing(sqlite3.connect(journal.path)) as db, db:
        db.execute(sql)
    with pytest.raises(ValidationError):
        journal.latest()


def test_replaced_journal_and_wrong_source_commitment_are_rejected(tmp_path):
    journal = make(tmp_path)
    with pytest.raises(ValidationError, match="identity"):
        MultiGraphJournal(journal.path, journal.flow, {"a": "c" * 64, "b": "b" * 64})
    journal.path.unlink()
    replacement = MultiGraphJournal(journal.path, journal.flow, dict(journal.source_commitments))
    assert replacement.latest().generation == 0
    with pytest.raises(ValidationError, match="identity"):
        journal.latest()


@pytest.mark.parametrize(
    "method", ["latest", "request", "operation", "output_cursor", "read_outputs"]
)
def test_sql_read_errors_are_normalized_and_close_connection(tmp_path, monkeypatch, method):
    journal = make(tmp_path)
    cursor = journal.output_cursor()
    closed = []

    class Broken(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            raise sqlite3.OperationalError("read failure")

        def close(self):
            super().close()
            closed.append(True)

    monkeypatch.setattr(
        journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=Broken)
    )
    arguments = {"request": ("a" * 32,), "operation": (1,), "read_outputs": (cursor,)}
    with pytest.raises(OutputError):
        getattr(journal, method)(*arguments.get(method, ()))
    assert closed == [True]


def test_failed_connection_setup_attempts_close(tmp_path, monkeypatch):
    journal = make(tmp_path)
    real_connect = sqlite3.connect
    closed = []

    class Setup(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql == "PRAGMA foreign_keys=ON":
                raise sqlite3.OperationalError("setup failed")
            return super().execute(sql, parameters)

        def close(self):
            closed.append(True)
            return super().close()

    monkeypatch.setattr(
        module.sqlite3, "connect", lambda *a, **kw: real_connect(*a, **kw, factory=Setup)
    )
    with pytest.raises(OutputError):
        journal.latest()
    assert closed == [True]


@pytest.mark.parametrize(
    "ceiling,value",
    [
        ("_MAX_HISTORY", 1),
        ("_MAX_BATCH_OUTPUTS", 1),
        ("_BATCH_BYTES", 1),
        ("_METADATA_BYTES", 1),
        ("_METADATA_BYTES", 1000),
    ],
)
def test_staged_capacity_failure_keeps_all_prefixes(tmp_path, monkeypatch, ceiling, value):
    journal = make(tmp_path)
    pending = request(journal, tuple(GraphInput("a", i, FlowRecord(i, "k")) for i in range(2)))
    before = journal.latest()
    with monkeypatch.context() as scoped:
        scoped.setattr(module, ceiling, value)
        with pytest.raises(ValidationError):
            journal.apply(pending)
    assert journal.latest() == before and journal.request(pending.request_id) is None


def test_api_type_guards_and_initial_empty_page(tmp_path):
    journal = make(tmp_path)
    page = journal.read_outputs(journal.output_cursor())
    assert page.outputs == () and page.cursor.next_sequence == 0
    with pytest.raises(ValidationError):
        journal.read_outputs(None)
    with pytest.raises(ValidationError):
        journal.apply(None)
    pending = request(journal, ())
    with pytest.raises(ValidationError):
        journal.apply(replace(pending, journal_id="f" * 32))
    with pytest.raises(ValidationError):
        journal.operation(1)


@pytest.mark.parametrize(
    "field,value",
    [
        ("sequence", 1),
        ("operation_sequence", 2),
        ("step_id", "left"),
    ],
)
def test_rehashed_output_index_or_terminal_contract_is_rejected(tmp_path, field, value):
    journal = make(tmp_path)
    journal.apply(request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),)))
    cursor = journal.output_cursor()
    rewrite(journal, "multi_output", 0, lambda v: v.update({field: value}))
    with pytest.raises(ValidationError):
        journal.read_outputs(cursor)


def receipt():
    return MultiGraphReceipt(
        "a" * 32,
        "b" * 32,
        "c" * 64,
        "committed",
        1,
        0,
        1,
        0,
        1,
        0,
        1,
        (("a", 0, False), ("b", 0, False)),
        (("a", 1, False), ("b", 0, False)),
        "d" * 64,
        "e" * 64,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "unknown"},
        {"before_generation": 1},
        {"after_generation": 3},
        {"command_count": 0},
        {"output_stop": 100001},
        {"output_start": 2},
        {"before_sources": ()},
        {"before_sources": (["a", 0, False],)},
        {"before_sources": (("a", 0, False), ("a", 0, False))},
        {"after_sources": (("a", 2, False), ("b", 0, False))},
        {"after_sources": (("unknown", 1, False), ("b", 0, False))},
        {"status": "no_op"},
        {
            "before_generation": 1,
            "after_generation": 2,
            "before_operation": 2,
            "after_operation": 3,
            "before_sources": (("a", 1, True), ("b", 0, False)),
            "after_sources": (("a", 2, True), ("b", 0, False)),
        },
    ],
)
def test_receipt_rejects_impossible_shape_and_counter_histories(changes):
    with pytest.raises(ValidationError):
        replace(receipt(), **changes)


def test_all_command_kinds_roundtrip_and_strict_json_boundaries(monkeypatch):
    pending = MultiGraphRequest("a" * 32, "b" * 32, 0, (GraphEOF("a", 0), GraphDrain(1)))
    assert MultiGraphRequest.from_json(pending.to_json()) == pending
    for payload in (None, b"\xff", '{"x":1,"x":2}'):
        with pytest.raises(ValidationError):
            MultiGraphRequest.from_json(payload)
    monkeypatch.setattr(wire, "_REQUEST_BYTES", 4)
    for payload in ("12345", "界界"):
        with pytest.raises(ValidationError):
            MultiGraphRequest.from_json(payload)


@pytest.mark.parametrize("encoded", [[], "1e0"])
def test_forged_direct_record_cannot_bypass_request_admission(encoded):
    record = object.__new__(FlowRecord)
    object.__setattr__(record, "key", "k")
    object.__setattr__(record, "_json", encoded)
    with pytest.raises(ValidationError):
        MultiGraphRequest("a" * 32, "b" * 32, 0, (GraphInput("a", 0, record),))


@pytest.mark.parametrize(
    "changes",
    [
        {"checkpoint": None},
        {"source_commitments": []},
        {"source_commitments": ((),)},
        {"source_commitments": (("b", "b" * 64), ("a", "a" * 64))},
        {"generation": 1},
    ],
)
def test_recovery_point_requires_exact_checkpoint_and_ordered_commitments(changes):
    point = MultiGraphRecoveryPoint(
        "a" * 32, (("a", "a" * 64), ("b", "b" * 64)), 0, MultiGraphRuntime(graph()).checkpoint()
    )
    with pytest.raises(ValidationError):
        replace(point, **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"cause": None},
        {"join_id": "j"},
        {"cause": "eof"},
        {"cause": "drain", "join_id": "j", "max_keys": 1},
    ],
)
def test_operation_causes_reject_cross_kind_fields(changes):
    op = MultiGraphOperation(1, 1, 0, "process", "a", 0, None, None, "d" * 64, 0, 1)
    with pytest.raises(ValidationError):
        replace(op, **changes)


def test_page_and_cursor_direct_contracts(monkeypatch):
    cursor = MultiGraphOutputCursor("a" * 32, 1, "b" * 64, 1, 1)
    output = MultiGraphJournalOutput(0, 1, "sink", FlowRecord(1))
    for args in (((), None), ([], cursor), ((replace(output, sequence=1),), cursor)):
        with pytest.raises(ValidationError):
            MultiGraphOutputPage(*args)
    with pytest.raises(ValidationError):
        replace(cursor, anchor_generation=0)
    monkeypatch.setattr(wire, "_BATCH_BYTES", 1)
    with pytest.raises(ValidationError):
        MultiGraphOutputPage((output,), cursor)


def test_offline_durable_example(capsys):
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "durable_multi_source_orders.py"),
        run_name="__main__",
    )
    result = json.loads(capsys.readouterr().out)
    assert result["generation"] == 3 and result["callback_calls"] == 3
    assert result["old_cursor_stop"] == 1 and len(result["outputs"]) == 2
