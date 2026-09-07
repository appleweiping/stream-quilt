from __future__ import annotations

import hashlib
import json
import runpy
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

import stream_quilt.flow_journal as module
from stream_quilt import (
    Dataflow,
    FlowExecutionError,
    FlowJournal,
    FlowLimits,
    FlowOutput,
    FlowRecord,
    FlowRecoveryPoint,
    FlowRuntime,
    FlowStep,
    OutputError,
    RecoveryConflict,
    StateUpdate,
    ValidationError,
)

SOURCE = hashlib.sha256(b"test-input-content-and-layout-v1").hexdigest()


def flow(*extra):
    return Dataflow(
        "words",
        "v1",
        (
            FlowStep(
                "sum",
                "stateful_map",
                lambda value, state: StateUpdate(state + value, state + value),
                lambda: 0,
            ),
            *extra,
        ),
    )


def test_atomic_prefix_reopen_and_output_pagination(tmp_path):
    path = tmp_path / "nested" / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    source = iter([FlowRecord(2, "a"), FlowRecord(3, "b"), FlowRecord(4, "a")])
    first = journal.advance(source, expected_generation=0, max_inputs=2)
    assert (first.generation, first.next_position) == (1, 2)
    assert first.checkpoint.cells == (("sum", "a", "2"), ("sum", "b", "3"))
    reopened = FlowJournal(path, flow(), SOURCE, create=False)
    second = reopened.advance(source, expected_generation=1)
    assert second.next_position == 3
    assert second.checkpoint.cells == (("sum", "a", "6"), ("sum", "b", "3"))
    assert [item.to_dict() for item in reopened.outputs(start=1, limit=2)] == [
        {"sequence": 1, "source_position": 1, "record": {"key": "b", "value": 3}},
        {"sequence": 2, "source_position": 2, "record": {"key": "a", "value": 6}},
    ]
    assert list(reopened.outputs(start=100)) == []
    assert reopened.advance([], expected_generation=2) == second


def test_filtered_inputs_and_expansion_keep_original_source_offset(tmp_path):
    spec = Dataflow(
        "expand",
        "1",
        (
            FlowStep("filter", "filter", lambda value: value > 0),
            FlowStep("expand", "flat_map", lambda value: [value, -value]),
        ),
    )
    journal = FlowJournal(tmp_path / "run.sqlite", spec, SOURCE)
    point = journal.advance(map(FlowRecord, [0, 2, -1, 3]), expected_generation=0)
    assert (point.next_position, point.checkpoint.emitted_records) == (4, 4)
    assert [(item.source_position, item.record.value) for item in journal.outputs()] == [
        (1, 2),
        (1, -2),
        (3, 3),
        (3, -3),
    ]


def test_callback_failure_does_not_publish_any_batch_prefix(tmp_path):
    def fail(value):
        if value == 4:
            raise RuntimeError("third input failed")
        return value

    journal = FlowJournal(tmp_path / "run.sqlite", flow(FlowStep("failure", "map", fail)), SOURCE)
    before = journal.advance([FlowRecord(1, "a")], expected_generation=0)
    with pytest.raises(FlowExecutionError):
        journal.advance([FlowRecord(1, "a"), FlowRecord(2, "a")], expected_generation=1)
    assert journal.latest() == before
    assert [item.record.value for item in journal.outputs()] == [1]


def test_source_failure_rolls_back_and_cap_does_not_look_ahead(tmp_path):
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)

    def source():
        yield FlowRecord(1, "a")
        raise RuntimeError("source stopped")

    iterator = source()
    journal.advance(iterator, expected_generation=0, max_inputs=1)
    before = journal.latest()
    with pytest.raises(RuntimeError, match="source stopped"):
        journal.advance(iterator, expected_generation=1)
    assert journal.latest() == before


def test_cas_rejects_before_source_consumption_and_never_retries_callbacks(tmp_path):
    path = tmp_path / "run.sqlite"
    calls = []
    competitor = None

    def callback(value, state):
        calls.append(value)
        if value == 9:
            competitor.advance([FlowRecord(2, "b")], expected_generation=0)
        return StateUpdate(state + value, state + value)

    spec = Dataflow("race", "1", (FlowStep("sum", "stateful_map", callback, lambda: 0),))
    journal = FlowJournal(path, spec, SOURCE)
    competitor = FlowJournal(path, spec, SOURCE)
    with pytest.raises(RecoveryConflict, match="during processing"):
        journal.advance([FlowRecord(9, "a")], expected_generation=0)
    assert calls == [9, 2]
    assert journal.latest().checkpoint.cells == (("sum", "b", "2"),)
    consumed = []

    def source():
        consumed.append(True)
        yield FlowRecord(1, "a")

    with pytest.raises(RecoveryConflict, match="before processing"):
        journal.advance(source(), expected_generation=0)
    assert consumed == []


def test_empty_source_detects_raced_head(tmp_path):
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)

    def source():
        journal.advance([FlowRecord(1, "a")], expected_generation=0)
        return
        yield  # pragma: no cover

    with pytest.raises(RecoveryConflict, match="empty input"):
        journal.advance(source(), expected_generation=0)


def test_partial_insert_sql_failure_rolls_back_head_and_all_outputs(tmp_path, monkeypatch):
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)
    before = journal.latest()

    class FailingConnection(sqlite3.Connection):
        def executemany(self, sql, parameters):
            values = list(parameters)
            self.execute(sql, values[0])
            raise sqlite3.OperationalError("injected after first row")

    monkeypatch.setattr(
        journal,
        "_connect",
        lambda **kwargs: sqlite3.connect(journal.path, factory=FailingConnection),
    )
    with pytest.raises(OutputError, match="previous prefix retained"):
        journal.advance([FlowRecord(1, "a"), FlowRecord(1, "a")], expected_generation=0)
    assert journal.latest() == before
    assert list(journal.outputs()) == []


@pytest.mark.parametrize(
    "kind", ["no-file", "unrelated", "empty-no-create", "empty-version", "application", "version"]
)
def test_refuses_unrelated_or_missing_database(tmp_path, kind):
    path = tmp_path / "run.sqlite"
    if kind == "no-file":
        with pytest.raises(ValidationError):
            FlowJournal(path, flow(), SOURCE, create=False)
        assert not path.exists()
        return
    if kind in ("application", "version"):
        FlowJournal(path, flow(), SOURCE)
    with closing(sqlite3.connect(path)) as connection, connection:
        if kind == "unrelated":
            connection.execute("CREATE TABLE user_data (x)")
        elif kind in ("version", "empty-version"):
            connection.execute("PRAGMA user_version=99")
        elif kind == "application":
            connection.execute("PRAGMA application_id=0")
    with pytest.raises(ValidationError):
        FlowJournal(path, flow(), SOURCE, create=kind != "empty-no-create")


def test_source_flow_and_reopen_configuration_binding(tmp_path):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    with pytest.raises(ValidationError, match="identity"):
        FlowJournal(path, flow(), "b" * 64)
    with pytest.raises(ValidationError, match="identity"):
        FlowJournal(path, replace(flow(), revision="changed"), SOURCE)
    with pytest.raises(ValidationError, match="identity"):
        FlowJournal(path, replace(flow(), limits=FlowLimits(max_state_keys=1)), SOURCE)
    assert journal.latest().generation == 0


def test_removed_journal_is_not_recreated_on_read(tmp_path):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    path.unlink()
    with pytest.raises(OutputError):
        journal.latest()
    assert not path.exists()


def rewrite(path, table, mutation):
    with closing(sqlite3.connect(path)) as connection, connection:
        payload = connection.execute(f"SELECT payload FROM {table}").fetchone()[0]
        document = json.loads(payload)
        mutation(document)
        payload = module._encode(document)
        connection.execute(
            f"UPDATE {table} SET payload=?,digest=?", (payload, module._digest(payload))
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(extra=True),
        lambda value: value.update(schema_version="2.0"),
        lambda value: value.update(generation=True),
        lambda value: value.update(generation=0),
        lambda value: value.update(next_position=10),
        lambda value: value["checkpoint"].update(emitted_records=0),
        lambda value: value["checkpoint"].update(
            cells=[{"step": "absent", "key": "a", "value": 1}]
        ),
    ],
)
def test_rehashed_malformed_heads_are_not_trusted(tmp_path, mutation):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    journal.advance([FlowRecord(1, "a")], expected_generation=0)
    rewrite(path, "flow_head", mutation)
    with pytest.raises(ValidationError):
        journal.latest()


def test_generation_zero_cannot_seed_unprocessed_keyed_state(tmp_path):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    seeded = replace(journal.latest().checkpoint, cells=(("sum", "a", "100"),))
    # Generic checkpoints may intentionally seed state; journal generation zero
    # is narrower because it can only be created from an empty runtime.
    assert FlowRuntime.from_checkpoint(flow(), seeded).process(FlowRecord(1, "a"))[0].value == 101
    with pytest.raises(ValidationError, match="inconsistent"):
        FlowRecoveryPoint(SOURCE, 0, 0, seeded)
    rewrite(path, "flow_head", lambda value: value.update(checkpoint=seeded.to_dict()))
    with pytest.raises(ValidationError, match="inconsistent"):
        journal.latest()
    with pytest.raises(ValidationError, match="inconsistent"):
        journal.advance([FlowRecord(1, "a")], expected_generation=0)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(extra=True),
        lambda value: value.update(sequence=True),
        lambda value: value.update(sequence=1),
        lambda value: value.update(source_position=-1),
        lambda value: value.update(source_position=1),
        lambda value: value.update(record=[]),
        lambda value: value["record"].update(extra=True),
        lambda value: value["record"].update(key=" a "),
    ],
)
def test_rehashed_invalid_output_contracts_are_rejected(tmp_path, mutation):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    journal.advance([FlowRecord(1, "a")], expected_generation=0)
    rewrite(path, "flow_output", mutation)
    with pytest.raises(ValidationError):
        list(journal.outputs())


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM flow_head",
        "DELETE FROM flow_output",
        "UPDATE flow_output SET seq=4",
        "UPDATE flow_head SET digest='incorrect'",
        "PRAGMA application_id=0",
        "PRAGMA user_version=8",
    ],
)
def test_missing_corrupt_and_noncontiguous_prefixes_fail(tmp_path, sql):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    journal.advance([FlowRecord(1, "a")], expected_generation=0)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(sql)
    with pytest.raises(ValidationError):
        journal.latest()


def test_subprocess_restart_and_precommit_crash(tmp_path):
    path = tmp_path / "run.sqlite"
    script = """
import os, sys
from stream_quilt import Dataflow, FlowStep, StateUpdate, FlowRecord, FlowJournal
def add(value, state):
    if value == 99: os._exit(7)
    return StateUpdate(state + value, state + value)
flow = Dataflow("process", "1", (FlowStep("sum", "stateful_map", add, lambda: 0),))
journal = FlowJournal(sys.argv[1], flow, "a"*64)
before = journal.latest()
records = [FlowRecord(int(value), "a") for value in sys.argv[2:]]
journal.advance(records, expected_generation=before.generation)
print(journal.latest().next_position)
"""

    def run(*values):
        return subprocess.run(
            [sys.executable, "-c", script, str(path), *map(str, values)],
            capture_output=True,
            text=True,
            timeout=30,
        )

    assert run(2, 3).stdout.strip() == "2"
    assert run(4, 99).returncode == 7
    result = run(4)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "3"
    spec = Dataflow(
        "process",
        "1",
        (
            FlowStep(
                "sum",
                "stateful_map",
                lambda value, state: StateUpdate(state + value, state + value),
                lambda: 0,
            ),
        ),
    )
    journal = FlowJournal(path, spec, "a" * 64, create=False)
    assert [item.record.value for item in journal.outputs()] == [2, 5, 9]


def test_models_and_argument_validation(tmp_path):
    checkpoint = FlowRuntime(flow()).checkpoint()
    with pytest.raises(ValidationError):
        FlowRecoveryPoint("a", 0, 0, checkpoint)
    with pytest.raises(ValidationError):
        FlowRecoveryPoint(SOURCE, 0, 0, {})
    with pytest.raises(ValidationError):
        FlowOutput(0, 0, {})
    with pytest.raises(ValidationError):
        FlowOutput(1_000_000, 0, FlowRecord(0))
    with pytest.raises(ValidationError):
        FlowJournal(tmp_path / "a", flow(), SOURCE, create=1)
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)
    for value in (0, True, -1, 10_001):
        with pytest.raises(ValidationError):
            journal.advance([], expected_generation=0, max_inputs=value)
        with pytest.raises(ValidationError):
            list(journal.outputs(limit=value))
    with pytest.raises(ValidationError):
        journal.advance([], expected_generation=True)
    with pytest.raises(ValidationError):
        list(journal.outputs(start=-1))


@pytest.mark.parametrize("payload", ['{"x":1,"x":2}', "1e999", "NaN", "{", '{ "x":1}', '"\\ud800"'])
def test_strict_json_document_reader(payload):
    with pytest.raises(ValidationError):
        module._decode(payload, module._digest(payload), len(payload.encode()))


def test_output_pages_keep_snapshot_and_release_connections(tmp_path, monkeypatch):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    journal.advance([FlowRecord(1, "a")] * 3, expected_generation=0)
    page = journal.outputs()
    assert next(page).record.value == 1
    journal.advance([FlowRecord(1, "a")], expected_generation=1)
    assert [item.record.value for item in page] == [2, 3]
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
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


def test_row_payload_is_bounded_inside_sqlite_before_python_decode(tmp_path, monkeypatch):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    journal.advance([FlowRecord(1, "a")], expected_generation=0)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("UPDATE flow_output SET payload=?", ("x" * 4096,))
    monkeypatch.setattr(module, "_MAX_DOCUMENT_BYTES", 2048)
    connect = journal._connect

    def guarded(**kwargs):
        connection = connect(**kwargs)

        def text_factory(value):
            assert len(value) <= 2048, "oversized value was materialized"
            return value.decode()

        connection.text_factory = text_factory
        return connection

    monkeypatch.setattr(journal, "_connect", guarded)
    with pytest.raises(ValidationError, match="journal document"):
        list(journal.outputs())


@pytest.mark.parametrize("budget", ["outputs", "bytes", "stored"])
def test_capacity_exhaustion_never_publishes_partial_batch(tmp_path, monkeypatch, budget):
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)
    before = journal.latest()
    if budget == "outputs":
        monkeypatch.setattr(module, "_MAX_BATCH_OUTPUTS", 1)
    elif budget == "stored":
        monkeypatch.setattr(module, "_MAX_RECORDS", 1)
    else:
        monkeypatch.setattr(module, "_MAX_DOCUMENT_BYTES", 1024)
    inputs = [FlowRecord(1, "key" * 100)] * 3
    with pytest.raises(ValidationError):
        journal.advance(inputs, expected_generation=0)
    assert journal.latest() == before
    assert list(journal.outputs()) == []


def test_checkpoint_and_output_contract_capacity(tmp_path):
    checkpoint = FlowRuntime(flow()).checkpoint()
    with pytest.raises(ValidationError, match="capacity"):
        FlowRecoveryPoint(
            SOURCE, 1, 1, replace(checkpoint, processed_inputs=1, emitted_records=1_000_001)
        )
    with pytest.raises(ValidationError, match="inconsistent"):
        FlowRecoveryPoint(SOURCE, 2, 1, replace(checkpoint, processed_inputs=1))
    spec = replace(flow(), limits=FlowLimits(max_record_bytes=1))
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, spec, SOURCE)
    journal.advance([FlowRecord(1, "a")], expected_generation=0)
    rewrite(path, "flow_output", lambda value: value["record"].update(value=10))
    with pytest.raises(ValidationError, match="record contract"):
        list(journal.outputs())


def test_database_io_errors_are_normalized(tmp_path, monkeypatch):
    invalid_parent = tmp_path / "file"
    invalid_parent.write_bytes(b"not a directory")
    with pytest.raises(OutputError):
        FlowJournal(invalid_parent / "run.sqlite", flow(), SOURCE)
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)

    def fail(**kwargs):
        raise sqlite3.OperationalError("injected open failure")

    monkeypatch.setattr(journal, "_connect", fail)
    with pytest.raises(OutputError):
        journal.latest()
    with pytest.raises(OutputError):
        list(journal.outputs())


def test_process_death_inside_sql_transaction_restores_previous_prefix(tmp_path):
    path = tmp_path / "run.sqlite"
    journal = FlowJournal(path, flow(), SOURCE)
    journal.advance([FlowRecord(2, "a")], expected_generation=0)
    before = journal.latest()
    script = """
import os, sqlite3, sys
from stream_quilt import Dataflow, FlowStep, StateUpdate, FlowRecord, FlowJournal
flow = Dataflow("words", "v1", (
    FlowStep("sum", "stateful_map", lambda v, s: StateUpdate(s+v,s+v),lambda:0),
))
journal = FlowJournal(sys.argv[1], flow, sys.argv[2], create=False)
class Crash(sqlite3.Connection):
    def executemany(self, sql, rows):
        super().executemany(sql, rows)
        os._exit(8)
journal._connect = lambda **kw: sqlite3.connect(journal.path, factory=Crash)
journal.advance([FlowRecord(4,"a")],expected_generation=1)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), SOURCE],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 8, result.stderr
    assert journal.latest() == before
    assert [item.record.value for item in journal.outputs()] == [2]


def test_encoder_rejects_invalid_and_oversized_values(monkeypatch):
    for value in (object(), float("nan"), "\ud800"):
        with pytest.raises(ValidationError):
            module._encode(value)
    monkeypatch.setattr(module, "_MAX_DOCUMENT_BYTES", 8)
    with pytest.raises(ValidationError):
        module._encode("many characters")


def test_executable_durable_example(capsys):
    runpy.run_path(
        str(Path(__file__).parents[1] / "examples" / "durable_keyed_totals.py"), run_name="__main__"
    )
    result = json.loads(capsys.readouterr().out)
    assert result["next_position"] == 4
    assert [item["record"]["value"] for item in result["outputs"]] == [2, 3, 6, 8]


@pytest.mark.parametrize("error", [sqlite3.OperationalError, KeyboardInterrupt])
def test_failed_connection_setup_closes_before_return(tmp_path, monkeypatch, error):
    journal = FlowJournal(tmp_path / "run.sqlite", flow(), SOURCE)
    real_connect = sqlite3.connect
    connections = []

    class BrokenSetup(sqlite3.Connection):
        def execute(self, sql, *args):
            if sql == "PRAGMA synchronous=FULL":
                raise error("injected setup failure")
            return super().execute(sql, *args)

    def connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs, factory=BrokenSetup)
        connections.append(connection)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", connect)
    with pytest.raises(OutputError if error is sqlite3.OperationalError else error):
        journal.latest()
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")
