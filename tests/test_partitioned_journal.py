"""Independent durable publication checks; transport stubs are not parallel evidence."""

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace

import pytest

from stream_quilt import (
    Dataflow,
    FlowRecord,
    FlowStep,
    OutputError,
    RecoveryConflict,
    ValidationError,
)
from stream_quilt._local_worker import _execute, _Pool
from stream_quilt.partitioned_checkpoint import _load
from stream_quilt.partitioned_journal import PartitionedFlowJournal
from stream_quilt.partitioned_journal_types import PartitionedFlowRequest, PartitionedOutputCursor


@pytest.fixture
def transport(monkeypatch):
    flow = Dataflow("durable-workers", "1", (FlowStep("text", "map", str),))
    monkeypatch.setattr(_Pool, "start", lambda *a: None)

    def execute(self, requests, quotas, deadline):
        return {
            index: _execute(
                flow, _load(raw, self.limits.max_message_bytes), index, 2, self.limits, self.session
            )
            for index, raw in requests.items()
        }

    monkeypatch.setattr(_Pool, "execute", execute)
    return flow


def test_durable_wave_receipt_and_eof_do_not_use_inmemory_publication(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "workers.db", transport, "source", "a" * 64)
    initial = journal.latest()
    request = PartitionedFlowRequest(
        initial.journal_id, "1" * 32, 0, "wave", 0, (FlowRecord(7, "a"), FlowRecord(9, "b"))
    )
    with journal.session() as session:
        receipt = session.apply(request)
        assert receipt.status == "committed" and receipt.output_stop == 2
        assert session.apply(request) == receipt
        # The private runtime is a candidate executor, never the durable state authority.
        assert session._runtime.checkpoint() == initial.checkpoint
        assert journal.latest().checkpoint.next_position == 2
        eof = PartitionedFlowRequest(initial.journal_id, "2" * 32, 1, "eof", 2)
        assert session.apply(eof).after_generation == 2
        assert journal.latest().checkpoint.source_closed
    page = journal.read_outputs(journal.output_cursor())
    assert [item.output.record.value for item in page.outputs] == ["7", "9"]
    assert page.cursor.next_sequence == page.cursor.stop_sequence == 2


def request(journal, number, *values, cause="wave"):
    point = journal.latest()
    return PartitionedFlowRequest(
        point.journal_id,
        f"{number:032x}",
        point.generation,
        cause,
        point.checkpoint.next_position,
        tuple(FlowRecord(value, "a") for value in values),
    )


def test_request_roundtrip_idempotency_conflicts_and_noop_eof(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, {"é": [1, None, False]}, '"x"')
    assert PartitionedFlowRequest.from_json(item.to_json()) == item
    with journal.session() as session:
        saved = session.apply(item)
        for changed in (
            replace(item, expected_generation=1),
            replace(item, records=(FlowRecord(2, "a"),)),
        ):
            with pytest.raises(RecoveryConflict):
                session.apply(changed)
        session.apply(request(journal, 2, cause="eof"))
        noop = request(journal, 3, cause="eof")
        before = journal.path.read_bytes()
        result = session.apply(noop)
        assert result.status == "no_op" and result.after_generation == 2
        assert journal.request(noop.request_id) is None
        assert journal.path.read_bytes() == before
        assert session.apply(item) == saved  # Receipt retained after later commits and EOF.
        with pytest.raises(RecoveryConflict):
            session.apply(replace(noop, request_id=item.request_id))
        assert journal.request(item.request_id) == saved


def test_fixed_prefix_cursor_and_exact_page_boundaries(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2, 3))
        cursor = journal.output_cursor()
        encoded = cursor.to_json()
        session.apply(request(journal, 2, 4, 5))
        first = journal.read_outputs(PartitionedOutputCursor.from_json(encoded), limit=1)
        rest = journal.read_outputs(first.cursor)
        assert [i.output.source_position for i in first.outputs + rest.outputs] == [0, 1, 2]
        assert not journal.read_outputs(rest.cursor).outputs
        assert journal.output_cursor().stop_sequence == 5
        size = len(
            json.dumps(
                first.outputs[0].to_dict(),
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        )
        assert len(journal.read_outputs(cursor, max_bytes=size).outputs) == 1
        with pytest.raises(ValidationError, match="page admission"):
            journal.read_outputs(cursor, max_bytes=size - 1)
        for changed in (
            replace(cursor, anchor_receipt_digest="b" * 64),
            replace(cursor, stop_sequence=4),
            replace(cursor, journal_id="c" * 32),
        ):
            with pytest.raises(ValidationError):
                journal.read_outputs(changed)


@pytest.mark.parametrize("same_id", [False, True])
def test_actual_sqlite_writer_cas_and_same_id_race(tmp_path, transport, monkeypatch, same_id):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    first = request(journal, 1, 4)
    second = first if same_id else replace(first, request_id="2" * 32)
    barrier = threading.Barrier(2)
    original = _Pool.execute
    calls = []

    def synchronized(self, *args):
        result = original(self, *args)
        calls.append(self.session)
        barrier.wait(timeout=15)
        return result

    monkeypatch.setattr(_Pool, "execute", synchronized)

    def writer(item):
        opened = PartitionedFlowJournal(journal.path, transport, "s", "a" * 64, create=False)
        with opened.session() as session:
            try:
                return session.apply(item)
            except RecoveryConflict:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(writer, item) for item in (first, second)]
        results = [future.result(timeout=30) for future in futures]
    assert len(calls) == 2
    assert journal.latest().generation == 1
    assert len(journal.read_outputs(journal.output_cursor()).outputs) == 1
    if same_id:
        assert results[0] == results[1] == journal.request(first.request_id)
    else:
        assert results.count("conflict") == 1


class ConnectionProxy:
    def __init__(self, connection, *, after=None, commit=None, close=None):
        self.connection, self.after, self.commit_hook, self.close_hook = (
            connection,
            after,
            commit,
            close,
        )

    def execute(self, sql, args=()):
        result = self.connection.execute(sql, args)
        if self.after:
            self.after(sql)
        return result

    def executemany(self, sql, args):
        result = self.connection.executemany(sql, args)
        if self.after:
            self.after(sql)
        return result

    def commit(self):
        changed = self.connection.total_changes
        self.connection.commit()
        if changed and self.commit_hook:
            self.commit_hook()

    def rollback(self):
        return self.connection.rollback()

    def close(self):
        self.connection.close()
        if self.close_hook:
            self.close_hook()


def install_proxy(monkeypatch, journal, **hooks):
    original = journal._connect
    monkeypatch.setattr(
        journal, "_connect", lambda **kwargs: ConnectionProxy(original(**kwargs), **hooks)
    )


@pytest.mark.parametrize(
    "after",
    ["INSERT INTO partition_commit", "INSERT INTO partition_output", "UPDATE partition_head"],
)
@pytest.mark.parametrize("control", [False, True])
def test_fault_at_each_write_rolls_back_every_table_and_control_settles_pool(
    tmp_path, transport, monkeypatch, after, control
):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    before = journal.latest()
    item = request(journal, 1, 8)
    failure = KeyboardInterrupt("control") if control else sqlite3.OperationalError("write failed")

    def fail(sql):
        if sql.startswith(after):
            raise failure

    with journal.session() as session:
        install_proxy(monkeypatch, journal, after=fail)
        with pytest.raises(KeyboardInterrupt if control else OutputError) as caught:
            session.apply(item)
        if control:
            assert caught.value is failure
            assert session._runtime.phase == "failed"
        assert journal.latest() == before and journal.request(item.request_id) is None


@pytest.mark.parametrize("control", [False, True])
def test_real_commit_then_raise_preserves_receipt_and_never_auto_reexecutes(
    tmp_path, transport, monkeypatch, control
):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, 6)
    failure = KeyboardInterrupt("ack lost") if control else OSError("ack lost")

    def fail():
        raise failure

    with journal.session() as session:
        original = journal._connect
        install_proxy(monkeypatch, journal, commit=fail)
        with pytest.raises(KeyboardInterrupt if control else OutputError):
            session.apply(item)
        monkeypatch.setattr(journal, "_connect", original)
        saved = journal.request(item.request_id)
        assert saved.status == "committed" and journal.latest().generation == 1
    monkeypatch.setattr(_Pool, "execute", lambda *a: pytest.fail("re-executed committed callbacks"))
    with journal.session() as next_session:
        assert next_session.apply(item) == saved


@pytest.mark.parametrize("limit", ["_STORE_BYTES", "_DATABASE_BYTES", "_FILE_BYTES"])
def test_global_storage_limits_are_not_per_row_allowances(tmp_path, transport, monkeypatch, limit):
    import stream_quilt.partitioned_journal as module

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    before = journal.latest()
    item = request(journal, 1, "x" * 2000)
    with journal.session() as session:
        with journal._transaction() as connection:
            current = journal._storage(connection)
        value = current + 100 if limit == "_STORE_BYTES" else 1
        monkeypatch.setattr(module, limit, value)
        with pytest.raises(ValidationError):
            session.apply(item)
        monkeypatch.undo()
        assert journal.latest() == before and journal.request(item.request_id) is None


@pytest.mark.parametrize("mode", ["wal", "truncate", "persist"])
def test_existing_database_mode_is_rejected_without_migration(tmp_path, transport, mode):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    # WAL persists in the file; the other modes are connection-local in SQLite.
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute(f"PRAGMA journal_mode={mode}")
        observed = connection.execute("PRAGMA journal_mode").fetchone()[0]
        assert observed == mode
    if mode == "wal":
        with pytest.raises(ValidationError, match="schema/mode"):
            journal.latest()
        with closing(sqlite3.connect(journal.path)) as connection, connection:
            assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)


def test_schema_identity_and_sql_materialization_guards(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with pytest.raises(ValidationError):
        PartitionedFlowJournal(journal.path, transport, "s", "b" * 64)
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute("UPDATE partition_head SET payload=?", ("x" * (65 * 1024 * 1024 + 1),))
    with pytest.raises(ValidationError, match="corrupted"):
        journal.latest()


def rewrite(journal, table, column, index, mutate):
    import hashlib

    with closing(sqlite3.connect(journal.path)) as connection, connection:
        payload = connection.execute(
            f"SELECT payload FROM {table} WHERE {column}=?", (index,)
        ).fetchone()[0]
        document = json.loads(payload)
        mutate(document)
        payload = json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        connection.execute(
            f"UPDATE {table} SET payload=?,digest=? WHERE {column}=?",
            (payload, hashlib.sha256(payload.encode()).hexdigest(), index),
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(output_index=3),
        lambda d: d.update(sequence=8),
        lambda d: d.update(source_position=100),
        lambda d: d.update(generation=3),
        lambda d: d.update(record={"key": None, "value": 1}),
        lambda d: d.update(extra=True),
    ],
)
def test_rehashed_output_corruption_is_not_a_valid_wave_member(tmp_path, transport, mutation):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2))
    cursor = journal.output_cursor()
    rewrite(journal, "partition_output", "seq", 0, mutation)
    with pytest.raises(ValidationError):
        journal.read_outputs(cursor)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(request_id="f" * 32),
        lambda d: d.update(status="no_op"),
        lambda d: d.update(before_head_digest="b" * 64),
        lambda d: d.update(after_head_digest="b" * 64),
        lambda d: d.update(journal_id="f" * 32),
        lambda d: d.update(after=[3, 1, False, 1]),
    ],
)
def test_rehashed_receipt_corruption_fails_current_prefix_checks(tmp_path, transport, mutation):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1))
    rewrite(journal, "partition_commit", "generation", 1, mutation)
    with pytest.raises(ValidationError):
        journal.latest()


def test_cross_page_predecessor_checks_ordinal_order(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2, 3))
    cursor = journal.output_cursor(start=1)
    rewrite(journal, "partition_output", "seq", 1, lambda d: d.update(output_index=2))
    with pytest.raises(ValidationError, match="ordinal"):
        journal.read_outputs(cursor)


def test_expanded_outputs_from_one_input_cannot_change_keys_across_pages(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2))
    cursor = journal.output_cursor(start=1)

    def change(document):
        document.update(source_position=0, output_index=1)
        document["record"]["key"] = "different-key"

    rewrite(journal, "partition_output", "seq", 1, change)
    with pytest.raises(ValidationError, match="ordinal"):
        journal.read_outputs(cursor)


def test_page_metadata_has_separate_prospective_admission(tmp_path, transport, monkeypatch):
    import stream_quilt.partitioned_journal as module

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        for i in range(3):
            session.apply(request(journal, i, i))
    cursor = journal.output_cursor()
    monkeypatch.setattr(module, "_PAGE_METADATA_BYTES", module._RECEIPT_BYTES * 3)
    first = journal.read_outputs(cursor)
    assert len(first.outputs) == 1
    # A predecessor plus a new generation requires two reservations.
    with pytest.raises(ValidationError, match="admission"):
        journal.read_outputs(first.cursor)


def test_short_sql_page_cannot_return_nonprogress_even_if_global_counts_pass(
    tmp_path, transport, monkeypatch
):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2))
    cursor = journal.output_cursor()
    original = journal._connect

    class Missing(ConnectionProxy):
        def execute(self, sql, args=()):
            if "WHERE seq>=?" in sql:
                return iter(())
            return super().execute(sql, args)

    monkeypatch.setattr(journal, "_connect", lambda **kw: Missing(original(**kw)))
    with pytest.raises(ValidationError, match="incomplete prefix"):
        journal.read_outputs(cursor)


@pytest.mark.parametrize("primary", [RuntimeError("primary"), KeyboardInterrupt("first control")])
def test_connection_cleanup_preserves_first_control_and_later_control_outranks_ordinary(
    tmp_path, transport, monkeypatch, primary
):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    later = SystemExit("cleanup control")

    def cleanup():
        raise later

    with journal.session() as session:
        item = request(journal, 1, 4)

        def fail(sql):
            if sql == "BEGIN":
                raise primary

        install_proxy(monkeypatch, journal, after=fail, close=cleanup)
        expected = primary if isinstance(primary, KeyboardInterrupt) else later
        with pytest.raises(type(expected)) as caught:
            session.apply(item)
        assert caught.value is expected and session._runtime.phase == "failed"


def test_cancellation_before_work_cleans_up_without_running_callbacks(
    tmp_path, transport, monkeypatch
):
    from stream_quilt import LocalWorkerError

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, 4)
    monkeypatch.setattr(_Pool, "execute", lambda *a: pytest.fail("ran cancelled callback"))
    with journal.session() as session:
        session.cancel()
        with pytest.raises(LocalWorkerError, match="cancelled"):
            session.apply(item)
        assert session._runtime.phase == "failed"
        assert journal.latest().generation == 0


def test_late_sql_writes_are_rolled_back_before_commit(tmp_path, transport, monkeypatch):
    import stream_quilt.partitioned_journal as module
    from stream_quilt import LocalWorkerError

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, 4)
    clock = [10.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])

    def delay(sql):
        if sql.startswith("UPDATE partition_head"):
            clock[0] += 100

    with journal.session() as session:
        install_proxy(monkeypatch, journal, after=delay)
        with pytest.raises(LocalWorkerError, match="deadline"):
            session.apply(item)
        assert journal.latest().generation == 0 and journal.request(item.request_id) is None


@pytest.mark.parametrize("kind", ["worker", "control", "cleanup_control"])
def test_candidate_failure_has_one_automatic_cleanup_owner(tmp_path, transport, monkeypatch, kind):
    from stream_quilt import LocalWorkerError

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, 4)
    failure = (
        LocalWorkerError("callback")
        if kind == "worker"
        else KeyboardInterrupt("primary")
        if kind == "control"
        else RuntimeError("callback")
    )
    cleanup_control = SystemExit("cleanup")
    calls = []
    original = _Pool.cleanup

    def execute(*args):
        raise failure

    def cleanup(self, primary=None):
        calls.append(primary)
        if kind == "cleanup_control" and len(calls) == 1:
            raise cleanup_control
        return original(self, primary)

    monkeypatch.setattr(_Pool, "execute", execute)
    monkeypatch.setattr(_Pool, "cleanup", cleanup)
    with journal.session() as session:
        expected = cleanup_control if kind == "cleanup_control" else failure
        with pytest.raises(type(expected)) as caught:
            session.apply(item)
        assert caught.value is expected
        assert calls == [
            failure
        ]  # Explicit later close may retry, apply must not double its budget.
        assert session._runtime.phase == "failed"
    assert len(calls) == 2


def test_interrupt_during_canonical_request_digest_settles_session(
    tmp_path, transport, monkeypatch
):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, {"x": "é" * 2000})
    interrupted = KeyboardInterrupt("interrupted JSON hashing")
    calls = []
    original = _Pool.cleanup

    def cleanup(self, primary=None):
        calls.append(primary)
        return original(self, primary)

    def digest(self):
        # Simulates SIGINT interrupting trusted JSON/hash work, not an input-defined callback.
        raise interrupted

    with journal.session() as session:
        monkeypatch.setattr(_Pool, "cleanup", cleanup)
        monkeypatch.setattr(PartitionedFlowRequest, "digest", property(digest))
        with pytest.raises(KeyboardInterrupt) as caught:
            session.apply(item)
        assert caught.value is interrupted and calls == [interrupted]


@pytest.mark.parametrize("change", [{"limits": {}}, {"create": 1}, {"workers": True}])
def test_invalid_configuration_never_creates_database(tmp_path, transport, change):
    path = tmp_path / "absent" / "j.db"
    with pytest.raises(ValidationError):
        PartitionedFlowJournal(path, transport, "s", "a" * 64, **change)
    assert not path.exists()


def test_missing_open_and_deleted_file_reads_do_not_create_a_replacement(tmp_path, transport):
    path = tmp_path / "j.db"
    with pytest.raises(ValidationError, match="does not exist"):
        PartitionedFlowJournal(path, transport, "s", "a" * 64, create=False)
    journal = PartitionedFlowJournal(path, transport, "s", "a" * 64)
    cursor = journal.output_cursor()
    path.unlink()
    for operation in (
        journal.latest,
        lambda: journal.request("1" * 32),
        journal.output_cursor,
        lambda: journal.read_outputs(cursor),
    ):
        with pytest.raises(OutputError):
            operation()
        assert not path.exists()


def test_empty_source_eof_and_explicit_session_close_are_distinct(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    session = journal.session()
    session.close()
    assert not journal.latest().checkpoint.source_closed
    with journal.session() as session:
        eof = session.apply(request(journal, 1, cause="eof"))
        assert eof.after_generation == 1 and eof.output_stop == 0
        assert session.apply(request(journal, 2, cause="eof")).status == "no_op"
    assert journal.latest().checkpoint.next_position == 0
    assert journal.read_outputs(journal.output_cursor()).outputs == ()


def test_missing_head_row_is_corruption_even_with_one_sql_row(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute("UPDATE partition_head SET slot=2")
    with pytest.raises(ValidationError, match="missing"):
        journal.latest()


@pytest.mark.parametrize(
    "table,index", [("partition_commit", "generation"), ("partition_output", "seq")]
)
def test_sql_wire_guard_precedes_python_materialization(
    tmp_path, transport, monkeypatch, table, index
):
    import stream_quilt.partitioned_journal as module

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1))
    cursor = journal.output_cursor()
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute(
            f"UPDATE {table} SET payload=? WHERE {index}=?",
            ("x" * 10_000, int(index == "generation")),
        )
    original = journal._connect

    def connect(**kwargs):
        connection = original(**kwargs)

        def text(raw):
            assert len(raw) < 10_000, "oversized SQLite text was materialized into Python"
            return raw.decode("utf-8")

        connection.text_factory = text
        return connection

    monkeypatch.setattr(journal, "_connect", connect)
    monkeypatch.setattr(
        module, "_RECEIPT_BYTES" if table == "partition_commit" else "_OUTPUT_BYTES", 2048
    )
    with pytest.raises(ValidationError, match="corrupted"):
        journal.read_outputs(cursor)


@pytest.mark.parametrize(
    "sql",
    [
        "PRAGMA application_id=1397837897",
        "PRAGMA user_version=2",
        "CREATE TABLE unexpected (x)",
        "ALTER TABLE partition_output ADD COLUMN unexpected INTEGER",
        "CREATE TRIGGER unexpected AFTER INSERT ON partition_commit BEGIN SELECT 1; END",
    ],
)
def test_foreign_or_changed_schema_is_rejected_without_writing(tmp_path, transport, sql):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute(sql)
    before = journal.path.read_bytes()
    with pytest.raises(ValidationError):
        PartitionedFlowJournal(journal.path, transport, "s", "a" * 64)
    assert journal.path.read_bytes() == before


def test_path_binding_survives_working_directory_change(tmp_path, transport, monkeypatch):
    monkeypatch.chdir(tmp_path)
    journal = PartitionedFlowJournal("j.db", transport, "s", "a" * 64)
    point = journal.latest()
    nested = tmp_path / "elsewhere"
    nested.mkdir()
    monkeypatch.chdir(nested)
    assert journal.path == tmp_path / "j.db" and journal.latest() == point
    assert not (nested / "j.db").exists()


@pytest.mark.parametrize(
    "change,error",
    [
        ({"journal_id": "f" * 32}, ValidationError),
        ({"expected_generation": 1}, RecoveryConflict),
        ({"start_position": 1}, ValidationError),
    ],
)
def test_stale_or_wrong_request_is_rejected_before_callbacks(
    tmp_path, transport, monkeypatch, change, error
):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = replace(request(journal, 1, 2), **change)
    monkeypatch.setattr(_Pool, "execute", lambda *a: pytest.fail("executed inadmissible input"))
    with journal.session() as session:
        with pytest.raises(error):
            session.apply(item)
        with pytest.raises(ValidationError):
            session.apply(item.to_dict())
    assert journal.latest().generation == 0


def test_sql_page_cap_rejects_actual_growth_and_session_can_retry(tmp_path, transport, monkeypatch):
    import stream_quilt.partitioned_journal as module

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    before = journal.latest()
    item = request(journal, 1, "x" * 40_000)
    with journal.session() as session:
        with monkeypatch.context() as patch:
            patch.setattr(module, "_DATABASE_BYTES", journal.path.stat().st_size)
            with pytest.raises(OutputError):
                session.apply(item)
        assert journal.latest() == before and journal.request(item.request_id) is None
        assert session.apply(item).after_generation == 1
    assert len(journal.read_outputs(journal.output_cursor()).outputs) == 1


def test_staged_output_budget_rejects_without_sql_and_reuses_pool(tmp_path, transport, monkeypatch):
    import stream_quilt.partitioned_journal as module

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, "é" * 100, "é" * 100)
    with journal.session() as session:
        with monkeypatch.context() as patch:
            patch.setattr(module, "_BATCH_BYTES", 100)
            with pytest.raises(ValidationError, match="durable wave output wire"):
                session.apply(item)
        assert journal.latest().generation == 0
        assert session.apply(item).after_generation == 1


def test_retained_row_and_wire_caps_admit_entire_store(tmp_path, transport, monkeypatch):
    import stream_quilt.partitioned_journal as module

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2))
    with monkeypatch.context() as patch:
        patch.setattr(module, "_STORAGE", tuple((a, b, 0) for a, b, _ in module._STORAGE))
        with pytest.raises(ValidationError, match="row count"):
            journal.latest()
    with monkeypatch.context() as patch:
        patch.setattr(module, "_STORE_BYTES", 1)
        with pytest.raises(ValidationError, match="text-wire"):
            journal.latest()
    assert journal.latest().generation == 1


@pytest.mark.parametrize("table", ["partition_commit", "partition_output"])
def test_missing_retained_row_cannot_be_read_as_complete_prefix(tmp_path, transport, table):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1, 2))
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        connection.execute(f"DELETE FROM {table}")
    with pytest.raises(ValidationError, match="prefix contradicts head"):
        journal.latest()


def test_receipts_are_checked_against_adjacent_head_boundaries(tmp_path, transport):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1))
        session.apply(request(journal, 2, 2))
    rewrite(
        journal, "partition_commit", "generation", 1, lambda d: d.update(after_head_digest="f" * 64)
    )
    with pytest.raises(ValidationError, match="adjacent"):
        journal.latest()


@pytest.mark.parametrize("operation", ["request", "cursor"])
def test_historical_receipt_must_still_bind_its_successor(tmp_path, transport, operation):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    with journal.session() as session:
        session.apply(request(journal, 1, 1))
        cursor = journal.output_cursor()
        session.apply(request(journal, 2, 2))
        session.apply(request(journal, 3, 3))
    rewrite(
        journal,
        "partition_commit",
        "generation",
        1,
        lambda d: d.update(after_head_digest="f" * 64),
    )
    # Supply the changed digest so this specifically tests historical linkage,
    # not merely the separate fixed-cursor checksum check.
    with closing(sqlite3.connect(journal.path)) as connection, connection:
        digest = connection.execute(
            "SELECT digest FROM partition_commit WHERE generation=1"
        ).fetchone()[0]
    cursor = replace(cursor, anchor_receipt_digest=digest)
    with pytest.raises(ValidationError, match="adjacent"):
        if operation == "request":
            journal.request(f"{1:032x}")
        else:
            journal.read_outputs(cursor)


def test_cancellation_waits_for_admitted_commit_then_prevents_next_wave(
    tmp_path, transport, monkeypatch
):
    from stream_quilt import LocalWorkerError

    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, 4)
    entered, completed = threading.Event(), threading.Event()
    with journal.session() as session:

        def cancel():
            entered.set()
            session.cancel()
            completed.set()

        thread = threading.Thread(target=cancel)

        def during_sql(sql):
            if sql.startswith("UPDATE partition_head"):
                thread.start()
                assert entered.wait(5)
                assert not completed.wait(0.05)

        install_proxy(monkeypatch, journal, after=during_sql)
        try:
            receipt = session.apply(item)
        finally:
            thread.join(5)
        assert completed.is_set() and journal.request(item.request_id) == receipt
        with pytest.raises(LocalWorkerError, match="cancelled"):
            session.apply(request(journal, 2, 5))
    assert journal.latest().generation == 1


def test_ordinary_sql_failure_never_installs_candidate_state(tmp_path, transport, monkeypatch):
    journal = PartitionedFlowJournal(tmp_path / "j.db", transport, "s", "a" * 64)
    item = request(journal, 1, 4)
    with journal.session() as session:
        original = journal._connect

        def fail(sql):
            if sql.startswith("UPDATE partition_head"):
                raise sqlite3.OperationalError("write failure")

        install_proxy(monkeypatch, journal, after=fail)
        with pytest.raises(OutputError):
            session.apply(item)
        monkeypatch.setattr(journal, "_connect", original)
        assert session._runtime.phase == "ready"
        assert session.apply(item).after_generation == 1
    assert [
        row.output.record.value for row in journal.read_outputs(journal.output_cursor()).outputs
    ] == ["4"]
