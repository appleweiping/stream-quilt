"""Real spawn/SQLite recovery, separate from in-process protocol fault stubs."""

import multiprocessing
import os
import random
import sqlite3
import time
from pathlib import Path

import pytest

from stream_quilt import (
    Dataflow,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    LocalWorkerError,
    StateFlatUpdate,
    StateUpdate,
)
from stream_quilt.partitioned_checkpoint import partition_for
from stream_quilt.partitioned_journal import PartitionedFlowJournal
from stream_quilt.partitioned_journal_types import PartitionedFlowRequest


def add(value, state):
    return StateUpdate(state + value, state + value)


def flow():
    return Dataflow("durable-totals", "1", (FlowStep("sum", "stateful_map", add, int),))


def barrier(value):
    folder = Path(value)
    (folder / str(os.getpid())).touch()
    until = time.monotonic() + 20
    while len(tuple(folder.iterdir())) < 2:
        if time.monotonic() >= until:
            raise RuntimeError("a serial dispatcher cannot complete the two-PID barrier")
        time.sleep(0.01)
    return os.getpid()


def test_real_two_pid_barrier_and_durable_output(tmp_path):
    markers = tmp_path / "barrier"
    markers.mkdir()
    graph = Dataflow("durable-barrier", "1", (FlowStep("wait", "map", barrier),))
    journal = PartitionedFlowJournal(tmp_path / "j.db", graph, "s", "a" * 64)
    keys = {}
    for value in range(100):
        key = str(value)
        keys.setdefault(partition_for(key, 2), key)
    assert set(keys) == {0, 1}
    point = journal.latest()
    item = PartitionedFlowRequest(
        point.journal_id,
        "1" * 32,
        0,
        "wave",
        0,
        tuple(FlowRecord(str(markers), key) for key in keys.values()),
    )
    with journal.session() as session:
        status = session.worker_status()
        session.apply(item)
        rows = journal.read_outputs(journal.output_cursor()).outputs
        pids = {row.output.record.value for row in rows}
        assert pids == {worker.pid for worker in status}
        assert len(pids) == 2 and os.getpid() not in pids
    assert not any(worker.alive for worker in session.worker_status())


def test_seeded_serial_oracle_across_real_process_sessions_and_sqlite_reopen(tmp_path):
    randomizer = random.Random(918)
    records = tuple(
        FlowRecord(randomizer.randint(-4, 12), randomizer.choice(("a", "b", "é")))
        for _ in range(30)
    )
    serial = FlowRuntime(flow())
    expected = [output.to_dict() for record in records for output in serial.process(record)]
    pids = []
    request_wires = []
    for restart in range(3):
        journal = PartitionedFlowJournal(
            tmp_path / "j.db", flow(), "s", "a" * 64, create=restart == 0
        )
        point = journal.latest()
        assert point.checkpoint.next_position == restart * 10
        with journal.session() as session:
            pids.append({worker.pid for worker in session.worker_status()})
            item = PartitionedFlowRequest(
                point.journal_id,
                f"{restart + 1:032x}",
                point.generation,
                "wave",
                point.checkpoint.next_position,
                records[restart * 10 : (restart + 1) * 10],
            )
            request_wires.append(item.to_json())
            session.apply(PartitionedFlowRequest.from_json(item.to_json()))
            if restart == 2:
                eof = PartitionedFlowRequest(point.journal_id, "f" * 32, 3, "eof", 30)
                session.apply(eof)
                for prior in request_wires:
                    assert (
                        session.apply(PartitionedFlowRequest.from_json(prior)).status == "committed"
                    )
        assert not any(worker.alive for worker in session.worker_status())
    assert all(len(group) == 2 for group in pids)
    point = journal.latest()
    assert point.generation == 4 and point.checkpoint.source_closed
    assert sum(shard.processed_inputs for shard in point.checkpoint.shards) == 30
    assert sorted(cell for shard in point.checkpoint.shards for cell in shard.cells) == sorted(
        serial.checkpoint().cells
    )
    cursor, actual = journal.output_cursor(), []
    while cursor.next_sequence < cursor.stop_sequence:
        page = journal.read_outputs(cursor, limit=7)
        actual.extend(item.output.record.to_dict() for item in page.outputs)
        cursor = page.cursor
    assert actual == expected


def crash_writer(path, stage):
    # This real parent-process death tests SQLite rollback, not parallelism.
    # A pure transport stub avoids leaving intentionally orphaned callback processes.
    from stream_quilt._local_worker import _execute, _Pool
    from stream_quilt.partitioned_checkpoint import _load

    definition = Dataflow("sql-death", "1", (FlowStep("str", "map", str),))
    _Pool.start = lambda *args: None

    def execute(self, requests, quotas, deadline):
        return {
            index: _execute(
                definition,
                _load(raw, self.limits.max_message_bytes),
                index,
                2,
                self.limits,
                self.session,
            )
            for index, raw in requests.items()
        }

    _Pool.execute = execute
    journal = PartitionedFlowJournal(path, definition, "s", "a" * 64, create=False)
    point = journal.latest()
    item = PartitionedFlowRequest(point.journal_id, "1" * 32, 0, "wave", 0, (FlowRecord(9, "a"),))

    class Crash(sqlite3.Connection):
        def execute(self, sql, *args):
            result = super().execute(sql, *args)
            if sql.startswith(stage):
                os._exit(23)
            return result

        def executemany(self, sql, *args):
            result = super().executemany(sql, *args)
            if sql.startswith(stage):
                os._exit(23)
            return result

        def commit(self):
            super().commit()
            if stage == "COMMIT" and self.total_changes:
                os._exit(23)

    with journal.session() as session:
        journal._connect = lambda **kwargs: sqlite3.connect(path, factory=Crash)
        session.apply(item)


@pytest.mark.parametrize(
    "stage",
    [
        "INSERT INTO partition_commit",
        "INSERT INTO partition_output",
        "UPDATE partition_head",
        "COMMIT",
    ],
)
def test_actual_process_death_at_sql_publication_boundaries(tmp_path, stage):
    definition = Dataflow("sql-death", "1", (FlowStep("str", "map", str),))
    journal = PartitionedFlowJournal(tmp_path / "j.db", definition, "s", "a" * 64)
    process = multiprocessing.get_context("spawn").Process(
        target=crash_writer, args=(str(journal.path), stage)
    )
    try:
        process.start()
        process.join(30)
        assert not process.is_alive() and process.exitcode == 23
    finally:
        if process.is_alive():
            process.terminate()
            process.join(10)
        process.close()
    point = journal.latest()
    committed = stage == "COMMIT"
    assert point.generation == int(committed)
    assert (journal.request("1" * 32) is not None) == committed
    rows = journal.read_outputs(journal.output_cursor()).outputs
    assert [item.output.record.value for item in rows] == (["9"] if committed else [])


def silent_or_pair(value, state):
    total = state + value
    return StateFlatUpdate(total, () if value < 0 else (state, total))


def test_hand_computed_silent_state_and_expansion_survive_worker_restart(tmp_path):
    definition = Dataflow(
        "silent-state", "1", (FlowStep("sum", "stateful_flat_map", silent_or_pair, int),)
    )
    # The middle wave has no output but changes both durable keyed cells.
    waves = (((3, "a"), (4, "b")), ((-2, "a"), (-3, "b")), ((6, "b"), (8, "a")))
    expected = [
        (0, 0, 0, "a", 0),
        (1, 0, 1, "a", 3),
        (2, 1, 0, "b", 0),
        (3, 1, 1, "b", 4),
        (4, 4, 0, "b", 1),
        (5, 4, 1, "b", 7),
        (6, 5, 0, "a", 1),
        (7, 5, 1, "a", 9),
    ]
    for index, wave in enumerate(waves):
        journal = PartitionedFlowJournal(
            tmp_path / "j.db", definition, "s", "a" * 64, workers=3, create=index == 0
        )
        point = journal.latest()
        item = PartitionedFlowRequest(
            point.journal_id,
            f"{index:032x}",
            index,
            "wave",
            index * 2,
            tuple(FlowRecord(value, key) for value, key in wave),
        )
        with journal.session() as session:
            saved = session.apply(item)
            if index == 1:
                assert saved.output_start == saved.output_stop == 4
                assert journal.output_cursor().anchor_generation == 2
        assert not any(worker.alive for worker in session.worker_status())
    actual, cursor = [], journal.output_cursor()
    while cursor.next_sequence < cursor.stop_sequence:
        page = journal.read_outputs(cursor, limit=3)
        actual.extend(
            (
                r.output.sequence,
                r.output.source_position,
                r.output.output_index,
                r.output.record.key,
                r.output.record.value,
            )
            for r in page.outputs
        )
        cursor = page.cursor
    assert actual == expected
    assert sorted(cell for shard in journal.latest().checkpoint.shards for cell in shard.cells) == [
        ("sum", "a", "9"),
        ("sum", "b", "7"),
    ]


def faulting_add(value, state):
    if value == "die":
        os._exit(27)
    if value == "control":
        raise KeyboardInterrupt("trusted callback control")
    if value == "raise":
        raise RuntimeError("trusted callback failure")
    return StateUpdate(state + value, state + value)


@pytest.mark.parametrize("fault", ["raise", "control", "die"])
def test_real_worker_failure_preserves_all_durable_shards_and_outputs(tmp_path, fault):
    definition = Dataflow("fault-state", "1", (FlowStep("sum", "stateful_map", faulting_add, int),))
    journal = PartitionedFlowJournal(tmp_path / "j.db", definition, "s", "a" * 64)
    keys = {}
    for value in range(100):
        key = str(value)
        keys.setdefault(partition_for(key, 2), key)
    key0, key1 = keys[0], keys[1]
    point = journal.latest()
    first = PartitionedFlowRequest(
        point.journal_id,
        "1" * 32,
        0,
        "wave",
        0,
        (FlowRecord(3, key0), FlowRecord(5, key1)),
    )
    with journal.session() as session:
        session.apply(first)
        before = journal.latest()
        failed = PartitionedFlowRequest(
            point.journal_id,
            "2" * 32,
            1,
            "wave",
            2,
            (FlowRecord(10, key0), FlowRecord(fault, key1)),
        )
        with pytest.raises(LocalWorkerError):
            session.apply(failed)
        assert journal.latest() == before and journal.request(failed.request_id) is None
        assert not any(worker.alive for worker in session.worker_status())
    reopened = PartitionedFlowJournal(journal.path, definition, "s", "a" * 64, create=False)
    replacement = PartitionedFlowRequest(
        point.journal_id,
        "3" * 32,
        1,
        "wave",
        2,
        (FlowRecord(1, key0), FlowRecord(2, key1)),
    )
    with reopened.session() as session:
        session.apply(replacement)
    outputs = reopened.read_outputs(reopened.output_cursor()).outputs
    assert [row.output.record.value for row in outputs] == [3, 5, 4, 7]
    assert reopened.latest().checkpoint.next_position == 4
