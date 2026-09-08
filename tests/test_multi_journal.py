"""Independent durable multi-source operations and recovery boundaries."""

import sqlite3
from contextlib import closing

import pytest

from stream_quilt import (
    FlowEntry,
    FlowJoin,
    FlowRecord,
    FlowStep,
    GraphInput,
    JoinEdge,
    KeyedJoin,
    MultiGraphDataflow,
    RecoveryConflict,
)
from stream_quilt.multi_journal import (
    GraphDrain,
    GraphEOF,
    MultiGraphJournal,
    MultiGraphRequest,
)


def graph(emit="final", callback=lambda x: x):
    return MultiGraphDataflow(
        "orders",
        "1",
        (
            FlowStep("left", "map", callback),
            FlowStep("right", "map", callback),
            FlowJoin("join", KeyedJoin("join", "1", ("l", "r"), "last", emit)),
        ),
        (JoinEdge("left", "join", "l"), JoinEdge("right", "join", "r")),
        (FlowEntry("a", "left"), FlowEntry("b", "right")),
    )


def request(journal, commands, number=1):
    point = journal.latest()
    return MultiGraphRequest(point.journal_id, f"{number:032x}", point.generation, commands)


def test_real_eof_drain_and_lost_ack_replay(tmp_path):
    calls = []
    flow = graph(callback=lambda value: calls.append(value) or value)
    path = tmp_path / "journal.sqlite"
    journal = MultiGraphJournal(path, flow, {"a": "a" * 64, "b": "b" * 64})
    first = request(journal, (GraphInput("a", 0, FlowRecord(3, "k")),))
    receipt = journal.apply(first)
    assert receipt.status == "committed"
    assert receipt.after_generation == 1
    assert journal.apply(MultiGraphRequest.from_json(first.to_json())) == receipt
    assert calls == [3]
    closing = request(journal, (GraphEOF("a", 1), GraphEOF("b", 0)), 2)
    assert journal.apply(closing).output_stop == 0
    drained = journal.apply(request(journal, (GraphDrain(1),), 3))
    assert drained.output_stop == 1
    reopened = MultiGraphJournal(path, flow, {"a": "a" * 64, "b": "b" * 64}, create=False)
    assert reopened.latest().checkpoint.sources == (("a", 1, True), ("b", 0, True))
    page = reopened.read_outputs(reopened.output_cursor())
    assert [item.record.value for item in page.outputs] == [
        {"present": [True, False], "values": [3, None]}
    ]
    assert page.outputs[0].operation_sequence == 4
    assert page.cursor.next_sequence == 1
    assert reopened.request(first.request_id) == receipt
    assert reopened.apply(first) == receipt
    assert calls == [3]


def test_noop_does_not_claim_id_or_advance_history(tmp_path):
    journal = MultiGraphJournal(tmp_path / "j", graph(), {"a": "a" * 64, "b": "b" * 64})
    before = journal.latest()
    noop = request(journal, (GraphDrain(),))
    result = journal.apply(noop)
    assert result.status == "no_op"
    assert journal.latest() == before
    assert journal.request(noop.request_id) is None
    with closing(sqlite3.connect(journal.path)) as db, db:
        assert db.execute("SELECT count(*) FROM multi_commit").fetchone() == (0,)
    journal.apply(request(journal, (GraphEOF("a", 0),), 2))
    with pytest.raises(RecoveryConflict):
        journal.apply(noop)


def test_later_callback_failure_does_not_publish_partial_batch(tmp_path):
    def callback(value):
        if value == 2:
            raise RuntimeError("failure")
        return value

    journal = MultiGraphJournal(
        tmp_path / "j", graph("running", callback), {"a": "a" * 64, "b": "b" * 64}
    )
    before = journal.latest()
    commands = tuple(GraphInput("a", i, FlowRecord(i + 1, "k")) for i in range(2))
    with pytest.raises(Exception, match="left"):
        journal.apply(request(journal, commands))
    assert journal.latest() == before


def test_id_binds_original_generation_and_full_commands(tmp_path):
    journal = MultiGraphJournal(tmp_path / "j", graph(), {"a": "a" * 64, "b": "b" * 64})
    first = request(journal, (GraphEOF("a", 0),))
    journal.apply(first)
    changed = MultiGraphRequest(first.journal_id, first.request_id, 0, (GraphEOF("b", 0),))
    with pytest.raises(RecoveryConflict):
        journal.apply(changed)


def test_cursor_stop_does_not_grow_after_append(tmp_path):
    journal = MultiGraphJournal(tmp_path / "j", graph("running"), {"a": "a" * 64, "b": "b" * 64})
    journal.apply(request(journal, (GraphInput("a", 0, FlowRecord(1, "k")),)))
    cursor = journal.output_cursor()
    journal.apply(request(journal, (GraphInput("a", 1, FlowRecord(2, "k")),), 2))
    assert len(journal.read_outputs(cursor).outputs) == 1
    assert len(journal.read_outputs(journal.output_cursor()).outputs) == 2
