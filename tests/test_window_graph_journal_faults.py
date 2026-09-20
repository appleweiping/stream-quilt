"""Actual SQLite acknowledgement loss, partial publication and process death."""

import sqlite3
import subprocess
import sys

import pytest

from stream_quilt import (
    FlowEdge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    OutputError,
    WindowFold,
    WindowGraphDataflow,
    WindowGraphDrain,
    WindowGraphInput,
    WindowGraphJournal,
    WindowGraphRequest,
    WindowGraphWatermark,
)


def _flow():
    return WindowGraphDataflow(
        "fault-window",
        "v1",
        (
            FlowStep("input", "map", lambda value: value),
            FlowWindow(
                "window",
                WindowFold("window", "v1", width=10, initial=lambda: 0, fold=lambda a, b: a + b),
            ),
        ),
        (FlowEdge("input", "window"),),
        entry="input",
    )


def _pending(journal):
    return WindowGraphRequest(
        journal.journal_id,
        "1" * 32,
        0,
        (
            WindowGraphInput(0, 1, FlowRecord(4, "k")),
            WindowGraphWatermark(10, 1),
            WindowGraphDrain(1),
        ),
    )


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_real_commit_then_lost_ack_is_resolved_by_receipt(tmp_path, monkeypatch, failure):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    pending = _pending(journal)

    class LostAck(sqlite3.Connection):
        def commit(self):
            written = self.total_changes > 0
            super().commit()
            if written:
                raise failure("ack lost after SQLite COMMIT")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            journal, "_connect", lambda **kw: sqlite3.connect(journal.path, factory=LostAck)
        )
        with pytest.raises(OutputError if failure is RuntimeError else failure):
            journal.apply(pending)
    assert journal.latest().generation == 1
    receipt = journal.request(pending.request_id)
    assert receipt is not None
    assert receipt.output_stop == 1
    assert journal.apply(pending) == receipt
    assert len(journal.read_outputs(journal.output_cursor()).outputs) == 1


@pytest.mark.parametrize("stage", ["receipt", "operation", "output", "head", "commit"])
def test_failed_write_or_commit_retains_complete_old_prefix(tmp_path, monkeypatch, stage):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    previous = journal.latest()
    pending = _pending(journal)

    class Broken(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if (stage == "receipt" and sql.startswith("INSERT INTO window_commit")) or (
                stage == "head" and sql.startswith("UPDATE window_head")
            ):
                raise sqlite3.OperationalError("injected after write")
            return result

        def executemany(self, sql, parameters):
            result = super().executemany(sql, parameters)
            if (stage == "operation" and sql.startswith("INSERT INTO window_operation")) or (
                stage == "output" and sql.startswith("INSERT INTO window_output")
            ):
                raise sqlite3.OperationalError("injected after batch write")
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
    assert journal.latest() == previous
    assert journal.request(pending.request_id) is None
    with sqlite3.connect(journal.path) as connection:
        assert [
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("window_commit", "window_operation", "window_output")
        ] == [0, 0, 0]


@pytest.mark.parametrize("stage", ["receipt", "operation", "output", "head"])
def test_real_process_death_before_commit_rolls_back_four_tables(tmp_path, stage):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    previous = journal.latest()
    script = r"""
import os, sqlite3, sys
from stream_quilt import FlowEdge, FlowRecord, FlowStep, FlowWindow, WindowFold
from stream_quilt import WindowGraphDataflow, WindowGraphDrain, WindowGraphInput
from stream_quilt import WindowGraphJournal, WindowGraphRequest, WindowGraphWatermark
flow=WindowGraphDataflow(
    "fault-window","v1",
    (FlowStep("input","map",lambda x:x),
     FlowWindow("window",WindowFold("window","v1",width=10,initial=lambda:0,fold=lambda a,b:a+b))),
    (FlowEdge("input","window"),),entry="input")
j=WindowGraphJournal(sys.argv[1],flow,"0"*64,"a"*64,create=False)
point=j.latest()
class Crash(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        value=super().execute(sql,parameters)
        if ((sys.argv[2]=="receipt" and sql.startswith("INSERT INTO window_commit")) or
                (sys.argv[2]=="head" and sql.startswith("UPDATE window_head"))):
            os._exit(43)
        return value
    def executemany(self,sql,parameters):
        value=super().executemany(sql,parameters)
        if ((sys.argv[2]=="operation" and sql.startswith("INSERT INTO window_operation")) or
                (sys.argv[2]=="output" and sql.startswith("INSERT INTO window_output"))):
            os._exit(43)
        return value
j._connect=lambda **kw:sqlite3.connect(j.path,factory=Crash)
j.apply(WindowGraphRequest(point.journal_id,"1"*32,0,(
    WindowGraphInput(0,1,FlowRecord(4,"k")),WindowGraphWatermark(10,1),WindowGraphDrain(1))))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(journal.path), stage],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 43, result.stderr
    assert journal.latest() == previous
    with sqlite3.connect(journal.path) as connection:
        assert [
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("window_commit", "window_operation", "window_output")
        ] == [0, 0, 0]
