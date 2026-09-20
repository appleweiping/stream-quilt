"""Persisted command boundaries against the independent raw-arrival interpreter."""

import uuid

import pytest
from test_window_graph_oracle import Interpreter, graph_for

from stream_quilt import (
    FlowExecutionError,
    FlowRecord,
    WindowGraphDrain,
    WindowGraphFinish,
    WindowGraphInput,
    WindowGraphJournal,
    WindowGraphRequest,
    WindowGraphWatermark,
)


@pytest.mark.parametrize("width,hop,origin", [(5, 5, 0), (7, 3, -2), (2, 5, 1)])
@pytest.mark.parametrize("policy", ["reject", "drop"])
def test_every_sqlite_boundary_against_raw_arrival_oracle(tmp_path, width, hop, origin, policy):
    flow = graph_for(width, hop, origin, policy)
    oracle = Interpreter(width, hop, origin, policy)
    source_id, commitment = "0" * 64, "a" * 64
    journal = WindowGraphJournal(tmp_path / "journal.sqlite", flow, source_id, commitment)
    emitted = []

    def execute(command, expected):
        nonlocal journal
        previous = journal.latest()
        request = WindowGraphRequest(
            journal.journal_id, uuid.uuid4().hex, previous.generation, (command,)
        )
        if expected is None:
            with pytest.raises(FlowExecutionError):
                journal.apply(request)
            assert journal.request(request.request_id) is None
            assert journal.latest() == previous
        else:
            receipt = journal.apply(request)
            effective = (
                expected["operation_sequence"]
                != previous.checkpoint.to_dict()["body"]["operation_sequence"]
            )
            assert receipt.status == ("committed" if effective else "no_op")
            assert receipt.after_generation == previous.generation + int(effective)
            assert receipt.after_operation == expected["operation_sequence"]
            assert receipt.after_position == expected["next_position"]
            assert receipt.after_finished == expected["status"]["finished"]
            assert receipt.after_watermark == expected["status"]["watermark"]
            if effective:
                assert journal.request(request.request_id) == receipt
                assert journal.apply(request) == receipt
            else:
                assert journal.request(request.request_id) is None
            emitted.extend(expected["outputs"])
            assert journal.latest().checkpoint.to_dict() == oracle.checkpoint()
            cursor = journal.output_cursor()
            persisted = journal.read_outputs(cursor)
            assert [
                {"step_id": output.step_id, "key": output.record.key, "value": output.record.value}
                for output in persisted.outputs
            ] == emitted
        journal = WindowGraphJournal(journal.path, flow, source_id, commitment, create=False)
        assert journal.latest().checkpoint.to_dict() == oracle.checkpoint()

    def process(timestamp, *, skip=False, account="a", amount=1, repeat=False):
        value = {"account": account, "amount": amount, "skip": skip, "repeat": repeat}
        position = oracle.position
        execute(
            WindowGraphInput(position, timestamp, FlowRecord(value)),
            oracle.process(timestamp, value),
        )

    def watermark(timestamp):
        execute(WindowGraphWatermark(timestamp, oracle.position), oracle.advance(timestamp))

    def drain(count):
        execute(WindowGraphDrain(count), oracle.drain(count))

    process(1, amount=2, repeat=True)
    process(3, skip=True)
    process(4, account="b", amount=3)
    watermark(5)
    while oracle.pending():
        drain(1)
    drain(1)  # Explicit empty-drain no-op partition.
    process(7, account="b", amount=-2)
    process(0, amount=5)  # Late reject or late drop.
    watermark(5)  # Equal watermark is a no-op.
    watermark(15)
    while oracle.pending():
        drain(2)
    execute(WindowGraphFinish(oracle.position), oracle.finish())
    while oracle.pending():
        drain(1)
    execute(WindowGraphFinish(oracle.position), oracle.finish())
    drain(1)
