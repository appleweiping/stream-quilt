"""Offline real-worker SQLite restart and independently computed output totals."""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

from stream_quilt import (
    Dataflow,
    FlowRecord,
    FlowStep,
    PartitionedFlowJournal,
    PartitionedFlowRequest,
    StateUpdate,
)


def accumulate(value: int, state: int) -> StateUpdate:
    return StateUpdate(state + value, state + value)


def main() -> None:
    source = [
        ("red", 1),
        ("blue", 2),
        ("red", 3),
        ("green", 4),
        ("blue", 5),
        ("red", -2),
        ("green", 6),
        ("blue", 7),
    ]
    source_digest = hashlib.sha256(json.dumps(source).encode()).hexdigest()
    flow = Dataflow(
        "durable-colour-totals", "1", (FlowStep("sum", "stateful_map", accumulate, int),)
    )
    expected, totals = [], {}
    for key, value in source:
        totals[key] = totals.get(key, 0) + value
        expected.append(totals[key])
    with tempfile.TemporaryDirectory(prefix="stream-quilt-durable-workers-") as folder:
        path = Path(folder)
        pids = []
        for restart in range(2):
            journal = PartitionedFlowJournal(
                path / "journal.db", flow, "colour-events-v1", source_digest, create=restart == 0
            )
            point = journal.latest()
            request = PartitionedFlowRequest(
                point.journal_id,
                f"{restart + 1:032x}",
                point.generation,
                "wave",
                point.checkpoint.next_position,
                tuple(
                    FlowRecord(value, key) for key, value in source[restart * 4 : (restart + 1) * 4]
                ),
            )
            saved = path / f"request-{restart}.json"
            saved.write_text(request.to_json(), encoding="utf-8")
            with journal.session() as session:
                pids.append([worker.pid for worker in session.worker_status()])
                receipt = session.apply(PartitionedFlowRequest.from_json(saved.read_bytes()))
                assert session.apply(request) == receipt
                if restart == 1:
                    eof = PartitionedFlowRequest(point.journal_id, "f" * 32, 2, "eof", len(source))
                    session.apply(eof)
        cursor, actual = journal.output_cursor(), []
        while cursor.next_sequence < cursor.stop_sequence:
            page = journal.read_outputs(cursor, limit=3)
            actual.extend(item.output.record.value for item in page.outputs)
            cursor = page.cursor
        assert actual == expected
        point = journal.latest()
        assert point.checkpoint.source_closed and point.generation == 3
        print(
            json.dumps(
                {
                    "worker_pids_by_session": pids,
                    "source_next": point.checkpoint.next_position,
                    "shard_counts": [shard.processed_inputs for shard in point.checkpoint.shards],
                    "generation": point.generation,
                    "outputs": actual,
                    "serial_match": True,
                }
            )
        )


if __name__ == "__main__":
    main()
