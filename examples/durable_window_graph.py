"""Offline window graph SQLite publication and lost-response reconciliation.

Run from an installed checkout: python -I examples/durable_window_graph.py
The source commitment belongs to the caller; external sink delivery is separate.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from stream_quilt import (
    FlowEdge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    WindowFold,
    WindowGraphDataflow,
    WindowGraphDrain,
    WindowGraphInput,
    WindowGraphJournal,
    WindowGraphOutputCursor,
    WindowGraphRequest,
    WindowGraphWatermark,
)


def expect(actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, received {actual!r}")


def configuration() -> WindowGraphDataflow:
    return WindowGraphDataflow(
        "durable-window-example",
        "v1",
        (
            FlowStep("input", "map", lambda value: value),
            FlowWindow(
                "window",
                WindowFold(
                    "window", "sum-v1", width=10, initial=lambda: 0, fold=lambda a, b: a + b
                ),
            ),
        ),
        (FlowEdge("input", "window"),),
        entry="input",
    )


def run_example() -> dict[str, Any]:
    source = [
        WindowGraphInput(0, 2, FlowRecord(3, "a")),
        WindowGraphInput(1, 4, FlowRecord(5, "a")),
        WindowGraphInput(2, 12, FlowRecord(7, "a")),
    ]
    source_bytes = json.dumps(
        [
            {
                "position": item.position,
                "timestamp": item.timestamp,
                "record": item.record.to_dict(),
            }
            for item in source
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    commitment = hashlib.sha256(source_bytes).hexdigest()
    flow = configuration()
    with TemporaryDirectory(prefix="stream-quilt-durable-window-") as folder:
        path = Path(folder)
        journal = WindowGraphJournal(path / "journal.sqlite", flow, "0" * 64, commitment)
        first = WindowGraphRequest(
            journal.journal_id,
            "1" * 32,
            0,
            (source[0], source[1], WindowGraphWatermark(10, 2)),
        )
        intent = path / "pending.json"
        intent.write_text(first.to_json(), encoding="utf-8")
        expect(journal.apply(first).after_operation, 3)

        # The caller can recover a lost response using its retained exact intent.
        journal = WindowGraphJournal(journal.path, flow, "0" * 64, commitment, create=False)
        restored = WindowGraphRequest.from_json(intent.read_bytes())
        receipt = journal.request(restored.request_id)
        if receipt is None:
            raise AssertionError("the committed request receipt disappeared")
        expect(journal.apply(restored), receipt)
        expect(
            journal.apply(
                WindowGraphRequest(journal.journal_id, "2" * 32, 1, (WindowGraphDrain(1),))
            ).output_stop,
            1,
        )
        old_cursor = journal.output_cursor()
        expect(old_cursor.stop_sequence, 1)
        next_request = WindowGraphRequest(
            journal.journal_id,
            "3" * 32,
            2,
            (source[2], WindowGraphWatermark(20, 3), WindowGraphDrain(1)),
        )
        expect(journal.apply(next_request).output_stop, 2)
        expect(len(journal.read_outputs(old_cursor).outputs), 1)
        cursor = journal.output_cursor()
        outputs = []
        while cursor.next_sequence < cursor.stop_sequence:
            page = journal.read_outputs(
                WindowGraphOutputCursor.from_json(cursor.to_json()), limit=1
            )
            outputs.extend(item.to_dict() for item in page.outputs)
            cursor = page.cursor
        expect([output["record"]["value"]["value"] for output in outputs], [8, 7])
        expect([output["record"]["value"]["start"] for output in outputs], [0, 10])
        no_op = WindowGraphRequest(
            journal.journal_id, "4" * 32, 3, (WindowGraphWatermark(20, 3), WindowGraphDrain(1))
        )
        expect(journal.apply(no_op).status, "no_op")
        expect(journal.request(no_op.request_id), None)
        result = {
            "generation": journal.latest().generation,
            "operations": journal.latest().checkpoint.to_dict()["body"]["operation_sequence"],
            "old_cursor_stop": old_cursor.stop_sequence,
            "new_cursor_stop": cursor.stop_sequence,
            "window_values": [output["record"]["value"]["value"] for output in outputs],
            "source_commitment": commitment,
        }
    return result


if __name__ == "__main__":
    print(json.dumps(run_example(), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
