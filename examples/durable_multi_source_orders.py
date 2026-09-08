"""Offline SQLite request receipts, explicit EOF/final drain and fixed output cursors."""

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from stream_quilt import (
    FlowEntry,
    FlowJoin,
    FlowRecord,
    FlowStep,
    GraphDrain,
    GraphEOF,
    GraphInput,
    JoinEdge,
    KeyedJoin,
    MultiGraphDataflow,
    MultiGraphJournal,
    MultiGraphOutputCursor,
    MultiGraphRequest,
)


def main() -> None:
    calls = []

    def observe(value):
        calls.append(value)
        return value

    flow = MultiGraphDataflow(
        "durable-orders",
        "1",
        (
            FlowStep("orders", "map", observe),
            FlowStep("payments", "map", observe),
            FlowJoin("join", KeyedJoin("join", "1", ("order", "payment"), "last", "final")),
        ),
        (JoinEdge("orders", "join", "order"), JoinEdge("payments", "join", "payment")),
        (FlowEntry("orders", "orders"), FlowEntry("payments", "payments")),
    )
    orders = [FlowRecord(2, "A"), FlowRecord(1, "B")]
    payments = [FlowRecord(7, "A")]
    commitments = {
        name: hashlib.sha256(
            json.dumps(
                [record.to_dict() for record in records], sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        for name, records in (("orders", orders), ("payments", payments))
    }
    with TemporaryDirectory(prefix="stream-quilt-multi-journal-") as directory:
        path = Path(directory)
        journal = MultiGraphJournal(path / "journal.sqlite", flow, commitments)
        pending = MultiGraphRequest(
            journal.latest().journal_id,
            "1" * 32,
            0,
            (
                GraphInput("orders", 0, orders[0]),
                GraphInput("orders", 1, orders[1]),
                GraphInput("payments", 0, payments[0]),
                GraphEOF("orders", 2),
                GraphEOF("payments", 1),
            ),
        )
        request_file = path / "pending.json"
        request_file.write_text(pending.to_json(), encoding="utf-8")
        # Simulate a caller losing the return value AFTER this real successful commit.
        # Driver commit-then-raise injection is exercised separately by the fault tests.
        journal.apply(pending)
        journal = MultiGraphJournal(journal.path, flow, commitments, create=False)
        saved = MultiGraphRequest.from_json(request_file.read_bytes())
        confirmed = journal.request(saved.request_id)
        assert confirmed is not None and journal.apply(saved) == confirmed
        assert len(calls) == 3  # Retrieving/replaying the committed request ran no callbacks.
        journal.apply(MultiGraphRequest(saved.journal_id, "2" * 32, 1, (GraphDrain(1),)))
        old_cursor = journal.output_cursor()
        journal.apply(MultiGraphRequest(saved.journal_id, "3" * 32, 2, (GraphDrain(1),)))
        assert len(journal.read_outputs(old_cursor).outputs) == 1  # Its stop did not grow.
        cursor = journal.output_cursor()
        outputs = []
        while cursor.next_sequence < cursor.stop_sequence:
            page = journal.read_outputs(MultiGraphOutputCursor.from_json(cursor.to_json()), limit=1)
            outputs.extend(item.to_dict() for item in page.outputs)
            cursor = page.cursor
        assert [item["record"]["value"] for item in outputs] == [
            {"present": [True, True], "values": [2, 7]},
            {"present": [True, False], "values": [1, None]},
        ]
        print(
            json.dumps(
                {
                    "generation": journal.latest().generation,
                    "callback_calls": len(calls),
                    "old_cursor_stop": old_cursor.stop_sequence,
                    "sources": journal.latest().checkpoint.to_dict()["sources"],
                    "outputs": outputs,
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
