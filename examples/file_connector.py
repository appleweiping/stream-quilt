"""Real-worker staged file replay, restart and idempotent output snapshot."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from stream_quilt import Dataflow, FlowStep, PartitionedFlowJournal
from stream_quilt.connectors.files import (
    StagedJsonlSource,
    materialize_file,
    run_file_journal,
)


def main() -> None:
    flow = Dataflow("staged-file-demo", "1", (FlowStep("stringify", "map", str),))
    with tempfile.TemporaryDirectory(prefix="stream-quilt-file-", dir=Path.cwd()) as folder:
        root = Path(folder)
        incoming = root / "incoming"
        incoming.mkdir()
        (incoming / "orders.jsonl").write_bytes(
            b'{"key":"red","value":1}\n{"key":"blue","value":2}\n{"key":"red","value":3}\n'
        )
        private = root / "private"
        source = StagedJsonlSource.stage(incoming, "orders.jsonl", private, "orders-v1")
        journal = PartitionedFlowJournal(
            private / "journal.db", flow, source.source_id, source.source_digest
        )
        first = run_file_journal(source, journal, max_new_inputs=2, batch_size=2)
        assert first.checkpoint.next_position == 2 and not first.checkpoint.source_closed
        restored = StagedJsonlSource.restore(
            private, "orders.jsonl", source.source_id, source.source_digest
        )
        reopened = PartitionedFlowJournal(
            private / "journal.db", flow, source.source_id, source.source_digest, create=False
        )
        final = run_file_journal(restored, reopened, max_new_inputs=2)
        output = materialize_file(reopened, private, "output.jsonl")
        before = output.read_bytes()
        assert materialize_file(reopened, private, "output.jsonl").read_bytes() == before
        rows = [json.loads(line) for line in before.splitlines()[1:]]
        assert [row["value"] for row in rows] == ["1", "2", "3"]
        assert final.checkpoint.source_closed
        print(
            json.dumps(
                {
                    "source_next": final.checkpoint.next_position,
                    "output_rows": len(rows),
                    "source_closed": final.checkpoint.source_closed,
                    "idempotent_snapshot": True,
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
