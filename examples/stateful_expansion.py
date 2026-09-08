"""Offline keyed batching with zero-output commits and actual SQLite restart."""

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from stream_quilt import Dataflow, FlowJournal, FlowRecord, FlowStep, StateFlatUpdate


def main() -> None:
    def batch(value, state):
        pending = [*state, value]
        if len(pending) < 2:
            return StateFlatUpdate(pending, ())
        return StateFlatUpdate(None, (pending, sum(pending)), retain=False)

    flow = Dataflow("keyed-batches", "v1", (FlowStep("batch", "stateful_flat_map", batch, list),))
    source = [FlowRecord(value, key) for key, value in (("a", 2), ("b", 9), ("a", 3), ("a", 4))]
    source_id = hashlib.sha256(b"keyed-batches-v1:a=2,b=9,a=3,a=4").hexdigest()
    with TemporaryDirectory(prefix="stream-quilt-stateful-expansion-") as temporary:
        path = Path(temporary) / "batches.sqlite"
        first = FlowJournal(path, flow, source_id).advance(
            source, expected_generation=0, max_inputs=1
        )
        assert first.next_position == 1 and first.checkpoint.emitted_records == 0
        reopened = FlowJournal(path, flow, source_id, create=False)
        final = reopened.advance(
            source[first.next_position :], expected_generation=first.generation
        )
        outputs = list(reopened.outputs())
        assert [item.record.value for item in outputs] == [[2, 3], 5]
        assert [item.source_position for item in outputs] == [2, 2]
        assert final.checkpoint.cells == (("batch", "a", "[4]"), ("batch", "b", "[9]"))
        assert final.next_position == 4
        print(
            json.dumps(
                {
                    "next_position": final.next_position,
                    "outputs": [item.to_dict() for item in outputs],
                }
            )
        )


if __name__ == "__main__":
    main()
