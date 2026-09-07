"""Run a keyed flow in two journal sessions without repository writes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from stream_quilt import Dataflow, FlowJournal, FlowRecord, FlowStep, StateUpdate


def main() -> None:
    values = [("a", 2), ("b", 3), ("a", 4), ("b", 5)]
    source_id = hashlib.sha256(json.dumps(values).encode()).hexdigest()
    spec = Dataflow(
        "durable-totals",
        "v1",
        (
            FlowStep(
                "sum",
                "stateful_map",
                lambda value, state: StateUpdate(state + value, state + value),
                lambda: 0,
            ),
        ),
    )
    with TemporaryDirectory(prefix="stream-quilt-journal-") as directory:
        path = Path(directory) / "totals.sqlite"
        journal = FlowJournal(path, spec, source_id)
        journal.advance(
            [FlowRecord(value, key) for key, value in values[:2]], expected_generation=0
        )
        reopened = FlowJournal(path, spec, source_id, create=False)
        point = reopened.latest()
        reopened.advance(
            [FlowRecord(value, key) for key, value in values[point.next_position :]],
            expected_generation=point.generation,
        )
        outputs = [item.to_dict() for item in reopened.outputs()]
        assert [item["record"]["value"] for item in outputs] == [2, 3, 6, 8]
        print(
            json.dumps(
                {"next_position": reopened.latest().next_position, "outputs": outputs},
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
