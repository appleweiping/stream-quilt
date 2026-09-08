"""Offline source/state/output recovery for two independently keyed graph branches."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from stream_quilt import (
    FlowEdge,
    FlowRecord,
    FlowStep,
    GraphDataflow,
    GraphJournal,
    StateUpdate,
)


def main() -> None:
    flow = GraphDataflow(
        "durable-two-views",
        "v1",
        (
            FlowStep("root", "map", lambda value: value),
            FlowStep(
                "totals",
                "stateful_map",
                lambda value, state: StateUpdate(state + value, state + value),
                lambda: 0,
            ),
            FlowStep(
                "scaled",
                "stateful_map",
                lambda value, state: StateUpdate(state + 10 * value, state + 10 * value),
                lambda: 0,
            ),
        ),
        (FlowEdge("root", "totals"), FlowEdge("root", "scaled")),
        entry="root",
    )
    source = [FlowRecord(value, "a") for value in (2, 3, 4)]
    # This demonstration's application-owned source commitment binds the exact
    # values/order; production callers must commit their actual source layout.
    import hashlib

    source_id = hashlib.sha256(b"FlowRecord(a):2,3,4;v1").hexdigest()
    with TemporaryDirectory(prefix="stream-quilt-graph-journal-") as temporary:
        path = Path(temporary) / "graph.sqlite"
        journal = GraphJournal(path, flow, source_id)
        first = journal.advance(source, expected_generation=0, max_inputs=2)
        reopened = GraphJournal(path, flow, source_id, create=False)
        after = reopened.advance(
            source[first.next_position :], expected_generation=first.generation
        )
        outputs = list(reopened.outputs())
        assert [item.record.value for item in outputs] == [2, 20, 5, 50, 9, 90]
        assert [item.source_position for item in outputs] == [0, 0, 1, 1, 2, 2]
        print(
            json.dumps(
                {
                    "next_position": after.next_position,
                    "outputs": [item.to_dict() for item in outputs],
                }
            )
        )


if __name__ == "__main__":
    main()
