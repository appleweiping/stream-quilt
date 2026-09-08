"""Offline three-side product join with real checkpoint files and partial drain restart.

The source schedule/position are application-owned. These temporary demonstration
file writes are not an atomic source/state/output transaction or a production journal.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from stream_quilt import FlowRecord, JoinCheckpoint, JoinRuntime, KeyedJoin


def run_example() -> dict[str, object]:
    join = KeyedJoin("inventory", "v1", ("catalog", "price", "stock"), "product", "final")
    arrivals = [
        ("catalog", "b", "camera"),
        ("catalog", "a", "lens"),
        ("catalog", "c", None),
        ("price", "a", 100),
        ("price", "a", 120),
        ("stock", "a", True),
        ("price", "b", 300),
        ("stock", "b", False),
    ]
    runtime = JoinRuntime(join)
    for side, key, value in arrivals[:4]:
        assert runtime.process(side, FlowRecord(value, key)).rows == ()
    runtime.close("catalog")
    with TemporaryDirectory(prefix="stream-quilt-join-") as folder:
        path = Path(folder) / "checkpoint.json"
        path.write_text(runtime.checkpoint().to_json(), encoding="utf-8")
        runtime = JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_json(path.read_bytes()))
        assert runtime.closed_sides == (True, False, False)
        for side, key, value in arrivals[4:]:
            runtime.process(side, FlowRecord(value, key))
        runtime.close("price")
        runtime.close("stock")
        first = runtime.drain(max_keys=1)
        assert first.drained_keys == 1 and first.phase == "draining"
        path.write_text(runtime.checkpoint().to_json(), encoding="utf-8")
        runtime = JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_json(path.read_bytes()))
        second = runtime.drain()
    rows = [row.to_dict() for row in (*first.rows, *second.rows)]
    assert [(row["key"], row["values"]) for row in rows] == [
        ("a", ["lens", 100, True]),
        ("a", ["lens", 120, True]),
        ("b", ["camera", 300, False]),
        ("c", [None, None, None]),
    ]
    assert rows[-1]["present"] == [True, False, False]
    assert runtime.phase == "closed"
    checkpoint = runtime.checkpoint()
    assert checkpoint.processed_inputs == (3, 3, 2) and checkpoint.emitted_rows == 4
    return {"rows": rows, "phase": runtime.phase, "processed_inputs": checkpoint.processed_inputs}


if __name__ == "__main__":
    print(json.dumps(run_example(), ensure_ascii=False, indent=2))
