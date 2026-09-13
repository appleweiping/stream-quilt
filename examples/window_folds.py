"""Offline observed arrivals, explicit watermarks and real checkpoint-file restart.

Run from an installed checkout: python -I examples/window_folds.py
The owned temporary files are demonstration snapshots, not atomic source/state/
output commits, broker offsets, consumer acknowledgements or crash durability.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from stream_quilt import FlowRecord, WindowCheckpoint, WindowFold, WindowFoldRuntime


def initial() -> dict[str, Any]:
    return {"sum": 0, "arrivals": []}


def fold(state: Any, value: Any) -> dict[str, Any]:
    return {"sum": state["sum"] + value, "arrivals": [*state["arrivals"], value]}


def expect(actual: Any, expected: Any) -> None:
    """Keep demonstration verification active even when Python runs with -O."""
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, received {actual!r}")


def restart(spec: WindowFold, runtime: WindowFoldRuntime, path: Path) -> WindowFoldRuntime:
    """Close a newly owned file before loading its bytes into a new runtime."""
    checkpoint = runtime.checkpoint()
    payload = checkpoint.to_json().encode("utf-8")
    with path.open("xb") as handle:
        if handle.write(payload) != len(payload):
            raise OSError("incomplete demonstration checkpoint write")
    restored = WindowFoldRuntime.from_checkpoint(
        spec, WindowCheckpoint.from_json(path.read_bytes())
    )
    expect(restored.checkpoint(), checkpoint)
    return restored


def run_example() -> dict[str, object]:
    spec = WindowFold(
        "offline-window-totals",
        "arrival-list-v1",
        width=5,
        hop=3,
        origin=1,
        tick_unit="tick",
        initial=initial,
        fold=fold,
        late_policy="drop",
    )
    runtime = WindowFoldRuntime(spec)
    rows: list[dict[str, Any]] = []
    with TemporaryDirectory(prefix="stream-quilt-window-") as folder:
        owned = Path(folder)
        for timestamp, key, value in [(2, "A", 2), (0, "A", 5), (2, "B", 7)]:
            expect(runtime.process(timestamp, FlowRecord(value, key)).outcome, "folded")
        expect(runtime.status.watermark, None)  # observed arrivals do not infer progress
        runtime = restart(spec, runtime, owned / "open.json")

        status = runtime.advance_watermark(3)
        expect((status.retained_windows, status.pending_windows, status.finished), (4, 2, False))
        runtime = restart(spec, runtime, owned / "watermark-pending.json")
        first = runtime.drain(max_windows=1)
        rows.extend(row.to_dict() for row in first.rows)
        expect(first.status.phase, "draining")
        second = runtime.drain(max_windows=1)
        rows.extend(row.to_dict() for row in second.rows)
        expect(second.status.phase, "open")

        expect(runtime.process(3, FlowRecord(11, "A")).outcome, "folded")  # equality is on time
        expect(runtime.process(2, FlowRecord(100, "A")).outcome, "late_dropped")
        expect(runtime.process(4, FlowRecord(13, "A")).memberships, 2)
        expect(runtime.finish().pending_windows, 3)
        rows.extend(row.to_dict() for row in runtime.drain(max_windows=1).rows)
        partial_counters = {
            "processed_inputs": 6,
            "late_drops": 1,
            "gap_inputs": 0,
            "membership_updates": 8,
            "created_windows": 5,
            "emitted_windows": 3,
            "finalized_memberships": 6,
        }
        expect(runtime.checkpoint().to_dict()["body"]["counters"], partial_counters)
        runtime = restart(spec, runtime, owned / "partial-eof.json")
        expect(
            (runtime.status.phase, runtime.status.finished, runtime.status.watermark),
            ("draining", True, 3),
        )
        while runtime.status.pending_windows:
            batch = runtime.drain(max_windows=1)
            expect(batch.drained_windows, 1)
            rows.extend(row.to_dict() for row in batch.rows)
        runtime = restart(spec, runtime, owned / "closed.json")
        expect(runtime.finish().phase, "closed")
        expect(runtime.drain().rows, ())

    # Handwritten complete expectations, not calculated with another runtime or
    # its membership helper. The first arrival list is deliberately [2, 5].
    expected_rows = [
        {
            "key": "A",
            "index": -1,
            "start": -2,
            "end": 3,
            "tick_unit": "tick",
            "input_count": 2,
            "value": {"sum": 7, "arrivals": [2, 5]},
        },
        {
            "key": "B",
            "index": -1,
            "start": -2,
            "end": 3,
            "tick_unit": "tick",
            "input_count": 1,
            "value": {"sum": 7, "arrivals": [7]},
        },
        {
            "key": "A",
            "index": 0,
            "start": 1,
            "end": 6,
            "tick_unit": "tick",
            "input_count": 3,
            "value": {"sum": 26, "arrivals": [2, 11, 13]},
        },
        {
            "key": "B",
            "index": 0,
            "start": 1,
            "end": 6,
            "tick_unit": "tick",
            "input_count": 1,
            "value": {"sum": 7, "arrivals": [7]},
        },
        {
            "key": "A",
            "index": 1,
            "start": 4,
            "end": 9,
            "tick_unit": "tick",
            "input_count": 1,
            "value": {"sum": 13, "arrivals": [13]},
        },
    ]
    expect(rows, expected_rows)
    final_counters = runtime.checkpoint().to_dict()["body"]["counters"]
    expected_counters = {
        "processed_inputs": 6,
        "late_drops": 1,
        "gap_inputs": 0,
        "membership_updates": 8,
        "created_windows": 5,
        "emitted_windows": 5,
        "finalized_memberships": 8,
    }
    expect(final_counters, expected_counters)
    expected_status = {
        "phase": "closed",
        "watermark": 3,
        "finished": True,
        "retained_windows": 0,
        "pending_windows": 0,
    }
    expect(runtime.status.to_dict(), expected_status)
    expect(owned.exists(), False)  # only this example's TemporaryDirectory was owned
    return {
        "rows": rows,
        "status": runtime.status.to_dict(),
        "counters": final_counters,
        "checkpoint_file_restarts": 4,
    }


if __name__ == "__main__":
    print(json.dumps(run_example(), sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2))
