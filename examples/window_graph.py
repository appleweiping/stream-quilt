"""Actual single-source DAG, explicit event progress and four file restarts.

Run from an installed checkout: python -I examples/window_graph.py
These temporary files are snapshots, not atomic journals or delivery receipts.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from stream_quilt import (
    FlowEdge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    StateUpdate,
    WindowFold,
    WindowGraphCheckpoint,
    WindowGraphDataflow,
    WindowGraphInput,
    WindowGraphRuntime,
)


def expect(actual: Any, expected: Any) -> None:
    """Do not remove checks or calls when the example is run with -O."""
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, received {actual!r}")


def fold(state: Any, amount: Any) -> dict[str, Any]:
    return {"sum": state["sum"] + amount, "arrivals": [*state["arrivals"], amount]}


def accumulate(row: Any, state: Any) -> StateUpdate:
    total = state + row["value"]["sum"]
    return StateUpdate(
        total,
        {
            "start": row["start"],
            "end": row["end"],
            "arrivals": row["value"]["arrivals"],
            "window_total": row["value"]["sum"],
            "running_total": total,
        },
    )


def configuration() -> WindowGraphDataflow:
    spec = WindowFold(
        "window",
        "sum-and-arrivals-v1",
        width=10,
        initial=lambda: {"sum": 0, "arrivals": []},
        fold=fold,
        late_policy="drop",
    )
    nodes = (
        FlowStep("valid", "filter", lambda event: event["valid"]),
        FlowStep("account", "key_by", lambda event: event["account"]),
        FlowStep(
            "seen",
            "stateful_map",
            lambda event, state: StateUpdate(state + 1, event["amount"]),
            lambda: 0,
        ),
        FlowWindow("window", spec),
        FlowStep("running", "stateful_map", accumulate, lambda: 0),
        FlowStep("summary", "map", lambda row: {"end": row["end"], "total": row["running_total"]}),
        FlowStep("audit", "map", lambda row: row),
    )
    edges = (
        FlowEdge("valid", "account"),
        FlowEdge("account", "seen"),
        FlowEdge("seen", "window"),
        FlowEdge("window", "running"),
        FlowEdge("running", "summary"),
        FlowEdge("running", "audit"),
    )
    return WindowGraphDataflow("account-window-totals", "arrival-v1", nodes, edges, entry="valid")


def restart(
    flow: WindowGraphDataflow, runtime: WindowGraphRuntime, path: Path
) -> WindowGraphRuntime:
    checkpoint = runtime.checkpoint()
    payload = checkpoint.to_json().encode("utf-8")
    with path.open("xb") as handle:
        if handle.write(payload) != len(payload):
            raise OSError("incomplete demonstration checkpoint write")
    restored = WindowGraphRuntime.from_checkpoint(
        flow, WindowGraphCheckpoint.from_json(path.read_bytes())
    )
    expect(restored.checkpoint(), checkpoint)
    return restored


def run_example() -> dict[str, Any]:
    flow = configuration()
    runtime = WindowGraphRuntime(flow)
    outputs: list[dict[str, Any]] = []

    def ingest(timestamp: int, account: str, amount: int, valid: bool = True) -> int:
        batch = runtime.process(
            WindowGraphInput(
                runtime.next_position,
                timestamp,
                FlowRecord({"account": account, "amount": amount, "valid": valid}),
            )
        )
        expect(batch.outputs, ())
        return batch.late_dropped_inputs

    def drain_one() -> None:
        batch = runtime.drain(max_windows=1)
        expect(batch.drained_windows, 1)
        outputs.extend(output.to_dict() for output in batch.outputs)

    with TemporaryDirectory(prefix="stream-quilt-window-graph-") as folder:
        owned = Path(folder)
        for timestamp, account, amount in [(2, "A", 2), (0, "A", 5), (12, "A", 7), (1, "B", 3)]:
            expect(ingest(timestamp, account, amount), 0)
        expect(ingest(5, "A", 99, valid=False), 0)  # filtering still consumes a source position
        expect(runtime.status.watermark, None)
        runtime = restart(flow, runtime, owned / "open.json")

        expect(runtime.advance_watermark(10, next_position=5).status.pending_windows, 2)
        runtime = restart(flow, runtime, owned / "watermark-pending.json")
        drain_one()
        expect(runtime.status.phase, "draining")
        drain_one()
        expect(runtime.status.phase, "open")

        expect(ingest(3, "A", 100), 1)  # the prefix's seen count commits; the window drops
        expect(ingest(10, "A", 11), 0)  # timestamp equal to the watermark is accepted
        expect(ingest(22, "B", 13), 0)
        expect(runtime.finish(next_position=8).status.pending_windows, 2)
        drain_one()
        runtime = restart(flow, runtime, owned / "partial-eof.json")
        expect((runtime.status.finished, runtime.status.watermark), (True, 10))
        drain_one()
        runtime = restart(flow, runtime, owned / "closed.json")
        expect(runtime.finish(next_position=8).operation_sequence, 14)
        expect(runtime.drain().outputs, ())

    # Complete handwritten row contents; no geometry/scheduler helper generates
    # these expectations. Each window's summary precedes its audit, including
    # across checkpoint restores and one-window drain calls.
    expected: list[dict[str, Any]] = []
    for key, start, end, arrivals, total, running in [
        ("A", 0, 10, [2, 5], 7, 7),
        ("B", 0, 10, [3], 3, 3),
        ("A", 10, 20, [7, 11], 18, 25),
        ("B", 20, 30, [13], 13, 16),
    ]:
        expected.extend(
            [
                {"step_id": "summary", "key": key, "value": {"end": end, "total": running}},
                {
                    "step_id": "audit",
                    "key": key,
                    "value": {
                        "start": start,
                        "end": end,
                        "arrivals": arrivals,
                        "window_total": total,
                        "running_total": running,
                    },
                },
            ]
        )
    expect(outputs, expected)
    body = runtime.checkpoint().to_dict()["body"]
    expect(body["edge_counts"], [7, 7, 7, 4, 4, 4])
    expect(body["counters"], {"watermark_advances": 1, "drain_operations": 4, "emitted_records": 8})
    expect(
        body["ordinary_cells"],
        [
            {"step_id": "running", "key": "A", "state": "25"},
            {"step_id": "running", "key": "B", "state": "16"},
            {"step_id": "seen", "key": "A", "state": "5"},
            {"step_id": "seen", "key": "B", "state": "2"},
        ],
    )
    expect(
        body["window"]["body"]["counters"],
        {
            "processed_inputs": 7,
            "late_drops": 1,
            "gap_inputs": 0,
            "membership_updates": 6,
            "created_windows": 4,
            "emitted_windows": 4,
            "finalized_memberships": 6,
        },
    )
    expect(
        runtime.status.to_dict(),
        {
            "phase": "closed",
            "watermark": 10,
            "finished": True,
            "retained_windows": 0,
            "pending_windows": 0,
        },
    )
    expect(owned.exists(), False)
    return {
        "outputs": outputs,
        "status": runtime.status.to_dict(),
        "source_inputs": runtime.next_position,
        "operations": runtime.operation_sequence,
        "edge_counts": body["edge_counts"],
        "counters": body["counters"],
        "checkpoint_file_restarts": 4,
    }


if __name__ == "__main__":
    print(json.dumps(run_example(), sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2))
