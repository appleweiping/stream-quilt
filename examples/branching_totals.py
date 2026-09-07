"""Run a conditional DAG, restore its state, and check independent totals offline."""

from __future__ import annotations

import json

from stream_quilt import (
    FlowBranch,
    FlowEdge,
    FlowMerge,
    FlowRecord,
    FlowStep,
    GraphCheckpoint,
    GraphDataflow,
    GraphRuntime,
    StateUpdate,
)


def accumulate(value: int, state: int) -> StateUpdate:
    total = state + value
    return StateUpdate(total, total)


def main() -> None:
    flow = GraphDataflow(
        "account-adjustments",
        "negative-adjustments-first-v1",
        (
            FlowStep("items", "flat_map", lambda values: values),
            FlowBranch("nonnegative", lambda value: value >= 0),
            FlowMerge("ordered"),
            FlowStep("total", "stateful_map", accumulate, lambda: 0),
        ),
        (
            FlowEdge("items", "nonnegative"),
            FlowEdge("nonnegative", "ordered", False),
            FlowEdge("nonnegative", "ordered", True),
            FlowEdge("ordered", "total"),
        ),
        "items",
    )
    runtime = GraphRuntime(flow)
    outputs = list(runtime.process(FlowRecord([3, 5], "account-a")))
    portable = json.loads(json.dumps(runtime.checkpoint().to_dict(), allow_nan=False))
    runtime = GraphRuntime.from_checkpoint(flow, GraphCheckpoint.from_dict(portable))
    # Merge's declared false edge comes first: -1 is applied before +8.
    outputs.extend(runtime.process(FlowRecord([8, -1], "account-a")))
    totals = [item.record.value for item in outputs]
    assert totals == [3, 8, 7, 15]
    assert runtime.checkpoint().cells == (("total", "account-a", "15"),)
    print(json.dumps({"outputs": totals, "processed_inputs": runtime.processed_inputs}))


if __name__ == "__main__":
    main()
