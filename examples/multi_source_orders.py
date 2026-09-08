"""Offline two-source final join, shared state, checkpoint restart and fanout.

Run: python examples/multi_source_orders.py
No network, broker, external sink, or implicit source EOF is involved.
"""

import json

from stream_quilt import (
    FlowEdge,
    FlowEntry,
    FlowJoin,
    FlowRecord,
    FlowStep,
    GraphInput,
    JoinEdge,
    KeyedJoin,
    MultiGraphCheckpoint,
    MultiGraphDataflow,
    MultiGraphRuntime,
    StateUpdate,
)


def build_flow() -> MultiGraphDataflow:
    return MultiGraphDataflow(
        "offline-order-reconciliation",
        "1",
        (
            FlowStep("orders", "key_by", lambda value: value["order_id"]),
            FlowStep("payments", "key_by", lambda value: value["order_id"]),
            FlowJoin(
                "reconcile",
                KeyedJoin(
                    "reconcile", "1", ("order", "payment"), insert_mode="last", emit_mode="final"
                ),
            ),
            FlowStep(
                "count",
                "stateful_map",
                lambda value, count: StateUpdate(
                    count + 1, {"match": value, "results_for_key": count + 1}
                ),
                lambda: 0,
            ),
            FlowStep("report", "map", lambda value: value),
            FlowStep("audit", "map", lambda value: {"matched": all(value["match"]["present"])}),
        ),
        (
            JoinEdge("orders", "reconcile", "order"),
            JoinEdge("payments", "reconcile", "payment"),
            FlowEdge("reconcile", "count"),
            FlowEdge("count", "report"),
            FlowEdge("count", "audit"),
        ),
        (FlowEntry("order-file", "orders"), FlowEntry("payment-file", "payments")),
    )


def main() -> None:
    flow = build_flow()
    runtime = MultiGraphRuntime(flow)
    runtime.process(GraphInput("order-file", 0, FlowRecord({"order_id": "A", "quantity": 2})))
    runtime.process(GraphInput("order-file", 1, FlowRecord({"order_id": "B", "quantity": 1})))
    runtime.close("order-file", next_position=2)

    # Portable JSON represents the NEXT local position for each source, all
    # operator/join state and explicit EOF. This is not a broker acknowledgement.
    saved = runtime.checkpoint().to_json()
    runtime = MultiGraphRuntime.from_checkpoint(flow, MultiGraphCheckpoint.from_json(saved))
    runtime.process(GraphInput("payment-file", 0, FlowRecord({"order_id": "A", "amount": 7})))
    runtime.close("payment-file", next_position=1)
    outputs = []
    while runtime.ready_joins:
        outputs.extend(output.to_dict() for output in runtime.drain(max_keys=1).outputs)
    print(
        json.dumps(
            {
                "phase": runtime.phase,
                "outputs": outputs,
                "sources": runtime.checkpoint().to_dict()["sources"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
