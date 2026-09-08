"""Offline real-process keyed totals, ordered output and portable restart.

Run from an installed checkout: python examples/local_partitioned_totals.py
The main guard is required by the multiprocessing spawn lifecycle.
"""

import hashlib
import json
import os

from stream_quilt import (
    Dataflow,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    LocalPartitionedFlow,
    PartitionedFlowCheckpoint,
    StateUpdate,
)


def total(value, state):
    result = state + value
    return StateUpdate(result, result)


def main():
    records = tuple(FlowRecord(i, ("camera", "audio", "text")[i % 3]) for i in range(24))
    source_digest = hashlib.sha256(
        json.dumps([item.to_dict() for item in records], sort_keys=True).encode()
    ).hexdigest()
    flow = Dataflow("offline-keyed-totals", "1", (FlowStep("sum", "stateful_map", total, int),))
    serial = FlowRuntime(flow)
    expected = [out.to_dict() for item in records for out in serial.process(item)]
    with LocalPartitionedFlow(flow, "generated-24", source_digest, workers=3) as runtime:
        first = runtime.process_batch(0, records[:11])
        first_workers = [row.pid for row in runtime.worker_status()]
        portable = first.checkpoint.to_json()
    restored = PartitionedFlowCheckpoint.from_json(portable)
    with LocalPartitionedFlow(
        flow, "generated-24", source_digest, workers=3, checkpoint=restored
    ) as runtime:
        second = runtime.process_batch(11, records[11:])
        second_workers = [row.pid for row in runtime.worker_status()]
        final = runtime.close_source(24)
    rows = (*first.outputs, *second.outputs)
    assert [row.record.to_dict() for row in rows] == expected
    assert final.next_position == sum(shard.processed_inputs for shard in final.shards) == 24
    print(
        json.dumps(
            {
                "parent_pid": os.getpid(),
                "initial_worker_pids": first_workers,
                "restored_worker_pids": second_workers,
                "source_next_position": final.next_position,
                "shard_input_counts": [shard.processed_inputs for shard in final.shards],
                "waves": final.waves,
                "source_closed": final.source_closed,
                "matches_serial": True,
                "outputs": [
                    {
                        "sequence": row.sequence,
                        "source_position": row.source_position,
                        "key": row.record.key,
                        "total": row.record.value,
                    }
                    for row in rows
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
