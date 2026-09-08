# Local keyed multi-process Flow

`LocalPartitionedFlow` executes a key-preserving linear `Dataflow` in 1..8 real
`spawn` processes. It shares the existing `FlowRuntime`; it is not a second
operator engine or an alias for the offline `partition_events` helper.
Run `python examples/local_partitioned_totals.py` for actual worker PIDs,
ordered keyed totals, JSON checkpoint restore and an independent serial result.

```python
from stream_quilt import LocalPartitionedFlow

# flow contains trusted, spawn-serializable top-level Python callbacks.
# source_digest identifies the application's replayable ordered source.
with LocalPartitionedFlow(flow, "source-v1", source_digest, workers=3) as runtime:
    batch = runtime.process_batch(0, tuple(keyed_records))
    checkpoint = batch.checkpoint
    for row in batch.outputs:
        print(row.sequence, row.source_position, row.output_index, row.record.value)
    final = runtime.close_source(checkpoint.next_position)
```

Put process-starting application code under `if __name__ == "__main__":`.
Callbacks must be importable/serializable in the spawn environment. Lambdas and
ordinary local closures are not made portable through hidden dependencies.

## Keys, order and the commit point

Inputs are exact immutable `FlowRecord` objects with existing canonical keys.
`map`, `filter`, `flat_map`, `stateful_map` and `stateful_flat_map` are supported.
`key_by` and `drop_key` are rejected before processes start: changing a key before
another stateful stage would require a new cross-worker exchange. Callback state
remains per-step/per-key as in [the linear Flow contract](dataflow.md).

Routing is the unsigned big-endian integer in the first eight bytes of
`SHA256(b"stream-quilt-key-route-v1\0" + key.encode("utf-8"))`, modulo the worker
count. Keys are not normalized, and Python's randomized `hash()` is not used.
All records of a key reach one worker in their original order. Every active shard
is dispatched before any shard is awaited. Empty shards receive no synthetic
record. The runtime maintains one bounded wave at a time.

For each wave, workers reconstruct the last parent-authoritative shard snapshot
and return candidate state plus ordered outputs. Parent validation checks message
identity, source positions, key preservation, per-input output ordinals, shard
counters, Flow invariants and whole-wave/state budgets. The return tuple and
complete next checkpoint are allocated before one parent pointer swap. The final
cancel check and publication share a lock: a cancel request linearized first
prevents that publication; cancellation after publication does not undo it.

`next_position` is the next position of the **one original ordered source**.
Each shard's `processed_inputs` is its routed input count, not a broker offset;
these counts sum to `next_position`. Outputs have a cumulative `sequence`, original
`source_position` and zero-based `output_index` within that input. They are returned
in source/ordinal order, never worker completion order. Filtering emits no row;
zero-output inputs still advance their shard count and the source position.

Serial equivalence requires deterministic callbacks whose relevant state lives
in Flow state. Per-process callback globals, randomness, wall time and external
effects need not match a single-process run. Callback mutations outside Flow
state cannot be rolled back. Failed waves are never automatically retried.

## Portable snapshots and failure scope

`PartitionedFlowCheckpoint.to_json()` and strict `from_json()` use the separate
`stream-quilt-partitioned-flow` / `1.0` / `sha256-key-v1` format. It binds the
Flow identity, application source ID/digest, worker count, limits, source position,
wave count, source-closed flag and all shard snapshots. Keys, counts, routed cells,
canonical value encodings and aggregate limits are revalidated. Changing worker
count or limits on restore is rejected, not interpreted as rescaling.
The active runtime's `flow`, `limits` and `workers` properties are read-only.

The application must change the Flow semantic revision when callback behavior
changes; Flow identity is not a Python-bytecode hash. Source digest is mandatory
application identity, not library authentication or automatic source-content
verification. Snapshot invariants are necessary consistency checks, not a proof
of every historical callback/output. PIDs are diagnostics, not restore identity.

An execution failure makes the session failed and leaves `checkpoint()` at the
last published state. `LocalWorkerError.reason` is a minimized classification;
child callback exception arguments/tracebacks are not sent in that protocol. A child
control exception is reported as `control`, not recreated as a parent interrupt.
A genuine parent `KeyboardInterrupt`/`SystemExit` remains the parent's exception.
Teardown is attempted for participating and idle owned workers; `worker_status()`
remains available, and OS refusal is reported as described below. A failed
input/configuration admission before dispatch leaves the
session usable. `close()` is idempotent after successful cleanup.
Trusted code inherits standard output/error and may log directly; those streams
and arbitrary callback-created external effects are not privacy-filtered.

This commit point is **in memory**, not SQLite. The application can serialize a
checkpoint, but publication, saving that file and delivering outputs are not one
durable transaction. A parent crash can lose unsaved state; restoring an older
checkpoint may repeat callbacks and external effects. The runtime does not retain
an output journal or reconcile a lost external acknowledgement.

## Pulling, EOF, ownership and cancellation

`run(iterable, max_inputs=100_000, batch_size=64, own_source=False)` is a pull
iterator of committed batches. It does not prefetch the next wave while a batch
is yielded. Reaching `max_inputs` does not probe one extra item or imply EOF.
When exhaustion is observed, a fully consumed helper calls `close_source` at the
exact next position. If it yields a final partial batch, its EOF close occurs
when the helper is resumed; stopping after that yield does not silently mark EOF.
Explicit repeated close at the same position returns the same checkpoint and
does not increment waves or call nonexistent linear Flow EOF callbacks.

From its first iteration until completion/closure, a pull helper holds an
exclusive source-position lease, including during `__iter__`, `__next__`, source
cleanup and yielded batches. Another helper or external `process_batch` or
`close_source` is rejected. Closing the helper releases the lease even when
source iteration/cleanup fails. Read-only inspection, `cancel()` and runtime
teardown `close()` remain available; a resumed helper cannot pull after teardown.
The owner must close a paused helper explicitly if abandoning it.

Sources are borrowed by default. With `own_source=True`, the helper closes the
iterator it actually acquired, not unrelated objects. Cleanup must be synchronous:
returned awaitables/async generators are rejected, never driven; a directly
returned native coroutine is closed without awaiting it (a started coroutine can
execute `finally` during close). Ordinary cleanup errors do not mask an existing
ordinary/control exception; genuine cleanup control outranks an ordinary primary,
but the first genuine control is preserved. Generator closure reports cleanup
failure rather than silently treating that failure as successful disposal.

Only the creating thread may operate/close the runtime. `cancel()` is the one
cross-thread request API. It does not interrupt an arbitrary trusted source's
`next()` or force its cleanup. A consumed-but-unpublished input is not replayed:
the application must seek/rebuild its source from the last published position.

## Resource and transport limits

Every limit is positive and has a hard ceiling; booleans, nonfinite values,
non-integral counters and huge integers are rejected before numeric conversion.
The following are whole-runtime/wave bounds, not allowances multiplied by workers.

| Limit | Default / maximum | Charged representation |
|---|---:|---|
| Workers | 2 / 8 | Actual owned spawn processes |
| `max_batch_inputs` | 256 / 256 | Input tuple length per wave |
| `max_input_bytes` | 8 / 8 MiB | Sum of canonical `{position,record}` wires including keys |
| `max_message_bytes` | 16 / 16 MiB | Each complete operation request/response frame |
| `max_result_bytes` | 64 / 64 MiB | Aggregate received reply bytes; each active worker is reserved `floor(limit/active_workers)`, capped by its message limit |
| `max_output_records` | 100,000 / 100,000 | Returned rows across all shards |
| `max_output_bytes` | 64 / 64 MiB | Sum of `{position,ordinal,record}` canonical output wires |
| `max_state_cells` | 10,000 / 100,000 | Retained cells across all shards |
| `max_state_bytes` | 16 / 64 MiB | Sum of `{step,key,encoded_value}` cell wires; encoded state is a quoted canonical JSON string |
| `max_checkpoint_bytes` | 64 / 64 MiB | Actual complete UTF-8 parent snapshot document |
| `max_callback_reservation` | 4,000,000 / 16,000,000 | Admitted input count times Flow's `max_calls_per_input`, conservatively reserved before dispatch |
| Startup / wave timeout | 30 / 3,600 seconds | Monotonic elapsed deadline, checked before acceptance |
| Cleanup timeout | 5 / 30 seconds | One aggregate time budget for owned joins/escalation |

Existing Flow per-input callback, expansion, value and state limits also apply.
All counters remain at most `2**53-1`. Startup serialization has a separate 1 MiB
trusted-pickle cap; each of at most eight ready/error handshake frames is at most
1 KiB. Aggregate request wires are capped by `max_checkpoint_bytes + max_input_bytes`.
Return sequence metadata is count-bounded, not included in the output-wire byte
sum. Multiple retained user batches/checkpoints are outside per-wave budgets.

No resource limit here is a Python/native allocator or RSS sandbox. The standard
JSON decoder materializes a size-admitted wire before object-shape validation;
encoder chunks and temporary copies can allocate before an aggregate rejection.
An input `FlowRecord` is revalidated up to its fixed 8 MiB value ceiling before
the smaller remaining input-wire budget is checked. State admission validates all
fixed cell shapes/encoded bytes before parsing state values. Malicious callback
allocation and caller-provided Python object construction are outside these caps.

Each active worker has one owned I/O thread, so the owner does not block in a
large pipe `send_bytes`/`recv_bytes` before it can check cancellation/deadline.
Payloads and received bytes are bounded before protocol interpretation. Workers
are not silently retried. Cleanup closes owned pipe ends, joins, then escalates
terminate/kill as needed; process, pipe and I/O-thread completion are tracked
separately, and failed closers retain retry ownership. OS refusal is reported,
not represented as a successfully closed worker.

`Process.start`, trusted startup serialization, source callbacks and native OS
close/termination calls are synchronous and are not forcibly interruptible by a
Python deadline. A late completed stage is rejected, but the timeout is not a hard
real-time bound on those operations. Termination does not guarantee a callback's
`finally` ran, kill callback-created descendants, or recover its external effects.
There is no unbounded queue or background source reader.
The startup clock begins after configuration/checkpoint admission and before
trusted serialization. The wave clock includes synchronous input/result admission
and final return construction, not just waiting for the children.

If construction fails after acquiring resources, the raised exception carries
`local_worker_cleanup: LocalWorkerCleanup`. Keep that explicit capability to
inspect `worker_status()` and retry `close()` if cleanup failed. During normal
use the runtime itself retains that ownership. Ordinary cleanup errors add only
a generic note to an existing failure; control-exception priority is preserved.

## Scope and verification

This implements one original ordered source, ingress key co-location and parallel
linear operators. It does not implement multi-source/multi-node execution,
per-operator exchange, dynamic partition migration, distributed epochs, source
connector partition ownership, broker acknowledgement or durable exactly-once
delivery. `MultiGraphRuntime` and all existing journals remain local and their
previous formats are unchanged.

Tests include a real two-PID barrier that a serial dispatcher cannot pass, a
seeded five-operator serial/restart oracle, large duplex frames, actual worker
death, cancellation/control cleanup, malformed wire, global budget rollback and
strict restore. Separate pure transport stubs test transaction/iterator edge
cases and are not counted as proof of parallelism.

Final Windows Python 3.12.13 verification: 1,525 tests passed, with one existing
`generator.close()` return-value case skipped because it requires Python 3.13+.
Warnings were errors; the unchanged 95% coverage gate passed at 97.0156268359%
combined statement/branch coverage. Python 3.14.5 passed all 202 new tests,
including 14 cases that start real child processes and two pre-start rejection
cases, with warnings as errors and no skips. The first
complete run (1,523 passed plus the same skip) is retained separately; two final
regressions then proved malformed Flow limits were rejected before startup.

The real parallel oracle uses seed 1832, 50 inputs, five preserving operators,
three workers and a two-wave process restart. A separate frozen-`767128a` oracle
compared 64 seeded sequences of 24 inputs: all 1,536 complete old v1 checkpoints
and output batches, comprising 4,628 rows, matched. The generated 24-input example
also matched serial output across two separate three-process sessions. These are
local correctness results, not reference-scale throughput or multi-node parity.
