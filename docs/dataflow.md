# Incremental local dataflow

`Dataflow` builds a bounded **linear** operator graph over isolated JSON values.
`FlowRuntime.process(record)` actually executes the configured callbacks. It
does not simulate work or silently spawn distributed workers.

```python
from stream_quilt import Dataflow, FlowRecord, FlowRuntime, FlowStep, StateUpdate

flow = Dataflow(
    "totals",
    "v1",
    (
        FlowStep("key", "key_by", lambda value: value["account"]),
        FlowStep(
            "sum",
            "stateful_map",
            lambda value, state: StateUpdate(state + value["amount"], state + value["amount"]),
            lambda: 0,
        ),
    ),
)
runtime = FlowRuntime(flow)
assert runtime.process(FlowRecord({"account": "a", "amount": 2}))[0].value == 2
assert runtime.process(FlowRecord({"account": "a", "amount": 3}))[0].value == 5
```

## Operator contracts

| Kind | Callback contract | Output/state behavior |
|---|---|---|
| `map` | `function(value) -> JSON` | Replace the value, preserve key |
| `filter` | `function(value) -> bool` | Preserve or drop; truthy non-booleans are errors |
| `flat_map` | `function(value) -> iterable[JSON]` | Ordered bounded expansion; strings/bytes/mappings are not output iterables |
| `key_by` | `function(value) -> str` | Set an exact canonical nonempty key, preserve original value |
| `drop_key` | No callback | Remove key, preserve value |
| `stateful_map` | `function(value, state) -> StateUpdate`; `initial() -> JSON` | Per-step/per-key state with explicit emit/retain choices |
| `stateful_flat_map` | `function(value, state) -> StateFlatUpdate`; `initial() -> JSON` | Per-key state plus ordered zero-to-many outputs, with explicit deletion |

Keys are whitespace-trimmed Unicode scalar strings without control characters.
Noncanonical keys are rejected rather than silently merged after normalization.
State keys are `(step_id, record.key)`, so independent steps never share state
accidentally. A keyed operator rejects an unkeyed record. Initializers run on
the first value for a key and again after explicit deletion; their containers
are copied before callbacks can mutate them.

`StateUpdate(state, output, emit=True, retain=True)` is a proposal. `None` is a
valid JSON state value; `retain=False` deletes state. `emit=False` suppresses an
output without undoing the proposed state update. Input and retained state are
isolated JSON snapshots, and `record.value` returns a fresh copy. JSON numeric
values must be finite; integers stay within the interoperable safe range.

[`StateFlatUpdate(state, outputs, retain=True)`](stateful-expansion.md) extends
the same state contract to an iterable of outputs. The proposed state is captured
before entering that iterable; subsequent generator mutations cannot alter it.
An empty iterable still commits a valid state proposal. Each yielded value is
independently snapshotted and passes the existing output budgets.

## Atomicity and backpressure

All internal state proposals from one input commit **only after every operator
and final output passes its contract**. A downstream exception, oversized
flat-map, malformed value, or capacity failure rolls back that input's internal
state and counters. Several records produced by one flat-map see earlier
proposals for the same key within that input. A dropped output can still commit
earlier state updates, as in a counter followed by a filter.

`run(source_iterator, max_inputs=N)` pulls at most N inputs. It consumes the
current input's returned outputs before pulling another source item. It never
reads an extra source record just to discover the cap. Keep the source iterator
to continue in a later call; reaching the cap is not evidence of source EOF.
Closing the output iterator early stops new input pulls, but the last processed
input already committed **all** its internal state. Its not-yet-consumed outputs
are not durably acknowledged anywhere.

The runtime is single-owner and rejects callback reentrancy. It does not make
concurrent calls safe or preempt callbacks. Callables, their global state, and
external side effects remain trusted application code. Only the runtime's own
state is rolled back. A failing `run()` may already have pulled its failing
source item; use a replayable source and the checkpoint's input count to retry.

## Resource limits

`FlowLimits` bounds callback invocations and output-record count per input,
UTF-8 bytes per record, aggregate output-value bytes per operator stage, bytes
per state value, total retained state-value bytes and retained key count.
Initializers count as invocations. Infinite expansion generators are closed at
the record limit. Return strings from callbacks have already been allocated by
trusted callback code before these checks; there is no general Python allocator
sandbox. JSON node/depth/Unicode bounds also apply through the existing event
validation boundary.

Encoded payload-byte limits do not include Python object/allocator overhead or
key/step metadata. Current/next output batches, original/proposed state and
temporary JSON snapshots may coexist. Their count/individual-byte bounds remain
separate; a `max_state_bytes` value is not a cap on total process RSS.

Publication stages a shallow copy of the retained state index before swapping it
into the runtime. This costs O(retained keys + touched keys) per source input;
immutable encoded values are shared, not deep-copied. A failure while allocating
or updating that index leaves the previous index and counters unchanged. This
does not make the in-memory runtime resilient to process death or OS shutdown.

## Checkpoints and semantic revision

`runtime.checkpoint()` returns an immutable `FlowCheckpoint`; `to_dict()` and
strict `from_dict()` support portable JSON. Restore with
`FlowRuntime.from_checkpoint(flow, checkpoint)`. Restoration validates all
state keys, values, counters and configured limits before returning a runtime.
`examples/keyed_totals.py` demonstrates a JSON round-trip and independently
specified running totals without network access or repository writes.

The identity binds flow ID, revision, ordered step IDs/operator kinds and all
limits. It **does not fingerprint callback code or closure data**. The caller
must change the explicit revision when callback semantics change. Checkpoints
are neither authentication proofs nor snapshots of external Python globals.

The optional [FlowJournal](flow-journal.md) commits generalized flow state,
source record offsets and local outputs in one SQLite transaction. `FlowRuntime`
itself remains in-memory. The existing `RecoveryStore` and `resume` CLI remain
specific to `WatermarkAligner`; they do not implicitly commit this flow's state
or outputs. The separate [GraphDataflow API](branching-dataflows.md) adds bounded
branch/merge topology with one internal transaction across siblings; its new
checkpoint kind is separate from FlowJournal and is durably recovered through
[GraphJournal](graph-journal.md). Notifications, general window operators,
partitioned connectors and distributed execution remain open.
