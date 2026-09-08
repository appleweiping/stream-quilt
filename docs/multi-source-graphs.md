# Multi-source local graphs

`MultiGraphDataflow` connects multiple explicitly tagged local sources to the
existing map/filter/flat-map/key/stateful/branch/merge operators, keyed join
nodes, and downstream fanout. `MultiGraphRuntime` is a single-owner synchronous
runtime. It reuses the operator transaction, deterministic graph walker and
join transition implementations; it is not an alias for offline `join_streams`.

Run `python examples/multi_source_orders.py` for an offline order/payment join,
portable checkpoint restart, ordinary keyed state and two terminal outputs.
The example has no network or external delivery dependency.

## Declaration and ordering

```python
from stream_quilt import (
    FlowEntry,
    FlowJoin,
    FlowRecord,
    FlowStep,
    GraphInput,
    JoinEdge,
    KeyedJoin,
    MultiGraphDataflow,
    MultiGraphRuntime,
)

flow = MultiGraphDataflow(
    "matches",
    "1",
    (
        FlowStep("left", "map", lambda value: value),
        FlowStep("right", "map", lambda value: value),
        FlowJoin("join", KeyedJoin("join", "1", ("l", "r"))),
    ),
    (JoinEdge("left", "join", "l"), JoinEdge("right", "join", "r")),
    (FlowEntry("a", "left"), FlowEntry("b", "right")),
)
runtime = MultiGraphRuntime(flow)
assert not runtime.process(GraphInput("a", 0, FlowRecord(3, "key"))).outputs
batch = runtime.process(GraphInput("b", 0, FlowRecord(None, "key")))
assert batch.outputs[0].record.value == {
    "present": [True, True],
    "values": [3, None],
}
```

Sources bind distinct ordinary root nodes. Every non-root ordinary node has
one incoming edge; `FlowMerge` accepts at least two. Each `FlowJoin` has one
producer `JoinEdge` for each declared side. To combine several producers into
one side, insert a `FlowMerge` first. A branch-to-join edge specifies both its
boolean `route` and its `side`. Ordinary targets use the existing `FlowEdge`.
`FlowJoin.step_id` must equal `KeyedJoin.join_id`.

All nodes must be reachable from the source-entry union, and cycles are rejected
without running callbacks. At most 16 sources, 64 nodes, 256 edges and 16 joins
are admitted. Join sides retain their existing 2..16 bound.

The caller's tagged input order is semantic. A source position must equal that
source's exact next local position, initially zero; duplicate, skipped and
out-of-order positions are rejected before callbacks. These positions are not
authenticated broker offsets, partition ownership or delivery acknowledgements.
Changing future cross-source interleaving can change first/last/running results.

The earliest declared ready node runs first. A node consumes edge batches in
edge declaration order; merge does not deduplicate. Terminal outputs appear in
node execution order, preserving each node's record order. All nine
first/last/product × complete/final/running combinations have the existing
[keyed-join semantics](keyed-joins.md). Join output explicitly becomes a keyed
`FlowRecord` containing `present` and `values`; missing and present JSON null
remain distinguishable. That conversion obeys FlowRecord's aggregate JSON
depth/node/value limits and the configured graph record-byte limit. A valid
standalone JoinRow is not guaranteed to fit those additional graph limits.

## Atomic operations and callback ownership

Every `process`, nonduplicate `close`, or effective `drain` stages:

- ordinary per-step/per-key state and all join transitions;
- source positions and EOF, edge-delivery counts and operation/output counters;
- all fanout/downstream outputs and the immutable returned `MultiGraphBatch`.

Only after all work and allocations succeed does the runtime replace one outer
state reference. An exception in a downstream node or sibling, a resource
rejection, a control exception or return/state allocation failure leaves the
prior checkpoint unchanged. Multiple updates to one key in the same operation
see earlier staged updates. Counters are bounded interoperable integers.

Callbacks are trusted synchronous Python, not a time/process sandbox. Callback
external side effects and source iterator consumption cannot be rolled back.
There is no automatic callback retry. Ordinary callback/value failures use
`FlowExecutionError(step_id)`; genuine control exceptions propagate. Native
generator/observed coroutine cleanup follows the existing
[shared expansion contract](stateful-expansion.md); arbitrary custom iterator
resources remain caller-owned. Reentrant process/close/drain/checkpoint/run is
rejected. State properties read inside a callback describe committed state.

## EOF and bounded drainage

`close(source_id, next_position=...)` is the only source EOF signal. Repeating
the exact close position performs no callbacks and does not advance operation
history. A different position still fails. Source iterator exhaustion, a run
cap, an empty operation batch and closing the output iterator do not imply EOF.
`run` borrows its iterable, pulls no more than `max_inputs`, does not look ahead
and never closes the caller's iterator itself.

Source EOF propagates through both branch routes, including unused routes.
Merge waits for all producers. A join side closes only after its producer's
output frontier ends. A final join with retained keys stays `draining`; its
output frontier does not end until its final rows have reached downstream nodes.
Ordinary keyed state is not implicitly deleted on EOF.

`drain(max_keys=100)` selects one earliest-topological ready final join. It
removes a bounded number of whole keys in sorted order, respecting that join's
row/byte batch limits and all graph limits. Rows pass through the same downstream
operators before the selected join's final EOF propagates. Thus a downstream
final join becomes ready only after its upstream final join's last rows arrive.
Draining a ready join is allowed while unrelated external sources remain open.

No ready join means a stable empty batch with no operation-history change.
`ready_joins` is a deterministic tuple; `phase` is `open` while any source is
open, `draining` after all sources close with pending joins, otherwise `closed`.
Repeat drain until no ready joins remain. No implicit partial-key emission or
adaptive callback retries occur. A key fitting a standalone join's batch can
still fail downstream graph limits; failure preserves the draining state and
does not promise that arbitrary user callbacks can eventually drain successfully.

## Resource accounting

`MultiGraphLimits.graph` contains the existing `GraphLimits` and `FlowLimits`.
One work record and its encoded VALUE bytes are charged for input admission,
every node emission and every edge delivery. Keys/metadata are not record-value
bytes. Fanout pays for each edge. The callback budget is shared by the entire
operation, not reset at each node. Cartesian row count/value bytes and immediate
fanout are checked before materializing join rows. Owed fanout is reserved so a
later join transition cannot reuse the same remaining work budget.

| Additional retained-state bound | Default | Hard maximum |
|---|---:|---:|
| Ordinary plus join cells | 10,000 | 100,000 |
| Retained join values | 100,000 | 1,000,000 |
| Summed canonical cell-wire bytes | 16 MiB | 64 MiB |

These bounds apply across all nodes and at each intermediate state proposal,
not just the final retained state. Existing ordinary-state and per-join limits
also apply; a tighter graph aggregate limit can reject an otherwise valid join.
Canonical cell-wire byte cost is the UTF-8 length of compact, non-ASCII-escaped
JSON, summed independently per cell:

```text
ordinary: {"step": STEP, "key": KEY, "value": ENCODED_JSON_STRING}
join:     {"step": STEP, "cell": {"key": KEY, "values": [[ENCODED_JSON_STRING], ...]}}
```

This includes IDs, punctuation and escaping of the encoded JSON strings. It
does not claim to measure Python object overhead, caller-retained outputs or
native allocator RSS. State indexes are copied for staged transitions, sorted
for checkpoints/drains and shared only through immutable encoded payloads;
this is bounded retained-state execution, not O(1) memory or work. EOF control
propagation is a bounded topological pass over at most 64 nodes/256 edges.

## Checkpoint identity and admission

`MultiGraphCheckpoint` uses kind `stream-quilt-multisource-graph-checkpoint`,
version `1.0`. This is separate from all existing v1 flow/graph/join wire kinds;
there is no silent old-state upgrade. It contains ordered source next positions
and EOF, sorted ordinary encoded cells, every join checkpoint (including empty
joins), declaration-ordered edge counters, operation sequence and terminal
output count. It contains actual application values; it is not a privacy-redacted
receipt or authenticated execution certificate.

Identity binds source-to-entry mapping, graph node/edge/port/route declaration
order, join identities/modes/limits, all graph limits and caller-managed revision.
Callback code is not hashed or serialized. Restore accepts explicit trusted
flow objects and never imports callback code from checkpoint strings.

Fixed shape, count and cumulative byte admission precede nested encoded-value
parsing. Unknown fields, duplicate JSON keys, nonfinite values, invalid Unicode,
noncanonical encoded state and unsorted/duplicate state keys are rejected.
`from_json` caps input at 66 MiB; the standard JSON parser first materializes
that bounded envelope before the fixed-shape admission pass. This is not an
incremental JSON parser or allocator sandbox.

Restore revalidates the existing join mode-specific invariants, configured state
limits and exact source/node/side identities. Join processed-side counters must
equal their producer edges' cumulative delivery counts, not source input counts:
flat-map and fanout can change cardinality. EOF frontiers are reconstructed
topologically and checked against each join's closed sides. Operation/drain/work
counters enforce necessary consistency bounds, not proof of every possible
hidden history. Checkpoint callers remain responsible for trustworthy storage.

## Deliberately separate persistence boundary

`GraphJournal` remains the single-entry v1 journal. Its scalar source-position
and generation rules do not describe EOF/drain output without a new source
record. It does not accept this runtime or checkpoint. The separate
[MultiGraphJournal](multi-source-journal.md) supplies a versioned operation log,
vector positions, persistent request receipts and atomic CAS publication of
complete state and outputs. Neither this API nor that local journal provides
broker acknowledgement, external sink atomicity,
distributed epochs, partition migration, event-time join windows or watermarks.

## Verification evidence

The new tests include a separately implemented list-based oracle for all nine
join modes under three seeded arrival sequences, JSON checkpoint/restore after
every input/EOF/drain, nested final joins, empty branch EOF and late-closing
merge producers. Boundary tests cover cross-join/ordinary/sibling rollback,
control exceptions, pre-materialization Cartesian/fanout admission, summed wire
costs and malformed checkpoints. A read-only comparison against the published
`75926503e50a7a5bcaa3fd3f8b18f463719482d7` modules matched 1,200 complete v1
output/checkpoint results across 100 seeded graphs; all 100 identities matched.
The final Windows Python 3.12.13 whole-suite gate passed 1,182 tests in
327.92 seconds, with one existing Python-3.13+-only generator-close test skipped.
Combined statement/branch coverage was 97.47% against the unchanged 95% gate;
the new checkpoint module covered all 201 statements and 96 branches, and the
new graph module reached 99.15% over 428 statements and 162 branches. The 179
new focused cases also passed on Python 3.14.5 in 24.84 seconds. Both runtime
and resource warnings were errors. These are local Windows checks; they do not
claim that this increment's full Linux suite or hosted CI has already run.

This increment narrows the local multi-source graph gap. It does not close the
[whole-reference repository ledger](parity-dataflow.md).
