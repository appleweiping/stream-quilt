# Bounded branching dataflows

`GraphDataflow` adds actual local DAG execution to the existing JSON operator
engine. `GraphRuntime.process(FlowRecord(...))` executes the graph and returns
immutable `GraphOutput(step_id, record)` values from terminal nodes. There are
no threads, distributed workers, hidden sinks or callback imports from a CLI.

Run the complete offline example, including JSON checkpoint restoration:

```bash
python examples/branching_totals.py
```

## Declare and inspect a graph

```python
from stream_quilt import FlowEdge, FlowMerge, FlowRecord, FlowStep, GraphDataflow, GraphRuntime

flow = GraphDataflow(
    "two-views",
    "v1",
    (
        FlowStep("input", "map", lambda value: value),
        FlowStep("double", "map", lambda value: value * 2),
        FlowStep("offset", "map", lambda value: value + 10),
        FlowMerge("merged"),
    ),
    (
        FlowEdge("input", "double"),
        FlowEdge("input", "offset"),
        FlowEdge("offset", "merged"),
        FlowEdge("double", "merged"),
    ),
    entry="input",
)
assert [output.record.value for output in GraphRuntime(flow).process(FlowRecord(3))] == [13, 6]
```

Graphs contain 1–64 nodes and at most 256 edges. An explicit `entry` names the
single source-admission node. Every node must be reachable from it. Validation
rejects cycles, missing endpoints, duplicate node IDs and duplicate complete
edges before callbacks run. A `FlowStep` or `FlowBranch` has exactly one incoming
edge, except the entry, which has none. Only `FlowMerge` accepts multiple inputs;
it requires at least two incoming edges. All nodes without outgoing edges are
terminal outputs. Use `filter` to intentionally discard a branch.

`flow.execution_order` exposes the execution order. `flow.to_dict()` describes
the topology, operator kinds and limits without serializing callback code. It
is an inspection document, not a callable loader or authenticated certificate.

## Fanout, branch and merge semantics

Ordinary outgoing `FlowEdge(source, target)` edges deliver every emitted record
to each target. A diamond intentionally delivers two copies to its merge; it
does not deduplicate values or pretend that both paths are one delivery.

`FlowBranch(step_id, predicate)` evaluates its synchronous predicate **once per
incoming record**, using an isolated value copy. The result must be exactly
`True` or `False`; truthy integers and awaitables fail. Its outgoing edges must
use `route=True` or `route=False`, and both routes must be connected. Each route
can itself fan out. Keys and values are preserved, even if a predicate mutates
its copy. True and false edges from the same branch into one merge are distinct
edges, not duplicates.

Ordering is logical and deterministic, not completion-time or event-time order:

1. Among currently ready nodes, execute the earliest declared node. Process its
   complete input batch before selecting the next ready node.
2. Deliver each node's emitted records once per matching outgoing edge, in
   output order. No edge is implicitly replayed or retried.
3. A merge concatenates the batches of its incoming edges **in edge declaration
   order**, preserving order within each batch. This can differ from upstream
   node execution order and can regroup records that took different routes.
4. Return terminal node outputs in node execution order, then their local record
   order. `GraphOutput.step_id` identifies the terminal.

A filtered/empty branch delivers an empty batch. Merge waits for all of the
current source input's predecessor batches, not for a later source input. It
is ordered union, not zip, a join, a timestamp sort or a cross-input buffer.
“Once per edge” describes this local evaluation only: no distributed
exactly-once delivery or external side-effect guarantee is implied.

## One transaction across all siblings

Graph and linear schedulers share the same operator transaction engine and
`FlowStep` contracts. Every node uses one common staged state transaction.
State remains indexed by `(step_id, record.key)`, isolating sibling operators
and different keys. A downstream stateful node after a merge intentionally sees
all preceding proposals for that key in merge order. Initializers, deletion,
suppressed output and isolated JSON state follow [the operator contracts](dataflow.md).

State, processed-input counters, emitted-output counters and returned terminal
outputs commit only after **all** siblings and output contracts succeed. A later
branch error discards earlier terminal output candidates and state proposals.
Ordinary errors report `FlowExecutionError.step_id`; callback details are not
included in that error. A control exception such as `SystemExit` propagates but
still discards proposals and clears the reentrancy guard.

This is internal in-memory atomicity, not a transaction around user callbacks'
files, network requests, globals, source cursors or external sinks. Retrying a
failed input is the caller's decision; callbacks may have already caused such
effects. No callback is automatically retried. The single-owner runtime rejects
reentrant `process()` and checkpoint calls; it does not support concurrent owners
or provide a callback CPU/time/memory sandbox.

Before publication, the engine stages a shallow copy of the retained state
index, so a failed index allocation/update cannot partly mutate retained state.
This costs O(retained keys + touched keys) per source input and temporarily
retains both indexes; immutable JSON payload strings are shared. Result tuples
and counters are also prepared before publication. This is not process-death
recovery or an operating-system shutdown guarantee.

`run(source, max_inputs=N)` retains linear pull semantics: no extra record is
read to discover the cap, and an input's outputs precede the next source pull.
Early iterator close stops new pulls, but the last input's entire state and all
output candidates have already committed. Keep the source iterator to continue.

## Whole-graph limits

`GraphLimits` adds `max_work_records` (default 10,000; maximum 1,000,000) and
`max_work_bytes` (default 64 MiB; maximum 256 MiB). These are **aggregate per
source input**, not a separate allowance for every branch. One unit and that
record's encoded UTF-8 JSON value bytes are charged for each of:

- source-record admission;
- every node emission, including branch/merge forwarding;
- every edge delivery, including each copy caused by fanout.

Terminal outputs have already paid their node-emission charge. Empty batches
cost no record units. A one-record root with two terminal map children consumes
six units: one admission, three node emissions and two edge deliveries. A
budget of five fails atomically. Unicode charges use bytes, not characters.
Expansion is checked during emission; even an infinite generator is stopped
and closed when the shared budget is exceeded.

`GraphLimits.operator_limits` is a `FlowLimits`. Its callback invocation budget
(including branch predicates and state initializers), retained state-key count
and state-value byte limits are shared across the **whole graph**. Record-byte
and per-operator expansion/batch-byte checks still apply to `FlowStep` nodes.
Branch and merge forwarding is bounded by the global work budget, not a fresh
per-node expansion allowance. Thus merging individually valid batches cannot
escape the aggregate bound.

These encoded-value bounds exclude key/step metadata, Python allocation
overhead, callback-owned objects and external allocations. Immutable edge
batches may share record snapshots; reads decode fresh copies. Pending batches,
output candidates, proposed/current state and temporary decoding allocations
can coexist. Work bytes are an auditable accounting bound, not a promise about
process RSS. Callbacks remain trusted code and cannot be forcibly preempted.

## Portable graph checkpoints and compatibility

`GraphRuntime.checkpoint()` returns immutable `GraphCheckpoint`. Its strict JSON
kind is `stream-quilt-graph-checkpoint`, schema version `1.0`. Use
`GraphCheckpoint.from_dict()` then `GraphRuntime.from_checkpoint(flow, checkpoint)`.
The digest binds graph ID, explicit semantic revision, entry, ordered node IDs
and kinds, ordered edge endpoints/routes and every limit. Restoration validates
the digest, canonical state cells, stateful node ownership, counters and limits.
Unknown fields, duplicate cells and incompatible topology/revision are rejected.

The revision is caller-managed: callback bytecode, closure/global values and
dependencies are not fingerprinted. Change it when semantics change. A digest
is not writer authentication; the graph object and checkpoint must come from
trusted application storage. A checkpoint does not include source position or
durably acknowledged outputs.

Existing `Dataflow`, `FlowRuntime` and their linear checkpoint identity/JSON
remain unchanged. `FlowJournal` currently accepts **linear Dataflow only**, and
rejects `GraphDataflow` before opening/creating a store. Graph checkpoints have
a different kind and cannot silently restore into a linear runtime. Durable DAG
source/state/output publication requires a separately designed journal extension;
it is not claimed by this implementation.

## Verification and remaining scope

`tests/test_branching.py` specifies manual diamond, route-grouping and keyed-state
oracles; repeated restore is compared with independent running sums. It checks
late-sibling rollback, immutable input/sibling boundaries, exact fanout budgets,
infinite expansion cleanup, shared state/call limits, no-extra-pull behavior,
callback rejection, counter exhaustion and malformed topology/checkpoints.
All existing linear and journal tests also run against the extracted engine.

Graph journal integration, typed edge schemas, incremental joins/windows,
notifications, distributed workers/epochs, connectors and throughput/memory
evidence remain open. This is not whole-repository Bytewax parity; see the
[remaining repository contracts](parity-dataflow.md).
