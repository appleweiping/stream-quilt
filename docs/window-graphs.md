# Explicit-watermark windows inside a local DAG

`WindowGraphRuntime` executes ordinary prefix operators, an actual keyed window
fold and downstream graph operators. Prefix state and window ingestion publish
together; a drain publishes all selected window retirements and suffix state
together. This is a distinct, single-source, one-window profile, not an adapter
which feeds committed standalone window rows into a second runtime.

Run `python -I examples/window_graph.py` from an installed checkout. The
[offline executable](../examples/window_graph.py) filters events, selects account
keys, counts seen records, folds actual arrivals, and fans out running totals to
summary and audit terminals. It closes and reloads four temporary checkpoint
files at open, watermark-pending, partial-EOF and closed boundaries. Every output
field, retained state, edge counter and progress counter has a handwritten
expectation. Verification also runs under `python -I -O`.

These example files demonstrate reinstantiation, not atomic source/state/output
storage, crash durability, broker offsets or delivery acknowledgements.

## Construct and operate

```python
from stream_quilt import (
    FlowEdge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    WindowFold,
    WindowGraphDataflow,
    WindowGraphInput,
    WindowGraphRuntime,
)

fold = WindowFold(
    "sum",
    "v1",
    width=10,
    initial=lambda: 0,
    fold=lambda state, value: state + value,
)
flow = WindowGraphDataflow(
    "account-sums",
    "v1",
    (FlowStep("input", "map", lambda value: value), FlowWindow("sum", fold)),
    (FlowEdge("input", "sum"),),
    entry="input",
)
runtime = WindowGraphRuntime(flow)
runtime.process(WindowGraphInput(0, 2, FlowRecord(3, "a")))
runtime.process(WindowGraphInput(1, 1, FlowRecord(7, "a")))
runtime.advance_watermark(10, next_position=2)
batch = runtime.drain(max_windows=1)
print(batch.outputs[0].record.value)  # window bounds/count/value; key is on FlowRecord
runtime.finish(next_position=2)
```

`FlowWindow(step_id, fold)` requires an exact `WindowFold` whose `fold_id` equals
the node ID. `WindowGraphDataflow(flow_id, revision, nodes, edges, *, entry,
limits=WindowGraphLimits())` admits exact tuples of 2..64 nodes and 1..256 edges.
There must be exactly one non-entry window. All other nodes are existing
`FlowStep`, `FlowBranch` and `FlowMerge`; the existing graph topology rules apply.
The window has one incoming edge and must lie on every source-to-terminal path.
Prefix/suffix branching, fanout and edge-ordered merges are supported, but bypass
terminals, multiple sources/windows and join nodes are rejected.

`process(WindowGraphInput(position, timestamp, record))` consumes exactly the next
zero-based source position. Even a filtered record consumes one position on
success. Inputs own an exact `FlowRecord`; the key may initially be absent if the
prefix supplies it. Every record reaching the window must have a valid key.
Timestamps are exact built-in integers, excluding bool, in
`[-(2**53-1), 2**53-1]`. Units are declared by the fold; no clock conversion occurs.
Prefix expansion preserves the source timestamp out of band for every output.

Successful arrival order is fold order, not timestamp order. Original
[window geometry and callback rules](window-folds.md) apply unchanged: exact
half-open membership, explicit gaps, detached JSON values, optional one-row
finalization, finite safe-integer coordinates and bounded state. No callback is
loaded from configuration or a checkpoint; applications supply trusted synchronous
code and increment semantic revisions when its behavior changes.

## Late input, explicit progress and EOF

Late policy is evaluated **at the window**, not at source ingress. With `reject`,
a late window input fails the whole process call, including proposed prefix state.
With `drop`, the prefix's successful state and source position commit, the window
accounts for the drop, and no initial/fold callback runs for that record. A prefix
filter can remove a late source record without creating a window drop. Key and
record-byte admission still precede late/drop handling.

`advance_watermark(timestamp, *, next_position)` must name the current source
prefix and a non-regressing exact integer frontier. Timestamps below the frontier
are late; equality is on time. Progress does not invoke callbacks or emit records.
Eligible retained windows create backpressure; drain them before more source
input or another advance. Equal watermarks are no-ops only in open state.

`finish(*, next_position)` declares permanent EOF, makes every retained window
eligible, preserves the last finite-or-None watermark and invokes no ordinary
EOF callback. It does not erase ordinary keyed state. An empty source operation
is never inferred to mean EOF. Repeated finish with the matching prefix is a no-op.

| State | Meaning | Accepted work |
|---|---|---|
| open | Unfinished, no eligible retained windows | process, advance, finish, empty drain |
| draining | At least one eligible retained window | drain, finish |
| closed | Finished with no retained windows | repeated finish, empty drain |

`status` returns the existing immutable `WindowStatus`. `next_position` and
`operation_sequence` are separate read-only properties. Successful state-changing
process/strict-advance/first-finish/nonempty-drain calls increment the sequence.
Equal progress, repeated finish and empty drain do not. Their valid no-op forms
remain available at saturated lifetime counters; requests still validate their
types, prefix and phase.

## Window-major suffix execution

`drain(*, max_windows=100)` selects a bounded prefix of eligible windows sorted
by `(window index, Unicode key)`. It passes each complete `WindowRow.to_record()`
through the suffix **one row at a time**, using one unpublished outer transaction
for the entire selected batch. Ordinary callback limits and edge/work counters
are not reset for each row. Existing ordinary node output-count/byte limits apply
to each node's batch in a row walk; the shared graph work limit bounds their sum
over the complete drain.

Terminal outputs are window-major: every terminal output for the first row
precedes every terminal output for the second. Within each row, ordinary
declaration/topological and incoming-edge merge ordering is unchanged. This
prevents a diamond merge followed by noncommutative keyed state from changing
fold history when successful drain batches are partitioned differently. Callback
external effects or nondeterministic application code are not made equivalent.

`WindowGraphBatch.outputs` contains owned `GraphOutput` records with terminal ID
and key. Its remaining fields are operation sequence, next source position,
status and per-operation deltas: folded inputs, late-dropped inputs, gap inputs,
membership updates and drained windows. Source processing never emits terminal
records in this profile; advance/finish likewise emit none. A zero-output suffix
may still successfully retire selected windows.

The row adapter adds metadata nesting to the original value. A valid standalone
JSON value at the old depth ceiling can therefore fail conversion to a
`FlowRecord`; the graph does not relax old depth or record limits. Such a failure
retains the complete selected batch for application correction.

## Atomicity, ownership and errors

One owner operates the runtime. Reentrant operations **and reads** are rejected;
this guard is not a thread lock or concurrency guarantee. Proposals, copied state,
all outputs and the returned batch are constructed before the one state-pointer
publication. Failure during a later expanded prefix record, window callback,
finalizer, suffix sibling, quota check or return construction leaves the preceding
checkpoint unchanged. Earlier successful calls remain committed.

This guarantee covers internal retained state and returned outputs. Application
callback side effects, consumed source records and external delivery are outside
it. Callback retries can repeat side effects; there is no preemption, time quota,
transactional file API, persistent request ID or remote exactly-once promise.

Original error boundaries remain visible. Ordinary node/graph-extension failures
carry `FlowExecutionError` context; window callbacks retain
`WindowFoldExecutionError` context as applicable. Preflight, row adaptation and
some shared quota checks raise `ValidationError` directly. Inspect exception
context rather than assuming every drain failure has one wrapper type.
KeyboardInterrupt/SystemExit propagate without ordinary exception wrapping.

A narrow prerequisite also corrects old native output-generator cleanup: when
both the active primary and cleanup are control exceptions, the original primary
object is preserved and cleanup is noted. If an ordinary primary is followed by a
cleanup control, that control still propagates; without a primary, cleanup still
propagates. All ordinary-cleanup note behavior, old wires, scheduler order and
budgets are unchanged. This explicit correction is not a claim of unchanged
error behavior at the previously defective control boundary. Native generators
remain owned under the existing expansion contract; arbitrary user iterators are
not silently given resource ownership.

## Shared bounded resources

`WindowGraphLimits` contains existing `GraphLimits` plus:

| Field | Default | Hard ceiling |
|---|---|---|
| max_state_cells | 10,000 | 100,000 |
| max_state_bytes | 16 MiB | 64 MiB |
| max_source_inputs | 1,000,000,000 | 2**53-1 |

Aggregate cells count ordinary `(node,key)` state and retained `(index,key)`
window state together. Aggregate bytes count the complete canonical cell objects,
including keys, node IDs, window geometry and escaped encoded state text. Existing
ordinary payload and per-value caps, window caps, per-node output caps and graph
work caps also apply; a new larger limit does not relax an old smaller limit.

Every ordinary/window callback shares one per-operation invocation budget.
Known window membership, geometry and cell-count bounds are checked before
window callbacks. All touched old window-cell bytes are reserved away before
admitting replacements, allowing a later shrinking sibling to fund growth.
Drain reserves retirement of all selected cells before suffix state proposals;
an error still rolls back all of them.

At construction, one maximum window row and its immediate fanout must fit the
record, batch and work bounds. Drain selection uses requested count, eligible
count, window row/batch caps, ordinary record/batch caps, immediate fanout work
reservation and finalizer callback capacity. No unselected finalizer is called.
Future selected rows retain their worst-case immediate work reservation while
earlier rows execute. Additional suffix expansion and callbacks can still exhaust
the shared budget: the whole drain fails, and a smaller retry can be appropriate.

Lifetime operation/source/window/edge/output counters are safe integers. The
terminal count is checked on actual output, so a zero-output suffix is not
rejected merely by pessimistically reserving terminal emissions. Limits bound
logical data and work, not total RSS, Python allocations, callback runtime or
malicious in-process mutation.

## Distinct strict checkpoints

`checkpoint()` returns owned `WindowGraphCheckpoint`. `to_dict()` returns a
detached document; `to_json()` returns canonical UTF-8-compatible text.
`from_json(str_or_bytes)` and `from_dict(document)` admit data, and
`WindowGraphRuntime.from_checkpoint(flow, checkpoint)` reinstates it using the
application's matching callbacks/revisions. Open, pending, partial-EOF and closed
states all restore without executing callbacks.

The closed envelope is `{body, sha256}`. Body kind is
`stream-quilt-window-graph-checkpoint`, version `1.0`; configuration has distinct
kind `stream-quilt-window-graph` and explicit arrival/source-preserved/window-major
policies. Body stores normalized configuration and digest, source position/EOF,
operation sequence, watermark/drain/terminal counters, one count per declared
edge, sorted ordinary cells, and the unchanged standalone window checkpoint as
a nested **object**. It is not an escaped checkpoint-document string.

Ordinary and window state values remain encoded canonical JSON text. Parsing
rejects malformed UTF-8, duplicate/unknown fields, nonfinite values, noncanonical
text, unsafe integers and contradictory geometry/counts. Raw cell arrays,
metadata, encoded-text lengths, global bytes and counter consistency are checked
for **both** state sets before either nested state decoder runs. Forged frozen
objects are revalidated. Necessary counter relations include:

- sequence = source inputs + strict watermark advances + first EOF + nonempty drains;
- window processed inputs equal its incoming edge deliveries;
- nonempty drain count and emitted-window count agree with the configured drain cap;
- edge/terminal counts respect their source/drain causes, branch fanout, merge and
  ordinary operator bounds; retained ordinary keys cannot exceed node input history;
- the original standalone window phase, geometry and membership-counter checks.

These are consistency checks, not exhaustive historical reachability. Hashes
detect corruption, not authenticated source history, callback identity, rollback,
forks or external delivery. Never restore an untrusted document expecting its
checksum to provide authenticity.

Configuration is bounded to 512 KiB. Construction reserves at most 1 MiB for the
actual configuration copies, hashes and maximum-width scalar/header tokens.
Checkpoint wire is bounded to 66 MiB. Exact accounting adds the header, complete
canonical cell objects and list commas; escaped state text and both configuration
copies are included. Source ingress and drainage preserve exportability under
these admitted bounds. Standalone, ordinary graph, multi-source and journal wire
formats/acceptance are not changed or implicitly migrated.

## Verification and remaining scope

An independent manual oracle enumerates window indices directly, keeps raw
arrival histories and reconstructs complete configuration, checkpoint bodies,
hashes, counters, terminal order and keyed prefix/suffix state after each
operation. It does not call production membership, scheduling or wire helpers.
Diamond-merge tests compare different successful drain partitions with actual
noncommutative downstream state. Additional tests cover rollback, native control
cleanup, callback/work/cell quotas, hostile checkpoints and publication failures.

The private standalone staging extraction is independently compared with frozen
expected transcripts captured from signed pre-change commit
`58eeb22be2da770f7226ccf778de2099a1ba2bd8`: 24 complete traces, 1,112 operations,
including callback order, errors, output and checkpoint bytes. Expected results
were not generated by the new implementation. Focused source checks are not
full-suite, other-platform, installed-artifact or hosted CI acceptance.

The subsequent [final acceptance record](parity-dataflow.md#final-one-window-graph-acceptance)
separately records the complete Windows suite, Python-version regression and
independent Windows/Linux installed checks. The example and independent installed
oracles preserve their checks under optimized Python; no Linux full-suite or
distributed-runtime result is inferred from those finite runs.

This first integration does not add multiple source/window nodes, window joins,
timestamp-order buffering, merging sessions, clocks/timers/notifications,
generalized window output streams, graph workers, window journals or CLI.
External delivery and distributed progress/recovery also remain open in the
[whole-reference ledger](parity-dataflow.md). Whole-repository functional and
scale parity is not claimed by this increment.
