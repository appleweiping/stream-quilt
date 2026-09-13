# Explicit-watermark keyed window folds

`WindowFoldRuntime` is a standalone local runtime which retains actual fold state
per key/window, closes windows through explicit progress, and resumes from strict
portable checkpoints. It does not wrap the offline sessionizer or aligner.

```python
from stream_quilt import FlowRecord, WindowFold, WindowFoldRuntime

spec = WindowFold(
    "account-sums",
    "v1",
    width=10,
    hop=5,
    tick_unit="tick",
    initial=lambda: 0,
    fold=lambda state, value: state + value,
)
runtime = WindowFoldRuntime(spec)
assert runtime.process(2, FlowRecord(3, "a")).memberships == 2
assert runtime.process(4, FlowRecord(7, "a")).memberships == 2
assert runtime.advance_watermark(5).phase == "draining"
row = runtime.drain(max_windows=1).rows[0]
assert (row.index, row.start, row.end, row.input_count, row.value) == (-1, -5, 5, 2, 10)
assert runtime.status.phase == "open"
runtime.finish()
assert runtime.drain().rows[0].value == 10
assert runtime.status.phase == "closed"
```

For a complete offline executable, run `python -I examples/window_folds.py` from
an installed checkout. The [example](../examples/window_folds.py) closes and reloads
four owned temporary checkpoint files at open, watermark-pending, partial-EOF and
closed boundaries, checking every output field and exact counters against
handwritten expectations. It demonstrates runtime reinstantiation, not a broker
offset, consumer acknowledgement, atomic file journal or process-crash guarantee.

## Time, membership and callback order

`WindowFold(fold_id, revision, *, width, initial, fold, hop=None, origin=0,
tick_unit="tick", finalize=None, late_policy="reject", limits=WindowFoldLimits())`
defines one fixed-window fold. IDs, revision, tick-unit labels and record keys
reuse the exact canonical Unicode key contract. Labels declare units; they do
not convert values to milliseconds, timestamps, datetimes or timezones.

Width/hop are positive built-in integers, excluding bool and numeric subclasses.
Omitted hop normalizes to width. Timestamp, watermark, origin, actual window index
and endpoints must be built-in integers in `[-(2**53-1), 2**53-1]`. Width/hop have
that positive ceiling. There is no floating-point arithmetic. An actual member
whose index/start/end is unrepresentable rejects the whole input before callbacks;
membership is never clipped. In particular, a one-tick window at the maximum tick
would require an unrepresentable end and rejects. Negative ticks are supported.

Window index `j` means `[origin + j*hop, origin + j*hop + width)`. Starts are
inclusive; ends are exclusive. Hop equal to width is tumbling; smaller hop gives
overlap. Larger hop explicitly allows gaps, with an accounted `gap` input outcome.
Only windows with an observed contribution are created. No empty windows appear.

Inputs are exact keyed immutable `FlowRecord` values. Their successful call order
is fold order, including duplicate timestamps/values and allowed disorder. The
runtime does **not** reorder by timestamp or buffer events for that purpose.

`initial() -> JSON` runs for each new key/window. Its result is snapshotted before
the folder receives it. `fold(state, value) -> JSON` proposes the next state;
every overlapping window receives separate detached state/input containers.
Optional `finalize(state) -> JSON` converts a closed state into exactly one output.
Without it the state itself is the output. JSON null is valid at every stage,
never an absent-state sentinel or a request to suppress the row.

Callbacks are trusted synchronous application code, not serialized functions.
Known coroutine functions reject at configuration; returned awaitables reject,
closing native coroutines. Arbitrary generator/resource protocols are not consumed
as JSON or automatically owned. Ordinary callback exceptions are wrapped in
`WindowFoldExecutionError` with phase/key/index and the original cause. Control
exceptions such as KeyboardInterrupt/SystemExit preserve their original type.

## Progress, EOF and backpressure

`process(timestamp, record)` returns immutable `WindowProcessResult` with outcome
`folded`, `late_dropped` or `gap`, membership count and resulting status. It emits
no rows. The latter two outcomes have zero memberships and run no callbacks.

The initial watermark is None. Inputs never advance it. Only
`advance_watermark(timestamp)` changes it, monotonically. While open, equality is
a no-op and regression rejects. Input strictly older than the current watermark
is late; equality is on time. Record/key/timestamp/input-byte admission precedes
late classification. `late_policy="reject"` changes no state or counters;
`"drop"` commits input/drop counts only. Late classification precedes gap
classification. There is no late-accept policy or separate late-output stream.

Advancing the watermark invokes no finalizer and emits no row. Retained windows
with `end <= watermark` become eligible. Their state remains owned by the runtime
until successfully drained. The explicit phases are:

| Phase | Meaning | Allowed progress operations |
| --- | --- | --- |
| open | Not finished, no eligible retained windows | process, advance, finish, empty drain |
| draining | One or more eligible retained windows | drain, finish |
| closed | Finished and no retained windows | repeated finish, empty drain |

Checkpoint is allowed in every phase outside an active callback/operation.
Further input and watermark calls, including equal watermark calls, reject during
draining. This barrier is explicit backpressure. After the eligible prefix drains,
an unfinished runtime reopens; future windows may still be retained.

`finish()` explicitly and permanently signals source EOF, making every retained
window eligible. It preserves the last finite-or-None watermark rather than
inventing infinity. Finish may occur during watermark drainage. Repeat finish is
a no-op in every phase; empty finish closes immediately. A finished runtime never
accepts new input, including after restore. Source iteration, exhaustion and
position ownership remain with the caller; there is no source-pulling helper.

`runtime.status` and returned statuses expose phase, watermark, finished,
retained_windows and pending_windows. Eligibility is derived from the retained
cells and watermark/EOF, not a second inconsistent pending queue.

## Whole-window reserved drain

`drain(max_windows=100)` selects a deterministic prefix of eligible windows in
`(window index, Unicode key)` order. For fixed positive hop this is chronological
end order, then key order. It is independent of insertion order. max_windows must
be a built-in integer in 1..100,000, even for an empty no-op drain.

Before any finalizer, the selected count is capped by requested count, pending
count, max_rows_per_batch and `max_batch_bytes // max_row_bytes`. Configuration
requires one maximum row to fit a batch. This reserves worst-case bytes and may
intentionally produce fewer rows than actual small outputs could fit. An
unselected window's finalizer is never speculatively called and then discarded
to fill the preceding batch.

Every selected window emits exactly one immutable `WindowRow`. It exposes key,
index, start, end, tick_unit, input_count and an independently readable JSON value.
`WindowBatch.rows` is an immutable tuple; `drained_windows == len(rows)` and its
status describes the resulting runtime. Empty drain is an unchanged empty batch.

A valid bounded row always fits its reserved allowance, so valid nonempty state
selects at least one window. An oversized or failing finalizer rejects the whole
selected drain: no selected state or counters change. There is no skip, truncation,
deletion, budget escalation or automatic retry. A broken/non-returning finalizer
has **no unconditional progress guarantee**; its state remains retained. The caller
owns repair of transient callback dependencies. Semantic changes require revision
discipline; this lane provides no state migration or discard escape hatch.

For identity finalization, input admission checks the complete projected output
row bytes after every proposed fold. Thus accepted state can fit an identity-output
drain with unchanged configuration. An arbitrary future finalizer cannot receive
that guarantee without prematurely running it.

`row.to_dict()` includes full metadata and value. `row.to_record()` explicitly
preserves the key and puts metadata/value in a regular `FlowRecord`. Conversion
still applies that record's separate aggregate JSON node/depth/byte bounds. A
valid window row can fail this conversion; existing record limits are not relaxed.

## Atomicity and ownership

Each process, watermark, finish and drain operation stages all state/counter/index
changes and its complete return object before one private state-reference swap.
Callback, JSON, byte/count, row, index or return-allocation failure before that
publication leaves the preceding state unchanged. A later selected-window failure
cannot publish an earlier selected window. Previously successful drain calls stay
committed; draining the whole runtime is not one transaction across future calls.

Known membership, geometry, new-key/window and lifetime-counter bounds are checked
before callbacks. Callback result bytes are checked afterwards inside the same
unpublished transaction. All touched old cell costs are removed from projected
accounting before adding proposals, so a later shrinking sibling can fund an
earlier growing sibling without an artificial prefix-budget rejection.

Processing, progress, drain and checkpoint all reject callback reentry. This is a
single-owner synchronous contract, not thread safety or a callback-time sandbox.
Callback globals, external side effects, source reads and returned-row delivery
are outside rollback. In-memory publication is not process-crash durability or
external delivery acknowledgement. Failure after successful publication is not
promised to roll it back.

## Resource profile

Every limit is a positive exact built-in integer and part of identity. Key count
cannot exceed window count; maximum row bytes cannot exceed batch bytes.

| WindowFoldLimits field | Default | Hard ceiling |
| --- | ---: | ---: |
| max_windows_per_input | 64 | 1,024 |
| max_keys | 10,000 | 100,000 |
| max_windows | 10,000 | 100,000 |
| max_input_bytes | 1 MiB | 8 MiB |
| max_state_value_bytes | 1 MiB | 8 MiB |
| max_state_bytes | 16 MiB | 64 MiB |
| max_row_bytes | 1 MiB | 8 MiB |
| max_rows_per_batch | 1,000 | 100,000 |
| max_batch_bytes | 16 MiB | 64 MiB |
| max_inputs | 1,000,000,000 | 2**53 - 1 |

All lifetime counters separately remain within `2**53-1`. Process invokes at most
two callbacks per admitted membership; drain at most one per selected window.
Input/state payloads reuse existing finite interoperable JSON, Unicode,
cycle/depth/node admission. max_state_bytes counts the complete canonical retained
cell wire, including metadata/key/punctuation and escaped encoded-state text.
Both future and eligible windows count toward retained limits. Row bytes count
complete canonical metadata/value wire; batch bytes sum those row wire sizes.
The bounded batch/status envelope is separately count/name bounded.

Aggregate wire admission is incremental. Checkpoint JSON import is capped at
66 MiB, allowing 64 MiB of cells plus bounded header/array/checksum overhead. The
standard JSON parser still materializes that bounded outer document. Old/proposed
indexes, encoded payloads, temporary JSON, output batches and caller-held snapshots
may coexist. Publication copies a bounded retained index; drain can scan/sort all
retained windows. These limits are not total RSS, constant-memory, allocator,
wall-time or reference-throughput guarantees.

## Strict checkpoint and semantic revision

`checkpoint()` returns an owned immutable `WindowCheckpoint`; `to_dict()` and
`to_json()` export it, and strict `from_dict()`/`from_json()` import it. Restore
with `WindowFoldRuntime.from_checkpoint(spec, checkpoint)`. No callback executes
during import or restore. Nested values/dictionary reads are detached.

The new format is an outer `{"body": ..., "sha256": ...}` envelope. Its closed
body has kind `stream-quilt-window-checkpoint`, version `1.0`, normalized
configuration, configuration identity, watermark, finished, phase, counters and
sorted cells. A cell has key, index, start, end, input_count and canonical JSON
state **text**. It is an encoded string, not code, pickle or a function name.

Configuration binds fold ID, explicit revision, geometry, tick unit, arrival-order
mode, late policy, finalizer presence and every limit. Identity is SHA-256 of its
canonical JSON. Application callback semantics must change revision when changed;
function code/closures are neither fingerprinted nor authenticated. The separate
body checksum detects accidental corruption, not honest state/history or source
authenticity. Checkpoints are local counts, not broker offsets or delivery receipts.

Imports require exact built-in closed shapes, sorted unique cells, canonical
UTF-8 JSON, bounded integer tokens, finite numbers and no duplicate/unknown fields.
Outer byte bounds precede JSON parsing. Aggregate cell/text/geometry/counter/phase
admission precedes nested state decoding. Supplied limits and identity-output
drainability are rechecked even when identities are recomputed on hostile data.
Lowering limits normally changes identity and rejects restore; a forged matching
identity cannot bypass capacity checks. Forged frozen instances are not trusted.

Stored counters are processed_inputs P, late_drops L, gap_inputs G,
membership_updates A, created_windows C, emitted_windows E and
finalized_memberships D. Let F=P-L-G, N=retained cells and R=sum(retained counts).
Necessary invariants include:

- `0 <= L+G <= P <= max_inputs`; reject policy requires L=0; width>=hop requires G=0.
- With q=width//hop, r=width%hop, lo=max(1,q),
  hi=min(max_windows_per_input,max(1,q+(r!=0))), require `F*lo <= A <= F*hi`.
- `C=N+E`, `A=R+D`, `C<=A`, and C=0 iff A=0.
- Every retained count is in 1..F and `E <= D <= E*F`.
- Eligibility is exactly EOF or end<=finite watermark. Open has no eligible
  windows and is unfinished; draining has eligible windows; closed is finished
  and empty. Consequently closed state has C=E and A=D.

Redundant geometry, key/window/cell-byte capacities and phase are recomputed.
These are necessary consistency checks, not exhaustive reachability or
authentication of omitted arrivals and historical watermark monotonicity.

## Reference boundary and verification approach

The frozen [Bytewax window/fold source](https://github.com/bytewax/bytewax/blob/9fce5b6ee43780329b05a2ecc1057ffddd51255d/pysrc/bytewax/operators/windowing.py)
is broader. Its default fold order is timestamp order; this runtime is explicitly
arrival ordered. Its SlidingWindower rejects gaps; this runtime accounts for them
as an original extension. Its event clock includes elapsed system time; this
runtime advances only through explicit calls. No upstream implementation was copied.

The focused oracle enumerates a fixed bounded index domain and tests half-open
intersection directly, without importing the production floor-range helper. It
retains raw arrival lists and independently reconstructs folds, row order, statuses,
complete checkpoint bodies/checksums and counters across restore after every
operation. Separate tests exercise pre-callback admission, reserved drainage,
container isolation, hostile wire, control failures and allocation/publication
boundaries. Source test evidence is not packaged or full-suite evidence.

Timestamp-order buffering, sessions/merging, system/event clock parity,
notifications, generalized window outputs, event-time joins, graph/worker/journal
integration, window-fold CLI, external delivery and distributed progress/recovery remain open in
the [whole-reference ledger](parity-dataflow.md). Existing runtime and journal
wires are unchanged. The explicit FlowRecord adapter does not close those gaps.
