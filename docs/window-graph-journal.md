# Durable local window graph journal

`WindowGraphJournal` publishes a single `WindowGraphRuntime`'s source position,
explicit watermark/EOF, complete ordinary and window checkpoint, effective
operations, and terminal outputs in one local SQLite transaction. It is a
separate SQWJ database format; existing `FlowJournal`, `GraphJournal` and
`MultiGraphJournal` databases and checkpoint wires are not migrated.

```python
from stream_quilt import (
    WindowGraphDrain,
    WindowGraphInput,
    WindowGraphJournal,
    WindowGraphRequest,
    WindowGraphWatermark,
)

# flow is an existing WindowGraphDataflow; source_id and source_commitment
# are caller-computed lowercase SHA-256 digests. The journal does not poll or
# hash the external source itself.
journal = WindowGraphJournal(path, flow, source_id, source_commitment)
point = journal.latest()
request = WindowGraphRequest(
    point.journal_id,
    "1" * 32,
    point.generation,
    (WindowGraphInput(0, 2, record), WindowGraphWatermark(10, 1), WindowGraphDrain(1)),
)
# Persist request.to_json() outside the journal until its result is known.
receipt = journal.apply(request)
cursor = journal.output_cursor()
page = journal.read_outputs(cursor, limit=100)
```

Run `python -I examples/durable_window_graph.py` from an installed checkout
for an offline request/reopen example with a caller-computed source commitment,
lost-response reconciliation, a fixed output prefix, and an all-no-op request.
`-O` must not remove its checks.

## Command and receipt semantics

The exact ordered request tuple may contain `WindowGraphInput(position,
timestamp, record)`, `WindowGraphWatermark(timestamp, next_position)`,
`WindowGraphFinish(next_position)` and `WindowGraphDrain(max_windows)`. There is
no inferred event time or implicit EOF. `process`, a strictly advancing
watermark, first finish, and nonempty drain each append one effective operation.
Equal watermark, repeated finish and empty drain are no-ops. The original
zero-based command index is retained on every effective operation so no-op
gaps are observable. A request with one or more effective operations adds one
durable generation; all its state and output rows commit together.

`WindowGraphRequest` binds the journal lineage, unique request ID, original
expected generation, and every command in canonical JSON. `apply` checks for a
previously committed request ID before executing callbacks, including for an
all-no-op request. The same ID and exact content returns its stored receipt;
changed content or original generation raises `RecoveryConflict`. `request(id)`
returns a persisted receipt or an absence observed at that read snapshot. The
receipt records before/after generation, operation/source/EOF/watermark/drain
counters, head digests and half-open output ranges, but not output records.

An all-no-op request performs no durable write or ID reservation. Its returned
`no_op` receipt is only a transient observation. If another writer advances the
head, replaying that old request conflicts; a caller must build fresh intent
against the new head. Repeated empty requests behave the same way.

Callbacks run on a detached runtime before the SQLite write lock. Publication
uses full-head compare-and-swap. A competing writer with different content
receives `RecoveryConflict`, with no automatic callback replay. Two writers
may execute callbacks for the same request before one commits; the loser may
then obtain the winner's identical receipt. Thus durable publication
idempotence is **not** callback or external side-effect exactly-once execution.

## Output and restart ownership

`latest()` validates the current checkpoint, history row counts and sequence
continuity, and the newest adjacent receipts. It does not scan every older
receipt or output row. Reopen with the same flow configuration/revision, source
ID, source commitment, and path;
the application must keep callback semantics in sync with its declared
revision. `operation(sequence)` inspects durable effective cause metadata.
`output_cursor(start=0)` fixes a retained generation and output stop.
`read_outputs(cursor, limit=..., max_bytes=...)` returns a detached page and
new cursor without acknowledging any external sink. Later commits cannot
extend an old cursor's stop. Output rows contain a global zero-based sequence,
owning operation sequence, terminal step ID and immutable full `FlowRecord`.
Their order is the runtime's window-major suffix emission order.

After an exception around COMMIT or connection close, do **not** assume
rollback. Reopen and query `request(original_id)` and `latest()` before any
replay, using the exact original request bytes. SQLite's local filesystem
transaction is the only atomicity claim: source reads, broker offsets,
callbacks, sink delivery and downstream acknowledgements are outside it.
Hashes and checksums detect accidental corruption; they do not authenticate
history or prove callback/source identity. The journal does not coordinate
workers, periodic snapshots, rescaling, connector acknowledgements, or
distributed transactions.

## Bounded format

The distinct four-table SQWJ schema stores one complete head, committed
request receipts, effective operations and terminal outputs. Requests have at
most 1,000 commands and 16 MiB canonical wire bytes. The complete head is
bounded to 67 MiB, each receipt to 256 KiB, each operation to 16 KiB, each
output row to 9 MiB, and one transaction to 100,000 outputs / 64 MiB output
bytes. Retained operation/output history is capped at one million rows each.
Output pages admit at most 1,000 records and a caller-selected byte budget
(default 16 MiB). SQL columns are length-gated before Python materialization.
These are acceptance ceilings, not assurances about peak process memory,
callback runtime, arbitrary external code or untrusted storage authentication.
