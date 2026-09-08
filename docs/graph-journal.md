# Durable local graph recovery

`GraphJournal` persists a complete source/state/terminal-output prefix for a
[`GraphDataflow`](branching-dataflows.md). It uses the same private SQLite
transaction engine as [`FlowJournal`](flow-journal.md), not a second SQL
implementation. Distinct application IDs and document kinds prevent accidental
cross-opening of linear, graph and aligner stores.

```python
from stream_quilt import GraphJournal

# graph is explicitly supplied; source_id commits to actual source values,
# order and interpretation. The application owns that commitment.
journal = GraphJournal("graph.sqlite", graph, source_id)
before = journal.latest()
# Reposition a replayable source to before.next_position before forming records.
after = journal.advance(records, expected_generation=before.generation, max_inputs=1000)
```

Run `python examples/durable_branching_totals.py` for a complete offline example.

## Prefix and ordering

`latest()` returns immutable `GraphRecoveryPoint(source_id, generation,
next_position, checkpoint)`. The exact `GraphCheckpoint` binds ordered topology,
explicit semantic revision and every graph/operator limit. Generation zero
must contain no processed inputs, state or outputs. Each nonempty `advance`
increments generation once, even if every input is filtered. Positions count
source inputs, not edge deliveries or outputs. Empty input is a read-only no-op
with another generation check.

Each batch runs on a detached `GraphRuntime` outside the SQLite write lock.
`BEGIN IMMEDIATE` compares the complete earlier recovery point before inserting
any output or replacing the head. A competitor raises `RecoveryConflict`; there
is no callback retry or extra input pull to discover a batch cap. Every sibling's
state and terminal outputs are committed together for the whole batch. Later
branch/input failure cannot publish an earlier proposal prefix.

`outputs(start=0, limit=1000)` yields immutable
`GraphJournalOutput(sequence, source_position, step_id, record)` values.
Sequences are consecutive terminal-record indexes starting at zero; outputs
from one input share its source position. `step_id` must identify a terminal.
Order follows the graph's **topological execution order**, then local terminal
record order, not merely raw node declaration or edge order. Diamond/conditional
behavior retains the documented graph semantics.

A page holds a stable SQLite read snapshot and inspects one bounded predecessor
when `start > 0`. That predecessor is not yielded; validating it detects source
and same-input terminal order reversals across page boundaries. Pages validate
checksums, canonical strict JSON, terminal IDs and record limits row by row.
A later invalid row can raise after earlier valid rows have been yielded. Head
reads check schema/source/graph identity, state ownership/limits and consecutive
output count/index metadata, not all historical output contents. A plausible
rehashed history is not authenticated or independently recomputed from callbacks.

## Failure and ownership

Callback, source, validation, capacity and pre-commit insertion failures retain
the previous durable prefix. Process-death tests terminate a writer after output
insertion and after head replacement, before COMMIT; reopening restores the old
complete prefix. `synchronous=FULL` is used, with durability still subject to
SQLite, filesystem and hardware guarantees.

**Failed COMMIT acknowledgment is not proof of rollback.** COMMIT or post-operation
cleanup errors tell callers to inspect `latest()` before replaying. Tests inject
an error after real COMMIT and confirm the proposed state/outputs already exist.
Never automatically replay from an old cached source offset after such an error.

Connections are operation-local. Rollback and close are both attempted; ordinary
cleanup errors do not replace active `KeyboardInterrupt`/`SystemExit`, and genuine
cleanup controls propagate. Explicitly close page iterators after early stopping;
ordinary close failures remain visible. Rollback-journal readers can delay
writers. If an application enables WAL, pages retain their old snapshot across
new commits; backups must include the corresponding WAL companions.

Source iteration and callbacks occur outside SQL publication. Their files,
network requests, globals and external effects are not rolled back. A failed or
conflicting batch may already have consumed records/executed callbacks. Re-seek
the source from the stored position and use pure or application-idempotent
callbacks. Source commitments and semantic revisions are caller-managed, not
derived from arbitrary iterators, bytecode or closures. No code is imported or
evaluated from checkpoints. No broker acknowledgment, distributed consensus,
external exactly-once or worker-ownership guarantee is implied.

## Shared limits and distinct format

Both journals share limits: 10,000 source inputs and 100,000 outputs per batch,
1,000,000 historical outputs, 64 MiB encoded output payloads per transaction and
64 MiB per encoded head/document. Pages yield at most 10,000 records and inspect
at most one extra predecessor. SQLite type/UTF-8-length guards reject oversized
stored text before Python materialization. These supplement graph-wide work,
callback, record and state limits; they are not RSS, database-file-size or
callback CPU/time quotas. Full history is not materialized on read.

Graph files use application ID `0x5351474A` (`SQGJ`), user version 1 and shared
internal `flow_head`/`flow_output` tables. Head kind is
`stream-quilt-graph-recovery-point`, schema version `1.0`; checkpoint kind remains
`stream-quilt-graph-checkpoint`. Outputs use `stream-quilt-graph-journal-output`
with explicit terminal provenance. Graph and linear databases reject each other
without migration or unrelated-schema mutation. Existing linear `SQFJ` ID,
head/output JSON bytes, checkpoints and API remain unchanged; a frozen canonical
head/output oracle verifies compatibility.

Retention, compaction, multi-source partitions, transactional external sinks,
network-filesystem qualification, migration and distributed recovery remain open.
This increment is not whole-repository Bytewax parity.
