# Durable local dataflow journals

`FlowJournal` connects the [generalized local operator runtime](dataflow.md) to
a real SQLite transaction. Its head contains the next source record offset and
the complete keyed state checkpoint; the same transaction appends all output
records from that input batch. Restarting reads the previously committed prefix,
not an inferred cursor from a partially written output file.

```python
from stream_quilt import FlowJournal, FlowRecord

# flow is an explicitly configured Dataflow; source_id is a lowercase SHA-256
# commitment supplied by the application for its source contents and layout.
journal = FlowJournal("run.sqlite", flow, source_id)
point = journal.latest()

# Reposition a replayable source to point.next_position before forming records.
# Each FlowRecord is one source record, whether its operators emit zero or many.
next_point = journal.advance(records, expected_generation=point.generation, max_inputs=1000)
```

`examples/durable_keyed_totals.py` runs a complete offline example using a
temporary directory. It reopens the journal, seeks a list by the persisted
position, resumes keyed totals and checks independently specified outputs.

## Transaction and concurrency contract

Creation writes an empty generation-zero checkpoint. A nonempty successful
`advance` increments generation once and consumes at most `max_inputs` without
an extra source read. An empty source is a read-only no-op that still checks
for a raced generation. A filtered input increments the source position even
when it emits nothing. Expanded outputs share their input's zero-based
`source_position` and receive consecutive zero-based output `sequence` values.

Each batch executes on a detached runtime **outside the SQLite write lock**.
After it finishes, `BEGIN IMMEDIATE` reloads the head and compares it with the
earlier snapshot before any result is inserted. A stale generation raises
`RecoveryConflict`; there is no automatic callback retry. Publication, including
all output rows and the replacement head, uses `synchronous=FULL` in one
transaction. A callback, source, serialization, capacity or SQL failure leaves
the previous durable prefix unchanged. The in-memory proposal is discarded.

Callbacks remain trusted Python code. Their external effects are not rolled
back, and a conflicting transaction may already have executed them. The source
iterator may already have yielded records that did not commit. Use pure or
application-idempotent callbacks and re-seek a replayable source after failure.
No broker acknowledgement, distributed consensus or exactly-once external
effect is implied. No callback is dynamically imported from a checkpoint.

The caller supplies the source identity and must ensure it actually identifies
the same record ordering/content. The journal does not hash an arbitrary source
iterator itself. Flow identity binds its explicit semantic revision and
configuration, not callable bytecode or captured external state. Restoring with
a different source, flow revision or limits is refused. `create=False` refuses
missing databases; later reads do not silently recreate a deleted file.

## Verified reads and pagination

`latest()` verifies the head, source/flow binding, checkpoint contracts, state
limits and consecutive output index/count metadata. It does not scan every
output's content on each head read. `outputs(start=0, limit=1000)` verifies each
requested row's checksum, JSON fields, record contract and source ordering
within the page before yielding it. Corruption later in a page can raise after
earlier valid records were yielded; this is a row-wise API, not an all-or-nothing
page result. Collect a page before publishing it if that distinction matters.

The iterator holds one SQLite read transaction, so it observes a stable output
prefix even if another connection commits under WAL. Close it explicitly after
an early stop, or use `contextlib.closing`. The default journal mode is SQLite's
rollback journal, where an open reader can delay a writer; the application may
configure WAL for concurrent reads, with its corresponding backup requirements.
No connection is shared between calls or retained after an operation completes.

Strict reads reject duplicate JSON keys, nonfinite numbers, Unicode errors,
unknown/derived inconsistent fields and noncanonical stored encodings. SQLite
`CASE` guards check field types and UTF-8 byte lengths before materializing
stored payloads in Python. SHA-256 is accidental-corruption detection, **not**
writer authentication or prevention of an attacker replacing a valid history.
Protect the database using normal filesystem permissions and trusted backups.

## Capacity and operational scope

The v1 format accepts at most 10,000 source inputs per batch, 100,000 emitted
records per batch, one million output records in a journal, 64 MiB of encoded
new output rows per batch and 64 MiB per encoded head/document. The flow's
smaller record/state limits still apply. A flow checkpoint that exceeds the
journal's document cap cannot be committed even when valid for in-memory use.
Output pages contain at most 10,000 records. Start a separately identified
bounded run when a journal reaches capacity; retention/compaction is not yet
implemented.

These limits are not total process-RSS or database-file-size quotas. Runtime
state snapshots, source values, serialized head/output proposals and SQLite
buffers coexist. The journal materializes its state and the bounded transaction
output payloads, but not the complete historical output collection. Callbacks
are not preemptible or allocator-sandboxed. SQLite files, journal/WAL companions
and metadata must be backed up consistently; filesystem/power-loss guarantees
remain those of SQLite and the host. No network filesystem durability or
distributed source/partition recovery has been verified.

The older `RecoveryStore` remains a distinct, aligner-specific format. Neither
format silently opens or migrates the other. There is no automatic schema
migration, broker connector, retention service, retry scheduler or distributed
worker ownership protocol in this increment.
