# Durable local multi-worker Flow

`PartitionedFlowJournal` adds a parent-owned SQLite commit boundary to the
fixed-key, single-source [`LocalPartitionedFlow`](local-partitioned-flow.md).
The workers execute the same shared candidate-wave code and existing `FlowRuntime`.
They do not own durable state, write the journal, or keep an authoritative state
between waves. This is a local closed loop, not a distributed epoch protocol.

Run `python examples/durable_partitioned_totals.py` for actual spawn PIDs, stored
requests, SQLite reopen, a duplicate committed request, and a serial-output oracle.
Put process-starting code under the standard `if __name__ == "__main__":` guard.

```python
from stream_quilt import PartitionedFlowJournal, PartitionedFlowRequest

journal = PartitionedFlowJournal(path, flow, "source-v1", source_digest, workers=2)
point = journal.latest()
request = PartitionedFlowRequest(
    point.journal_id,
    request_id,
    point.generation,
    "wave",
    point.checkpoint.next_position,
    tuple(keyed_records),
)
saved_request = request.to_json()  # Retain this exact request before submitting it.
with journal.session() as session:
    receipt = session.apply(PartitionedFlowRequest.from_json(saved_request))
page = journal.read_outputs(journal.output_cursor())
```

## Authority, ordering, identity

Creating/opening a journal or reading `latest`, `request`, `output_cursor` and
`read_outputs` starts no workers. An explicit `PartitionedFlowSession` owns
1..8 actual spawn processes, their transport threads and cleanup capability.
Its worker pool is reused across requests; every new candidate starts from the
current database head, not a cached or previously computed in-memory candidate.
There is no public session checkpoint that can expose unpublished state.

The separate `SQPJ` SQLite application ID is `0x5351504A`, with `user_version=1`.
Three strictly checked tables store one head, retained commit receipts and
outputs. Old Flow, Graph, MultiGraph journals and all checkpoint wire formats are
unchanged. SQPJ rejects those formats, changed schema objects and persistent WAL
mode rather than silently migrating them. It uses SQLite DELETE journaling,
foreign-key checks and `synchronous=FULL`. Paths are bound absolutely at creation.

The head nests an unchanged partitioned checkpoint as an object. It binds source
ID/digest, Flow identity/revision, worker count, routing and all worker limits.
Generation equals the number of committed nonempty waves plus the first EOF.
Original source positions remain distinct from per-worker routed input counts.
Output rows retain original source position, per-input output index and global
sequence, ordered exactly as in the in-memory partitioned runtime.

Request IDs and journal IDs are 32-character lowercase hexadecimal strings.
The complete immutable request binds the original expected generation, cause,
source position and every input value/key. SHA-256 hashes canonical UTF-8 JSON
with sorted keys, compact separators and finite JSON numbers. Record values in
request wires are separately canonical encoded strings; aggregate outer-wire
admission precedes their decoding. Duplicate keys, noncanonical inner JSON,
unknown fields, unkeyed values, nonfinite numbers and implicit coercions are
rejected. The source digest and Flow semantic revision are application-provided
identities, not automatic verification of source content or Python bytecode.

## One request, one publication

`cause="wave"` requires 1..256 input records; `cause="eof"` requires none.
Both require the exact durable next position. A full request is admitted before
callbacks. Existing matching request receipts return without calling a callback,
even after later commits. Reusing an ID with different content or a different
original expected generation raises `RecoveryConflict`, including after EOF.

For a new wave, all active workers are dispatched before any is awaited. Their
candidate replies, aggregate budgets and complete next checkpoint are validated.
All output rows, hashes, head and returned receipt are allocated before SQL writes.
A short `BEGIN IMMEDIATE` transaction checks the request ID again, compares the
complete original head and digest, inserts the receipt and outputs and replaces
the head. SQLite COMMIT is the only durable publication point. No in-memory
`process_batch()` publication is wrapped and retroactively called durable.

Different losing writers get `RecoveryConflict`; the worker pool can be reused
for a later explicit request, which again reads the database. Concurrent writers
of the same request can both run callbacks before one publishes; both may then
receive the same retained receipt. **Idempotent publication does not mean exactly
once callback execution or external effects.** No failure automatically retries
callbacks. Checkpoints and receipts validate necessary consistency, not every
historical execution or recomputation of every output.

First EOF closes the original source without advancing its position or wave
count and without a callback or output. Repeated exact-position EOF is a no-op:
the complete return is constructed before a final head check, but no receipt,
request-ID reservation, output or generation is stored. The matching-ID check
always precedes no-op handling. Closing a session only tears down its workers;
it does not imply source EOF. This API intentionally has no implicit pull helper.

## Failure, cancellation and acknowledgement

A precommit SQL/validation failure leaves the old durable prefix. Once COMMIT
has been attempted, an error or lost return acknowledgement does **not** establish
rollback. Retain the original request and call `journal.request(request_id)`;
a matching committed receipt resolves that request even when newer commits exist.
Then use `latest()` for current state. Historical full checkpoints are not stored.

Parent control exceptions and worker/cancellation failures settle the owned pool
using the existing control-preserving cleanup. Failed closers retain ownership for
an explicit retry of `close`; failed construction exposes the existing
`local_worker_cleanup` capability. Ordinary SQL/CAS failures do not install a
candidate as session authority. A true control exception during COMMIT may coexist
with a successfully committed receipt; cleanup does not undo that commit.

One creating thread operates/closes a session. `cancel()` is the cross-thread
request. Its publication lock covers final write admission through the COMMIT
attempt, so cancellation linearizes before that admission or after the attempt;
it can wait for that short transaction/SQLite busy timeout. The monotonic wave
deadline includes synchronous admission, candidate work and staging, and is checked
again after SQL writes, immediately before the reversible boundary ends. A COMMIT
already started is not forcibly interrupted or retroactively rolled back because
its return was late. The connection busy timeout is 10 seconds. Native SQLite I/O,
OS process startup/cleanup and trusted callbacks are not hard-real-time operations.

All earlier spawn trust limits remain: callbacks and startup pickle are trusted;
callback globals/external state need not match serial execution, native termination
need not run `finally`, and abrupt parent death need not kill callback-created or
owned child processes. Worker protocol diagnostics omit raw callback arguments,
but callback stdout/stderr and arbitrary external effects are not privacy-filtered.

## Detached output pages

`output_cursor(start=0)` captures a fixed retained generation, receipt digest and
output stop. New appends do not extend that cursor. `read_outputs` returns an
immutable page with a next cursor, not an open transaction or live database cursor.
Each call uses one short consistent read transaction. It validates the anchor,
selected receipt ranges and both adjacent receipt boundaries, original source
positions, output ordinals, same-input key preservation and one predecessor
across page boundaries. Missing rows cannot yield a silently empty
nonprogressing page. Empty pages are valid at the fixed stop.

The cursor is a read position, **not a sink acknowledgement**, signed token or
proof of completeness to an untrusted party. Page `max_bytes` charges complete
stored output wires, independently of bounded receipt metadata. A page may stop
early before admitting the next generation's metadata. A next row that cannot
fit an otherwise empty page raises; it is never skipped.

## Bounded storage and work profile

The worker runtime's global/per-wave limits still apply. Additional SQPJ ceilings:

| Resource | Ceiling / actual charged representation |
|---|---|
| Head | 65 MiB, complete UTF-8 document including nested checkpoint |
| Request | 9 MiB, complete wire; at most 256 inputs |
| Receipt | 16 KiB, complete wire |
| Output row | 9 MiB, complete stored output wire |
| Wave outputs | 100,000 rows, 64 MiB complete output wires |
| History | 1,000,000 source positions, generations and output rows |
| Retained logical storage | 256 MiB, head/receipt/output payloads plus stored digest and request-ID UTF-8 text |
| Main database | 512 MiB, admitted file and SQLite page-count bound; page limit installed before new writes |
| File admission | 1 GiB aggregate main plus `-journal`, `-wal`, `-shm`, checked before connection |
| Page | at most 1,000 rows; configurable output-wire bytes up to 64 MiB |
| Page receipt metadata | 8 MiB conservative reservation; three 16 KiB receipts per selected generation |

Logical admission projects the actual replacement head and every added row before
writing. Count/size aggregate SQL scans are bounded by the retained database/row
profile, not constant-time index operations. SQL CASE guards inspect stored UTF-8
length before transferring a payload into Python. Page work additionally admits
one predecessor and one current candidate of at most 9 MiB each, the current head
of at most 65 MiB, and bounded anchor/adjacent receipts. Complete returned page
serialization has a separate 64 MiB + 4 KiB envelope cap.

These bounds reject oversized stored data or new durable publication. They are
not an RSS sandbox, a reservation of free disk space, or a hostile-filesystem
transaction guarantee. Standard JSON parsing/encoding, native SQLite recovery,
temporary copies and trusted Python objects can allocate before a semantic
rejection. SQLite DELETE rollback-journal space is separate from the main-page
cap; file admission observes existing sidecars and cannot prohibit external
concurrent writes. SQLite's normal journal/recovery guarantees apply on a supported
local filesystem; OS/disk loss, malicious replacement, backup rollback and replicas
are not authenticated by plain checksums. No compaction or automatic retention
deletion is provided; use a new explicitly identified bounded run when capacity
is exhausted.

This increment does not implement multi-source parallel graphs, cross-stage key
exchange, rescaling, broker connector ownership, distributed checkpoints, or
external source/sink atomicity. Whole-reference parity remains open.
