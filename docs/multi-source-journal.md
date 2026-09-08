# Durable multi-source operation journal

`MultiGraphJournal` publishes a complete bounded request's source-position/EOF
vector, ordinary and join state, operation causes, and terminal outputs in one
local SQLite transaction. It reuses `MultiGraphRuntime`; it does not implement
a second graph scheduler or turn the single-entry `GraphJournal` into a
multi-source protocol. Existing v1 journal and checkpoint formats are unchanged.

Run `python examples/durable_multi_source_orders.py` for actual SQLite restart,
persisted-request retry, final draining and detached output pages. It requires
no network, broker, provider client or runtime dependency.

## Commands, complete requests and retry identity

```python
from stream_quilt import GraphDrain, GraphEOF, MultiGraphJournal, MultiGraphRequest

journal = MultiGraphJournal(path, flow, source_commitments, create=True)
point = journal.latest()
pending = MultiGraphRequest(
    point.journal_id,
    request_id,  # fresh 32-character lowercase hexadecimal ID
    point.generation,
    (GraphEOF("orders", next_position=20), GraphDrain(max_keys=10)),
)
saved = pending.to_json()  # retain this exact request before submission
receipt = journal.apply(MultiGraphRequest.from_json(saved))
```

Commands are an immutable tuple of existing `GraphInput`, `GraphEOF` and
`GraphDrain` objects, not a pull iterator. The ordered tuple is semantic. A
request's SHA-256 digest binds its wire kind/version, journal ID, request ID,
original expected generation and every command, including no-ops and input
values. Request import admits the fixed outer shapes and aggregate encoded
bytes before parsing the separately encoded JSON record values. Duplicate JSON
keys, unknown fields, nonfinite numbers and noncanonical inner record strings
are rejected. This is Stream Quilt's existing strict JSON record domain, not
arbitrary Python object serialization.

Request and stored-row digests use SHA-256 of their complete canonical UTF-8
JSON documents (sorted keys, compact separators, no nonfinite numbers). A
process operation's `input_digest` hashes its complete process-command document,
including source ID, position, record key and canonical encoded value; it is
not just a hash of the application value.

`source_commitments` must contain exactly the configured source IDs, each with
a 64-character lowercase hexadecimal digest. The journal captures their order
and values. The application defines and verifies the actual source content and
ordering behind these commitments; the library does not hash a broker or seek
an input file automatically. Source positions are exact local next positions,
initially zero, not authenticated broker offsets.

Creation generates a fixed random `journal_id`. Opening a mismatched source,
flow revision, topology or limits fails; replacing the file with another
journal is detected on subsequent access. The ID describes a lineage, not a
signature. A copied backup retains that lineage. Hostile modification, rollback
of a backup and malicious forks require guarantees outside this API.

Before executing callbacks, `apply` validates the complete request and reads a
consistent head. A matching previously committed request ID returns the original
receipt without callbacks, even when newer commits exist. Reusing that ID with
different content or a changed original expected generation raises
`RecoveryConflict`. Historical full checkpoints are not retained: call `latest`
to resume the current state, not to infer it from an older returned receipt.

## Publication, EOF, drain and no-op

Business callbacks run outside the SQL write lock on a detached restored
runtime. All commands must succeed before any journal write. The final
checkpoint is nested as a JSON object in the head; the complete actual UTF-8
head document is charged, not an assumed size of a doubly escaped string.

After staging all outputs, metadata, head and return receipt, a `BEGIN IMMEDIATE`
transaction checks the request ID again and compares the complete original
head and its digest. It atomically inserts the receipt, operations and outputs,
and replaces the head. A different winning writer causes `RecoveryConflict`;
callbacks are never automatically retried. Two writers of the same request may
both run callbacks before one wins. They publish only one prefix and may both
receive the same receipt. **Publication idempotency is not callback or external
side-effect exactly-once execution.**

- `GraphInput` advances its source position and operation sequence even when
  all terminal outputs are filtered.
- First exact-position EOF advances the operation sequence and closes that
  source without advancing its position. EOF has no terminal output.
- Effective drain advances the operation sequence without a source position.
  It drains one earliest-topological ready final join, recording that join ID
  and the requested `max_keys`. Downstream filters may leave zero outputs. The
  log does not invent a drained-key count that the runtime does not expose.
- Repeated exact-position EOF and drain with no ready join are no-ops.

A committed request increments generation once and retains only its effective
operations, with original `command_index` values; gaps therefore represent
no-op commands. A pure no-op request returns `status="no_op"`, after constructing
the result and rechecking the complete head. It performs no durable writes,
does not advance generation and does not reserve the request ID. Empty tuples
are also no-ops. A later changed head makes an old uncommitted request stale;
absence of a no-op receipt is intentional.

## Unknown commit outcomes and recovery

`request(request_id)` returns a validated persisted receipt or `None` at that
read snapshot. A receipt binds the request digest, original command count,
before/after generations and operation sequences, output half-open interval,
source vectors and adjacent head digests. The SQLite file retains receipts
after later commits so a lost acknowledgement is still resolvable.

If the real COMMIT succeeds and then the driver, caller or connection cleanup
fails, the new prefix can already exist. Commit/cleanup errors instruct callers
to inspect persisted state before retry. Query the request ID and resubmit the
**same complete request** if necessary. `None` is not proof that another writer
is not still in flight. Do not advance the expected generation and call that
an identical retry.

Connection ownership is scoped to each operation. Every applicable rollback
and close is attempted. Ordinary cleanup failures do not hide a primary error;
the first genuine control exception is preserved if later cleanup also raises
a control exception. `GeneratorExit` remains normal iterator-close signalling
for the shared legacy helper, where cleanup failures stay visible.

## Detached output pages and provenance

```python
cursor = journal.output_cursor(start=0)
page = journal.read_outputs(cursor, limit=100, max_bytes=2 * 1024 * 1024)
for output in page.outputs:
    cause = journal.operation(output.operation_sequence)
    consume(output.sequence, output.step_id, output.record, cause)
cursor_json = page.cursor.to_json()
```

`MultiGraphJournalOutput` records a global zero-based output sequence and its
one-based operation sequence. `operation(sequence)` verifies that operation's
bounded commit metadata and exposes `process`, `eof` or `drain` provenance.
It does not replay input payloads. Different sources' local positions are not
globally monotonic. Output ordering is by operation sequence and then the
runtime's terminal topological order, including across one-record pages.

An unsigned `MultiGraphOutputCursor` binds journal ID, retained anchor
generation/receipt digest, fixed exclusive output stop and next output position.
Generation zero has an explicit empty initial-head anchor. Later append does
not enlarge an old cursor. Every page uses a short read transaction, validates
the entire returned page and constructs immutable results before closing its
connection. No live SQLite iterator escapes. An unexplained missing row or
early end is an error, never an empty successful page with no progress.

Output-byte or metadata admission may conservatively produce a shorter page.
The cursor advances only over returned records. A next record larger than the
caller's page-byte limit raises rather than returning a misleading empty page.
The cursor is not an acknowledgement and does not prove output authenticity,
query completeness against an adversary, or that an external sink consumed it.
Use `(journal_id, output_sequence)` as a consumer idempotency key where suitable.
There is no local consumer-ack table or external broker/sink transaction here.

## Fixed format and resource profile

The application ID is `0x53514D4A` (`SQMJ`, decimal `1397837130`), with
`user_version=1`. Exact tables are `multi_head`, `multi_commit`,
`multi_operation` and `multi_output`. Unexpected user schema objects, altered
definitions, versions or application IDs are rejected; no automatic migration
or cross-opening of another journal format occurs. Paths are bound absolutely
when the instance is created. Later accesses use SQLite `mode=rw`, so a missing
journal is not silently recreated. Connections enable foreign keys and
`synchronous=FULL`; no automatic WAL mode change is made.

| Charged representation | Ceiling |
|---|---:|
| Complete request | 1,000 commands / 16 MiB canonical UTF-8 |
| Complete current head, checkpoint nested as object | 67 MiB UTF-8 |
| One receipt / operation / output row | 256 KiB / 16 KiB / 9 MiB UTF-8 |
| Newly staged output rows per request | 100,000 rows / 64 MiB UTF-8 |
| Receipt plus new operation rows per request | 16 MiB UTF-8 |
| Retained operations / outputs | 1,000,000 each |
| Output page | 1,000 rows; default 16 MiB, maximum 64 MiB output-row UTF-8 |
| Page commit metadata | 2,000 operations / 32 MiB including inspected adjacent receipts |
| Complete serialized output cursor | 1 KiB |

Generation cannot exceed operation count; each committed generation has
1..1,000 effective operations. There is no retention, compaction or disk-quota
manager. Start a separate journal before exhausting this bounded run.

SQL `typeof` and UTF-8 byte guards apply before Python text materialization for
all selected payload/digest fields. Indexed counters and request IDs are checked
against their decoded rows. Standard JSON parsing still materializes a
byte-bounded outer document before fixed-shape admission. The JSON encoder may
produce a whole string as one chunk before the total-byte check. These limits
are not a native allocator, process RSS, disk-space or CPU-time sandbox.

A page's output budget excludes the complete head (up to 67 MiB), current/adjacent
head receipts, one predecessor output (up to 9 MiB), and the current candidate
output (up to 9 MiB) that is parsed before admission. New commit metadata scans
reserve their worst-case allowance before fetching, so the page can stop
conservatively even when actual remaining metadata would be smaller. Runtime
state and one runtime output batch remain separately bounded by the graph's
existing limits; a journal output limit can reject that already-constructed
batch. Staged rows, JSON objects, returned copies and SQLite buffers can coexist.
Caller-retained requests/pages and arbitrary trusted callback effects are
outside these internal accumulation bounds.

Requests, checkpoints and outputs may contain actual application data. There
is no encryption or redaction. Digests, including input digests, are not a
privacy guarantee for guessable values.

## Verification scope and remaining boundaries

`latest` validates the current runtime checkpoint, table count/min/max prefixes
and adjacent receipt/head bindings. It does not scan every historical payload.
Request/operation lookup checks that commit's bounded operation sequence,
command indices, source-vector changes and output ranges. Page reads additionally
validate selected output records and ordering. These are necessary consistency
checks, not reconstruction of hidden callback history, all historical join
readiness, or output-content replay from the original request. Checksums detect
accidental changes; plausible data rewritten with matching checksums can pass.

Tests cover real writer barriers, true COMMIT-before-error outcomes, process
death after each kind of SQL write, whole-request rollback, SQL pre-materialization
guards, strict requests, no-op races, output budgets and missing rows. A separate
list-based interpreter checks all nine join modes across SQLite restarts.
Windows Python 3.12.13 passed the complete suite: **1,323 passed, one existing
Python-version skip**, in 275.25 seconds. Branch-inclusive coverage was **97.17%**
(5,235/5,348 lines and 1,882/1,976 branches). The 141 new journal tests plus
108 existing Flow/Graph journal tests passed separately (249 cases), with
resource/runtime warnings treated as errors. A separate baseline interpreter
comparison matched 80 complete old-v1 head prefixes and 1,500 cumulative output
rows byte-for-byte across 20 actual SQLite flow configurations. All 141 new tests
also passed on Python 3.14.5 in 43.58 seconds with resource/runtime warnings
treated as errors. Ruff lint/format, strict Mypy (28 modules), Bandit, frozen-lock
and whitespace checks passed. An isolated Python 3.14.5 wheel installation with
`--no-index --no-deps` exercised all nine modes, actual restarts, receipts,
pages, no-ops and the offline example. All 28 Python modules matched the checkout
byte-for-byte in the wheel and installed package. Final sdist/wheel metadata and
changed-file byte checks are documented in the parity ledger. No local Linux
full run or completed hosted CI is claimed here.

The journal is local SQLite recovery, not distributed epochs, partition
migration, broker acknowledgement, external sink atomicity, authenticated
history, event-time join windows or whole-reference parity. See the still-open
[whole-repository ledger](parity-dataflow.md).
