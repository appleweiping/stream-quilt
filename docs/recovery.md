# Durable file replay

`stream-quilt resume config.json events.jsonl --database run.sqlite` replays a
finite JSONL or CloudEvents source in arrival order. `--batch-size 100` commits
after at most 100 new events; `--max-new-events 250` stops at a durable boundary
without flushing the unconsumed source. Running the same command again resumes
at the committed offset. An exhausted source is flushed exactly once.

```python
from stream_quilt import AlignmentConfig, Event, RecoveryStore, resume_events

events = tuple(Event(str(i), "sensor", "text", i * 10) for i in range(20))
config = AlignmentConfig(window_ms=10, hop_ms=10, required_streams=("sensor",))
resume_events(events, config, "run.sqlite", batch_size=5, max_new_events=7)
finished = resume_events(events, config, "run.sqlite", batch_size=5)
assert finished.position == 20 and finished.checkpoint.closed
windows = RecoveryStore("run.sqlite").window_documents()
```

The checkpoint, source position and newly closed windows commit in one SQLite
transaction (`BEGIN IMMEDIATE`, `synchronous=FULL`). A transaction failure leaves
the earlier state, offset and outputs intact. Competing writers must supply the
generation they read; a stale generation raises `RecoveryConflict` and commits
nothing. Reopening validates a SHA-256 digest for state and each output row and
checks that the committed window range is complete.

Checkpoint JSON version 1.1 includes retention settings, normalized live events,
identity tracking and stream maxima. `AlignerCheckpoint.from_dict` rejects unknown
fields, invalid statuses/numbers and inconsistent record identities. Restore also
checks config/retention identity and grid position. The public v1.0 writer had no
strict reader and omitted retention; it is not automatically accepted as a complete
recovery record. Positive infinite retention/exclusive horizons use JSON null as
their explicitly typed sentinel; all other times are finite numbers.
Overflowing JSON numbers such as `1e999` are refused before constructing models.
Release counters must be non-negative integers at most `2**53 - 1`, ensuring
lossless integer interchange. Stored windows are reconstructed through the typed
event/window models and must match every serialized field, including the derived
`complete` and `modalities` values.

Input identity hashes the complete canonical event sequence; changing any event or
its order prevents reuse of the checkpoint. The finite source remains bounded by
`MAX_EVENTS` and the 64 MiB canonical-document limit. Each transaction stores one
latest checkpoint and at most `MAX_OUTPUT_WINDOWS` output rows. The database grows
with output size. Results are independent of batch size and interruption boundary.

For large output histories, consume verified windows one at a time:

```python
from contextlib import closing

with closing(RecoveryStore("run.sqlite").iter_window_documents()) as documents:
    for document in documents:
        process_window(document)  # application-defined consumer
```

The iterator holds one SQLite read transaction and connection from its first
iteration until exhaustion or explicit close. Complete exhaustion verifies both
all row hashes/contracts and the complete committed window range. If the caller
stops early, only the consumed prefix has been verified; `closing` releases the
connection even on early exit or a consumer exception. A long-lived read
transaction can delay writers under SQLite's default journal mode.

State and output queries use SQL `CASE` guards on UTF-8 byte length and storage
type before SQLite materializes the TEXT into Python. Oversized payloads and
non-text/incorrect-length checksums are rejected. Each accepted document is
bounded by 64 MiB; SQLite's own allocations/page cache and malformed database
scan cost are not covered by that bound.

`window_documents()` still collects the iterator into a tuple, so its memory
scales with all returned rows. The iterator's Python memory scales with the
materialized checkpoint and the current window, plus JSON/model overhead. It
does not make the whole pipeline constant-memory: the finite input source and
latest checkpoint remain materialized, and caller retention of yielded objects
also consumes memory. Resume verifies previous output through the iterator
without collecting a second copy of the entire output history.

Initialization creates tables only in an empty database or validates an existing
recovery schema containing its three expected tables. It refuses unrelated
tables/views, partial recovery schemas and a deleted schema marker instead of
silently adding or repairing tables in an existing user database. Extra indexes
or triggers on an otherwise complete recovery schema are permitted; file access
is not an authentication boundary.

This runner uses one local SQLite sink. Broker acknowledgement, external side effects,
distributed partitions and cross-system exactly-once delivery remain separate work.
Hashes detect accidental changes; they do not authenticate an actor who can rewrite
both data and hashes. Use a SQLite backup or stop writers when copying the database.
