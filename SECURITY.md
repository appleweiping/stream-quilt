# Security policy

## Reporting

Use GitHub private vulnerability reporting instead of a public issue. Include a minimal reproduction,
affected version, impact, and suggested mitigation. You should receive an acknowledgement within
seven days.

## Trust boundaries

Stream Quilt reads local JSON/JSONL and writes local JSON/HTML. It does not fetch media, execute event
payloads, contact external services, or render user-provided HTML. Stream names, event IDs,
modalities, and other rendered labels are escaped in reports.

The parser rejects duplicate JSON keys and non-finite numeric constants. Event metadata is validated
as finite JSON, and aligners snapshot clock maps and nested event data before retaining them.

The configured `max_events_per_window` and `max_output_windows` are resource guards, not complete
input quotas. Applications accepting untrusted data should also limit file size, line length, event
count, nesting depth, and the size of each `data` object before calling the library.

Window emission is transactional. A resource-limit exception leaves the triggering event and its
watermark update uncommitted, so callers can fail closed without losing an unreturned earlier window.

Alignment does not establish provenance or authenticity. Sign and verify events before ingestion when
the source affects safety, access control, billing, or compliance.

`LocalPartitionedFlow` is an explicitly trusted-code execution API. Its bounded
startup pickle contains only the caller-supplied Python `Dataflow` and is loaded
only by its owned spawn children. Never supply untrusted Python objects or
replace those children with an untrusted peer. Runtime data and portable
checkpoints use strict bounded JSON, not pickle. Callbacks can access the host,
allocate memory and spawn descendants; this is not an OS sandbox. Force-stopping
owned workers neither guarantees callback `finally` execution nor kills their
descendants. See [worker lifecycle and limits](docs/local-partitioned-flow.md).

`PartitionedFlowJournal` retains that trusted-worker boundary and adds local
SQLite source/state/output transactions with bounded request, row, page and
retained-storage admission. Its hashes detect corruption; they do not authenticate
writers or prevent a valid older database from being substituted. Idempotent
receipt publication does not make callback effects exactly-once. Read the
[durable worker journal contract](docs/partitioned-flow-journal.md) before
interpreting cancellation, ambiguous COMMIT errors or output cursors as delivery
acknowledgements.
