# Bounded local file connectors

`stream_quilt.connectors.files` binds one strict keyed JSONL file to an existing
`PartitionedFlowJournal` and can materialize a complete output prefix as one
atomically replaced file. It is a local, single-source profile. Put code that
starts workers under `if __name__ == "__main__":` for Windows `spawn`.

```python
from stream_quilt import PartitionedFlowJournal
from stream_quilt.connectors.files import (
    StagedJsonlSource,
    materialize_file,
    run_file_journal,
)

source = StagedJsonlSource.stage(input_root, "rows.jsonl", private_root, "orders-v1")
journal = PartitionedFlowJournal(
    private_root / "run.db", flow, source.source_id, source.source_digest
)
run_file_journal(source, journal, max_new_inputs=100_000, batch_size=64)
materialize_file(journal, private_root, "output.jsonl")
```

To restart after process death, reopen the *staged* source, not the original
input path:

```python
source = StagedJsonlSource.restore(
    private_root, "rows.jsonl", journal_source_id, journal_source_digest
)
journal = PartitionedFlowJournal(
    private_root / "run.db",
    flow,
    source.source_id,
    source.source_digest,
    create=False,
)
run_file_journal(source, journal, max_new_inputs=100_000)
materialize_file(journal, private_root, "output.jsonl")
```

An input line is exactly `{"key":"account-1","value":42}`. Each nonempty
physical line is one original source position; the byte-offset table maps
position `n` to the byte after line `n-1` (or zero for position zero). LF and
CRLF are supported; the last line may omit its terminator. Blank lines, BOM,
duplicate JSON keys, nonfinite numbers, unkeyed records, extra fields and
invalid UTF-8 are rejected. A source is limited to 64 MiB, one million lines
and 8 MiB per line. Every wave also obeys the journal's request, input and
worker limits. `max_new_inputs` is a cooperative cap, not EOF; the source is
closed only when the driver reaches the staged end with capacity remaining.

Staging copies and validates the file into a private `blobs` directory before
creating a journal. The journal-bound digest includes the profile, relative
input name and complete raw bytes. `restore` and every new driver invocation
rehash and validate the blob before workers start. Changing the original file
after staging does not change that run. Source batches are parsed from the
exact validated in-memory byte snapshot (bounded to 64 MiB), so an in-place
blob edit after preflight cannot change records committed by that invocation;
the next restore/driver invocation will reject the changed blob. A failed/cancelled wave may run trusted
callbacks again on an explicit restart; their external effects are not rolled
back. The driver derives repeatable request IDs and reconciles a lost return
with the journal receipt. SQLite remains the authority for source position,
state, outputs and EOF. A crash can leave an unreferenced staged blob or private
temporary file. The connector does not automatically garbage-collect these;
remove them only after independently establishing that no journal uses them.

The output file contains a canonical metadata line followed by canonical JSON
output rows, retaining global sequence, source position, per-input ordinal,
key and value. The header includes a fixed journal cursor anchor and a hash of
the row bytes. Before replacing an existing file, the sink verifies the old
file against the retained journal prefix. A fresh complete candidate is
bounded to 64 MiB, written and `fsync`ed in the target directory, then
same-directory replaced. Repeating publication of the same prefix leaves its
bytes unchanged. A crash before replacement leaves the old complete snapshot;
a crash after replacement leaves the new complete snapshot. The file can lag
the journal; the two are not one transaction. Do not tail the file, read temp
files, or treat it as a downstream acknowledgement. Full-prefix rewrites are
not a high-throughput log sink. Cooperating publishers to one target are
serialized by a persistent owner-private lockfile in the target directory;
lock acquisition has a 10-second deadline and the OS releases the advisory
lock after process death. The lockfile is never unlinked (which would create
an inode-swap race) and is not part of the output snapshot. A different
journal attempting the same target after another publisher wins fails closed.
Processes that ignore this lock can still race and are outside this profile.

Paths must be relative slash-separated names under explicit roots. Static
symlinks, junctions and reparse points are rejected, as are path traversal,
drive/UNC/device paths, alternate data streams and non-regular targets. The
connector/output root must be app-owned and owner-private on POSIX; on Windows
the application must configure an equivalent private ACL. The profile assumes
stable directory topology while staging or publishing. Python's portable path
APIs do not provide race-proof confinement against an adversary concurrently
replacing Windows directory components. On Windows, process-crash replacement
is supported, but directory-entry power-loss durability is not claimed from
`os.replace`. This is neither a distributed connector protocol nor an
external exactly-once sink.
