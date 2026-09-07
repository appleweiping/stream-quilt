# Whole-repository dataflow gap ledger

Status: **OPEN**, assessed 2026-09-07 against the fixed
[Bytewax repository at 9fce5b6ee43780329b05a2ecc1057ffddd51255d](https://github.com/bytewax/bytewax/tree/9fce5b6ee43780329b05a2ecc1057ffddd51255d).
The complete reference tree contains 281 files; 127 Python/Rust/C-family/TS
code-extension files total 841,358 bytes, including tests/tooling. This is a
tree inventory, not production LOC or evidence of equivalent functionality.

The original implementation adds actual local transaction recovery and a local
linear and bounded DAG operator runtimes. It does not implement a distributed dataflow engine.

| Reference surface | Current evidence | Remaining work |
|---|---|---|
| `dataflow.py`, typed stream/operator graph | Validated bounded linear and acyclic branching graphs over isolated JSON records; explicit semantic revision and topology inspection | Typed edges, multi-source graphs and multi-worker lifecycle |
| `operators/__init__.py`: map/value map, filter/value filter, flat-map/batch, branch, merge, key-on/remove | Composable map/filter/flat-map/key_by/drop_key, fanout, strict boolean branching and edge-ordered merge; bounded expansion and pull-based source consumption | General batches and full cross-operator reference conformance |
| `StatefulLogic`, `StatefulBatchLogic`, stateful map/flat-map | Per-step/per-key stateful_map, explicit deletion/emission, per-input rollback and strict portable snapshots | Stateful flat-map/batch, notifications, EOF handling and partition ownership |
| Final folds/reductions, counts, min/max, collect, cached enrichment and joins | Offline time-tolerance `join_streams` | Incremental keyed joins, final aggregation and collection contracts; cache expiration and enrichment error handling |
| `operators/windowing.py`: system/event clocks, sliding/tumbling/session windowers | Event-time alignment, watermark/late policies and offline session segmentation | General window logic/aggregation, processing-time clocks, idle notification, mergeable state and window metadata streams |
| `inputs.py`, `outputs.py`, file/Kafka/stdio/demo connectors | Bounded file/CloudEvents event readers; aligner/general-flow local transactional SQLite sinks | Source/sink partition protocols, external broker offsets, connector cancellation/retry, Kafka serde and broker integration verification |
| `recovery.py`, Rust recovery implementation | Strict aligner and keyed-flow snapshots, atomic offset/state/output SQLite transactions, CAS writer conflicts; aligner restart CLI and general-flow restart API | Distributed epochs, partition migration and recovery coordination, backup/retention policy and real external source/sink delivery contracts |
| Rust `worker.rs`, `run.rs`, `timely.rs`, `operators.rs` | Deterministic local partition assignment helper | Actual multi-process/multi-node operators, transport/exchange, progress coordination, worker failure handling and compiled hot paths |
| `testing.py`, `run.py`, errors | CLI, independent expected windows, separate-process restart/crash/rollback tests | General flow test harness, worker/cluster launcher and cross-operator failure propagation contracts |
| `visualize.py`, metrics, tracing and webserver | Local JSON/Markdown alignment diagnostics | Flow visualization, runtime metrics service, Jaeger/OTLP tracing, deployment and operational security |
| Examples, docs and evaluation | Alignment examples and existing synthetic alignment benchmarks | Full operator/connector tutorials, independently reproduced runtime/throughput/memory workloads, deployment examples and remaining exhaustive API review |

The source inventory was obtained from the frozen first-party GitHub tree and
public API declarations in
[operators](https://github.com/bytewax/bytewax/blob/9fce5b6ee43780329b05a2ecc1057ffddd51255d/pysrc/bytewax/operators/__init__.py),
[windowing](https://github.com/bytewax/bytewax/blob/9fce5b6ee43780329b05a2ecc1057ffddd51255d/pysrc/bytewax/operators/windowing.py),
and the [runtime source tree](https://github.com/bytewax/bytewax/tree/9fce5b6ee43780329b05a2ecc1057ffddd51255d/src).
No upstream implementation was copied into this project. API inventory is still
not an exhaustive reviewed contract catalogue; each row needs finer-grained
behavioral acceptance work before it can close.

## Recovery increment evidence

`tests/test_recovery.py` independently compares restarted processing with an
uninterrupted source, checks cursor monotonicity, executes separate-process
restart and pre-commit crash tests, races writers, injects a partially inserted
transaction failure, and verifies malformed/checksummed records. Strict parsing
rejects nonfinite JSON numeric overflow and validates reconstructed windows,
including redundant derived fields. SQL byte guards are tested with a SQLite
text factory that refuses materialization of oversized fields. Snapshot
iteration is checked under concurrent WAL writes and explicit early close.

See [recovery API and limits](recovery.md). Checksums detect accidental edits;
they are not writer authentication. Local SQLite output atomicity does not
acknowledge a broker or create exactly-once external side effects. Full-source
and checkpoint materialization are bounded separately from row-wise output
iteration. These are explicit operational boundaries, not completed reference
parity.

## Local operator increment evidence

`tests/test_dataflow.py` compares independent expected running totals, isolates
keys and callback containers, and verifies multiple updates to one key within
an expanded input. A downstream failure or exhausted expansion/state/output
budget must leave retained state and counters unchanged. Tests also exercise
reentrant calls, counter overflow, interrupted generators, revision mismatches,
strict checkpoint parsing and pull limits without an extra source read.

See [dataflow contracts and limits](dataflow.md) and the executable
`examples/keyed_totals.py`. These callbacks are trusted synchronous Python code,
not a process or wall-time sandbox. External side effects and source offsets
are outside the per-input rollback boundary. General-flow local persistence is
now available through the separate `FlowJournal` API below. External delivery,
notifications and distributed state remain open.

Verification for this increment (Windows, Python 3.12.13): 591 full tests passed,
96.02% combined statement/branch coverage, with 57 focused operator tests and
98.21% coverage in `dataflow.py`. Ruff lint/format, strict Mypy, Bandit, wheel/sdist
build, Twine and wheel-content checks passed. An independent read-only review
also ran a seeded three-key, 100-input oracle with 15 rejected transactions and
repeated snapshot restores, plus generator failure/early-close and counter
overflow checks. Its expected outputs/state/counters matched; no reference
throughput equivalence is asserted.

## General-flow journal increment

`FlowJournal` adds its own strict SQLite format, not an alias for the aligner
store. It runs callbacks on detached state, then uses generation CAS to publish
all source-offset/state/output changes atomically without automatically retrying
callbacks. Tests verify filtered/expanded source positions, races, re-open and
separate-process recovery, actual death inside a partially written SQL
transaction, snapshot pagination under WAL, early reader close, resource limits,
rehashed malformed records and pre-materialization SQLite byte guards.

The contract and operational limits are in [flow-journal.md](flow-journal.md).
The application supplies a replayable source and its identity. This establishes
local journal semantics, not broker acknowledgement, source partition ownership,
distributed epochs, retention/compaction or full Bytewax recovery equivalence.

Final verification of this increment (Windows, Python 3.12.13): 648 full tests
passed, 96.32% combined coverage and 100% statement/branch coverage for the new
journal module. Ruff lint/format, strict Mypy, Bandit, wheel/sdist build, Twine
and wheel-content checks passed. The 57 focused journal cases include the
review-discovered setup-connection leak and impossible generation-zero seeded
state. Independent checks matched a 55-record keyed-sum oracle over eight
restarts, a real two-thread CAS race and process death after both output and
head SQL writes. No remote matrix result or throughput equivalence is implied
by these local checks.

## Bounded branching graph increment

`GraphRuntime` executes a single-entry local DAG through the same internal
operator transaction engine as linear `FlowRuntime`. It adds explicit fanout,
one-evaluation boolean routing and edge-ordered union. Isolated per-node/per-key
state and shared downstream state proposals commit together with all terminal
outputs after every sibling succeeds. Whole-graph work-record/byte accounting
charges each edge delivery, preventing fanout from multiplying an unbounded
per-branch allowance. Versioned graph snapshots bind the topology, declaration
orders, routes, limits and explicit semantic revision.

Tests use independently specified diamonds, branch grouping, sibling state and
downstream sum oracles; a 60-input oracle spans repeated JSON snapshot restores.
They also check late-sibling failure, exact aggregate limits, callback reentrancy,
coroutine rejection, closed infinite generators, incompatible topology and
unchanged linear checkpoint bytes. See [branching contracts](branching-dataflows.md)
and `examples/branching_totals.py`.

Graph checkpoints are currently in-memory/portable state only. `FlowJournal`
rejects graph configurations before filesystem changes; journal integration,
multi-source inputs, typed edges, incremental joins/windows, notifications and
distributed delivery remain open. Local once-per-edge evaluation is not an
exactly-once external effect or distributed recovery claim.

Verification for this increment: all 727 repository tests passed on Windows
Python 3.12.13 and WSL Ubuntu Python 3.12.3. Windows combined statement/branch
coverage was 96.58%, with 100% for `branching.py` and 97.89% for the shared
`dataflow.py` engine. The 79 graph cases include review-driven state-index
allocation failure and generator cleanup/control-exception regressions, tested
against both schedulers. Linux ran the full suite without collecting coverage.
These are local correctness results, not remote CI or throughput-equivalence
evidence.
