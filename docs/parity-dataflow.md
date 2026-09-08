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
| `StatefulLogic`, `StatefulBatchLogic`, stateful map/flat-map | Per-step/per-key stateful_map and stateful_flat_map, explicit deletion/emission, ordered expansion, per-input rollback and strict portable snapshots | Stateful batch, notifications, EOF handling and partition ownership |
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

Graph checkpoints remain a separate portable format. `FlowJournal` rejects graph
configurations before filesystem changes; `GraphJournal` now connects them to the
shared SQLite transaction engine through a distinct graph storage contract.
Multi-source inputs, typed edges, incremental joins/windows, notifications and
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

## Durable graph journal increment

From baseline `2dd15c1`, `GraphJournal`, `GraphRecoveryPoint` and
`GraphJournalOutput` provide actual local SQLite source/state/terminal-output
publication for general branching DAGs. They specialize one shared private
journal engine rather than duplicate SQL. Distinct application IDs and strict
graph kinds bind topology, revision and all limits without changing the linear
wire format. Read [graph recovery contracts](graph-journal.md).

The 51 new tests include independent two-terminal keyed sums and diamond-merge
oracles, restart/source-offset recovery, filtered inputs, actual two-writer CAS
barriers, late-sibling rollback, process death after output insertion and after
head replacement, stable WAL pages, SQL pre-materialization limits, cross-format
rejection, rehashed corruption, control-preserving connection cleanup and a
frozen canonical linear head/output oracle. A bounded predecessor now protects
source/terminal ordering across page boundaries. Commit-then-error tests confirm
that diagnostics instruct callers to inspect persisted state before replaying,
not assume rollback or automatically retry callbacks.

### Timing-guard diagnosis and measurement correction

Initial Windows full-suite runs with Python 3.12.13 and coverage 7.16.0 CTracer
had **777 passes and one failure** in the existing 600-event benchmark guard:
timed workload values were **8.90 s**, **5.87 s**, and **5.78 s**, exceeding its
unchanged 5.0 s cutoff. The last guard ran during a coordinated quiet window;
these failures are not attributed solely to competing test processes. An
additional standalone traced guard failed at 6.80 s. An uninstrumented probe
took 0.313 s wall/0.297 s CPU; a separately enabled CTracer probe took
3.96 s wall/3.03 s CPU.

An exact, unmodified `2dd15c1` Git archive reproduced the same failure in its
full suite: **726 passed, one failed**, with a **10.11 s** workload. Individual
baseline/current traced guards also passed in separate A/B runs (4.63 s and
5.43 s total pytest time, respectively). This demonstrates sensitivity of the
old measurement to full-suite/tracing conditions, not a benchmark algorithm
change; it does not establish one exclusive root cause for every timing variance.

Only the test measurement harness changed: it runs the same 600-event, one-repeat,
zero-warmup workload with the same strict **less-than-5.0-second** assertion in an
isolated `python -I -S` child. The exact absolute source directory is supplied;
site hooks/environment-driven coverage startup and trace/profile hooks are
excluded from that timing. A 30-second outer subprocess timeout bounds setup.
The parent checks both mode names, 20 windows per mode, equivalent outputs and
the published-baseline canonical output digest
`933b1f3d572ae2748af4abd83af3d274f81674e34507da58d6b8e91f06bf749f`.
Production `benchmark.py`, `aligner.py`, `models.py` and `interval_index.py` have
no changes in this increment; other semantic/benchmark tests remain covered.
There is no coverage exclusion, workload reduction or relaxed acceptance cutoff.

The stale lock entry for this root package was synchronized from 0.3.0 to the
already-existing pyproject version 0.5.0, without a version bump or dependency
upgrade. Whole-reference distributed recovery, external transactional sinks,
retention/compaction, typed edges and other open capability rows remain open.

### Final verification after measurement correction

- Windows Python 3.12.13: **778 passed**, no skips, in 165.22 s. The unchanged
  95% combined statement/branch coverage gate passes at **96.68%**;
  `graph_journal.py` is **100%**, the shared `flow_journal.py` **99.47%**.
- WSL Ubuntu Python 3.12.3: **778 passed**, no skips, in 131.56 s; this run did
  not collect coverage. Both final full suites escalated resource warnings.
- Ruff lint/format, strict Mypy (23 modules), Bandit, frozen-lock and whitespace
  checks passed. The offline restart example produced the manual two-branch
  totals `[2, 20, 5, 50, 9, 90]` at source positions `[0, 0, 1, 1, 2, 2]`.

These final runs are distinct from the failed pre-correction runs above. They
are local correctness evidence, not remote CI results, performance parity with
the whole reference repository, or external-effect durability certification.

## Transactional stateful expansion increment

From baseline `5dbb4c4`, `stateful_flat_map` extends the shared operator engine
with an explicit `StateFlatUpdate(state, outputs, retain=True)` decision.
Zero output still commits the proposed state; retained JSON null is distinct
from deletion. Both the linear and graph schedulers defer all per-input state
and output publication until expansion and every downstream sibling succeeds.
The existing SQLite journals restore the new operator kind without a new wire
version or a second SQL implementation. See [the exact contract](stateful-expansion.md).

The 58 new cases cover independent keyed totals, repeated keys within one input,
empty output, deletion/reinitialization, state/output/record limits, late-sibling
rollback and actual SQLite recovery. Iteration setup is inside the proposal's
rollback boundary: a custom `__iter__` that mutates its proposed state and then
fails cannot leak that state. Native generator cleanup preserves primary control
exceptions and rejects/closes known coroutine outputs, including return values
from `generator.close()` on Python 3.13+. Arbitrary user-defined resource and
iterator protocols remain caller-owned, not automatically guessed or closed.

Final Windows/Python 3.12.13 full-suite verification passed **835 tests**, with
**one Python-3.13+-specific skip**, in 378.29 s. Combined statement/branch
coverage was **96.74%**, with **98.29%** in `dataflow.py` and **100%** in
`branching.py` and `graph_journal.py`; the 95% gate is unchanged. All **58** new
cases subsequently passed on Python **3.14.5**, including that version-specific
generator cleanup case. Runtime and resource warnings were errors. An independent
read-only review passed 164 seeded checks spanning both schedulers, repeated
same-key records, rejected inputs, snapshot restarts and real SQLite batch
rollback. The executable example recovered outputs `[[2, 3], 5]` at source
positions `[2, 2]`.

Ruff lint/format, strict Mypy (23 source modules), Bandit, frozen-lock checking,
wheel/sdist builds, strict Twine metadata and wheel-content checks passed. An
isolated Python 3.14.5 environment installed only the built wheel with no index
or runtime dependencies, then executed the same SQLite example successfully.

These are local checks, not a claim that this increment's full Linux suite or
remote CI has run. Distributed execution, multi-source event-time joins/windows,
external transactional sinks and remaining whole-reference gaps stay open.
