# Changelog

## Unreleased

- `MultiGraphJournal` adds atomic multi-source process/EOF/drain recovery in a
  distinct bounded SQLite format, reusing the existing graph runtime. Complete
  immutable requests and retained receipts support idempotent publication and
  unknown-commit recovery; fixed-prefix detached pages retain operation provenance.
  Local transactions do not make callbacks or external broker/sink effects
  exactly-once, and existing v1 journal formats remain unchanged.
- `StateFlatUpdate` and `stateful_flat_map` add atomic keyed zero-to-many output
  to the shared linear/graph execution engine, portable checkpoints and both
  local journals. State is snapshotted before output iteration; bounded native
  generator cleanup and malformed-coroutine rejection also cover `flat_map`.
- `GraphJournal`, `GraphRecoveryPoint` and `GraphJournalOutput` add atomic local
  DAG source/state/terminal-output recovery using the existing shared SQLite
  engine. Separate graph IDs/kinds, topology binding and terminal ordering preserve
  linear database compatibility. A generated offline example and crash, race,
  corruption and frozen-linear-wire regressions exercise actual storage behavior.
- Journal output pages validate a bounded predecessor across page boundaries.
  Ambiguous COMMIT/cleanup failures instruct callers to inspect the stored prefix
  before replay; genuine control exceptions survive ordinary cleanup errors.

- Bounded acyclic operator graphs add fanout, strict conditional routing and
  deterministic edge-ordered merges. Graph execution shares the linear operator
  transaction engine, with whole-graph work/state budgets and atomic per-input
  sibling publication. Separate portable graph checkpoints bind topology and
  revision; separately identified graph journals now supply durable local recovery.
- Local composable operators execute map/filter/flat-map/keying callbacks and
  isolated keyed state, with per-input rollback, bounded expansion and pull
  backpressure. Portable state checkpoints bind explicit semantic revisions;
  `FlowJournal` now supplies local atomic source-position/state/output transactions
  with generation conflicts, stable verified output pages and restart recovery.
  External broker acknowledgement and distributed recovery remain out of scope.
- Strict portable aligner checkpoints retain the retention policy and reject
  malformed state, nonfinite numeric encodings and inconsistent identity records.
- Atomic SQLite source-offset/state/window commits, generation conflicts,
  bounded snapshot readers and the `resume` CLI support real process restarts.
- Added separate-process crash/restart, transaction rollback, concurrent writer,
  corruption and SQL pre-materialization resource tests. Whole-reference
  dataflow/runtime gaps remain explicitly open in `docs/parity-dataflow.md`.

This project follows semantic versioning.

## [0.5.0] - 2026-09-07

### Added

- Added deterministic stream-local event-time session windows with explicit gap semantics.
- Added portable aligner checkpoints with configuration digests and fail-closed recovery.
- Added deterministic SHA-256 event partitioning for parallel worker topologies.

## [0.4.0] - 2026-09-07

### Added

- `join_streams()` and `stream-quilt join` for deterministic cross-stream event
  pairing after offline alignment. Pairs use normalized start-time deltas,
  retain shared-window provenance, and deduplicate events repeated by
  overlapping windows.
- A hard `MAX_JOIN_COMPARISONS` budget and inclusive `max_delta_ms` tolerance;
  an omitted tolerance is explicit unbounded matching within the comparison
  ceiling.

## [0.3.0] - 2026-09-07

### Added

- Grid-aligned interval index for window membership. Each buffered event is stored once per node of
  the canonical dyadic decomposition of the contiguous window-index range it covers, so building a
  window costs `O(log span)` lookups plus the events it returns instead of a full buffer scan. Long
  events cost `O(log span)` to index rather than one entry per covered window. Output is unchanged.
- `RetentionPolicy` and the `WatermarkAligner(config, retention=...)` keyword: an explicit,
  watermark-bounded policy that releases per-event identity records once no future window can reach
  them, so a long-running aligner no longer grows with the number of events it has seen. Each record
  is classified before it is released, so reported ID tuples are unchanged.
- `WatermarkAligner.retained_event_count` and `WatermarkAligner.released_event_count`.
- `estimate_drift()` and `DriftEstimate`: a Theil-Sen fit of `reference - observed` against observed
  time that recovers an offset *and* a rate from matched clock anchors. The rate is the median of the
  slopes of every anchor pair with distinct observed timestamps and the offset is the median of what
  remains, so the breakdown point is near 29% of the anchors and one mismatched pair cannot swing the
  line. Anchors are sorted before any arithmetic, so any permutation of the same anchors returns the
  identical record. The estimate reports `rate_ppm`, `offset_ms`, `epoch_ms`, `anchors_used`,
  `pairs_used`, and both a median-absolute and a maximum residual, with `to_dict()` for logging.
- `ClockDrift` and the `clock_drifts` configuration mapping: an opt-in per-stream affine correction,
  `offset_ms + rate_ppm x 1e-6 x (t - epoch_ms)`, accepted from JSON as well as Python. A stream with
  no entry takes exactly the constant `offsets_ms` lookup it always did, so alignment output is
  unchanged byte for byte when the feature is unused, and `rate_ppm = 0` reproduces a constant offset
  through the same expression.

### Changed

- Release engineering now uses a pinned Hatchling backend with explicit wheel and source-distribution
  contents, deterministic build timestamps, locked-environment CI, installed-wheel checks, checksums,
  and provenance attestations.
- CloudEvents context validation now follows the signed 32-bit Integer, Unicode String, URI-reference,
  and absolute `dataschema` contracts, with a deterministic SHA-256 fallback when a valid source/ID
  pair would exceed the internal label ceiling after encoding.
- A retention policy whose `horizon_ms` is shorter than `window_ms` is refused, because such a
  frontier could pass an event a still-open window can legitimately include.
- Reaching `max_tracked_events` raises instead of forgetting a reported event ID.
- `estimate_drift()` refuses rather than reports when the anchors cannot support the claim: fewer
  than five anchors, anchors at a single observed instant, or an anchor set in which one anchor takes
  part in half or more of the usable pairs. `estimate_offset()` remains the documented fallback,
  needing one anchor and claiming only a constant.
- A fitted or configured drift rate outside +/-10,000 ppm is refused as far more likely bad anchor
  data than a real clock, and a stream listed in both `offsets_ms` and `clock_drifts` is refused
  rather than silently double-corrected.
- The README non-goal on clock drift is corrected: drift is now synchronized from caller-supplied
  anchors, while anchor discovery, stepped clocks, and time-varying rates stay outside the model.

## [0.2.0] - 2026-09-01

### Added

- CloudEvents 1.0 structured JSONL adapter with preserved provenance and payloads.
- Machine-readable offline/watermark benchmark protocol with semantic output digests.
- Performance regression coverage and explicit research evidence boundaries.
- PEP 561 type marker and complete documentation/example source-distribution manifest.
- Checked-in, host-labelled Windows/CPython reference benchmark with semantic-digest regression.

### Changed

- Public event, configuration, window, gap, result, and benchmark models now defensively snapshot
  nested collections and validate direct construction and `dataclasses.replace()` operations.
- Native and CloudEvents inputs, nested JSON, alignment collections, and benchmark work now enforce
  documented resource ceilings.
- The CloudEvents adapter now follows JSON `null`-as-unset rules for context/extension attributes,
  validates absolute `dataschema` URI schemes and media-type syntax, and explicitly documents its
  controlled structured-JSONL subset.

## [0.1.0] - 2026-08-31

### Added

- Strict JSON configuration and JSONL event contracts.
- Clock normalization with robust anchor-based offset estimates.
- Offline, arrival-order-independent window alignment.
- Incremental required-stream watermarks and bounded lateness.
- Reject, drop, and accept-without-reopening policies for late events.
- Half-open windows, overlapping hops, long-event overlap, and completeness diagnostics.
- Per-stream cadence-gap detection.
- JSON output, standalone HTML timeline, replay CLI, and deterministic demo.
