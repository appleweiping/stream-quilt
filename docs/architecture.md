# Architecture and semantics

Stream Quilt treats alignment as an event-time problem. It never uses the machine's wall clock, so
the same inputs and configuration produce the same windows on every run.

```mermaid
flowchart LR
    A[JSONL arrivals] --> B[Strict event parser]
    B --> C[Per-stream clock offsets and drift]
    C --> D{Mode}
    D -->|align| E[Stable event-time sort]
    D -->|replay| F[Required-stream watermark]
    E --> G[Half-open windows]
    F --> G
    G --> H[Completeness + gaps]
    H --> I[JSON]
    H --> J[HTML timeline]
    K[CloudEvents structured JSONL] --> B
```

## Event time and offsets

An event's normalized start is `timestamp_ms + offset(stream, timestamp_ms)`. Duration is not scaled.
A positive offset moves an observed clock forward. A stream with no `clock_drifts` entry takes the
constant `offsets_ms[stream]` through the same lookup it always did, so unused drift support cannot
change a single floating-point result. `estimate_offset()` accepts matched `(reference, observed)`
anchors and returns the median of `reference - observed`, plus residual diagnostics.

A stream may appear in `offsets_ms` or in `clock_drifts`, never in both. A drift correction already
carries its own constant term, so listing both would double-correct the clock silently; the
configuration is refused instead.

## Clock drift

A constant offset cannot describe a clock whose *rate* is wrong. Over a capture of length `T` a skew
`r` leaves an error `r × T` that grows without bound, and multi-device captures routinely skew by tens
to hundreds of ppm: at 300 ppm a fifty-minute capture ends nearly a second out, which is a whole
window on a one-second grid.

`ClockDrift` is the affine correction:

```text
offset(t) = offset_ms + rate_ppm × 1e-6 × (t - epoch_ms)
```

`offset_ms` is the offset that applies exactly at `epoch_ms`, and `rate_ppm` is how fast it grows, in
parts per million of observed elapsed time. `rate_ppm = 0` is the constant offset evaluated by the
same expression, so the two models are one model rather than two code paths.

### Fitting

`estimate_drift()` is a Theil-Sen fit of `reference - observed` against `observed`:

1. the rate is the median of the slopes of every anchor pair with distinct observed timestamps;
2. the offset is the median of the anchor offsets once that rate is removed, referenced to the median
   observed instant so the reported constant sits in the middle of the evidence rather than being
   extrapolated to zero.

Both stages are medians, so the breakdown point is near 29% of the anchors, against zero for a
least-squares fit where one badly matched pair moves the line by an unbounded amount. Nothing iterates
to convergence, so there is no seed, tolerance, or iteration cap that could make the answer depend on
how the fit was started.

Determinism is structural rather than incidental. Anchors are validated, reduced to
`(observed, offset)` samples, and sorted before any arithmetic happens, so any permutation of the same
anchors performs the identical sequence of floating-point operations and returns the identical record.
Residuals are computed with the same expression `ClockDrift.offset_at()` evaluates, so the reported
error is the error of the correction that will actually be applied rather than of a differently
rounded twin.

### Identifiability

Two anchors are a line through two points: no residual, and no way to tell a mismatched pair from a
real rate. The fit is refused unless the clean anchors are a majority of the evidence.

- Fewer than `MIN_DRIFT_ANCHORS` (5) anchors are refused. One bad anchor takes part in `n - 1` of the
  `n × (n - 1) / 2` pairwise slopes, a fraction `2 / n`; the median follows the clean majority only
  while that stays below one half, which is `n > 4`.
- Anchor sets are refused when one anchor takes part in half or more of the *usable* pairs. That is
  the general form of the same condition once anchors repeat an observed instant: a pair at one
  instant yields no slope, so five anchors clustered on two instants can still be decided by a single
  one of them, and a bare count would not catch it.
- Anchors at a single observed instant are refused outright. They fix an offset but no rate.

Refusal is the point. Degrading to a confident-looking line through two points is exactly the failure
this guards against, and `estimate_offset()` remains available for a constant: it needs one anchor and
claims no rate.

### Plausibility

A fitted rate outside `MAX_DRIFT_RATE_PPM` (10,000 ppm, one part in a hundred, one second of
divergence every hundred seconds) is refused rather than reported. Commodity crystal oscillators are
specified in the tens of ppm and stay within a few hundred across their whole temperature range, so
the bound sits two orders of magnitude above any real clock while still admitting a badly behaved one.
A rate implying a clock at half speed is 500,000 ppm, fifty times the bound, and is far more likely to
be mismatched anchors, a units error, or a clock stepped mid-capture than a real oscillator. The bound
also keeps `1 + rate` positive, so the correction is strictly increasing and can never reorder a
stream against itself.

### What the fit reports

`DriftEstimate` carries `rate_ppm`, `offset_ms`, `epoch_ms`, `anchors_used`, `pairs_used`,
`median_absolute_residual_ms`, and `max_residual_ms`; `to_dict()` renders them as stable rounded JSON
for a log. `pairs_used` is the size of the evidence the median was taken over. The two residual
measures separate "the fit is good" from "one anchor is wrong": a clean fit containing one bad anchor
reports a median residual near zero and a maximum equal to the mismatch, which is the signal an
operator needs and a single scalar would hide.

### Limits

The rate is one constant over the anchor span. A clock stepped mid-capture is two clocks and has to be
fitted in two pieces; a rate that wanders with temperature is averaged, not tracked. Anchors are
supplied by the caller, and nothing here discovers them. Durations move with their events but are
never scaled, exactly as constant offsets do not scale them; the residual error that leaves on an
event of length `d` is `|rate| × d`, at most one part in a hundred and below a millisecond for any
event shorter than 100 seconds.

## Window membership

Windows are `[start, end)` intervals. An instantaneous event at `end` belongs to the next window. A
duration event belongs to every window whose interval it overlaps. `hop_ms < window_ms` creates
overlap; `hop_ms > window_ms` intentionally leaves uncovered gaps. Accepted events in those gaps are
reported through `unassigned_event_ids`.

Required streams determine `complete`: a window is complete when it contains at least one event from
each required stream. Optional streams can still have offsets and cadence expectations.

## Streaming watermarks

The replay API records the greatest normalized timestamp seen for each required stream. Once all are
present:

```text
watermark = min(max_seen[required_stream]) - allowed_lateness_ms
```

A window closes when its end is at or behind that watermark. No previously returned window is ever
modified. This makes output append-only and auditable.

When `required_streams` is empty, the watermark stays undefined and replay emits only on `flush()`.
The aligner cannot know that an unseen stream will not later arrive, so inferring participants from
the first arrival could close windows prematurely. Configure one required stream explicitly for a
single-stream live replay.

An event is late when it overlaps or precedes output that has already closed. `reject` raises and
`drop` records its ID. `accept` records a partially late event and retains the portion that can still
affect open windows; it never reopens closed output. A fully obsolete event has no legal destination
and is recorded as dropped even under `accept`.

`flush()` emits the fixed window grid through the final buffered event horizon. Intermediate windows
can therefore be empty; preserving them keeps indexes and time ranges stable across runs.

Ready windows are previewed and validated as one transaction. Only after every window in the batch
passes resource checks does the aligner advance its index, prune its buffer, and return the batch.

## Offline mode

`align_events()` sorts by normalized timestamp, stream, and event ID before replaying. It is therefore
independent of input order and suitable for datasets. The CLI `replay` intentionally preserves JSONL
arrival order to exercise live watermark behavior.

## Gap diagnostics

For streams listed in `expected_cadence_ms`, consecutive normalized start times are compared. A gap is
reported only when the observed interval is strictly greater than `expected × gap_factor`. Gap start
marks one expected interval after the prior event; gap end is the next event start.
Cadence is a source-observation diagnostic, so it includes arrivals that a replay later drops as late.

## Window membership index

Windows form the regular half-open grid `[origin + i×hop, origin + i×hop + window)`, so every event
covers one *contiguous* range of window indexes. The aligner stores each buffered event once per node
of the canonical dyadic decomposition of that range: node `(level, block)` covers indexes
`[block × 2^level, (block + 1) × 2^level)`. Answering "which events overlap window `i`" reads one
node per level on the path from that leaf to the root.

Building `W` windows over a buffer of `E` events therefore costs `O(W log E)` lookups plus the events
actually returned, instead of the earlier `O(E × W)` full scan. One very long event costs
`O(log span)` to index rather than one entry per covered window, and expiry costs `O(log span)`.

Three properties of this workload pick the structure:

- Arrivals are append-mostly behind a monotonically advancing watermark, so the index must accept
  near-frontier inserts and expire from behind. A dyadic decomposition is implicit, so there is no
  tree to rebalance and no sorted array to shift.
- Window indexes are integers on a fixed grid, which is what makes a grid-aligned structure
  applicable at all; the two grid boundaries per event are found by binary search over the same
  overlap predicate the window builder uses, so no floating-point division can make the index and the
  builder disagree.
- Queries never mutate the index. A sweep-line active set would have to be rolled back whenever a
  previewed batch is rejected; a pure query needs no rollback.

Expiry is exact rather than heuristic: "horizon at or before the next open window start" and "last
covered window index below the next open window index" are the same predicate, so the index drops
precisely what the buffer scan used to drop.

## Retention

The interval index expires a buffered event as soon as no future window can contain it, so buffered
memory is bounded by the watermark. Per-event *identity* records — duplicate detection, window
assignment, and the reported ID tuples — are separate, and without a policy they grow for the life of
the aligner.

`RetentionPolicy` bounds that state:

```text
release_frontier = watermark - horizon_ms
```

An identity record is released once the event's horizon is at or behind the frontier. Records are
released in arrival order, and each one is classified before it is forgotten, so
`dropped_event_ids`, `accepted_late_event_ids`, and `unassigned_event_ids` report exactly what they
would report with every record retained. A long-lived event blocks release of the records behind it,
which is the conservative direction.

Releasing can never lose an event a future window could include. A ready batch always stops with
`next_start + window_ms > watermark`, so `next_start > watermark - window_ms`. Requiring
`horizon_ms >= window_ms` therefore puts the frontier strictly behind `next_start`, which is where
the interval index has already expired the event. A policy with a shorter horizon is refused at
construction rather than silently accepted.

`max_tracked_events` is a hard ceiling on retained records, counting IDs kept only for reporting.
Reaching it raises `ValidationError`. Bounded memory cannot hold an unbounded list of reported IDs,
so the aligner stops rather than dropping one.

The default policy retains everything, which is the right choice for finite datasets and reproduces
the historical behavior exactly. Retention is a runtime property of a live aligner rather than an
alignment semantic, so it is a `WatermarkAligner` argument and not a JSON configuration field; the
file-driven `align` and `replay` paths are already bounded by the event-file record limit.

## Integration and experimental boundary

The CloudEvents adapter terminates at validated `Event` objects; transport acknowledgement,
authentication, retry, and broker offsets remain the caller's responsibility. The benchmark compares
offline sorting and watermark replay on a versioned generated workload and checks their canonical
result digests. Full mapping rules and responsible reporting are documented in
[cloudevents-and-benchmarks.md](cloudevents-and-benchmarks.md).
