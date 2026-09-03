# Stream Quilt

**Deterministic event-time alignment for video, audio, text, sensor, and custom streams.**

[![CI](https://github.com/appleweiping/stream-quilt/actions/workflows/ci.yml/badge.svg)](https://github.com/appleweiping/stream-quilt/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-3776AB)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-22c55e.svg)](LICENSE)

Multimodal evidence rarely arrives in order or on one clock. A camera frame may lead its nominal
timestamp, an audio chunk may arrive late, and a transcript segment may span more than one analysis
window. Stream Quilt makes those choices explicit: clock offsets, half-open windows, required streams,
watermarks, lateness, and cadence gaps are all configuration rather than hidden behavior.

It supports two complementary workflows:

- `align`: deterministic offline alignment independent of input order;
- `replay`: preserve arrival order and close windows through required-stream watermarks.

## Demo

```text
$ stream-quilt demo --output examples/demo-output --write-input
aligned 11 events into 5 windows (1 incomplete, 1 gaps, 0 dropped, 0 accepted late, 0 unassigned)
  alignment    examples/demo-output/alignment.json
  report       examples/demo-output/timeline.html
  config       examples/demo-output/config.json
  events       examples/demo-output/events.jsonl
```

The checked-in image below is a screenshot of the actual generated
[standalone report](examples/demo-output/timeline.html), not a design mockup.

![Stream Quilt timeline with three normalized streams, five windows, and a cadence gap](docs/assets/demo-timeline.png)

## Features

- Strict JSON configuration and newline-delimited event input.
- Arbitrary stream and modality names while preserving opaque event metadata.
- Per-stream constant clock offsets.
- Robust median offset estimation from matched clock anchors.
- Fixed and overlapping half-open windows.
- Long events included in every overlapping window.
- Required-stream completeness and missing-stream diagnostics.
- Deterministic offline sorting.
- Incremental event-time watermarks with bounded lateness.
- Explicit reject, drop, or accept-partial-without-reopening late policy.
- Explicit diagnostics for events that fall into gaps between non-overlapping windows.
- Per-stream cadence-gap diagnostics.
- Resource guards for events per window and total output windows.
- Stable JSON and dependency-free HTML output.
- Standard-library runtime with no network, model, or media dependency.
- CloudEvents 1.0 structured JSONL ingestion with explicit alignment extensions.
- Reproducible offline-versus-watermark benchmark JSON with semantic output digests.

## Installation

Stream Quilt requires Python 3.11 or newer.

```bash
git clone https://github.com/appleweiping/stream-quilt.git
cd stream-quilt
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows: .venv\Scripts\activate
python -m pip install -e .
```

Install development tools with `python -m pip install -e ".[dev]"`.

## Quick start

Validate input without producing output:

```bash
stream-quilt validate examples/demo-output/config.json examples/demo-output/events.jsonl
```

Align a finite dataset regardless of file order:

```bash
stream-quilt align examples/demo-output/config.json examples/demo-output/events.jsonl --output aligned
```

Exercise live arrival semantics:

```bash
stream-quilt replay examples/demo-output/config.json examples/demo-output/events.jsonl --output replayed
```

Ingest structured CloudEvents or run the versioned CPU benchmark protocol:

```bash
stream-quilt align examples/cloudevents-config.json examples/cloudevents.jsonl \
  --input-format cloudevents --output aligned
stream-quilt benchmark --events 3000 --streams 3 --repeats 7 --output benchmark.json
```

See [CloudEvents integration and benchmark protocol](docs/cloudevents-and-benchmarks.md) for the exact
mapping, semantic comparison, reporting rules, and limits of the current evidence.

See [input-format.md](docs/input-format.md) for every field and validation rule.

## Python API

```python
from stream_quilt import align_events, load_config, load_events
from stream_quilt.report import write_report_bundle

config = load_config("examples/demo-output/config.json")
events = load_events("examples/demo-output/events.jsonl")
result = align_events(events, config)

for window in result.windows:
    print(window.start_ms, window.end_ms, window.complete, len(window.events))

write_report_bundle(result, config, "aligned")
```

Public models are defensively immutable: constructors snapshot caller-owned mappings and sequences,
nested event JSON is exposed through read-only mappings and tuples, and direct construction plus
`dataclasses.replace()` re-run domain validation. Every `to_dict()` method returns a detached,
ordinary JSON-ready object.

For live arrival order:

```python
from stream_quilt import WatermarkAligner

aligner = WatermarkAligner(config)
for event in events:
    for closed_window in aligner.ingest(event):
        publish(closed_window)
for final_window in aligner.flush():
    publish(final_window)
```

`flush()` is deterministic and idempotent. Ingestion after flush is rejected.

## Clock offsets

Offsets are added to observed event timestamps. Estimate a constant offset from matched anchors:

```python
from stream_quilt import estimate_offset

estimate = estimate_offset([(1_000, 972), (2_000, 1_971), (3_000, 2_500)])
print(estimate.offset_ms, estimate.max_residual_ms)
```

The median resists a single outlier, while residuals reveal whether a constant offset is credible.
Clock drift and resampling are intentionally outside the current model.

## Semantics that matter

- Windows are `[start, end)`. A point at `end` belongs to the next window.
- Duration events join every window they overlap.
- When `hop_ms > window_ms`, accepted events in uncovered intervals are listed in
  `unassigned_event_ids` rather than disappearing silently.
- Replay watermarks use the least-advanced required stream minus allowed lateness.
- With no required streams, replay deliberately buffers until `flush()`; list a single required
  stream to enable safe single-stream incremental closure.
- Returned windows are append-only and never reopened.
- An event is late if it overlaps or precedes already-closed output. `accept` retains only the portion
  that can still affect open windows and records its ID; a fully obsolete event is reported dropped.
- Gaps compare normalized start-to-start cadence and trigger strictly above the configured factor.
- Gap diagnostics describe source observations, including arrivals later dropped by replay policy.

Read [architecture.md](docs/architecture.md) before using replay results in an alerting or evaluation
pipeline.

## Non-goals and limits

Stream Quilt does not decode media, infer timestamps, synchronize clock drift, authenticate sources,
or guarantee real-time throughput. The current window builder scans the relevant in-memory buffer and
is intended for offline datasets and moderate-rate live streams. Very high event rates or very long
events need an interval index and explicit retention layer.

`max_output_windows` rejects configurations or inputs that would expand one aligner lifecycle beyond
the configured output budget. Increase it deliberately for long timelines rather than disabling the
guard implicitly.

Each incremental emission is transactional: every ready window is built and checked before the
aligner advances. A resource-limit error therefore does not consume the triggering event or hide an
earlier window from the caller.

The HTML report is for inspection; `alignment.json` is the machine interface.

## Development

```bash
python -m ruff check src tests
python -m ruff format --check src tests
python -m coverage run -m pytest
python -m coverage report
```

The suite exercises exact boundaries, long-event overlap, required-stream watermarks, out-of-order
arrival, every late policy, offset normalization, cadence thresholds, deterministic permutations,
CloudEvents mapping, baseline equivalence, performance guards, escaping, CLI behavior, and malformed
inputs.

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and the
[code of conduct](CODE_OF_CONDUCT.md). Maintainer authority is documented in
[GOVERNANCE.md](GOVERNANCE.md), release verification in [docs/releases.md](docs/releases.md), and
versioned citation metadata in [CITATION.cff](CITATION.cff).

## Companion repositories

Stream Quilt is one independent part of a small multimodal tooling suite. [Payload Palette](https://github.com/appleweiping/payload-palette) validates request media, [Frame Quorum](https://github.com/appleweiping/frame-quorum) selects auditable key frames, [Evidence Braid](https://github.com/appleweiping/evidence-braid) fuses evidence under explicit policies, and [Graph Sail](https://github.com/appleweiping/graph-sail) plans heterogeneous DAGs. The repositories have separate contracts and release cycles; no runtime dependency is implied.

## License

Stream Quilt is released under the [MIT License](LICENSE).
