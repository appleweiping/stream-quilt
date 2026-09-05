# Changelog

This project follows semantic versioning.

## [Unreleased]

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

### Changed

- A retention policy whose `horizon_ms` is shorter than `window_ms` is refused, because such a
  frontier could pass an event a still-open window can legitimately include.
- Reaching `max_tracked_events` raises instead of forgetting a reported event ID.

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
