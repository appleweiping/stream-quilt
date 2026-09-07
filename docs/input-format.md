# Input format

## Event JSONL

Each nonblank line is one object:

```json
{"id":"v1","stream":"camera","modality":"video","timestamp_ms":540,"duration_ms":420,"data":{"label":"merge"}}
```

| Field | Required | Meaning |
|---|---:|---|
| `id` | yes | Unique event ID within a file or aligner instance. |
| `stream` | yes | Clock/source identity. |
| `modality` | yes | Free-form modality such as `video`, `audio`, `text`, or `sensor`. |
| `timestamp_ms` | yes | Finite observed start time in milliseconds. |
| `duration_ms` | no | Non-negative duration; defaults to `0`. |
| `data` | no | Opaque JSON object retained in output; defaults to `{}`. |

Unknown fields are rejected. `data` is never interpreted or rendered as HTML.

## Configuration JSON

```json
{
  "window_ms": 1000,
  "hop_ms": 500,
  "allowed_lateness_ms": 150,
  "origin_ms": 0,
  "required_streams": ["camera", "microphone", "transcript"],
  "offsets_ms": {"camera": -40, "microphone": 15},
  "clock_drifts": {"transcript": {"rate_ppm": 120.5, "offset_ms": -12, "epoch_ms": 0}},
  "expected_cadence_ms": {"camera": 500},
  "gap_factor": 1.6,
  "late_policy": "reject",
  "max_events_per_window": 1000,
  "max_output_windows": 10000
}
```

`window_ms` and `hop_ms` are required and positive; `gap_factor` must be greater than one so a
reported cadence gap always starts before the next event. Numeric values must be finite. Required streams
must be unique. `max_events_per_window` and `max_output_windows` are positive integer resource
limits. An empty `required_streams` list disables incremental watermark closure until `flush()`.
Unknown configuration fields are rejected.

`clock_drifts` is optional and opt-in per stream. Each entry is an object with a required finite
`rate_ppm` plus optional `offset_ms` and `epoch_ms`, both defaulting to `0`. The correction added to
an observed timestamp is `offset_ms + rate_ppm x 1e-6 x (timestamp_ms - epoch_ms)`, so a `rate_ppm` of
`0` is exactly a constant offset. `rate_ppm` must lie within +/-10,000 ppm; a larger rate is refused
as a configuration error rather than a real clock. A stream listed in both `offsets_ms` and
`clock_drifts` is rejected, because a drift correction already carries its own constant term. Unknown
fields inside a drift object are rejected like any other unknown field.

Input uses strict JSON: duplicate object keys and the non-standard `NaN`, `Infinity`, and `-Infinity`
tokens are rejected, including inside opaque event `data`.

Config files are limited to 1 MiB. Event JSONL is limited to 64 MiB and 100,000 nonblank records.
Event `data` is copied into a deeply read-only JSON snapshot and is limited to 64 nesting levels,
100,000 values, and 24 MiB of canonical UTF-8 JSON. Labels are limited to 1,024 characters;
configuration mappings/stream sets are limited to 4,096 entries. `max_events_per_window` cannot
exceed 1,000,000 and `max_output_windows` cannot exceed 100,000.

For CloudEvents 1.0 structured JSONL, use `--input-format cloudevents`. The adapter contract and its
three alignment-specific extensions are documented in
[cloudevents-and-benchmarks.md](cloudevents-and-benchmarks.md).
