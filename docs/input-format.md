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
  "expected_cadence_ms": {"camera": 500},
  "gap_factor": 1.6,
  "late_policy": "reject",
  "max_events_per_window": 1000,
  "max_output_windows": 10000
}
```

`window_ms` and `hop_ms` are required and positive. Numeric values must be finite. Required streams
must be unique. `max_events_per_window` and `max_output_windows` are positive integer resource
limits. An empty `required_streams` list disables incremental watermark closure until `flush()`.
Unknown configuration fields are rejected.

Input uses strict JSON: duplicate object keys and the non-standard `NaN`, `Infinity`, and `-Infinity`
tokens are rejected, including inside opaque event `data`.
