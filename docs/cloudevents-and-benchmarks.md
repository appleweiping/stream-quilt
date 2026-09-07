# CloudEvents integration and benchmark protocol

## Structured CloudEvents boundary

Stream Quilt can ingest one [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
structured JSON envelope per line. This is a controlled structured-JSONL subset, not a general
CloudEvents SDK. The mapping follows the official
[JSON event format](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md)
for core attributes and payload placement.

```bash
stream-quilt align config.json events.jsonl --input-format cloudevents --output aligned
```

CloudEvents requires `specversion`, `id`, `source`, and `type`; its general-purpose `time` attribute
is optional. Stream Quilt additionally requires `time` with an explicit RFC 3339 UTC offset because
event-time alignment cannot safely invent occurrence time. `source` and `id` are encoded together as
the internal unique event ID. The original envelope remains under `event.data.cloudevent`.

For exact millisecond semantics, this adapter supports the RFC 3339 profile
`YYYY-MM-DDTHH:MM:SS{.sss}{Z|±HH:MM}`: uppercase `T`/`Z`, an explicit offset, and zero to three
fractional digits. Leap seconds and higher sub-millisecond precision must be normalized upstream.
`source` is validated against the ASCII RFC 3986 URI-reference grammar, including authority,
path, query, percent-escape, and single-fragment rules. In particular, a relative first path
segment cannot contain a colon. `dataschema`, when present, must be an RFC 3986 absolute URI;
the absolute-URI grammar excludes fragments. `datacontenttype` must use a syntactically valid
media type. Context/extension attribute names use the CloudEvents 1.0 lowercase-alphanumeric
character set; this adapter applies the specification's recommended 20-character maximum as an
input ceiling. As required by the JSON format, a `null` context or extension
attribute is treated as unset; explicit `data: null` remains a payload. A `null` `data_base64` is
unset and therefore does not conflict with `data`.

CloudEvents Integer attributes use the normative signed 32-bit range from -2,147,483,648 through
2,147,483,647; the non-negative `durationms` extension therefore tops out at 2,147,483,647.
CloudEvents String attributes reject C0/C1 controls, Unicode noncharacters, and invalid scalar
values. An unknown String extension may be empty. Values are never trimmed or otherwise
normalized: whitespace that makes a URI or adapter label invalid is rejected rather than silently
changing event identity or stream semantics.

The internal event ID normally retains the readable percent-encoded `source`/`id` pair. If that
derived label would exceed Stream Quilt's label ceiling even though each CloudEvents field is
valid, the adapter uses a deterministic SHA-256 identity over the unambiguous JSON pair instead;
the original values remain in `event.data.cloudevent`.

Three optional extension attributes influence the adapter:

| Extension | Mapping |
|---|---|
| `stream` | Stream/clock identity; defaults to `source`. |
| `modality` | Modality label; defaults to `type`. |
| `durationms` | Non-negative duration in milliseconds; defaults to zero. |

All extension attributes—including the three adapter extensions—are retained under
`event.data.extensions`. Core context attributes remain under `event.data.cloudevent`. JSON `data` is retained under
`event.data.payload`; `data_base64` is decoded only for bounded validation and its encoded text is
retained under
`event.data.payload_base64`. Encoded length is preflighted so base64 validation allocates at most
approximately 16 MiB of decoded data.
Envelopes containing `data` and a non-null `data_base64` are rejected. The adapter reads structured
JSONL files, not HTTP binary-mode headers or CloudEvents batch envelopes.

Structured and native event JSONL files are capped at 64 MiB and 100,000 nonblank records. Config
files are capped at 1 MiB. Frozen event payloads permit at most 64 nested levels, 100,000 JSON values,
and 24 MiB of canonical UTF-8 JSON. Labels, stream/mapping collections, output windows, and benchmark
work also have explicit fixed ceilings. Public iterable and mapping boundaries stop consuming input
as soon as the relevant ceiling is crossed, including callers whose iterators have no length. These
checks bound local allocation; this adapter still does
not authenticate content or provide transport backpressure.

## Reproducible execution benchmark

```bash
stream-quilt benchmark --events 3000 --streams 3 --repeats 7 --warmups 2 \
  --output benchmark.json
```

The versioned `round-robin-monotonic-v1` generator builds a dependency-free, in-memory workload. Two
public execution paths are measured:

- `offline-sort`: finite alignment independent of input order;
- `watermark-replay`: append-only arrival-order processing and flush.

The report records median and nearest-rank p95 runtime, median events/second, window count, environment
metadata, and a SHA-256 digest of canonical result JSON. Equal digests demonstrate semantic agreement
for this ordered workload. Generation and JSON serialization are outside the timing region.

`benchmarks/results/windows-python314.json` is a checked-in run on the Windows and CPython environment
named inside that file. Regression tests recompute its workload identity, semantic output digest,
mode names, and window counts while ignoring runtime and throughput fields. Its timings characterize
only that host and run and must not be generalized to other machines or workloads.

Runtime comparisons require a controlled host, pinned commit and Python version, fixed power mode,
and retained machine-readable results. This synthetic benchmark exercises the implementation; it is
not evidence of production tail latency, unbounded retention, media decode throughput, or behavior on
a named public dataset.

## Evidence still required for deployment claims

A research or production claim should add a redistributable event corpus (or generator derived from
documented public statistics), characterize disorder and clock drift, report multiple workload sizes,
and capture peak memory and confidence intervals. The integration and benchmark contracts make those
experiments repeatable without pretending that the checked-in example is field data.
