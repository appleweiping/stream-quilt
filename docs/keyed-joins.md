# Incremental local keyed joins

`JoinRuntime` updates a retained keyed join on each explicit tagged arrival. It
does not consume an entire alignment result or call the offline time-tolerance
`join_streams` function. `KeyedJoin` binds an ordered tuple of 2–16 side names,
insertion/emission modes, a semantic revision and resource limits.

```python
from stream_quilt import FlowRecord, JoinRuntime, KeyedJoin

runtime = JoinRuntime(KeyedJoin("accounts", "v1", ("name", "email")))
assert runtime.process("name", FlowRecord("Alice", "123")).rows == ()
batch = runtime.process("email", FlowRecord("alice@example.invalid", "123"))
assert batch.rows[0].values == ("Alice", "alice@example.invalid")
assert batch.rows[0].present == (True, True)
```

Arrival order is the caller's order of successful `process(side, record)` calls.
Inputs must be keyed immutable `FlowRecord` values. Side names and keys use the
existing canonical Unicode key contract; no timestamp, event-time ordering,
watermark, source partition or remote connector is inferred.

## Exact mode semantics

Insertion and emission are independent, with all nine combinations supported:

| Setting | Meaning |
|---|---|
| `insert_mode="first"` | Retain only the first value observed for that side/key in the current retained state. |
| `insert_mode="last"` | Replace that side/key value on every arrival; the default. |
| `insert_mode="product"` | Retain every value, including duplicates, in arrival order. |
| `emit_mode="complete"` | Emit when every side has a value, then remove the whole key; the default. |
| `emit_mode="running"` | Emit on every arrival, retaining state. This includes an arrival ignored by first-value insertion. |
| `emit_mode="final"` | Retain without emitting until all sides have explicitly closed, then use bounded `drain()`. |

Product rows follow side declaration order, with earlier-side choices changing
more slowly. Running/product emits the **entire current Cartesian set** on each
arrival, not only newly introduced combinations. Repeated rows are intentional.
Complete mode starts a new matching cycle after emitting and deleting a key;
an unmatched later value does not inherit a side from the previous cycle.

`JoinRow` exposes `key`, ordered `sides`, explicit `present` flags and isolated
`values`. Missing values are represented by null with `present=False`; a real
JSON null has `present=True`. A key with no observed side never produces a row.
Reads return fresh nested JSON containers. Accepted encoded values can be shared
internally without exposing mutable references. `to_dict()` includes the side
names and presence flags, so serialized nulls are not ambiguous.

`row.to_record()` explicitly creates a regular `FlowRecord` containing presence
flags and values. That conversion is subject to the regular record's separate
**aggregate** JSON depth/node/byte limits. A valid join row can contain several
individually valid side values whose combination exceeds that aggregate limit;
conversion then rejects rather than silently dropping values or relaxing the
existing record contract.

## EOF, final draining and pull ownership

`close(side)` signals actual EOF for that side. It emits no rows and is
idempotent, including after the runtime has closed. New input on a closed side
is rejected; its previously retained values can still match other open sides.
Closing an unknown side is an error.

Once every side closes:

- Complete mode discards unmatched keys; it does not fabricate partial rows.
- Running mode releases retained state without re-emitting its last rows.
- Final mode becomes `draining` when state remains, or `closed` if empty.

`drain(max_keys=100)` is only valid after all sides close. It selects whole keys
in sorted Unicode-key order, capped by `max_keys`, batch rows and batch bytes.
It may process fewer keys than requested to satisfy the latter limits, but a
valid nonempty draining state always makes progress. A key is never partly
emitted. The returned immutable `JoinBatch` includes `rows`, resulting `phase`,
`pending_keys` and `drained_keys`; only drain operations increment the last
field. Final `closed` means all retained keys are gone, not that a consumer has
durably acknowledged returned rows. Repeated draining after closed returns an
empty closed batch.

`run(source_iterator, max_inputs=1000)` yields one committed batch per tagged
`(side, FlowRecord)` source item, including empty batches. It pulls at most the
cap, without an extra read, and does not own or close the caller's source.
Closing this output iterator stops further pulls, but cannot undo already
committed batches. A cap, empty source, `StopIteration` or early close does
**not** imply side EOF. Call `close(side)` when the application actually knows
that side is finished. Iterator exceptions may occur after preceding inputs
have committed; a failing input has already been pulled.

## Per-operation atomicity

Each process, side-close and drain operation stages all rows, state-index changes
and counters before one private state-reference swap. Validation, byte/count
overflow, row construction, index allocation and control exceptions before that
swap preserve the entire previous state. Draining multiple keys cannot publish
an earlier key if a later one fails. The runtime rejects reentrant operations
and is intended for one owner; it is not a thread-safe or distributed scheduler.

The core does not invoke user callbacks or manage external sources. Python
objects, source iteration, external side effects and output delivery remain
application-owned. In-memory atomicity is not process-crash durability.

## Exact bounds and work

`JoinLimits` accepts only built-in positive integers, not bool. Each configured
limit is bound into identity. Row limits per key must not exceed batch rows;
single-row bytes must not exceed batch bytes.

| Limit | Default | Source ceiling |
|---|---:|---:|
| `max_keys` | 10,000 | 100,000 |
| `max_values_per_side` (per key) | 256 | 10,000 |
| `max_values` (retained across all keys/sides) | 100,000 | 1,000,000 |
| `max_value_bytes` | 1 MiB | 8 MiB |
| `max_key_bytes` | 4 MiB | 16 MiB |
| `max_state_bytes` | 16 MiB | 64 MiB |
| `max_rows_per_key` | 1,000 | 100,000 |
| `max_rows_per_batch` | 10,000 | 100,000 |
| `max_row_bytes` | 1 MiB | 8 MiB |
| `max_batch_bytes` | 16 MiB | 64 MiB |

Values reuse `_snapshot`'s existing finite interoperable JSON, Unicode,
cycle/depth/node and per-value byte checks. Key/state bytes count the canonical
**checkpoint cell encoding**, including key text, array punctuation and escaped
encoded-value strings, not merely raw value payloads. Admission sums that wire
cost incrementally rather than first materializing an oversized cell string.
Metadata in the checkpoint header is separately bounded by 16 sides and the
existing 1,024-character key/name ceiling. Counters remain within `2**53 - 1`.

For each proposed key, bounded multiplication first checks Cartesian row count.
The runtime then computes the exact sum of all projected record-value JSON bytes
and the maximum individual row size, including presence flags and punctuation,
without expanding the product. This applies even to complete/final keys not yet
emitted. Consequently every accepted retained key can fit one final drain batch.
Row/batch payload-byte counts exclude the repeated key/side-name metadata in
`JoinRow.to_dict()`; those are bounded separately by row count and name lengths.

Process work snapshots one input and examines its proposed key's bounded values;
publication shallow-copies the retained index, O(retained keys). Drain sorts the
retained keys, O(keys log keys), then constructs only the admitted whole-key
output batch and copies the index. Checkpoints validate/serialize all retained
state. Existing/proposed indexes, immutable encoded values, output references,
temporary JSON copies and caller-retained outputs can coexist. These limits do
not bound total RSS, allocator overhead, user-source work or wall time. This is
incremental bounded state, not a constant-memory or compiled-performance claim.

## Checkpoints and source identity

`checkpoint()` returns immutable `JoinCheckpoint`; `to_dict()`/`to_json()` and
strict `from_dict()`/`from_json()` support portable local snapshots. Restore via
`JoinRuntime.from_checkpoint(join, checkpoint)`. Identity binds join ID, explicit
revision, ordered side names, both modes and all limits. Checkpoints include
closed-side flags, lifecycle phase, per-side admitted-input counts, emitted-row
count and sorted retained cells. Partial final draining is checkpointable.

The local format is `kind="stream-quilt-join-checkpoint"`, `version="1.0"`.
Each cell is `{"key": key, "values": [[canonical_json_text, ...], ...]}`: the
inner strings are deliberately encoded JSON, not values to evaluate as code.
Unknown fields, wrong exact container types, duplicate/unsorted keys, unsafe or
noncanonical nested JSON, impossible counts/phase, incompatible mode state and
identity mismatches reject. First/last cannot restore multiple values per side;
complete cannot restore a retained complete key; final cannot have emitted rows
while any side is still open. Configured capacity is rechecked after hard-ceiling
shape/byte/count admission.

Restore also checks necessary mode-specific counter consistency. Running joins
emit at least one row per admitted input, exactly one for first/last insertion.
For complete joins, subtract each side's retained values from its admitted
inputs: first/last rows cannot exceed the smallest remainder; product rows
cannot exceed that remainder times the configured per-key row cap. While open,
running/final product joins retain every input exactly, and first/last retain
at least one value for every side with prior input. Non-complete product joins
cannot ever admit more than `max_values`, even after EOF releases their state.
Final first/last rows cannot exceed total inputs minus retained keys. Final
output also cannot exceed the maximum number of already-drained keys
(`max_keys` minus retained keys), times the per-key row cap for product mode.
A closed final join with any input must have emitted at least one row. For
final/product, outputs must also be at least the largest consumed side-value
count, and at most total consumed values times the per-key row cap: every
drained Cartesian product contains at least as many rows as any one of its
side buckets and must consume input. Consumed means admitted minus retained.
These are necessary consistency constraints, not an exhaustive reachability test or
authentication of a hidden arrival history.

String/byte import is capped at 66 MiB before JSON parsing. The standard-library
parser still materializes that bounded outer document before shape validation;
this is not an incremental checkpoint parser. Nested encoded values are parsed
only after cumulative cell count/byte admission. The wire format contains no
callable names, arbitrary imports, pickle, signatures or credentials.

Counters describe this local arrival schedule. They are **not independently
verifiable broker offsets**, source-content commitments or proof that the state
was honestly produced. The digest is configuration identity, not authentication.
Current `FlowJournal`/`GraphJournal` still persist a single `source_id` and
`next_position`; they do not automatically include this separate runtime. A
future join journal needs its own explicit source/state/output publication
contract. Do not separately advance existing journal state and claim an atomic
multi-source transaction.

Run `python examples/keyed_join.py` for a real temporary-file checkpoint roundtrip,
continued three-side input and another restart after a partial final drain. The
fixture verifies four manually specified product/partial rows. Its demonstration
file writes are not atomic delivery or a production durable journal.

## Reference boundary

The frozen [Bytewax operator declarations](https://github.com/bytewax/bytewax/blob/9fce5b6ee43780329b05a2ecc1057ffddd51255d/pysrc/bytewax/operators/__init__.py#L1998)
specify first/last/product insertion and complete/final/running emission. This
is an original implementation of that local capability category, with explicit
presence bits, finite admission and paged final draining; it is not copied
source, wire/API compatibility or whole-repository parity.

Multi-source DAG inputs, source partition ownership, broker-offset transactions,
event-time join windows, distributed scheduling/recovery and the remaining
[whole-reference gaps](parity-dataflow.md) are still open.
