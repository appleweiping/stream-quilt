# Keyed state with ordered expansion

`stateful_flat_map` executes a synchronous local callback once for each incoming
record. Its `StateFlatUpdate` proposes a keyed state replacement or deletion and
an iterable of zero-to-many output values. The operator is available in both
`Dataflow` and `GraphDataflow`, using their shared transaction engine, limits,
checkpoints and corresponding SQLite journals.

```python
from stream_quilt import Dataflow, FlowRecord, FlowRuntime, FlowStep, StateFlatUpdate

flow = Dataflow(
    "running-pairs",
    "v1",
    (
        FlowStep(
            "pairs",
            "stateful_flat_map",
            lambda value, state: StateFlatUpdate(state + value, (state, state + value)),
            lambda: 0,
        ),
    ),
)
runtime = FlowRuntime(flow)
assert [r.value for r in runtime.process(FlowRecord(2, "account-a"))] == [0, 2]
assert [r.value for r in runtime.process(FlowRecord(3, "account-a"))] == [2, 5]
```

## State and output semantics

The callback contract is `function(value, state) -> StateFlatUpdate`; `initial()`
returns the first JSON state for each `(step_id, record.key)`. An unkeyed record
is an error. Input values and existing state are isolated copies, not references
to the runtime's retained containers.

`StateFlatUpdate(state, outputs, retain=True)` has these explicit meanings:

- `state=None` with `retain=True` stores JSON null. It does not delete the key.
- `retain=False` deletes the key; its unused `state` is not validated or stored.
  A later record for that key calls `initial()` again.
- Empty `outputs` still commits the valid state proposal. Deletion can emit
  output values too; it is independent of the output count.
- Outputs preserve iteration order and the input key. Every yielded value is
  snapshotted independently before it can be mutated by the next iteration.
- The proposed state is snapshotted **before calling `iter(outputs)`**, not just
  before the first `next()`. Neither custom iterable entry nor a generator's
  subsequent body can retroactively change that snapshot.
- Several records reaching the same key in one input see preceding proposals,
  including deletion, even though none has yet been published.

The result is a proposal, not a deeply immutable callback object: applications
can still hold its containers. The runtime owns the validated encoded snapshot.
No callback function, closure, iterator or external handle is serialized into a
checkpoint. Change the flow's explicit revision when callback semantics change.
The operator kind is bound into flow identity, so changing an existing
`stateful_map` step into an expansion requires a new compatible application plan;
it cannot silently restore the old checkpoint under a different kind.

## Atomicity and durable recovery

Any malformed output, callback/iteration error, capacity failure or later
operator failure rolls back the **whole source input's internal state and
counters**. A graph includes all its siblings in that same boundary. An output
that was built internally before a later sibling failed is not returned as a
successful batch. Already executed application side effects cannot be undone.

All proposals commit only after the complete input has been accepted. During
`run()`, the last input is fully committed before its first output is yielded;
closing `run()` after one output does not reverse that input or durably
acknowledge its remaining outputs. No next source input is pulled until the
current batch has been consumed, and `max_inputs` does not over-read the source.

`FlowJournal` and `GraphJournal` commit the source position, resulting keyed
state and **all** ordered local outputs together in SQLite. A source input that
emits nothing still advances the committed source prefix. Multiple outputs of
one input retain the same source position. On reopen, resume the application-
owned source at `next_position`, using the returned generation for the next CAS.
The unchanged journal contract for ambiguous COMMIT acknowledgements applies:
inspect durable state before deciding whether to replay; do not infer rollback.

Run the completely offline example:

```sh
python examples/stateful_expansion.py
```

It deliberately creates a partial batch with no output, restarts from a real
SQLite journal, emits completed keyed batches, deletes completed state and
checks source positions against an explicit expected sequence.

## Resource ownership and bounds

`outputs` must be a synchronous iterable; strings, bytes and mappings are not
accepted as an output batch. The existing `FlowLimits` cover callback calls,
per-step output count, record/aggregate output bytes and retained state
keys/value/total bytes. Graph work accounting additionally charges fanout,
merges and delivered records. No independent allowance multiplies state
capacity for every emitted value. An infinite generator is stopped by the
existing record cap after at most one yielded value beyond that cap is observed.
The runtime must pull that value to distinguish exhaustion from overflow.

Native output generators, including those returned by a custom iterable, are
closed on normal completion and failure. An already-primed generator directly
supplied as `outputs` is also closed if state admission fails before iteration.
Ordinary cleanup errors do not replace an existing control exception; a new
control exception is propagated. Generator code and cleanup remain cooperative:
there is no time preemption if application code blocks or ignores shutdown.

Native coroutines directly encountered as the batch, a yielded value or a
generator's return value are closed without awaiting them, then rejected.
A previously started coroutine may execute its own cleanup code while closing.
A non-awaitable generator return value is not an output. Generic custom
iterators, custom protocol methods and arbitrary awaitables remain caller-owned;
the runtime does not invoke arbitrary `close()` methods or inspect inaccessible
values discarded inside a malformed `__iter__` implementation. This is not a
general resource manager or Python execution sandbox. These ownership rules
also apply to the shared plain `flat_map` implementation.

## Comparison boundary

The frozen [Bytewax operator surface](https://github.com/bytewax/bytewax/blob/9fce5b6ee43780329b05a2ecc1057ffddd51255d/pysrc/bytewax/operators/__init__.py)
includes persistent keyed state with flat-mapped output. This is an original
local implementation of that capability category, not source reuse or API/wire
compatibility. In particular, state deletion is explicitly represented by
`retain=False`, so JSON null remains usable state.

Notifications, event-time window operators inside this graph, partitioned
connectors, distributed scheduling/recovery and external-effect transactions
remain separate open rows in the [whole-repository ledger](parity-dataflow.md).
