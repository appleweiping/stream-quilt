# Bounded ordered window folds

`OrderedWindowFoldRuntime` is a distinct, single-owner local profile over the
existing `WindowFold` geometry and fold callbacks. The ordinary
`WindowFoldRuntime` remains arrival-ordered, and its API and checkpoint wire are
unchanged. This profile requires `late_policy="reject"`.

```python
from stream_quilt import FlowRecord, OrderedWindowFoldRuntime, WindowFold

spec = WindowFold(
    "concat",
    "revision-1",
    width=10,
    initial=lambda: "",
    fold=lambda state, value: state + value,
)
runtime = OrderedWindowFoldRuntime(spec)
runtime.process(0, 5, FlowRecord("A", "account"))
runtime.process(1, 2, FlowRecord("B", "account"))
release = runtime.advance_watermark(6, next_position=2)
assert [effect.position for effect in release.effects] == [1, 0]
runtime.finish(next_position=2)
assert runtime.drain().rows[0].value == "BA"
```

The caller supplies contiguous, zero-based source positions. `process` admits
one isolated keyed record but does not run fold callbacks. `advance_watermark(W)`
folds buffered records with timestamp **strictly below `W`**, sorting by
`(timestamp, source position)`. Records at `W` stay buffered. This differs from
Bytewax's `<= watermark` release; the operator does not claim API, latency or
distributed compatibility with Bytewax. A repeated equal watermark is a no-op.
Progress and EOF require the current `next_position`; invalid positions and
timestamps below the current watermark are rejected without consuming input.
No wall clock or end-of-media time is inferred. `finish` releases all remaining
buffered records in the same order and marks EOF; drain complete windows
through `drain(max_windows=...)`. If a watermark left eligible windows pending,
drain them before another watermark, input, or an EOF with nonempty buffer.

The default buffer caps are 1,000 records and 16 MiB of canonical record wire.
`OrderedWindowLimits` can lower either cap, never raise the hard ceiling.
Underlying `WindowFoldLimits` still bound input, state, windows, rows, total
inputs and memberships. A release stages the entire fold candidate before
publishing buffer, watermark, counters or rows. If a callback or admission
fails, the runtime state is unchanged. Trusted callback globals and external
side effects cannot be rolled back.

`checkpoint().to_json()` captures the ordered buffer, source prefix, nested
window state and configuration identities in a separate strict canonical
`OrderedWindowCheckpoint` wire. Resume with
`OrderedWindowFoldRuntime.from_checkpoint(spec, checkpoint, limits=...)`,
passing the same explicitly lowered limits when used. Resume the *same*
source at `runtime.status.next_position`. The SHA-256 checksum detects
accidental corruption but is not authentication. This runtime alone does not
atomically pair source offsets, checkpoints and sink acknowledgements, so it
does not provide external exactly-once delivery. Sorting is `O(B log B)` for
buffer size `B`; staged fold-state copying can be more expensive. Callback
time and memory outside owned JSON are not sandboxed.
