# Example

The checked-in example contains 11 intentionally out-of-order arrivals from camera, microphone, and
transcript clocks. Configured offsets place them on one timeline. The camera stream deliberately skips
one expected cadence interval.

Regenerate the input and every output from the real CLI:

```bash
stream-quilt demo --output examples/demo-output --write-input
```

The command writes `config.json`, `events.jsonl`, `alignment.json`, and a standalone
`timeline.html`. No network, media model, or browser library is required.

`cloudevents.jsonl` and `cloudevents-config.json` form a second, directly runnable integration
example using CloudEvents 1.0 structured JSON envelopes:

```bash
stream-quilt align examples/cloudevents-config.json examples/cloudevents.jsonl \
  --input-format cloudevents --output cloudevents-output
```

## Window folds with explicit progress and checkpoint files

From an installed checkout, run:

```bash
python -I examples/window_folds.py
```

The [self-contained example](window_folds.py) folds out-of-order keyed arrivals,
signals an explicit watermark, drains whole windows, drops an explicitly late
record, and restores actual owned temporary checkpoint files before and during
EOF drainage. It checks all five complete output rows and exact counters against
handwritten expectations, then removes its temporary directory. These files are
demonstration snapshots, not broker offsets, delivery acknowledgements, a durable
journal or a process-crash guarantee. See [the contract](../docs/window-folds.md).
