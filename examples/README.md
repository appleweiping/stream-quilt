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
