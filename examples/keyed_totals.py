"""Run an incremental keyed flow, then resume from a portable JSON checkpoint."""

import json

from stream_quilt import Dataflow, FlowCheckpoint, FlowRecord, FlowRuntime, FlowStep, StateUpdate


def count_word(word, count):
    return StateUpdate(count + 1, {"word": word, "count": count + 1})


def main():
    flow = Dataflow(
        "word-totals",
        "v1",
        (
            FlowStep("words", "flat_map", str.split),
            FlowStep("word-key", "key_by", lambda word: word),
            FlowStep("totals", "stateful_map", count_word, lambda: 0),
        ),
    )
    runtime = FlowRuntime(flow)
    first = runtime.process(FlowRecord("red blue red"))
    portable = json.loads(json.dumps(runtime.checkpoint().to_dict(), allow_nan=False))
    restored = FlowRuntime.from_checkpoint(flow, FlowCheckpoint.from_dict(portable))
    second = restored.process(FlowRecord("blue green"))
    result = [record.value for record in (*first, *second)]
    assert result == [
        {"word": "red", "count": 1},
        {"word": "blue", "count": 1},
        {"word": "red", "count": 2},
        {"word": "blue", "count": 2},
        {"word": "green", "count": 1},
    ]
    print(json.dumps({"results": result, "processed_inputs": restored.processed_inputs}))


if __name__ == "__main__":
    main()
