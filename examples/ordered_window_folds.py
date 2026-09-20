"""Run with an installed package: python -I examples/ordered_window_folds.py."""

from stream_quilt import (
    FlowRecord,
    OrderedWindowCheckpoint,
    OrderedWindowFoldRuntime,
    WindowFold,
)


def main() -> None:
    spec = WindowFold(
        "ordered-demo",
        "v1",
        width=10,
        initial=lambda: "",
        fold=lambda state, value: state + value,
    )
    runtime = OrderedWindowFoldRuntime(spec)
    runtime.process(0, 5, FlowRecord("A", "account"))
    runtime.process(1, 2, FlowRecord("B", "account"))
    checkpoint = OrderedWindowCheckpoint.from_json(runtime.checkpoint().to_json())
    resumed = OrderedWindowFoldRuntime.from_checkpoint(spec, checkpoint)
    release = resumed.advance_watermark(6, next_position=2)
    assert [effect.position for effect in release.effects] == [1, 0]
    resumed.finish(next_position=2)
    rows = resumed.drain().rows
    assert [(row.key, row.value) for row in rows] == [("account", "BA")]
    print("ordered positions:", [effect.position for effect in release.effects])
    print("window rows:", [(row.key, row.value) for row in rows])


if __name__ == "__main__":
    main()
