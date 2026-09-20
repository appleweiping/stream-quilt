"""Timestamp-order window folds with explicit source positions and watermarks."""

from __future__ import annotations

import hashlib
import json
import random

import pytest

from stream_quilt import (
    FlowRecord,
    OrderedWindowCheckpoint,
    OrderedWindowEffect,
    OrderedWindowFoldRuntime,
    OrderedWindowLimits,
    ValidationError,
    WindowFold,
)


def test_direct_release_effect_rejects_hostile_outcome_without_equality() -> None:
    class HostileOutcome:
        calls = 0

        def __eq__(self, other: object) -> bool:
            self.calls += 1
            raise RuntimeError("outcome equality must not run")

    outcome = HostileOutcome()
    with pytest.raises(ValidationError, match="invalid ordered release outcome"):
        OrderedWindowEffect(0, 0, outcome, 0)
    assert outcome.calls == 0


def test_ordered_fold_releases_in_timestamp_order() -> None:
    spec = WindowFold(
        "ordered-concat",
        "v1",
        width=10,
        initial=lambda: "",
        fold=lambda state, value: state + value,
    )
    runtime = OrderedWindowFoldRuntime(spec)
    runtime.process(0, 5, FlowRecord("A", "account"))
    runtime.process(1, 2, FlowRecord("B", "account"))
    released = runtime.advance_watermark(6, next_position=2)
    assert [item.position for item in released.effects] == [1, 0]
    assert runtime.finish(next_position=2).effects == ()
    rows = runtime.drain().rows
    assert [(row.key, row.value) for row in rows] == [("account", "BA")]


def _spec(*, fold=None) -> WindowFold:
    return WindowFold(
        "ordered-concat",
        "v1",
        width=10,
        initial=lambda: "",
        fold=fold or (lambda state, value: state + value),
    )


def _oracle(inputs, watermarks):
    """Independent tumbling reference, with no runtime/stage-helper calls."""
    remaining = list(inputs)
    state = {}
    effects = []
    for watermark in watermarks:
        due = sorted(
            (item for item in remaining if item[1] < watermark), key=lambda item: (item[1], item[0])
        )
        remaining = [item for item in remaining if item[1] >= watermark]
        effects.append([item[0] for item in due])
        for _, timestamp, key, value in due:
            index = timestamp // 10
            state[index, key] = state.get((index, key), "") + value
    for _, timestamp, key, value in sorted(remaining, key=lambda item: (item[1], item[0])):
        index = timestamp // 10
        state[index, key] = state.get((index, key), "") + value
    return effects, [(key, index, value) for (index, key), value in sorted(state.items())]


@pytest.mark.parametrize("seed", range(12))
def test_seeded_order_matches_independent_stable_oracle(seed: int) -> None:
    rng = random.Random(seed)
    records = [
        (position, rng.choice((-8, -5, 0, 2, 2, 8)), rng.choice(("a", "b")), str(position))
        for position in range(12)
    ]
    expected_effects, expected_rows = _oracle(records, [0, 5])
    runtime = OrderedWindowFoldRuntime(_spec())
    for position, timestamp, key, value in records:
        runtime.process(position, timestamp, FlowRecord(value, key))
    first = runtime.advance_watermark(0, next_position=len(records))
    assert [effect.position for effect in first.effects] == expected_effects[0]
    actual = list(runtime.drain(max_windows=2).rows)
    second = runtime.advance_watermark(5, next_position=len(records))
    assert [effect.position for effect in second.effects] == expected_effects[1]
    assert runtime.finish(next_position=len(records)).status.window.finished
    while runtime.status.window.pending_windows:
        actual.extend(runtime.drain(max_windows=2).rows)
    assert [(row.key, row.index, row.value) for row in actual] == expected_rows
    assert runtime.status.window.phase == "closed"


def test_equal_watermark_boundary_and_zero_effects() -> None:
    runtime = OrderedWindowFoldRuntime(_spec())
    runtime.process(0, 5, FlowRecord("A", "k"))
    assert runtime.advance_watermark(5, next_position=1).effects == ()
    saved = runtime.checkpoint().to_json()
    assert runtime.advance_watermark(5, next_position=1).effects == ()
    assert runtime.checkpoint().to_json() == saved
    assert [
        effect.position for effect in runtime.advance_watermark(6, next_position=1).effects
    ] == [0]
    assert runtime.finish(next_position=1).effects == ()


def test_failed_second_fold_rolls_back_entire_release_and_can_retry() -> None:
    fail = [True]

    def fold(state: str, value: str) -> str:
        if value == "B" and fail[0]:
            raise RuntimeError("injected")
        return state + value

    runtime = OrderedWindowFoldRuntime(_spec(fold=fold))
    runtime.process(0, 2, FlowRecord("A", "k"))
    runtime.process(1, 3, FlowRecord("B", "k"))
    before = runtime.checkpoint().to_json()
    with pytest.raises(ValidationError):
        runtime.advance_watermark(4, next_position=2)
    assert runtime.checkpoint().to_json() == before
    fail[0] = False
    assert [item.position for item in runtime.advance_watermark(4, next_position=2).effects] == [
        0,
        1,
    ]
    runtime.finish(next_position=2)
    assert runtime.drain().rows[0].value == "AB"


def test_limits_position_lateness_and_private_snapshot() -> None:
    runtime = OrderedWindowFoldRuntime(
        _spec(fold=lambda state, value: state + str(value)),
        limits=OrderedWindowLimits(max_buffer_records=1),
    )
    value = ["A"]
    runtime.process(0, 5, FlowRecord(value, "k"))
    value[0] = "mutated"
    with pytest.raises(ValidationError):
        runtime.process(1, 6, FlowRecord("B", "k"))
    with pytest.raises(ValidationError):
        runtime.process(2, 6, FlowRecord("B", "k"))
    assert runtime.status.next_position == 1
    runtime.advance_watermark(6, next_position=1)
    with pytest.raises(ValidationError):
        runtime.process(1, 5, FlowRecord("late", "k"))
    assert runtime.status.next_position == 1
    runtime.process(1, 6, FlowRecord("B", "k"))
    runtime.finish(next_position=2)
    assert runtime.drain().rows[0].value == "['A']B"


def test_checkpoint_restores_buffer_and_rejects_tampered_position() -> None:
    spec = _spec()
    runtime = OrderedWindowFoldRuntime(spec)
    runtime.process(0, 5, FlowRecord("A", "k"))
    runtime.process(1, 2, FlowRecord("B", "k"))
    runtime.advance_watermark(3, next_position=2)
    checkpoint = OrderedWindowCheckpoint.from_json(runtime.checkpoint().to_json())
    restored = OrderedWindowFoldRuntime.from_checkpoint(spec, checkpoint)
    assert restored.checkpoint().to_json() == runtime.checkpoint().to_json()
    for instance in (runtime, restored):
        instance.process(2, 4, FlowRecord("C", "k"))
        instance.finish(next_position=3)
    assert runtime.drain().rows[0].value == restored.drain().rows[0].value == "BCA"
    forged = checkpoint.to_dict()
    forged["body"]["next_position"] += 1
    forged["sha256"] = hashlib.sha256(
        json.dumps(
            forged["body"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()
    with pytest.raises(ValidationError):
        OrderedWindowCheckpoint.from_dict(forged)


def test_reentry_is_rejected_without_publish() -> None:
    runtime = None

    def fold(state: str, value: str) -> str:
        assert runtime is not None
        with pytest.raises(ValidationError):
            runtime.process(1, 3, FlowRecord("X", "k"))
        return state + value

    runtime = OrderedWindowFoldRuntime(_spec(fold=fold))
    runtime.process(0, 2, FlowRecord("A", "k"))
    runtime.advance_watermark(3, next_position=1)
    assert runtime.status.next_position == 1
