"""Original event-window folds and atomic local lifecycle contracts."""

from dataclasses import replace

import pytest

import stream_quilt.window_fold as module
from stream_quilt import (
    FlowRecord,
    ValidationError,
    WindowBatch,
    WindowCheckpoint,
    WindowFold,
    WindowFoldExecutionError,
    WindowFoldLimits,
    WindowFoldRuntime,
    WindowProcessResult,
    WindowRow,
    WindowStatus,
)


def config(**kwargs):
    options = {"width": 5, "initial": lambda: 0, "fold": lambda s, v: s + v}
    options.update(kwargs)
    return WindowFold("sum", "v1", **options)


def snapshot(runtime):
    return runtime.checkpoint().to_json()


def counters(runtime):
    return runtime.checkpoint().to_dict()["body"]["counters"]


def test_missing_window_fold_api() -> None:
    spec = WindowFold("sum", "v1", width=5, initial=lambda: 0, fold=lambda s, v: s + v)
    runtime = WindowFoldRuntime(spec)
    assert runtime.process(0, FlowRecord(3, "a")).memberships == 1
    runtime.finish()
    assert runtime.drain().rows[0].value == 3


def test_normalized_configuration_and_callback_revision_identity():
    assert config().hop == 5
    assert config().identity == config(hop=5).identity
    assert config().identity == config(fold=lambda s, v: 99).identity
    assert config().identity != replace(config(), revision="v2").identity
    assert config().identity != config(finalize=lambda s: s).identity


@pytest.mark.parametrize(
    "field,value",
    [
        ("width", 0),
        ("width", True),
        ("width", 1.0),
        ("width", 2**53),
        ("hop", 0),
        ("hop", -1),
        ("hop", False),
        ("hop", 2**53),
        ("origin", 2**53),
        ("origin", -(2**53)),
        ("origin", False),
        ("tick_unit", " ms "),
        ("tick_unit", ""),
        ("tick_unit", "x" * 1025),
        ("tick_unit", "\ud800"),
        ("late_policy", "accept"),
        ("late_policy", None),
        ("initial", None),
        ("fold", 42),
        ("finalize", False),
        ("limits", {}),
    ],
)
def test_invalid_configuration(field, value):
    with pytest.raises(ValidationError):
        config(**{field: value})


@pytest.mark.parametrize("field", list(module._CEILINGS))
@pytest.mark.parametrize("value", [0, -1, True, 1.0, "1"])
def test_limit_types_and_lower_bound(field, value):
    with pytest.raises(ValidationError):
        replace(WindowFoldLimits(), **{field: value})


@pytest.mark.parametrize("field,ceiling", module._CEILINGS.items())
def test_limit_hard_ceiling(field, ceiling):
    with pytest.raises(ValidationError):
        replace(WindowFoldLimits(), **{field: ceiling + 1})


def test_limit_cross_constraints_and_async_configuration():
    with pytest.raises(ValidationError, match="key limit"):
        config(limits=WindowFoldLimits(max_windows=1))
    with pytest.raises(ValidationError, match="one window row"):
        config(limits=WindowFoldLimits(max_batch_bytes=1))

    async def wrong(*args):
        return None

    class AsyncCallable:
        async def __call__(self, *args):
            return None

    for field in ("initial", "fold", "finalize"):
        for function in (wrong, AsyncCallable()):
            with pytest.raises(ValidationError, match="synchronous"):
                config(**{field: function})


def test_explicit_watermark_and_eof_barrier_no_clock_or_callback():
    calls = []
    runtime = WindowFoldRuntime(config(finalize=lambda s: calls.append(s) or s))
    assert runtime.status == WindowStatus("open", None, False, 0, 0)
    runtime.process(2, FlowRecord(3, "a"))
    assert runtime.status.watermark is None
    assert runtime.advance_watermark(4).phase == "open"
    before = snapshot(runtime)
    assert runtime.advance_watermark(4).watermark == 4
    assert snapshot(runtime) == before
    with pytest.raises(ValidationError, match="regress"):
        runtime.advance_watermark(3)
    assert runtime.advance_watermark(5).phase == "draining"
    assert calls == []
    before = snapshot(runtime)
    for operation in (
        lambda: runtime.process(5, FlowRecord(1, "b")),
        lambda: runtime.advance_watermark(5),
        lambda: runtime.advance_watermark(10),
    ):
        with pytest.raises(ValidationError):
            operation()
        assert snapshot(runtime) == before
    assert runtime.drain().rows[0].value == 3
    assert runtime.status.phase == "open"
    runtime.process(5, FlowRecord(7, "a"))
    assert runtime.finish().phase == "draining"
    assert runtime.status.watermark == 5
    before = snapshot(runtime)
    assert runtime.finish().finished
    assert snapshot(runtime) == before
    assert runtime.drain().rows[0].value == 7
    assert runtime.status.phase == "closed"
    before = snapshot(runtime)
    assert runtime.finish().phase == "closed"
    assert runtime.drain().rows == ()
    assert snapshot(runtime) == before
    with pytest.raises(ValidationError):
        runtime.process(50, FlowRecord(9, "a"))
    with pytest.raises(ValidationError):
        runtime.advance_watermark(50)


def test_empty_finish_and_finish_during_watermark_drain():
    empty = WindowFoldRuntime(config())
    assert empty.drain().rows == ()
    assert empty.finish() == WindowStatus("closed", None, True, 0, 0)
    runtime = WindowFoldRuntime(config())
    runtime.process(0, FlowRecord(1, "a"))
    runtime.process(10, FlowRecord(2, "a"))
    assert runtime.advance_watermark(5).pending_windows == 1
    assert runtime.finish().pending_windows == 2
    assert [row.index for row in runtime.drain().rows] == [0, 2]


@pytest.mark.parametrize("drop", [False, True])
def test_late_classification_before_gap_and_equality(drop):
    runtime = WindowFoldRuntime(config(width=2, hop=5, late_policy="drop" if drop else "reject"))
    runtime.advance_watermark(4)
    before = snapshot(runtime)
    if drop:
        assert runtime.process(3, FlowRecord(5, "a")).outcome == "late_dropped"
        assert counters(runtime)["late_drops"] == 1
        assert counters(runtime)["gap_inputs"] == 0
    else:
        with pytest.raises(ValidationError, match="watermark"):
            runtime.process(3, FlowRecord(5, "a"))
        assert snapshot(runtime) == before
    assert runtime.process(4, FlowRecord(2, "a")).outcome == "gap"
    assert runtime.process(5, FlowRecord(3, "a")).outcome == "folded"
    runtime.finish()
    assert runtime.drain().rows[0].value == 3


def test_null_state_is_retained_and_each_read_is_owned():
    calls = []
    runtime = WindowFoldRuntime(
        config(initial=lambda: calls.append("initial"), fold=lambda s, v: None)
    )
    runtime.process(0, FlowRecord(None, "a"))
    runtime.process(1, FlowRecord(None, "a"))
    assert calls == ["initial"]
    runtime.finish()
    row = runtime.drain().rows[0]
    assert row.value is None and row.input_count == 2
    row = WindowRow("a", -1, -5, 0, "tick", 1, {"items": [1]})
    row.value["items"].append(9)
    row.to_dict()["value"]["items"].append(8)
    assert row.value == {"items": [1]}
    assert row.to_record().value == {
        "index": -1,
        "start": -5,
        "end": 0,
        "tick_unit": "tick",
        "input_count": 1,
        "value": {"items": [1]},
    }


def test_overlapping_callbacks_have_isolated_initializer_state_and_input():
    initial = []
    original = {"values": [2]}
    inputs = []

    def folder(state, value):
        inputs.append(list(value["values"]))
        state.append(value["values"].pop())
        return state

    runtime = WindowFoldRuntime(config(width=6, hop=2, initial=lambda: initial, fold=folder))
    runtime.process(1, FlowRecord(original, "a"))
    assert initial == [] and original == {"values": [2]}
    assert inputs == [[2], [2], [2]]
    runtime.finish()
    assert [row.value for row in runtime.drain().rows] == [[2], [2], [2]]


@pytest.mark.parametrize("operation", ["process", "advance", "finish", "drain", "checkpoint"])
@pytest.mark.parametrize("phase", ["initial", "fold", "finalize"])
def test_all_callback_phases_reject_reentry(operation, phase):
    runtime = None

    def reenter(*args):
        actions = {
            "process": lambda: runtime.process(0, FlowRecord(1, "a")),
            "advance": lambda: runtime.advance_watermark(0),
            "finish": runtime.finish,
            "drain": runtime.drain,
            "checkpoint": runtime.checkpoint,
        }
        actions[operation]()

    runtime = WindowFoldRuntime(config(**{phase: reenter}))
    if phase == "finalize":
        runtime.process(0, FlowRecord(1, "a"))
        runtime.finish()
    before = snapshot(runtime)
    with pytest.raises(WindowFoldExecutionError) as error:
        runtime.drain() if phase == "finalize" else runtime.process(0, FlowRecord(1, "a"))
    assert error.value.phase == phase
    assert "reentrant" in str(error.value.__cause__)
    assert snapshot(runtime) == before


@pytest.mark.parametrize("phase", ["initial", "fold", "finalize"])
@pytest.mark.parametrize("error", [KeyboardInterrupt, SystemExit, MemoryError, RuntimeError])
def test_callback_failure_preserves_whole_operation(phase, error):
    calls = 0

    def fail_later(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise error("later window")
        return 1

    runtime = WindowFoldRuntime(config(width=6, hop=2, **{phase: fail_later}))
    if phase == "finalize":
        runtime.process(0, FlowRecord(1, "a"))
        runtime.finish()
    before = snapshot(runtime)
    expected = (
        error if issubclass(error, (KeyboardInterrupt, SystemExit)) else WindowFoldExecutionError
    )
    with pytest.raises(expected):
        runtime.drain() if phase == "finalize" else runtime.process(0, FlowRecord(1, "a"))
    assert snapshot(runtime) == before
    assert calls == 2


@pytest.mark.parametrize("phase", ["initial", "fold", "finalize"])
def test_returned_native_coroutines_are_closed(phase):
    returned = []

    async def bad():
        return 1

    def wrong(*args):
        coroutine = bad()
        returned.append(coroutine)
        return coroutine

    runtime = WindowFoldRuntime(config(**{phase: wrong}))
    if phase == "finalize":
        runtime.process(0, FlowRecord(1, "a"))
        runtime.finish()
    before = snapshot(runtime)
    with pytest.raises(WindowFoldExecutionError):
        runtime.drain() if phase == "finalize" else runtime.process(0, FlowRecord(1, "a"))
    assert all(coroutine.cr_frame is None for coroutine in returned)
    assert snapshot(runtime) == before


@pytest.mark.parametrize("timestamp", [True, 1.0, None, "1", 2**53, -(2**53)])
def test_timestamp_admission_precedes_callbacks(timestamp):
    def forbidden(*args):
        pytest.fail("callback before admission")

    runtime = WindowFoldRuntime(config(initial=forbidden, fold=forbidden))
    before = snapshot(runtime)
    with pytest.raises(ValidationError):
        runtime.process(timestamp, FlowRecord(1, "a"))
    assert snapshot(runtime) == before


@pytest.mark.parametrize(
    "options,timestamp",
    [
        ({"width": 2**53 - 1, "hop": 1}, 0),
        ({"width": 1, "hop": 1}, 2**53 - 1),
        ({"width": 1, "hop": 1, "origin": 2**53 - 1}, -(2**53 - 1)),
    ],
)
def test_geometry_and_fanout_preflight_no_callbacks(options, timestamp):
    def forbidden(*args):
        pytest.fail("callback before geometry admission")

    runtime = WindowFoldRuntime(config(**options, initial=forbidden, fold=forbidden))
    before = snapshot(runtime)
    with pytest.raises(ValidationError):
        runtime.process(timestamp, FlowRecord(1, "a"))
    assert snapshot(runtime) == before


def test_negative_extreme_is_not_rejected_for_sign():
    runtime = WindowFoldRuntime(config(width=1))
    runtime.process(-(2**53 - 1), FlowRecord(1, "a"))
    runtime.finish()
    row = runtime.drain().rows[0]
    assert (row.index, row.start, row.end) == (-(2**53 - 1), -(2**53 - 1), -(2**53 - 2))


@pytest.mark.parametrize("limit", ["max_keys", "max_windows", "max_inputs"])
def test_known_capacity_failure_has_no_extra_callback(limit):
    calls = []
    limits = replace(WindowFoldLimits(), max_keys=1, **({limit: 1} if limit != "max_keys" else {}))
    runtime = WindowFoldRuntime(config(limits=limits, fold=lambda s, v: calls.append(v) or v))
    runtime.process(0, FlowRecord(1, "a"))
    before = snapshot(runtime)
    with pytest.raises(ValidationError):
        runtime.process(5, FlowRecord(2, "b" if limit == "max_keys" else "a"))
    assert calls == [1]
    assert snapshot(runtime) == before


@pytest.mark.parametrize("record", [None, {}, FlowRecord(1), FlowRecord("long", "a")])
def test_record_and_byte_boundary(record):
    runtime = WindowFoldRuntime(config(limits=replace(WindowFoldLimits(), max_input_bytes=1)))
    before = snapshot(runtime)
    with pytest.raises(ValidationError):
        runtime.process(0, record)
    assert snapshot(runtime) == before


def test_reservation_selects_before_callbacks_and_failed_row_retains_ownership():
    calls = []
    fail = True

    def finalize(state):
        calls.append(state)
        return "x" * 500 if fail and state == 2 else state

    limits = replace(WindowFoldLimits(), max_row_bytes=200, max_batch_bytes=400)
    runtime = WindowFoldRuntime(config(limits=limits, finalize=finalize))
    for number in range(4):
        runtime.process(number * 5, FlowRecord(number, "a"))
    runtime.finish()
    assert [row.value for row in runtime.drain(max_windows=100).rows] == [0, 1]
    assert calls == [0, 1]
    before = snapshot(runtime)
    with pytest.raises(ValidationError, match="row byte"):
        runtime.drain()
    assert calls == [0, 1, 2]
    assert snapshot(runtime) == before
    fail = False
    assert [row.value for row in runtime.drain().rows] == [2, 3]
    assert runtime.status.phase == "closed"


@pytest.mark.parametrize("slot", ["WindowProcessResult", "WindowBatch"])
def test_complete_return_allocation_precedes_publication(monkeypatch, slot):
    runtime = WindowFoldRuntime(config())
    if slot == "WindowBatch":
        runtime.process(0, FlowRecord(1, "a"))
        runtime.finish()
    before = snapshot(runtime)

    def fail(*args, **kwargs):
        raise MemoryError("return allocation")

    with monkeypatch.context() as patch:
        patch.setattr(module, slot, fail)
        with pytest.raises(MemoryError, match="return allocation"):
            runtime.drain() if slot == "WindowBatch" else runtime.process(0, FlowRecord(1, "a"))
    assert snapshot(runtime) == before


def test_index_allocation_failure_precedes_publication(monkeypatch):
    runtime = WindowFoldRuntime(config())
    before = snapshot(runtime)

    def fail(*args, **kwargs):
        raise MemoryError("index allocation")

    with monkeypatch.context() as patch:
        patch.setattr(module, "dict", fail, raising=False)
        with pytest.raises(MemoryError, match="index allocation"):
            runtime.process(0, FlowRecord(1, "a"))
    assert snapshot(runtime) == before


def test_exact_identity_row_and_cell_wire_limits():
    probe = WindowFoldRuntime(config(fold=lambda s, v: v))
    value = {"quotes": '"\\\n😀'}
    probe.process(0, FlowRecord(value, "é"))
    cell = probe.checkpoint().to_dict()["body"]["cells"][0]
    cell_bytes = len(module._wire(cell).encode())
    probe.finish()
    row_bytes = probe.drain().rows[0].byte_size
    for reduction in (0, 1):
        limits = replace(
            WindowFoldLimits(),
            max_state_bytes=cell_bytes - reduction,
            max_row_bytes=row_bytes,
            max_batch_bytes=row_bytes,
        )
        runtime = WindowFoldRuntime(config(limits=limits, fold=lambda s, v: v))
        if reduction:
            with pytest.raises(ValidationError):
                runtime.process(0, FlowRecord(value, "é"))
        else:
            runtime.process(0, FlowRecord(value, "é"))
            runtime.finish()
            assert runtime.drain().rows[0].byte_size == row_bytes
    limits = replace(WindowFoldLimits(), max_row_bytes=row_bytes - 1)
    runtime = WindowFoldRuntime(config(limits=limits, fold=lambda s, v: v))
    before = snapshot(runtime)
    with pytest.raises(ValidationError, match="identity-finalized"):
        runtime.process(0, FlowRecord(value, "é"))
    assert snapshot(runtime) == before


def test_growing_sibling_can_use_later_shrinking_sibling_budget():
    phase = 0
    calls = 0

    def folder(state, value):
        nonlocal calls
        calls += 1
        return "x" * (100 if (calls % 2 == phase) else 1)

    runtime = WindowFoldRuntime(config(width=10, hop=5, fold=folder))
    runtime.process(0, FlowRecord(1, "a"))
    total = runtime._state.byte_size
    spec = replace(runtime.spec, limits=replace(runtime.spec.limits, max_state_bytes=total))
    other = WindowFoldRuntime(spec)
    calls = 0
    other.process(0, FlowRecord(1, "a"))
    phase = 1
    other.process(1, FlowRecord(1, "a"))
    assert other._state.byte_size == total


@pytest.mark.parametrize("value", [0, -1, True, 1.0, "1", 100001])
def test_invalid_empty_drain_argument(value):
    with pytest.raises(ValidationError):
        WindowFoldRuntime(config()).drain(max_windows=value)


def test_public_result_boundaries():
    status = WindowStatus("open", None, False, 0, 0)
    assert status.to_dict()["phase"] == "open"
    with pytest.raises(ValidationError):
        WindowProcessResult("folded", 0, status)
    with pytest.raises(ValidationError):
        WindowProcessResult("gap", 1, status)
    with pytest.raises(ValidationError):
        WindowBatch([], status)
    with pytest.raises(ValidationError):
        WindowBatch((None,), status)
    with pytest.raises(ValidationError):
        WindowStatus("closed", None, False, 0, 0)
    with pytest.raises(ValidationError):
        WindowStatus("open", None, True, 1, 0)
    with pytest.raises(ValidationError):
        WindowRow("a", 0, 1, 1, "tick", 1, None)
    with pytest.raises(ValidationError):
        WindowFoldRuntime({})
    with pytest.raises(ValidationError):
        WindowFoldRuntime.from_checkpoint(config(), {})
    with pytest.raises(ValidationError):
        WindowStatus("open", None, 1, 0, 0)
    with pytest.raises(ValidationError):
        WindowProcessResult("unknown", 0, status)
    with pytest.raises(ValidationError):
        WindowProcessResult("gap", 0, None)
    with pytest.raises(ValidationError):
        WindowBatch((), None)
    assert WindowBatch((), status).drained_windows == 0


def test_row_aggregate_depth_is_separate_from_record_conversion():
    value = 0
    for _ in range(63):
        value = [value]
    row = WindowRow("a", 0, 0, 1, "tick", 1, value)
    assert row.value == value
    with pytest.raises(ValidationError, match="depth"):
        row.to_record()


def test_real_hard_row_byte_boundary_includes_metadata():
    with pytest.raises(ValidationError, match="hard byte"):
        WindowRow("a", 0, 0, 1, "tick", 1, "x" * (8 * 1024 * 1024 - 2))


def test_batch_hard_admission_uses_complete_row_wire(monkeypatch):
    row = WindowRow("a", 0, 0, 1, "tick", 1, None)
    monkeypatch.setitem(module._CEILINGS, "max_batch_bytes", row.byte_size * 2 - 1)
    with pytest.raises(ValidationError, match="hard byte"):
        WindowBatch((row, row), WindowStatus("closed", None, True, 0, 0))


def test_multiple_proposals_aggregate_budget_rolls_back_callbacks():
    probe = WindowFoldRuntime(config())
    probe.process(0, FlowRecord(1, "a"))
    one_cell = probe._state.byte_size
    calls = []
    runtime = WindowFoldRuntime(
        config(
            limits=replace(WindowFoldLimits(), max_state_bytes=one_cell * 2 - 1),
            fold=lambda state, value: calls.append(value) or value,
        )
    )
    runtime.process(0, FlowRecord(1, "a"))
    before = snapshot(runtime)
    with pytest.raises(ValidationError, match="aggregate cell-wire"):
        runtime.process(0, FlowRecord(2, "b"))
    assert calls == [1, 2]
    assert snapshot(runtime) == before


@pytest.mark.parametrize("phase", ["initial", "fold", "finalize"])
@pytest.mark.parametrize("kind", ["nonfinite", "cycle", "oversized"])
def test_invalid_callback_json_and_value_budget_roll_back(phase, kind):
    def invalid(*args):
        if kind == "nonfinite":
            return float("nan")
        if kind == "cycle":
            result = []
            result.append(result)
            return result
        return "x" * 1000

    limits = replace(WindowFoldLimits(), max_state_value_bytes=50, max_row_bytes=200)
    runtime = WindowFoldRuntime(config(limits=limits, **{phase: invalid}))
    if phase == "finalize":
        runtime.process(0, FlowRecord(1, "a"))
        runtime.finish()
    before = snapshot(runtime)
    with pytest.raises(ValidationError):
        runtime.drain() if phase == "finalize" else runtime.process(0, FlowRecord(1, "a"))
    assert snapshot(runtime) == before


@pytest.mark.parametrize("operation", ["advance", "finish"])
def test_progress_return_allocation_before_state_swap(monkeypatch, operation):
    runtime = WindowFoldRuntime(config())
    runtime.process(0, FlowRecord(1, "a"))
    before = snapshot(runtime)
    original = module._status

    def fail_candidate(state):
        if state.watermark is not None or state.finished:
            raise MemoryError("status allocation")
        return original(state)

    with monkeypatch.context() as patch:
        patch.setattr(module, "_status", fail_candidate)
        with pytest.raises(MemoryError, match="status allocation"):
            runtime.advance_watermark(5) if operation == "advance" else runtime.finish()
    assert snapshot(runtime) == before


def test_checkpoint_detachment_and_original_semantic_callbacks_restored():
    runtime = WindowFoldRuntime(config())
    runtime.process(0, FlowRecord(2, "a"))
    checkpoint = runtime.checkpoint()
    document = checkpoint.to_dict()
    document["body"]["cells"].clear()
    assert len(checkpoint.to_dict()["body"]["cells"]) == 1
    restored = WindowFoldRuntime.from_checkpoint(
        config(), WindowCheckpoint.from_json(checkpoint.to_json())
    )
    restored.process(1, FlowRecord(3, "a"))
    restored.finish()
    assert restored.drain().rows[0].value == 5
