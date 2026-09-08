"""Independent keyed semantics and actual Windows-spawn concurrency checks."""

import os
import random
import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest

from stream_quilt import (
    Dataflow,
    FlowLimits,
    FlowRecord,
    FlowRuntime,
    FlowStep,
    StateFlatUpdate,
    StateUpdate,
    ValidationError,
)
from stream_quilt.partitioned_flow import LocalPartitionedFlow, LocalWorkerError, LocalWorkerLimits


def add(value, state):
    return StateUpdate(state + value, state + value)


def duplicate(value):
    return (value, value + 1)


def positive(value):
    return value >= 0


def accumulate_many(value, state):
    total = state + value
    return StateFlatUpdate(total, (total, -total))


def explode(value):
    raise RuntimeError("private callback payload must not appear: " + str(value))


def control(value):
    raise KeyboardInterrupt("private worker control detail")


def die(value):
    os._exit(17)


def wait_forever(value):
    Path(value).touch()
    while True:
        time.sleep(0.01)


def make_flow():
    return Dataflow("partitioned", "1", (FlowStep("sum", "stateful_map", add, int),))


def test_source_positions_are_not_shard_counts_and_restart_matches_serial():
    flow = make_flow()
    records = tuple(FlowRecord(value, key) for value, key in ((1, "a"), (2, "b"), (4, "a")))
    serial = FlowRuntime(flow)
    expected = [item.to_dict() for record in records for item in serial.process(record)]
    with LocalPartitionedFlow(flow, "source", "a" * 64, workers=2) as runtime:
        first = runtime.process_batch(0, records[:2])
        checkpoint = first.checkpoint
        assert checkpoint.next_position == sum(
            shard.processed_inputs for shard in checkpoint.shards
        )
        assert checkpoint.next_position == 2
    with LocalPartitionedFlow(
        flow, "source", "a" * 64, workers=2, checkpoint=checkpoint
    ) as runtime:
        second = runtime.process_batch(2, records[2:])
        assert [item.record.to_dict() for item in (*first.outputs, *second.outputs)] == expected
        assert [item.source_position for item in (*first.outputs, *second.outputs)] == [0, 1, 2]
        assert runtime.close_source(3).source_closed
        assert runtime.close_source(3) == runtime.checkpoint()


def barrier(value):
    folder = Path(value)
    (folder / str(os.getpid())).touch()
    end = time.monotonic() + 20
    while len(tuple(folder.iterdir())) < 2:
        if time.monotonic() >= end:
            raise RuntimeError("a serial worker dispatcher cannot pass this barrier")
        time.sleep(0.01)
    return os.getpid()


def test_workers_execute_concurrently_in_distinct_child_pids(tmp_path):
    flow = Dataflow("barrier", "1", (FlowStep("wait", "map", barrier),))
    with LocalPartitionedFlow(flow, "source", "a" * 64, workers=2) as runtime:
        keys = {}
        for index in range(100):
            key = str(index)
            keys.setdefault(runtime.partition_for(key), key)
        assert set(keys) == {0, 1}
        batch = runtime.process_batch(
            0, tuple(FlowRecord(str(tmp_path), key) for key in keys.values())
        )
        pids = {item.record.value for item in batch.outputs}
        assert len(pids) == 2 and os.getpid() not in pids
        assert pids == {item.pid for item in runtime.worker_status()}


@pytest.mark.parametrize("operator", ["key_by", "drop_key"])
def test_key_changing_operators_are_rejected_before_start(operator, monkeypatch):
    monkeypatch.setattr(
        "multiprocessing.process.BaseProcess.start", lambda _: pytest.fail("started")
    )
    flow = Dataflow("bad", "1", (FlowStep("key", operator, str if operator == "key_by" else None),))
    with pytest.raises(ValidationError):
        LocalPartitionedFlow(flow, "source", "a" * 64)


def test_global_wave_limits_and_rejected_input_preserve_checkpoint():
    with LocalPartitionedFlow(make_flow(), "source", "a" * 64, workers=2) as runtime:
        before = runtime.checkpoint()
        with pytest.raises(ValidationError):
            runtime.process_batch(1, (FlowRecord(1, "a"),))
        with pytest.raises(ValidationError):
            runtime.process_batch(0, (FlowRecord(1),))
        assert runtime.checkpoint() == before


def test_worker_count_is_checkpoint_identity_not_a_rescaling_switch():
    with LocalPartitionedFlow(make_flow(), "source", "a" * 64, workers=2) as runtime:
        checkpoint = runtime.checkpoint()
    with pytest.raises(ValidationError):
        LocalPartitionedFlow(make_flow(), "source", "a" * 64, workers=3, checkpoint=checkpoint)
    with pytest.raises(ValidationError):
        replace(LocalWorkerLimits(), wave_timeout=10**1000)


def test_seeded_five_operator_serial_oracle_and_portable_restart():
    flow = Dataflow(
        "oracle",
        "3",
        (
            FlowStep("filter", "filter", positive),
            FlowStep("duplicate", "flat_map", duplicate),
            FlowStep("sum", "stateful_map", add, int),
            FlowStep("many", "stateful_flat_map", accumulate_many, int),
            FlowStep("text", "map", str),
        ),
    )
    rng = random.Random(1832)
    inputs = tuple(
        FlowRecord(rng.randrange(-2, 8), rng.choice(("a", "b", "é", "中"))) for _ in range(50)
    )
    serial = FlowRuntime(flow)
    expected = [
        (position, ordinal, result.to_dict())
        for position, record in enumerate(inputs)
        for ordinal, result in enumerate(serial.process(record))
    ]
    actual = []
    checkpoint = None
    for start, stop in ((0, 17), (17, 50)):
        with LocalPartitionedFlow(
            flow, "source", "a" * 64, workers=3, checkpoint=checkpoint
        ) as runtime:
            batch = runtime.process_batch(start, inputs[start:stop])
            actual.extend(
                (row.source_position, row.output_index, row.record.to_dict())
                for row in batch.outputs
            )
            checkpoint = type(batch.checkpoint).from_json(batch.checkpoint.to_json())
    assert actual == expected
    assert checkpoint.next_position == 50
    assert {cell for shard in checkpoint.shards for cell in shard.cells} == set(
        serial.checkpoint().cells
    )


@pytest.mark.parametrize(
    "callback,reason", [(explode, "callback"), (control, "control"), (die, None)]
)
def test_real_child_failure_does_not_publish_or_leak_private_error(callback, reason):
    flow = Dataflow("failure", "1", (FlowStep("call", "map", callback),))
    with LocalPartitionedFlow(flow, "source", "a" * 64) as runtime:
        before = runtime.checkpoint()
        with pytest.raises(LocalWorkerError) as caught:
            runtime.process_batch(0, (FlowRecord("sensitive-419", "a"),))
        assert "sensitive" not in str(caught.value) and "private" not in str(caught.value)
        if reason:
            assert caught.value.reason == reason
        assert runtime.checkpoint() == before and runtime.phase == "failed"
        assert all(not child.alive for child in runtime.worker_status())
        with pytest.raises(ValidationError):
            runtime.process_batch(0, (FlowRecord(1, "a"),))


@pytest.mark.parametrize("cancel", [False, True])
def test_running_callback_timeout_or_external_cancel_settles_owned_workers(tmp_path, cancel):
    marker = tmp_path / "entered"
    flow = Dataflow("waiting", "1", (FlowStep("call", "map", wait_forever),))
    limits = replace(LocalWorkerLimits(), wave_timeout=20 if cancel else 1.0)
    with LocalPartitionedFlow(flow, "source", "a" * 64, limits=limits) as runtime:
        before = runtime.checkpoint()
        thread = None
        if cancel:

            def request_cancel():
                deadline = time.monotonic() + 10
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                runtime.cancel()

            thread = threading.Thread(target=request_cancel)
            thread.start()
        try:
            with pytest.raises(LocalWorkerError) as caught:
                runtime.process_batch(0, (FlowRecord(str(marker), "a"),))
            assert caught.value.reason == ("cancelled" if cancel else "deadline")
            assert runtime.checkpoint() == before
            assert all(not child.alive for child in runtime.worker_status())
            assert not any(
                t.name == "stream-quilt-worker-io" and t.is_alive() for t in threading.enumerate()
            )
        finally:
            if thread is not None:
                thread.join(15)
                assert not thread.is_alive()


def test_large_duplex_frames_do_not_block_owner_before_all_shards_dispatch():
    flow = Dataflow(
        "large", "1", (FlowStep("text", "map", str),), FlowLimits(max_record_bytes=3 * 1024 * 1024)
    )
    value = "x" * (1024 * 1024)
    with LocalPartitionedFlow(flow, "source", "a" * 64) as runtime:
        keys = {runtime.partition_for(str(i)): str(i) for i in range(50)}
        batch = runtime.process_batch(0, tuple(FlowRecord(value, key) for key in keys.values()))
        assert [row.record.value for row in batch.outputs] == [value, value]


def test_parent_control_preserves_identity_after_dispatch_and_cleanup(monkeypatch):
    from stream_quilt._local_worker import _Pool

    with LocalPartitionedFlow(make_flow(), "source", "a" * 64) as runtime:
        before = runtime.checkpoint()
        primary = KeyboardInterrupt("parent interrupt")

        def fail_wait(*args):
            raise primary

        monkeypatch.setattr(_Pool, "wait", fail_wait)
        with pytest.raises(KeyboardInterrupt) as caught:
            runtime.process_batch(0, (FlowRecord(1, "a"), FlowRecord(2, "b")))
        assert caught.value is primary
        assert runtime.checkpoint() == before
        assert all(not child.alive for child in runtime.worker_status())


def test_result_allocation_failure_rolls_back_all_completed_shards(monkeypatch):
    import stream_quilt.partitioned_flow as module

    with LocalPartitionedFlow(make_flow(), "source", "a" * 64) as runtime:
        before = runtime.checkpoint()

        def fail_batch(*args):
            raise MemoryError("allocation failed")

        monkeypatch.setattr(module, "PartitionedFlowBatch", fail_batch)
        with pytest.raises(MemoryError):
            runtime.process_batch(0, (FlowRecord(1, "a"), FlowRecord(2, "b")))
        assert runtime.checkpoint() == before
        assert all(not child.alive for child in runtime.worker_status())


def test_real_start_then_raise_retains_child_until_cleanup_finishes(monkeypatch):
    from multiprocessing.process import BaseProcess

    original = BaseProcess.start

    def late_failure(process):
        original(process)
        raise OSError("failure after actual spawn")

    monkeypatch.setattr(BaseProcess, "start", late_failure)
    with pytest.raises(OSError) as caught:
        LocalPartitionedFlow(make_flow(), "source", "a" * 64)
    handle = caught.value.local_worker_cleanup
    status = handle.worker_status()
    assert len(status) == 1 and status[0].pid is not None and not status[0].alive
    handle.close()
