"""Pure transport stubs isolate parent atomicity and source lifecycle contracts.

Actual parallel spawn, pipe framing and process failure are tested separately in
test_partitioned_flow; these tests never claim their in-process stub is parallel.
"""

import inspect
import threading
from dataclasses import replace

import pytest

from stream_quilt import Dataflow, FlowRecord, FlowStep, ValidationError
from stream_quilt._local_worker import LocalWorkerError, _execute, _Pool
from stream_quilt.partitioned_checkpoint import LocalWorkerLimits, _json, _load
from stream_quilt.partitioned_flow import LocalPartitionedFlow


@pytest.fixture
def transport(monkeypatch):
    flow = Dataflow("stubbed-transport", "1", (FlowStep("text", "map", str),))
    monkeypatch.setattr(_Pool, "start", lambda *a: None)

    def execute(self, requests, quotas, deadline):
        return {
            index: _execute(
                flow, _load(raw, self.limits.max_message_bytes), index, 2, self.limits, self.session
            )
            for index, raw in requests.items()
        }

    monkeypatch.setattr(_Pool, "execute", execute)
    return flow


def test_pull_limit_is_not_eof_and_does_not_pull_extra_input(transport):
    pulled = []

    def source():
        for i in range(4):
            pulled.append(i)
            yield FlowRecord(i, "a")

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        iterator = source()
        batches = list(runtime.run(iterator, max_inputs=2, batch_size=1))
        assert pulled == [0, 1]
        assert len(batches) == 2 and not runtime.checkpoint().source_closed
        assert next(iterator).value == 2
        iterator.close()


def test_observed_eof_empty_source_and_repeated_close_are_not_synthetic_inputs(transport):
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        assert list(runtime.run([])) == []
        point = runtime.checkpoint()
        assert point.source_closed and point.next_position == point.waves == 0
        assert runtime.close_source(0) is point
        with pytest.raises(ValidationError):
            runtime.close_source(1)
        with pytest.raises(ValidationError):
            list(runtime.run([FlowRecord(1, "a")]))


def test_partial_batch_eof_published_when_iterator_is_fully_consumed(transport):
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        batches = list(runtime.run([FlowRecord(1, "a")], batch_size=3))
        assert len(batches) == 1 and batches[0].checkpoint.next_position == 1
        assert runtime.checkpoint().source_closed and runtime.checkpoint().waves == 1


def test_borrowed_iterator_is_not_closed_but_explicit_owned_generator_is(transport):
    closed = []

    def source():
        try:
            yield FlowRecord(1, "a")
            yield FlowRecord(2, "a")
        finally:
            closed.append(True)

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        borrowed = source()
        assert len(list(runtime.run(borrowed, max_inputs=1))) == 1
        assert not closed
        borrowed.close()
        owned = source()
        assert len(list(runtime.run(owned, max_inputs=1, own_source=True))) == 1
        assert closed == [True, True]


@pytest.mark.parametrize("primary", [RuntimeError("source"), KeyboardInterrupt("source")])
@pytest.mark.parametrize("lookup", [False, True])
def test_owned_cleanup_lookup_or_call_does_not_mask_source_primary(transport, primary, lookup):
    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            raise primary

        @property
        def close(self):
            if lookup:
                raise OSError("cleanup lookup")

            def fail():
                raise OSError("cleanup call")

            return fail

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        with pytest.raises(type(primary)) as caught:
            list(runtime.run(Source(), own_source=True))
        assert caught.value is primary and primary.__notes__
        assert runtime.checkpoint().next_position == 0


def test_owned_async_close_is_rejected_and_native_coroutine_closed(transport):
    returned = []

    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self):
            async def cleanup():
                return 1

            result = cleanup()
            returned.append(result)
            return result

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        with pytest.raises(ValidationError, match="synchronous"):
            list(runtime.run(Source(), own_source=True))
        assert inspect.getcoroutinestate(returned[0]) == inspect.CORO_CLOSED


def test_cancel_before_pull_never_advances_borrowed_source(transport):
    class Source:
        def __iter__(self):
            pytest.fail("consumed cancelled source")

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        runtime.cancel()
        with pytest.raises(LocalWorkerError, match="cancelled"):
            list(runtime.run(Source()))


def test_cancellation_during_return_allocation_rejects_parent_publication(transport, monkeypatch):
    import stream_quilt.partitioned_flow as module

    real = module.PartitionedFlowBatch
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        before = runtime.checkpoint()

        def allocate(*args):
            result = real(*args)
            runtime.cancel()
            return result

        monkeypatch.setattr(module, "PartitionedFlowBatch", allocate)
        with pytest.raises(LocalWorkerError, match="cancelled"):
            runtime.process_batch(0, (FlowRecord(1, "a"),))
        assert runtime.checkpoint() == before


@pytest.mark.parametrize(
    "budget,value",
    [
        ("max_callback_reservation", 1),
        ("max_input_bytes", 1),
        ("max_message_bytes", 1),
        ("max_output_records", 1),
        ("max_output_bytes", 1),
        ("max_result_bytes", 1),
    ],
)
def test_global_limits_never_publish_partial_wave(transport, budget, value):
    limits = replace(LocalWorkerLimits(), **{budget: value})
    with LocalPartitionedFlow(transport, "source", "a" * 64, limits=limits) as runtime:
        before = runtime.checkpoint()
        with pytest.raises(ValidationError):
            runtime.process_batch(0, (FlowRecord(1, "a"), FlowRecord(2, "b")))
        assert runtime.checkpoint() == before


@pytest.mark.parametrize(
    "change",
    [
        {"wave": True},
        {"worker": True},
        {"status": "no"},
        {"extra": 1},
        {"outputs": [{"position": 99, "ordinal": 0, "record": {"key": "a", "value": 1}}]},
        {"outputs": [{"position": 0, "ordinal": 1, "record": {"key": "a", "value": 1}}]},
        {"outputs": [{"position": 0, "ordinal": 0, "record": {"key": "other", "value": 1}}]},
        {"outputs": []},
    ],
)
def test_malformed_candidate_has_no_partial_parent_publication(transport, monkeypatch, change):
    original = _Pool.execute

    def altered(self, requests, quotas, deadline):
        replies = original(self, requests, quotas, deadline)
        index = next(iter(replies))
        message = _load(replies[index], quotas[index])
        message.update(change)
        replies[index] = _json(message, quotas[index]).encode()
        return replies

    monkeypatch.setattr(_Pool, "execute", altered)
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        before = runtime.checkpoint()
        with pytest.raises(ValidationError):
            runtime.process_batch(0, (FlowRecord(1, "a"),))
        assert runtime.checkpoint() == before and runtime.phase == "failed"


def test_owner_affinity_and_nonreentrant_checks(transport):
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        errors = []

        def other_thread():
            try:
                runtime.close()
            except ValidationError as error:
                errors.append(error)

        thread = threading.Thread(target=other_thread)
        thread.start()
        thread.join(2)
        assert len(errors) == 1
        with runtime._operation(), pytest.raises(ValidationError, match="not reentrant"):
            runtime.process_batch(0, (FlowRecord(1, "a"),))


def test_context_exit_preserves_first_control_and_reports_failed_close_phase(
    transport, monkeypatch
):
    primary = KeyboardInterrupt("primary")
    cleanup = SystemExit("cleanup")
    runtime = LocalPartitionedFlow(transport, "source", "a" * 64)
    original = _Pool.cleanup

    def fail(self, primary=None):
        raise cleanup

    monkeypatch.setattr(_Pool, "cleanup", fail)
    with pytest.raises(KeyboardInterrupt) as caught, runtime:
        raise primary
    assert caught.value is primary and runtime.phase == "closing"
    monkeypatch.setattr(_Pool, "cleanup", original)
    runtime.close()
    assert runtime.phase == "closed"


@pytest.mark.parametrize("stage", ["iter", "next", "yield"])
def test_pull_lease_rejects_reentrant_or_interleaved_source_advancement(transport, stage):
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:

        def attempt_advancement():
            with pytest.raises(ValidationError, match="pull"):
                runtime.process_batch(runtime.checkpoint().next_position, (FlowRecord(9, "a"),))
            with pytest.raises(ValidationError, match="pull"):
                runtime.close_source(runtime.checkpoint().next_position)

        class Source:
            def __iter__(self):
                if stage == "iter":
                    attempt_advancement()
                return self

            def __next__(self):
                if stage == "next":
                    attempt_advancement()
                return FlowRecord(1, "a")

        helper = runtime.run(Source(), max_inputs=2, batch_size=1)
        try:
            first = next(helper)
            if stage == "yield":
                attempt_advancement()
            assert first.start_position == 0 and first.checkpoint.next_position == 1
            assert next(helper).start_position == 1
            with pytest.raises(StopIteration):
                next(helper)
        finally:
            helper.close()


@pytest.mark.parametrize("field,value", [("workers", 3), ("limits", None), ("flow", None)])
def test_active_configuration_cannot_be_changed_after_worker_start(transport, field, value):
    with (
        LocalPartitionedFlow(transport, "source", "a" * 64) as runtime,
        pytest.raises(AttributeError),
    ):
        setattr(runtime, field, value)


def test_abandoned_pull_lease_is_explicit_and_teardown_prevents_later_pulls(transport):
    pulled = []

    def source():
        for i in range(3):
            pulled.append(i)
            yield FlowRecord(i, "a")

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        first = runtime.run(source(), batch_size=1)
        next(first)
        other = runtime.run(source(), batch_size=1)
        with pytest.raises(ValidationError, match="pull"):
            next(other)
        assert pulled == [0]
        runtime.close()
        with pytest.raises(ValidationError, match="not active"):
            next(first)
        assert pulled == [0]
        first.close()


def test_failed_iter_acquisition_releases_source_lease(transport):
    class Source:
        def __iter__(self):
            raise RuntimeError("source initialization")

    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        with pytest.raises(RuntimeError):
            list(runtime.run(Source(), own_source=True))
        assert runtime.process_batch(0, (FlowRecord(1, "a"),)).checkpoint.next_position == 1


def test_late_synchronous_result_allocation_is_rejected_by_whole_wave_deadline(
    transport, monkeypatch
):
    import stream_quilt.partitioned_flow as module

    clock = [100.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    real = module.PartitionedFlowBatch
    with LocalPartitionedFlow(transport, "source", "a" * 64) as runtime:
        before = runtime.checkpoint()

        def late(*args):
            result = real(*args)
            clock[0] += 40
            return result

        monkeypatch.setattr(module, "PartitionedFlowBatch", late)
        with pytest.raises(LocalWorkerError, match="deadline"):
            runtime.process_batch(0, (FlowRecord(1, "a"),))
        assert runtime.checkpoint() == before
