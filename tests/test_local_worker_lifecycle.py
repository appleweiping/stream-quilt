"""Deterministic independent transport/cleanup fault injection, no OS timing oracle."""

import os
import pickle
import threading
import time
from dataclasses import replace

import pytest

from stream_quilt import Dataflow, FlowRuntime, FlowStep, ValidationError
from stream_quilt._local_worker import (
    LocalWorkerCleanup,
    LocalWorkerError,
    LocalWorkerStatus,
    _Child,
    _cleanup_errors,
    _Exchange,
    _execute,
    _main,
    _Pool,
    _serialize_flow,
)
from stream_quilt.partitioned_checkpoint import LocalWorkerLimits, _json, _load, _shard_wire
from stream_quilt.partitioned_flow import LocalPartitionedFlow


class Pipe:
    def __init__(self, response=b"{}"):
        self.response = response
        self.closed = False
        self.error = None
        self.sent = []

    def close(self):
        if self.error is not None:
            raise self.error
        self.closed = True

    def send_bytes(self, value):
        self.sent.append(value)

    def recv_bytes(self, maxlength=None):
        return self.response


class Process:
    def __init__(self):
        self.pid = 123
        self.alive = True
        self.closed = False
        self.exitcode = None
        self.terminate_error = None
        self.kill_error = None
        self.close_error = None
        self.calls = []

    def is_alive(self):
        assert not self.closed, "touched a closed process"
        return self.alive

    def join(self, timeout):
        assert timeout >= 0
        self.calls.append("join")

    def terminate(self):
        self.calls.append("terminate")
        if self.terminate_error:
            raise self.terminate_error
        self.alive = False
        self.exitcode = -15

    def kill(self):
        self.calls.append("kill")
        if self.kill_error:
            raise self.kill_error
        self.alive = False
        self.exitcode = -9

    def close(self):
        self.calls.append("close")
        if self.close_error:
            raise self.close_error
        assert not self.alive
        self.closed = True


def pool_with_child():
    pool = _Pool(replace(LocalWorkerLimits(), cleanup_timeout=0.01), "session", threading.Event())
    process, pipe = Process(), Pipe()
    child = _Child(0, process, pipe, pid=process.pid, started=True)
    pool.children.append(child)
    return pool, child


def test_cleanup_escalates_after_terminate_error_but_reports_and_can_retry():
    pool, child = pool_with_child()
    child.process.terminate_error = OSError("OS detail")
    with pytest.raises(LocalWorkerError, match="cleanup"):
        pool.cleanup()
    assert not pool.status()[0].alive
    assert child.process.calls == ["join", "terminate", "join", "kill", "join", "close"]
    pool.cleanup()


def test_failed_kill_keeps_truthful_live_owner_then_retry_stops_it():
    pool, child = pool_with_child()
    child.process.terminate_error = OSError()
    child.process.kill_error = OSError()
    with pytest.raises(LocalWorkerError):
        pool.cleanup()
    assert pool.status()[0].alive and not child.process_closed
    child.process.kill_error = None
    child.process.terminate_error = None
    pool.cleanup()
    assert not pool.status()[0].alive


def test_pipe_close_failure_remains_retryable_after_process_was_closed():
    pool, child = pool_with_child()
    child.connection.error = OSError()
    with pytest.raises(LocalWorkerError):
        pool.cleanup()
    assert child.process_closed and not child.pipe_closed
    child.connection.error = None
    pool.cleanup()
    assert child.pipe_closed


def test_io_thread_failure_to_settle_remains_retryable_after_process_close():
    pool, child = pool_with_child()
    release = threading.Event()
    exchange = child.exchange = _Exchange(child.connection, 1024)
    exchange.thread = threading.Thread(target=release.wait, daemon=True)
    exchange.thread.start()
    try:
        with pytest.raises(LocalWorkerError):
            pool.cleanup()
        assert child.process_closed and child.exchange is exchange
    finally:
        release.set()
        exchange.thread.join(2)
    pool.cleanup()
    assert child.exchange is None


@pytest.mark.parametrize("primary", [None, RuntimeError("primary"), KeyboardInterrupt("primary")])
@pytest.mark.parametrize("cleanup", [OSError("cleanup"), SystemExit("cleanup")])
def test_cleanup_error_priority_preserves_first_genuine_control(primary, cleanup):
    if primary is not None and not isinstance(primary, Exception):
        _cleanup_errors([cleanup], primary)
        assert primary.__notes__
    elif not isinstance(cleanup, Exception):
        with pytest.raises(SystemExit) as caught:
            _cleanup_errors([cleanup], primary)
        assert caught.value is cleanup
    elif primary is not None:
        _cleanup_errors([cleanup], primary)
        assert primary.__notes__
    else:
        with pytest.raises(LocalWorkerError):
            _cleanup_errors([cleanup], primary)


def test_all_loose_startup_pipes_attempted_and_retryable():
    pool, _ = pool_with_child()
    first, second = Pipe(), Pipe()
    first.error = OSError()
    with pytest.raises(LocalWorkerError):
        pool._close_start_pipes((first, second), None)
    assert second.closed and pool._loose_pipes == [first]
    first.error = None
    pool.cleanup()
    assert first.closed and pool._loose_pipes == []


def test_startup_failure_exception_keeps_retry_capability(monkeypatch):
    child_holder = []
    primary = KeyboardInterrupt("owner")

    def fail_start(self, *args):
        _, child = pool_with_child()
        child.process.kill_error = OSError()
        child.process.terminate_error = OSError()
        child_holder.append(child)
        self.children.append(child)
        raise primary

    monkeypatch.setattr(_Pool, "start", fail_start)
    flow = Dataflow("startup", "1", (FlowStep("text", "map", str),))
    with pytest.raises(KeyboardInterrupt) as caught:
        LocalPartitionedFlow(flow, "source", "a" * 64)
    assert caught.value is primary
    handle = caught.value.local_worker_cleanup
    assert isinstance(handle, LocalWorkerCleanup) and handle.worker_status()[0].alive
    child_holder[0].process.terminate_error = None
    handle.close()
    assert not handle.worker_status()[0].alive


def test_exchange_is_owned_thread_and_preserves_transport_control_as_result():
    class ControlPipe(Pipe):
        def recv_bytes(self, maxlength=None):
            raise KeyboardInterrupt("transport")

    pipe = ControlPipe()
    exchange = _Exchange(pipe, 1024, b"request")
    exchange.start()
    assert exchange.done.wait(2)
    exchange.thread.join(2)
    assert pipe.sent == [b"request"]
    assert isinstance(exchange.error, KeyboardInterrupt)


def test_wait_rejects_dead_child_incomplete_exchange_and_completed_transport_error():
    pool, child = pool_with_child()
    child.exchange = _Exchange(child.connection, 1024)
    child.process.alive = False
    with pytest.raises(LocalWorkerError, match="worker_died"):
        pool.wait((0,), time.monotonic() + 1)
    child.exchange.error = OSError()
    child.exchange.done.set()
    with pytest.raises(LocalWorkerError, match="transport"):
        pool.wait((0,), time.monotonic() + 1)
    pool.cleanup()


def test_completed_exchange_respects_deadline_and_cancellation_before_acceptance():
    pool, child = pool_with_child()
    child.exchange = _Exchange(child.connection, 1024)
    child.exchange.start()
    assert child.exchange.done.wait(2)
    assert pool.wait((0,), time.monotonic() + 1) == {0: b"{}"}
    with pytest.raises(LocalWorkerError, match="deadline"):
        pool.wait((0,), 0)
    pool.cancel.set()
    with pytest.raises(LocalWorkerError, match="cancelled"):
        pool.wait((0,), time.monotonic() + 1)
    pool.cleanup()


@pytest.mark.parametrize(
    "change",
    [
        {"worker": True},
        {"worker": 2},
        {"session": "bad"},
        {"wave": True},
        {"reply_limit": False},
        {"items": []},
        {"extra": 1},
        {"items": [{"position": 0, "record": {"key": None, "value": 1}}]},
        {"items": [{"position": False, "record": {"key": "a", "value": 1}}]},
    ],
)
def test_worker_operation_protocol_has_strict_shape_numbers_and_keys(change):
    flow = Dataflow("wire", "1", (FlowStep("text", "map", str),))
    request = {
        "session": "s",
        "wave": 1,
        "worker": 0,
        "checkpoint": _shard_wire(FlowRuntime(flow).checkpoint()),
        "items": [{"position": 0, "record": {"key": "a", "value": 1}}],
        "reply_limit": 1024,
    }
    request.update(change)
    with pytest.raises(ValidationError):
        _execute(flow, request, 0, 1, LocalWorkerLimits(), "s")


def test_worker_operation_wire_matches_direct_runtime_and_exact_json():
    flow = Dataflow("wire", "1", (FlowStep("text", "map", str),))
    request = {
        "session": "s",
        "wave": 1,
        "worker": 0,
        "checkpoint": _shard_wire(FlowRuntime(flow).checkpoint()),
        "items": [{"position": 19, "record": {"key": "a", "value": 4}}],
        "reply_limit": 1024,
    }
    raw = _execute(flow, request, 0, 1, LocalWorkerLimits(), "s")
    response = _load(raw, 1024)
    assert response["outputs"] == [
        {"position": 19, "ordinal": 0, "record": {"key": "a", "value": "4"}}
    ]
    assert response["checkpoint"]["processed_inputs"] == 1
    assert _json(response, 1024).encode() == raw


def test_trusted_startup_serialization_rejects_lambda_without_starting_workers():
    flow = Dataflow("unportable", "1", (FlowStep("map", "map", lambda v: v),))
    with pytest.raises(ValidationError, match="trusted spawn"):
        _serialize_flow(flow)


@pytest.mark.parametrize(
    "args",
    [
        (True, 123, True, None),
        (0, None, True, None),
        (0, 123, 1, None),
        (0, 123, True, 0),
        (0, None, False, 0),
        (0, 0, False, None),
    ],
)
def test_worker_status_has_strict_diagnostic_contract(args):
    with pytest.raises(ValidationError):
        LocalWorkerStatus(*args)


class WorkerPipe(Pipe):
    def __init__(self, requests):
        super().__init__()
        self.requests = iter(requests)

    def recv_bytes(self, maxlength=None):
        try:
            return next(self.requests)
        except StopIteration:
            raise EOFError from None


def test_child_entry_uses_trusted_startup_then_json_until_explicit_pipe_eof():
    flow = Dataflow("entry", "1", (FlowStep("text", "map", str),))
    request = {
        "session": "s",
        "wave": 1,
        "worker": 0,
        "checkpoint": _shard_wire(FlowRuntime(flow).checkpoint()),
        "items": [{"position": 0, "record": {"key": "a", "value": 1}}],
        "reply_limit": 1024,
    }
    pipe = WorkerPipe([_json(request, 4096).encode()])
    _main(pipe, _serialize_flow(flow), flow.identity, 0, 1, LocalWorkerLimits(), "s")
    assert pipe.closed and len(pipe.sent) == 2
    assert _load(pipe.sent[0], 1024) == {
        "status": "ready",
        "worker": 0,
        "pid": os.getpid(),
        "session": "s",
    }
    assert _load(pipe.sent[1], 1024)["outputs"][0]["record"]["value"] == "1"


@pytest.mark.parametrize("payload", [b"private invalid wire", b'{"extra":true}', b"NaN"])
def test_child_malformed_wire_returns_only_sanitized_error_and_closes(payload):
    flow = Dataflow("entry", "1", (FlowStep("text", "map", str),))
    pipe = WorkerPipe([payload])
    _main(pipe, _serialize_flow(flow), flow.identity, 0, 1, LocalWorkerLimits(), "s")
    assert pipe.closed and len(pipe.sent) == 2
    assert _load(pipe.sent[-1], 1024) == {
        "status": "error",
        "worker": 0,
        "session": "s",
        "reason": "contract",
    }


@pytest.mark.parametrize("bad", ["pickle", "type", "identity"])
def test_child_rejects_invalid_trusted_startup_without_emitting_raw_details(bad):
    flow = Dataflow("entry", "1", (FlowStep("text", "map", str),))
    encoded = (
        b"secret malformed"
        if bad == "pickle"
        else pickle.dumps(1)
        if bad == "type"
        else _serialize_flow(flow)
    )
    pipe = WorkerPipe([])
    _main(pipe, encoded, "0" * 64, 0, 1, LocalWorkerLimits(), "s")
    assert pipe.closed and not pipe.sent


@pytest.mark.parametrize("bad", [None, "worker_bool", "session", "pid"])
def test_pool_start_admits_strict_ready_frames_and_closes_child_pipe(monkeypatch, bad):
    import stream_quilt._local_worker as module

    pool = _Pool(LocalWorkerLimits(), "s", threading.Event())
    endpoints = []

    class Context:
        def Pipe(self, duplex):
            message = {"status": "ready", "worker": 0, "pid": 123, "session": "s"}
            if bad == "worker_bool":
                message["worker"] = False
            elif bad == "session":
                message["session"] = "wrong"
            elif bad == "pid":
                message["pid"] = 456
            pair = (Pipe(_json(message, 1024).encode()), Pipe())
            endpoints.extend(pair)
            return pair

        def Process(self, **kwargs):
            process = Process()
            process.start = lambda: None
            return process

    monkeypatch.setattr(module.multiprocessing, "get_context", lambda kind: Context())
    flow = Dataflow("entry", "1", (FlowStep("text", "map", str),))
    try:
        if bad is None:
            pool.start(flow, b"trusted", 1, time.monotonic() + 1)
            assert pool.status()[0].alive
        else:
            with pytest.raises(ValidationError):
                pool.start(flow, b"trusted", 1, time.monotonic() + 1)
        assert endpoints[1].closed
    finally:
        pool.cleanup()
    assert endpoints[0].closed


def test_process_factory_failure_closes_both_unowned_pipe_ends(monkeypatch):
    import stream_quilt._local_worker as module

    pool = _Pool(LocalWorkerLimits(), "s", threading.Event())
    endpoints = (Pipe(), Pipe())

    class Context:
        def Pipe(self, duplex):
            return endpoints

        def Process(self, **kwargs):
            raise OSError("process creation")

    monkeypatch.setattr(module.multiprocessing, "get_context", lambda kind: Context())
    flow = Dataflow("entry", "1", (FlowStep("text", "map", str),))
    with pytest.raises(OSError):
        pool.start(flow, b"trusted", 1, time.monotonic() + 1)
    assert all(endpoint.closed for endpoint in endpoints)
    pool.cleanup()


@pytest.mark.parametrize(
    "error,reason",
    [
        (KeyboardInterrupt("private"), "control"),
        (SystemExit("private"), "control"),
        (ValueError("private"), "contract"),
    ],
)
def test_child_controls_are_sanitized_not_raised_in_parent(monkeypatch, error, reason):
    import stream_quilt._local_worker as module

    flow = Dataflow("entry", "1", (FlowStep("text", "map", str),))

    def fail(*args):
        raise error

    monkeypatch.setattr(module, "_execute", fail)
    pipe = WorkerPipe([b"{}"])
    _main(pipe, _serialize_flow(flow), flow.identity, 0, 1, LocalWorkerLimits(), "s")
    assert pipe.closed
    assert _load(pipe.sent[-1], 1024)["reason"] == reason
    assert b"private" not in pipe.sent[-1]


def test_trusted_startup_size_cap_prevents_oversized_buffer_write():
    from stream_quilt._local_worker import _BoundedBuffer

    with _BoundedBuffer() as buffer:
        buffer.write(b"a" * (1024 * 1024))
        with pytest.raises(ValidationError, match="1 MiB"):
            buffer.write(b"b")
        assert buffer.tell() == 1024 * 1024


def test_missing_exchange_or_missing_result_is_not_accepted():
    pool, child = pool_with_child()
    with pytest.raises(LocalWorkerError, match="transport_contract"):
        pool.wait((0,), time.monotonic() + 1)
    child.exchange = _Exchange(child.connection, 1024)
    child.exchange.done.set()
    with pytest.raises(LocalWorkerError, match="transport_contract"):
        pool.wait((0,), time.monotonic() + 1)
    pool.cleanup()


def test_dead_idle_worker_or_dead_worker_at_dispatch_rejects_session():
    pool, child = pool_with_child()
    child.process.alive = False
    with pytest.raises(LocalWorkerError, match="worker_died"):
        pool.check_alive()
    with pytest.raises(LocalWorkerError, match="worker_died"):
        pool.execute({0: b"{}"}, {0: 1024}, time.monotonic() + 1)
    pool.cleanup()


def test_process_close_failure_is_retained_for_retry():
    pool, child = pool_with_child()
    child.process.close_error = OSError("close")
    with pytest.raises(LocalWorkerError):
        pool.cleanup()
    assert not child.process_closed and not pool.status()[0].alive
    child.process.close_error = None
    pool.cleanup()
    assert child.process_closed


def test_startup_handle_owner_affinity_and_strict_construction():
    pool, _ = pool_with_child()
    handle = LocalWorkerCleanup(pool, threading.get_ident())
    errors = []

    def other():
        try:
            handle.close()
        except ValidationError as error:
            errors.append(error)

    thread = threading.Thread(target=other)
    thread.start()
    thread.join(2)
    assert len(errors) == 1
    handle.close()
    with pytest.raises(ValidationError):
        LocalWorkerCleanup(None, 1)
    with pytest.raises(ValidationError):
        LocalWorkerError("private arbitrary message")


@pytest.mark.parametrize(
    "field,limit",
    [("max_callback_reservation", 1), ("max_output_records", 1), ("max_output_bytes", 1)],
)
def test_worker_enforces_reserved_work_and_aggregate_output(field, limit):
    flow = Dataflow("wire", "1", (FlowStep("text", "map", str),))
    request = {
        "session": "s",
        "wave": 1,
        "worker": 0,
        "checkpoint": _shard_wire(FlowRuntime(flow).checkpoint()),
        "items": [{"position": i, "record": {"key": "a", "value": i}} for i in range(2)],
        "reply_limit": 1024,
    }
    with pytest.raises(ValidationError):
        _execute(flow, request, 0, 1, replace(LocalWorkerLimits(), **{field: limit}), "s")
