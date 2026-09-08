"""Owned spawn processes; trusted startup pickle, bounded JSON operation frames."""

from __future__ import annotations

import io
import multiprocessing
import os

# Startup deliberately trusts caller code; runtime records/checkpoints never use pickle.
import pickle  # nosec B403
import threading
import time
from contextlib import suppress
from dataclasses import dataclass, field
from multiprocessing.process import BaseProcess
from typing import Any, Protocol

from .dataflow import Dataflow, FlowExecutionError, FlowRecord, FlowRuntime, _count
from .errors import ValidationError
from .partitioned_checkpoint import (
    LocalWorkerLimits,
    _flow,
    _json,
    _load,
    _shape,
    _shard_load,
    _shard_wire,
    partition_for,
)

_STARTUP_BYTES = 1024 * 1024
_REASONS = frozenset(
    {
        "callback",
        "control",
        "contract",
        "cancelled",
        "deadline",
        "startup_contract",
        "transport",
        "transport_contract",
        "transport_cleanup",
        "transport_still_alive",
        "worker_died",
        "worker_still_alive",
        "cleanup",
        "session_contract",
        "response_contract",
        "output_limit",
        "output_order",
        "output_contract",
        "shard_counts",
    }
)


class _Connection(Protocol):
    def send_bytes(self, buf: bytes) -> None: ...

    def recv_bytes(self, maxlength: int | None = None) -> bytes: ...

    def close(self) -> None: ...


class LocalWorkerError(ValidationError):
    """Privacy-minimized execution failure; inspect the last parent checkpoint."""

    def __init__(self, reason: str, worker: int | None = None) -> None:
        if type(reason) is not str or reason not in _REASONS:
            raise ValidationError("invalid local worker error classification")
        if worker is not None:
            _count(worker, "worker", 0, 7)
        self.reason, self.worker = reason, worker
        super().__init__(f"local worker execution failed ({reason})")


class _BoundedBuffer(io.BytesIO):
    def write(self, value: Any) -> int:
        if self.tell() + len(value) > _STARTUP_BYTES:
            raise ValidationError("trusted flow startup serialization exceeds 1 MiB")
        return super().write(value)


def _serialize_flow(flow: Dataflow) -> bytes:
    _flow(flow)
    try:
        with _BoundedBuffer() as target:
            pickle.Pickler(target, protocol=5).dump(flow)
            return target.getvalue()
    except Exception:
        raise ValidationError(
            "flow callbacks must support bounded trusted spawn serialization"
        ) from None


def _record(value: Any) -> FlowRecord:
    _shape(value, {"key", "value"})
    return FlowRecord(value["value"], value["key"])


def _copy_record(record: FlowRecord) -> FlowRecord:
    if type(record) is not FlowRecord or record.key is None:
        raise ValidationError("partitioned input must be keyed FlowRecord")
    partition_for(record.key, 1)
    if type(record._json) is not str:
        raise ValidationError("invalid encoded FlowRecord")
    value = _load(record._json, 8 * 1024 * 1024)
    return FlowRecord(value, record.key)


def _execute(
    flow: Dataflow,
    document: Any,
    worker: int,
    workers: int,
    limits: LocalWorkerLimits,
    session: str,
) -> bytes:
    _shape(document, {"session", "wave", "worker", "checkpoint", "items", "reply_limit"})
    _count(document["worker"], "worker", 0, workers - 1)
    if document["session"] != session or document["worker"] != worker:
        raise ValidationError("worker request identity mismatch")
    _count(document["wave"], "wave", 1, 2**53 - 1)
    reply_limit = _count(document["reply_limit"], "reply limit", 1, limits.max_message_bytes)
    items = document["items"]
    if type(items) is not list or not 1 <= len(items) <= limits.max_batch_inputs:
        raise ValidationError("invalid worker input batch")
    if len(items) * flow.limits.max_calls_per_input > limits.max_callback_reservation:
        raise ValidationError("worker callback reservation exceeded")
    checkpoint = _shard_load(document["checkpoint"], limits)
    runtime = FlowRuntime.from_checkpoint(flow, checkpoint)
    admitted = []
    previous = -1
    for item in items:
        _shape(item, {"position", "record"})
        position = _count(item["position"], "source position", previous + 1, 2**53 - 1)
        record = _record(item["record"])
        if record.key is None or partition_for(record.key, workers) != worker:
            raise ValidationError("input key belongs to another worker")
        admitted.append((position, record))
        previous = position
    rows: list[dict[str, Any]] = []
    wire_size = 0
    for position, record in admitted:
        result = runtime.process(record)
        if len(rows) + len(result) > limits.max_output_records:
            raise ValidationError("worker output count exceeded")
        for ordinal, output in enumerate(result):
            row = {"position": position, "ordinal": ordinal, "record": output.to_dict()}
            wire_size += len(_json(row, limits.max_output_bytes).encode("utf-8"))
            if wire_size > limits.max_output_bytes or wire_size > reply_limit:
                raise ValidationError("worker output wire exceeded")
            rows.append(row)
    response = {
        "status": "ok",
        "session": session,
        "wave": document["wave"],
        "worker": worker,
        "checkpoint": _shard_wire(runtime.checkpoint()),
        "outputs": rows,
    }
    return _json(response, reply_limit).encode("utf-8")


def _main(
    connection: _Connection,
    flow_bytes: bytes,
    identity: str,
    worker: int,
    workers: int,
    limits: LocalWorkerLimits,
    session: str,
) -> None:
    try:
        # This is explicitly trusted Python code supplied by the caller, never a wire import.
        flow = pickle.loads(flow_bytes)  # nosec B301
        _flow(flow)
        if flow.identity != identity:
            raise ValidationError("spawned flow identity changed")
        connection.send_bytes(
            _json(
                {"status": "ready", "worker": worker, "pid": os.getpid(), "session": session}, 1024
            ).encode()
        )
        while True:
            try:
                raw = connection.recv_bytes(limits.max_message_bytes)
            except EOFError:
                return
            try:
                response = _execute(
                    flow, _load(raw, limits.max_message_bytes), worker, workers, limits, session
                )
            except BaseException as error:
                reason = (
                    "callback"
                    if isinstance(error, FlowExecutionError)
                    else "control"
                    if not isinstance(error, Exception)
                    else "contract"
                )
                response = _json(
                    {"status": "error", "worker": worker, "session": session, "reason": reason},
                    1024,
                ).encode()
                connection.send_bytes(response)
                return
            connection.send_bytes(response)
    except BaseException:
        # No traceback, error arguments, callback data or startup pickle enters diagnostics.
        return
    finally:
        with suppress(BaseException):
            connection.close()


@dataclass(frozen=True, slots=True)
class LocalWorkerStatus:
    worker: int
    pid: int | None
    alive: bool
    exitcode: int | None

    def __post_init__(self) -> None:
        _count(self.worker, "worker", 0, 7)
        if self.pid is not None:
            _count(self.pid, "pid", 1, 2**53 - 1)
        if type(self.alive) is not bool or (self.alive and self.pid is None):
            raise ValidationError("invalid local worker alive state")
        if self.exitcode is not None:
            _count(self.exitcode, "exitcode", -(2**32), 2**32 - 1)
            if self.alive or self.pid is None:
                raise ValidationError("invalid local worker exit state")


@dataclass(slots=True)
class _Exchange:
    connection: _Connection
    maximum: int
    payload: bytes | None = None
    done: threading.Event = field(default_factory=threading.Event)
    result: bytes | None = None
    error: BaseException | None = None
    thread: threading.Thread | None = None

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="stream-quilt-worker-io", daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            if self.payload is not None:
                self.connection.send_bytes(self.payload)
            self.result = self.connection.recv_bytes(self.maximum)
        except BaseException as error:
            self.error = error
        finally:
            self.done.set()


@dataclass(slots=True)
class _Child:
    worker: int
    process: BaseProcess
    connection: _Connection
    pid: int | None = None
    started: bool = False
    process_closed: bool = False
    pipe_closed: bool = False
    exitcode: int | None = None
    exchange: _Exchange | None = None


class _Pool:
    def __init__(
        self,
        limits: LocalWorkerLimits,
        session: str,
        cancel: threading.Event,
    ) -> None:
        self.children: list[_Child] = []
        self._loose_pipes: list[_Connection] = []
        self.limits, self.cancel, self.session = limits, cancel, session

    def start(self, flow: Dataflow, flow_bytes: bytes, workers: int, deadline: float) -> None:
        context = multiprocessing.get_context("spawn")
        for index in range(workers):
            self.check(deadline)
            parent, child = context.Pipe(duplex=True)
            primary: BaseException | None = None
            member: _Child | None = None
            try:
                process = context.Process(
                    target=_main,
                    args=(
                        child,
                        flow_bytes,
                        flow.identity,
                        index,
                        workers,
                        self.limits,
                        self.session,
                    ),
                )
                member = _Child(index, process, parent)
                self.children.append(member)
                try:
                    process.start()
                finally:
                    # A start implementation can create the child and then raise.
                    member.pid = process.pid
                    member.started = member.pid is not None
            except BaseException as error:
                primary = error
                raise
            finally:
                # Record not-yet-owned pipe ends too, so a failed close is retryable.
                self._close_start_pipes((child, parent) if member is None else (child,), primary)
            member.exchange = _Exchange(parent, 1024)
            member.exchange.start()
        responses = self.wait(tuple(range(workers)), deadline)
        for index, raw in responses.items():
            message = _load(raw, 1024)
            _shape(message, {"status", "worker", "pid", "session"})
            _count(message["worker"], "worker", 0, workers - 1)
            _count(message["pid"], "pid", 1, 2**53 - 1)
            if message != {
                "status": "ready",
                "worker": index,
                "pid": self.children[index].pid,
                "session": self.session,
            }:
                raise LocalWorkerError("startup_contract", index)

    def _close_start_pipes(
        self, pipes: tuple[_Connection, ...], primary: BaseException | None
    ) -> None:
        # Register before trying close, including paths with no constructed process.
        self._loose_pipes.extend(pipes)
        errors = []
        for pipe in pipes:
            try:
                pipe.close()
            except BaseException as error:
                errors.append(error)
            else:
                self._loose_pipes.remove(pipe)
        _cleanup_errors(errors, primary)

    def check(self, deadline: float) -> None:
        if self.cancel.is_set():
            raise LocalWorkerError("cancelled")
        if time.monotonic() >= deadline:
            raise LocalWorkerError("deadline")

    def wait(self, active: tuple[int, ...], deadline: float) -> dict[int, bytes]:
        while True:
            self.check(deadline)
            complete = True
            for index in active:
                child = self.children[index]
                exchange = child.exchange
                if exchange is None:
                    raise LocalWorkerError("transport_contract", index)
                if exchange.done.is_set():
                    if exchange.error is not None:
                        raise LocalWorkerError("transport", index)
                else:
                    complete = False
                    if not child.process.is_alive():
                        raise LocalWorkerError("worker_died", index)
            if complete:
                break
            self.cancel.wait(min(0.01, max(0, deadline - time.monotonic())))
        result = {}
        for index in active:
            exchange = self.children[index].exchange
            if exchange is None or exchange.thread is None or exchange.result is None:
                raise LocalWorkerError("transport_contract", index)
            exchange.thread.join(max(0, deadline - time.monotonic()))
            if exchange.thread.is_alive():
                raise LocalWorkerError("transport_cleanup", index)
            result[index] = exchange.result
        self.check(deadline)
        return result

    def execute(
        self, requests: dict[int, bytes], quotas: dict[int, int], deadline: float
    ) -> dict[int, bytes]:
        for index, payload in requests.items():
            self.check(deadline)
            child = self.children[index]
            if not child.process.is_alive():
                raise LocalWorkerError("worker_died", index)
            child.exchange = _Exchange(child.connection, quotas[index], payload)
            child.exchange.start()
        # Do not await any shard until every participating shard has been dispatched.
        return self.wait(tuple(requests), deadline)

    def check_alive(self) -> None:
        for child in self.children:
            if child.process_closed or not child.process.is_alive():
                raise LocalWorkerError("worker_died", child.worker)

    def status(self) -> tuple[LocalWorkerStatus, ...]:
        rows = []
        for child in self.children:
            if child.process_closed or not child.started:
                alive, exitcode = False, child.exitcode
            else:
                # Exit can race a status read. Sample exit first, never report
                # alive=True together with an already observed terminal code.
                exitcode = child.process.exitcode
                alive = exitcode is None and child.process.is_alive()
            rows.append(LocalWorkerStatus(child.worker, child.pid, alive, exitcode))
        return tuple(rows)

    def cleanup(self, primary: BaseException | None = None) -> None:
        errors: list[BaseException] = []
        deadline = time.monotonic() + self.limits.cleanup_timeout

        def attempt(function: Any) -> Any:
            try:
                return function()
            except BaseException as error:
                errors.append(error)
                return None

        for pipe in tuple(self._loose_pipes):
            try:
                pipe.close()
            except BaseException as error:
                errors.append(error)
            else:
                self._loose_pipes.remove(pipe)
        for child in self.children:
            if not child.pipe_closed:
                try:
                    child.connection.close()
                except BaseException as error:
                    errors.append(error)
                else:
                    child.pipe_closed = True
        for child in self.children:
            process = child.process
            if child.started and not child.process_closed:
                attempt(lambda p=process: p.join(min(0.05, max(0, deadline - time.monotonic()))))
                if attempt(process.is_alive) is not False:
                    attempt(process.terminate)
                attempt(lambda p=process: p.join(min(0.05, max(0, deadline - time.monotonic()))))
                if attempt(process.is_alive) is not False:
                    attempt(process.kill)
                attempt(
                    lambda p=process: p.join(
                        max(0, (deadline - time.monotonic()) / max(1, len(self.children)))
                    )
                )
            exchange = child.exchange
            if exchange is not None and exchange.thread is not None:
                attempt(lambda t=exchange.thread: t.join(max(0, deadline - time.monotonic())))
                if exchange.thread.is_alive():
                    errors.append(LocalWorkerError("transport_still_alive", child.worker))
                else:
                    child.exchange = None
            if not child.process_closed:
                if child.started and attempt(process.is_alive) is not False:
                    errors.append(LocalWorkerError("worker_still_alive", child.worker))
                else:
                    try:
                        child.exitcode = process.exitcode if child.started else None
                        process.close()
                        child.process_closed = True
                    except BaseException as error:
                        errors.append(error)
        _cleanup_errors(errors, primary)


def _cleanup_errors(errors: list[BaseException], primary: BaseException | None) -> None:
    if not errors:
        return
    detail = "local worker cleanup did not finish cleanly; inspect worker_status and retry close"
    if primary is not None and not isinstance(primary, Exception):
        primary.add_note(detail)
        return
    control = next((error for error in errors if not isinstance(error, Exception)), None)
    if control is not None:
        control.add_note(detail)
        raise control
    if primary is not None:
        primary.add_note(detail)
    else:
        raise LocalWorkerError("cleanup")


@dataclass(frozen=True, slots=True)
class LocalWorkerCleanup:
    """Explicit owner-retaining retry handle attached to startup exceptions.

    A failed constructor cannot return its runtime. Its exception instead carries
    this capability as ``local_worker_cleanup``, even if initial cleanup succeeded.
    """

    _pool: _Pool = field(repr=False)
    _owner: int = field(repr=False)

    def __post_init__(self) -> None:
        if type(self._pool) is not _Pool:
            raise ValidationError("cleanup requires an owned worker pool")
        _count(self._owner, "owner thread", 1, 2**64 - 1)

    def worker_status(self) -> tuple[LocalWorkerStatus, ...]:
        return self._pool.status()

    def close(self) -> None:
        if threading.get_ident() != self._owner:
            raise ValidationError("startup cleanup has one owning thread")
        self._pool.cleanup()
