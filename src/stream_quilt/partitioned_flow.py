"""Actual local keyed worker waves with a single parent publication point."""

from __future__ import annotations

import inspect
import threading
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

from ._local_worker import (
    LocalWorkerCleanup,
    LocalWorkerError,
    LocalWorkerStatus,
    _copy_record,
    _Pool,
    _record,
    _serialize_flow,
)
from .dataflow import _MAX_COUNT, Dataflow, FlowRecord, FlowRuntime, _count
from .errors import ValidationError
from .partitioned_checkpoint import (
    LocalWorkerLimits,
    PartitionedFlowCheckpoint,
    _flow,
    _hex,
    _json,
    _load,
    _name,
    _shape,
    _shard_load,
    _shard_wire,
    partition_for,
)


@dataclass(frozen=True, slots=True)
class PartitionedFlowOutput:
    sequence: int
    source_position: int
    output_index: int
    record: FlowRecord

    def __post_init__(self) -> None:
        for name in ("sequence", "source_position", "output_index"):
            _count(getattr(self, name), name, 0, _MAX_COUNT)
        if type(self.record) is not FlowRecord or self.record.key is None:
            raise ValidationError("partitioned output requires a keyed FlowRecord")
        _copy_record(self.record)


@dataclass(frozen=True, slots=True)
class PartitionedFlowBatch:
    start_position: int
    outputs: tuple[PartitionedFlowOutput, ...]
    checkpoint: PartitionedFlowCheckpoint

    def __post_init__(self) -> None:
        if type(self.checkpoint) is not PartitionedFlowCheckpoint:
            raise ValidationError("batch requires a partitioned checkpoint")
        self.checkpoint.__post_init__()
        _count(self.start_position, "batch start", 0, self.checkpoint.next_position)
        if (
            self.checkpoint.next_position - self.start_position
            > self.checkpoint.limits.max_batch_inputs
        ):
            raise ValidationError("batch input count exceeded")
        if (
            type(self.outputs) is not tuple
            or len(self.outputs) > self.checkpoint.limits.max_output_records
        ):
            raise ValidationError("batch output count exceeded")
        expected = self.checkpoint.emitted_records - len(self.outputs)
        previous = (-1, -1)
        wire_bytes = 0
        for output in self.outputs:
            if type(output) is not PartitionedFlowOutput:
                raise ValidationError("invalid partitioned output")
            output.__post_init__()
            pair = (output.source_position, output.output_index)
            if (
                not self.start_position <= output.source_position < self.checkpoint.next_position
                or pair <= previous
            ):
                raise ValidationError("partitioned output order or source range mismatch")
            if (
                output.output_index != (previous[1] + 1 if previous[0] == pair[0] else 0)
                or output.sequence != expected
            ):
                raise ValidationError("partitioned output sequence mismatch")
            wire_bytes += len(
                _json(
                    {
                        "position": output.source_position,
                        "ordinal": output.output_index,
                        "record": output.record.to_dict(),
                    },
                    self.checkpoint.limits.max_output_bytes,
                ).encode("utf-8")
            )
            if wire_bytes > self.checkpoint.limits.max_output_bytes:
                raise ValidationError("partitioned batch output wire exceeded")
            previous = pair
            expected += 1


class LocalPartitionedFlow:
    """One owner, a bounded wave in flight, and no automatic callback replay.

    Caller-supplied callbacks are trusted spawn-serializable Python. Relevant
    state must live in Flow state for serial equivalence; callback globals,
    external side effects, source seeking and durable delivery are caller-owned.
    """

    def __init__(
        self,
        flow: Dataflow,
        source_id: str,
        source_digest: str,
        *,
        workers: int = 2,
        limits: LocalWorkerLimits | None = None,
        checkpoint: PartitionedFlowCheckpoint | None = None,
    ) -> None:
        self._owner = threading.get_ident()
        self._busy = threading.Lock()
        self._publication = threading.Lock()
        self._pull_owner: object | None = None
        self._cancel = threading.Event()
        self._pool: _Pool | None = None
        self._phase = "starting"
        _flow(flow)
        _name(source_id, "source ID")
        _hex(source_digest)
        _count(workers, "workers", 1, 8)
        active = LocalWorkerLimits() if limits is None else limits
        if type(active) is not LocalWorkerLimits:
            raise ValidationError("limits must be LocalWorkerLimits")
        active.__post_init__()
        self._flow_definition, self._active_limits, self._worker_count = flow, active, workers
        if checkpoint is None:
            point = PartitionedFlowCheckpoint(
                flow.identity,
                source_id,
                source_digest,
                workers,
                active,
                0,
                0,
                False,
                tuple(FlowRuntime(flow).checkpoint() for _ in range(workers)),
            )
        else:
            if type(checkpoint) is not PartitionedFlowCheckpoint:
                raise ValidationError("checkpoint must be PartitionedFlowCheckpoint")
            point = PartitionedFlowCheckpoint.from_dict(checkpoint.to_dict())
            if (
                point.flow_identity,
                point.source_id,
                point.source_digest,
                point.workers,
                point.limits,
            ) != (flow.identity, source_id, source_digest, workers, active):
                raise ValidationError("source, flow, worker count or limits mismatch")
            point.validate_for(flow)
        self._point = point
        self._session = uuid.uuid4().hex
        deadline = time.monotonic() + active.startup_timeout
        with self._busy:
            # No process exists until configuration/checkpoint/startup serialization succeeds.
            flow_bytes = _serialize_flow(flow)
            if time.monotonic() >= deadline:
                raise LocalWorkerError("deadline")
            pool = self._pool = _Pool(active, self._session, self._cancel)
            try:
                try:
                    pool.start(flow, flow_bytes, workers, deadline)
                except BaseException as primary:
                    self._phase = "failed"
                    pool.cleanup(primary)
                    raise
            except BaseException as error:
                object.__setattr__(
                    error, "local_worker_cleanup", LocalWorkerCleanup(pool, self._owner)
                )
                raise
        self._phase = "ready"

    @contextmanager
    def _operation(self, lease: object | None = None) -> Iterator[None]:
        self._owned()
        if not self._busy.acquire(blocking=False):
            raise ValidationError("local partitioned runtime is not reentrant")
        try:
            if self._pull_owner is not None and self._pull_owner is not lease:
                raise ValidationError("a pull helper owns the source positions")
            if self._phase != "ready":
                raise ValidationError("local partitioned session is not active")
            yield
        finally:
            self._busy.release()

    def _owned(self) -> None:
        if threading.get_ident() != self._owner:
            raise ValidationError("local partitioned runtime has one owning thread")

    def partition_for(self, key: str) -> int:
        return partition_for(key, self.workers)

    def checkpoint(self) -> PartitionedFlowCheckpoint:
        """The last published snapshot; it remains readable after a failed wave."""
        return self._point

    def worker_status(self) -> tuple[LocalWorkerStatus, ...]:
        return () if self._pool is None else self._pool.status()

    @property
    def phase(self) -> str:
        return self._phase

    @property
    def flow(self) -> Dataflow:
        return self._flow_definition

    @property
    def limits(self) -> LocalWorkerLimits:
        return self._active_limits

    @property
    def workers(self) -> int:
        return self._worker_count

    def cancel(self) -> None:
        """Thread-safe request; the owner settles/tears down an in-flight wave."""
        with self._publication:
            self._cancel.set()

    def process_batch(
        self, start_position: int, records: tuple[FlowRecord, ...]
    ) -> PartitionedFlowBatch:
        return self._process_batch(start_position, records)

    def _process_batch(
        self, start_position: int, records: tuple[FlowRecord, ...], lease: object | None = None
    ) -> PartitionedFlowBatch:
        with self._operation(lease):
            deadline = time.monotonic() + self.limits.wave_timeout
            batch = self._candidate_batch(self._point, start_position, records, deadline)
            pool = self._pool
            if pool is None:
                raise LocalWorkerError("session_contract")
            try:
                with self._publication:
                    pool.check(deadline)
                    pool.check_alive()
                    self._point = batch.checkpoint  # The only in-memory wave publication point.
                return batch
            except BaseException as primary:
                self._phase = "failed"
                pool.cleanup(primary)
                raise

    def _candidate_batch(
        self,
        before: PartitionedFlowCheckpoint,
        start_position: int,
        records: tuple[FlowRecord, ...],
        deadline: float,
    ) -> PartitionedFlowBatch:
        """Execute from an admitted parent snapshot without publishing it.

        The private caller holds _operation and owns the final publication boundary.
        This method neither reads nor changes _point; workers restore every wave.
        """
        _count(start_position, "source position", 0, _MAX_COUNT)
        if start_position != before.next_position or before.source_closed:
            raise ValidationError("source position must match the next open position")
        if type(records) is not tuple or not 1 <= len(records) <= self.limits.max_batch_inputs:
            raise ValidationError("records must be a nonempty bounded tuple")
        _count(start_position + len(records), "source next position", 0, _MAX_COUNT)
        if (
            len(records) * self.flow.limits.max_calls_per_input
            > self.limits.max_callback_reservation
        ):
            raise ValidationError("global callback reservation exceeded")
        groups: dict[int, list[dict[str, Any]]] = {}
        input_bytes = 0
        expected: dict[int, str] = {}
        for position, record in enumerate(records, start_position):
            if type(record) is not FlowRecord or record.key is None:
                raise ValidationError("partitioned input must be keyed FlowRecord")
            # Revalidate a potentially forged exact-type public object at the boundary.
            copied = _copy_record(record)
            key = cast(str, copied.key)  # _copy_record already rejected absent keys.
            wire = {"position": position, "record": copied.to_dict()}
            input_bytes += len(_json(wire, self.limits.max_input_bytes).encode("utf-8"))
            if input_bytes > self.limits.max_input_bytes:
                raise ValidationError("global input wire exceeded")
            groups.setdefault(self.partition_for(key), []).append(wire)
            expected[position] = key
        quotas = {
            index: min(self.limits.max_message_bytes, self.limits.max_result_bytes // len(groups))
            for index in groups
        }
        requests = {}
        request_bytes = 0
        for index, items in groups.items():
            payload = _json(
                {
                    "session": self._session,
                    "wave": before.waves + 1,
                    "worker": index,
                    "checkpoint": _shard_wire(before.shards[index]),
                    "items": items,
                    "reply_limit": quotas[index],
                },
                self.limits.max_message_bytes,
            ).encode("utf-8")
            request_bytes += len(payload)
            if request_bytes > self.limits.max_checkpoint_bytes + self.limits.max_input_bytes:
                raise ValidationError("aggregate request wire exceeded")
            requests[index] = payload
        pool = self._pool
        if pool is None:
            raise LocalWorkerError("session_contract")
        try:
            pool.check(deadline)
            pool.check_alive()
            responses = pool.execute(requests, quotas, deadline)
            shards = list(before.shards)
            rows: list[tuple[int, int, FlowRecord]] = []
            output_bytes = 0
            for index, raw in responses.items():
                message = _load(raw, quotas[index])
                if type(message) is dict and message.get("status") == "error":
                    _shape(message, {"status", "worker", "session", "reason"})
                    _count(message["worker"], "worker", 0, self.workers - 1)
                    if (
                        message["worker"] != index
                        or message["session"] != self._session
                        or message["reason"] not in {"callback", "control", "contract"}
                    ):
                        raise LocalWorkerError("response_contract", index)
                    raise LocalWorkerError(message["reason"], index)
                _shape(message, {"status", "session", "wave", "worker", "checkpoint", "outputs"})
                _count(message["worker"], "worker", 0, self.workers - 1)
                _count(message["wave"], "wave", 1, _MAX_COUNT)
                if (
                    message["status"],
                    message["session"],
                    message["wave"],
                    message["worker"],
                ) != ("ok", self._session, before.waves + 1, index):
                    raise LocalWorkerError("response_contract", index)
                emitted = message["outputs"]
                if (
                    type(emitted) is not list
                    or len(rows) + len(emitted) > self.limits.max_output_records
                ):
                    raise LocalWorkerError("output_limit", index)
                positions = {item["position"] for item in groups[index]}
                previous = (-1, -1)
                for item in emitted:
                    _shape(item, {"position", "ordinal", "record"})
                    position = _count(item["position"], "output position", 0, _MAX_COUNT)
                    ordinal = _count(item["ordinal"], "output ordinal", 0, _MAX_COUNT)
                    pair = (position, ordinal)
                    if (
                        position not in positions
                        or pair <= previous
                        or ordinal != (previous[1] + 1 if previous[0] == position else 0)
                    ):
                        raise LocalWorkerError("output_order", index)
                    record = _record(item["record"])
                    if (
                        record.key != expected[position]
                        or record.byte_size > self.flow.limits.max_record_bytes
                    ):
                        raise LocalWorkerError("output_contract", index)
                    output_bytes += len(_json(item, self.limits.max_output_bytes).encode("utf-8"))
                    if output_bytes > self.limits.max_output_bytes:
                        raise LocalWorkerError("output_limit", index)
                    rows.append((position, ordinal, record))
                    previous = pair
                shard = _shard_load(message["checkpoint"], self.limits)
                if shard.processed_inputs != before.shards[index].processed_inputs + len(
                    groups[index]
                ) or shard.emitted_records != before.shards[index].emitted_records + len(emitted):
                    raise LocalWorkerError("shard_counts", index)
                shards[index] = shard
            after = replace(
                before,
                next_position=start_position + len(records),
                waves=before.waves + 1,
                shards=tuple(shards),
            )
            after.validate_for(self.flow)
            rows.sort(key=lambda row: (row[0], row[1]))
            outputs = tuple(
                PartitionedFlowOutput(sequence, position, ordinal, record)
                for sequence, (position, ordinal, record) in enumerate(rows, before.emitted_records)
            )
            batch = PartitionedFlowBatch(start_position, outputs, after)
            pool.check(deadline)
            pool.check_alive()
            return batch
        except BaseException as primary:
            self._phase = "failed"
            pool.cleanup(primary)
            raise

    def close_source(self, next_position: int) -> PartitionedFlowCheckpoint:
        return self._close_source(next_position)

    def _close_source(
        self, next_position: int, lease: object | None = None
    ) -> PartitionedFlowCheckpoint:
        with self._operation(lease):
            _count(next_position, "source next position", 0, _MAX_COUNT)
            if next_position != self._point.next_position:
                raise ValidationError("source EOF position mismatch")
            after = (
                self._point
                if self._point.source_closed
                else replace(self._point, source_closed=True)
            )
            with self._publication:
                if self._cancel.is_set():
                    raise LocalWorkerError("cancelled")
                self._point = after
            return self._point

    def run(
        self,
        records: Iterable[FlowRecord],
        *,
        max_inputs: int = 100_000,
        batch_size: int = 64,
        own_source: bool = False,
    ) -> Iterator[PartitionedFlowBatch]:
        """Borrow by default; a complete yielded wave is committed before more pull.

        A cap is not EOF. On failure a caller must seek its source back to the
        last checkpoint; consumed-but-unpublished input is not secretly replayed.
        """
        self._owned()
        _count(max_inputs, "max_inputs", 1, 1_000_000)
        _count(batch_size, "batch_size", 1, self.limits.max_batch_inputs)
        if type(own_source) is not bool:
            raise ValidationError("own_source must be boolean")
        lease = object()
        with self._operation():
            if self._cancel.is_set():
                raise LocalWorkerError("cancelled")
            if self._point.source_closed:
                raise ValidationError("source is already closed")
            self._pull_owner = lease
        iterator: Iterator[FlowRecord] | None = None
        primary: BaseException | None = None
        try:
            iterator = iter(records)
            remaining = max_inputs
            while remaining:
                values = []
                eof = False
                for _ in range(min(remaining, batch_size)):
                    with self._operation(lease):
                        if self._cancel.is_set():
                            raise LocalWorkerError("cancelled")
                    try:
                        values.append(next(iterator))
                    except StopIteration:
                        eof = True
                        break
                if values:
                    batch = self._process_batch(self._point.next_position, tuple(values), lease)
                    remaining -= len(values)
                    yield batch
                if eof:
                    self._close_source(self._point.next_position, lease)
                    return
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                if own_source and iterator is not None:
                    try:
                        closer = getattr(iterator, "close", None)
                        if closer is not None:
                            result = closer()
                            if inspect.isawaitable(result) or inspect.isasyncgen(result):
                                if inspect.iscoroutine(result):
                                    result.close()
                                raise ValidationError("source cleanup must be synchronous")
                    except BaseException as cleanup:
                        if (
                            primary is None
                            or isinstance(primary, GeneratorExit)
                            or (
                                isinstance(primary, Exception)
                                and not isinstance(cleanup, Exception)
                            )
                        ):
                            raise
                        primary.add_note("owned source cleanup also failed")
            finally:
                self._pull_owner = None

    def close(self) -> None:
        self._owned()
        if not self._busy.acquire(blocking=False):
            raise ValidationError("cancel from another thread; close after the active wave settles")
        try:
            self._phase = "closing"
            if self._pool is not None:
                self._pool.cleanup()
            self._phase = "closed"
        finally:
            self._busy.release()

    def __enter__(self) -> LocalPartitionedFlow:
        self._owned()
        if self._phase != "ready":
            raise ValidationError("local worker session is not active")
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> None:
        try:
            self.close()
        except BaseException as cleanup:
            if exc is None or (isinstance(exc, Exception) and not isinstance(cleanup, Exception)):
                raise
            exc.add_note("local worker cleanup also failed")


__all__ = [
    "LocalPartitionedFlow",
    "LocalWorkerCleanup",
    "LocalWorkerError",
    "LocalWorkerLimits",
    "LocalWorkerStatus",
    "PartitionedFlowBatch",
    "PartitionedFlowCheckpoint",
    "PartitionedFlowOutput",
]
