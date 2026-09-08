"""Incremental local operators with isolated keyed state and atomic input steps.

Callables are explicitly trusted local Python code. A failed input rolls back
this runtime's state, not arbitrary external side effects of those callables.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from .errors import ValidationError
from .models import _freeze_event_data, _label, _thaw_json

Operator = Literal[
    "map", "filter", "flat_map", "key_by", "drop_key", "stateful_map", "stateful_flat_map"
]
_STATEFUL = {"stateful_map", "stateful_flat_map"}
_OPERATORS = {"map", "filter", "flat_map", "key_by", "drop_key"} | _STATEFUL
_REMOVED = object()
_MAX_COUNT = 2**53 - 1


def _count(value: Any, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _key(value: Any, name: str) -> str:
    result = _label(value, name)
    if type(value) is not str or result != value:
        raise ValidationError(f"{name} must use a canonical, whitespace-trimmed string")
    return result


def _snapshot(value: Any, maximum: int) -> str:
    # Reuse the event boundary's node/depth/Unicode/interoperable-number checks.
    frozen = _freeze_event_data({"value": value})
    encoded = json.dumps(
        _thaw_json(frozen)["value"],
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    if len(encoded.encode("utf-8")) > maximum:
        raise ValidationError("dataflow value exceeds the configured byte limit")
    return encoded


def _sync(function: Any, name: str) -> None:
    if (
        not callable(function)
        or inspect.iscoroutinefunction(function)
        or inspect.iscoroutinefunction(getattr(function, "__call__", None))  # noqa: B004
    ):
        raise ValidationError(f"{name} must be a synchronous callable")


class FlowExecutionError(ValidationError):
    """An input step failed; its internal state changes were not committed."""

    def __init__(self, step_id: str) -> None:
        self.step_id = step_id
        super().__init__(f"dataflow step {step_id!r} failed its callback or value contract")


@dataclass(frozen=True, slots=True, init=False)
class FlowRecord:
    """Immutable JSON value and optional exact key; reads return isolated copies."""

    key: str | None
    _json: str = field(repr=False)

    def __init__(self, value: Any, key: str | None = None) -> None:
        object.__setattr__(self, "key", None if key is None else _key(key, "record key"))
        object.__setattr__(self, "_json", _snapshot(value, 8 * 1024 * 1024))

    @property
    def value(self) -> Any:
        return json.loads(self._json)

    @property
    def byte_size(self) -> int:
        return len(self._json.encode("utf-8"))

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "value": self.value}


@dataclass(frozen=True, slots=True)
class StateUpdate:
    """Callback proposal: replace/delete key state and optionally emit one value.

    ``state=None`` stores JSON null; ``retain=False`` deletes the key. Proposed
    containers are snapshotted immediately by the runtime before acceptance.
    """

    state: Any
    output: Any = None
    emit: bool = True
    retain: bool = True

    def __post_init__(self) -> None:
        if type(self.emit) is not bool or type(self.retain) is not bool:
            raise ValidationError("state update emit/retain must be booleans")


@dataclass(frozen=True, slots=True)
class StateFlatUpdate:
    """Replace/delete keyed state and emit ordered zero-to-many JSON values.

    State is snapshotted before output iteration, so a generator cannot change
    its proposed state by later mutating that object. Native output generators
    are closed on success or failure; other iterator resources remain caller-owned.
    """

    state: Any
    outputs: Iterable[Any]
    retain: bool = True

    def __post_init__(self) -> None:
        if type(self.retain) is not bool:
            raise ValidationError("state expansion retain must be boolean")


@dataclass(frozen=True, slots=True)
class FlowStep:
    step_id: str
    operator: Operator
    function: Callable[..., Any] | None = field(default=None, repr=False)
    initial: Callable[[], Any] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _key(self.step_id, "step_id")
        if type(self.operator) is not str or self.operator not in _OPERATORS:
            raise ValidationError("unknown dataflow operator")
        if self.operator == "drop_key":
            if self.function is not None:
                raise ValidationError("drop_key does not accept a callback")
        else:
            _sync(self.function, "step function")
        if self.operator in _STATEFUL:
            _sync(self.initial, "state initializer")
        elif self.initial is not None:
            raise ValidationError("only stateful operators accept an initializer")


@dataclass(frozen=True, slots=True)
class FlowLimits:
    """Per-FlowStep emission bounds and shared transaction callback/state bounds.

    Record count/batch bytes apply to each FlowStep's emitted batch. A branching
    graph additionally bounds merge, branch and edge work using GraphLimits.
    """

    max_records_per_input: int = 1_000
    max_calls_per_input: int = 10_000
    max_record_bytes: int = 1 * 1024 * 1024
    max_batch_bytes: int = 16 * 1024 * 1024
    max_state_value_bytes: int = 1 * 1024 * 1024
    max_state_keys: int = 10_000
    max_state_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        for name, ceiling in (
            ("max_records_per_input", 100_000),
            ("max_calls_per_input", 1_000_000),
            ("max_record_bytes", 8 * 1024 * 1024),
            ("max_batch_bytes", 64 * 1024 * 1024),
            ("max_state_value_bytes", 8 * 1024 * 1024),
            ("max_state_keys", 100_000),
            ("max_state_bytes", 256 * 1024 * 1024),
        ):
            _count(getattr(self, name), name, 1, ceiling)

    def to_dict(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class Dataflow:
    """A bounded linear operator graph with an explicit semantic revision.

    Identity includes step IDs/kinds, limits and revision, not Python bytecode or
    closure contents. Change ``revision`` when callback semantics change before
    restoring checkpoints. No function code is serialized or imported by name.
    """

    flow_id: str
    revision: str
    steps: tuple[FlowStep, ...]
    limits: FlowLimits = field(default_factory=FlowLimits)

    def __post_init__(self) -> None:
        _key(self.flow_id, "flow_id")
        _key(self.revision, "revision")
        if type(self.steps) is not tuple or not 1 <= len(self.steps) <= 64:
            raise ValidationError("steps must be a tuple of 1..64 FlowStep values")
        for step in self.steps:
            if type(step) is not FlowStep:
                raise ValidationError("steps must contain FlowStep values")
            step.__post_init__()
        if len({step.step_id for step in self.steps}) != len(self.steps):
            raise ValidationError("step IDs must be unique")
        if type(self.limits) is not FlowLimits:
            raise ValidationError("limits must be FlowLimits")

    @property
    def identity(self) -> str:
        document = {
            "flow_id": self.flow_id,
            "revision": self.revision,
            "steps": [(step.step_id, step.operator) for step in self.steps],
            "limits": self.limits.to_dict(),
        }
        return hashlib.sha256(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class FlowCheckpoint:
    """Portable immutable state; external cursor/output commit is caller-owned."""

    identity: str
    processed_inputs: int
    emitted_records: int
    cells: tuple[tuple[str, str, str], ...]

    def __post_init__(self) -> None:
        if (
            type(self.identity) is not str
            or len(self.identity) != 64
            or any(char not in "0123456789abcdef" for char in self.identity)
        ):
            raise ValidationError("flow checkpoint identity must be a SHA-256 hex digest")
        _count(self.processed_inputs, "processed_inputs", 0, _MAX_COUNT)
        _count(self.emitted_records, "emitted_records", 0, _MAX_COUNT)
        if type(self.cells) is not tuple or len(self.cells) > 100_000:
            raise ValidationError("checkpoint cells must be a bounded tuple")
        keys: set[tuple[str, str]] = set()
        total = 0
        for cell in self.cells:
            if type(cell) is not tuple or len(cell) != 3:
                raise ValidationError("invalid checkpoint cell")
            step, key, encoded = cell
            _key(step, "checkpoint step")
            _key(key, "checkpoint key")
            if (step, key) in keys:
                raise ValidationError("duplicate checkpoint state key")
            keys.add((step, key))
            if type(encoded) is not str:
                raise ValidationError("checkpoint state must be canonical JSON text")
            if len(encoded) > 8 * 1024 * 1024:
                raise ValidationError("checkpoint state exceeds value byte limit")
            try:
                encoded_bytes = encoded.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValidationError(
                    "checkpoint state must contain Unicode scalar values"
                ) from exc
            if len(encoded_bytes) > 8 * 1024 * 1024:
                raise ValidationError("checkpoint state exceeds value byte limit")
            from .recovery import _decode

            value = _decode(encoded, hashlib.sha256(encoded_bytes).hexdigest())
            if _snapshot(value, 8 * 1024 * 1024) != encoded:
                raise ValidationError("checkpoint state must use canonical JSON")
            total += len(encoded_bytes)
            if total > 256 * 1024 * 1024:
                raise ValidationError("checkpoint exceeds aggregate state bytes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "stream-quilt-dataflow-checkpoint",
            "schema_version": "1.0",
            "identity": self.identity,
            "processed_inputs": self.processed_inputs,
            "emitted_records": self.emitted_records,
            "cells": [
                {"step": step, "key": key, "value": json.loads(value)}
                for step, key, value in self.cells
            ],
        }

    @classmethod
    def from_dict(cls, document: Any) -> FlowCheckpoint:
        if type(document) is not dict or set(document) != {
            "kind",
            "schema_version",
            "identity",
            "processed_inputs",
            "emitted_records",
            "cells",
        }:
            raise ValidationError("invalid dataflow checkpoint fields")
        if (
            document["kind"] != "stream-quilt-dataflow-checkpoint"
            or document["schema_version"] != "1.0"
        ):
            raise ValidationError("unsupported dataflow checkpoint kind/version")
        if type(document["cells"]) is not list or len(document["cells"]) > 100_000:
            raise ValidationError("checkpoint cells must be a bounded JSON array")
        cells = []
        total = 0
        for cell in document["cells"]:
            if type(cell) is not dict or set(cell) != {"step", "key", "value"}:
                raise ValidationError("invalid dataflow checkpoint cell fields")
            _key(cell["step"], "checkpoint step")
            _key(cell["key"], "checkpoint key")
            encoded = _snapshot(cell["value"], min(8 * 1024 * 1024, 256 * 1024 * 1024 - total))
            total += len(encoded.encode())
            cells.append((cell["step"], cell["key"], encoded))
        return cls(
            document["identity"],
            document["processed_inputs"],
            document["emitted_records"],
            tuple(cells),
        )


def _no_awaitable(value: Any) -> None:
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise ValidationError("dataflow values and iteration must be synchronous")


@contextmanager
def _flat_values(values: Any) -> Iterator[Callable[[], Iterator[Any]]]:
    """Own native generators while letting state snapshot precede iterable entry."""
    iterator: Iterator[Any] | None = None
    primary: BaseException | None = None

    def open_iterator() -> Iterator[Any]:
        nonlocal iterator
        iterator = iter(values)
        return iterator

    try:
        _no_awaitable(values)
        if not isinstance(values, Iterable) or isinstance(values, (str, bytes, Mapping)):
            raise ValidationError("flat_map must return an iterable of JSON values")
        yield open_iterator
    except BaseException as error:
        primary = error
        raise
    finally:
        owner = iterator if inspect.isgenerator(iterator) else values
        if inspect.isgenerator(owner):
            try:
                # Python 3.13+ can return the generator's return value here;
                # earlier typing stubs describe only None. Inspect either case.
                returned = cast(Callable[[], Any], owner.close)()
                _no_awaitable(returned)
            except Exception as cleanup:
                if primary is None:
                    raise
                primary.add_note(f"output generator cleanup also failed: {type(cleanup).__name__}")


def _emit_values(
    iterator: Iterator[Any], emit: Callable[[Any, str | None], None], key: str | None
) -> None:
    while True:
        try:
            value = next(iterator)
        except StopIteration as end:
            # A generator return value is not an output. Reject/close a known
            # misplaced coroutine rather than dropping it with the sentinel.
            _no_awaitable(end.value)
            return
        _no_awaitable(value)
        emit(value, key)


class _FlowTransaction:
    """One staged state/callback budget shared by linear and graph schedulers."""

    def __init__(
        self, limits: FlowLimits, state: dict[tuple[str, str], str], state_bytes: int
    ) -> None:
        self.limits = limits
        self.state = state
        self.pending: dict[tuple[str, str], str | object] = {}
        self.projected_keys = len(state)
        self.projected_bytes = state_bytes
        self.calls = 0

    def invoke(self, function: Callable[..., Any], *args: Any) -> Any:
        self.calls += 1
        if self.calls > self.limits.max_calls_per_input:
            raise ValidationError("callback invocation budget exceeded")
        value = function(*args)
        if inspect.isawaitable(value):
            if inspect.iscoroutine(value):
                value.close()
            raise ValidationError("dataflow callbacks must not return awaitables")
        return value

    def _propose(self, key: tuple[str, str], previous: object, state: Any, retain: bool) -> None:
        replacement = _snapshot(state, self.limits.max_state_value_bytes) if retain else _REMOVED
        self.projected_keys += (replacement is not _REMOVED) - (previous is not _REMOVED)
        self.projected_bytes += (
            len(str(replacement).encode()) if replacement is not _REMOVED else 0
        ) - (len(str(previous).encode()) if previous is not _REMOVED else 0)
        if (
            self.projected_keys > self.limits.max_state_keys
            or self.projected_bytes > self.limits.max_state_bytes
        ):
            raise ValidationError("dataflow keyed state capacity exceeded")
        self.pending[key] = replacement

    def apply(
        self,
        step: FlowStep,
        current: Iterable[FlowRecord],
        on_emit: Callable[[FlowRecord], None] | None = None,
    ) -> tuple[FlowRecord, ...]:
        limits = self.limits
        output: list[FlowRecord] = []
        batch_bytes = 0

        def emit(value: Any, key: str | None) -> None:
            nonlocal batch_bytes
            if len(output) >= limits.max_records_per_input:
                raise ValidationError("dataflow expansion exceeds per-input record limit")
            item = FlowRecord(value, key)
            if item.byte_size > limits.max_record_bytes:
                raise ValidationError("dataflow output exceeds record byte limit")
            batch_bytes += item.byte_size
            if batch_bytes > limits.max_batch_bytes:
                raise ValidationError("dataflow batch exceeds aggregate byte limit")
            if on_emit is not None:
                on_emit(item)
            output.append(item)

        try:
            for item in current:
                function = cast(Callable[..., Any], step.function)
                if step.operator == "drop_key":
                    emit(item.value, None)
                    continue
                if step.operator in _STATEFUL:
                    if item.key is None:
                        raise ValidationError("stateful operators require keyed records")
                    state_key = (step.step_id, item.key)
                    previous = self.pending.get(state_key, self.state.get(state_key, _REMOVED))
                    state = (
                        self.invoke(cast(Callable[[], Any], step.initial))
                        if previous is _REMOVED
                        else json.loads(str(previous))
                    )
                    state = json.loads(_snapshot(state, limits.max_state_value_bytes))
                    update = self.invoke(function, item.value, state)
                    if step.operator == "stateful_flat_map":
                        if type(update) is not StateFlatUpdate:
                            raise ValidationError("stateful expansion must return StateFlatUpdate")
                        with _flat_values(update.outputs) as open_iterator:
                            update.__post_init__()
                            self._propose(state_key, previous, update.state, update.retain)
                            _emit_values(open_iterator(), emit, item.key)
                        continue
                    if type(update) is not StateUpdate:
                        raise ValidationError("stateful callback must return StateUpdate")
                    update.__post_init__()
                    self._propose(state_key, previous, update.state, update.retain)
                    if update.emit:
                        emit(update.output, item.key)
                    continue
                result = self.invoke(function, item.value)
                if step.operator == "map":
                    emit(result, item.key)
                elif step.operator == "key_by":
                    emit(item.value, _key(result, "key_by result"))
                elif step.operator == "filter":
                    if type(result) is not bool:
                        raise ValidationError("filter callback must return bool")
                    if result:
                        emit(item.value, item.key)
                else:
                    with _flat_values(result) as open_iterator:
                        _emit_values(open_iterator(), emit, item.key)
        except Exception:
            raise FlowExecutionError(step.step_id) from None
        return tuple(output)

    def commit(self) -> tuple[dict[tuple[str, str], str], int]:
        # Stage allocations before publication. Values are immutable JSON text,
        # so this copies the index, not all retained value payloads.
        state = self.state.copy()
        for key, value in self.pending.items():
            if value is _REMOVED:
                state.pop(key, None)
            else:
                state[key] = str(value)
        return state, self.projected_bytes


class FlowRuntime:
    """Single-owner pull runtime. Each process() call commits its state atomically.

    There is no worker concurrency, wall-time sandbox, transport or durable sink
    hidden in this class. External mutable callback state is outside its rollback
    boundary. Reentrant processing is rejected before callbacks are invoked.
    """

    def __init__(self, flow: Dataflow) -> None:
        if type(flow) is not Dataflow:
            raise ValidationError("flow must be Dataflow")
        flow.__post_init__()
        self.flow = flow
        self._state: dict[tuple[str, str], str] = {}
        self._state_bytes = 0
        self.processed_inputs = 0
        self.emitted_records = 0
        self._busy = False

    def checkpoint(self) -> FlowCheckpoint:
        if self._busy:
            raise ValidationError("cannot checkpoint during a processing transaction")
        return FlowCheckpoint(
            self.flow.identity,
            self.processed_inputs,
            self.emitted_records,
            tuple((step, key, value) for (step, key), value in sorted(self._state.items())),
        )

    @classmethod
    def from_checkpoint(cls, flow: Dataflow, checkpoint: FlowCheckpoint) -> FlowRuntime:
        runtime = cls(flow)
        if type(checkpoint) is not FlowCheckpoint:
            raise ValidationError("checkpoint must be FlowCheckpoint")
        checked = FlowCheckpoint.from_dict(checkpoint.to_dict())
        if checked.identity != flow.identity:
            raise ValidationError("dataflow identity/revision/configuration mismatch")
        stateful = {step.step_id for step in flow.steps if step.operator in _STATEFUL}
        for step, key, value in checked.cells:
            if step not in stateful or len(value.encode()) > flow.limits.max_state_value_bytes:
                raise ValidationError("checkpoint state is incompatible with the flow")
            runtime._state[(step, key)] = value
            runtime._state_bytes += len(value.encode())
        if (
            len(runtime._state) > flow.limits.max_state_keys
            or runtime._state_bytes > flow.limits.max_state_bytes
        ):
            raise ValidationError("checkpoint exceeds configured state limits")
        runtime.processed_inputs, runtime.emitted_records = (
            checked.processed_inputs,
            checked.emitted_records,
        )
        return runtime

    def process(self, record: FlowRecord) -> tuple[FlowRecord, ...]:
        if self._busy:
            raise ValidationError("dataflow runtime is not reentrant")
        if type(record) is not FlowRecord:
            raise ValidationError("input must be FlowRecord")
        if record.byte_size > self.flow.limits.max_record_bytes:
            raise ValidationError("input record exceeds byte limit")
        _count(self.processed_inputs + 1, "processed_inputs", 0, _MAX_COUNT)
        self._busy = True
        try:
            return self._process(record)
        finally:
            self._busy = False

    def _process(self, record: FlowRecord) -> tuple[FlowRecord, ...]:
        transaction = _FlowTransaction(self.flow.limits, self._state, self._state_bytes)
        current: tuple[FlowRecord, ...] = (record,)
        for step in self.flow.steps:
            current = transaction.apply(step, current)
        processed_inputs = self.processed_inputs + 1
        emitted_records = _count(
            self.emitted_records + len(current), "emitted_records", 0, _MAX_COUNT
        )
        self._state, self._state_bytes = transaction.commit()
        self.processed_inputs = processed_inputs
        self.emitted_records = emitted_records
        return current

    def run(
        self, records: Iterable[FlowRecord], *, max_inputs: int = 100_000
    ) -> Iterator[FlowRecord]:
        """Pull up to max_inputs, consuming each input's outputs before the next.

        An early iterator close stops pulling new input; the last processed item
        may already have committed all of its state and outputs. Durable delivery
        requires a separate source/state/sink transaction, not generator close.
        The cap is a cooperative boundary, not an assertion of source exhaustion;
        keep the source iterator to continue without pulling one record too far.
        """
        _count(max_inputs, "max_inputs", 1, 1_000_000)
        iterator = iter(records)
        for _ in range(max_inputs):
            try:
                record = next(iterator)
            except StopIteration:
                return
            yield from self.process(record)


__all__ = [
    "Dataflow",
    "FlowCheckpoint",
    "FlowExecutionError",
    "FlowLimits",
    "FlowRecord",
    "FlowRuntime",
    "FlowStep",
    "StateFlatUpdate",
    "StateUpdate",
]
