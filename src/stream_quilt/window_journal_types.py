"""Bounded wire records for one durable explicit-watermark window graph."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from .branching import GraphOutput
from .dataflow import _MAX_COUNT, FlowRecord, _count
from .errors import ValidationError
from .flow_journal import _decode, _digest, _encode, _stored_record
from .io import _reject_duplicate_keys, _reject_json_constant
from .recovery import _finite_json_float, _input_id
from .window_fold import _tick
from .window_graph import WindowGraphInput
from .window_graph_checkpoint import WindowGraphCheckpoint

_HEAD_BYTES = 67 * 1024 * 1024
_REQUEST_BYTES = 16 * 1024 * 1024
_RECEIPT_BYTES = 256 * 1024
_OPERATION_BYTES = 16 * 1024
_OUTPUT_BYTES = 9 * 1024 * 1024
_METADATA_BYTES = 16 * 1024 * 1024
_BATCH_BYTES = 64 * 1024 * 1024
_MAX_COMMANDS = 1_000
_MAX_BATCH_OUTPUTS = 100_000
_MAX_HISTORY = 1_000_000


def _hex(value: Any, size: int = 64) -> str:
    if (
        type(value) is not str
        or len(value) != size
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValidationError(f"window journal requires {size} lowercase hex characters")
    return value


def _fields(value: Any, names: set[str], kind: str | None = None) -> dict[str, Any]:
    expected = names | ({"kind", "version"} if kind is not None else set())
    if type(value) is not dict or len(value) != len(expected) or set(value) != expected:
        raise ValidationError("invalid window journal document fields")
    if kind is not None and (value["kind"] != kind or value["version"] != "1.0"):
        raise ValidationError("invalid window journal kind/version")
    return value


def _wire(kind: str, **fields: Any) -> dict[str, Any]:
    return {"kind": kind, "version": "1.0", **fields}


def _encoded(value: Any, maximum: int) -> str:
    return _encode(value, max_bytes=maximum, label="window journal")


def _loaded(payload: str | bytes, maximum: int) -> Any:
    if type(payload) not in (str, bytes) or len(payload) > maximum:
        raise ValidationError("window journal input exceeds its byte limit")
    try:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if len(raw) > maximum:
            raise ValidationError("window journal input exceeds its UTF-8 byte limit")
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ValidationError("invalid window journal JSON") from error
    if _encoded(value, maximum) != raw.decode("utf-8"):
        raise ValidationError("window journal requires canonical JSON")
    return value


def _checked(row: Any, maximum: int) -> Any:
    if row is None:
        raise ValidationError("missing window journal row")
    return _decode(*row, max_bytes=maximum, label="window journal")


@dataclass(frozen=True, slots=True)
class WindowGraphWatermark:
    timestamp: int
    next_position: int

    def __post_init__(self) -> None:
        _tick(self.timestamp, "window journal watermark")
        _count(self.next_position, "next source position", 0, _MAX_COUNT)


@dataclass(frozen=True, slots=True)
class WindowGraphFinish:
    next_position: int

    def __post_init__(self) -> None:
        _count(self.next_position, "next source position", 0, _MAX_COUNT)


@dataclass(frozen=True, slots=True)
class WindowGraphDrain:
    max_windows: int = 100

    def __post_init__(self) -> None:
        _count(self.max_windows, "drain max_windows", 1, 100_000)


_Command = WindowGraphInput | WindowGraphWatermark | WindowGraphFinish | WindowGraphDrain


def _command_document(command: _Command) -> dict[str, Any]:
    if type(command) is WindowGraphInput:
        return {
            "cause": "process",
            "position": command.position,
            "timestamp": command.timestamp,
            "record": {"key": command.record.key, "encoded_value": command.record._json},
        }
    if type(command) is WindowGraphWatermark:
        return {
            "cause": "watermark",
            "timestamp": command.timestamp,
            "next_position": command.next_position,
        }
    if type(command) is WindowGraphFinish:
        return {"cause": "finish", "next_position": command.next_position}
    if type(command) is WindowGraphDrain:
        return {"cause": "drain", "max_windows": command.max_windows}
    raise ValidationError("unsupported window journal command")


@dataclass(frozen=True, slots=True)
class WindowGraphRequest:
    """Caller-retained command intent; an all-no-op request is not persisted."""

    journal_id: str
    request_id: str
    expected_generation: int
    commands: tuple[_Command, ...]

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _hex(self.request_id, 32)
        _count(self.expected_generation, "expected generation", 0, _MAX_HISTORY)
        if type(self.commands) is not tuple or len(self.commands) > _MAX_COMMANDS:
            raise ValidationError("window request requires at most 1000 exact commands")
        for command in self.commands:
            if type(command) not in (
                WindowGraphInput,
                WindowGraphWatermark,
                WindowGraphFinish,
                WindowGraphDrain,
            ):
                raise ValidationError("unsupported window journal command")
            command.__post_init__()
            if type(command) is WindowGraphInput and (
                type(command.record._json) is not str or len(command.record._json) > 8 * 1024 * 1024
            ):
                raise ValidationError("window input value exceeds its encoded bound")
        _encoded(self.to_dict(), _REQUEST_BYTES)
        for command in self.commands:
            if type(command) is WindowGraphInput:
                record = command.record
                if FlowRecord(_loaded(record._json, 8 * 1024 * 1024), record.key) != record:
                    raise ValidationError("window input value is not canonical finite JSON")

    def to_dict(self) -> dict[str, Any]:
        return _wire(
            "stream-quilt-window-graph-request",
            journal_id=self.journal_id,
            request_id=self.request_id,
            expected_generation=self.expected_generation,
            commands=[_command_document(command) for command in self.commands],
        )

    def to_json(self) -> str:
        self.__post_init__()
        return _encoded(self.to_dict(), _REQUEST_BYTES)

    @property
    def digest(self) -> str:
        return _digest(self.to_json())

    @classmethod
    def from_dict(cls, value: Any) -> WindowGraphRequest:
        _fields(
            value,
            {"journal_id", "request_id", "expected_generation", "commands"},
            "stream-quilt-window-graph-request",
        )
        raw = value["commands"]
        if type(raw) is not list or len(raw) > _MAX_COMMANDS:
            raise ValidationError("invalid window request command array")
        _hex(value["journal_id"], 32)
        _hex(value["request_id"], 32)
        _count(value["expected_generation"], "expected generation", 0, _MAX_HISTORY)
        for item in raw:
            if type(item) is not dict or len(item) > 4:
                raise ValidationError("invalid window command")
            cause = item.get("cause")
            if cause == "process":
                _fields(item, {"cause", "position", "timestamp", "record"})
                _count(item["position"], "source position", 0, _MAX_COUNT)
                _tick(item["timestamp"], "source timestamp")
                _fields(item["record"], {"key", "encoded_value"})
                if item["record"]["key"] is not None and type(item["record"]["key"]) is not str:
                    raise ValidationError("invalid source key")
                encoded = item["record"]["encoded_value"]
                if type(encoded) is not str or len(encoded) > 8 * 1024 * 1024:
                    raise ValidationError("invalid encoded source value")
            elif cause == "watermark":
                _fields(item, {"cause", "timestamp", "next_position"})
                WindowGraphWatermark(item["timestamp"], item["next_position"])
            elif cause == "finish":
                _fields(item, {"cause", "next_position"})
                WindowGraphFinish(item["next_position"])
            elif cause == "drain":
                _fields(item, {"cause", "max_windows"})
                WindowGraphDrain(item["max_windows"])
            else:
                raise ValidationError("unknown window command cause")
        _encoded(value, _REQUEST_BYTES)
        commands: list[_Command] = []
        for item in raw:
            if item["cause"] == "process":
                record = item["record"]
                encoded = record["encoded_value"]
                value_record = FlowRecord(_loaded(encoded, 8 * 1024 * 1024), record["key"])
                if value_record._json != encoded:
                    raise ValidationError("encoded source value is not canonical")
                commands.append(WindowGraphInput(item["position"], item["timestamp"], value_record))
            elif item["cause"] == "watermark":
                commands.append(WindowGraphWatermark(item["timestamp"], item["next_position"]))
            elif item["cause"] == "finish":
                commands.append(WindowGraphFinish(item["next_position"]))
            else:
                commands.append(WindowGraphDrain(item["max_windows"]))
        return cls(
            value["journal_id"], value["request_id"], value["expected_generation"], tuple(commands)
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> WindowGraphRequest:
        return cls.from_dict(_loaded(payload, _REQUEST_BYTES))


@dataclass(frozen=True, slots=True)
class WindowGraphRecoveryPoint:
    journal_id: str
    source_id: str
    source_commitment: str
    generation: int
    checkpoint: WindowGraphCheckpoint

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _input_id(self.source_id)
        _hex(self.source_commitment)
        _count(self.generation, "generation", 0, _MAX_HISTORY)
        if type(self.checkpoint) is not WindowGraphCheckpoint:
            raise ValidationError("window journal requires WindowGraphCheckpoint")
        body = self.checkpoint.to_dict()["body"]
        operations = body["operation_sequence"]
        emitted = body["counters"]["emitted_records"]
        if not self.generation <= operations <= min(self.generation * _MAX_COMMANDS, _MAX_HISTORY):
            raise ValidationError("window journal generation and operations disagree")
        if body["next_position"] > operations or emitted > _MAX_HISTORY:
            raise ValidationError("window journal source/output prefix exceeds history")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _wire(
            "stream-quilt-window-graph-recovery-point",
            journal_id=self.journal_id,
            source_id=self.source_id,
            source_commitment=self.source_commitment,
            generation=self.generation,
            checkpoint=self.checkpoint.to_dict(),
        )

    @classmethod
    def from_dict(cls, value: Any) -> WindowGraphRecoveryPoint:
        _fields(
            value,
            {"journal_id", "source_id", "source_commitment", "generation", "checkpoint"},
            "stream-quilt-window-graph-recovery-point",
        )
        return cls(
            value["journal_id"],
            value["source_id"],
            value["source_commitment"],
            value["generation"],
            WindowGraphCheckpoint.from_dict(value["checkpoint"]),
        )


@dataclass(frozen=True, slots=True)
class WindowGraphReceipt:
    journal_id: str
    request_id: str
    request_digest: str
    status: Literal["committed", "no_op"]
    command_count: int
    before_generation: int
    after_generation: int
    before_operation: int
    after_operation: int
    before_position: int
    after_position: int
    before_finished: bool
    after_finished: bool
    before_watermark: int | None
    after_watermark: int | None
    before_watermarks: int
    after_watermarks: int
    before_drains: int
    after_drains: int
    output_start: int
    output_stop: int
    before_head_digest: str
    after_head_digest: str

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _hex(self.request_id, 32)
        for digest in (self.request_digest, self.before_head_digest, self.after_head_digest):
            _hex(digest)
        if type(self.status) is not str or self.status not in ("committed", "no_op"):
            raise ValidationError("invalid window receipt status")
        _count(self.command_count, "command count", 0, _MAX_COMMANDS)
        for name in (
            "before_generation",
            "after_generation",
            "before_operation",
            "after_operation",
            "before_position",
            "after_position",
            "before_watermarks",
            "after_watermarks",
            "before_drains",
            "after_drains",
            "output_start",
            "output_stop",
        ):
            _count(getattr(self, name), name, 0, _MAX_HISTORY)
        if type(self.before_finished) is not bool or type(self.after_finished) is not bool:
            raise ValidationError("window receipt EOF flags require booleans")
        for watermark in (self.before_watermark, self.after_watermark):
            if watermark is not None:
                _tick(watermark, "receipt watermark")
        if (
            self.after_operation < self.before_operation
            or self.after_position < self.before_position
            or self.after_watermarks < self.before_watermarks
            or self.after_drains < self.before_drains
            or self.output_stop < self.output_start
            or self.output_stop - self.output_start > _MAX_BATCH_OUTPUTS
            or (self.before_finished and not self.after_finished)
            or (self.before_watermark is not None and self.after_watermark is None)
            or (
                self.before_watermark is not None
                and self.after_watermark is not None
                and self.after_watermark < self.before_watermark
            )
        ):
            raise ValidationError("window receipt prefix regressed")
        if self.status == "no_op":
            if any(
                (
                    self.before_generation != self.after_generation,
                    self.before_operation != self.after_operation,
                    self.before_position != self.after_position,
                    self.before_finished != self.after_finished,
                    self.before_watermark != self.after_watermark,
                    self.before_watermarks != self.after_watermarks,
                    self.before_drains != self.after_drains,
                    self.output_start != self.output_stop,
                    self.before_head_digest != self.after_head_digest,
                )
            ):
                raise ValidationError("no-op receipt changed a durable prefix")
        elif (
            self.after_generation != self.before_generation + 1
            or not 1 <= self.after_operation - self.before_operation <= self.command_count
        ):
            raise ValidationError("invalid committed window receipt progression")
        if self.after_operation - self.before_operation != (
            self.after_position
            - self.before_position
            + self.after_watermarks
            - self.before_watermarks
            + int(self.after_finished)
            - int(self.before_finished)
            + self.after_drains
            - self.before_drains
        ):
            raise ValidationError("window receipt causes contradict operation range")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _wire(
            "stream-quilt-window-graph-receipt",
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        )

    @classmethod
    def from_dict(cls, value: Any) -> WindowGraphReceipt:
        _fields(value, set(cls.__dataclass_fields__), "stream-quilt-window-graph-receipt")
        return cls(**{name: value[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class WindowGraphOperation:
    sequence: int
    generation: int
    command_index: int
    cause: Literal["process", "watermark", "finish", "drain"]
    position: int | None
    timestamp: int | None
    max_windows: int | None
    input_digest: str | None
    drained_windows: int
    output_start: int
    output_stop: int

    def __post_init__(self) -> None:
        _count(self.sequence, "operation sequence", 1, _MAX_HISTORY)
        _count(self.generation, "generation", 1, _MAX_HISTORY)
        _count(self.command_index, "command index", 0, _MAX_COMMANDS - 1)
        _count(self.drained_windows, "drained windows", 0, 100_000)
        _count(self.output_start, "output start", 0, _MAX_HISTORY)
        _count(self.output_stop, "output stop", self.output_start, _MAX_HISTORY)
        if self.output_stop - self.output_start > _MAX_BATCH_OUTPUTS:
            raise ValidationError("operation output range exceeds request bound")
        if self.cause == "process":
            _count(self.position, "source position", 0, _MAX_COUNT)
            _tick(self.timestamp, "source timestamp")
            _hex(self.input_digest)
            if (
                self.max_windows is not None
                or self.drained_windows
                or self.output_stop != self.output_start
            ):
                raise ValidationError("process cannot claim drain output")
        elif self.cause == "watermark":
            _count(self.position, "next source position", 0, _MAX_COUNT)
            _tick(self.timestamp, "watermark")
            if (
                self.max_windows is not None
                or self.input_digest is not None
                or self.drained_windows
                or self.output_stop != self.output_start
            ):
                raise ValidationError("watermark cannot claim drain output")
        elif self.cause == "finish":
            _count(self.position, "next source position", 0, _MAX_COUNT)
            if (
                self.timestamp is not None
                or self.max_windows is not None
                or self.input_digest is not None
                or self.drained_windows
                or self.output_stop != self.output_start
            ):
                raise ValidationError("finish cannot claim drain output")
        elif self.cause == "drain":
            _count(self.max_windows, "drain max_windows", 1, 100_000)
            if (
                self.position is not None
                or self.timestamp is not None
                or self.input_digest is not None
                or self.max_windows is None
                or not 1 <= self.drained_windows <= self.max_windows
            ):
                raise ValidationError("drain must retire actual bounded windows")
        else:
            raise ValidationError("invalid window operation cause")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _wire(
            "stream-quilt-window-graph-operation",
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        )

    @classmethod
    def from_dict(cls, value: Any) -> WindowGraphOperation:
        _fields(value, set(cls.__dataclass_fields__), "stream-quilt-window-graph-operation")
        return cls(**{name: value[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class WindowGraphJournalOutput:
    sequence: int
    operation_sequence: int
    step_id: str
    record: FlowRecord

    def __post_init__(self) -> None:
        _count(self.sequence, "output sequence", 0, _MAX_HISTORY - 1)
        _count(self.operation_sequence, "operation sequence", 1, _MAX_HISTORY)
        GraphOutput(self.step_id, self.record)

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _wire(
            "stream-quilt-window-graph-output",
            sequence=self.sequence,
            operation_sequence=self.operation_sequence,
            step_id=self.step_id,
            record=self.record.to_dict(),
        )

    @classmethod
    def from_dict(cls, value: Any) -> WindowGraphJournalOutput:
        _fields(
            value,
            {"sequence", "operation_sequence", "step_id", "record"},
            "stream-quilt-window-graph-output",
        )
        return cls(
            value["sequence"],
            value["operation_sequence"],
            value["step_id"],
            _stored_record(value["record"]),
        )


@dataclass(frozen=True, slots=True)
class WindowGraphOutputCursor:
    """Unsigned fixed-prefix read position, not a sink acknowledgement."""

    journal_id: str
    anchor_generation: int
    anchor_receipt_digest: str
    stop_sequence: int
    next_sequence: int

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _hex(self.anchor_receipt_digest)
        _count(self.anchor_generation, "anchor generation", 0, _MAX_HISTORY)
        _count(self.stop_sequence, "cursor stop", 0, _MAX_HISTORY)
        _count(self.next_sequence, "cursor next", 0, self.stop_sequence)
        if self.anchor_generation == 0 and self.stop_sequence:
            raise ValidationError("initial window cursor must be empty")

    def to_json(self) -> str:
        self.__post_init__()
        return _encoded(
            _wire(
                "stream-quilt-window-graph-output-cursor",
                **{name: getattr(self, name) for name in self.__dataclass_fields__},
            ),
            1024,
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> WindowGraphOutputCursor:
        value = _loaded(payload, 1024)
        _fields(value, set(cls.__dataclass_fields__), "stream-quilt-window-graph-output-cursor")
        return cls(**{name: value[name] for name in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class WindowGraphOutputPage:
    outputs: tuple[WindowGraphJournalOutput, ...]
    cursor: WindowGraphOutputCursor

    def __post_init__(self) -> None:
        if (
            type(self.cursor) is not WindowGraphOutputCursor
            or type(self.outputs) is not tuple
            or len(self.outputs) > 1000
        ):
            raise ValidationError("invalid window output page")
        self.cursor.__post_init__()
        first = self.cursor.next_sequence - len(self.outputs)
        size = 0
        for index, output in enumerate(self.outputs):
            if type(output) is not WindowGraphJournalOutput or output.sequence != first + index:
                raise ValidationError("window output page is not contiguous")
            output.__post_init__()
            size += len(_encoded(output.to_dict(), _OUTPUT_BYTES).encode("utf-8"))
            if size > _BATCH_BYTES:
                raise ValidationError("window output page exceeds byte bound")


__all__ = [
    "WindowGraphDrain",
    "WindowGraphFinish",
    "WindowGraphJournalOutput",
    "WindowGraphOperation",
    "WindowGraphOutputCursor",
    "WindowGraphOutputPage",
    "WindowGraphReceipt",
    "WindowGraphRecoveryPoint",
    "WindowGraphRequest",
    "WindowGraphWatermark",
]
