"""Strict SQPJ wire contracts; hashes detect corruption, not authenticated history."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from ._local_worker import _copy_record
from .dataflow import FlowRecord, _count
from .errors import ValidationError
from .flow_journal import _decode, _digest, _encode
from .partitioned_checkpoint import PartitionedFlowCheckpoint, _hex, _load, _name, _shape
from .partitioned_flow import PartitionedFlowOutput

_MIB = 1024 * 1024
_HEAD_BYTES = 65 * _MIB
_REQUEST_BYTES = 9 * _MIB
_RECEIPT_BYTES = 16 * 1024
_OUTPUT_BYTES = 9 * _MIB
_BATCH_BYTES = 64 * _MIB
_STORE_BYTES = 256 * _MIB
_DATABASE_BYTES = 512 * _MIB
_FILE_BYTES = 1024 * _MIB
_MAX_HISTORY = 1_000_000
_Progress = tuple[int, int, bool, int]


def _id(value: Any) -> None:
    if type(value) is not str or len(value) != 32:
        raise ValidationError("journal/request ID must be 32 lowercase hexadecimal characters")
    _hex(value + value)


def _encoded(value: Any, maximum: int) -> str:
    return _encode(value, max_bytes=maximum, label="partitioned journal")


def _document(kind: str, **fields: Any) -> dict[str, Any]:
    return {"kind": "stream-quilt-partitioned-" + kind, "version": "1.0", **fields}


def _fields(value: Any, kind: str, fields: set[str]) -> None:
    _shape(value, fields | {"kind", "version"})
    if value["kind"] != "stream-quilt-partitioned-" + kind or value["version"] != "1.0":
        raise ValidationError("unsupported partitioned journal wire profile")


def _checked(row: Any, maximum: int) -> Any:
    if row is None:
        raise ValidationError("missing partitioned journal row")
    return _decode(*row, max_bytes=maximum, label="partitioned journal")


def _progress(value: Any) -> None:
    if type(value) is not tuple or len(value) != 4:
        raise ValidationError("progress must be (position, waves, closed, outputs)")
    position, waves, closed, outputs = value
    _count(position, "source position", 0, _MAX_HISTORY)
    _count(waves, "waves", 0, position)
    _count(outputs, "outputs", 0, _MAX_HISTORY)
    if type(closed) is not bool or position > waves * 256 or (not position and outputs):
        raise ValidationError("inconsistent progress counters")
    _count(waves + closed, "generation", 0, _MAX_HISTORY)


def _progress_load(value: Any) -> _Progress:
    if type(value) is not list or len(value) != 4:
        raise ValidationError("invalid progress wire")
    result = (value[0], value[1], value[2], value[3])
    _progress(result)
    return result


def _point_progress(point: PartitionedFlowCheckpoint) -> _Progress:
    return point.next_position, point.waves, point.source_closed, point.emitted_records


@dataclass(frozen=True, slots=True)
class PartitionedFlowRecoveryPoint:
    journal_id: str
    generation: int
    checkpoint: PartitionedFlowCheckpoint

    def __post_init__(self) -> None:
        _id(self.journal_id)
        _count(self.generation, "generation", 0, _MAX_HISTORY)
        if type(self.checkpoint) is not PartitionedFlowCheckpoint:
            raise ValidationError("recovery point requires PartitionedFlowCheckpoint")
        self.checkpoint.__post_init__()
        _progress(_point_progress(self.checkpoint))
        if self.generation != self.checkpoint.waves + self.checkpoint.source_closed:
            raise ValidationError("generation must count effective waves and EOF")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _document(
            "head",
            journal_id=self.journal_id,
            generation=self.generation,
            checkpoint=self.checkpoint.to_dict(),
        )

    def to_json(self) -> str:
        return _encoded(self.to_dict(), _HEAD_BYTES)

    @classmethod
    def from_dict(cls, value: Any) -> PartitionedFlowRecoveryPoint:
        _fields(value, "head", {"journal_id", "generation", "checkpoint"})
        _id(value["journal_id"])
        _count(value["generation"], "generation", 0, _MAX_HISTORY)
        return cls(
            value["journal_id"],
            value["generation"],
            PartitionedFlowCheckpoint.from_dict(value["checkpoint"]),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> PartitionedFlowRecoveryPoint:
        return cls.from_dict(_load(value, _HEAD_BYTES))


@dataclass(frozen=True, slots=True)
class PartitionedFlowRequest:
    journal_id: str
    request_id: str
    expected_generation: int
    cause: Literal["wave", "eof"]
    start_position: int
    records: tuple[FlowRecord, ...] = ()

    def __post_init__(self) -> None:
        _id(self.journal_id)
        _id(self.request_id)
        _count(self.expected_generation, "expected generation", 0, _MAX_HISTORY)
        _count(self.start_position, "start position", 0, _MAX_HISTORY)
        if type(self.cause) is not str or self.cause not in {"wave", "eof"}:
            raise ValidationError("request cause must be wave or eof")
        if type(self.records) is not tuple or not (
            1 <= len(self.records) <= 256 if self.cause == "wave" else len(self.records) == 0
        ):
            raise ValidationError("wave requires 1..256 records; EOF requires none")
        _count(self.start_position + len(self.records), "input stop", 0, _MAX_HISTORY)
        for record in self.records:
            if type(record) is not FlowRecord or record.key is None:
                raise ValidationError("request requires exact keyed FlowRecords")
            _name(record.key, "record key")
            if type(record._json) is not str or len(record._json) > 8 * _MIB:
                raise ValidationError("record requires bounded encoded JSON")
        _encoded(self._document(), _REQUEST_BYTES)
        for record in self.records:
            _copy_record(record)

    def _document(self) -> dict[str, Any]:
        return _document(
            "request",
            journal_id=self.journal_id,
            request_id=self.request_id,
            expected_generation=self.expected_generation,
            cause=self.cause,
            start_position=self.start_position,
            records=[{"key": record.key, "encoded_value": record._json} for record in self.records],
        )

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return self._document()

    def to_json(self) -> str:
        return _encoded(self.to_dict(), _REQUEST_BYTES)

    @property
    def digest(self) -> str:
        return _digest(self.to_json())

    @classmethod
    def from_dict(cls, value: Any) -> PartitionedFlowRequest:
        _fields(
            value,
            "request",
            {
                "journal_id",
                "request_id",
                "expected_generation",
                "cause",
                "start_position",
                "records",
            },
        )
        records = value["records"]
        if type(records) is not list or len(records) > 256:
            raise ValidationError("request record array exceeds admission")
        _id(value["journal_id"])
        _id(value["request_id"])
        _count(value["expected_generation"], "generation", 0, _MAX_HISTORY)
        _count(value["start_position"], "position", 0, _MAX_HISTORY)
        if type(value["cause"]) is not str or value["cause"] not in {"wave", "eof"}:
            raise ValidationError("unsupported request cause")
        for record in records:
            _shape(record, {"key", "encoded_value"})
            _name(record["key"], "record key")
            if type(record["encoded_value"]) is not str or len(record["encoded_value"]) > 8 * _MIB:
                raise ValidationError("request wire requires keyed encoded values")
        # Complete wire admission precedes decoding any inner record value.
        _encoded(value, _REQUEST_BYTES)
        return cls(
            value["journal_id"],
            value["request_id"],
            value["expected_generation"],
            cast(Literal["wave", "eof"], value["cause"]),
            value["start_position"],
            tuple(
                FlowRecord(_load(record["encoded_value"], 8 * _MIB), record["key"])
                for record in records
            ),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> PartitionedFlowRequest:
        return cls.from_dict(_load(value, _REQUEST_BYTES))


@dataclass(frozen=True, slots=True)
class PartitionedFlowReceipt:
    journal_id: str
    request_id: str
    request_digest: str
    status: Literal["committed", "no_op"]
    expected_generation: int
    cause: Literal["wave", "eof"]
    before: _Progress
    after: _Progress
    before_head_digest: str
    after_head_digest: str

    def __post_init__(self) -> None:
        _id(self.journal_id)
        _id(self.request_id)
        for value in (self.request_digest, self.before_head_digest, self.after_head_digest):
            _hex(value)
        _count(self.expected_generation, "expected generation", 0, _MAX_HISTORY)
        _progress(self.before)
        _progress(self.after)
        if self.expected_generation != self.before[1] + self.before[2]:
            raise ValidationError("receipt generation differs from before progress")
        if type(self.status) is not str or self.status not in {"committed", "no_op"}:
            raise ValidationError("invalid receipt status")
        if type(self.cause) is not str or self.cause not in {"wave", "eof"}:
            raise ValidationError("invalid receipt cause")
        if self.status == "no_op":
            if (
                self.cause != "eof"
                or not self.before[2]
                or self.before != self.after
                or self.before_head_digest != self.after_head_digest
            ):
                raise ValidationError("only repeated EOF is a no-op")
        elif self.before[2] or self.after[1] + self.after[2] != self.expected_generation + 1:
            raise ValidationError("committed receipt must advance one open generation")
        elif self.cause == "eof":
            if self.after != (self.before[0], self.before[1], True, self.before[3]):
                raise ValidationError("EOF must not advance input, wave or outputs")
        elif (
            not 1 <= self.after[0] - self.before[0] <= 256
            or self.after[1] != self.before[1] + 1
            or self.after[2]
            or not 0 <= self.after[3] - self.before[3] <= 100_000
        ):
            raise ValidationError("wave receipt has inconsistent input/output counters")

    @property
    def after_generation(self) -> int:
        return self.expected_generation + (self.status == "committed")

    @property
    def output_start(self) -> int:
        return self.before[3]

    @property
    def output_stop(self) -> int:
        return self.after[3]

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _document(
            "receipt",
            journal_id=self.journal_id,
            request_id=self.request_id,
            request_digest=self.request_digest,
            status=self.status,
            expected_generation=self.expected_generation,
            cause=self.cause,
            before=list(self.before),
            after=list(self.after),
            before_head_digest=self.before_head_digest,
            after_head_digest=self.after_head_digest,
        )

    def to_json(self) -> str:
        return _encoded(self.to_dict(), _RECEIPT_BYTES)

    @classmethod
    def from_dict(cls, value: Any) -> PartitionedFlowReceipt:
        _fields(
            value,
            "receipt",
            {
                "journal_id",
                "request_id",
                "request_digest",
                "status",
                "expected_generation",
                "cause",
                "before",
                "after",
                "before_head_digest",
                "after_head_digest",
            },
        )
        return cls(
            value["journal_id"],
            value["request_id"],
            value["request_digest"],
            value["status"],
            value["expected_generation"],
            value["cause"],
            _progress_load(value["before"]),
            _progress_load(value["after"]),
            value["before_head_digest"],
            value["after_head_digest"],
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> PartitionedFlowReceipt:
        return cls.from_dict(_load(value, _RECEIPT_BYTES))


@dataclass(frozen=True, slots=True)
class PartitionedJournalOutput:
    generation: int
    output: PartitionedFlowOutput

    def __post_init__(self) -> None:
        _count(self.generation, "output generation", 1, _MAX_HISTORY)
        if type(self.output) is not PartitionedFlowOutput:
            raise ValidationError("journal output requires PartitionedFlowOutput")
        self.output.__post_init__()
        _count(self.output.sequence, "output sequence", 0, _MAX_HISTORY - 1)
        _count(self.output.source_position, "output source position", 0, _MAX_HISTORY - 1)

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _document(
            "output",
            generation=self.generation,
            sequence=self.output.sequence,
            source_position=self.output.source_position,
            output_index=self.output.output_index,
            record=self.output.record.to_dict(),
        )

    @classmethod
    def from_dict(cls, value: Any) -> PartitionedJournalOutput:
        _fields(
            value, "output", {"generation", "sequence", "source_position", "output_index", "record"}
        )
        _shape(value["record"], {"value", "key"})
        return cls(
            value["generation"],
            PartitionedFlowOutput(
                value["sequence"],
                value["source_position"],
                value["output_index"],
                FlowRecord(value["record"]["value"], value["record"]["key"]),
            ),
        )


@dataclass(frozen=True, slots=True)
class PartitionedOutputCursor:
    journal_id: str
    anchor_generation: int
    anchor_receipt_digest: str
    stop_sequence: int
    next_sequence: int

    def __post_init__(self) -> None:
        _id(self.journal_id)
        _hex(self.anchor_receipt_digest)
        _count(self.anchor_generation, "anchor generation", 0, _MAX_HISTORY)
        _count(self.stop_sequence, "output stop", 0, _MAX_HISTORY)
        _count(self.next_sequence, "output next", 0, self.stop_sequence)
        if not self.anchor_generation and self.stop_sequence:
            raise ValidationError("initial cursor cannot include outputs")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return _document(
            "cursor",
            journal_id=self.journal_id,
            anchor_generation=self.anchor_generation,
            anchor_receipt_digest=self.anchor_receipt_digest,
            stop_sequence=self.stop_sequence,
            next_sequence=self.next_sequence,
        )

    def to_json(self) -> str:
        return _encoded(self.to_dict(), 2048)

    @classmethod
    def from_dict(cls, value: Any) -> PartitionedOutputCursor:
        _fields(
            value,
            "cursor",
            {
                "journal_id",
                "anchor_generation",
                "anchor_receipt_digest",
                "stop_sequence",
                "next_sequence",
            },
        )
        return cls(
            value["journal_id"],
            value["anchor_generation"],
            value["anchor_receipt_digest"],
            value["stop_sequence"],
            value["next_sequence"],
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> PartitionedOutputCursor:
        return cls.from_dict(_load(value, 2048))


@dataclass(frozen=True, slots=True)
class PartitionedOutputPage:
    outputs: tuple[PartitionedJournalOutput, ...]
    cursor: PartitionedOutputCursor

    def __post_init__(self) -> None:
        if type(self.cursor) is not PartitionedOutputCursor:
            raise ValidationError("page requires PartitionedOutputCursor")
        self.cursor.__post_init__()
        if type(self.outputs) is not tuple or len(self.outputs) > 1000:
            raise ValidationError("page output count exceeded")
        size = 0
        start = self.cursor.next_sequence - len(self.outputs)
        for index, item in enumerate(self.outputs, start):
            if type(item) is not PartitionedJournalOutput:
                raise ValidationError("invalid journal output page")
            item.__post_init__()
            if item.output.sequence != index or item.generation > self.cursor.anchor_generation:
                raise ValidationError("page output sequence/generation mismatch")
            size += len(_encoded(item.to_dict(), _OUTPUT_BYTES).encode("utf-8"))
            if size > _BATCH_BYTES:
                raise ValidationError("page wire budget exceeded")

    def to_json(self) -> str:
        self.__post_init__()
        return _encoded(
            _document(
                "page",
                outputs=[item.to_dict() for item in self.outputs],
                cursor=self.cursor.to_dict(),
            ),
            _BATCH_BYTES + 4096,
        )
