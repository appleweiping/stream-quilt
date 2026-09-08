"""Bounded immutable SQMJ wire contracts; hashes detect corruption, not identity."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from .branching import GraphOutput
from .dataflow import _MAX_COUNT, FlowRecord, _count, _key
from .errors import ValidationError
from .flow_journal import _decode, _digest, _encode, _stored_record
from .io import _reject_duplicate_keys, _reject_json_constant
from .multi_checkpoint import MultiGraphCheckpoint
from .multi_graph import GraphInput
from .recovery import _finite_json_float

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
_SourceVector = tuple[tuple[str, int, bool], ...]
_Commitments = tuple[tuple[str, str], ...]


def _hex(value: Any, size: int = 64) -> None:
    if (
        type(value) is not str
        or len(value) != size
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValidationError(f"expected a {size}-character lowercase hex identifier")


def _fields(value: Any, fields: set[str], kind: str | None = None) -> None:
    expected = fields | ({"kind", "version"} if kind is not None else set())
    if type(value) is not dict or len(value) != len(expected) or set(value) != expected:
        raise ValidationError("invalid multi-source journal document fields")
    if kind is not None and (value["kind"] != kind or value["version"] != "1.0"):
        raise ValidationError("invalid multi-source journal kind/version")


def _wire(kind: str, **fields: Any) -> dict[str, Any]:
    return {"kind": kind, "version": "1.0", **fields}


def _encoded(value: Any, maximum: int) -> str:
    return _encode(value, max_bytes=maximum, label="multi-source journal")


def _loaded(payload: str | bytes, maximum: int) -> Any:
    if type(payload) not in (str, bytes) or len(payload) > maximum:
        raise ValidationError("multi-source document exceeds its byte limit")
    try:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if len(raw) > maximum:
            raise ValidationError("multi-source document exceeds its UTF-8 byte limit")
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (ValueError, RecursionError) as exc:
        raise ValidationError("invalid multi-source journal JSON") from exc


def _checked(row: Any, maximum: int) -> Any:
    if row is None:
        raise ValidationError("missing multi-source journal row")
    return _decode(*row, max_bytes=maximum, label="multi-source journal")


def _vector(value: Any) -> None:
    if type(value) is not tuple or not 1 <= len(value) <= 16:
        raise ValidationError("source vector requires 1..16 entries")
    seen: set[str] = set()
    for item in value:
        if type(item) is not tuple or len(item) != 3:
            raise ValidationError("invalid source vector entry")
        name, position, closed = item
        _key(name, "source_id")
        _count(position, "next position", 0, _MAX_COUNT)
        if type(closed) is not bool or name in seen:
            raise ValidationError("invalid or duplicate source vector entry")
        seen.add(name)
    _count(sum(p for _, p, _ in value), "total source positions", 0, _MAX_COUNT)


def _vector_document(value: _SourceVector) -> list[dict[str, Any]]:
    return [{"source_id": n, "next_position": p, "closed": c} for n, p, c in value]


def _read_vector(value: Any) -> _SourceVector:
    if type(value) is not list or not 1 <= len(value) <= 16:
        raise ValidationError("invalid source vector array")
    for item in value:
        _fields(item, {"source_id", "next_position", "closed"})
    result = tuple((v["source_id"], v["next_position"], v["closed"]) for v in value)
    _vector(result)
    return result


@dataclass(frozen=True, slots=True)
class GraphEOF:
    source_id: str
    next_position: int

    def __post_init__(self) -> None:
        _key(self.source_id, "source_id")
        _count(self.next_position, "next_position", 0, _MAX_COUNT)


@dataclass(frozen=True, slots=True)
class GraphDrain:
    max_keys: int = 100

    def __post_init__(self) -> None:
        _count(self.max_keys, "max_keys", 1, 100_000)


_Command = GraphInput | GraphEOF | GraphDrain


def _command_document(command: _Command) -> dict[str, Any]:
    if type(command) is GraphInput:
        return {
            "cause": "process",
            "source_id": command.source_id,
            "position": command.position,
            "record": {"key": command.record.key, "encoded_value": command.record._json},
        }
    if type(command) is GraphEOF:
        return {
            "cause": "eof",
            "source_id": command.source_id,
            "next_position": command.next_position,
        }
    if type(command) is GraphDrain:
        return {"cause": "drain", "max_keys": command.max_keys}
    raise ValidationError("unsupported journal command")


@dataclass(frozen=True, slots=True)
class MultiGraphRequest:
    """Complete retry identity; commands are admitted before any business callback."""

    journal_id: str
    request_id: str
    expected_generation: int
    commands: tuple[_Command, ...]

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _hex(self.request_id, 32)
        _count(self.expected_generation, "expected generation", 0, _MAX_HISTORY)
        if type(self.commands) is not tuple or len(self.commands) > _MAX_COMMANDS:
            raise ValidationError("request requires a tuple of at most 1000 commands")
        for command in self.commands:
            if type(command) not in (GraphInput, GraphEOF, GraphDrain):
                raise ValidationError("unsupported journal command")
            command.__post_init__()
            if type(command) is GraphInput:
                if command.record.key is not None:
                    _key(command.record.key, "record key")
                if (
                    type(command.record._json) is not str
                    or len(command.record._json) > 8 * 1024 * 1024
                ):
                    raise ValidationError("record must contain a bounded encoded JSON string")
        # Encoded value strings permit the aggregate wire admission to precede nested JSON parsing.
        _encoded(self.to_dict(), _REQUEST_BYTES)
        for command in self.commands:
            if type(command) is GraphInput:
                record = command.record
                if FlowRecord(_loaded(record._json, 8 * 1024 * 1024), record.key) != record:
                    raise ValidationError("request record must contain canonical finite JSON")

    def to_dict(self) -> dict[str, Any]:
        return _wire(
            "stream-quilt-multi-request",
            journal_id=self.journal_id,
            request_id=self.request_id,
            expected_generation=self.expected_generation,
            commands=[_command_document(c) for c in self.commands],
        )

    def to_json(self) -> str:
        self.__post_init__()
        return _encoded(self.to_dict(), _REQUEST_BYTES)

    @property
    def digest(self) -> str:
        return _digest(self.to_json())

    @classmethod
    def from_dict(cls, value: Any) -> MultiGraphRequest:
        _fields(
            value,
            {"journal_id", "request_id", "expected_generation", "commands"},
            "stream-quilt-multi-request",
        )
        commands = value["commands"]
        if type(commands) is not list or len(commands) > _MAX_COMMANDS:
            raise ValidationError("invalid request command array")
        _hex(value["journal_id"], 32)
        _hex(value["request_id"], 32)
        _count(value["expected_generation"], "expected generation", 0, _MAX_HISTORY)
        for item in commands:
            if type(item) is not dict or len(item) > 4:
                raise ValidationError("invalid command fields")
            cause = item.get("cause")
            if cause == "process":
                _fields(item, {"cause", "source_id", "position", "record"})
                _key(item["source_id"], "source_id")
                _count(item["position"], "position", 0, _MAX_COUNT)
                _fields(item["record"], {"key", "encoded_value"})
                if item["record"]["key"] is not None:
                    _key(item["record"]["key"], "record key")
                encoded = item["record"]["encoded_value"]
                if type(encoded) is not str or len(encoded) > 8 * 1024 * 1024:
                    raise ValidationError("invalid bounded encoded record")
            elif cause == "eof":
                _fields(item, {"cause", "source_id", "next_position"})
                GraphEOF(item["source_id"], item["next_position"])
            elif cause == "drain":
                _fields(item, {"cause", "max_keys"})
                GraphDrain(item["max_keys"])
            else:
                raise ValidationError("unknown operation cause")
        _encoded(value, _REQUEST_BYTES)
        result: list[_Command] = []
        for item in commands:
            if item["cause"] == "process":
                encoded = item["record"]["encoded_value"]
                record = FlowRecord(_loaded(encoded, 8 * 1024 * 1024), item["record"]["key"])
                if record._json != encoded:
                    raise ValidationError("encoded record must use canonical JSON")
                result.append(GraphInput(item["source_id"], item["position"], record))
            elif item["cause"] == "eof":
                result.append(GraphEOF(item["source_id"], item["next_position"]))
            else:
                result.append(GraphDrain(item["max_keys"]))
        return cls(
            value["journal_id"], value["request_id"], value["expected_generation"], tuple(result)
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> MultiGraphRequest:
        return cls.from_dict(_loaded(payload, _REQUEST_BYTES))


@dataclass(frozen=True, slots=True)
class MultiGraphRecoveryPoint:
    journal_id: str
    source_commitments: _Commitments
    generation: int
    checkpoint: MultiGraphCheckpoint

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _count(self.generation, "generation", 0, _MAX_HISTORY)
        if type(self.checkpoint) is not MultiGraphCheckpoint:
            raise ValidationError("journal requires MultiGraphCheckpoint")
        self.checkpoint.__post_init__()
        if (
            type(self.source_commitments) is not tuple
            or not 1 <= len(self.source_commitments) <= 16
        ):
            raise ValidationError("invalid source commitments")
        for item in self.source_commitments:
            if type(item) is not tuple or len(item) != 2:
                raise ValidationError("invalid source commitment entry")
            _key(item[0], "source_id")
            _hex(item[1])
        if tuple(n for n, _ in self.source_commitments) != tuple(
            n for n, _, _ in self.checkpoint.sources
        ):
            raise ValidationError("source commitments must match ordered checkpoint sources")
        operations = self.checkpoint.operation_sequence
        if not self.generation <= operations <= min(self.generation * _MAX_COMMANDS, _MAX_HISTORY):
            raise ValidationError("generation and operation prefix are inconsistent")
        _count(self.checkpoint.emitted_records, "journal outputs", 0, _MAX_HISTORY)

    def to_dict(self) -> dict[str, Any]:
        return _wire(
            "stream-quilt-multi-recovery-point",
            journal_id=self.journal_id,
            source_commitments=[{"source_id": n, "digest": d} for n, d in self.source_commitments],
            generation=self.generation,
            checkpoint=self.checkpoint.to_dict(),
        )

    @classmethod
    def from_dict(cls, value: Any) -> MultiGraphRecoveryPoint:
        _fields(
            value,
            {"journal_id", "source_commitments", "generation", "checkpoint"},
            "stream-quilt-multi-recovery-point",
        )
        commitments = value["source_commitments"]
        if type(commitments) is not list or not 1 <= len(commitments) <= 16:
            raise ValidationError("invalid source commitments array")
        for item in commitments:
            _fields(item, {"source_id", "digest"})
            _key(item["source_id"], "source_id")
            _hex(item["digest"])
        _hex(value["journal_id"], 32)
        _count(value["generation"], "generation", 0, _MAX_HISTORY)
        return cls(
            value["journal_id"],
            tuple((c["source_id"], c["digest"]) for c in commitments),
            value["generation"],
            MultiGraphCheckpoint.from_dict(value["checkpoint"]),
        )


@dataclass(frozen=True, slots=True)
class MultiGraphReceipt:
    """A committed request or an unpersisted no-op observation, never a sink ack."""

    journal_id: str
    request_id: str
    request_digest: str
    status: Literal["committed", "no_op"]
    command_count: int
    before_generation: int
    after_generation: int
    before_operation: int
    after_operation: int
    output_start: int
    output_stop: int
    before_sources: _SourceVector
    after_sources: _SourceVector
    before_head_digest: str
    after_head_digest: str

    def __post_init__(self) -> None:
        _hex(self.journal_id, 32)
        _hex(self.request_id, 32)
        for digest in (self.request_digest, self.before_head_digest, self.after_head_digest):
            _hex(digest)
        if type(self.status) is not str or self.status not in ("committed", "no_op"):
            raise ValidationError("invalid receipt status")
        _count(self.command_count, "command count", 0, _MAX_COMMANDS)
        for number in (
            self.before_generation,
            self.after_generation,
            self.before_operation,
            self.after_operation,
            self.output_start,
            self.output_stop,
        ):
            _count(number, "receipt counter", 0, _MAX_HISTORY)
        for generation, operation in (
            (self.before_generation, self.before_operation),
            (self.after_generation, self.after_operation),
        ):
            if not generation <= operation <= generation * _MAX_COMMANDS:
                raise ValidationError("receipt generation and operation counters disagree")
        _vector(self.before_sources)
        _vector(self.after_sources)
        for sources, operations in (
            (self.before_sources, self.before_operation),
            (self.after_sources, self.after_operation),
        ):
            if sum(p + closed for _, p, closed in sources) > operations:
                raise ValidationError("receipt source operations exceed its history")
        if tuple(n for n, _, _ in self.before_sources) != tuple(
            n for n, _, _ in self.after_sources
        ):
            raise ValidationError("receipt source identities changed")
        for (_, before, closed), (_, after, ended) in zip(
            self.before_sources, self.after_sources, strict=True
        ):
            if after < before or (closed and (not ended or after != before)):
                raise ValidationError("receipt source prefix moved backwards")
        if (
            self.output_stop < self.output_start
            or self.output_stop - self.output_start > _MAX_BATCH_OUTPUTS
        ):
            raise ValidationError("invalid receipt output range")
        if self.status == "no_op":
            if (
                self.before_generation != self.after_generation
                or self.before_operation != self.after_operation
                or self.output_start != self.output_stop
                or self.before_sources != self.after_sources
                or self.before_head_digest != self.after_head_digest
            ):
                raise ValidationError("no-op receipt must not change any prefix")
        elif (
            self.after_generation != self.before_generation + 1
            or not 1 <= self.after_operation - self.before_operation <= self.command_count
        ):
            raise ValidationError("invalid committed receipt progression")

    def to_dict(self) -> dict[str, Any]:
        return _wire(
            "stream-quilt-multi-receipt",
            **{
                name: getattr(self, name)
                for name in self.__dataclass_fields__
                if name not in ("before_sources", "after_sources")
            },
            before_sources=_vector_document(self.before_sources),
            after_sources=_vector_document(self.after_sources),
        )

    @classmethod
    def from_dict(cls, value: Any) -> MultiGraphReceipt:
        _fields(value, set(cls.__dataclass_fields__), "stream-quilt-multi-receipt")
        fields = {k: value[k] for k in cls.__dataclass_fields__}
        fields["before_sources"] = _read_vector(fields["before_sources"])
        fields["after_sources"] = _read_vector(fields["after_sources"])
        return cls(**fields)


@dataclass(frozen=True, slots=True)
class MultiGraphOperation:
    """Provenance without input payloads; necessary metadata, not execution proof."""

    sequence: int
    generation: int
    command_index: int
    cause: Literal["process", "eof", "drain"]
    source_id: str | None
    position: int | None
    join_id: str | None
    max_keys: int | None
    input_digest: str | None
    output_start: int
    output_stop: int

    def __post_init__(self) -> None:
        _count(self.sequence, "operation sequence", 1, _MAX_HISTORY)
        _count(self.generation, "generation", 1, _MAX_HISTORY)
        _count(self.command_index, "command index", 0, _MAX_COMMANDS - 1)
        _count(self.output_start, "output start", 0, _MAX_HISTORY)
        _count(
            self.output_stop,
            "output stop",
            self.output_start,
            min(_MAX_HISTORY, self.output_start + _MAX_BATCH_OUTPUTS),
        )
        if type(self.cause) is not str:
            raise ValidationError("operation cause must be a string")
        if self.cause in ("process", "eof"):
            _key(self.source_id, "source_id")
            _count(self.position, "position", 0, _MAX_COUNT)
            if self.join_id is not None or self.max_keys is not None:
                raise ValidationError("source operation cannot name a join")
            if self.cause == "process":
                _hex(self.input_digest)
            elif self.input_digest is not None or self.output_start != self.output_stop:
                raise ValidationError("EOF must not have input digest or terminal output")
        elif self.cause == "drain":
            _key(self.join_id, "join_id")
            _count(self.max_keys, "max_keys", 1, 100_000)
            if (
                self.source_id is not None
                or self.position is not None
                or self.input_digest is not None
            ):
                raise ValidationError("drain must not fabricate a source position")
        else:
            raise ValidationError("invalid operation cause")

    def to_dict(self) -> dict[str, Any]:
        return _wire(
            "stream-quilt-multi-operation",
            **{n: getattr(self, n) for n in self.__dataclass_fields__},
        )

    @classmethod
    def from_dict(cls, value: Any) -> MultiGraphOperation:
        _fields(value, set(cls.__dataclass_fields__), "stream-quilt-multi-operation")
        return cls(**{n: value[n] for n in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class MultiGraphJournalOutput:
    sequence: int
    operation_sequence: int
    step_id: str
    record: FlowRecord

    def __post_init__(self) -> None:
        _count(self.sequence, "output sequence", 0, _MAX_HISTORY - 1)
        _count(self.operation_sequence, "operation sequence", 1, _MAX_HISTORY)
        GraphOutput(self.step_id, self.record)

    def to_dict(self) -> dict[str, Any]:
        return _wire(
            "stream-quilt-multi-output",
            sequence=self.sequence,
            operation_sequence=self.operation_sequence,
            step_id=self.step_id,
            record=self.record.to_dict(),
        )

    @classmethod
    def from_dict(cls, value: Any) -> MultiGraphJournalOutput:
        _fields(
            value,
            {"sequence", "operation_sequence", "step_id", "record"},
            "stream-quilt-multi-output",
        )
        return cls(
            value["sequence"],
            value["operation_sequence"],
            value["step_id"],
            _stored_record(value["record"]),
        )


@dataclass(frozen=True, slots=True)
class MultiGraphOutputCursor:
    """Unsigned fixed-prefix read position, not authentication or acknowledgement."""

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
            raise ValidationError("initial cursor must be empty")

    def to_json(self) -> str:
        self.__post_init__()
        return _encoded(
            _wire(
                "stream-quilt-multi-output-cursor",
                **{n: getattr(self, n) for n in self.__dataclass_fields__},
            ),
            1024,
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> MultiGraphOutputCursor:
        value = _loaded(payload, 1024)
        _fields(value, set(cls.__dataclass_fields__), "stream-quilt-multi-output-cursor")
        return cls(**{n: value[n] for n in cls.__dataclass_fields__})


@dataclass(frozen=True, slots=True)
class MultiGraphOutputPage:
    outputs: tuple[MultiGraphJournalOutput, ...]
    cursor: MultiGraphOutputCursor

    def __post_init__(self) -> None:
        if type(self.cursor) is not MultiGraphOutputCursor:
            raise ValidationError("page requires an output cursor")
        self.cursor.__post_init__()
        self.cursor.to_json()
        if type(self.outputs) is not tuple or len(self.outputs) > 1000:
            raise ValidationError("invalid output page tuple")
        first = self.cursor.next_sequence - len(self.outputs)
        total = 0
        for i, item in enumerate(self.outputs):
            if type(item) is not MultiGraphJournalOutput or item.sequence != first + i:
                raise ValidationError("page outputs must be contiguous")
            item.__post_init__()
            total += len(_encoded(item.to_dict(), _OUTPUT_BYTES).encode("utf-8"))
            if total > _BATCH_BYTES:
                raise ValidationError("page exceeds output byte ceiling")
