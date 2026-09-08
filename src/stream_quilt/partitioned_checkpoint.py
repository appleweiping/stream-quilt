"""Bounded portable parent-authoritative snapshots for local keyed workers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .dataflow import _MAX_COUNT, Dataflow, FlowCheckpoint, FlowRuntime, _count, _key
from .errors import ValidationError
from .flow_journal import _encode
from .io import _reject_duplicate_keys, _reject_json_constant
from .recovery import _finite_json_float

_ROUTE = "sha256-key-v1"
_PREFIX = b"stream-quilt-key-route-v1\0"
_MAX_WIRE = 64 * 1024 * 1024
_PRESERVING = {"map", "filter", "flat_map", "stateful_map", "stateful_flat_map"}


def _name(value: Any, label: str) -> str:
    if type(value) is not str or len(value) > 1024:
        raise ValidationError(f"{label} must be a bounded string")
    return _key(value, label)


def _hex(value: Any) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValidationError("expected a lowercase SHA-256 digest")


def _shape(value: Any, fields: set[str]) -> None:
    if type(value) is not dict or len(value) != len(fields) or set(value) != fields:
        raise ValidationError("invalid local worker document fields")


def _json(value: Any, maximum: int) -> str:
    return _encode(value, max_bytes=maximum, label="local worker")


def _load(value: str | bytes, maximum: int) -> Any:
    if type(value) not in (str, bytes) or len(value) > maximum:
        raise ValidationError("local worker JSON exceeds byte limit")
    try:
        raw = value.encode("utf-8") if isinstance(value, str) else value
        if len(raw) > maximum:
            raise ValidationError("local worker JSON exceeds UTF-8 limit")
        text = raw.decode("utf-8")
        document = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
        if _json(document, maximum) != text:
            raise ValidationError("local worker JSON must be canonical")
        return document
    except (ValueError, RecursionError) as exc:
        raise ValidationError("invalid local worker JSON") from exc


def partition_for(key: str, workers: int) -> int:
    _name(key, "record key")
    _count(workers, "workers", 1, 8)
    return (
        int.from_bytes(hashlib.sha256(_PREFIX + key.encode("utf-8")).digest()[:8], "big") % workers
    )


@dataclass(frozen=True, slots=True)
class LocalWorkerLimits:
    """Whole-wave wire/work/state limits, not callback or native allocator quotas."""

    max_batch_inputs: int = 256
    max_input_bytes: int = 8 * 1024 * 1024
    max_message_bytes: int = 16 * 1024 * 1024
    max_result_bytes: int = 64 * 1024 * 1024
    max_output_records: int = 100_000
    max_output_bytes: int = 64 * 1024 * 1024
    max_state_cells: int = 10_000
    max_state_bytes: int = 16 * 1024 * 1024
    max_checkpoint_bytes: int = 64 * 1024 * 1024
    max_callback_reservation: int = 4_000_000
    startup_timeout: float = 30.0
    wave_timeout: float = 30.0
    cleanup_timeout: float = 5.0

    def __post_init__(self) -> None:
        for name, ceiling in (
            ("max_batch_inputs", 256),
            ("max_input_bytes", 8 * 1024 * 1024),
            ("max_message_bytes", 16 * 1024 * 1024),
            ("max_result_bytes", _MAX_WIRE),
            ("max_output_records", 100_000),
            ("max_output_bytes", _MAX_WIRE),
            ("max_state_cells", 100_000),
            ("max_state_bytes", _MAX_WIRE),
            ("max_checkpoint_bytes", _MAX_WIRE),
            ("max_callback_reservation", 16_000_000),
        ):
            _count(getattr(self, name), name, 1, ceiling)
        for name, ceiling in (
            ("startup_timeout", 3600),
            ("wave_timeout", 3600),
            ("cleanup_timeout", 30),
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not 0 < value <= ceiling:
                raise ValidationError(f"{name} must be a finite positive bounded number")
            object.__setattr__(self, name, float(value))

    def to_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Any) -> LocalWorkerLimits:
        _shape(value, set(cls.__dataclass_fields__))
        return cls(**value)


def _flow(flow: Dataflow) -> None:
    if type(flow) is not Dataflow:
        raise ValidationError("local partitioned execution requires Dataflow")
    flow.__post_init__()
    flow.limits.__post_init__()
    if any(step.operator not in _PRESERVING for step in flow.steps):
        raise ValidationError("local partitioned flows must preserve their input keys")


def _shard_wire(shard: FlowCheckpoint) -> dict[str, Any]:
    # Exact-type frozen instances can still be forged with object.__setattr__.
    # Reject malformed/oversized fixed shape before a comprehension or JSON parse.
    if type(shard) is not FlowCheckpoint or type(shard.cells) is not tuple:
        raise ValidationError("local shard requires bounded FlowCheckpoint cells")
    if len(shard.cells) > 100_000:
        raise ValidationError("local shard has too many state cells")
    _hex(shard.identity)
    _count(shard.processed_inputs, "processed_inputs", 0, _MAX_COUNT)
    _count(shard.emitted_records, "emitted_records", 0, _MAX_COUNT)
    for cell in shard.cells:
        if type(cell) is not tuple or len(cell) != 3:
            raise ValidationError("invalid local shard cell shape")
        _name(cell[0], "state step")
        _name(cell[1], "state key")
        if type(cell[2]) is not str or len(cell[2]) > 8 * 1024 * 1024:
            raise ValidationError("local shard has oversized encoded state")
    return {
        "identity": shard.identity,
        "processed_inputs": shard.processed_inputs,
        "emitted_records": shard.emitted_records,
        "cells": [
            {"step": step, "key": key, "encoded_value": value} for step, key, value in shard.cells
        ],
    }


def _admit_shard(value: Any, limits: LocalWorkerLimits) -> tuple[int, int]:
    """Admit all fixed cells and their encoded bytes before parsing any cell JSON."""
    _shape(value, {"identity", "processed_inputs", "emitted_records", "cells"})
    _hex(value["identity"])
    for name in ("processed_inputs", "emitted_records"):
        _count(value[name], name, 0, _MAX_COUNT)
    cells = value["cells"]
    if type(cells) is not list or len(cells) > limits.max_state_cells:
        raise ValidationError("local shard has too many state cells")
    size = 0
    for cell in cells:
        _shape(cell, {"step", "key", "encoded_value"})
        _name(cell["step"], "state step")
        _name(cell["key"], "state key")
        raw = cell["encoded_value"]
        if type(raw) is not str or len(raw) > 8 * 1024 * 1024:
            raise ValidationError("local shard has oversized encoded state")
        size += len(_json(cell, limits.max_state_bytes).encode("utf-8"))
        if size > limits.max_state_bytes:
            raise ValidationError("local shard exceeds state wire capacity")
    return len(cells), size


def _shard_load(value: Any, limits: LocalWorkerLimits) -> FlowCheckpoint:
    _admit_shard(value, limits)
    return FlowCheckpoint(
        value["identity"],
        value["processed_inputs"],
        value["emitted_records"],
        tuple((cell["step"], cell["key"], cell["encoded_value"]) for cell in value["cells"]),
    )


@dataclass(frozen=True, slots=True)
class PartitionedFlowCheckpoint:
    """Necessary consistency, not authenticated history or a durable sink receipt."""

    flow_identity: str
    source_id: str
    source_digest: str
    workers: int
    limits: LocalWorkerLimits
    next_position: int
    waves: int
    source_closed: bool
    shards: tuple[FlowCheckpoint, ...]
    _size: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _hex(self.flow_identity)
        _hex(self.source_digest)
        _name(self.source_id, "source ID")
        _count(self.workers, "workers", 1, 8)
        if type(self.limits) is not LocalWorkerLimits:
            raise ValidationError("limits must be LocalWorkerLimits")
        self.limits.__post_init__()
        _count(self.next_position, "source next position", 0, _MAX_COUNT)
        _count(self.waves, "waves", 0, self.next_position)
        if self.next_position > self.waves * self.limits.max_batch_inputs:
            raise ValidationError("source position is inconsistent with wave count")
        if (
            type(self.source_closed) is not bool
            or type(self.shards) is not tuple
            or len(self.shards) != self.workers
        ):
            raise ValidationError("invalid local checkpoint shard/EOF shape")
        count = size = processed = emitted = 0
        for index, shard in enumerate(self.shards):
            if type(shard) is not FlowCheckpoint:
                raise ValidationError("local shard requires FlowCheckpoint")
            cells, wire_size = _admit_shard(_shard_wire(shard), self.limits)
            count += cells
            size += wire_size
            if count > self.limits.max_state_cells or size > self.limits.max_state_bytes:
                raise ValidationError("aggregate local shard state limit exceeded")
            if shard.identity != self.flow_identity:
                raise ValidationError("shard flow identity mismatch")
            if shard.processed_inputs == 0 and (shard.cells or shard.emitted_records):
                raise ValidationError("unused shard cannot contain computed state or output")
            if any(partition_for(key, self.workers) != index for _, key, _ in shard.cells):
                raise ValidationError("state key belongs to a different shard")
            processed += shard.processed_inputs
            emitted += shard.emitted_records
        if processed != self.next_position:
            raise ValidationError("source position differs from summed shard counts")
        _count(emitted, "emitted records", 0, _MAX_COUNT)
        for shard in self.shards:
            shard.__post_init__()
        object.__setattr__(
            self,
            "_size",
            len(_json(self._document(), self.limits.max_checkpoint_bytes).encode("utf-8")),
        )

    @property
    def emitted_records(self) -> int:
        return sum(shard.emitted_records for shard in self.shards)

    def validate_for(self, flow: Dataflow) -> None:
        self.__post_init__()
        _flow(flow)
        if self.flow_identity != flow.identity:
            raise ValidationError("local checkpoint flow mismatch")
        stateful = sum(
            step.operator in {"stateful_map", "stateful_flat_map"} for step in flow.steps
        )
        for shard in self.shards:
            if (
                len(shard.cells) > shard.processed_inputs * stateful
                or shard.emitted_records
                > shard.processed_inputs * flow.limits.max_records_per_input
            ):
                raise ValidationError("shard counters cannot arise from this flow")
            FlowRuntime.from_checkpoint(flow, shard)

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return self._document()

    def _document(self) -> dict[str, Any]:
        return {
            "kind": "stream-quilt-partitioned-flow",
            "version": "1.0",
            "routing": _ROUTE,
            "flow_identity": self.flow_identity,
            "source_id": self.source_id,
            "source_digest": self.source_digest,
            "workers": self.workers,
            "limits": self.limits.to_dict(),
            "next_position": self.next_position,
            "waves": self.waves,
            "source_closed": self.source_closed,
            "shards": [_shard_wire(s) for s in self.shards],
        }

    def to_json(self) -> str:
        return _json(self.to_dict(), self.limits.max_checkpoint_bytes)

    @classmethod
    def from_dict(cls, value: Any) -> PartitionedFlowCheckpoint:
        _shape(
            value,
            {
                "kind",
                "version",
                "routing",
                "flow_identity",
                "source_id",
                "source_digest",
                "workers",
                "limits",
                "next_position",
                "waves",
                "source_closed",
                "shards",
            },
        )
        if (value["kind"], value["version"], value["routing"]) != (
            "stream-quilt-partitioned-flow",
            "1.0",
            _ROUTE,
        ):
            raise ValidationError("unsupported local checkpoint profile")
        limits = LocalWorkerLimits.from_dict(value["limits"])
        _count(value["workers"], "workers", 1, 8)
        _hex(value["flow_identity"])
        _hex(value["source_digest"])
        _name(value["source_id"], "source ID")
        _count(value["next_position"], "next position", 0, _MAX_COUNT)
        _count(value["waves"], "waves", 0, value["next_position"])
        if type(value["source_closed"]) is not bool:
            raise ValidationError("invalid source EOF state")
        if type(value["shards"]) is not list or len(value["shards"]) != value["workers"]:
            raise ValidationError("invalid local checkpoint shard count")
        cells = size = 0
        for shard in value["shards"]:
            nc, nb = _admit_shard(shard, limits)
            cells += nc
            size += nb
            if cells > limits.max_state_cells or size > limits.max_state_bytes:
                raise ValidationError("aggregate local checkpoint admission limit exceeded")
        _json(value, limits.max_checkpoint_bytes)
        return cls(
            value["flow_identity"],
            value["source_id"],
            value["source_digest"],
            value["workers"],
            limits,
            value["next_position"],
            value["waves"],
            value["source_closed"],
            tuple(_shard_load(s, limits) for s in value["shards"]),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> PartitionedFlowCheckpoint:
        return cls.from_dict(_load(value, _MAX_WIRE))
