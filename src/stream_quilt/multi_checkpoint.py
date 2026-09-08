"""Strict portable multi-source state; local positions are not authenticated offsets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .dataflow import _MAX_COUNT, _count, _key, _snapshot
from .errors import ValidationError
from .io import _reject_duplicate_keys, _reject_json_constant
from .keyed_join import _HARD_LIMITS, JoinCheckpoint, _json, _shape

_MAX_CELLS = 100_000
_MAX_VALUES = 1_000_000
_MAX_STATE_BYTES = 64 * 1024 * 1024
_MAX_DOCUMENT_BYTES = 66 * 1024 * 1024
_Cell = tuple[str, str, str]
_Source = tuple[str, int, bool]


def _ordinary_bytes(key: tuple[str, str], value: str | None) -> int:
    return (
        0
        if value is None
        else len(_json({"step": key[0], "key": key[1], "value": value}).encode("utf-8"))
    )


def _join_overhead(step: str) -> int:
    return len(_json({"step": step, "cell": None}).encode("utf-8")) - 4


def _digest(value: Any) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValidationError("multi-source identity must be a SHA-256 hex digest")


class _Admission:
    """Count all cells/values/wire before parsing any encoded state values."""

    def __init__(self) -> None:
        self.cells = self.values = self.byte_size = 0

    def add(self, cells: int, values: int, byte_size: int) -> None:
        self.cells += cells
        self.values += values
        self.byte_size += byte_size
        if (
            self.cells > _MAX_CELLS
            or self.values > _MAX_VALUES
            or self.byte_size > _MAX_STATE_BYTES
        ):
            raise ValidationError("multi-source checkpoint aggregate state exceeds hard limits")

    def ordinary(self, step: Any, key: Any, value: Any) -> None:
        _key(step, "state step")
        _key(key, "state key")
        if type(value) is not str or len(value) > _HARD_LIMITS.max_value_bytes:
            raise ValidationError("state value must be a bounded encoded JSON string")
        try:
            if len(value.encode("utf-8")) > _HARD_LIMITS.max_value_bytes:
                raise ValidationError("state value exceeds its UTF-8 byte limit")
            self.add(1, 0, _ordinary_bytes((step, key), value))
        except UnicodeError as exc:
            raise ValidationError("state value must contain valid Unicode") from exc

    def join(
        self,
        step: str,
        cells: Any,
        side_count: int,
        kind: type[list[Any]] | type[tuple[Any, ...]],
    ) -> None:
        if type(cells) is not kind or len(cells) > _MAX_CELLS:
            raise ValidationError("invalid join state cells")
        overhead = _join_overhead(step)
        for cell in cells:
            if kind is tuple:
                if type(cell) is not tuple or len(cell) != 2:
                    raise ValidationError("invalid join state cell")
                key, values = cell
            else:
                if type(cell) is not dict or set(cell) != {"key", "values"}:
                    raise ValidationError("invalid join state cell fields")
                key, values = cell["key"], cell["values"]
            count, size = _shape(key, values, side_count, _HARD_LIMITS, kind)
            self.add(1, count, size + overhead)


@dataclass(frozen=True, slots=True)
class MultiGraphCheckpoint:
    """Versioned local position vector, shared state and complete join snapshots.

    Consistency checks do not authenticate the producer or reconstruct hidden history.
    JSON import bounds input bytes; the standard JSON parser materializes that bounded
    document before the fixed-shape, aggregate admission pass.
    """

    identity: str
    sources: tuple[_Source, ...]
    cells: tuple[_Cell, ...]
    joins: tuple[tuple[str, JoinCheckpoint], ...]
    edge_counts: tuple[int, ...]
    operation_sequence: int
    emitted_records: int

    def __post_init__(self) -> None:
        _digest(self.identity)
        if type(self.sources) is not tuple or not 1 <= len(self.sources) <= 16:
            raise ValidationError("checkpoint requires 1..16 sources")
        seen: set[str] = set()
        inputs = closed = 0
        for source in self.sources:
            if type(source) is not tuple or len(source) != 3:
                raise ValidationError("invalid checkpoint source")
            name, position, eof = source
            _key(name, "source_id")
            _count(position, "source next position", 0, _MAX_COUNT)
            if type(eof) is not bool or name in seen:
                raise ValidationError("invalid or duplicate checkpoint source")
            seen.add(name)
            inputs += position
            closed += eof
        _count(inputs, "total processed inputs", 0, _MAX_COUNT)
        _count(self.operation_sequence, "operation sequence", inputs + closed, _MAX_COUNT)
        _count(self.emitted_records, "emitted records", 0, _MAX_COUNT)
        if type(self.edge_counts) is not tuple or len(self.edge_counts) > 256:
            raise ValidationError("invalid edge counters")
        for count in self.edge_counts:
            _count(count, "edge deliveries", 0, _MAX_COUNT)
        if type(self.cells) is not tuple or len(self.cells) > _MAX_CELLS:
            raise ValidationError("invalid ordinary state cells")
        if type(self.joins) is not tuple or len(self.joins) > 16:
            raise ValidationError("invalid join checkpoint array")
        admission = _Admission()
        previous: tuple[str, str] | None = None
        for cell in self.cells:
            if type(cell) is not tuple or len(cell) != 3:
                raise ValidationError("invalid ordinary state cell")
            step, key, value = cell
            admission.ordinary(step, key, value)
            pair = (step, key)
            if previous is not None and pair <= previous:
                raise ValidationError("ordinary cells must have unique sorted step/key pairs")
            previous = pair
        seen.clear()
        for item in self.joins:
            if type(item) is not tuple or len(item) != 2:
                raise ValidationError("invalid join checkpoint entry")
            step, checkpoint = item
            _key(step, "join step")
            if step in seen or type(checkpoint) is not JoinCheckpoint:
                raise ValidationError("invalid or duplicate join checkpoint")
            seen.add(step)
            # Validate bounded side metadata before using its length for admission.
            if type(checkpoint.sides) is not tuple or not 2 <= len(checkpoint.sides) <= 16:
                raise ValidationError("invalid join side metadata")
            admission.join(step, checkpoint.cells, len(checkpoint.sides), tuple)
        for _, _, encoded in self.cells:
            try:
                value = json.loads(
                    encoded,
                    object_pairs_hook=_reject_duplicate_keys,
                    parse_constant=_reject_json_constant,
                )
                if _snapshot(value, _HARD_LIMITS.max_value_bytes) != encoded:
                    raise ValidationError("ordinary state JSON must be canonical")
            except (ValueError, RecursionError) as exc:
                raise ValidationError("invalid ordinary state JSON") from exc
        for _, checkpoint in self.joins:
            checkpoint.__post_init__()
        if not inputs and (
            self.cells
            or self.emitted_records
            or any(self.edge_counts)
            or any(cp.cells or cp.emitted_rows or any(cp.processed_inputs) for _, cp in self.joins)
        ):
            raise ValidationError("zero-input checkpoint cannot contain execution state")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "stream-quilt-multisource-graph-checkpoint",
            "version": "1.0",
            "identity": self.identity,
            "sources": [
                {"source_id": name, "next_position": position, "closed": eof}
                for name, position, eof in self.sources
            ],
            "cells": [
                {"step": step, "key": key, "value": value} for step, key, value in self.cells
            ],
            "joins": [{"step": step, "checkpoint": cp.to_dict()} for step, cp in self.joins],
            "edge_counts": list(self.edge_counts),
            "operation_sequence": self.operation_sequence,
            "emitted_records": self.emitted_records,
        }

    def to_json(self) -> str:
        self.__post_init__()
        payload = _json(self.to_dict())
        if len(payload.encode("utf-8")) > _MAX_DOCUMENT_BYTES:
            raise ValidationError("checkpoint exceeds document byte limit")
        return payload

    @classmethod
    def from_dict(cls, document: Any) -> MultiGraphCheckpoint:
        fields = {
            "kind",
            "version",
            "identity",
            "sources",
            "cells",
            "joins",
            "edge_counts",
            "operation_sequence",
            "emitted_records",
        }
        if (
            type(document) is not dict
            or set(document) != fields
            or document["kind"] != "stream-quilt-multisource-graph-checkpoint"
            or document["version"] != "1.0"
        ):
            raise ValidationError("invalid multi-source checkpoint fields/version")
        for name, low, high in (
            ("sources", 1, 16),
            ("cells", 0, _MAX_CELLS),
            ("joins", 0, 16),
            ("edge_counts", 0, 256),
        ):
            if type(document[name]) is not list or not low <= len(document[name]) <= high:
                raise ValidationError("invalid checkpoint array shape")
        _digest(document["identity"])
        _count(document["operation_sequence"], "operation sequence", 0, _MAX_COUNT)
        _count(document["emitted_records"], "emitted records", 0, _MAX_COUNT)
        source_ids: set[str] = set()
        inputs = closed = 0
        for source in document["sources"]:
            if type(source) is not dict or set(source) != {"source_id", "next_position", "closed"}:
                raise ValidationError("invalid source checkpoint fields")
            _key(source["source_id"], "source_id")
            _count(source["next_position"], "source next position", 0, _MAX_COUNT)
            if type(source["closed"]) is not bool or source["source_id"] in source_ids:
                raise ValidationError("invalid or duplicate checkpoint source")
            source_ids.add(source["source_id"])
            inputs += source["next_position"]
            closed += source["closed"]
        _count(inputs, "total processed inputs", 0, _MAX_COUNT)
        _count(document["operation_sequence"], "operation sequence", inputs + closed, _MAX_COUNT)
        for count in document["edge_counts"]:
            _count(count, "edge deliveries", 0, _MAX_COUNT)
        admission = _Admission()
        for cell in document["cells"]:
            if type(cell) is not dict or set(cell) != {"step", "key", "value"}:
                raise ValidationError("invalid ordinary cell fields")
            admission.ordinary(cell["step"], cell["key"], cell["value"])
        for item in document["joins"]:
            if type(item) is not dict or set(item) != {"step", "checkpoint"}:
                raise ValidationError("invalid join entry fields")
            _key(item["step"], "join step")
            cp = item["checkpoint"]
            if (
                type(cp) is not dict
                or set(cp)
                != {
                    "kind",
                    "version",
                    "identity",
                    "sides",
                    "closed_sides",
                    "phase",
                    "processed_inputs",
                    "emitted_rows",
                    "cells",
                }
                or cp["kind"] != "stream-quilt-join-checkpoint"
                or cp["version"] != "1.0"
                or type(cp.get("sides")) is not list
                or not 2 <= len(cp["sides"]) <= 16
            ):
                raise ValidationError("invalid nested join checkpoint shape")
            _digest(cp["identity"])
            for side in cp["sides"]:
                _key(side, "join side")
            if len(set(cp["sides"])) != len(cp["sides"]):
                raise ValidationError("duplicate join side")
            if (
                type(cp["closed_sides"]) is not list
                or len(cp["closed_sides"]) != len(cp["sides"])
                or any(type(flag) is not bool for flag in cp["closed_sides"])
                or type(cp["processed_inputs"]) is not list
                or len(cp["processed_inputs"]) != len(cp["sides"])
            ):
                raise ValidationError("invalid nested join side metadata")
            for count in cp["processed_inputs"]:
                _count(count, "join processed inputs", 0, _MAX_COUNT)
            _count(sum(cp["processed_inputs"]), "total join inputs", 0, _MAX_COUNT)
            _count(cp["emitted_rows"], "emitted join rows", 0, _MAX_COUNT)
            if type(cp["phase"]) is not str or cp["phase"] not in ("open", "draining", "closed"):
                raise ValidationError("invalid nested join phase")
            admission.join(item["step"], cp.get("cells"), len(cp["sides"]), list)
        return cls(
            document["identity"],
            tuple((s["source_id"], s["next_position"], s["closed"]) for s in document["sources"]),
            tuple((c["step"], c["key"], c["value"]) for c in document["cells"]),
            tuple(
                (j["step"], JoinCheckpoint.from_dict(j["checkpoint"])) for j in document["joins"]
            ),
            tuple(document["edge_counts"]),
            document["operation_sequence"],
            document["emitted_records"],
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> MultiGraphCheckpoint:
        if type(payload) not in (str, bytes) or len(payload) > _MAX_DOCUMENT_BYTES:
            raise ValidationError("checkpoint exceeds document byte limit")
        try:
            raw = payload.encode("utf-8") if isinstance(payload, str) else payload
            if len(raw) > _MAX_DOCUMENT_BYTES:
                raise ValidationError("checkpoint exceeds document UTF-8 byte limit")
            document = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (ValueError, RecursionError) as exc:
            raise ValidationError("invalid multi-source checkpoint JSON") from exc
        return cls.from_dict(document)


__all__ = ["MultiGraphCheckpoint"]
