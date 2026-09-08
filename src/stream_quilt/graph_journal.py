"""Durable source/state/terminal-output prefixes for bounded local DAGs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .branching import GraphCheckpoint, GraphDataflow, GraphOutput, GraphRuntime
from .dataflow import FlowCheckpoint, FlowRecord
from .errors import ValidationError
from .flow_journal import (
    FlowOutput,
    _JournalEngine,
    _point_document,
    _stored_record,
    _validate_point,
)

_APPLICATION_ID = 0x5351474A  # SQGJ; not the linear SQFJ or aligner journal.
_POINT_KIND = "stream-quilt-graph-recovery-point"
_OUTPUT_KIND = "stream-quilt-graph-journal-output"


@dataclass(frozen=True, slots=True)
class GraphRecoveryPoint:
    """One committed graph prefix; positions count source inputs, not outputs."""

    source_id: str
    generation: int
    next_position: int
    checkpoint: GraphCheckpoint

    def __post_init__(self) -> None:
        _validate_point(
            self.source_id, self.generation, self.next_position, self.checkpoint, GraphCheckpoint
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": _POINT_KIND,
            "schema_version": "1.0",
            "source_id": self.source_id,
            "generation": self.generation,
            "next_position": self.next_position,
            "checkpoint": self.checkpoint.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class GraphJournalOutput:
    """One immutable terminal output with durable sequence and source position."""

    sequence: int
    source_position: int
    step_id: str
    record: FlowRecord

    def __post_init__(self) -> None:
        FlowOutput(self.sequence, self.source_position, self.record)
        GraphOutput(self.step_id, self.record)

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": _OUTPUT_KIND,
            "sequence": self.sequence,
            "source_position": self.source_position,
            "step_id": self.step_id,
            "record": self.record.to_dict(),
        }


class GraphJournal(_JournalEngine[GraphDataflow, GraphRecoveryPoint, GraphJournalOutput]):
    """Local SQLite CAS over every sibling's state and terminal outputs.

    Uses the same private transaction/capacity engine as FlowJournal, but a
    distinct graph format. No distributed or external-effect atomicity implied.
    """

    _application_id = _APPLICATION_ID
    _application_pragma = "PRAGMA application_id = 1397835594"

    def __init__(
        self, path: str | Path, flow: GraphDataflow, source_id: str, *, create: bool = True
    ) -> None:
        GraphRuntime(flow)  # Validate before accessing topology or touching the path.
        nonterminal = {edge.source for edge in flow.edges}
        self._terminals = {
            step: rank for rank, step in enumerate(flow.execution_order) if step not in nonterminal
        }
        super().__init__(path, flow, source_id, create=create)

    def _runtime(self, checkpoint: FlowCheckpoint | None = None) -> GraphRuntime:
        if checkpoint is None:
            return GraphRuntime(self.flow)
        if type(checkpoint) is not GraphCheckpoint:
            raise ValidationError("graph journal requires GraphCheckpoint")
        return GraphRuntime.from_checkpoint(self.flow, checkpoint)

    def _point(
        self, generation: int, position: int, checkpoint: FlowCheckpoint
    ) -> GraphRecoveryPoint:
        if type(checkpoint) is not GraphCheckpoint:
            raise ValidationError("graph journal requires GraphCheckpoint")
        return GraphRecoveryPoint(self.source_id, generation, position, checkpoint)

    def _decode_point(self, document: Any) -> GraphRecoveryPoint:
        _point_document(document, kind=_POINT_KIND)
        return GraphRecoveryPoint(
            document["source_id"],
            document["generation"],
            document["next_position"],
            GraphCheckpoint.from_dict(document["checkpoint"]),
        )

    def _output(
        self, sequence: int, position: int, value: FlowRecord | GraphOutput
    ) -> GraphJournalOutput:
        if type(value) is not GraphOutput:
            raise ValidationError("graph journal requires terminal GraphOutput")
        return GraphJournalOutput(sequence, position, value.step_id, value.record)

    def _decode_output(self, document: Any) -> GraphJournalOutput:
        if (
            type(document) is not dict
            or set(document) != {"kind", "sequence", "source_position", "step_id", "record"}
            or document["kind"] != _OUTPUT_KIND
        ):
            raise ValidationError("invalid graph journal output fields")
        return GraphJournalOutput(
            document["sequence"],
            document["source_position"],
            document["step_id"],
            _stored_record(document["record"]),
        )

    def _validate_output(
        self, item: GraphJournalOutput, previous: GraphJournalOutput | None
    ) -> None:
        if item.record.byte_size > self.flow.limits.operator_limits.max_record_bytes:
            raise ValidationError("graph output violates its record contract")
        if item.step_id not in self._terminals:
            raise ValidationError("graph journal output must belong to a terminal node")
        if (
            previous is not None
            and previous.source_position == item.source_position
            and self._terminals[item.step_id] < self._terminals[previous.step_id]
        ):
            raise ValidationError("graph journal terminal output order is invalid")


__all__ = ["GraphJournal", "GraphJournalOutput", "GraphRecoveryPoint"]
