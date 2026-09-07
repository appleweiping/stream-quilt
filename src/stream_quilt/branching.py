"""Bounded, deterministic local DAG execution over the shared operator engine."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from itertools import chain
from typing import Any

from .dataflow import (
    _MAX_COUNT,
    FlowCheckpoint,
    FlowExecutionError,
    FlowLimits,
    FlowRecord,
    FlowStep,
    _count,
    _FlowTransaction,
    _key,
    _sync,
)
from .errors import ValidationError


@dataclass(frozen=True, slots=True)
class FlowBranch:
    """Evaluate one strict boolean predicate per record; preserve value and key."""

    step_id: str
    predicate: Callable[[Any], bool] = field(repr=False)

    def __post_init__(self) -> None:
        _key(self.step_id, "step_id")
        _sync(self.predicate, "branch predicate")


@dataclass(frozen=True, slots=True)
class FlowMerge:
    """Concatenate incoming edges in their declaration order; never deduplicate."""

    step_id: str

    def __post_init__(self) -> None:
        _key(self.step_id, "step_id")


@dataclass(frozen=True, slots=True)
class FlowEdge:
    source: str
    target: str
    route: bool | None = None

    def __post_init__(self) -> None:
        _key(self.source, "edge source")
        _key(self.target, "edge target")
        if self.route is not None and type(self.route) is not bool:
            raise ValidationError("edge route must be a boolean or None")


@dataclass(frozen=True, slots=True)
class GraphLimits:
    """Global per-input work bounds, in addition to shared operator/state bounds.

    Charge one work record and its encoded value bytes for input admission,
    each node emission, and each edge delivery. Fanout therefore pays per edge.
    """

    operator_limits: FlowLimits = field(default_factory=FlowLimits)
    max_work_records: int = 10_000
    max_work_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        if type(self.operator_limits) is not FlowLimits:
            raise ValidationError("operator_limits must be FlowLimits")
        self.operator_limits.__post_init__()
        _count(self.max_work_records, "max_work_records", 1, 1_000_000)
        _count(self.max_work_bytes, "max_work_bytes", 1, 256 * 1024 * 1024)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator_limits": self.operator_limits.to_dict(),
            "max_work_records": self.max_work_records,
            "max_work_bytes": self.max_work_bytes,
        }


GraphNode = FlowStep | FlowBranch | FlowMerge


@dataclass(frozen=True, slots=True)
class GraphDataflow:
    """One explicit entry, bounded acyclic operators, and terminal output nodes.

    Revision is caller-managed: identities bind declarations, not callback code.
    Declaration order is semantic, including the order of merge inputs.
    """

    flow_id: str
    revision: str
    nodes: tuple[GraphNode, ...]
    edges: tuple[FlowEdge, ...]
    entry: str
    limits: GraphLimits = field(default_factory=GraphLimits)
    _order: tuple[str, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _key(self.flow_id, "flow_id")
        _key(self.revision, "revision")
        _key(self.entry, "entry")
        if type(self.nodes) is not tuple or not 1 <= len(self.nodes) <= 64:
            raise ValidationError("nodes must be a tuple of 1..64 graph nodes")
        for node in self.nodes:
            if type(node) not in (FlowStep, FlowBranch, FlowMerge):
                raise ValidationError("nodes must contain FlowStep, FlowBranch or FlowMerge")
            node.__post_init__()
        ids = [node.step_id for node in self.nodes]
        if len(set(ids)) != len(ids):
            raise ValidationError("graph node IDs must be unique")
        if self.entry not in ids:
            raise ValidationError("entry must name a graph node")
        if type(self.edges) is not tuple or len(self.edges) > 256:
            raise ValidationError("edges must be a tuple of at most 256 FlowEdge values")
        incoming: dict[str, list[FlowEdge]] = {key: [] for key in ids}
        outgoing: dict[str, list[FlowEdge]] = {key: [] for key in ids}
        seen: set[FlowEdge] = set()
        for edge in self.edges:
            if type(edge) is not FlowEdge:
                raise ValidationError("edges must contain FlowEdge values")
            edge.__post_init__()
            if edge.source not in incoming or edge.target not in incoming:
                raise ValidationError("edge names a missing graph node")
            if edge in seen:
                raise ValidationError("duplicate graph edge")
            seen.add(edge)
            incoming[edge.target].append(edge)
            outgoing[edge.source].append(edge)
        # Repeatedly select the earliest declared ready node. No callbacks run.
        order: list[str] = []
        remaining = dict.fromkeys(ids)
        while remaining:
            ready = next(
                (key for key in remaining if all(e.source in order for e in incoming[key])), None
            )
            if ready is None:
                raise ValidationError("graph must be acyclic")
            order.append(ready)
            del remaining[ready]
        reachable = {self.entry}
        for key in order:
            if key in reachable:
                reachable.update(edge.target for edge in outgoing[key])
        if reachable != set(ids):
            raise ValidationError("all graph nodes must be reachable from entry")
        for node in self.nodes:
            inputs, outputs = incoming[node.step_id], outgoing[node.step_id]
            if isinstance(node, FlowMerge):
                if len(inputs) < 2:
                    raise ValidationError("merge requires at least two incoming edges")
            elif len(inputs) != (0 if node.step_id == self.entry else 1):
                raise ValidationError("only merge nodes accept multiple incoming edges")
            if isinstance(node, FlowBranch):
                if {edge.route for edge in outputs} != {True, False}:
                    raise ValidationError("branch requires explicitly true and false routes")
            elif any(edge.route is not None for edge in outputs):
                raise ValidationError("only branch edges accept a route")
        if type(self.limits) is not GraphLimits:
            raise ValidationError("limits must be GraphLimits")
        self.limits.__post_init__()
        object.__setattr__(self, "_order", tuple(order))

    @property
    def execution_order(self) -> tuple[str, ...]:
        return self._order

    def to_dict(self) -> dict[str, Any]:
        """Inspect declarations; this does not serialize or load callback code."""
        return {
            "kind": "stream-quilt-graph",
            "schema_version": "1.0",
            "flow_id": self.flow_id,
            "revision": self.revision,
            "entry": self.entry,
            "nodes": [
                {
                    "step_id": node.step_id,
                    "operator": (
                        node.operator
                        if isinstance(node, FlowStep)
                        else "branch"
                        if isinstance(node, FlowBranch)
                        else "merge"
                    ),
                }
                for node in self.nodes
            ],
            "edges": [
                {"source": edge.source, "target": edge.target, "route": edge.route}
                for edge in self.edges
            ],
            "limits": self.limits.to_dict(),
        }

    @property
    def identity(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class GraphCheckpoint(FlowCheckpoint):
    """Graph-specific format using the strict portable state-cell validator."""

    def to_dict(self) -> dict[str, Any]:
        document = FlowCheckpoint.to_dict(self)
        document["kind"] = "stream-quilt-graph-checkpoint"
        return document

    @classmethod
    def from_dict(cls, document: Any) -> GraphCheckpoint:
        if type(document) is not dict or document.get("kind") != "stream-quilt-graph-checkpoint":
            raise ValidationError("invalid graph checkpoint kind")
        checked = FlowCheckpoint.from_dict({**document, "kind": "stream-quilt-dataflow-checkpoint"})
        return cls(
            checked.identity, checked.processed_inputs, checked.emitted_records, checked.cells
        )


@dataclass(frozen=True, slots=True)
class GraphOutput:
    """One terminal node's immutable value; outputs are not external sink writes."""

    step_id: str
    record: FlowRecord

    def __post_init__(self) -> None:
        _key(self.step_id, "output step_id")
        if type(self.record) is not FlowRecord:
            raise ValidationError("output record must be FlowRecord")

    def to_dict(self) -> dict[str, Any]:
        return {"step_id": self.step_id, **self.record.to_dict()}


class GraphRuntime:
    """Single-owner in-memory DAG; one atomic internal transaction per input."""

    def __init__(self, flow: GraphDataflow) -> None:
        if type(flow) is not GraphDataflow:
            raise ValidationError("flow must be GraphDataflow")
        flow.__post_init__()
        self._flow = flow
        self._nodes = {node.step_id: node for node in flow.nodes}
        self._incoming = {
            key: tuple(edge for edge in flow.edges if edge.target == key) for key in self._nodes
        }
        self._outgoing = {
            key: tuple(edge for edge in flow.edges if edge.source == key) for key in self._nodes
        }
        self._state: dict[tuple[str, str], str] = {}
        self._state_bytes = 0
        self._processed_inputs = 0
        self._emitted_records = 0
        self._busy = False

    @property
    def flow(self) -> GraphDataflow:
        return self._flow

    @property
    def processed_inputs(self) -> int:
        return self._processed_inputs

    @property
    def emitted_records(self) -> int:
        return self._emitted_records

    def checkpoint(self) -> GraphCheckpoint:
        if self._busy:
            raise ValidationError("cannot checkpoint during a graph transaction")
        return GraphCheckpoint(
            self.flow.identity,
            self.processed_inputs,
            self.emitted_records,
            tuple((step, key, value) for (step, key), value in sorted(self._state.items())),
        )

    @classmethod
    def from_checkpoint(cls, flow: GraphDataflow, checkpoint: GraphCheckpoint) -> GraphRuntime:
        runtime = cls(flow)
        if type(checkpoint) is not GraphCheckpoint:
            raise ValidationError("checkpoint must be GraphCheckpoint")
        checked = GraphCheckpoint.from_dict(checkpoint.to_dict())
        if checked.identity != flow.identity:
            raise ValidationError("graph identity/topology/revision/configuration mismatch")
        limits = flow.limits.operator_limits
        stateful = {
            node.step_id
            for node in flow.nodes
            if isinstance(node, FlowStep) and node.operator == "stateful_map"
        }
        for step, key, value in checked.cells:
            if step not in stateful or len(value.encode()) > limits.max_state_value_bytes:
                raise ValidationError("checkpoint state is incompatible with the graph")
            runtime._state[(step, key)] = value
            runtime._state_bytes += len(value.encode())
        if (
            len(runtime._state) > limits.max_state_keys
            or runtime._state_bytes > limits.max_state_bytes
        ):
            raise ValidationError("checkpoint exceeds graph state limits")
        runtime._processed_inputs = checked.processed_inputs
        runtime._emitted_records = checked.emitted_records
        return runtime

    def process(self, record: FlowRecord) -> tuple[GraphOutput, ...]:
        if self._busy:
            raise ValidationError("graph runtime is not reentrant")
        if type(record) is not FlowRecord:
            raise ValidationError("input must be FlowRecord")
        if record.byte_size > self.flow.limits.operator_limits.max_record_bytes:
            raise ValidationError("input record exceeds byte limit")
        _count(self.processed_inputs + 1, "processed_inputs", 0, _MAX_COUNT)
        self._busy = True
        try:
            return self._process(record)
        finally:
            self._busy = False

    def _process(self, record: FlowRecord) -> tuple[GraphOutput, ...]:
        limits = self.flow.limits
        transaction = _FlowTransaction(limits.operator_limits, self._state, self._state_bytes)
        work_records = work_bytes = 0

        def charge(item: FlowRecord) -> None:
            nonlocal work_records, work_bytes
            work_records += 1
            work_bytes += item.byte_size
            if work_records > limits.max_work_records or work_bytes > limits.max_work_bytes:
                raise ValidationError("graph aggregate per-input work budget exceeded")

        charge(record)
        mailboxes: dict[FlowEdge, tuple[FlowRecord, ...]] = {}
        published: list[GraphOutput] = []
        for key in self.flow.execution_order:
            node = self._nodes[key]
            # Edge batches are already charged. Empty branches still close their
            # edge with an empty tuple; merge never waits for another input.
            current = (
                iter((record,))
                if key == self.flow.entry
                else chain.from_iterable(mailboxes.pop(edge) for edge in self._incoming[key])
            )
            try:
                routed: dict[bool | None, tuple[FlowRecord, ...]]
                if isinstance(node, FlowStep):
                    routed = {None: transaction.apply(node, current, charge)}
                elif isinstance(node, FlowBranch):
                    branches: dict[bool, list[FlowRecord]] = {True: [], False: []}
                    for item in current:
                        route = transaction.invoke(node.predicate, item.value)
                        if type(route) is not bool:
                            raise ValidationError("branch predicate must return bool")
                        charge(item)
                        branches[route].append(item)
                    routed = {route: tuple(batch) for route, batch in branches.items()}
                else:
                    merged = []
                    for item in current:
                        charge(item)
                        merged.append(item)
                    routed = {None: tuple(merged)}
                for edge in self._outgoing[key]:
                    batch = routed[edge.route]
                    for item in batch:
                        charge(item)
                    mailboxes[edge] = batch
                if not self._outgoing[key]:
                    published.extend(GraphOutput(key, item) for item in routed[None])
            except Exception:
                raise FlowExecutionError(key) from None
        processed_inputs = self.processed_inputs + 1
        emitted_records = _count(
            self.emitted_records + len(published), "emitted_records", 0, _MAX_COUNT
        )
        # Allocate the return tuple before committing so a materialization failure
        # cannot publish state without returning this input's terminal outputs.
        result = tuple(published)
        self._state, self._state_bytes = transaction.commit()
        self._processed_inputs = processed_inputs
        self._emitted_records = emitted_records
        return result

    def run(
        self, records: Iterable[FlowRecord], *, max_inputs: int = 100_000
    ) -> Iterator[GraphOutput]:
        """Pull no more than the cap; one input's outputs precede the next pull."""
        _count(max_inputs, "max_inputs", 1, 1_000_000)
        if self._busy:
            raise ValidationError("graph runtime is not reentrant")
        iterator = iter(records)
        for _ in range(max_inputs):
            try:
                record = next(iterator)
            except StopIteration:
                return
            yield from self.process(record)


__all__ = [
    "FlowBranch",
    "FlowEdge",
    "FlowMerge",
    "GraphCheckpoint",
    "GraphDataflow",
    "GraphLimits",
    "GraphOutput",
    "GraphRuntime",
]
