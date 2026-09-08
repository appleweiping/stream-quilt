"""Atomic local multi-entry DAGs, shared operators and explicitly closed keyed joins."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

from .branching import (
    FlowBranch,
    FlowEdge,
    FlowMerge,
    GraphLimits,
    GraphOutput,
    _topological_order,
    _walk_graph,
)
from .dataflow import _MAX_COUNT, FlowRecord, FlowStep, _count, _FlowTransaction, _key
from .errors import ValidationError
from .keyed_join import (
    JoinCheckpoint,
    JoinRuntime,
    KeyedJoin,
    _json,
    _stage_close,
    _stage_drain,
    _stage_process,
)
from .keyed_join import (
    _State as _JoinState,
)
from .multi_checkpoint import (
    _MAX_CELLS,
    _MAX_STATE_BYTES,
    _MAX_VALUES,
    MultiGraphCheckpoint,
    _join_overhead,
    _ordinary_bytes,
)

GraphPhase = Literal["open", "draining", "closed"]


@dataclass(frozen=True, slots=True)
class FlowEntry:
    source_id: str
    step_id: str

    def __post_init__(self) -> None:
        _key(self.source_id, "source_id")
        _key(self.step_id, "entry step_id")


@dataclass(frozen=True, slots=True)
class FlowJoin:
    step_id: str
    join: KeyedJoin

    def __post_init__(self) -> None:
        _key(self.step_id, "join step_id")
        if type(self.join) is not KeyedJoin:
            raise ValidationError("join node requires KeyedJoin")
        self.join.__post_init__()
        if self.join.join_id != self.step_id:
            raise ValidationError("join_id must equal its graph step_id")


@dataclass(frozen=True, slots=True)
class JoinEdge:
    source: str
    target: str
    side: str
    route: bool | None = None

    def __post_init__(self) -> None:
        FlowEdge(self.source, self.target, self.route)
        _key(self.side, "join side")


@dataclass(frozen=True, slots=True)
class MultiGraphLimits:
    """Shared operation budget and summed canonical cell-wire retention budgets."""

    graph: GraphLimits = field(default_factory=GraphLimits)
    max_state_cells: int = 10_000
    max_join_values: int = 100_000
    max_state_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        if type(self.graph) is not GraphLimits:
            raise ValidationError("graph limits must be GraphLimits")
        self.graph.__post_init__()
        _count(self.max_state_cells, "max_state_cells", 1, _MAX_CELLS)
        _count(self.max_join_values, "max_join_values", 1, _MAX_VALUES)
        _count(self.max_state_bytes, "max_state_bytes", 1, _MAX_STATE_BYTES)

    def to_dict(self) -> dict[str, Any]:
        return {
            "graph": self.graph.to_dict(),
            "max_state_cells": self.max_state_cells,
            "max_join_values": self.max_join_values,
            "max_state_bytes": self.max_state_bytes,
        }


_Node = FlowStep | FlowBranch | FlowMerge | FlowJoin
_Edge = FlowEdge | JoinEdge


@dataclass(frozen=True, slots=True)
class MultiGraphDataflow:
    """Ordered trusted callbacks; identity binds declarations, not callback code."""

    flow_id: str
    revision: str
    nodes: tuple[_Node, ...]
    edges: tuple[_Edge, ...]
    entries: tuple[FlowEntry, ...]
    limits: MultiGraphLimits = field(default_factory=MultiGraphLimits)
    _order: tuple[str, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _key(self.flow_id, "flow_id")
        _key(self.revision, "revision")
        if type(self.nodes) is not tuple or not 1 <= len(self.nodes) <= 64:
            raise ValidationError("nodes must be a tuple of 1..64 graph nodes")
        for node in self.nodes:
            if type(node) not in (FlowStep, FlowBranch, FlowMerge, FlowJoin):
                raise ValidationError("invalid multi-source graph node")
            node.__post_init__()
        nodes = {node.step_id: node for node in self.nodes}
        if len(nodes) != len(self.nodes) or sum(type(n) is FlowJoin for n in self.nodes) > 16:
            raise ValidationError("graph requires unique node IDs and at most 16 joins")
        if type(self.entries) is not tuple or not 1 <= len(self.entries) <= 16:
            raise ValidationError("entries must be a tuple of 1..16 sources")
        source_ids: set[str] = set()
        roots: set[str] = set()
        for entry in self.entries:
            if type(entry) is not FlowEntry:
                raise ValidationError("entries must contain FlowEntry")
            entry.__post_init__()
            if (
                entry.source_id in source_ids
                or entry.step_id in roots
                or type(nodes.get(entry.step_id)) not in (FlowStep, FlowBranch)
            ):
                raise ValidationError("sources require distinct ordinary entry nodes")
            source_ids.add(entry.source_id)
            roots.add(entry.step_id)
        if type(self.edges) is not tuple or len(self.edges) > 256:
            raise ValidationError("edges must be a tuple of at most 256 edges")
        incoming: dict[str, list[_Edge]] = {key: [] for key in nodes}
        outgoing: dict[str, list[_Edge]] = {key: [] for key in nodes}
        seen: set[_Edge] = set()
        for edge in self.edges:
            if type(edge) not in (FlowEdge, JoinEdge):
                raise ValidationError("invalid graph edge")
            edge.__post_init__()
            if edge.source not in nodes or edge.target not in nodes or edge in seen:
                raise ValidationError("duplicate edge or unknown graph node")
            seen.add(edge)
            if isinstance(nodes[edge.target], FlowJoin) != isinstance(edge, JoinEdge):
                raise ValidationError(
                    "join ports require JoinEdge; ordinary ports require FlowEdge"
                )
            incoming[edge.target].append(edge)
            outgoing[edge.source].append(edge)
        order = _topological_order(tuple(nodes), incoming)
        reachable = set(roots)
        for key in order:
            if key in reachable:
                reachable.update(edge.target for edge in outgoing[key])
        if reachable != set(nodes):
            raise ValidationError("all nodes must be reachable from the declared sources")
        for key, node in nodes.items():
            inputs, outputs = incoming[key], outgoing[key]
            if isinstance(node, FlowJoin):
                sides = tuple(edge.side for edge in inputs if isinstance(edge, JoinEdge))
                if len(sides) != len(node.join.sides) or set(sides) != set(node.join.sides):
                    raise ValidationError("each join side requires exactly one producer edge")
            elif isinstance(node, FlowMerge):
                if len(inputs) < 2:
                    raise ValidationError("merge requires at least two incoming edges")
            elif len(inputs) != (0 if key in roots else 1):
                raise ValidationError("ordinary nodes require one input except source entries")
            if isinstance(node, FlowBranch):
                if {edge.route for edge in outputs} != {True, False}:
                    raise ValidationError("branch requires explicitly true and false routes")
            elif any(edge.route is not None for edge in outputs):
                raise ValidationError("only branch edges accept a route")
        if type(self.limits) is not MultiGraphLimits:
            raise ValidationError("limits must be MultiGraphLimits")
        self.limits.__post_init__()
        object.__setattr__(self, "_order", tuple(order))

    @property
    def execution_order(self) -> tuple[str, ...]:
        return self._order

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "stream-quilt-multisource-graph",
            "version": "1.0",
            "flow_id": self.flow_id,
            "revision": self.revision,
            "entries": [{"source_id": e.source_id, "step_id": e.step_id} for e in self.entries],
            "nodes": [
                {
                    "step_id": n.step_id,
                    "operator": n.operator
                    if isinstance(n, FlowStep)
                    else "branch"
                    if isinstance(n, FlowBranch)
                    else "merge"
                    if isinstance(n, FlowMerge)
                    else "join",
                    **({"join_identity": n.join.identity} if isinstance(n, FlowJoin) else {}),
                }
                for n in self.nodes
            ],
            "edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "route": e.route,
                    **({"side": e.side} if isinstance(e, JoinEdge) else {}),
                }
                for e in self.edges
            ],
            "limits": self.limits.to_dict(),
        }

    @property
    def identity(self) -> str:
        return hashlib.sha256(_json(self.to_dict()).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class GraphInput:
    """One local per-source sequential position, not broker delivery authentication."""

    source_id: str
    position: int
    record: FlowRecord

    def __post_init__(self) -> None:
        _key(self.source_id, "source_id")
        _count(self.position, "source position", 0, _MAX_COUNT)
        if type(self.record) is not FlowRecord:
            raise ValidationError("graph input record must be FlowRecord")


@dataclass(frozen=True, slots=True)
class MultiGraphBatch:
    outputs: tuple[GraphOutput, ...]
    operation_sequence: int
    phase: GraphPhase
    ready_joins: tuple[str, ...]

    def __post_init__(self) -> None:
        _count(self.operation_sequence, "operation sequence", 0, _MAX_COUNT)
        if type(self.phase) is not str or self.phase not in ("open", "draining", "closed"):
            raise ValidationError("invalid multi-source graph phase")
        if type(self.outputs) is not tuple or len(self.outputs) > 1_000_000:
            raise ValidationError("graph outputs must be a bounded tuple")
        byte_size = 0
        for output in self.outputs:
            if type(output) is not GraphOutput:
                raise ValidationError("graph outputs must contain GraphOutput")
            output.__post_init__()
            byte_size += output.record.byte_size
            if byte_size > 256 * 1024 * 1024:
                raise ValidationError("graph output bytes exceed the work ceiling")
        if type(self.ready_joins) is not tuple or len(self.ready_joins) > 16:
            raise ValidationError("invalid ready join tuple")
        for key in self.ready_joins:
            _key(key, "ready join")
        if len(set(self.ready_joins)) != len(self.ready_joins):
            raise ValidationError("duplicate ready join")
        if (self.phase == "closed" and self.ready_joins) or (
            self.phase == "draining" and not self.ready_joins
        ):
            raise ValidationError("graph phase contradicts ready joins")


@dataclass(frozen=True, slots=True)
class _State:
    ordinary: dict[tuple[str, str], str]
    ordinary_bytes: int
    joins: dict[str, _JoinState]
    positions: tuple[int, ...]
    closed: tuple[bool, ...]
    edge_counts: tuple[int, ...]
    operations: int = 0
    emitted: int = 0
    cells: int = 0
    values: int = 0
    wire_bytes: int = 0


class _Work:
    def __init__(self, limits: GraphLimits) -> None:
        self.limits = limits
        self.records = self.bytes = 0
        self.reserved_records = self.reserved_bytes = 0

    def ensure(self, records: int, byte_size: int) -> None:
        if (
            self.records + self.reserved_records + records > self.limits.max_work_records
            or self.bytes + self.reserved_bytes + byte_size > self.limits.max_work_bytes
        ):
            raise ValidationError("graph aggregate per-operation work budget exceeded")

    def charge(self, record: FlowRecord) -> None:
        if record.byte_size > self.limits.operator_limits.max_record_bytes:
            raise ValidationError("graph record exceeds configured byte limit")
        self.ensure(1, record.byte_size)
        self.records += 1
        self.bytes += record.byte_size


class MultiGraphRuntime:
    """One owner, explicit source EOF, and one outer state swap per successful operation."""

    def __init__(self, flow: MultiGraphDataflow) -> None:
        if type(flow) is not MultiGraphDataflow:
            raise ValidationError("flow must be MultiGraphDataflow")
        flow.__post_init__()
        self._flow = flow
        self._identity = flow.identity
        self._nodes = {n.step_id: n for n in flow.nodes}
        self._joins = {n.step_id: n.join for n in flow.nodes if isinstance(n, FlowJoin)}
        self._incoming = {
            key: tuple(e for e in flow.edges if e.target == key) for key in self._nodes
        }
        self._outgoing = {
            key: tuple(e for e in flow.edges if e.source == key) for key in self._nodes
        }
        self._edge_index = {edge: index for index, edge in enumerate(flow.edges)}
        self._sources = {entry.source_id: index for index, entry in enumerate(flow.entries)}
        self._roots = {entry.step_id: index for index, entry in enumerate(flow.entries)}
        self._state = _State(
            {},
            0,
            {
                key: _JoinState({}, (False,) * len(j.sides), (0,) * len(j.sides))
                for key, j in self._joins.items()
            },
            (0,) * len(flow.entries),
            (False,) * len(flow.entries),
            (0,) * len(flow.edges),
        )
        self._busy = False

    @property
    def flow(self) -> MultiGraphDataflow:
        return self._flow

    @property
    def phase(self) -> GraphPhase:
        if not all(self._state.closed):
            return "open"
        return "draining" if self.ready_joins else "closed"

    @property
    def ready_joins(self) -> tuple[str, ...]:
        return tuple(
            key
            for key in self.flow.execution_order
            if key in self._state.joins and self._state.joins[key].phase == "draining"
        )

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if self._busy:
            raise ValidationError("multi-source graph runtime is not reentrant")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    def _source(self, source_id: str, position: int) -> int:
        if type(source_id) is not str or source_id not in self._sources:
            raise ValidationError("unknown graph source")
        _count(position, "source position", 0, _MAX_COUNT)
        index = self._sources[source_id]
        if self._state.positions[index] != position:
            raise ValidationError("source position must equal its exact next local position")
        return index

    def _empty(self) -> MultiGraphBatch:
        return MultiGraphBatch((), self._state.operations, self.phase, self.ready_joins)

    def process(self, item: GraphInput) -> MultiGraphBatch:
        with self._operation():
            if type(item) is not GraphInput:
                raise ValidationError("input must be GraphInput")
            item.__post_init__()
            index = self._source(item.source_id, item.position)
            if self._state.closed[index]:
                raise ValidationError("graph source has already reached EOF")
            _count(sum(self._state.positions) + 1, "total processed inputs", 0, _MAX_COUNT)
            return self._execute(index=index, record=item.record)

    def close(self, source_id: str, *, next_position: int) -> MultiGraphBatch:
        """Exact-position repeated EOF is a no-callback, no-history-advance operation."""
        with self._operation():
            index = self._source(source_id, next_position)
            if self._state.closed[index]:
                return self._empty()
            return self._execute(index=index)

    def drain(self, *, max_keys: int = 100) -> MultiGraphBatch:
        """Drain one earliest ready final join, or return a stable no-op batch."""
        with self._operation():
            _count(max_keys, "drain max_keys", 1, _MAX_CELLS)
            ready = self.ready_joins
            if not ready:
                return self._empty()
            return self._execute(drain=ready[0], max_keys=max_keys)

    def _execute(
        self,
        *,
        index: int | None = None,
        record: FlowRecord | None = None,
        drain: str | None = None,
        max_keys: int = 100,
    ) -> MultiGraphBatch:
        before = self._state
        operations = _count(before.operations + 1, "operation sequence", 0, _MAX_COUNT)
        positions, closed, counts = (
            list(before.positions),
            list(before.closed),
            list(before.edge_counts),
        )
        joins = dict(before.joins)
        cells, values, wire_bytes = before.cells, before.values, before.wire_bytes
        work = _Work(self.flow.limits.graph)
        # Pending fanout reservations prevent a second join transition from
        # materializing rows using budget already owed to earlier transitions.
        pending: dict[_Edge, tuple[int, int]] = {}

        def retention(new_cells: int, new_values: int, new_bytes: int) -> None:
            limits = self.flow.limits
            if (
                new_cells > limits.max_state_cells
                or new_values > limits.max_join_values
                or new_bytes > limits.max_state_bytes
            ):
                raise ValidationError("multi-source aggregate retained state limit exceeded")

        def propose(key: tuple[str, str], old: str | None, new: str | None) -> None:
            nonlocal cells, wire_bytes
            nc = cells + int(new is not None) - int(old is not None)
            nb = wire_bytes + _ordinary_bytes(key, new) - _ordinary_bytes(key, old)
            retention(nc, values, nb)
            cells, wire_bytes = nc, nb

        transaction = _FlowTransaction(
            self.flow.limits.graph.operator_limits, before.ordinary, before.ordinary_bytes, propose
        )

        def admit(key: str, after: _JoinState, rows: int, byte_size: int) -> None:
            nonlocal cells, values, wire_bytes
            old = joins[key]
            nc = cells + len(after.cells) - len(old.cells)
            nv = values + after.values - old.values
            nb = (
                wire_bytes
                + after.byte_size
                - old.byte_size
                + (len(after.cells) - len(old.cells)) * _join_overhead(key)
            )
            retention(nc, nv, nb)
            edges = self._outgoing[key]
            work.ensure(rows * (1 + len(edges)), byte_size * (1 + len(edges)))
            work.reserved_records += rows * len(edges)
            work.reserved_bytes += byte_size * len(edges)
            for edge in edges:
                n, b = pending.get(edge, (0, 0))
                pending[edge] = (n + rows, b + byte_size)
            cells, values, wire_bytes = nc, nv, nb

        def joined(
            key: str, batches: tuple[tuple[_Edge, tuple[FlowRecord, ...]], ...]
        ) -> tuple[FlowRecord, ...]:
            output: list[FlowRecord] = []
            for edge, batch in batches:
                if not isinstance(edge, JoinEdge):
                    raise ValidationError("join received an ordinary edge")
                for item in batch:
                    after, result = _stage_process(
                        self._joins[key],
                        joins[key],
                        edge.side,
                        item,
                        lambda state, n, b: admit(key, state, n, b),
                    )
                    for row in result.rows:
                        converted = row.to_record()
                        work.charge(converted)
                        output.append(converted)
                    joins[key] = after
            return tuple(output)

        def delivered(edge: _Edge, count: int) -> None:
            reserved, size = pending.pop(edge, (0, 0))
            work.reserved_records -= reserved
            work.reserved_bytes -= size
            place = self._edge_index[edge]
            counts[place] = _count(counts[place] + count, "edge deliveries", 0, _MAX_COUNT)

        seeds: dict[str, tuple[FlowRecord, ...]] = {}
        seed_outputs: dict[str, tuple[FlowRecord, ...]] = {}
        if index is not None:
            if record is None:
                closed[index] = True
            else:
                work.charge(record)
                positions[index] += 1
                seeds[self.flow.entries[index].step_id] = (record,)
        if drain is not None:
            after, batch = _stage_drain(
                self._joins[drain],
                joins[drain],
                max_keys,
                lambda state, n, b: admit(drain, state, n, b),
            )
            seed_outputs[drain] = tuple(row.to_record() for row in batch.rows)
            joins[drain] = after
        published = _walk_graph(
            self.flow.execution_order,
            self._nodes,
            self._incoming,
            self._outgoing,
            seeds,
            transaction,
            work.charge,
            extension=joined,
            seed_outputs=seed_outputs,
            delivered=delivered,
        )
        # Final rows reach every downstream node before that join's EOF propagates.
        eof: dict[str, bool] = {}
        for key in self.flow.execution_order:
            if key in self._roots:
                eof[key] = closed[self._roots[key]]
            elif key in joins:
                for edge in self._incoming[key]:
                    if isinstance(edge, JoinEdge) and eof[edge.source]:
                        after, _ = _stage_close(self._joins[key], joins[key], edge.side)
                        admit(key, after, 0, 0)
                        joins[key] = after
                eof[key] = joins[key].phase == "closed"
            else:
                eof[key] = all(eof[e.source] for e in self._incoming[key])
        ready = tuple(
            key
            for key in self.flow.execution_order
            if key in joins and joins[key].phase == "draining"
        )
        phase: GraphPhase = "open" if not all(closed) else "draining" if ready else "closed"
        emitted = _count(before.emitted + len(published), "emitted records", 0, _MAX_COUNT)
        ordinary, ordinary_bytes = transaction.commit()
        result = MultiGraphBatch(tuple(published), operations, phase, ready)
        after_graph = _State(
            ordinary,
            ordinary_bytes,
            joins,
            tuple(positions),
            tuple(closed),
            tuple(counts),
            operations,
            emitted,
            cells,
            values,
            wire_bytes,
        )
        self._state = after_graph
        return result

    def checkpoint(self) -> MultiGraphCheckpoint:
        with self._operation():
            state = self._state
            return MultiGraphCheckpoint(
                self._identity,
                tuple(
                    (entry.source_id, state.positions[i], state.closed[i])
                    for i, entry in enumerate(self.flow.entries)
                ),
                tuple((step, key, value) for (step, key), value in sorted(state.ordinary.items())),
                tuple(
                    (
                        key,
                        JoinCheckpoint(
                            self._joins[key].identity,
                            self._joins[key].sides,
                            s.closed,
                            s.phase,
                            s.counts,
                            s.emitted,
                            tuple((k, c.values) for k, c in sorted(s.cells.items())),
                        ),
                    )
                    for key, s in state.joins.items()
                ),
                state.edge_counts,
                state.operations,
                state.emitted,
            )

    @classmethod
    def from_checkpoint(
        cls, flow: MultiGraphDataflow, checkpoint: MultiGraphCheckpoint
    ) -> MultiGraphRuntime:
        runtime = cls(flow)
        if type(checkpoint) is not MultiGraphCheckpoint:
            raise ValidationError("restore requires MultiGraphCheckpoint")
        checkpoint.__post_init__()
        if (
            checkpoint.identity != runtime._identity
            or tuple(s[0] for s in checkpoint.sources) != tuple(runtime._sources)
            or tuple(j[0] for j in checkpoint.joins) != tuple(runtime._joins)
            or len(checkpoint.edge_counts) != len(flow.edges)
        ):
            raise ValidationError("multi-source checkpoint configuration identity mismatch")
        limits = flow.limits.graph.operator_limits
        ordinary: dict[tuple[str, str], str] = {}
        ordinary_bytes = wire_bytes = 0
        stateful = {
            node.step_id
            for node in flow.nodes
            if isinstance(node, FlowStep) and node.operator in ("stateful_map", "stateful_flat_map")
        }
        for step, key, value in checkpoint.cells:
            size = len(value.encode("utf-8"))
            if step not in stateful or size > limits.max_state_value_bytes:
                raise ValidationError("checkpoint ordinary state is incompatible with graph")
            ordinary[(step, key)] = value
            ordinary_bytes += size
            wire_bytes += _ordinary_bytes((step, key), value)
        if len(ordinary) > limits.max_state_keys or ordinary_bytes > limits.max_state_bytes:
            raise ValidationError("checkpoint ordinary state exceeds configured limits")
        joins: dict[str, _JoinState] = {}
        cells, values = len(ordinary), 0
        for key, cp in checkpoint.joins:
            joins[key] = JoinRuntime.from_checkpoint(runtime._joins[key], cp)._state
            s = joins[key]
            cells += len(s.cells)
            values += s.values
            wire_bytes += s.byte_size + len(s.cells) * _join_overhead(key)
        if (
            cells > flow.limits.max_state_cells
            or values > flow.limits.max_join_values
            or wire_bytes > flow.limits.max_state_bytes
        ):
            raise ValidationError("checkpoint exceeds aggregate graph state limits")
        closed = tuple(s[2] for s in checkpoint.sources)
        inputs = sum(source[1] for source in checkpoint.sources)
        drains = checkpoint.operation_sequence - inputs - sum(closed)
        final_rows = sum(
            joins[key].emitted for key, join in runtime._joins.items() if join.emit_mode == "final"
        )
        if drains > final_rows or (not drains and final_rows):
            raise ValidationError("operation history contradicts final-join drainage")
        work_operations = inputs + drains
        if (
            sum(checkpoint.edge_counts) + checkpoint.emitted_records
            > work_operations * flow.limits.graph.max_work_records
        ):
            raise ValidationError("delivery counters exceed aggregate operation work")
        eof: dict[str, bool] = {}
        for key in flow.execution_order:
            if key in runtime._roots:
                eof[key] = closed[runtime._roots[key]]
            elif key in joins:
                for edge in runtime._incoming[key]:
                    if isinstance(edge, JoinEdge):
                        side = runtime._joins[key].sides.index(edge.side)
                        if (
                            joins[key].closed[side] != eof[edge.source]
                            or joins[key].counts[side]
                            != checkpoint.edge_counts[runtime._edge_index[edge]]
                        ):
                            raise ValidationError("join counters or EOF contradict producer edges")
                eof[key] = joins[key].phase == "closed"
            else:
                eof[key] = all(eof[e.source] for e in runtime._incoming[key])
        runtime._state = _State(
            ordinary,
            ordinary_bytes,
            joins,
            tuple(s[1] for s in checkpoint.sources),
            closed,
            checkpoint.edge_counts,
            checkpoint.operation_sequence,
            checkpoint.emitted_records,
            cells,
            values,
            wire_bytes,
        )
        return runtime

    def run(
        self, source: Iterable[GraphInput], *, max_inputs: int = 1000
    ) -> Iterator[MultiGraphBatch]:
        """Borrow the iterable, do not look ahead, and never infer EOF from exhaustion."""
        _count(max_inputs, "max_inputs", 1, 100_000)
        if self._busy:
            raise ValidationError("multi-source graph runtime is not reentrant")
        iterator = iter(source)
        for _ in range(max_inputs):
            try:
                item = next(iterator)
            except StopIteration:
                return
            yield self.process(item)


__all__ = [
    "FlowEntry",
    "FlowJoin",
    "GraphInput",
    "JoinEdge",
    "MultiGraphBatch",
    "MultiGraphCheckpoint",
    "MultiGraphDataflow",
    "MultiGraphLimits",
    "MultiGraphRuntime",
]
