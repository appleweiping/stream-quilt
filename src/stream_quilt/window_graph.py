"""One-window local DAGs; explicit progress and one unpublished graph transaction."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from .branching import (
    FlowBranch,
    FlowEdge,
    FlowMerge,
    GraphDataflow,
    GraphLimits,
    GraphOutput,
    _walk_graph,
)
from .dataflow import _MAX_COUNT, FlowRecord, FlowStep, _count, _FlowTransaction
from .errors import ValidationError
from .flow_journal import _encode
from .multi_graph import _Work
from .window_fold import (
    WindowFold,
    WindowRow,
    WindowStatus,
    _Cell,
    _checked_value,
    _digest,
    _name,
    _stage_advance,
    _stage_drain,
    _stage_finish,
    _stage_process,
    _StageHooks,
    _status,
    _tick,
    _unavailable,
)
from .window_fold import (
    _State as _WindowState,
)

if TYPE_CHECKING:
    from .window_graph_checkpoint import WindowGraphCheckpoint

_MAX_MANIFEST = 512 * 1024
_MAX_CELLS = 100_000
_MAX_STATE_BYTES = 64 * 1024 * 1024
_POLICIES = {
    "order": "arrival",
    "timestamp": "source-preserved",
    "drain_order": "window-major",
    "callback_budget": "operation",
}


def _wire(value: Any, maximum: int = 66 * 1024 * 1024) -> str:
    return _encode(value, max_bytes=maximum, label="window graph")


def _ordinary_wire(key: tuple[str, str], encoded: str) -> dict[str, str]:
    return {"step_id": key[0], "key": key[1], "state": encoded}


def _ordinary_size(key: tuple[str, str], encoded: str) -> int:
    return len(_wire(_ordinary_wire(key, encoded), _MAX_STATE_BYTES).encode("utf-8"))


@dataclass(frozen=True, slots=True)
class FlowWindow:
    step_id: str
    fold: WindowFold

    def __post_init__(self) -> None:
        _name(self.step_id, "window node ID")
        if type(self.fold) is not WindowFold:
            raise ValidationError("window node requires WindowFold")
        self.fold.__post_init__()
        if self.step_id != self.fold.fold_id:
            raise ValidationError("window node ID must match its fold ID")


@dataclass(frozen=True, slots=True)
class WindowGraphLimits:
    graph: GraphLimits = field(default_factory=GraphLimits)
    max_state_cells: int = 10_000
    max_state_bytes: int = 16 * 1024 * 1024
    max_source_inputs: int = 1_000_000_000

    def __post_init__(self) -> None:
        if type(self.graph) is not GraphLimits:
            raise ValidationError("window graph requires GraphLimits")
        self.graph.__post_init__()
        _count(self.max_state_cells, "aggregate state cells", 1, _MAX_CELLS)
        _count(self.max_state_bytes, "aggregate cell wire bytes", 1, _MAX_STATE_BYTES)
        _count(self.max_source_inputs, "source inputs", 1, _MAX_COUNT)

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return {
            "graph": self.graph.to_dict(),
            "max_state_cells": self.max_state_cells,
            "max_state_bytes": self.max_state_bytes,
            "max_source_inputs": self.max_source_inputs,
        }


WindowGraphNode = FlowStep | FlowBranch | FlowMerge | FlowWindow


@dataclass(frozen=True, slots=True)
class WindowGraphDataflow:
    """An original exact profile; old graphs and workers do not accept it."""

    flow_id: str
    revision: str
    nodes: tuple[WindowGraphNode, ...]
    edges: tuple[FlowEdge, ...]
    entry: str = field(kw_only=True)
    limits: WindowGraphLimits = field(default_factory=WindowGraphLimits, kw_only=True)
    _order: tuple[str, ...] = field(init=False, repr=False)
    _window: FlowWindow = field(init=False, repr=False)
    _prefix: frozenset[str] = field(init=False, repr=False)
    _drain_cap: int = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for label in ("flow_id", "revision", "entry"):
            _name(getattr(self, label), label)
        if type(self.nodes) is not tuple or not 2 <= len(self.nodes) <= 64:
            raise ValidationError("window graph nodes require an exact tuple of 2..64 nodes")
        if type(self.edges) is not tuple or not 1 <= len(self.edges) <= 256:
            raise ValidationError("window graph edges require an exact tuple of 1..256 edges")
        if type(self.limits) is not WindowGraphLimits:
            raise ValidationError("window graph limits require WindowGraphLimits")
        self.limits.__post_init__()
        windows = []
        ordinary: list[FlowStep | FlowBranch | FlowMerge] = []
        for node in self.nodes:
            if type(node) not in (FlowStep, FlowBranch, FlowMerge, FlowWindow):
                raise ValidationError("unsupported window graph node")
            _name(node.step_id, "node ID")
            node.__post_init__()
            if isinstance(node, FlowWindow):
                windows.append(node)
                # Reuse ordinary topology validation, never execute this placeholder.
                ordinary.append(FlowStep(node.step_id, "map", _unavailable))
            else:
                ordinary.append(node)
        if len(windows) != 1 or windows[0].step_id == self.entry:
            raise ValidationError("exactly one non-entry window node is required")
        for edge in self.edges:
            if type(edge) is not FlowEdge:
                raise ValidationError("window graph edges must be FlowEdge")
            _name(edge.source, "edge source")
            _name(edge.target, "edge target")
        topology = GraphDataflow(
            self.flow_id, self.revision, tuple(ordinary), self.edges, self.entry, self.limits.graph
        )
        window = windows[0]
        bypass = {self.entry}
        outgoing: dict[str, list[FlowEdge]] = {node.step_id: [] for node in self.nodes}
        for edge in self.edges:
            outgoing[edge.source].append(edge)
        for key in topology.execution_order:
            if key in bypass:
                bypass.update(
                    edge.target for edge in outgoing[key] if edge.target != window.step_id
                )
        if any(key in bypass and not values for key, values in outgoing.items()):
            raise ValidationError("the window must dominate every terminal output")
        graph, operator, fold = self.limits.graph, self.limits.graph.operator_limits, window.fold
        row, copies = fold.limits.max_row_bytes, 1 + len(outgoing[window.step_id])
        if (
            row > operator.max_record_bytes
            or row > operator.max_batch_bytes
            or copies > graph.max_work_records
            or copies * row > graph.max_work_bytes
        ):
            raise ValidationError("one maximum window row must fit its immediate graph reservation")
        cap = min(
            fold.limits.max_rows_per_batch,
            fold.limits.max_batch_bytes // row,
            operator.max_records_per_input,
            operator.max_batch_bytes // row,
            graph.max_work_records // copies,
            graph.max_work_bytes // (copies * row),
        )
        if fold.finalize is not None:
            cap = min(cap, operator.max_calls_per_input)
        object.__setattr__(self, "_order", topology.execution_order)
        object.__setattr__(self, "_window", window)
        object.__setattr__(self, "_prefix", frozenset(bypass))
        object.__setattr__(self, "_drain_cap", cap)
        _wire(self.to_dict(), _MAX_MANIFEST)
        # Imported lazily to keep the config/runtime and wire validators independent.
        from .window_graph_checkpoint import _header_reservation

        _header_reservation(self)

    @property
    def execution_order(self) -> tuple[str, ...]:
        return self._order

    def to_dict(self) -> dict[str, Any]:
        nodes = []
        for node in self.nodes:
            if isinstance(node, FlowWindow):
                nodes.append(
                    {
                        "step_id": node.step_id,
                        "operator": "window_fold",
                        "fold": node.fold._configuration(),
                    }
                )
            else:
                operator = (
                    node.operator
                    if isinstance(node, FlowStep)
                    else "branch"
                    if isinstance(node, FlowBranch)
                    else "merge"
                )
                nodes.append({"step_id": node.step_id, "operator": operator})
        return {
            "kind": "stream-quilt-window-graph",
            "version": "1.0",
            "flow_id": self.flow_id,
            "revision": self.revision,
            "entry": self.entry,
            "nodes": nodes,
            "edges": [
                {"source": edge.source, "target": edge.target, "route": edge.route}
                for edge in self.edges
            ],
            "limits": self.limits.to_dict(),
            "policies": dict(_POLICIES),
        }

    @property
    def identity(self) -> str:
        return _digest(_wire(self.to_dict(), _MAX_MANIFEST))


@dataclass(frozen=True, slots=True)
class WindowGraphInput:
    position: int
    timestamp: int
    record: FlowRecord

    def __post_init__(self) -> None:
        _count(self.position, "source position", 0, _MAX_COUNT)
        _tick(self.timestamp, "source timestamp")
        if type(self.record) is not FlowRecord:
            raise ValidationError("window graph input requires FlowRecord")
        if self.record.key is not None:
            _name(self.record.key, "source key")
        _checked_value(self.record._json, 8 * 1024 * 1024)


@dataclass(frozen=True, slots=True)
class WindowGraphBatch:
    outputs: tuple[GraphOutput, ...]
    operation_sequence: int
    next_position: int
    status: WindowStatus
    folded_inputs: int = 0
    late_dropped_inputs: int = 0
    gap_inputs: int = 0
    membership_updates: int = 0
    drained_windows: int = 0

    def __post_init__(self) -> None:
        if type(self.outputs) is not tuple or len(self.outputs) > 1_000_000:
            raise ValidationError("window graph outputs require a bounded exact tuple")
        for output in self.outputs:
            if type(output) is not GraphOutput:
                raise ValidationError("window graph output requires GraphOutput")
            output.__post_init__()
        if type(self.status) is not WindowStatus:
            raise ValidationError("window graph batch requires WindowStatus")
        self.status.__post_init__()
        for label in (
            "operation_sequence",
            "next_position",
            "folded_inputs",
            "late_dropped_inputs",
            "gap_inputs",
            "membership_updates",
            "drained_windows",
        ):
            _count(getattr(self, label), label, 0, _MAX_COUNT)
        if self.drained_windows > _MAX_CELLS:
            raise ValidationError("window graph drain count exceeded")

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return {
            "outputs": [output.to_dict() for output in self.outputs],
            "operation_sequence": self.operation_sequence,
            "next_position": self.next_position,
            "status": self.status.to_dict(),
            "folded_inputs": self.folded_inputs,
            "late_dropped_inputs": self.late_dropped_inputs,
            "gap_inputs": self.gap_inputs,
            "membership_updates": self.membership_updates,
            "drained_windows": self.drained_windows,
        }


@dataclass(frozen=True, slots=True)
class _State:
    ordinary: dict[tuple[str, str], str] = field(default_factory=dict)
    ordinary_bytes: int = 0
    cell_bytes: int = 0
    window: _WindowState = field(default_factory=_WindowState)
    next_position: int = 0
    operations: int = 0
    watermarks: int = 0
    drains: int = 0
    emitted: int = 0
    edge_counts: tuple[int, ...] = ()


class _Cells:
    def __init__(self, flow: WindowGraphDataflow, state: _State) -> None:
        self.limits = flow.limits
        self.count = len(state.ordinary) + len(state.window.cells)
        self.bytes = state.cell_bytes

    def _check(self) -> None:
        if self.count > self.limits.max_state_cells or self.bytes > self.limits.max_state_bytes:
            raise ValidationError("window graph aggregate retained cell capacity exceeded")

    def ordinary(self, key: tuple[str, str], previous: str | None, value: str | None) -> None:
        self.count += (value is not None) - (previous is not None)
        self.bytes += (_ordinary_size(key, value) if value is not None else 0) - (
            _ordinary_size(key, previous) if previous is not None else 0
        )
        self._check()

    def reserve(self, removed: tuple[_Cell, ...], added: int) -> None:
        self.count += added - len(removed)
        self.bytes -= sum(cell.byte_size for cell in removed)
        self._check()

    def admit(self, cell: _Cell) -> None:
        self.bytes += cell.byte_size
        self._check()


class WindowGraphRuntime:
    """Single-owner local state; callback effects and output delivery are external."""

    def __init__(self, flow: WindowGraphDataflow) -> None:
        if type(flow) is not WindowGraphDataflow:
            raise ValidationError("window graph runtime requires WindowGraphDataflow")
        flow.__post_init__()
        self._flow = flow
        self._busy = False
        self._state = _State(edge_counts=(0,) * len(flow.edges))
        self._nodes = {node.step_id: node for node in flow.nodes}
        self._incoming = {
            key: tuple(edge for edge in flow.edges if edge.target == key) for key in self._nodes
        }
        self._outgoing = {
            key: tuple(edge for edge in flow.edges if edge.source == key) for key in self._nodes
        }
        self._edge_index = {edge: index for index, edge in enumerate(flow.edges)}

    @contextmanager
    def _operation(self) -> Iterator[None]:
        if self._busy:
            raise ValidationError("window graph does not allow reentrant operations or reads")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    @property
    def status(self) -> WindowStatus:
        with self._operation():
            return _status(self._state.window)

    @property
    def next_position(self) -> int:
        with self._operation():
            return self._state.next_position

    @property
    def operation_sequence(self) -> int:
        with self._operation():
            return self._state.operations

    def _position(self, position: int) -> None:
        _count(position, "next source position", 0, _MAX_COUNT)
        if position != self._state.next_position:
            raise ValidationError("window graph source position does not match its prefix")

    def _batch(
        self, before: _State, after: _State, outputs: tuple[GraphOutput, ...] = ()
    ) -> WindowGraphBatch:
        old, new = before.window, after.window
        drops, gaps = new.late_drops - old.late_drops, new.gap_inputs - old.gap_inputs
        return WindowGraphBatch(
            outputs,
            after.operations,
            after.next_position,
            _status(new),
            new.processed_inputs - old.processed_inputs - drops - gaps,
            drops,
            gaps,
            new.membership_updates - old.membership_updates,
            new.emitted_windows - old.emitted_windows,
        )

    def process(self, item: WindowGraphInput) -> WindowGraphBatch:
        with self._operation():
            if type(item) is not WindowGraphInput:
                raise ValidationError("window graph process requires WindowGraphInput")
            item.__post_init__()
            self._position(item.position)
            before = self._state
            if _status(before.window).phase != "open":
                raise ValidationError("window graph input requires open, non-draining state")
            _count(
                before.next_position + 1, "source inputs", 0, self._flow.limits.max_source_inputs
            )
            _count(before.operations + 1, "operation sequence", 0, _MAX_COUNT)
            return self._execute(item=item)

    def advance_watermark(self, timestamp: int, *, next_position: int) -> WindowGraphBatch:
        with self._operation():
            _tick(timestamp, "watermark")
            self._position(next_position)
            before = self._state
            if _status(before.window).phase != "open":
                raise ValidationError("watermark advancement requires open state")
            if before.window.watermark is not None and timestamp < before.window.watermark:
                raise ValidationError("watermark must not regress")
            if timestamp == before.window.watermark:
                return self._batch(before, before)
            operations = _count(before.operations + 1, "operation sequence", 0, _MAX_COUNT)
            watermarks = _count(before.watermarks + 1, "watermark advances", 0, _MAX_COUNT)
            window, _ = _stage_advance(before.window, timestamp)
            after = replace(before, window=window, operations=operations, watermarks=watermarks)
            result = self._batch(before, after)
            self._state = after
            return result

    def finish(self, *, next_position: int) -> WindowGraphBatch:
        with self._operation():
            self._position(next_position)
            before = self._state
            if before.window.finished:
                return self._batch(before, before)
            operations = _count(before.operations + 1, "operation sequence", 0, _MAX_COUNT)
            window, _ = _stage_finish(before.window)
            after = replace(before, window=window, operations=operations)
            result = self._batch(before, after)
            self._state = after
            return result

    def drain(self, *, max_windows: int = 100) -> WindowGraphBatch:
        with self._operation():
            _count(max_windows, "drain max_windows", 1, _MAX_CELLS)
            before = self._state
            pending = _status(before.window).pending_windows
            if not pending:
                return self._batch(before, before)
            _count(before.operations + 1, "operation sequence", 0, _MAX_COUNT)
            _count(before.drains + 1, "nonempty drain operations", 0, _MAX_COUNT)
            return self._execute(drain_count=min(max_windows, pending, self._flow._drain_cap))

    def _execute(
        self, *, item: WindowGraphInput | None = None, drain_count: int = 0
    ) -> WindowGraphBatch:
        before, flow = self._state, self._flow
        cells = _Cells(flow, before)
        transaction = _FlowTransaction(
            flow.limits.graph.operator_limits,
            before.ordinary,
            before.ordinary_bytes,
            cells.ordinary,
        )
        work = _Work(flow.limits.graph)
        edges = list(before.edge_counts)
        window = before.window
        window_id, fold = flow._window.step_id, flow._window.fold
        outputs: list[GraphOutput] = []

        def before_call(phase: str, key: str, index: int) -> None:
            transaction.calls += 1
            if transaction.calls > transaction.limits.max_calls_per_input:
                raise ValidationError("callback invocation budget exceeded")

        def delivered(edge: FlowEdge, count: int) -> None:
            index = self._edge_index[edge]
            edges[index] = _count(edges[index] + count, "edge deliveries", 0, _MAX_COUNT)

        def extension(
            key: str, batches: tuple[tuple[FlowEdge, tuple[FlowRecord, ...]], ...]
        ) -> tuple[FlowRecord, ...]:
            nonlocal window
            if key != window_id or item is None:
                raise ValidationError("invalid window graph extension boundary")
            for _, records in batches:
                for record in records:
                    window, _ = _stage_process(
                        fold,
                        window,
                        item.timestamp,
                        record,
                        hooks=_StageHooks(before_call, cells.reserve, cells.admit),
                    )
            return ()

        if item is not None:
            work.charge(item.record)
            outputs = _walk_graph(
                flow._order,
                self._nodes,
                self._incoming,
                self._outgoing,
                {flow.entry: (item.record,)},
                transaction,
                work.charge,
                extension=extension,
                delivered=delivered,
            )
        else:
            copies, maximum = 1 + len(self._outgoing[window_id]), fold.limits.max_row_bytes
            work.reserved_records = drain_count * copies
            work.reserved_bytes = drain_count * copies * maximum
            work.ensure(0, 0)

            def on_row(row: WindowRow) -> None:
                record = row.to_record()
                work.reserved_records -= copies
                work.reserved_bytes -= copies * maximum
                current = _walk_graph(
                    flow._order,
                    self._nodes,
                    self._incoming,
                    self._outgoing,
                    {},
                    transaction,
                    work.charge,
                    seed_outputs={window_id: (record,)},
                    delivered=delivered,
                )
                _count(
                    before.emitted + len(outputs) + len(current),
                    "terminal emissions",
                    0,
                    _MAX_COUNT,
                )
                outputs.extend(current)

            window, batch = _stage_drain(
                fold,
                window,
                max_windows=drain_count,
                hooks=_StageHooks(before_call, cells.reserve, cells.admit, on_row),
            )
            if batch.drained_windows != drain_count or work.reserved_records or work.reserved_bytes:
                raise ValidationError("window graph drain reservation was not reconciled")
        ordinary, ordinary_bytes = transaction.commit()
        after = _State(
            ordinary,
            ordinary_bytes,
            cells.bytes,
            window,
            before.next_position + int(item is not None),
            before.operations + 1,
            before.watermarks,
            before.drains + int(item is None),
            before.emitted + len(outputs),
            tuple(edges),
        )
        result = self._batch(before, after, tuple(outputs))
        self._state = after
        return result

    def checkpoint(self) -> WindowGraphCheckpoint:
        from .window_graph_checkpoint import WindowGraphCheckpoint, _document

        with self._operation():
            return WindowGraphCheckpoint(_document(self._flow, self._state))

    @classmethod
    def from_checkpoint(
        cls, flow: WindowGraphDataflow, checkpoint: WindowGraphCheckpoint
    ) -> WindowGraphRuntime:
        from .window_graph_checkpoint import WindowGraphCheckpoint, _validate_document

        runtime = cls(flow)
        if type(checkpoint) is not WindowGraphCheckpoint:
            raise ValidationError("window graph restore requires WindowGraphCheckpoint")
        _, state, _ = _validate_document(checkpoint.to_dict(), flow)
        runtime._state = state
        return runtime


__all__ = [
    "FlowWindow",
    "WindowGraphBatch",
    "WindowGraphDataflow",
    "WindowGraphInput",
    "WindowGraphLimits",
    "WindowGraphRuntime",
]
