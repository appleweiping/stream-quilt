"""Distinct bounded window-graph snapshots; no callbacks or old wire migrations."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, cast

from .branching import FlowBranch, FlowEdge, FlowMerge, GraphLimits
from .dataflow import _MAX_COUNT, FlowLimits, FlowStep, Operator, _count
from .errors import ValidationError
from .window_fold import (
    _CELL_FIELDS,
    _COUNTERS,
    _Cell,
    _check_counters,
    _checked_value,
    _digest,
    _geometry,
    _hex,
    _load,
    _name,
    _read_spec,
    _shape,
    _status,
    _text,
    _tick,
    _unavailable,
)
from .window_fold import (
    _body as _window_body,
)
from .window_fold import (
    _State as _WindowState,
)
from .window_fold import (
    _validate_document as _window_document,
)
from .window_graph import (
    _MAX_MANIFEST,
    _POLICIES,
    FlowWindow,
    WindowGraphDataflow,
    WindowGraphLimits,
    WindowGraphNode,
    _ordinary_size,
    _ordinary_wire,
    _State,
    _wire,
)

_MAX_WIRE = 66 * 1024 * 1024
_MAX_HEADER = 1024 * 1024
_BODY_FIELDS = {
    "kind",
    "version",
    "configuration",
    "identity",
    "next_position",
    "source_finished",
    "operation_sequence",
    "counters",
    "edge_counts",
    "ordinary_cells",
    "window",
}
_WINDOW_FIELDS = {
    "kind",
    "version",
    "configuration",
    "identity",
    "watermark",
    "finished",
    "phase",
    "counters",
    "cells",
}
_STATEFUL = frozenset({"stateful_map", "stateful_flat_map"})
_OPERATORS = frozenset({"map", "filter", "flat_map", "key_by", "drop_key", *_STATEFUL})


def _configuration(document: Any) -> WindowGraphDataflow:
    _shape(
        document,
        {"kind", "version", "flow_id", "revision", "entry", "nodes", "edges", "limits", "policies"},
    )
    if (
        type(document["kind"]) is not str
        or document["kind"] != "stream-quilt-window-graph"
        or type(document["version"]) is not str
        or document["version"] != "1.0"
    ):
        raise ValidationError("unsupported window graph configuration kind/version")
    _shape(document["policies"], set(_POLICIES))
    if (
        any(type(value) is not str for value in document["policies"].values())
        or document["policies"] != _POLICIES
    ):
        raise ValidationError("unsupported window graph policies")
    limits = document["limits"]
    _shape(limits, {"graph", "max_state_cells", "max_state_bytes", "max_source_inputs"})
    graph = limits["graph"]
    _shape(graph, {"operator_limits", "max_work_records", "max_work_bytes"})
    _shape(graph["operator_limits"], set(FlowLimits.__dataclass_fields__))
    active = WindowGraphLimits(
        GraphLimits(
            FlowLimits(**graph["operator_limits"]),
            graph["max_work_records"],
            graph["max_work_bytes"],
        ),
        limits["max_state_cells"],
        limits["max_state_bytes"],
        limits["max_source_inputs"],
    )
    raw_nodes, raw_edges = document["nodes"], document["edges"]
    if type(raw_nodes) is not list or not 2 <= len(raw_nodes) <= 64:
        raise ValidationError("window graph configuration requires bounded node list")
    if type(raw_edges) is not list or not 1 <= len(raw_edges) <= 256:
        raise ValidationError("window graph configuration requires bounded edge list")
    nodes: list[WindowGraphNode] = []
    for raw in raw_nodes:
        if type(raw) is not dict or type(raw.get("operator")) is not str:
            raise ValidationError("invalid window graph node document")
        operator = raw["operator"]
        _shape(
            raw,
            {"step_id", "operator", "fold"}
            if operator == "window_fold"
            else {"step_id", "operator"},
        )
        if operator == "window_fold":
            nodes.append(FlowWindow(raw["step_id"], _read_spec(raw["fold"])))
        elif operator == "branch":
            nodes.append(FlowBranch(raw["step_id"], _unavailable))
        elif operator == "merge":
            nodes.append(FlowMerge(raw["step_id"]))
        elif operator in _OPERATORS:
            nodes.append(
                FlowStep(
                    raw["step_id"],
                    cast(Operator, operator),
                    None if operator == "drop_key" else _unavailable,
                    _unavailable if operator in _STATEFUL else None,
                )
            )
        else:
            raise ValidationError("unsupported window graph operator")
    edges = []
    for raw in raw_edges:
        _shape(raw, {"source", "target", "route"})
        edges.append(FlowEdge(raw["source"], raw["target"], raw["route"]))
    _wire(document, _MAX_MANIFEST)
    flow = WindowGraphDataflow(
        document["flow_id"],
        document["revision"],
        tuple(nodes),
        tuple(edges),
        entry=document["entry"],
        limits=active,
    )
    if flow.to_dict() != document:
        raise ValidationError("window graph configuration must be normalized")
    return flow


def _body(flow: WindowGraphDataflow, state: _State, window: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "stream-quilt-window-graph-checkpoint",
        "version": "1.0",
        "configuration": flow.to_dict(),
        "identity": flow.identity,
        "next_position": state.next_position,
        "source_finished": state.window.finished,
        "operation_sequence": state.operations,
        "counters": {
            "watermark_advances": state.watermarks,
            "drain_operations": state.drains,
            "emitted_records": state.emitted,
        },
        "edge_counts": list(state.edge_counts),
        "ordinary_cells": [
            _ordinary_wire(key, state.ordinary[key]) for key in sorted(state.ordinary)
        ],
        "window": window,
    }


def _document(flow: WindowGraphDataflow, state: _State) -> dict[str, Any]:
    window = _window_body(flow._window.fold, state.window)
    body = _body(flow, state, {"body": window, "sha256": _digest(_wire(window))})
    return {"body": body, "sha256": _digest(_wire(body))}


def _header_reservation(flow: WindowGraphDataflow) -> int:
    """A token-width reservation, intentionally not a reachable checkpoint."""
    window = _window_body(flow._window.fold, _WindowState())
    window.update(
        watermark=-_MAX_COUNT,
        finished=False,
        phase="draining",
        counters=dict.fromkeys(_COUNTERS, _MAX_COUNT),
    )
    state = _State(
        next_position=_MAX_COUNT,
        operations=_MAX_COUNT,
        watermarks=_MAX_COUNT,
        drains=_MAX_COUNT,
        emitted=_MAX_COUNT,
        edge_counts=(_MAX_COUNT,) * len(flow.edges),
    )
    body = _body(flow, state, {"body": window, "sha256": "0" * 64})
    return len(_wire({"body": body, "sha256": "0" * 64}, _MAX_HEADER).encode("utf-8"))


def _counts(flow: WindowGraphDataflow, state: _State) -> None:
    window = state.window
    i, w, d, o, e = (
        state.next_position,
        state.watermarks,
        state.drains,
        state.emitted,
        window.emitted_windows,
    )
    if state.operations != i + w + int(window.finished) + d:
        raise ValidationError("window graph operation counters contradict their causes")
    if (window.watermark is None) != (w == 0):
        raise ValidationError("window graph watermark count contradicts its frontier")
    if i > flow.limits.max_source_inputs:
        raise ValidationError("window graph source input capacity exceeded")
    if (d == 0) != (e == 0) or not d <= e <= d * flow._drain_cap:
        raise ValidationError("window graph drain counts are inconsistent")
    if o > d * flow.limits.graph.max_work_records:
        raise ValidationError("window graph terminal output count is inconsistent")
    incoming: dict[str, int] = {node.step_id: 0 for node in flow.nodes}
    incoming[flow.entry] = i
    outgoing: dict[str, list[tuple[FlowEdge, int]]] = {node.step_id: [] for node in flow.nodes}
    for edge, count in zip(flow.edges, state.edge_counts, strict=True):
        incoming[edge.target] += count
        outgoing[edge.source].append((edge, count))
        cause = i if edge.source in flow._prefix else d
        if count > cause * flow.limits.graph.max_work_records:
            raise ValidationError("window graph edge deliveries exceed their operation causes")
    if incoming[flow._window.step_id] != window.processed_inputs:
        raise ValidationError("window input count differs from its incoming edge")
    lower = upper = 0
    for node in flow.nodes:
        amount, sent = incoming[node.step_id], outgoing[node.step_id]
        if isinstance(node, FlowWindow):
            low = high = e
        elif isinstance(node, FlowBranch):
            routes: dict[bool, int] = {}
            for edge, count in sent:
                route = cast(bool, edge.route)
                if route in routes and routes[route] != count:
                    raise ValidationError("branch fanout delivery counts disagree")
                routes[route] = count
            if sum(routes.values()) != amount:
                raise ValidationError("branch delivery counts contradict input")
            continue
        elif isinstance(node, FlowMerge) or node.operator in ("map", "key_by", "drop_key"):
            low = high = amount
        else:
            low = 0
            high = amount * (
                flow.limits.graph.operator_limits.max_records_per_input
                if node.operator in ("flat_map", "stateful_flat_map")
                else 1
            )
        if sent:
            if any(count != sent[0][1] or not low <= count <= high for _, count in sent):
                raise ValidationError("node fanout delivery counts contradict its operator")
        else:
            lower += low
            upper += high
    if not lower <= o <= upper:
        raise ValidationError("terminal output count contradicts its terminal nodes")
    retained: dict[str, int] = {}
    for step, _ in state.ordinary:
        retained[step] = retained.get(step, 0) + 1
    if any(count > incoming[step] for step, count in retained.items()):
        raise ValidationError("ordinary retained keys exceed node input history")


def _validate_document(
    document: Any, supplied: WindowGraphDataflow | None = None
) -> tuple[WindowGraphDataflow, _State, str]:
    _shape(document, {"body", "sha256"})
    _hex(document["sha256"])
    body = document["body"]
    _shape(body, _BODY_FIELDS)
    if (
        type(body["kind"]) is not str
        or body["kind"] != "stream-quilt-window-graph-checkpoint"
        or type(body["version"]) is not str
        or body["version"] != "1.0"
    ):
        raise ValidationError("unsupported window graph checkpoint kind/version")
    flow = _configuration(body["configuration"])
    _hex(body["identity"])
    if body["identity"] != flow.identity:
        raise ValidationError("window graph configuration digest mismatch")
    if supplied is not None:
        if type(supplied) is not WindowGraphDataflow:
            raise ValidationError("window graph restore requires its exact configuration")
        supplied.__post_init__()
        if supplied.identity != flow.identity:
            raise ValidationError("window graph configuration/revision identity mismatch")
        flow = supplied
    for label in ("next_position", "operation_sequence"):
        _count(body[label], label, 0, _MAX_COUNT)
    if type(body["source_finished"]) is not bool:
        raise ValidationError("window graph source EOF requires boolean")
    counters = body["counters"]
    _shape(counters, {"watermark_advances", "drain_operations", "emitted_records"})
    for label, value in counters.items():
        _count(value, label, 0, _MAX_COUNT)
    edges = body["edge_counts"]
    if type(edges) is not list or len(edges) != len(flow.edges):
        raise ValidationError("window graph requires one count per declared edge")
    for count in edges:
        _count(count, "edge count", 0, _MAX_COUNT)
    nested = body["window"]
    _shape(nested, {"body", "sha256"})
    _hex(nested["sha256"])
    raw_window = nested["body"]
    _shape(raw_window, _WINDOW_FIELDS)
    fold = flow._window.fold
    if (
        type(raw_window["kind"]) is not str
        or raw_window["kind"] != "stream-quilt-window-checkpoint"
        or type(raw_window["version"]) is not str
        or raw_window["version"] != "1.0"
    ):
        raise ValidationError("unsupported nested window checkpoint")
    if _read_spec(raw_window["configuration"])._configuration() != fold._configuration():
        raise ValidationError("nested window configuration differs from its graph node")
    _hex(raw_window["identity"])
    if raw_window["identity"] != fold.identity:
        raise ValidationError("nested window configuration identity mismatch")
    if (
        type(raw_window["finished"]) is not bool
        or raw_window["finished"] != body["source_finished"]
    ):
        raise ValidationError("nested window and source EOF disagree")
    if raw_window["watermark"] is not None:
        _tick(raw_window["watermark"], "window watermark")
    _shape(raw_window["counters"], set(_COUNTERS))
    for label, value in raw_window["counters"].items():
        _count(value, label, 0, _MAX_COUNT)
    raw_ordinary, raw_cells = body["ordinary_cells"], raw_window["cells"]
    operator = flow.limits.graph.operator_limits
    if (
        type(raw_ordinary) is not list
        or len(raw_ordinary) > operator.max_state_keys
        or type(raw_cells) is not list
        or len(raw_cells) > fold.limits.max_windows
        or len(raw_cells) + len(raw_ordinary) > flow.limits.max_state_cells
    ):
        raise ValidationError("window graph requires bounded aggregate cell arrays")
    stateful = {
        node.step_id
        for node in flow.nodes
        if isinstance(node, FlowStep) and node.operator in _STATEFUL
    }
    ordinary: dict[tuple[str, str], str] = {}
    previous: tuple[str, str] | None = None
    wire_size = payload_size = 0
    for raw in raw_ordinary:
        _shape(raw, {"step_id", "key", "state"})
        key = (_name(raw["step_id"], "ordinary node"), _name(raw["key"], "ordinary key"))
        if key[0] not in stateful or (previous is not None and key <= previous):
            raise ValidationError("ordinary cells require sorted unique stateful identities")
        previous = key
        encoded = _text(raw["state"], operator.max_state_value_bytes)
        payload_size += len(encoded.encode("utf-8"))
        wire_size += _ordinary_size(key, encoded)
        if payload_size > operator.max_state_bytes or wire_size > flow.limits.max_state_bytes:
            raise ValidationError("ordinary or aggregate state byte capacity exceeded")
        ordinary[key] = encoded
    cells: dict[tuple[int, str], _Cell] = {}
    previous_window: tuple[int, str] | None = None
    window_size = 0
    keys = set()
    for raw in raw_cells:
        _shape(raw, _CELL_FIELDS)
        window_key = _name(raw["key"], "window key")
        start, end = _geometry(fold, raw["index"])
        _tick(raw["start"], "window start")
        _tick(raw["end"], "window end")
        if (raw["start"], raw["end"]) != (start, end):
            raise ValidationError("window cell redundant geometry mismatch")
        identity = (raw["index"], window_key)
        if previous_window is not None and identity <= previous_window:
            raise ValidationError("window cells require sorted unique identities")
        previous_window = identity
        count = _count(raw["input_count"], "window input count", 1, _MAX_COUNT)
        encoded = _text(raw["state"], fold.limits.max_state_value_bytes)
        size = len(_wire(raw, flow.limits.max_state_bytes).encode("utf-8"))
        window_size += size
        wire_size += size
        keys.add(window_key)
        if (
            window_size > fold.limits.max_state_bytes
            or wire_size > flow.limits.max_state_bytes
            or len(keys) > fold.limits.max_keys
        ):
            raise ValidationError("window or aggregate cell capacity exceeded")
        cells[identity] = _Cell(window_key, identity[0], start, end, count, encoded, size)
    window = _WindowState(
        cells=cells,
        watermark=raw_window["watermark"],
        finished=raw_window["finished"],
        byte_size=window_size,
        **raw_window["counters"],
    )
    _check_counters(fold, window)
    if type(raw_window["phase"]) is not str or raw_window["phase"] != _status(window).phase:
        raise ValidationError("window graph phase contradicts pending state")
    state = _State(
        ordinary,
        payload_size,
        wire_size,
        window,
        body["next_position"],
        body["operation_sequence"],
        counters["watermark_advances"],
        counters["drain_operations"],
        counters["emitted_records"],
        tuple(edges),
    )
    _counts(flow, state)
    encoded_body = _wire(body, _MAX_WIRE)
    if document["sha256"] != _digest(encoded_body) or nested["sha256"] != _digest(
        _wire(raw_window)
    ):
        raise ValidationError("window graph checkpoint checksum mismatch")
    encoded_document = _wire(document, _MAX_WIRE)
    empty_window = {**raw_window, "cells": []}
    empty_body = {**body, "ordinary_cells": [], "window": {**nested, "body": empty_window}}
    header_size = len(_wire({**document, "body": empty_body}).encode("utf-8"))
    expected_size = header_size + wire_size + max(0, len(ordinary) - 1) + max(0, len(cells) - 1)
    if (
        header_size > _header_reservation(flow)
        or len(encoded_document.encode("utf-8")) != expected_size
    ):
        raise ValidationError("window graph checkpoint wire accounting mismatch")
    # Every known metadata/global quota/counter failure precedes nested state decoding.
    _, checked_window, _ = _window_document(nested, fold)
    for encoded in ordinary.values():
        _checked_value(encoded, operator.max_state_value_bytes)
    return flow, replace(state, window=checked_window), encoded_document


@dataclass(frozen=True, slots=True, init=False)
class WindowGraphCheckpoint:
    """Owned canonical JSON; checksum integrity is not source/history authentication."""

    _json: str = field(repr=False)

    def __init__(self, document: Any) -> None:
        _, _, encoded = _validate_document(document)
        object.__setattr__(self, "_json", encoded)

    def to_dict(self) -> dict[str, Any]:
        text = _text(self._json, _MAX_WIRE)
        document = _load(text)
        _, _, encoded = _validate_document(document)
        if encoded != text:
            raise ValidationError("window graph checkpoint must be canonical")
        return cast(dict[str, Any], document)

    def to_json(self) -> str:
        self.to_dict()
        return self._json

    @classmethod
    def from_dict(cls, document: Any) -> WindowGraphCheckpoint:
        return cls(document)

    @classmethod
    def from_json(cls, payload: str | bytes) -> WindowGraphCheckpoint:
        if type(payload) not in (str, bytes) or len(payload) > _MAX_WIRE:
            raise ValidationError("window graph checkpoint input exceeds its byte bound")
        try:
            text = payload.decode("utf-8") if type(payload) is bytes else payload
        except UnicodeError as error:
            raise ValidationError("window graph checkpoint must be UTF-8") from error
        text = _text(text, _MAX_WIRE)
        result = cls(_load(text))
        if result._json != text:
            raise ValidationError("window graph checkpoint must be canonical")
        return result


__all__ = ["WindowGraphCheckpoint"]
