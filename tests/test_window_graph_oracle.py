"""Independent raw-arrival/explicit-DAG interpreter with every-boundary restore."""

import hashlib
import json
import random
from copy import deepcopy
from dataclasses import replace

import pytest

from stream_quilt import (
    FlowEdge,
    FlowExecutionError,
    FlowMerge,
    FlowRecord,
    FlowStep,
    FlowWindow,
    StateFlatUpdate,
    StateUpdate,
    WindowFold,
    WindowFoldLimits,
    WindowGraphCheckpoint,
    WindowGraphDataflow,
    WindowGraphInput,
    WindowGraphRuntime,
)


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":")
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def graph_for(width, hop, origin, policy, reverse=False):
    fold = WindowFold(
        "window",
        "v1",
        width=width,
        hop=hop,
        origin=origin,
        initial=list,
        fold=lambda state, value: [*state, value],
        late_policy=policy,
        limits=replace(WindowFoldLimits(), max_row_bytes=512, max_batch_bytes=2048),
    )
    nodes = (
        FlowStep("input", "filter", lambda value: not value["skip"]),
        FlowStep("key", "key_by", lambda value: value["account"]),
        FlowStep(
            "prefix",
            "stateful_flat_map",
            lambda value, state: StateFlatUpdate(
                state + 1, [value["amount"]] * (1 + value["repeat"])
            ),
            lambda: 0,
        ),
        FlowWindow("window", fold),
        FlowStep("left", "map", lambda value: {**value, "side": "L"}),
        FlowStep("right", "map", lambda value: {**value, "side": "R"}),
        FlowMerge("merge"),
        FlowStep(
            "history",
            "stateful_map",
            lambda value, state: StateUpdate([*state, value], [*state, value]),
            list,
        ),
        FlowStep("summary", "map", lambda value: sum(sum(row["value"]) for row in value)),
        FlowStep("detail", "map", lambda value: value),
    )
    pair = [FlowEdge("left", "merge"), FlowEdge("right", "merge")]
    if reverse:
        pair.reverse()
    edges = (
        FlowEdge("input", "key"),
        FlowEdge("key", "prefix"),
        FlowEdge("prefix", "window"),
        FlowEdge("window", "left"),
        FlowEdge("window", "right"),
        *pair,
        FlowEdge("merge", "history"),
        FlowEdge("history", "summary"),
        FlowEdge("history", "detail"),
    )
    return WindowGraphDataflow("oracle", "v1", nodes, edges, entry="input")


class Interpreter:
    """No production scheduler, range formula, staged transition or checkpoint helper."""

    def __init__(self, width, hop, origin, policy, reverse=False):
        self.width, self.hop, self.origin, self.policy, self.reverse = (
            width,
            hop,
            origin,
            policy,
            reverse,
        )
        self.position = self.operations = self.advances = self.drains = self.emitted = 0
        self.watermark, self.finished = None, False
        self.prefix, self.history, self.arrivals = {}, {}, {}
        self.edges = [0] * 10
        self.p = self.late = self.gaps = self.memberships = self.created = self.closed = (
            self.finalized
        ) = 0

    def bounds(self, index):
        start = self.origin + index * self.hop
        return start, start + self.width

    def pending(self):
        return sorted(
            key
            for key in self.arrivals
            if self.finished
            or (self.watermark is not None and self.bounds(key[0])[1] <= self.watermark)
        )

    def status(self):
        count = len(self.pending())
        return {
            "phase": "draining" if count else "closed" if self.finished else "open",
            "watermark": self.watermark,
            "finished": self.finished,
            "retained_windows": len(self.arrivals),
            "pending_windows": count,
        }

    def batch(self, outputs=None, folded=0, drops=0, gaps=0, memberships=0, drained=0):
        return {
            "outputs": outputs or [],
            "operation_sequence": self.operations,
            "next_position": self.position,
            "status": self.status(),
            "folded_inputs": folded,
            "late_dropped_inputs": drops,
            "gap_inputs": gaps,
            "membership_updates": memberships,
            "drained_windows": drained,
        }

    def process(self, timestamp, value):
        if value["skip"]:
            self.position += 1
            self.operations += 1
            return self.batch()
        late = self.watermark is not None and timestamp < self.watermark
        if late and self.policy == "reject":
            return None
        self.position += 1
        self.operations += 1
        key = value["account"]
        self.prefix[key] = self.prefix.get(key, 0) + 1
        expanded = [value["amount"]] * (1 + value["repeat"])
        self.edges[0] += 1
        self.edges[1] += 1
        self.edges[2] += len(expanded)
        self.p += len(expanded)
        if late:
            self.late += len(expanded)
            return self.batch(drops=len(expanded))
        members = [
            index
            for index in range(-100, 101)
            if self.bounds(index)[0] <= timestamp < self.bounds(index)[1]
        ]
        if not members:
            self.gaps += len(expanded)
            return self.batch(gaps=len(expanded))
        for value in expanded:
            for index in members:
                identity = index, key
                if identity not in self.arrivals:
                    self.arrivals[identity] = []
                    self.created += 1
                self.arrivals[identity].append(value)
        updates = len(members) * len(expanded)
        self.memberships += updates
        return self.batch(folded=len(expanded), memberships=updates)

    def advance(self, timestamp):
        if timestamp != self.watermark:
            self.watermark = timestamp
            self.advances += 1
            self.operations += 1
        return self.batch()

    def finish(self):
        if not self.finished:
            self.finished = True
            self.operations += 1
        return self.batch()

    def drain(self, requested):
        selected = self.pending()[: min(requested, 4)]
        if not selected:
            return self.batch()
        outputs = []
        for index, key in selected:
            values = self.arrivals.pop((index, key))
            start, end = self.bounds(index)
            row = {
                "index": index,
                "start": start,
                "end": end,
                "tick_unit": "tick",
                "input_count": len(values),
                "value": list(values),
            }
            branch_values = [{**row, "side": side} for side in ("RL" if self.reverse else "LR")]
            histories = []
            for value in branch_values:
                self.history[key] = [*self.history.get(key, []), deepcopy(value)]
                histories.append(deepcopy(self.history[key]))
            # Existing single-record DAG terminal order, repeated for EACH window.
            outputs.extend(
                {
                    "step_id": "summary",
                    "key": key,
                    "value": sum(sum(item["value"]) for item in history),
                }
                for history in histories
            )
            outputs.extend(
                {"step_id": "detail", "key": key, "value": history} for history in histories
            )
            for slot in (3, 4, 5, 6):
                self.edges[slot] += 1
            for slot in (7, 8, 9):
                self.edges[slot] += 2
            self.closed += 1
            self.finalized += len(values)
        self.drains += 1
        self.operations += 1
        self.emitted += len(outputs)
        return self.batch(outputs, drained=len(selected))

    def fold_configuration(self):
        return {
            "fold_id": "window",
            "revision": "v1",
            "width": self.width,
            "hop": self.hop,
            "origin": self.origin,
            "tick_unit": "tick",
            "order": "arrival",
            "late_policy": self.policy,
            "finalizer": False,
            "limits": {
                "max_windows_per_input": 64,
                "max_keys": 10_000,
                "max_windows": 10_000,
                "max_input_bytes": 1048576,
                "max_state_value_bytes": 1048576,
                "max_state_bytes": 16777216,
                "max_row_bytes": 512,
                "max_rows_per_batch": 1000,
                "max_batch_bytes": 2048,
                "max_inputs": 1000000000,
            },
        }

    def configuration(self):
        operators = [
            "filter",
            "key_by",
            "stateful_flat_map",
            "window_fold",
            "map",
            "map",
            "merge",
            "stateful_map",
            "map",
            "map",
        ]
        names = [
            "input",
            "key",
            "prefix",
            "window",
            "left",
            "right",
            "merge",
            "history",
            "summary",
            "detail",
        ]
        nodes = [
            {"step_id": name, "operator": operator}
            for name, operator in zip(names, operators, strict=True)
        ]
        nodes[3]["fold"] = self.fold_configuration()
        pair = [("left", "merge"), ("right", "merge")]
        if self.reverse:
            pair.reverse()
        edges = [
            ("input", "key"),
            ("key", "prefix"),
            ("prefix", "window"),
            ("window", "left"),
            ("window", "right"),
            *pair,
            ("merge", "history"),
            ("history", "summary"),
            ("history", "detail"),
        ]
        return {
            "kind": "stream-quilt-window-graph",
            "version": "1.0",
            "flow_id": "oracle",
            "revision": "v1",
            "entry": "input",
            "nodes": nodes,
            "edges": [{"source": a, "target": b, "route": None} for a, b in edges],
            "limits": {
                "graph": {
                    "max_work_records": 10_000,
                    "max_work_bytes": 67108864,
                    "operator_limits": {
                        "max_records_per_input": 1000,
                        "max_calls_per_input": 10000,
                        "max_record_bytes": 1048576,
                        "max_batch_bytes": 16777216,
                        "max_state_value_bytes": 1048576,
                        "max_state_keys": 10000,
                        "max_state_bytes": 67108864,
                    },
                },
                "max_state_cells": 10000,
                "max_state_bytes": 16777216,
                "max_source_inputs": 1000000000,
            },
            "policies": {
                "order": "arrival",
                "timestamp": "source-preserved",
                "drain_order": "window-major",
                "callback_budget": "operation",
            },
        }

    def checkpoint(self):
        ordinary = []
        for step, mapping in (("history", self.history), ("prefix", self.prefix)):
            ordinary.extend(
                {"step_id": step, "key": key, "state": canonical(value)}
                for key, value in sorted(mapping.items())
            )
        cells = []
        for (index, key), values in sorted(self.arrivals.items()):
            start, end = self.bounds(index)
            cells.append(
                {
                    "index": index,
                    "key": key,
                    "start": start,
                    "end": end,
                    "input_count": len(values),
                    "state": canonical(values),
                }
            )
        fold_config = self.fold_configuration()
        window = {
            "kind": "stream-quilt-window-checkpoint",
            "version": "1.0",
            "configuration": fold_config,
            "identity": digest(fold_config),
            "watermark": self.watermark,
            "finished": self.finished,
            "phase": self.status()["phase"],
            "counters": {
                "processed_inputs": self.p,
                "late_drops": self.late,
                "gap_inputs": self.gaps,
                "membership_updates": self.memberships,
                "created_windows": self.created,
                "emitted_windows": self.closed,
                "finalized_memberships": self.finalized,
            },
            "cells": cells,
        }
        config = self.configuration()
        body = {
            "kind": "stream-quilt-window-graph-checkpoint",
            "version": "1.0",
            "configuration": config,
            "identity": digest(config),
            "next_position": self.position,
            "source_finished": self.finished,
            "operation_sequence": self.operations,
            "counters": {
                "watermark_advances": self.advances,
                "drain_operations": self.drains,
                "emitted_records": self.emitted,
            },
            "edge_counts": self.edges.copy(),
            "ordinary_cells": ordinary,
            "window": {"body": window, "sha256": digest(window)},
        }
        return {"body": body, "sha256": digest(body)}


def restored(runtime, flow, oracle):
    point = runtime.checkpoint()
    assert point.to_dict() == oracle.checkpoint()
    assert point.to_json() == canonical(oracle.checkpoint())
    expected_bytes = sum(
        len(canonical(cell).encode()) for cell in oracle.checkpoint()["body"]["ordinary_cells"]
    )
    expected_bytes += sum(
        len(canonical(cell).encode())
        for cell in oracle.checkpoint()["body"]["window"]["body"]["cells"]
    )
    assert runtime._state.cell_bytes == expected_bytes
    result = WindowGraphRuntime.from_checkpoint(
        flow, WindowGraphCheckpoint.from_json(point.to_json())
    )
    assert result.status.to_dict() == oracle.status()
    return result


@pytest.mark.parametrize("geometry", [(5, 5, 0), (7, 3, -2), (2, 5, 1)])
@pytest.mark.parametrize("policy", ["reject", "drop"])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("seed", [3, 29])
def test_independent_full_graph_interpreter_every_operation_restore(
    geometry, policy, reverse, seed
):
    flow, oracle = graph_for(*geometry, policy, reverse), Interpreter(*geometry, policy, reverse)
    runtime = restored(WindowGraphRuntime(flow), flow, oracle)
    rng = random.Random(seed)
    for index in range(18):
        timestamp = rng.randrange(-12, 18)
        value = {
            "account": rng.choice(["a", "z", "é"]),
            "amount": rng.randrange(-5, 8),
            "skip": index % 7 == 0,
            "repeat": index % 3 == 0,
        }
        expected = oracle.process(timestamp, value)
        before = runtime.checkpoint()
        item = WindowGraphInput(runtime.next_position, timestamp, FlowRecord(value))
        if expected is None:
            with pytest.raises(FlowExecutionError):
                runtime.process(item)
            assert runtime.checkpoint() == before
        else:
            assert runtime.process(item).to_dict() == expected
        runtime = restored(runtime, flow, oracle)
        if index in (5, 11, 17):
            timestamp = [-5, 3, 11][(index - 5) // 6]
            expected = oracle.advance(timestamp)
            assert (
                runtime.advance_watermark(timestamp, next_position=runtime.next_position).to_dict()
                == expected
            )
            runtime = restored(runtime, flow, oracle)
            while oracle.pending():
                count = rng.randrange(1, 6)
                expected = oracle.drain(count)
                assert runtime.drain(max_windows=count).to_dict() == expected
                runtime = restored(runtime, flow, oracle)
    expected = oracle.finish()
    assert runtime.finish(next_position=runtime.next_position).to_dict() == expected
    runtime = restored(runtime, flow, oracle)
    while oracle.pending():
        expected = oracle.drain(2)
        assert runtime.drain(max_windows=2).to_dict() == expected
        runtime = restored(runtime, flow, oracle)
    assert runtime.drain().to_dict() == oracle.drain(100)
    assert runtime.finish(next_position=runtime.next_position).to_dict() == oracle.finish()


@pytest.mark.parametrize("reverse", [False, True])
def test_diamond_suffix_is_window_major_and_drain_partition_invariant(reverse):
    all_outputs, all_values = [], []
    for count in (1, 2, 4, 100):
        flow, oracle = (
            graph_for(5, 5, 0, "reject", reverse),
            Interpreter(5, 5, 0, "reject", reverse),
        )
        runtime = WindowGraphRuntime(flow)
        for timestamp, amount in [(1, 1), (11, 2), (6, 3), (1, 4)]:
            value = {"account": "a", "amount": amount, "skip": False, "repeat": False}
            assert runtime.process(
                WindowGraphInput(runtime.next_position, timestamp, FlowRecord(value))
            ).to_dict() == oracle.process(timestamp, value)
        assert runtime.finish(next_position=4).to_dict() == oracle.finish()
        outputs = []
        while oracle.pending():
            expected = oracle.drain(count)
            actual = runtime.drain(max_windows=count).to_dict()
            assert actual == expected
            outputs.extend(actual["outputs"])
        body = runtime.checkpoint().to_dict()["body"]
        all_outputs.append(outputs)
        all_values.append(body["ordinary_cells"])
    assert all(value == all_outputs[0] for value in all_outputs)
    assert all(value == all_values[0] for value in all_values)
    assert [row["step_id"] for row in all_outputs[0]] == [
        "summary",
        "summary",
        "detail",
        "detail",
    ] * 3
