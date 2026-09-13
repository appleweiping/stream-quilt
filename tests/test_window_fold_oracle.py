"""Independent finite-domain enumeration/list oracle, not a copied range formula."""

import hashlib
import json
import random
from dataclasses import replace

import pytest

from stream_quilt import (
    FlowRecord,
    ValidationError,
    WindowCheckpoint,
    WindowFold,
    WindowFoldLimits,
    WindowFoldRuntime,
)


def canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def accumulator(values):
    return {"sum": sum(values), "sequence": list(values)}


class EnumerationOracle:
    """Retain raw arrivals; derive fold state anew rather than updating accumulators."""

    def __init__(self, spec):
        self.spec = spec
        self.windows = {}
        self.watermark = None
        self.finished = False
        self.inputs = self.late = self.gaps = self.updates = 0
        self.created = self.emitted = self.finalized = 0

    def bounds(self, index):
        start = self.spec.origin + index * self.spec.hop
        return start, start + self.spec.width

    def pending(self):
        return sorted(
            identity
            for identity in self.windows
            if self.finished
            or (self.watermark is not None and self.bounds(identity[0])[1] <= self.watermark)
        )

    def status(self):
        pending = len(self.pending())
        return {
            "phase": "draining" if pending else "closed" if self.finished else "open",
            "watermark": self.watermark,
            "finished": self.finished,
            "retained_windows": len(self.windows),
            "pending_windows": pending,
        }

    def process(self, timestamp, key, value):
        if self.watermark is not None and timestamp < self.watermark:
            if self.spec.late_policy == "reject":
                return "rejected", 0
            self.inputs += 1
            self.late += 1
            return "late_dropped", 0
        # This fixed domain strictly contains every possible window in the seeded
        # fixtures. No division/floor/range-bound helper from production is used.
        members = []
        for index in range(-100, 101):
            start, end = self.bounds(index)
            if start <= timestamp < end:
                members.append(index)
        self.inputs += 1
        if not members:
            self.gaps += 1
            return "gap", 0
        self.updates += len(members)
        for index in members:
            identity = index, key
            if identity not in self.windows:
                self.created += 1
                self.windows[identity] = []
            self.windows[identity].append(value)
        return "folded", len(members)

    def advance(self, watermark):
        self.watermark = watermark

    def finish(self):
        self.finished = True

    def drain(self, requested):
        allowance = self.spec.limits.max_batch_bytes // self.spec.limits.max_row_bytes
        selected = self.pending()[: min(requested, allowance, self.spec.limits.max_rows_per_batch)]
        rows = []
        for index, key in selected:
            values = self.windows.pop((index, key))
            start, end = self.bounds(index)
            rows.append(
                {
                    "key": key,
                    "index": index,
                    "start": start,
                    "end": end,
                    "tick_unit": self.spec.tick_unit,
                    "input_count": len(values),
                    "value": accumulator(values),
                }
            )
            self.emitted += 1
            self.finalized += len(values)
        return rows

    def checkpoint(self):
        spec = self.spec
        configuration = {
            "fold_id": spec.fold_id,
            "revision": spec.revision,
            "width": spec.width,
            "hop": spec.hop,
            "origin": spec.origin,
            "tick_unit": spec.tick_unit,
            "order": "arrival",
            "late_policy": spec.late_policy,
            "finalizer": False,
            "limits": spec.limits.to_dict(),
        }
        cells = []
        for (index, key), values in sorted(self.windows.items()):
            start, end = self.bounds(index)
            cells.append(
                {
                    "key": key,
                    "index": index,
                    "start": start,
                    "end": end,
                    "input_count": len(values),
                    "state": canonical(accumulator(values)),
                }
            )
        body = {
            "kind": "stream-quilt-window-checkpoint",
            "version": "1.0",
            "configuration": configuration,
            "identity": digest(configuration),
            "watermark": self.watermark,
            "finished": self.finished,
            "phase": self.status()["phase"],
            "counters": {
                "processed_inputs": self.inputs,
                "late_drops": self.late,
                "gap_inputs": self.gaps,
                "membership_updates": self.updates,
                "created_windows": self.created,
                "emitted_windows": self.emitted,
                "finalized_memberships": self.finalized,
            },
            "cells": cells,
        }
        return {"body": body, "sha256": digest(body)}


def restored(runtime, oracle):
    checkpoint = runtime.checkpoint()
    assert checkpoint.to_dict() == oracle.checkpoint()
    assert checkpoint.to_json() == canonical(oracle.checkpoint())
    assert runtime.status.to_dict() == oracle.status()
    result = WindowFoldRuntime.from_checkpoint(
        oracle.spec, WindowCheckpoint.from_json(checkpoint.to_json().encode())
    )
    assert result.checkpoint() == checkpoint
    return result


@pytest.mark.parametrize("width,hop", [(1, 1), (5, 5), (7, 3), (2, 5)])
@pytest.mark.parametrize("origin", [-3, 0, 4])
@pytest.mark.parametrize("policy", ["reject", "drop"])
@pytest.mark.parametrize("seed", [0, 17])
def test_seeded_enumeration_every_operation_restore(width, hop, origin, policy, seed):
    spec = WindowFold(
        "oracle",
        "arrival-list-v1",
        width=width,
        hop=hop,
        origin=origin,
        initial=lambda: accumulator([]),
        fold=lambda state, value: {
            "sum": state["sum"] + value,
            "sequence": [*state["sequence"], value],
        },
        late_policy=policy,
        limits=replace(WindowFoldLimits(), max_row_bytes=512, max_batch_bytes=1024),
    )
    oracle = EnumerationOracle(spec)
    runtime = restored(WindowFoldRuntime(spec), oracle)
    rng = random.Random(seed)
    for position in range(32):
        timestamp, key, value = (
            rng.randrange(-20, 31),
            rng.choice(["a", "z", "é"]),
            rng.randrange(-8, 9),
        )
        expected, memberships = oracle.process(timestamp, key, value)
        if expected == "rejected":
            before = runtime.checkpoint()
            with pytest.raises(ValidationError):
                runtime.process(timestamp, FlowRecord(value, key))
            assert runtime.checkpoint() == before
        else:
            result = runtime.process(timestamp, FlowRecord(value, key))
            assert (result.outcome, result.memberships) == (expected, memberships)
        runtime = restored(runtime, oracle)
        if position in (5, 11, 17, 23, 29):
            watermark = [-15, -5, 0, 9, 20][(position - 5) // 6]
            oracle.advance(watermark)
            runtime.advance_watermark(watermark)
            runtime = restored(runtime, oracle)
            while oracle.pending():
                requested = rng.randrange(1, 4)
                expected_rows = oracle.drain(requested)
                assert [
                    row.to_dict() for row in runtime.drain(max_windows=requested).rows
                ] == expected_rows
                runtime = restored(runtime, oracle)
    oracle.finish()
    runtime.finish()
    runtime = restored(runtime, oracle)
    while oracle.pending():
        expected_rows = oracle.drain(1)
        assert [row.to_dict() for row in runtime.drain(max_windows=1).rows] == expected_rows
        runtime = restored(runtime, oracle)
    assert runtime.status.phase == "closed"
    assert runtime.finish() == runtime.status
    assert runtime.drain().rows == ()
    restored(runtime, oracle)


def test_manual_arrival_order_and_negative_index_anchor():
    spec = WindowFold(
        "manual",
        "v1",
        width=5,
        hop=3,
        origin=1,
        initial=list,
        fold=lambda state, value: [*state, value],
        late_policy="drop",
    )
    runtime = WindowFoldRuntime(spec)
    for timestamp, key, value in [(2, "A", 2), (0, "A", 5), (2, "B", 7)]:
        runtime.process(timestamp, FlowRecord(value, key))
    runtime.advance_watermark(3)
    rows = runtime.drain(max_windows=1).rows + runtime.drain(max_windows=1).rows
    assert [(row.index, row.key, row.value) for row in rows] == [(-1, "A", [2, 5]), (-1, "B", [7])]
    for timestamp, value in [(3, 11), (2, 100), (4, 13)]:
        runtime.process(timestamp, FlowRecord(value, "A"))
    runtime.finish()
    rows = runtime.drain().rows
    assert [(row.index, row.key, row.input_count, sum(row.value)) for row in rows] == [
        (0, "A", 3, 26),
        (0, "B", 1, 7),
        (1, "A", 1, 13),
    ]
    assert runtime.checkpoint().to_dict()["body"]["counters"] == {
        "processed_inputs": 6,
        "late_drops": 1,
        "gap_inputs": 0,
        "membership_updates": 8,
        "created_windows": 5,
        "emitted_windows": 5,
        "finalized_memberships": 8,
    }
