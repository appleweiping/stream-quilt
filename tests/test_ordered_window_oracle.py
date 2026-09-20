"""Independent event-time oracle and recovery boundaries for ordered folds."""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from stream_quilt import (
    FlowRecord,
    OrderedWindowAdmission,
    OrderedWindowCheckpoint,
    OrderedWindowEffect,
    OrderedWindowFoldRuntime,
    OrderedWindowLimits,
    OrderedWindowRelease,
    OrderedWindowStatus,
    ValidationError,
    WindowFold,
    WindowFoldLimits,
    WindowFoldRuntime,
)
from stream_quilt.connectors import files as file_connector
from stream_quilt.connectors.files import StagedJsonlSource


def _spec(*, width: int = 5, hop: int = 3, finalize: Any = None) -> WindowFold:
    return WindowFold(
        "ordered-oracle",
        "v1",
        width=width,
        hop=hop,
        initial=lambda: "",
        fold=lambda state, value: state + value,
        finalize=finalize,
    )


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def _resign(document: dict[str, Any]) -> dict[str, Any]:
    document["sha256"] = hashlib.sha256(_canonical(document["body"]).encode("utf-8")).hexdigest()
    return document


def _memberships(timestamp: int, width: int, hop: int) -> list[int]:
    # Deliberately enumerate half-open intervals instead of reusing the runtime's
    # floor-division geometry formula or its private stage functions.
    return [index for index in range(-8, 9) if index * hop <= timestamp < index * hop + width]


@pytest.mark.parametrize("width,hop", [(5, 3), (2, 5)])
@pytest.mark.parametrize("seed", range(6))
def test_overlap_and_gap_match_stable_sort_oracle(width: int, hop: int, seed: int) -> None:
    samples = [
        (-8, "a"),
        (-6, "b"),
        (-5, "a"),
        (-4, "b"),
        (-1, "a"),
        (0, "b"),
        (1, "a"),
        (2, "b"),
        (2, "a"),
        (4, "b"),
        (5, "a"),
        (6, "b"),
        (7, "a"),
        (8, "b"),
    ]
    random.Random(seed).shuffle(samples)
    inputs = [
        (position, timestamp, key, chr(65 + position))
        for position, (timestamp, key) in enumerate(samples)
    ]
    runtime = OrderedWindowFoldRuntime(_spec(width=width, hop=hop))
    for position, timestamp, key, value in inputs:
        admission = runtime.process(position, timestamp, FlowRecord(value, key))
        assert admission.position == position
        assert admission.status.next_position == position + 1

    remaining = list(inputs)
    cells: dict[tuple[int, str], tuple[str, int]] = {}
    counters = {
        "processed_inputs": 0,
        "late_drops": 0,
        "gap_inputs": 0,
        "membership_updates": 0,
        "created_windows": 0,
        "emitted_windows": 0,
        "finalized_memberships": 0,
    }

    for watermark in (0, 5, 9, None):
        due = sorted(
            (item for item in remaining if watermark is None or item[1] < watermark),
            key=lambda item: (item[1], item[0]),
        )
        remaining = [item for item in remaining if item not in due]
        release = (
            runtime.finish(next_position=len(inputs))
            if watermark is None
            else runtime.advance_watermark(watermark, next_position=len(inputs))
        )
        expected_effects = []
        for position, timestamp, key, value in due:
            indices = _memberships(timestamp, width, hop)
            counters["processed_inputs"] += 1
            counters["membership_updates"] += len(indices)
            counters["gap_inputs"] += not indices
            expected_effects.append(
                (position, timestamp, "folded" if indices else "gap", len(indices))
            )
            for index in indices:
                identity = (index, key)
                if identity not in cells:
                    counters["created_windows"] += 1
                prior, count = cells.get(identity, ("", 0))
                cells[identity] = (prior + value, count + 1)
        assert [
            (e.position, e.timestamp, e.outcome, e.memberships) for e in release.effects
        ] == expected_effects

        pending = sorted(
            identity
            for identity in cells
            if watermark is None or identity[0] * hop + width <= watermark
        )
        assert release.status.buffered_records == len(remaining)
        assert release.status.buffered_bytes == sum(
            len(_canonical({"position": p, "timestamp": t, "key": k, "value": v}).encode("utf-8"))
            for p, t, k, v in remaining
        )
        assert release.status.window.watermark == (9 if watermark is None else watermark)
        assert release.status.window.finished == (watermark is None)
        assert release.status.window.retained_windows == len(cells)
        assert release.status.window.pending_windows == len(pending)
        assert release.status.window.phase == (
            "draining" if pending else "closed" if watermark is None else "open"
        )

        expected_rows = []
        for identity in pending:
            value, count = cells.pop(identity)
            index, key = identity
            expected_rows.append((index, key, index * hop, index * hop + width, count, value))
            counters["finalized_memberships"] += count
        counters["emitted_windows"] += len(pending)
        rows = []
        while runtime.status.window.pending_windows:
            rows.extend(runtime.drain(max_windows=2).rows)
        assert [
            (r.index, r.key, r.start, r.end, r.input_count, r.value) for r in rows
        ] == expected_rows
        assert runtime.checkpoint().to_dict()["body"]["window"]["body"]["counters"] == counters
    assert runtime.status.window.phase == "closed"


def test_second_finalizer_failure_rolls_back_whole_drain_and_retry() -> None:
    fail = [True]
    observed: list[str] = []

    def finalize(value: str) -> str:
        observed.append(value)
        if value == "B" and fail[0]:
            raise RuntimeError("injected second finalizer failure")
        return value

    runtime = OrderedWindowFoldRuntime(_spec(width=2, hop=2, finalize=finalize))
    runtime.process(0, 0, FlowRecord("A", "k"))
    runtime.process(1, 2, FlowRecord("B", "k"))
    runtime.finish(next_position=2)
    before = runtime.checkpoint().to_json()
    with pytest.raises(ValidationError):
        runtime.drain(max_windows=2)
    assert observed == ["A", "B"]  # External callback effects are not rolled back.
    assert runtime.checkpoint().to_json() == before
    assert runtime.status.window.pending_windows == 2
    fail[0] = False
    assert [(r.index, r.value) for r in runtime.drain(max_windows=2).rows] == [(0, "A"), (1, "B")]
    assert runtime.status.window.phase == "closed"


def test_buffer_byte_limit_position_and_rehashed_checkpoint_tamper() -> None:
    first = {"position": 0, "timestamp": 5, "key": "k", "value": "A"}
    limit = len(_canonical(first).encode("utf-8"))
    spec = _spec()
    bounds = OrderedWindowLimits(max_buffer_records=2, max_buffer_bytes=limit)
    runtime = OrderedWindowFoldRuntime(spec, limits=bounds)
    runtime.process(0, 5, FlowRecord("A", "k"))
    before = runtime.checkpoint().to_json()
    for position, timestamp in ((1, 6), (2, 6), (0, 6)):
        with pytest.raises(ValidationError):
            runtime.process(position, timestamp, FlowRecord("B", "k"))
        assert runtime.checkpoint().to_json() == before
    with pytest.raises(ValidationError):
        runtime.advance_watermark(6, next_position=0)
    assert runtime.checkpoint().to_json() == before

    forged = json.loads(before)
    forged["body"]["buffer"][0]["value"] = "AA"
    with pytest.raises(ValidationError):
        OrderedWindowCheckpoint.from_dict(_resign(forged))
    forged = json.loads(before)
    forged["body"]["buffer"][0]["position"] = 1
    with pytest.raises(ValidationError):
        OrderedWindowCheckpoint.from_dict(_resign(forged))

    runtime.advance_watermark(3, next_position=1)
    saved = runtime.checkpoint().to_json()
    forged = json.loads(saved)
    forged["body"]["buffer"][0]["timestamp"] = 2
    with pytest.raises(ValidationError):
        OrderedWindowCheckpoint.from_dict(_resign(forged))
    forged = json.loads(saved)
    forged["body"]["window"]["body"]["counters"]["gap_inputs"] = 1
    with pytest.raises(ValidationError):
        OrderedWindowCheckpoint.from_dict(_resign(forged))

    recovered = OrderedWindowFoldRuntime.from_checkpoint(
        spec, OrderedWindowCheckpoint.from_json(saved), limits=bounds
    )
    for instance in (runtime, recovered):
        instance.finish(next_position=1)
    assert recovered.checkpoint().to_json() == runtime.checkpoint().to_json()
    assert recovered.drain().rows[0].value == runtime.drain().rows[0].value == "A"


def test_draining_with_future_buffer_rejects_operations_without_losing_prefix() -> None:
    runtime = OrderedWindowFoldRuntime(_spec(width=2, hop=2))
    runtime.process(0, 10, FlowRecord("later", "k"))
    runtime.process(1, 0, FlowRecord("first", "k"))
    released = runtime.advance_watermark(2, next_position=2)
    assert [(e.position, e.timestamp) for e in released.effects] == [(1, 0)]
    assert released.status.window.phase == "draining"
    assert released.status.buffered_records == 1
    before = runtime.checkpoint().to_json()
    for operation in (
        lambda: runtime.process(2, 11, FlowRecord("extra", "k")),
        lambda: runtime.advance_watermark(3, next_position=2),
        lambda: runtime.finish(next_position=2),
    ):
        with pytest.raises(ValidationError):
            operation()
        assert runtime.checkpoint().to_json() == before

    assert [(r.index, r.value) for r in runtime.drain().rows] == [(0, "first")]
    assert runtime.advance_watermark(2, next_position=2).effects == ()
    after_drain = runtime.checkpoint().to_json()
    with pytest.raises(ValidationError, match="regress"):
        runtime.advance_watermark(1, next_position=2)
    assert runtime.checkpoint().to_json() == after_drain
    final = runtime.finish(next_position=2)
    assert [(e.position, e.timestamp) for e in final.effects] == [(0, 10)]
    assert [(r.index, r.value) for r in runtime.drain().rows] == [(5, "later")]
    saved = runtime.checkpoint().to_json()
    assert runtime.finish(next_position=2).effects == ()
    assert runtime.checkpoint().to_json() == saved


def test_input_limit_and_invalid_record_leave_admitted_prefix_unchanged() -> None:
    spec = WindowFold(
        "ordered-cap",
        "v1",
        width=2,
        initial=lambda: "",
        fold=lambda state, value: state + value,
        limits=WindowFoldLimits(max_inputs=1),
    )
    runtime = OrderedWindowFoldRuntime(spec)
    with pytest.raises(ValidationError, match="keyed FlowRecord"):
        runtime.process(0, 0, "not a record")
    with pytest.raises(ValidationError, match="keyed FlowRecord"):
        runtime.process(0, 0, FlowRecord("ignored"))
    runtime.process(0, 0, FlowRecord("A", "k"))
    before = runtime.checkpoint().to_json()
    with pytest.raises(ValidationError, match="input limit"):
        runtime.process(1, 1, FlowRecord("B", "k"))
    assert runtime.checkpoint().to_json() == before
    assert [(e.position, e.memberships) for e in runtime.finish(next_position=1).effects] == [
        (0, 1)
    ]
    assert [(r.index, r.value) for r in runtime.drain().rows] == [(0, "A")]
    for operation in (
        lambda: runtime.process(1, 1, FlowRecord("B", "k")),
        lambda: runtime.advance_watermark(2, next_position=1),
    ):
        with pytest.raises(ValidationError):
            operation()


@pytest.mark.parametrize(
    "mutate,message",
    [
        (lambda d: d["body"].update(kind="unknown"), "kind/version"),
        (lambda d: d["body"].update(fold_identity="0" * 64), "fold identity"),
        (lambda d: d["body"].update(buffer={}), "buffer exceeds record limit"),
        (lambda d: d["body"]["buffer"][0].update(key=7), "keyed records"),
    ],
)
def test_rehashed_checkpoint_rejects_semantically_invalid_buffer(mutate, message: str) -> None:
    runtime = OrderedWindowFoldRuntime(_spec())
    runtime.process(0, 5, FlowRecord("A", "k"))
    valid = runtime.checkpoint().to_json()
    forged = json.loads(valid)
    mutate(forged)
    with pytest.raises(ValidationError, match=message):
        OrderedWindowCheckpoint.from_dict(_resign(forged))
    assert runtime.checkpoint().to_json() == valid


def test_rehashed_checkpoint_rejects_finished_buffer_and_excess_source_prefix() -> None:
    spec = _spec()
    finished = OrderedWindowFoldRuntime(spec)
    finished.finish(next_position=0)
    forged = finished.checkpoint().to_dict()
    forged["body"]["buffer"].append({"position": 0, "timestamp": 0, "key": "k", "value": "A"})
    forged["body"]["next_position"] = 1
    with pytest.raises(ValidationError, match="finished ordered window"):
        OrderedWindowCheckpoint.from_dict(_resign(forged))

    capped_spec = WindowFold(
        "ordered-cap",
        "v1",
        width=2,
        initial=lambda: "",
        fold=lambda state, value: state + value,
        limits=WindowFoldLimits(max_inputs=1),
    )
    capped = OrderedWindowFoldRuntime(capped_spec)
    capped.process(0, 0, FlowRecord("A", "k"))
    forged = capped.checkpoint().to_dict()
    forged["body"]["buffer"].append({"position": 1, "timestamp": 1, "key": "k", "value": "B"})
    forged["body"]["next_position"] = 2
    with pytest.raises(ValidationError, match="input limit"):
        OrderedWindowCheckpoint.from_dict(_resign(forged))


def test_checkpoint_canonical_encoding_checksum_limits_and_utf8() -> None:
    spec = _spec()
    runtime = OrderedWindowFoldRuntime(spec)
    runtime.process(0, 5, FlowRecord("A", "k"))
    checkpoint = runtime.checkpoint()
    forged = checkpoint.to_dict()
    forged["sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="checksum"):
        OrderedWindowCheckpoint.from_dict(forged)
    with pytest.raises(ValidationError, match="limits mismatch"):
        OrderedWindowFoldRuntime.from_checkpoint(
            spec, checkpoint, limits=OrderedWindowLimits(max_buffer_records=1)
        )
    with pytest.raises(ValidationError, match="canonical"):
        OrderedWindowCheckpoint.from_json(json.dumps(checkpoint.to_dict(), indent=2))
    # A caller able to bypass a frozen dataclass cannot smuggle a noncanonical
    # representation through the public serializer either.
    corrupted = object.__new__(OrderedWindowCheckpoint)
    object.__setattr__(corrupted, "_json", json.dumps(checkpoint.to_dict(), indent=2))
    with pytest.raises(ValidationError, match="canonical"):
        corrupted.to_dict()
    with pytest.raises(ValidationError, match="byte bound"):
        OrderedWindowCheckpoint.from_json(42)
    with pytest.raises(ValidationError, match="UTF-8"):
        OrderedWindowCheckpoint.from_json(b"\xff")
    assert (
        OrderedWindowFoldRuntime.from_checkpoint(spec, checkpoint).checkpoint().to_json()
        == checkpoint.to_json()
    )


def test_ordered_public_values_and_restore_require_exact_types() -> None:
    status = OrderedWindowFoldRuntime(_spec()).status
    with pytest.raises(ValidationError, match="requires WindowStatus"):
        OrderedWindowStatus(0, 0, 0, status)
    with pytest.raises(ValidationError, match="requires status"):
        OrderedWindowAdmission(0, status.window)
    with pytest.raises(ValidationError, match="contradicts membership"):
        OrderedWindowEffect(0, 0, "gap", 1)
    with pytest.raises(ValidationError, match="invalid ordered release"):
        OrderedWindowRelease([OrderedWindowEffect(0, 0, "gap", 0)], status)
    with pytest.raises(ValidationError, match="reject-late"):
        OrderedWindowFoldRuntime(
            WindowFold(
                "drop",
                "v1",
                width=2,
                initial=lambda: "",
                fold=lambda a, b: a + b,
                late_policy="drop",
            )
        )
    with pytest.raises(ValidationError, match="OrderedWindowLimits"):
        OrderedWindowFoldRuntime(_spec(), limits={})
    with pytest.raises(ValidationError, match="requires OrderedWindowCheckpoint"):
        OrderedWindowFoldRuntime.from_checkpoint(_spec(), {})
    with pytest.raises(ValidationError, match="requires WindowCheckpoint"):
        WindowFoldRuntime.from_checkpoint(_spec(), OrderedWindowFoldRuntime(_spec()).checkpoint())


def test_staged_source_replays_ordered_positions_across_checkpoint(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    nested = incoming / "daily"
    nested.mkdir(parents=True)
    raw = (
        b'{"key":"k","value":{"timestamp":10,"text":"later"}}\n'
        b'{"key":"k","value":{"timestamp":0,"text":"first"}}\n'
        b'{"key":"k","value":{"timestamp":1,"text":"second"}}\n'
    )
    (nested / "rows.jsonl").write_bytes(raw)
    store = tmp_path / "private"
    source = StagedJsonlSource.stage(incoming, "daily/rows.jsonl", store, "daily")
    assert source.record_count == 3
    spec = _spec(width=2, hop=2)
    runtime = OrderedWindowFoldRuntime(spec)
    first = source.read_batch(0, 1)[0]
    runtime.process(0, first.value["timestamp"], FlowRecord(first.value["text"], first.key))
    checkpoint = runtime.checkpoint()

    # The original input may disappear after staging: replay is bound to the
    # owned blob and exact source offsets, not to a fresh arrival-order read.
    (nested / "rows.jsonl").unlink()
    replay = StagedJsonlSource.restore(
        store, "daily/rows.jsonl", source.source_id, source.source_digest
    )
    replay.verify()
    recovered = OrderedWindowFoldRuntime.from_checkpoint(spec, checkpoint)
    for position, record in enumerate(replay.read_batch(1, 2), start=1):
        recovered.process(
            position, record.value["timestamp"], FlowRecord(record.value["text"], record.key)
        )
    release = recovered.advance_watermark(2, next_position=3)
    assert [(e.position, e.timestamp) for e in release.effects] == [(1, 0), (2, 1)]
    assert [(r.index, r.value) for r in recovered.drain().rows] == [(0, "firstsecond")]
    assert [(e.position, e.timestamp) for e in recovered.finish(next_position=3).effects] == [
        (0, 10)
    ]
    assert [(r.index, r.value) for r in recovered.drain().rows] == [(5, "later")]

    with pytest.raises(ValidationError, match="outside staged file"):
        replay.read_batch(2, 2)
    with pytest.raises(ValidationError, match="offset table changed"):
        replace(replay, byte_offsets=(0, len(raw))).verify()
    with pytest.raises(ValidationError, match="staged file identity"):
        StagedJsonlSource.restore(store, "daily/rows.jsonl", "other", source.source_digest)
    with pytest.raises(ValidationError, match="too long"):
        StagedJsonlSource.stage(incoming, "a" * 1025, store, "daily")


def test_staging_rejects_changed_blob_and_unadmittable_source(tmp_path: Path, monkeypatch) -> None:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    raw = b'{"key":"k","value":"A"}\n{"key":"k","value":"B"}\n'
    (incoming / "rows.jsonl").write_bytes(raw)
    store = tmp_path / "private"

    with pytest.raises(ValidationError, match="cannot read staged source"):
        StagedJsonlSource.stage(incoming, "missing.jsonl", store, "rows")
    (incoming / "not-a-file").mkdir()
    with pytest.raises(ValidationError, match="not a plain file"):
        StagedJsonlSource.stage(incoming, "not-a-file", store, "rows")
    with monkeypatch.context() as patch:
        patch.setattr(file_connector, "_MAX_SOURCE_BYTES", len(raw) - 1)
        with pytest.raises(ValidationError, match="staged source exceeds byte limit"):
            StagedJsonlSource.stage(incoming, "rows.jsonl", store, "rows")
    with monkeypatch.context() as patch:
        patch.setattr(file_connector, "_MAX_LINES", 1)
        with pytest.raises(ValidationError, match="source file exceeds record limit"):
            StagedJsonlSource.stage(incoming, "rows.jsonl", store, "rows")
    assert not store.exists()

    source = StagedJsonlSource.stage(incoming, "rows.jsonl", store, "rows")
    source.blob_path.write_bytes(raw.replace(b'"A"', b'"X"'))
    with pytest.raises(ValidationError, match="differs from its digest"):
        StagedJsonlSource.stage(incoming, "rows.jsonl", store, "rows")
