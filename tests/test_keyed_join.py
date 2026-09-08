from __future__ import annotations

import copy
import itertools
import json
import random
from dataclasses import FrozenInstanceError, replace

import pytest

import stream_quilt.keyed_join as module
from stream_quilt import (
    FlowRecord,
    JoinBatch,
    JoinCheckpoint,
    JoinLimits,
    JoinRow,
    JoinRuntime,
    KeyedJoin,
    ValidationError,
)

MODES = [
    (insert, emit)
    for insert in ("first", "last", "product")
    for emit in ("complete", "final", "running")
]


def config(insert="last", emit="complete", *, sides=("left", "right"), limits=None):
    return KeyedJoin("join", "v1", sides, insert, emit, limits or JoinLimits())


def plain(rows):
    return [(row.key, row.present, row.values) for row in rows]


def expand_reference(key, buckets):
    combinations = [[]]
    for bucket in buckets:
        combinations = [[*prefix, value] for prefix in combinations for value in (bucket or [None])]
    return [
        (key, tuple(bool(bucket) for bucket in buckets), tuple(values)) for values in combinations
    ]


def reference(arrivals, sides, insert, emit):
    """A simple unbounded materialized oracle; no runtime/snapshot/private helpers."""
    state = {}
    batches = []
    for side, key, value in arrivals:
        buckets = state.setdefault(key, [[] for _ in sides])
        index = sides.index(side)
        if insert == "product":
            buckets[index].append(value)
        elif insert == "last" or not buckets[index]:
            buckets[index] = [value]
        if emit == "running" or (emit == "complete" and all(buckets)):
            batches.append(expand_reference(key, buckets))
            if emit == "complete":
                del state[key]
        else:
            batches.append([])
    final = (
        [row for key in sorted(state) for row in expand_reference(key, state[key])]
        if emit == "final"
        else []
    )
    return batches, final


@pytest.mark.parametrize(("insert", "emit"), MODES)
def test_manual_three_side_modes_with_overwrites_and_complete_cycles(insert, emit):
    sides = ("name", "email", "role")
    arrivals = [
        ("name", "a", "Alice"),
        ("name", "a", "Alicia"),
        ("email", "a", "first"),
        ("email", "a", "second"),
        ("role", "a", "reader"),
        ("role", "a", "writer"),
        ("name", "b", None),
        ("email", "a", "third"),
    ]
    expected, final = reference(arrivals, sides, insert, emit)
    runtime = JoinRuntime(config(insert, emit, sides=sides))
    for (side, key, value), want in zip(arrivals, expected, strict=True):
        assert plain(runtime.process(side, FlowRecord(value, key)).rows) == want
    assert runtime.close("name").phase == "open"
    assert runtime.close("role").phase == "open"
    terminal = runtime.close("email")
    assert terminal.rows == ()
    drained = []
    while runtime.phase == "draining":
        batch = runtime.drain(max_keys=1)
        assert batch.drained_keys == 1
        drained.extend(plain(batch.rows))
    assert drained == final
    assert runtime.phase == "closed"
    assert runtime.checkpoint().processed_inputs == (3, 3, 2)
    assert runtime.checkpoint().emitted_rows == sum(map(len, expected)) + len(final)


@pytest.mark.parametrize(("insert", "emit"), MODES)
def test_seeded_prefix_and_every_boundary_restart_matches_independent_oracle(insert, emit):
    rng = random.Random(71513)
    sides = ("third", "first", "second")
    arrivals = [
        (rng.choice(sides), rng.choice(("雪", "a", "b")), rng.choice((None, False, 0, "x")))
        for _ in range(24)
    ]
    expected, final = reference(arrivals, sides, insert, emit)
    join = config(insert, emit, sides=sides)
    runtime = JoinRuntime(join)
    uninterrupted = JoinRuntime(join)
    for (side, key, value), want in zip(arrivals, expected, strict=True):
        assert plain(runtime.process(side, FlowRecord(value, key)).rows) == want
        assert plain(uninterrupted.process(side, FlowRecord(value, key)).rows) == want
        runtime = JoinRuntime.from_checkpoint(
            join, JoinCheckpoint.from_json(runtime.checkpoint().to_json())
        )
        assert runtime.checkpoint() == uninterrupted.checkpoint()
    for side in reversed(sides):
        assert runtime.close(side) == uninterrupted.close(side)
        runtime = JoinRuntime.from_checkpoint(
            join, JoinCheckpoint.from_dict(runtime.checkpoint().to_dict())
        )
    output = []
    while runtime.phase == "draining":
        batch = runtime.drain(max_keys=1)
        assert batch == uninterrupted.drain(max_keys=1)
        output.extend(plain(batch.rows))
        runtime = JoinRuntime.from_checkpoint(join, runtime.checkpoint())
    assert output == final
    assert runtime.checkpoint() == uninterrupted.checkpoint()


def test_running_first_reemits_unchanged_row_and_product_reemits_old_combinations():
    first = JoinRuntime(config("first", "running"))
    assert plain(first.process("left", FlowRecord(1, "k")).rows) == [
        ("k", (True, False), (1, None))
    ]
    assert plain(first.process("left", FlowRecord(2, "k")).rows) == [
        ("k", (True, False), (1, None))
    ]
    product = JoinRuntime(config("product", "running"))
    product.process("left", FlowRecord(1, "k"))
    assert [row.values for row in product.process("left", FlowRecord(2, "k")).rows] == [
        (1, None),
        (2, None),
    ]
    assert [row.values for row in product.process("right", FlowRecord(3, "k")).rows] == [
        (1, 3),
        (2, 3),
    ]
    assert [row.values for row in product.process("right", FlowRecord(3, "k")).rows] == [
        (1, 3),
        (1, 3),
        (2, 3),
        (2, 3),
    ]


def test_null_presence_snapshot_isolation_and_explicit_flow_conversion():
    value = {"雪": [1, None]}
    record = FlowRecord(value, "k")
    runtime = JoinRuntime(config(emit="running"))
    row = runtime.process("left", record).rows[0]
    value["雪"].append(99)
    row.values[0]["雪"].append(33)
    copied = row.to_dict()
    copied["values"][0]["雪"].clear()
    assert row.values == ({"雪": [1, None]}, None)
    assert row.present == (True, False)
    assert row.to_record().value == {"present": [True, False], "values": [{"雪": [1, None]}, None]}
    actual_null = runtime.process("right", FlowRecord(None, "k")).rows[0]
    assert actual_null.present == (True, True)
    with pytest.raises(FrozenInstanceError):
        row.key = "other"
    with pytest.raises(AttributeError):
        runtime.join = config()


@pytest.mark.parametrize("emit", ["complete", "running", "final"])
def test_empty_eof_idempotency_and_no_fabricated_results(emit):
    runtime = JoinRuntime(config(emit=emit))
    assert runtime.close("left") == JoinBatch((), "open", 0)
    assert runtime.close("left") == JoinBatch((), "open", 0)
    with pytest.raises(ValidationError):
        runtime.drain()
    assert runtime.close("right") == JoinBatch((), "closed", 0)
    before = runtime.checkpoint()
    assert runtime.close("right") == JoinBatch((), "closed", 0)
    assert runtime.drain() == JoinBatch((), "closed", 0)
    assert runtime.checkpoint() == before
    with pytest.raises(ValidationError):
        runtime.process("left", FlowRecord(1, "k"))


def test_closed_side_values_still_contribute_until_all_sides_close():
    runtime = JoinRuntime(config("last", "complete"))
    runtime.process("left", FlowRecord(1, "k"))
    runtime.close("left")
    assert [row.values for row in runtime.process("right", FlowRecord(2, "k")).rows] == [(1, 2)]
    runtime.process("right", FlowRecord(3, "k"))
    assert runtime.close("right") == JoinBatch((), "closed", 0)
    assert runtime.checkpoint().cells == ()


def test_drain_obeys_key_count_row_count_bytes_and_sorted_order():
    limits = JoinLimits(max_rows_per_key=2, max_rows_per_batch=2)
    runtime = JoinRuntime(config("product", "final", limits=limits))
    for key in ("z", "a", "m"):
        runtime.process("left", FlowRecord(1, key))
    runtime.process("left", FlowRecord(2, "a"))
    runtime.close("left")
    runtime.close("right")
    first = runtime.drain(max_keys=100)
    assert [row.key for row in first.rows] == ["a", "a"]
    assert (first.drained_keys, first.pending_keys, first.phase) == (1, 2, "draining")
    second = runtime.drain(max_keys=1)
    assert [row.key for row in second.rows] == ["m"]
    assert [row.key for row in runtime.drain().rows] == ["z"]
    row_bytes = JoinRow("a", ("left", "right"), (True, False), (1, None)).byte_size
    limits = JoinLimits(max_row_bytes=row_bytes, max_batch_bytes=row_bytes)
    bounded = JoinRuntime(config(emit="final", limits=limits))
    for key in ("b", "a"):
        bounded.process("left", FlowRecord(1, key))
    bounded.close("left")
    bounded.close("right")
    assert bounded.drain().drained_keys == 1
    assert bounded.drain().phase == "closed"


@pytest.mark.parametrize("value", [None, '雪"\\\n', [1, False], {"x": "🙂"}])
def test_row_and_checkpoint_cell_byte_accounting_independent_json_encoder(value):
    runtime = JoinRuntime(config(emit="running"))
    row = runtime.process("left", FlowRecord(value, "key-雪")).rows[0]
    rendered = json.dumps(
        {"present": list(row.present), "values": list(row.values)},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert row.byte_size == len(rendered.encode("utf-8"))
    cell = runtime.checkpoint().to_dict()["cells"][0]
    exact = len(json.dumps(cell, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    allowed = JoinRuntime(
        config(emit="running", limits=JoinLimits(max_key_bytes=exact, max_state_bytes=exact))
    )
    allowed.process("left", FlowRecord(value, "key-雪"))
    denied = JoinRuntime(config(emit="running", limits=JoinLimits(max_key_bytes=exact - 1)))
    before = denied.checkpoint()
    with pytest.raises(ValidationError):
        denied.process("left", FlowRecord(value, "key-雪"))
    assert denied.checkpoint() == before


@pytest.mark.parametrize("emit", ["complete", "running", "final"])
def test_product_limit_rejects_before_output_iteration_and_rolls_back(monkeypatch, emit):
    runtime = JoinRuntime(config("product", emit, limits=JoinLimits(max_rows_per_key=2)))
    runtime.process("left", FlowRecord(1, "k"))
    runtime.process("left", FlowRecord(2, "k"))
    before = runtime.checkpoint()

    def forbidden(*args):
        raise AssertionError("product output must not be materialized")

    monkeypatch.setattr(module, "_rows", forbidden)
    with pytest.raises(ValidationError, match="Cartesian"):
        runtime.process("left", FlowRecord(3, "k"))
    assert runtime.checkpoint() == before


@pytest.mark.parametrize(
    "limit",
    [
        JoinLimits(max_keys=1),
        JoinLimits(max_values=1),
        JoinLimits(max_values_per_side=1),
        JoinLimits(max_value_bytes=1),
        JoinLimits(max_state_bytes=1),
        JoinLimits(max_row_bytes=1),
        JoinLimits(max_batch_bytes=1, max_row_bytes=1),
    ],
)
def test_capacity_failures_keep_all_state_and_counts(limit):
    runtime = JoinRuntime(config("product", "final", limits=limit))
    first = FlowRecord(1, "a")
    before = runtime.checkpoint()
    try:
        runtime.process("left", first)
    except ValidationError:
        assert runtime.checkpoint() == before
        return
    before = runtime.checkpoint()
    key = "b" if limit.max_keys == 1 else "a"
    value = "x" if limit.max_value_bytes == 1 else 2
    with pytest.raises(ValidationError):
        runtime.process("left", FlowRecord(value, key))
    assert runtime.checkpoint() == before


def test_complete_removal_releases_capacity_before_next_key():
    runtime = JoinRuntime(config(limits=JoinLimits(max_keys=1, max_values=1)))
    runtime.process("left", FlowRecord(1, "a"))
    assert runtime.process("right", FlowRecord(2, "a")).rows[0].values == (1, 2)
    runtime.process("left", FlowRecord(3, "b"))
    assert runtime.checkpoint().cells[0][0] == "b"


@pytest.mark.parametrize("error", [RuntimeError("failure"), KeyboardInterrupt(), SystemExit(7)])
def test_failed_drain_after_first_row_preserves_entire_operation(monkeypatch, error):
    runtime = JoinRuntime(config("product", "final"))
    runtime.process("left", FlowRecord(1, "a"))
    runtime.process("left", FlowRecord(2, "b"))
    runtime.close("left")
    runtime.close("right")
    before = runtime.checkpoint()
    original = module._rows

    def fail(key, cell, sides):
        yield from original(key, cell, sides)
        raise error

    with monkeypatch.context() as patch:
        patch.setattr(module, "_rows", fail)
        with pytest.raises(type(error)):
            runtime.drain()
    assert runtime.checkpoint() == before
    assert runtime.drain().phase == "closed"


@pytest.mark.parametrize("operation", ["process", "close", "drain"])
def test_final_publication_allocation_failure_rolls_back(monkeypatch, operation):
    runtime = JoinRuntime(config(emit="final"))
    runtime.process("left", FlowRecord(1, "k"))
    if operation == "drain":
        runtime.close("left")
        runtime.close("right")
    before = runtime.checkpoint()

    def fail(*args, **kwargs):
        raise MemoryError("state allocation")

    with monkeypatch.context() as patch:
        patch.setattr(module, "_State", fail)
        with pytest.raises(MemoryError):
            if operation == "process":
                runtime.process("right", FlowRecord(2, "k"))
            elif operation == "close":
                runtime.close("left")
            else:
                runtime.drain()
    assert runtime.checkpoint() == before


def test_reentrant_process_cannot_publish_inner_or_outer_change(monkeypatch):
    runtime = JoinRuntime(config(emit="running"))
    before = runtime.checkpoint()

    def reenter(*args):
        runtime.process("left", FlowRecord(2, "k"))
        return iter(())

    with monkeypatch.context() as patch:
        patch.setattr(module, "_rows", reenter)
        with pytest.raises(ValidationError, match="reentrant"):
            runtime.process("right", FlowRecord(1, "k"))
    assert runtime.checkpoint() == before


def valid_document():
    runtime = JoinRuntime(config(emit="final"))
    runtime.process("left", FlowRecord(1, "k"))
    return runtime.checkpoint().to_dict()


@pytest.mark.parametrize(
    "field,value",
    [
        ("extra", 1),
        ("kind", "other"),
        ("version", "2.0"),
        ("identity", "X" * 64),
        ("sides", ["left"]),
        ("sides", ["left", "left"]),
        ("closed_sides", [False, 0]),
        ("closed_sides", [True, True]),
        ("processed_inputs", [False, 0]),
        ("processed_inputs", [0, 0]),
        ("processed_inputs", [2**53, 0]),
        ("emitted_rows", -1),
        ("emitted_rows", 100_001),
        ("phase", "unknown"),
        ("phase", "closed"),
        ("cells", {}),
        ("cells", [{"key": "k", "values": [["1"], []], "extra": 1}]),
        ("cells", [{"key": "k", "values": [[], []]}]),
        ("cells", [{"key": "k", "values": [["1"]]}]),
        ("cells", [{"key": "k", "values": [[1], []]}]),
        ("cells", [{"key": " k", "values": [["1"], []]}]),
    ],
)
def test_strict_checkpoint_document_shape(field, value):
    document = valid_document()
    document[field] = value
    with pytest.raises(ValidationError):
        JoinCheckpoint.from_dict(document)


@pytest.mark.parametrize(
    "encoded",
    [
        "NaN",
        "Infinity",
        "1e999",
        "1 ",
        '{"x":1,"x":2}',
        '"\\ud800"',
        "[" * 1000 + "0" + "]" * 1000,
        "\ud800",
    ],
)
def test_checkpoint_rejects_noncanonical_unsafe_or_excessively_deep_nested_json(encoded):
    document = valid_document()
    document["cells"][0]["values"][0][0] = encoded
    with pytest.raises(ValidationError):
        JoinCheckpoint.from_dict(document)


def test_checkpoint_preflight_charges_aliased_encoded_values_before_nested_parse(monkeypatch):
    document = valid_document()
    document["processed_inputs"] = [2, 0]
    document["cells"][0]["values"][0] = ["1", "1"]
    limits = replace(module._HARD_LIMITS, max_values=1)

    def forbidden(*args, **kwargs):
        raise AssertionError("nested JSON materialized before cumulative preflight")

    monkeypatch.setattr(module, "_HARD_LIMITS", limits)
    monkeypatch.setattr(module.json, "loads", forbidden)
    with pytest.raises(ValidationError, match="total state"):
        JoinCheckpoint.from_dict(document)


def test_cell_wire_preflight_stops_before_serializing_wide_aliased_values(monkeypatch):
    document = valid_document()
    document["cells"][0]["values"][0] = ['"' + "x" * 100 + '"'] * 1000
    limit = replace(module._HARD_LIMITS, max_key_bytes=200)
    original = module._json
    visited = []

    def observed(value):
        visited.append(value)
        return original(value)

    monkeypatch.setattr(module, "_HARD_LIMITS", limit)
    monkeypatch.setattr(module, "_json", observed)
    with pytest.raises(ValidationError, match="cell byte"):
        JoinCheckpoint.from_dict(document)
    assert len(visited) <= 3


def test_checkpoint_json_duplicate_keys_input_bytes_invalid_utf8_and_cycles(monkeypatch):
    for raw in ('{"kind":0,"kind":1}', b"\xff", "\ud800", "[" * 2000):
        with pytest.raises(ValidationError):
            JoinCheckpoint.from_json(raw)
    document = valid_document()
    document["cells"][0]["values"][0] = document["cells"]
    with pytest.raises(ValidationError):
        JoinCheckpoint.from_dict(document)
    monkeypatch.setattr(module, "_MAX_DOCUMENT_BYTES", 3)
    for raw in ("1234", "雪雪", b"1234", bytearray(b"{}")):
        with pytest.raises(ValidationError):
            JoinCheckpoint.from_json(raw)


def test_checkpoint_rejects_duplicate_or_nonordered_keys():
    document = valid_document()
    document["processed_inputs"] = [2, 0]
    document["cells"].append(copy.deepcopy(document["cells"][0]))
    with pytest.raises(ValidationError, match="unique sorted"):
        JoinCheckpoint.from_dict(document)
    document["cells"][1]["key"] = "a"
    with pytest.raises(ValidationError, match="unique sorted"):
        JoinCheckpoint.from_dict(document)


@pytest.mark.parametrize(
    "change",
    [
        {"revision": "v2"},
        {"join_id": "other"},
        {"sides": ("right", "left")},
        {"insert_mode": "first"},
        {"emit_mode": "running"},
        {"limits": JoinLimits(max_keys=2)},
    ],
)
def test_snapshot_identity_binds_exact_modes_sides_revision_and_limits(change):
    join = config(emit="final")
    runtime = JoinRuntime(join)
    with pytest.raises(ValidationError, match="identity"):
        JoinRuntime.from_checkpoint(replace(join, **change), runtime.checkpoint())


@pytest.mark.parametrize(
    "case", ["multiple_first", "complete", "draining", "premature_final", "capacity"]
)
def test_restore_checks_semantics_even_with_recomputed_config_identity(case):
    join = config(emit="final")
    doc = valid_document()
    if case == "multiple_first":
        join = config("first", "final")
        doc["processed_inputs"] = [2, 0]
        doc["cells"][0]["values"][0] = ["1", "2"]
    elif case == "complete":
        join = config()
        doc["processed_inputs"] = [1, 1]
        doc["cells"][0]["values"][1] = ["2"]
    elif case == "draining":
        join = config(emit="running")
        doc["closed_sides"] = [True, True]
        doc["phase"] = "draining"
    elif case == "premature_final":
        doc["emitted_rows"] = 1
    else:
        join = config(emit="final", limits=JoinLimits(max_state_bytes=1))
    doc["identity"] = join.identity
    checkpoint = JoinCheckpoint.from_dict(doc)
    with pytest.raises(ValidationError):
        JoinRuntime.from_checkpoint(join, checkpoint)


def test_process_and_drain_counter_overflow_leave_snapshot_unchanged(monkeypatch):
    join = config(emit="running")
    empty = JoinRuntime(join).checkpoint().to_dict()
    empty["processed_inputs"] = [2**53 - 1, 0]
    empty["emitted_rows"] = 2**53 - 1
    empty["cells"] = [{"key": "k", "values": [["1"], []]}]
    runtime = JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_dict(empty))
    before = runtime.checkpoint()
    with pytest.raises(ValidationError):
        runtime.process("right", FlowRecord(1, "k"))
    assert runtime.checkpoint() == before
    runtime = JoinRuntime(config("product", "final"))
    for key in ("a", "b"):
        runtime.process("left", FlowRecord(1, key))
    runtime.close("left")
    runtime.close("right")
    before = runtime.checkpoint()
    # The public retained-value cap prevents actual MAX_COUNT final/product state.
    # Lower the private guard only while draining a reachable two-input snapshot.
    with monkeypatch.context() as patch:
        patch.setattr(module, "_MAX_COUNT", 1)
        with pytest.raises(ValidationError):
            runtime.drain()
    assert runtime.checkpoint() == before


def test_run_pull_cap_early_close_and_eof_leave_caller_source_owned():
    events = []

    def source():
        try:
            for value in range(4):
                events.append(value)
                yield "left", FlowRecord(value, "k")
        finally:
            events.append("closed")

    owned = source()
    runtime = JoinRuntime(config(emit="running"))
    assert len(list(runtime.run(owned, max_inputs=2))) == 2
    assert events == [0, 1]
    pull = runtime.run(owned, max_inputs=2)
    next(pull)
    pull.close()
    assert events == [0, 1, 2]
    assert runtime.phase == "open" and runtime.closed_sides == (False, False)
    owned.close()
    assert events[-1] == "closed"
    assert list(runtime.run(iter(()))) == []
    assert runtime.phase == "open"


@pytest.mark.parametrize(
    "item", [None, [], ("left",), ("left", FlowRecord(1)), ("unknown", FlowRecord(1, "k"))]
)
def test_bad_source_items_do_not_change_state(item):
    runtime = JoinRuntime(config())
    before = runtime.checkpoint()
    with pytest.raises(ValidationError):
        list(runtime.run(iter([item])))
    assert runtime.checkpoint() == before


@pytest.mark.parametrize("value", [0, -1, True, 1.5, 10**1000])
def test_run_and_drain_limits_are_exact_finite_integers(value):
    runtime = JoinRuntime(config())
    with pytest.raises(ValidationError):
        list(runtime.run(iter(()), max_inputs=value))
    with pytest.raises(ValidationError):
        runtime.drain(max_keys=value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("join_id", " x"),
        ("revision", ""),
        ("sides", ["a", "b"]),
        ("sides", ("a", "a")),
        ("sides", ("a", "\ud800")),
        ("insert_mode", "all"),
        ("emit_mode", "latest"),
        ("limits", {}),
    ],
)
def test_bad_join_configuration(field, value):
    with pytest.raises(ValidationError):
        replace(config(), **{field: value})


@pytest.mark.parametrize("field", list(module._CEILINGS))
def test_limits_reject_boolean_and_above_source_ceiling(field):
    for value in (True, module._CEILINGS[field] + 1):
        with pytest.raises(ValidationError):
            replace(JoinLimits(), **{field: value})


def test_impossible_limit_combinations_and_bad_public_rows_batches():
    with pytest.raises(ValidationError):
        JoinLimits(max_rows_per_key=2, max_rows_per_batch=1)
    with pytest.raises(ValidationError):
        JoinLimits(max_row_bytes=2, max_batch_bytes=1)
    for present, values in [
        ((False, False), (None, None)),
        ((True, 0), (1, None)),
        ((True,), (1,)),
        ((True, False), (1, 2)),
    ]:
        with pytest.raises(ValidationError):
            JoinRow("k", ("a", "b"), present, values)
    row = JoinRow("k", ("a", "b"), (True, False), (1, None))
    for rows, phase, pending in [
        ([row], "open", 1),
        ((1,), "open", 1),
        ((), "wrong", 0),
        ((), "closed", 1),
        ((), "draining", 0),
    ]:
        with pytest.raises(ValidationError):
            JoinBatch(rows, phase, pending)
    other = JoinRow("k", ("b", "a"), (True, False), (1, None))
    with pytest.raises(ValidationError):
        JoinBatch((row, other), "open", 1)


def test_to_record_explicitly_obeys_its_stricter_aggregate_node_limit():
    runtime = JoinRuntime(config())
    value = [0] * 60_000
    runtime.process("left", FlowRecord(value, "k"))
    row = runtime.process("right", FlowRecord(value, "k")).rows[0]
    assert len(row.values[0]) == len(row.values[1]) == 60_000
    with pytest.raises(ValidationError, match="value limit"):
        row.to_record()


def test_source_failure_keeps_only_prior_committed_input():
    def source():
        yield "left", FlowRecord(1, "k")
        raise KeyboardInterrupt

    runtime = JoinRuntime(config(emit="final"))
    with pytest.raises(KeyboardInterrupt):
        list(runtime.run(source()))
    assert runtime.checkpoint().processed_inputs == (1, 0)
    assert runtime.closed_sides == (False, False)


def test_public_constructor_boundaries_are_checked_before_large_followup_work(monkeypatch):
    row = JoinRow("k", ("a", "b"), (True, False), (1, None))
    with monkeypatch.context() as patch:
        patch.setattr(module, "_HARD_LIMITS", replace(module._HARD_LIMITS, max_row_bytes=1))
        with pytest.raises(ValidationError, match="row exceeds"):
            JoinRow("k", ("a", "b"), (True, False), (1, None))
    with monkeypatch.context() as patch:
        patch.setattr(
            module,
            "_HARD_LIMITS",
            replace(
                module._HARD_LIMITS, max_row_bytes=row.byte_size, max_batch_bytes=row.byte_size
            ),
        )
        with pytest.raises(ValidationError, match="batch exceeds"):
            JoinBatch((row, row), "open", 1)
    for join in (None, {}, ()):
        with pytest.raises(ValidationError):
            JoinRuntime(join)
    runtime = JoinRuntime(config())
    with pytest.raises(ValidationError):
        runtime.close("missing")
    with pytest.raises(ValidationError):
        JoinRuntime.from_checkpoint(config(), {})


@pytest.mark.parametrize(
    "change",
    [
        {"cells": []},
        {"cells": (("k",),)},
        {"closed_sides": (True, True), "phase": "closed"},
        {"closed_sides": (True, True), "phase": "draining", "cells": ()},
    ],
)
def test_direct_checkpoint_constructor_checks_same_closed_state_contract(change):
    checkpoint = JoinCheckpoint.from_dict(valid_document())
    with pytest.raises(ValidationError):
        replace(checkpoint, **change)


def test_direct_checkpoint_cumulative_admission_before_nested_json_parse(monkeypatch):
    checkpoint = JoinCheckpoint.from_dict(valid_document())
    monkeypatch.setattr(module, "_HARD_LIMITS", replace(module._HARD_LIMITS, max_values=1))
    with pytest.raises(ValidationError, match="total state"):
        replace(checkpoint, processed_inputs=(2, 0), cells=(("k", (("1", "2"), ())),))


def test_checkpoint_shape_rejects_wrong_side_type_and_utf8_bytes(monkeypatch):
    document = valid_document()
    document["cells"][0]["values"][0] = "1"
    with pytest.raises(ValidationError, match="side state"):
        JoinCheckpoint.from_dict(document)
    document = valid_document()
    document["cells"][0]["values"][0] = ['"雪"']
    monkeypatch.setattr(module, "_HARD_LIMITS", replace(module._HARD_LIMITS, max_value_bytes=4))
    with pytest.raises(ValidationError, match="UTF-8 byte"):
        JoinCheckpoint.from_dict(document)


def test_recomputed_checkpoint_output_counter_still_obeys_configured_ceiling():
    join = config(emit="running", limits=JoinLimits(max_rows_per_key=1))
    document = valid_document()
    document.update(identity=join.identity, emitted_rows=2)
    with pytest.raises(ValidationError, match="output count violates"):
        JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_dict(document))


def test_exact_projected_product_output_byte_limit_matches_materialized_oracle():
    sides = ("left", "right")
    expected = expand_reference("k", [["雪", "🙂"], [None, 42, "\\"]])
    sizes = [
        len(
            json.dumps(
                {"present": list(present), "values": list(values)},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        for _, present, values in expected
    ]
    limit = JoinLimits(
        max_rows_per_key=6,
        max_rows_per_batch=6,
        max_row_bytes=max(
            max(sizes),
            len(
                json.dumps(
                    {"present": [True, False], "values": ["🙂", None]},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
        ),
        max_batch_bytes=sum(sizes),
    )
    runtime = JoinRuntime(config("product", "final", limits=limit))
    arrivals = [("left", "雪"), ("left", "🙂"), ("right", None), ("right", 42), ("right", "\\")]
    for side, value in arrivals:
        runtime.process(side, FlowRecord(value, "k"))
    for side in sides:
        runtime.close(side)
    assert plain(runtime.drain().rows) == expected
    denied = JoinRuntime(
        config("product", "final", limits=replace(limit, max_batch_bytes=sum(sizes) - 1))
    )
    for side, value in arrivals[:-1]:
        denied.process(side, FlowRecord(value, "k"))
    before = denied.checkpoint()
    with pytest.raises(ValidationError, match="projected output"):
        denied.process(arrivals[-1][0], FlowRecord(arrivals[-1][1], "k"))
    assert denied.checkpoint() == before


@pytest.mark.parametrize(
    "insert,emit,counts,emitted,phase",
    [
        ("last", "running", [1, 0], 0, "open"),
        ("first", "running", [2, 0], 1, "open"),
        ("product", "running", [2, 0], 1, "open"),
        ("last", "complete", [1, 1], 2, "open"),
        ("first", "complete", [1, 1], 2, "open"),
        ("product", "complete", [1, 0], 1, "open"),
        ("first", "final", [1, 1], 2, "draining"),
        ("last", "final", [1, 1], 2, "draining"),
    ],
)
def test_forged_checkpoint_rejects_impossible_mode_specific_counters(
    insert, emit, counts, emitted, phase
):
    join = config(insert, emit)
    document = valid_document()
    document.update(
        identity=join.identity, processed_inputs=counts, emitted_rows=emitted, phase=phase
    )
    if phase == "draining":
        document["closed_sides"] = [True, True]
    if insert == "product" and emit == "running":
        document["cells"][0]["values"][0] = ["1", "2"]
    checkpoint = JoinCheckpoint.from_dict(document)
    with pytest.raises(ValidationError, match="counter"):
        JoinRuntime.from_checkpoint(join, checkpoint)


@pytest.mark.parametrize(
    "insert,emit,counts,emitted,phase,retained",
    [
        ("product", "running", [2, 0], 2, "open", ["1"]),
        ("product", "final", [2, 0], 0, "open", ["1"]),
        ("first", "running", [1, 0], 1, "open", []),
        ("last", "final", [1, 0], 0, "open", []),
        ("first", "complete", [1, 1], 1, "open", ["1"]),
        ("last", "complete", [1, 1], 1, "open", ["1"]),
        ("product", "complete", [2, 1], 1, "open", ["1", "2"]),
        ("first", "final", [1, 0], 0, "closed", []),
        ("product", "final", [1, 0], 0, "closed", []),
    ],
)
def test_forged_checkpoint_requires_mode_specific_state_conservation(
    insert, emit, counts, emitted, phase, retained
):
    join = config(insert, emit)
    document = valid_document()
    document.update(
        identity=join.identity, processed_inputs=counts, emitted_rows=emitted, phase=phase
    )
    document["cells"] = [{"key": "k", "values": [retained, []]}] if retained else []
    if phase == "closed":
        document["closed_sides"] = [True, True]
    checkpoint = JoinCheckpoint.from_dict(document)
    with pytest.raises(ValidationError, match="counter"):
        JoinRuntime.from_checkpoint(join, checkpoint)


@pytest.mark.parametrize(
    "emit,phase,emitted",
    [
        ("running", "closed", 3),
        ("final", "closed", 1),
        ("final", "draining", 0),
    ],
)
def test_product_history_remains_bounded_after_eof_and_partial_drain(emit, phase, emitted):
    join = config("product", emit, limits=JoinLimits(max_values=2))
    document = valid_document()
    document.update(
        identity=join.identity,
        processed_inputs=[3, 0],
        emitted_rows=emitted,
        closed_sides=[True, True],
        phase=phase,
    )
    if phase == "closed":
        document["cells"] = []
    with pytest.raises(ValidationError, match="lifetime product"):
        JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_dict(document))


@pytest.mark.parametrize(
    "insert,phase,emitted",
    [
        ("first", "closed", 2),
        ("last", "draining", 1),
        ("product", "closed", 3),
        ("product", "draining", 1),
    ],
)
def test_final_history_cannot_emit_more_keys_than_admitted_capacity(insert, phase, emitted):
    join = config(insert, "final", limits=JoinLimits(max_keys=1, max_rows_per_key=2))
    document = valid_document()
    document.update(
        identity=join.identity,
        processed_inputs=[3, 0],
        emitted_rows=emitted,
        closed_sides=[True, True],
        phase=phase,
    )
    if phase == "closed":
        document["cells"] = []
    with pytest.raises(ValidationError, match="possible drained keys"):
        JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_dict(document))


@pytest.mark.parametrize("insert", ["first", "last", "product"])
def test_complete_cycles_can_exceed_retained_value_limit_in_lifetime_inputs(insert):
    join = config(insert, "complete", limits=JoinLimits(max_values=1))
    runtime = JoinRuntime(join)
    for value in range(8):
        assert not runtime.process("left", FlowRecord(value, "k")).rows
        runtime = JoinRuntime.from_checkpoint(join, runtime.checkpoint())
        assert len(runtime.process("right", FlowRecord(value, "k")).rows) == 1
        runtime = JoinRuntime.from_checkpoint(join, runtime.checkpoint())
    assert runtime.checkpoint().processed_inputs == (8, 8)
    assert runtime.checkpoint().emitted_rows == 8


@pytest.mark.parametrize(
    "insert,emit", list(itertools.product(["first", "last"], ["running", "final"]))
)
def test_overwrites_can_exceed_retained_value_limit_in_lifetime_inputs(insert, emit):
    join = config(insert, emit, limits=JoinLimits(max_values=1))
    runtime = JoinRuntime(join)
    for value in range(8):
        runtime.process("left", FlowRecord(value, "k"))
        runtime = JoinRuntime.from_checkpoint(join, runtime.checkpoint())
    runtime.close("left")
    runtime.close("right")
    runtime = JoinRuntime.from_checkpoint(join, runtime.checkpoint())
    if emit == "final":
        assert len(runtime.drain().rows) == 1
        runtime = JoinRuntime.from_checkpoint(join, runtime.checkpoint())
    assert runtime.checkpoint().processed_inputs == (8, 0)


@pytest.mark.parametrize(
    "admitted,retained,emitted",
    [
        (2, ["1"], 0),
        (3, ["1"], 1),
        (2, [], 1),
        (2, ["1", "2"], 1),
    ],
)
def test_final_product_output_counters_obey_consumed_value_conservation(
    admitted, retained, emitted
):
    join = config("product", "final")
    document = valid_document()
    document.update(
        identity=join.identity,
        processed_inputs=[admitted, 0],
        emitted_rows=emitted,
        closed_sides=[True, True],
        phase="draining" if retained else "closed",
    )
    document["cells"] = [{"key": "k", "values": [retained, []]}] if retained else []
    with pytest.raises(ValidationError, match="consumed"):
        JoinRuntime.from_checkpoint(join, JoinCheckpoint.from_dict(document))
