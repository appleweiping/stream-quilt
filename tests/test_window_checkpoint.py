"""Hostile new-wire boundaries and necessary window-history invariants."""

import copy
import hashlib
import json
from dataclasses import replace

import pytest

import stream_quilt.window_fold as module
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


def reseal(document, configuration=False):
    if configuration:
        document["body"]["identity"] = digest(document["body"]["configuration"])
    document["sha256"] = digest(document["body"])
    return document


def spec(**kwargs):
    return WindowFold(
        "checkpoint",
        "v1",
        width=5,
        initial=lambda: 0,
        fold=lambda state, value: state + value,
        **kwargs,
    )


def checkpoint():
    runtime = WindowFoldRuntime(spec())
    runtime.process(0, FlowRecord(1, "a"))
    runtime.process(1, FlowRecord(2, "a"))
    runtime.process(0, FlowRecord(3, "b"))
    return runtime.checkpoint()


@pytest.mark.parametrize(
    "field,value",
    [
        ("processed_inputs", 2),
        ("processed_inputs", True),
        ("processed_inputs", 2**53),
        ("processed_inputs", -1),
        ("processed_inputs", 1.0),
        ("late_drops", 1),
        ("gap_inputs", 1),
        ("membership_updates", 4),
        ("created_windows", 1),
        ("emitted_windows", 1),
        ("finalized_memberships", 1),
    ],
)
def test_rehashed_counter_corruption(field, value):
    document = checkpoint().to_dict()
    document["body"]["counters"][field] = value
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(reseal(document))


@pytest.mark.parametrize(
    "field,value",
    [
        ("key", " a"),
        ("key", None),
        ("key", "a" * 1025),
        ("index", True),
        ("index", 1.0),
        ("index", 2**53),
        ("index", 1),
        ("start", False),
        ("start", -1),
        ("end", 6),
        ("end", 2**53),
        ("input_count", 0),
        ("input_count", 4),
        ("input_count", 1.0),
        ("state", None),
        ("state", "\ud800"),
    ],
)
def test_rehashed_cell_corruption(field, value):
    document = checkpoint().to_dict()
    document["body"]["cells"][0][field] = value
    if field == "state" and value == "\ud800":
        with pytest.raises(ValidationError, match="Unicode"):
            WindowCheckpoint.from_dict(document)
    else:
        with pytest.raises(ValidationError):
            WindowCheckpoint.from_dict(reseal(document))


@pytest.mark.parametrize(
    "encoded",
    [
        " 3",
        "3.00",
        "3e0",
        "-0",
        "NaN",
        "1e999",
        "{} trailing",
        "[1,]",
        '{"a":1,"a":2}',
        '{"z":1,"a":2}',
        '"\\ud800"',
        "9007199254740992",
        "9" * 5000,
        "[" * 80 + "0" + "]" * 80,
    ],
)
def test_strict_canonical_nested_state(encoded):
    document = checkpoint().to_dict()
    document["body"]["cells"][0]["state"] = encoded
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(reseal(document))


@pytest.mark.parametrize("action", ["missing", "extra", "duplicate", "reverse", "tuple", "empty"])
def test_closed_cell_shapes_and_order(action):
    document = checkpoint().to_dict()
    cells = document["body"]["cells"]
    if action == "missing":
        cells[0].pop("state")
    elif action == "extra":
        cells[0]["extra"] = 1
    elif action == "duplicate":
        cells.append(copy.deepcopy(cells[0]))
    elif action == "reverse":
        cells.reverse()
    elif action == "tuple":
        document["body"]["cells"] = tuple(cells)
    else:
        cells.clear()
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(reseal(document))


@pytest.mark.parametrize("target", ["envelope", "body", "configuration", "limits", "counters"])
@pytest.mark.parametrize("action", ["missing", "extra"])
def test_closed_document_shapes(target, action):
    document = checkpoint().to_dict()
    parts = {
        "envelope": document,
        "body": document["body"],
        "configuration": document["body"]["configuration"],
        "limits": document["body"]["configuration"]["limits"],
        "counters": document["body"]["counters"],
    }
    selected = parts[target]
    if action == "missing":
        selected.pop(next(iter(selected)))
    else:
        selected["extra"] = True
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(document)


@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", "stream-quilt-join-checkpoint"),
        ("version", "2.0"),
        ("identity", "0" * 64),
        ("phase", "closed"),
        ("phase", "draining"),
        ("phase", None),
        ("watermark", True),
        ("watermark", 5),
        ("watermark", 2**53),
        ("finished", 1),
        ("finished", True),
    ],
)
def test_rehashed_header_corruption(field, value):
    document = checkpoint().to_dict()
    document["body"][field] = value
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(reseal(document))


@pytest.mark.parametrize(
    "field,value",
    [
        ("hop", None),
        ("hop", True),
        ("width", 0),
        ("order", "timestamp"),
        ("finalizer", 1),
        ("late_policy", "accept"),
        ("origin", 1),
        ("tick_unit", " tick"),
    ],
)
def test_rehashed_configuration_corruption(field, value):
    document = checkpoint().to_dict()
    document["body"]["configuration"][field] = value
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(reseal(document, configuration=True))


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_windows", 1),
        ("max_keys", 1),
        ("max_inputs", 2),
        ("max_state_value_bytes", 1),
        ("max_state_bytes", 1),
        ("max_row_bytes", 1),
    ],
)
def test_lowered_bound_is_rechecked_even_with_recomputed_identity(field, value):
    document = checkpoint().to_dict()
    limits = document["body"]["configuration"]["limits"]
    limits[field] = value
    if field == "max_windows":
        limits["max_keys"] = 1
    if field == "max_state_value_bytes":
        document["body"]["cells"][0]["state"] = "10"
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(reseal(document, configuration=True))


def test_configuration_revision_and_limit_mismatch_on_restore():
    for changed in (
        replace(spec(), revision="v2"),
        replace(spec(), tick_unit="ms"),
        replace(spec(), limits=replace(WindowFoldLimits(), max_inputs=100)),
    ):
        with pytest.raises(ValidationError, match="identity mismatch"):
            WindowFoldRuntime.from_checkpoint(changed, checkpoint())


def test_restore_never_calls_supplied_callbacks():
    def forbidden(*args):
        pytest.fail("callback during restore")

    actual = replace(spec(), initial=forbidden, fold=forbidden)
    restored = WindowFoldRuntime.from_checkpoint(actual, checkpoint())
    assert restored.checkpoint() == checkpoint()


@pytest.mark.parametrize("payload", [None, bytearray(b"{}"), b"\xff", "{}", "[]", "null", "NaN"])
def test_invalid_outer_json(payload):
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_json(payload)


def test_outer_canonical_bytes_and_checksum():
    text = checkpoint().to_json()
    for value in (" " + text, text + "\n", text.replace('"body":', '"body":{} ,"body":', 1)):
        with pytest.raises(ValidationError):
            WindowCheckpoint.from_json(value)
    document = checkpoint().to_dict()
    document["sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="checksum"):
        WindowCheckpoint.from_dict(document)
    document["sha256"] = "A" * 64
    with pytest.raises(ValidationError, match="digest"):
        WindowCheckpoint.from_dict(document)


@pytest.mark.parametrize("payload", ["x" * 21, b"x" * 21, "😀" * 10])
def test_raw_byte_limits_precede_json_parsing(monkeypatch, payload):
    monkeypatch.setattr(module, "_MAX_WIRE", 20)

    def forbidden(*args):
        pytest.fail("JSON parsing before byte admission")

    monkeypatch.setattr(module, "_load", forbidden)
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_json(payload)


@pytest.mark.parametrize("malformed", ["aggregate", "phase", "counter", "too_many"])
def test_all_known_checkpoint_admission_precedes_nested_parse(monkeypatch, malformed):
    document = checkpoint().to_dict()
    if malformed == "aggregate":
        document["body"]["configuration"]["limits"]["max_state_bytes"] = 1
    elif malformed == "phase":
        document["body"]["phase"] = "closed"
    elif malformed == "counter":
        document["body"]["counters"]["membership_updates"] = 5
    else:
        document["body"]["configuration"]["limits"].update(max_windows=1, max_keys=1)
    reseal(document, configuration=True)

    def forbidden(*args):
        pytest.fail("nested JSON parsing before cumulative/counter/phase admission")

    monkeypatch.setattr(module, "_checked_value", forbidden)
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(document)


@pytest.mark.parametrize("value", [None, 1, "{}", " " * 100])
def test_forged_frozen_checkpoint_is_not_trusted(value):
    forged = checkpoint()
    object.__setattr__(forged, "_json", value)
    with pytest.raises(ValidationError):
        WindowFoldRuntime.from_checkpoint(spec(), forged)


def test_forged_parseable_noncanonical_checkpoint_rejects():
    forged = checkpoint()
    object.__setattr__(forged, "_json", " " + forged.to_json())
    with pytest.raises(ValidationError, match="canonical"):
        forged.to_dict()


def test_canonical_finite_float_state_is_not_coerced_to_int():
    document = checkpoint().to_dict()
    document["body"]["cells"][0]["state"] = "3.0"
    accepted = WindowCheckpoint.from_dict(reseal(document))
    assert accepted.to_dict()["body"]["cells"][0]["state"] == "3.0"


def test_membership_counter_overflow_rejected_before_callbacks():
    maximum = 2**53 - 1
    folded = maximum // 2
    limits = replace(WindowFoldLimits(), max_inputs=maximum)
    original = WindowFold(
        "huge-history",
        "v1",
        width=2,
        hop=1,
        initial=lambda: 0,
        fold=lambda state, value: 0,
        limits=limits,
    )
    runtime = WindowFoldRuntime(original)
    runtime.process(0, FlowRecord(0, "a"))
    document = runtime.checkpoint().to_dict()
    document["body"]["counters"].update(processed_inputs=folded, membership_updates=2 * folded)
    for cell in document["body"]["cells"]:
        cell["input_count"] = folded
    checked = WindowCheckpoint.from_dict(reseal(document))

    def forbidden(*args):
        pytest.fail("callback before counter overflow admission")

    actual = replace(original, initial=forbidden, fold=forbidden)
    runtime = WindowFoldRuntime.from_checkpoint(actual, checked)
    before = runtime.checkpoint()
    with pytest.raises(ValidationError, match="membership updates"):
        runtime.process(0, FlowRecord(0, "a"))
    assert runtime.checkpoint() == before


def test_counter_constraints_for_finalized_and_empty_histories():
    runtime = WindowFoldRuntime(spec())
    runtime.process(0, FlowRecord(1, "a"))
    runtime.process(5, FlowRecord(2, "a"))
    runtime.finish()
    runtime.drain(max_windows=1)
    good = runtime.checkpoint().to_dict()
    mutations = [
        {"created_windows": 0},
        {"emitted_windows": 0, "created_windows": 1},
        {"finalized_memberships": 0, "membership_updates": 1},
        {"processed_inputs": 0},
        {"finalized_memberships": 3, "membership_updates": 4, "processed_inputs": 4},
    ]
    # Last mutation is structurally possible: two admitted contributions may have
    # been added to the consumed window. Checks are necessary, not authentication.
    for update in mutations[:-1]:
        bad = copy.deepcopy(good)
        bad["body"]["counters"].update(update)
        with pytest.raises(ValidationError):
            WindowCheckpoint.from_dict(reseal(bad))
    plausible = copy.deepcopy(good)
    plausible["body"]["counters"].update(mutations[-1])
    assert WindowCheckpoint.from_dict(reseal(plausible))


def test_exact_type_strings_and_containers_reject_subclasses():
    class Text(str):
        pass

    class Mapping(dict):
        pass

    class Sequence(list):
        pass

    for target, value in [
        ("kind", Text("stream-quilt-window-checkpoint")),
        ("version", Text("1.0")),
        ("cells", Sequence()),
    ]:
        document = checkpoint().to_dict()
        document["body"][target] = value
        with pytest.raises(ValidationError):
            WindowCheckpoint.from_dict(reseal(document))
    with pytest.raises(ValidationError):
        WindowCheckpoint.from_dict(Mapping(checkpoint().to_dict()))
