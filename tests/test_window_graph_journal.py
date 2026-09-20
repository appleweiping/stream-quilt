"""Independent command oracle and durable-prefix boundary tests."""

from __future__ import annotations

import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from threading import Barrier

import pytest

import stream_quilt.window_journal as journal_module
from stream_quilt import (
    FlowEdge,
    FlowExecutionError,
    FlowRecord,
    FlowStep,
    FlowWindow,
    RecoveryConflict,
    ValidationError,
    WindowFold,
    WindowFoldLimits,
    WindowGraphDataflow,
    WindowGraphDrain,
    WindowGraphFinish,
    WindowGraphInput,
    WindowGraphJournal,
    WindowGraphOutputCursor,
    WindowGraphRequest,
    WindowGraphRuntime,
    WindowGraphWatermark,
)


def _flow(*, fold=None):
    spec = fold or WindowFold(
        "window",
        "v1",
        width=10,
        initial=lambda: 0,
        fold=lambda state, value: state + value,
        limits=replace(WindowFoldLimits(), max_row_bytes=512, max_batch_bytes=2048),
    )
    return WindowGraphDataflow(
        "journal-window",
        "v1",
        (FlowStep("input", "map", lambda value: value), FlowWindow("window", spec)),
        (FlowEdge("input", "window"),),
        entry="input",
    )


def _request(journal, commands, *, generation=None, request_id=None):
    return WindowGraphRequest(
        journal.journal_id,
        request_id or uuid.uuid4().hex,
        journal.latest().generation if generation is None else generation,
        tuple(commands),
    )


def _commands():
    return (
        WindowGraphInput(0, 2, FlowRecord(3, "a")),
        WindowGraphInput(1, 4, FlowRecord(5, "a")),
        WindowGraphWatermark(10, 2),
        WindowGraphDrain(1),
        WindowGraphFinish(2),
    )


def test_commit_matches_independent_runtime_and_fixed_cursor(tmp_path):
    flow = _flow()
    journal = WindowGraphJournal(tmp_path / "window.sqlite", flow, "0" * 64, "a" * 64)
    runtime = WindowGraphRuntime(flow)
    commands = _commands()
    expected = []
    for command in commands:
        if isinstance(command, WindowGraphInput):
            batch = runtime.process(command)
        elif isinstance(command, WindowGraphWatermark):
            batch = runtime.advance_watermark(
                command.timestamp, next_position=command.next_position
            )
        elif isinstance(command, WindowGraphDrain):
            batch = runtime.drain(max_windows=command.max_windows)
        else:
            batch = runtime.finish(next_position=command.next_position)
        expected.extend((output.step_id, output.record) for output in batch.outputs)
    request = _request(journal, commands)
    receipt = journal.apply(request)
    assert receipt.status == "committed"
    assert receipt.after_operation == 5
    assert receipt.after_position == 2
    assert receipt.after_watermarks == 1
    assert receipt.after_drains == 1
    assert journal.latest().checkpoint == runtime.checkpoint()
    assert journal.request(request.request_id) == receipt
    assert journal.apply(request) == receipt
    assert journal.operation(4).cause == "drain"
    cursor = journal.output_cursor()
    page = journal.read_outputs(cursor, limit=1)
    assert [(output.step_id, output.record) for output in page.outputs] == expected
    assert page.cursor.next_sequence == page.cursor.stop_sequence == 1
    assert journal.read_outputs(page.cursor).outputs == ()
    assert WindowGraphOutputCursor.from_json(cursor.to_json()) == cursor
    reopened = WindowGraphJournal(journal.path, flow, "0" * 64, "a" * 64, create=False)
    assert reopened.latest() == journal.latest()
    assert reopened.read_outputs(cursor).outputs == page.outputs


def test_no_op_is_unpersisted_and_rechecked_after_head_change(tmp_path):
    journal = WindowGraphJournal(tmp_path / "window.sqlite", _flow(), "0" * 64, "b" * 64)
    same = _request(journal, (), request_id="1" * 32)
    receipt = journal.apply(same)
    assert receipt.status == "no_op"
    assert journal.latest().generation == 0
    assert journal.request(same.request_id) is None
    assert journal.apply(_request(journal, (WindowGraphDrain(),))).status == "no_op"
    committed = journal.apply(_request(journal, (WindowGraphWatermark(0, 0),), request_id="2" * 32))
    assert committed.after_generation == 1
    with pytest.raises(RecoveryConflict):
        journal.apply(same)
    with pytest.raises(RecoveryConflict):
        journal.apply(_request(journal, (WindowGraphFinish(0),), request_id="2" * 32))


def test_invalid_batch_and_callback_failure_leave_all_tables_unchanged(tmp_path):
    def explode(_state, _value):
        raise RuntimeError("fold callback failed")

    flow = _flow(fold=WindowFold("window", "v1", width=10, initial=lambda: 0, fold=explode))
    journal = WindowGraphJournal(tmp_path / "window.sqlite", flow, "0" * 64, "c" * 64)
    before = journal.latest()
    invalid = _request(
        journal,
        (WindowGraphInput(0, 1, FlowRecord(1)), WindowGraphInput(2, 2, FlowRecord(2))),
    )
    with pytest.raises(ValidationError):
        journal.apply(invalid)
    with pytest.raises(FlowExecutionError, match="window"):
        journal.apply(_request(journal, (WindowGraphInput(0, 1, FlowRecord(1)),)))
    assert journal.latest() == before
    with sqlite3.connect(journal.path) as connection:
        for table in ("window_commit", "window_operation", "window_output"):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)


def test_source_commitment_and_flow_are_checked_on_reopen(tmp_path):
    path = tmp_path / "window.sqlite"
    flow = _flow()
    journal = WindowGraphJournal(path, flow, "0" * 64, "d" * 64)
    journal.apply(_request(journal, (WindowGraphWatermark(1, 0),)))
    with pytest.raises(ValidationError, match="mismatch"):
        WindowGraphJournal(path, flow, "0" * 64, "e" * 64, create=False)
    with pytest.raises(ValidationError, match="mismatch"):
        WindowGraphJournal(path, flow, "f" * 64, "d" * 64, create=False)
    other_flow = replace(flow, revision="v2")
    with pytest.raises(ValidationError, match="mismatch"):
        WindowGraphJournal(path, other_flow, "0" * 64, "d" * 64, create=False)


def test_page_cursor_is_fixed_across_later_commits_and_has_byte_budget(tmp_path):
    journal = WindowGraphJournal(tmp_path / "window.sqlite", _flow(), "0" * 64, "a" * 64)
    first = _request(
        journal,
        (
            WindowGraphInput(0, 1, FlowRecord(1, "a")),
            WindowGraphInput(1, 1, FlowRecord(2, "b")),
            WindowGraphWatermark(10, 2),
            WindowGraphDrain(2),
        ),
    )
    journal.apply(first)
    old = journal.output_cursor()
    assert old.stop_sequence == 2
    first_page = journal.read_outputs(old, limit=1)
    assert len(first_page.outputs) == 1
    assert len(journal.read_outputs(first_page.cursor).outputs) == 1
    with pytest.raises(ValidationError, match="byte budget"):
        journal.read_outputs(old, max_bytes=1)
    journal.apply(
        _request(
            journal,
            (
                WindowGraphInput(2, 11, FlowRecord(3, "c")),
                WindowGraphWatermark(20, 3),
                WindowGraphDrain(1),
            ),
        )
    )
    assert old.stop_sequence == 2
    assert journal.output_cursor().stop_sequence == 3
    assert len(journal.read_outputs(old).outputs) == 2
    assert journal.read_outputs(first_page.cursor).cursor.stop_sequence == 2
    with pytest.raises(ValidationError):
        journal.read_outputs(replace(old, anchor_receipt_digest="0" * 64))


@pytest.mark.parametrize("identical_request", [False, True])
def test_competing_writers_publish_one_generation_without_callback_replay(
    tmp_path, identical_request
):
    barrier = Barrier(2, timeout=15)
    calls = []

    def fold(state, value):
        calls.append(value)
        barrier.wait()
        return state + value

    spec = WindowFold("window", "v1", width=10, initial=lambda: 0, fold=fold)
    journal = WindowGraphJournal(tmp_path / "window.sqlite", _flow(fold=spec), "0" * 64, "a" * 64)
    common_id = uuid.uuid4().hex
    requests = [
        _request(
            journal,
            (WindowGraphInput(0, 1, FlowRecord(1, "a")),),
            generation=0,
            request_id=common_id if identical_request else uuid.uuid4().hex,
        )
        for _ in range(2)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(journal.apply, request) for request in requests]
        results = []
        for future in futures:
            try:
                results.append(future.result(timeout=30))
            except RecoveryConflict as error:
                results.append(error)
    assert len(calls) == 2  # Local CAS does not promise callback exactly-once.
    assert journal.latest().generation == 1
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT count(*) FROM window_commit").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM window_operation").fetchone() == (1,)
    if identical_request:
        assert results[0] == results[1]
    else:
        assert sum(isinstance(result, RecoveryConflict) for result in results) == 1


def test_request_and_stored_head_corruption_are_rejected(tmp_path):
    journal = WindowGraphJournal(tmp_path / "window.sqlite", _flow(), "0" * 64, "a" * 64)
    request = _request(journal, (WindowGraphWatermark(1, 0),))
    with pytest.raises(ValidationError):
        WindowGraphRequest.from_json(
            request.to_json().replace('"version":"1.0"', '"version":"1.0","version":"1.0"')
        )
    journal.apply(request)
    with sqlite3.connect(journal.path) as connection:
        payload = connection.execute("SELECT payload FROM window_head").fetchone()[0]
        bad = payload.replace('"generation":1', '"generation":0')
        assert bad != payload
        connection.execute(
            "UPDATE window_head SET payload=?, digest=?", (bad, sha256(bad.encode()).hexdigest())
        )
    with pytest.raises(ValidationError):
        journal.latest()


def test_nonempty_drain_with_zero_output_is_still_durable(tmp_path):
    fold = WindowFold("window", "v1", width=10, initial=lambda: 0, fold=lambda a, b: a + b)
    flow = WindowGraphDataflow(
        "filtered-window",
        "v1",
        (
            FlowStep("input", "map", lambda value: value),
            FlowWindow("window", fold),
            FlowStep("hidden", "filter", lambda _row: False),
        ),
        (FlowEdge("input", "window"), FlowEdge("window", "hidden")),
        entry="input",
    )
    journal = WindowGraphJournal(tmp_path / "j.sqlite", flow, "0" * 64, "a" * 64)
    request = _request(
        journal,
        (
            WindowGraphInput(0, 1, FlowRecord(4, "k")),
            WindowGraphWatermark(10, 1),
            WindowGraphDrain(1),
        ),
    )
    receipt = journal.apply(request)
    assert receipt.after_operation == 3
    assert receipt.after_drains == 1
    assert receipt.output_stop == 0
    assert journal.operation(3).cause == "drain"
    assert journal.operation(3).drained_windows == 1
    assert journal.read_outputs(journal.output_cursor()).outputs == ()


def test_invalid_source_commitment_is_rejected_before_filesystem_io(tmp_path):
    path = tmp_path / "not-created" / "j.sqlite"
    with pytest.raises(ValidationError):
        WindowGraphJournal(path, _flow(), "0" * 64, "INVALID")
    assert not path.parent.exists()


def test_duplicate_command_index_and_output_payload_are_detected(tmp_path):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    request = _request(journal, _commands())
    journal.apply(request)
    with sqlite3.connect(journal.path) as connection:
        row = connection.execute("SELECT payload FROM window_operation WHERE seq=2").fetchone()
        assert row is not None
        payload = row[0]
        bad = payload.replace('"command_index":1', '"command_index":0')
        assert bad != payload
        # The table UNIQUE constraint prevents duplicate SQL keys, but a
        # matching checksum cannot conceal contradictory payload content.
        connection.execute(
            "UPDATE window_operation SET payload=?, digest=? WHERE seq=2",
            (bad, sha256(bad.encode()).hexdigest()),
        )
    with pytest.raises(ValidationError):
        journal.operation(2)
    with sqlite3.connect(journal.path) as connection:
        connection.execute(
            "UPDATE window_operation SET payload=?, digest=? WHERE seq=2",
            (payload, sha256(payload.encode()).hexdigest()),
        )
        output = connection.execute("SELECT payload FROM window_output WHERE seq=0").fetchone()[0]
        changed = output.replace('"step_id":"window"', '"step_id":"input"')
        assert changed != output
        connection.execute(
            "UPDATE window_output SET payload=?, digest=? WHERE seq=0",
            (changed, sha256(changed.encode()).hexdigest()),
        )
    with pytest.raises(ValidationError):
        journal.read_outputs(journal.output_cursor())


def test_known_capacity_rejection_precedes_fold_callback_and_allows_no_op(tmp_path, monkeypatch):
    calls = []

    def fold(state, value):
        calls.append(value)
        return state + value

    spec = WindowFold("window", "v1", width=10, initial=lambda: 0, fold=fold)
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(fold=spec), "0" * 64, "a" * 64)
    journal.apply(_request(journal, (WindowGraphInput(0, 1, FlowRecord(1, "k")),)))
    assert calls == [1]
    with monkeypatch.context() as scoped:
        scoped.setattr(journal_module, "_MAX_HISTORY", 1)
        with pytest.raises(ValidationError, match="capacity"):
            journal.apply(_request(journal, (WindowGraphInput(1, 2, FlowRecord(2, "k")),)))
        assert journal.apply(_request(journal, (WindowGraphDrain(1),))).status == "no_op"
    assert calls == [1]
    assert journal.latest().generation == 1


@pytest.mark.parametrize(
    "command",
    [
        WindowGraphInput(0, 2, FlowRecord({"n": 3}, "clé")),
        WindowGraphWatermark(10, 0),
        WindowGraphFinish(0),
        WindowGraphDrain(2),
    ],
)
def test_each_public_command_roundtrips_through_strict_request_wire(command):
    request = WindowGraphRequest("a" * 32, "b" * 32, 0, (command,))
    assert WindowGraphRequest.from_dict(request.to_dict()) == request
    assert WindowGraphRequest.from_json(request.to_json()) == request
    assert WindowGraphRequest.from_json(request.to_json().encode("utf-8")) == request
    assert WindowGraphRequest.from_json(request.to_json()).digest == request.digest


def test_parsed_mixed_request_applies_with_same_receipt_and_no_callback_replay(tmp_path):
    calls = []

    def fold(state, value):
        calls.append(value)
        return state + value

    flow = _flow(fold=WindowFold("window", "v1", width=10, initial=lambda: 0, fold=fold))
    journal = WindowGraphJournal(tmp_path / "j.sqlite", flow, "0" * 64, "a" * 64)
    original = _request(
        journal,
        (
            WindowGraphInput(0, 2, FlowRecord(3, "k")),
            WindowGraphWatermark(10, 1),
            WindowGraphDrain(1),
            WindowGraphFinish(1),
        ),
    )
    parsed = WindowGraphRequest.from_json(original.to_json().encode("utf-8"))
    assert parsed == original
    receipt = journal.apply(parsed)
    assert receipt.after_operation == 4
    assert receipt.output_stop == 1
    assert journal.apply(original) == receipt
    assert calls == [3]


@pytest.mark.parametrize(
    "change",
    [
        lambda value: value.update(commands=()),
        lambda value: value["commands"].__setitem__(0, "process"),
        lambda value: value["commands"][0].pop("position"),
        lambda value: value["commands"][0].update(position=True),
        lambda value: value["commands"][0]["record"].update(key=7),
        lambda value: value["commands"][0]["record"].update(encoded_value="[1, 2]"),
        lambda value: value["commands"][0]["record"].update(encoded_value="NaN"),
        lambda value: value["commands"][0].update(cause="unknown"),
        lambda value: value["commands"][0].update(unexpected=1),
        lambda value: value.update(expected_generation=True),
    ],
)
def test_request_parser_rejects_hostile_command_documents(change):
    request = WindowGraphRequest(
        "a" * 32, "b" * 32, 0, (WindowGraphInput(0, 2, FlowRecord(3, "k")),)
    )
    document = deepcopy(request.to_dict())
    change(document)
    with pytest.raises(ValidationError):
        WindowGraphRequest.from_dict(document)


@pytest.mark.parametrize(
    "payload",
    [
        lambda text: text + " ",
        lambda text: text.replace('"version":"1.0"', '"version":"1.0","version":"1.0"'),
        lambda _text: b"\xff",
    ],
)
def test_request_parser_rejects_noncanonical_duplicate_or_bad_utf8(payload):
    request = WindowGraphRequest("a" * 32, "b" * 32, 0, (WindowGraphFinish(0),))
    with pytest.raises(ValidationError):
        WindowGraphRequest.from_json(payload(request.to_json()))


def test_open_requires_existing_exact_schema_and_boolean_create(tmp_path):
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(ValidationError, match="does not exist"):
        WindowGraphJournal(missing, _flow(), "0" * 64, "a" * 64, create=False)
    assert not missing.exists()
    with pytest.raises(ValidationError, match="boolean"):
        WindowGraphJournal(missing, _flow(), "0" * 64, "a" * 64, create=1)
    assert not missing.exists()
    unrelated = tmp_path / "unrelated.sqlite"
    with sqlite3.connect(unrelated) as connection:
        connection.execute("CREATE TABLE unrelated (value INTEGER)")
    with pytest.raises(ValidationError, match="unrelated"):
        WindowGraphJournal(unrelated, _flow(), "0" * 64, "a" * 64, create=False)


@pytest.mark.parametrize("tamper", ["application_id", "extra_table"])
def test_schema_substitution_is_rejected_on_each_read(tmp_path, tamper):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    with sqlite3.connect(journal.path) as connection:
        if tamper == "application_id":
            connection.execute("PRAGMA application_id = 0")
        else:
            connection.execute("CREATE TABLE extra (value INTEGER)")
    with pytest.raises(ValidationError):
        journal.latest()


def test_receipt_sql_key_mismatch_is_rejected_even_with_matching_row_hash(tmp_path):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    request = _request(journal, (WindowGraphWatermark(1, 0),))
    journal.apply(request)
    with sqlite3.connect(journal.path) as connection:
        payload = connection.execute("SELECT payload FROM window_commit").fetchone()[0]
        bad = payload.replace(request.request_id, "0" * 32)
        assert bad != payload
        connection.execute(
            "UPDATE window_commit SET payload=?, digest=?",
            (bad, sha256(bad.encode()).hexdigest()),
        )
    with pytest.raises(ValidationError, match="receipt"):
        journal.request(request.request_id)


def test_oversized_receipt_is_rejected_before_sql_payload_materialization(tmp_path):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    journal.apply(_request(journal, (WindowGraphWatermark(1, 0),)))
    oversized = "x" * (256 * 1024 + 1)
    with sqlite3.connect(journal.path) as connection:
        connection.execute(
            "UPDATE window_commit SET payload=?, digest=?",
            (oversized, sha256(oversized.encode()).hexdigest()),
        )
    with pytest.raises(ValidationError):
        journal.latest()


def test_output_sql_owner_and_cursor_wire_bounds_are_rejected(tmp_path):
    journal = WindowGraphJournal(tmp_path / "j.sqlite", _flow(), "0" * 64, "a" * 64)
    journal.apply(_request(journal, _commands()))
    cursor = journal.output_cursor()
    with pytest.raises(ValidationError):
        WindowGraphOutputCursor.from_json(cursor.to_json() + " ")
    with pytest.raises(ValidationError):
        WindowGraphOutputCursor.from_json(
            cursor.to_json().replace('"next_sequence":0', '"next_sequence":2')
        )
    with pytest.raises(ValidationError):
        journal.output_cursor(start=2)
    with pytest.raises(ValidationError):
        journal.read_outputs(cursor, limit=0)
    with pytest.raises(ValidationError):
        journal.read_outputs(cursor, max_bytes=0)
    with sqlite3.connect(journal.path) as connection:
        connection.execute("UPDATE window_output SET operation_seq=1 WHERE seq=0")
    with pytest.raises(ValidationError, match="SQL key"):
        journal.read_outputs(cursor)
