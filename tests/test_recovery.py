from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from stream_quilt import (
    AlignerCheckpoint,
    AlignmentConfig,
    Event,
    RecoveryConflict,
    RecoveryStore,
    RetentionPolicy,
    TrackedCheckpoint,
    ValidationError,
    WatermarkAligner,
    resume_events,
)
from stream_quilt.cli import main
from stream_quilt.errors import OutputError

SOURCE = hashlib.sha256(b"test-source").hexdigest()


def source() -> tuple[Event, ...]:
    return tuple(
        Event(f"e{i}", "sensor", "text", time, duration, {"value": i})
        for i, (time, duration) in enumerate(((0, 14), (5, 0), (13, 3), (27, 0), (15, 20), (45, 1)))
    )


def config() -> AlignmentConfig:
    return AlignmentConfig(
        window_ms=10,
        hop_ms=5,
        required_streams=("sensor",),
        allowed_lateness_ms=3,
        late_policy="accept",
        offsets_ms={"sensor": 1.5},
    )


def oracle() -> list[dict]:
    aligner = WatermarkAligner(config())
    windows = [window for event in source() for window in aligner.ingest(event)]
    windows.extend(aligner.flush())
    return [window.to_dict() for window in windows]


@pytest.mark.parametrize("split", range(7))
@pytest.mark.parametrize("batch_size", [1, 2, 100])
def test_resume_matches_uninterrupted_oracle(tmp_path: Path, split: int, batch_size: int) -> None:
    path = tmp_path / "recovery.db"
    first = resume_events(source(), config(), path, batch_size=batch_size, max_new_events=split)
    assert first.position == split
    finished = resume_events(source(), config(), path, batch_size=batch_size)
    assert finished.position == 6 and finished.checkpoint.closed
    assert list(RecoveryStore(path).window_documents()) == oracle()
    # Repeating a completed run neither appends output nor advances the generation.
    assert resume_events(source(), config(), path) == finished


def test_checkpoint_cross_process_json_and_retention(tmp_path: Path) -> None:
    cfg = AlignmentConfig(window_ms=10, hop_ms=10, required_streams=("s",))
    retention = RetentionPolicy(20, 4)
    aligner = WatermarkAligner(cfg, retention=retention)
    for i in range(8):
        aligner.ingest(Event(str(i), "s", "text", i * 20))
    snapshot = aligner.checkpoint()
    assert snapshot.released_count > 0
    encoded = json.dumps(snapshot.to_dict(), allow_nan=False)
    code = (
        "from stream_quilt import AlignerCheckpoint; import sys,json; "
        "print(json.dumps(AlignerCheckpoint.from_dict(json.load(sys.stdin)).to_dict(), "
        "allow_nan=False))"
    )
    child = subprocess.run(
        [sys.executable, "-c", code],
        input=encoded,
        text=True,
        capture_output=True,
        check=True,
        timeout=20,
    )
    parsed = AlignerCheckpoint.from_dict(json.loads(child.stdout))
    restored = WatermarkAligner.from_checkpoint(cfg, parsed)
    assert restored.retention == retention
    event = Event("future", "s", "text", 200)
    assert restored.ingest(event) == aligner.ingest(event)
    assert restored.flush() == aligner.flush()
    with pytest.raises(ValidationError, match="retention"):
        WatermarkAligner.from_checkpoint(cfg, parsed, retention=RetentionPolicy(30, 4))


@pytest.mark.parametrize(
    "field,value",
    [
        ("next_start", True),
        ("next_start", math.nan),
        ("next_start", math.inf),
        ("next_index", True),
        ("next_index", 100001),
        ("max_seen", {1: 2}),
        ("max_seen", {"s": "1"}),
        ("max_seen", {"s": math.nan}),
        ("retention", {}),
        ("released_count", -1),
        ("released_reported_count", 1),
        ("assigned_event_ids", ("unknown",)),
        ("closed", 1),
    ],
)
def test_checkpoint_rejects_malformed_models(field: str, value: object) -> None:
    point = WatermarkAligner(config()).checkpoint()
    with pytest.raises(ValidationError):
        replace(point, **{field: value})


@pytest.mark.parametrize("status", [-1, 3, True, "0"])
def test_tracked_status_is_strict(status: object) -> None:
    with pytest.raises(ValidationError):
        TrackedCheckpoint("e", 1, status)  # type: ignore[arg-type]


def test_checkpoint_copies_collections_and_checks_cross_fields() -> None:
    aligner = WatermarkAligner(config())
    aligner.ingest(source()[0])
    original = aligner.checkpoint()
    events, tracked = list(original.live_events), list(original.seen_order)
    point = replace(original, live_events=events, seen_order=tracked)  # type: ignore[arg-type]
    events.clear()
    tracked.clear()
    assert len(point.live_events) == len(point.seen_order) == 1
    for values in (
        {"live_events": original.live_events * 2},
        {"seen_order": original.seen_order * 2},
        {"seen_order": ()},
        {"max_seen": {"sensor": 0}},
        {"closed": True},
    ):
        with pytest.raises(ValidationError):
            replace(original, **values)
    with pytest.raises(ValidationError, match="window index"):
        WatermarkAligner.from_checkpoint(config(), replace(original, next_start=99))
    bad_horizon = replace(original.seen_order[0], horizon_ms=999)
    with pytest.raises(ValidationError, match="horizon"):
        WatermarkAligner.from_checkpoint(config(), replace(original, seen_order=(bad_horizon,)))


@pytest.mark.parametrize(
    "change",
    [
        {"extra": 1},
        {"schema_version": "1.0"},
        {"kind": "wrong"},
        {"live_events": "wrong"},
        {"seen_order": [42]},
        {"seen_order": [{}]},
        {"retention": {}},
        {"retention": None},
    ],
)
def test_checkpoint_strict_deserialize(change: dict) -> None:
    doc = WatermarkAligner(config()).checkpoint().to_dict()
    doc.update(change)
    with pytest.raises(ValidationError):
        AlignerCheckpoint.from_dict(doc)
    with pytest.raises(ValidationError):
        AlignerCheckpoint.from_dict([])


def test_infinite_exclusive_horizon_has_json_representation() -> None:
    record = TrackedCheckpoint("largest-point", math.inf, 0)
    point = replace(WatermarkAligner(config()).checkpoint(), seen_order=(record,))
    encoded = json.dumps(point.to_dict(), allow_nan=False)
    assert AlignerCheckpoint.from_dict(json.loads(encoded)).seen_order == (record,)


def test_only_one_concurrent_writer_wins(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "cas.db")
    checkpoint = WatermarkAligner(config()).checkpoint()

    def save() -> bool:
        try:
            store.commit(checkpoint, input_id=SOURCE, position=0, expected_generation=0)
            return True
        except RecoveryConflict:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: save(), range(2)))
    assert sorted(outcomes) == [False, True]
    assert store.load().generation == 1  # type: ignore[union-attr]


def test_transaction_failure_rolls_back_output_state_and_offset(tmp_path: Path) -> None:
    path = tmp_path / "atomic.db"
    store = RecoveryStore(path)
    cfg = AlignmentConfig(10, 10)
    aligner = WatermarkAligner(cfg)
    before = store.commit(aligner.checkpoint(), input_id=SOURCE, position=0, expected_generation=0)
    aligner.ingest(Event("a", "s", "text", 25))
    windows = aligner.flush()
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_second BEFORE INSERT ON sq_windows WHEN NEW.idx=1 "
            "BEGIN SELECT RAISE(ABORT, 'fault injection'); END"
        )
    with pytest.raises(OutputError, match="fault injection"):
        store.commit(
            aligner.checkpoint(),
            input_id=SOURCE,
            position=1,
            expected_generation=1,
            windows=windows,
        )
    assert store.load() == before
    assert store.window_documents() == ()
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TRIGGER fail_second")
    after = store.commit(
        aligner.checkpoint(), input_id=SOURCE, position=1, expected_generation=1, windows=windows
    )
    assert after.generation == 2 and after.position == 1
    assert len(store.window_documents()) == 3


def test_changed_source_and_configuration_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "identity.db"
    resume_events(source(), config(), path, max_new_events=1)
    with pytest.raises(ValidationError, match="source"):
        resume_events(source()[:-1], config(), path)
    with pytest.raises(ValidationError, match="configuration"):
        resume_events(source(), replace(config(), hop_ms=4), path)


def test_tampered_state_and_window_are_detected(tmp_path: Path) -> None:
    path = tmp_path / "tamper.db"
    resume_events(source(), config(), path)
    store = RecoveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE sq_windows SET payload='{}' WHERE idx=0")
    with pytest.raises(ValidationError, match="checksum"):
        store.window_documents()
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE sq_state SET payload='{}'")
    with pytest.raises(ValidationError, match="checksum"):
        store.load()


def test_missing_output_is_not_silently_resumed(tmp_path: Path) -> None:
    path = tmp_path / "missing.db"
    resume_events(source(), config(), path)
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM sq_windows WHERE idx=0")
    with pytest.raises(ValidationError, match="missing"):
        resume_events(source(), config(), path)


def test_commit_requires_complete_output_and_monotone_offsets(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "complete.db")
    aligner = WatermarkAligner(AlignmentConfig(10, 10))
    store.commit(aligner.checkpoint(), input_id=SOURCE, position=1, expected_generation=0)
    with pytest.raises(ValidationError, match="offset"):
        store.commit(aligner.checkpoint(), input_id=SOURCE, position=0, expected_generation=1)
    aligner.ingest(Event("x", "s", "text", 15))
    aligner.flush()
    with pytest.raises(ValidationError, match="every newly"):
        store.commit(aligner.checkpoint(), input_id=SOURCE, position=2, expected_generation=1)
    assert store.load().position == 1  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "kwargs",
    [{"batch_size": 0}, {"batch_size": True}, {"max_new_events": -1}, {"max_new_events": True}],
)
def test_invalid_run_options(tmp_path: Path, kwargs: dict) -> None:
    with pytest.raises(ValidationError):
        resume_events(source(), config(), tmp_path / "invalid.db", **kwargs)


def test_resume_cli_across_invocations(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    cfg = tmp_path / "config.json"
    events = tmp_path / "events.jsonl"
    cfg.write_text('{"window_ms":10,"hop_ms":10}')
    events.write_text("\n".join(json.dumps(event.to_dict()) for event in source()))
    args = ["resume", str(cfg), str(events), "--database", str(tmp_path / "cli.db")]
    assert main([*args, "--max-new-events", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["position"] == 2
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["closed"] is True
    assert main([*args, "--batch-size", "0"]) == 2


def test_empty_source_can_be_reopened(tmp_path: Path) -> None:
    point = resume_events((), config(), tmp_path / "empty.db")
    assert point.position == 0 and point.checkpoint.closed
    assert resume_events((), config(), tmp_path / "empty.db") == point


def test_window_cursor_cannot_move_backwards(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "monotone.db")
    aligner = WatermarkAligner(AlignmentConfig(10, 10, required_streams=("s",)))
    initial = aligner.checkpoint()
    windows = aligner.ingest(Event("advance", "s", "text", 25))
    assert windows and not aligner.checkpoint().closed
    point = store.commit(
        aligner.checkpoint(), input_id=SOURCE, position=1, expected_generation=0, windows=windows
    )
    with pytest.raises(ValidationError, match="backwards"):
        store.commit(initial, input_id=SOURCE, position=2, expected_generation=1)
    assert store.load() == point
    assert len(store.window_documents()) == len(windows)


@pytest.mark.parametrize("payload", [b"{}", 42, "not json", '{"a":1,"a":2}', "NaN"])
def test_malformed_checksummed_records_fail_closed(tmp_path: Path, payload: object) -> None:
    path = tmp_path / "malformed.db"
    store = RecoveryStore(path)
    digest = hashlib.sha256(str(payload).encode()).hexdigest()
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO sq_state VALUES (1, ?, ?)", (payload, digest))
    with pytest.raises(ValidationError):
        store.load()


@pytest.mark.parametrize("change", [{"generation": 0}, {"position": True}, {"input_id": "x"}])
def test_stored_recovery_point_fields_are_checked(tmp_path: Path, change: dict) -> None:
    path = tmp_path / "fields.db"
    store = RecoveryStore(path)
    point = store.commit(
        WatermarkAligner(config()).checkpoint(), input_id=SOURCE, position=0, expected_generation=0
    )
    document = {
        "generation": point.generation,
        "position": point.position,
        "input_id": point.input_id,
        "checkpoint": point.checkpoint.to_dict(),
        **change,
    }
    payload = json.dumps(document)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE sq_state SET payload=?, digest=?",
            (payload, hashlib.sha256(payload.encode()).hexdigest()),
        )
    with pytest.raises(ValidationError):
        store.load()


def test_database_errors_have_public_error_types(tmp_path: Path) -> None:
    path = tmp_path / "errors.db"
    store = RecoveryStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE sq_state")
    with pytest.raises(ValidationError, match="cannot read recovery"):
        store.load()
    with pytest.raises(ValidationError, match="cannot read recovery"):
        store.window_documents()
    with pytest.raises(OutputError, match="initialize"):
        RecoveryStore(path / "child.db")
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE sq_meta SET version=2")
    with pytest.raises(ValidationError, match="schema"):
        RecoveryStore(path)


def test_recovery_document_size_is_checked_before_commit(tmp_path: Path, monkeypatch) -> None:
    import stream_quilt.recovery as recovery

    store = RecoveryStore(tmp_path / "bounded.db")
    checkpoint = WatermarkAligner(config()).checkpoint()
    monkeypatch.setattr(recovery, "MAX_EVENT_FILE_BYTES", 32)
    with pytest.raises(ValidationError, match="byte limit"):
        store.commit(checkpoint, input_id=SOURCE, position=0, expected_generation=0)
    assert store.load() is None


def test_commit_configuration_and_closed_state_are_guarded(tmp_path: Path) -> None:
    store = RecoveryStore(tmp_path / "guards.db")
    checkpoint = WatermarkAligner(config()).checkpoint()
    store.commit(checkpoint, input_id=SOURCE, position=0, expected_generation=0)
    with pytest.raises(ValidationError, match="configuration"):
        store.commit(
            replace(checkpoint, retention=RetentionPolicy(10)),
            input_id=SOURCE,
            position=1,
            expected_generation=1,
        )
    store.commit(
        replace(checkpoint, closed=True), input_id=SOURCE, position=0, expected_generation=1
    )
    with pytest.raises(ValidationError, match="flushed"):
        store.commit(checkpoint, input_id=SOURCE, position=0, expected_generation=2)


def test_real_process_restart_preserves_outputs(tmp_path: Path) -> None:
    cfg = tmp_path / "config.json"
    events = tmp_path / "events.jsonl"
    path = tmp_path / "process.db"
    cfg.write_text('{"window_ms":10,"hop_ms":10}')
    events.write_text("\n".join(json.dumps(event.to_dict()) for event in source()))
    command = [
        sys.executable,
        "-m",
        "stream_quilt",
        "resume",
        str(cfg),
        str(events),
        "--database",
        str(path),
    ]
    child = subprocess.run(
        [*command, "--max-new-events", "2"], capture_output=True, text=True, check=True, timeout=30
    )
    assert json.loads(child.stdout)["position"] == 2
    child = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
    assert json.loads(child.stdout)["closed"] is True
    uninterrupted = WatermarkAligner(AlignmentConfig(10, 10))
    expected = [window for event in source() for window in uninterrupted.ingest(event)]
    expected.extend(uninterrupted.flush())
    assert RecoveryStore(path).window_documents() == tuple(window.to_dict() for window in expected)


def test_process_exit_before_commit_leaves_previous_generation(tmp_path: Path) -> None:
    path = tmp_path / "crash.db"
    store = RecoveryStore(path)
    point = store.commit(
        WatermarkAligner(config()).checkpoint(), input_id=SOURCE, position=0, expected_generation=0
    )
    # Terminate the separate process without Python cleanup after two transactional writes.
    code = (
        "import sqlite3,sys,os; c=sqlite3.connect(sys.argv[1]); "
        "c.execute('BEGIN IMMEDIATE'); "
        "c.execute(\"UPDATE sq_state SET payload='partial'\"); "
        "c.execute(\"INSERT INTO sq_windows VALUES (0,'partial','partial')\"); "
        "os._exit(23)"
    )
    child = subprocess.run([sys.executable, "-c", code, str(path)], timeout=30)
    assert child.returncode == 23
    assert RecoveryStore(path).load() == point
    assert store.window_documents() == ()


def _single_window_store(tmp_path: Path) -> tuple[RecoveryStore, Path, dict[str, Any]]:
    path = tmp_path / "strict-windows.db"
    resume_events(
        (Event("a", "sensor", "text", 0, data={"score": 0.5}),), AlignmentConfig(10, 10), path
    )
    store = RecoveryStore(path)
    return store, path, store.window_documents()[0]


def _replace_window_payload(path: Path, payload: str) -> None:
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "UPDATE sq_windows SET payload=?, digest=? WHERE idx=0",
            (payload, hashlib.sha256(payload.encode()).hexdigest()),
        )


@pytest.mark.parametrize(
    "field_path,value",
    [
        (("index",), False),
        (("index",), True),
        (("index",), 0.0),
        (("complete",), 1),
        (("complete",), False),
        (("modalities",), ["audio"]),
        (("modalities",), [False]),
        (("modalities",), [{"invalid": "text"}]),
        (("modalities",), ["text", "text"]),
        (("modalities",), "text"),
        (("missing_streams",), {}),
        (("missing_streams",), ["sensor"]),
        (("events",), {}),
        (("events",), [None]),
        (("end_ms",), 0.0),
        (("start_ms",), True),
        (("events", 0, "timestamp_ms"), 20.0),
        (("events", 0, "timestamp_ms"), True),
        (("events", 0, "id"), " a "),
        (("events", 0, "unknown"), "unrecognized"),
        (("extra",), "unexpected"),
    ],
)
def test_recovery_window_reconstructs_typed_contract(
    tmp_path: Path,
    field_path: tuple[Any, ...],
    value: Any,
) -> None:
    store, path, original = _single_window_store(tmp_path)
    changed = deepcopy(original)
    current = changed
    for field in field_path[:-1]:
        current = current[field]
    current[field_path[-1]] = value
    _replace_window_payload(path, json.dumps(changed))
    with pytest.raises(ValidationError):
        store.window_documents()


@pytest.mark.parametrize("document", [{"index": 0}, [], None])
def test_recovery_rejects_incomplete_or_non_object_windows(tmp_path: Path, document: Any) -> None:
    store, path, _ = _single_window_store(tmp_path)
    _replace_window_payload(path, json.dumps(document))
    with pytest.raises(ValidationError, match="window fields"):
        store.window_documents()


def test_window_reader_returns_detached_validated_document(tmp_path: Path) -> None:
    store, _, original = _single_window_store(tmp_path)
    changed = store.window_documents()[0]
    changed["events"][0]["data"]["score"] = 999
    changed["modalities"].append("audio")
    assert store.window_documents()[0] == original


@pytest.mark.parametrize("value", ["1e999", "-1e999"])
def test_recovery_rejects_overflow_numbers_in_window_data(tmp_path: Path, value: str) -> None:
    store, path, document = _single_window_store(tmp_path)
    document["events"][0]["data"]["score"] = "OVERFLOW"
    payload = json.dumps(document).replace('"OVERFLOW"', value)
    _replace_window_payload(path, payload)
    with pytest.raises(ValidationError, match="invalid recovery JSON"):
        store.window_documents()


@pytest.mark.parametrize("target", ["retention", "tracked"])
def test_recovery_rejects_numeric_infinity_before_horizon_reconstruction(
    tmp_path: Path,
    target: str,
) -> None:
    path = tmp_path / "overflow-state.db"
    store = RecoveryStore(path)
    point = store.commit(
        WatermarkAligner(config()).checkpoint(), input_id=SOURCE, position=0, expected_generation=0
    )
    checkpoint = point.checkpoint.to_dict()
    if target == "retention":
        checkpoint["retention"]["horizon_ms"] = "OVERFLOW"
    else:
        checkpoint["seen_order"] = [{"event_id": "max", "horizon_ms": "OVERFLOW", "status": 0}]
    document = {"generation": 1, "input_id": SOURCE, "position": 0, "checkpoint": checkpoint}
    payload = json.dumps(document).replace('"OVERFLOW"', "1e999")
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "UPDATE sq_state SET payload=?, digest=?",
            (payload, hashlib.sha256(payload.encode()).hexdigest()),
        )
    with pytest.raises(ValidationError, match="invalid recovery JSON"):
        store.load()


@pytest.mark.parametrize("target", ["retention", "tracked", "live"])
@pytest.mark.parametrize("value", [math.inf, -math.inf, math.nan])
def test_checkpoint_reader_requires_null_for_infinite_horizons(target: str, value: float) -> None:
    aligner = WatermarkAligner(config())
    aligner.ingest(source()[0])
    document = aligner.checkpoint().to_dict()
    if target == "retention":
        document["retention"]["horizon_ms"] = value
    elif target == "tracked":
        document["seen_order"][0]["horizon_ms"] = value
    else:
        document["live_events"][0]["timestamp_ms"] = value
    with pytest.raises(ValidationError):
        AlignerCheckpoint.from_dict(document)


@pytest.mark.parametrize("field", ["released_count", "released_reported_count"])
@pytest.mark.parametrize(
    "value", [True, -1, 2**53, 10**5000], ids=["bool", "negative", "unsafe", "huge"]
)
def test_checkpoint_counters_must_be_json_safe(field: str, value: int) -> None:
    checkpoint = WatermarkAligner(config()).checkpoint()
    with pytest.raises(ValidationError, match="safe integer"):
        replace(checkpoint, **{field: value})
    document = checkpoint.to_dict()
    document[field] = value
    with pytest.raises(ValidationError, match="safe integer"):
        AlignerCheckpoint.from_dict(document)


def test_largest_safe_release_count_round_trips() -> None:
    checkpoint = replace(WatermarkAligner(config()).checkpoint(), released_count=2**53 - 1)
    assert AlignerCheckpoint.from_dict(json.loads(json.dumps(checkpoint.to_dict()))) == checkpoint


def test_window_reader_rejects_non_string_keys_and_event_limit(monkeypatch) -> None:
    import stream_quilt.recovery as recovery
    from stream_quilt.recovery import _window_document

    window = oracle()[0]
    bad_key = {**window, 7: "unknown"}
    with pytest.raises(ValidationError, match="window fields"):
        _window_document(bad_key, 0)
    nested = deepcopy(window)
    nested["events"][0]["data"] = {7: "bad-key"}
    with pytest.raises(ValidationError):
        _window_document(nested, 0)
    monkeypatch.setattr(recovery, "MAX_EVENTS_PER_WINDOW", 0)
    with pytest.raises(ValidationError, match="event limit"):
        _window_document(window, 0)


@pytest.mark.parametrize("table", ["sq_state", "sq_windows"])
@pytest.mark.parametrize("field", ["payload", "digest"])
def test_sql_guards_reject_large_fields_before_python_text_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    field: str,
) -> None:
    import stream_quilt.recovery as recovery

    store, path, _ = _single_window_store(tmp_path)
    limit = 4096
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(f"UPDATE {table} SET {field}=?", ("x" * (limit + 1),))
    monkeypatch.setattr(recovery, "MAX_EVENT_FILE_BYTES", limit)
    original_connect = store._connect

    def guarded_connect() -> sqlite3.Connection:
        connection = original_connect()

        def guarded_text(raw: bytes) -> str:
            # SQLite calls this while materializing a TEXT column for Python.
            # A post-fetch length check cannot satisfy this independent guard.
            assert len(raw) <= limit, "oversized TEXT reached Python before validation"
            return raw.decode("utf-8")

        connection.text_factory = guarded_text
        return connection

    monkeypatch.setattr(store, "_connect", guarded_connect)
    with pytest.raises(ValidationError, match=r"byte limit|text and a checksum"):
        if table == "sq_state":
            store.load()
        else:
            list(store.iter_window_documents())


def test_sql_guards_measure_utf8_bytes_not_character_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import stream_quilt.recovery as recovery

    store, path, _ = _single_window_store(tmp_path)
    limit = 4096
    # Fewer than 4,096 characters, but more than 4,096 UTF-8 bytes.
    payload = "界" * 1500
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "UPDATE sq_windows SET payload=?, digest=?",
            (payload, hashlib.sha256(payload.encode()).hexdigest()),
        )
    monkeypatch.setattr(recovery, "MAX_EVENT_FILE_BYTES", limit)
    with pytest.raises(ValidationError, match="byte limit"):
        store.window_documents()


def test_output_iterator_close_releases_connection(tmp_path: Path, monkeypatch) -> None:
    store, _, original = _single_window_store(tmp_path)
    connections: list[sqlite3.Connection] = []
    connect = store._connect

    def observed_connect() -> sqlite3.Connection:
        connection = connect()
        connections.append(connection)
        return connection

    monkeypatch.setattr(store, "_connect", observed_connect)
    with closing(store.iter_window_documents()) as documents:
        assert next(documents) == original
        assert connections[0].execute("SELECT 1").fetchone() == (1,)
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


def test_output_iterator_reads_one_snapshot_across_later_commit(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.db"
    store = RecoveryStore(path)
    # WAL permits another connection to commit while the iterator retains its
    # older read snapshot. This tests consistency independently of writer locks.
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    aligner = WatermarkAligner(AlignmentConfig(10, 10, required_streams=("s",)))
    first = aligner.ingest(Event("a", "s", "text", 25))
    store.commit(
        aligner.checkpoint(), input_id=SOURCE, position=1, expected_generation=0, windows=first
    )
    with closing(store.iter_window_documents()) as documents:
        prefix = next(documents)
        next_batch = aligner.ingest(Event("b", "s", "text", 45))
        store.commit(
            aligner.checkpoint(),
            input_id=SOURCE,
            position=2,
            expected_generation=1,
            windows=next_batch,
        )
        assert [prefix, *documents] == [window.to_dict() for window in first]
    assert store.window_documents() == tuple(window.to_dict() for window in (*first, *next_batch))


def test_output_iterator_verifies_later_rows_only_when_consumed(tmp_path: Path) -> None:
    path = tmp_path / "lazy-check.db"
    store = RecoveryStore(path)
    aligner = WatermarkAligner(AlignmentConfig(10, 10, required_streams=("s",)))
    windows = aligner.ingest(Event("a", "s", "text", 25))
    store.commit(
        aligner.checkpoint(), input_id=SOURCE, position=1, expected_generation=0, windows=windows
    )
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("UPDATE sq_windows SET digest='bad' WHERE idx=1")
    with closing(store.iter_window_documents()) as documents:
        assert next(documents) == windows[0].to_dict()
        with pytest.raises(ValidationError):
            next(documents)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE important (value TEXT)",
        "CREATE VIEW important AS SELECT 'keep' AS value",
        "CREATE TABLE sq_meta (version INTEGER NOT NULL)",
    ],
)
def test_unrelated_or_incomplete_database_is_preserved(tmp_path: Path, statement: str) -> None:
    path = tmp_path / "unrelated.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(statement)
    before = path.read_bytes()
    with pytest.raises(ValidationError, match="unrelated recovery database schema"):
        RecoveryStore(path)
    assert path.read_bytes() == before


def test_deleted_schema_marker_is_not_silently_reinitialized(tmp_path: Path) -> None:
    path = tmp_path / "missing-marker.db"
    RecoveryStore(path)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("DELETE FROM sq_meta")
    before = path.read_bytes()
    with pytest.raises(ValidationError, match="schema"):
        RecoveryStore(path)
    assert path.read_bytes() == before
