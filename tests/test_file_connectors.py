"""Public local file connector behavior and real durable recovery."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from stream_quilt import Dataflow, FlowStep, PartitionedFlowJournal, ValidationError
from stream_quilt.connectors import files as file_connector
from stream_quilt.connectors.files import (
    StagedJsonlSource,
    materialize_file,
    run_file_journal,
)
from stream_quilt.partitioned_journal import PartitionedFlowSession


def _hold_sink_lock_then_exit(folder: str, ready) -> None:
    with file_connector._publication_lock(Path(folder), "out.jsonl"):
        ready.set()
        os._exit(0)


def _hold_sink_lock_until_released(folder: str, ready, release) -> None:
    with file_connector._publication_lock(Path(folder), "out.jsonl"):
        ready.set()
        if not release.wait(15):
            raise RuntimeError("lock holder was not released")


def stringify(value: object) -> str:
    return str(value)


def _flow() -> Dataflow:
    return Dataflow("file-connector", "1", (FlowStep("text", "map", stringify),))


def _source(tmp_path: Path, raw: bytes) -> StagedJsonlSource:
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "rows.jsonl").write_bytes(raw)
    return StagedJsonlSource.stage(incoming, "rows.jsonl", tmp_path / "private", "rows")


def test_staged_exact_offsets_and_original_file_change(tmp_path: Path) -> None:
    raw = b'{"key":"a","value":1}\r\n{"key":"b","value":2}\n{"key":"a","value":3}'
    source = _source(tmp_path, raw)
    assert source.byte_offsets == (0, 23, 45, len(raw))
    assert [record.value for record in source.read_batch(1, 2)] == [2, 3]
    (tmp_path / "incoming" / "rows.jsonl").write_text("changed", encoding="utf-8")
    restored = StagedJsonlSource.restore(
        tmp_path / "private", "rows.jsonl", source.source_id, source.source_digest
    )
    assert restored.byte_offsets == source.byte_offsets
    assert [record.value for record in restored.read_batch(0, 3)] == [1, 2, 3]


def test_empty_file_commits_eof_at_zero(tmp_path: Path) -> None:
    source = _source(tmp_path, b"")
    assert source.byte_offsets == (0,) and source.record_count == 0
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    point = run_file_journal(source, journal, max_new_inputs=1)
    assert point.checkpoint.next_position == 0 and point.checkpoint.source_closed
    output = materialize_file(journal, tmp_path / "private", "out.jsonl")
    assert len(output.read_bytes().splitlines()) == 1


def test_source_line_limit_rejected_before_staging(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(file_connector, "_MAX_LINE_BYTES", 8)
    with pytest.raises(ValidationError, match="line exceeds"):
        _source(tmp_path, b'{"key":"a","value":1}\n')
    assert not (tmp_path / "private").exists()


def test_real_journal_restart_cap_eof_and_idempotent_snapshot(tmp_path: Path) -> None:
    source = _source(
        tmp_path,
        b'{"key":"a","value":1}\n{"key":"b","value":2}\n{"key":"a","value":3}\n',
    )
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    first = run_file_journal(source, journal, max_new_inputs=2, batch_size=2)
    assert first.checkpoint.next_position == 2 and not first.checkpoint.source_closed
    restored = StagedJsonlSource.restore(
        tmp_path / "private", "rows.jsonl", source.source_id, source.source_digest
    )
    reopened = PartitionedFlowJournal(
        tmp_path / "private" / "run.db",
        _flow(),
        source.source_id,
        source.source_digest,
        create=False,
    )
    final = run_file_journal(restored, reopened, max_new_inputs=10, batch_size=2)
    assert final.checkpoint.next_position == 3 and final.checkpoint.source_closed
    output = materialize_file(reopened, tmp_path / "private", "out.jsonl")
    original = output.read_bytes()
    assert materialize_file(reopened, tmp_path / "private", "out.jsonl").read_bytes() == original
    rows = [json.loads(line) for line in original.splitlines()]
    assert [row["sequence"] for row in rows[1:]] == [0, 1, 2]
    assert [row["source_position"] for row in rows[1:]] == [0, 1, 2]
    assert [row["value"] for row in rows[1:]] == ["1", "2", "3"]


@pytest.mark.parametrize(
    "raw",
    [
        b"\xef\xbb\xbf" + b'{"key":"a","value":1}\n',
        b'{"key":"a","value":1}\n\n',
        b'{"key":"a","key":"b","value":1}\n',
        b'{"key":"a","value":NaN}\n',
        b'{"key":"a","value":1,"extra":2}\n',
    ],
)
def test_invalid_input_rejected_before_journal(tmp_path: Path, raw: bytes) -> None:
    with pytest.raises(ValidationError):
        _source(tmp_path, raw)


def test_corrupt_staged_blob_rejected_on_restore(tmp_path: Path) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n')
    source.blob_path.write_bytes(b'{"key":"a","value":9}\n')
    with pytest.raises(ValidationError):
        StagedJsonlSource.restore(
            tmp_path / "private", "rows.jsonl", source.source_id, source.source_digest
        )


def test_staged_mutation_stops_before_worker_entry(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    source.blob_path.write_bytes(b'{"key":"a","value":9}\n')
    entered = []

    def forbidden_session(self):
        entered.append(True)
        raise AssertionError("workers must not start after source mutation")

    monkeypatch.setattr(PartitionedFlowJournal, "session", forbidden_session)
    with pytest.raises(ValidationError, match="staged source identity"):
        run_file_journal(source, journal, max_new_inputs=1)
    assert not entered
    assert journal.latest().checkpoint.next_position == 0


def test_equal_length_mutation_after_verify_cannot_change_committed_value(
    tmp_path: Path, monkeypatch
) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    original_verify = StagedJsonlSource.verify

    def mutate_after_verify(self):
        original_verify(self)
        self.blob_path.write_bytes(b'{"key":"a","value":9}\n')

    monkeypatch.setattr(StagedJsonlSource, "verify", mutate_after_verify)
    point = run_file_journal(source, journal, max_new_inputs=1)
    assert point.checkpoint.next_position == 1
    page = journal.read_outputs(journal.output_cursor())
    assert [item.output.record.value for item in page.outputs] == ["1"]
    assert source.read_batch(0, 1)[0].value == 1


def test_foreign_or_corrupt_output_fails_closed(tmp_path: Path) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    run_file_journal(source, journal, max_new_inputs=10)
    output = materialize_file(journal, tmp_path / "private", "out.jsonl")
    output.write_bytes(output.read_bytes().replace(b'"value":"1"', b'"value":"9"'))
    with pytest.raises(ValidationError):
        materialize_file(journal, tmp_path / "private", "out.jsonl")


def test_snapshot_from_different_journal_fails_closed(tmp_path: Path) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n')
    first = PartitionedFlowJournal(
        tmp_path / "private" / "first.db", _flow(), source.source_id, source.source_digest
    )
    run_file_journal(source, first, max_new_inputs=2)
    output = materialize_file(first, tmp_path / "private", "out.jsonl")
    before = output.read_bytes()
    second = PartitionedFlowJournal(
        tmp_path / "private" / "second.db", _flow(), source.source_id, source.source_digest
    )
    with pytest.raises(ValidationError, match="identity or digest mismatch"):
        materialize_file(second, tmp_path / "private", "out.jsonl")
    assert output.read_bytes() == before


def test_input_and_output_path_escapes_rejected(tmp_path: Path) -> None:
    root = tmp_path / "incoming"
    root.mkdir()
    (root / "rows.jsonl").write_bytes(b'{"key":"a","value":1}\n')
    for name in (
        "../outside",
        "/outside",
        "C:evil",
        "a:stream",
        "x/../rows.jsonl",
        "CON.txt",
        "NUL",
        "x?y",
    ):
        with pytest.raises(ValidationError):
            StagedJsonlSource.stage(root, name, tmp_path / "private", "rows")
    source = StagedJsonlSource.stage(root, "rows.jsonl", tmp_path / "private", "rows")
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    with pytest.raises(ValidationError):
        materialize_file(journal, tmp_path / "private", "../outside")


@pytest.mark.skipif(os.name != "nt", reason="case-alias behavior is Windows-specific")
def test_windows_case_alias_rejected(tmp_path: Path) -> None:
    root = tmp_path / "incoming"
    root.mkdir()
    (root / "Rows.jsonl").write_bytes(b'{"key":"a","value":1}\n')
    with pytest.raises(ValidationError, match="case alias"):
        StagedJsonlSource.stage(root, "rows.jsonl", tmp_path / "private", "rows")


def test_static_input_and_output_symlinks_rejected(tmp_path: Path) -> None:
    root = tmp_path / "incoming"
    root.mkdir()
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(b'{"key":"a","value":1}\n')
    try:
        (root / "link.jsonl").symlink_to(outside)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    with pytest.raises(ValidationError):
        StagedJsonlSource.stage(root, "link.jsonl", tmp_path / "private", "rows")
    (root / "dangling.jsonl").symlink_to(root / "missing.jsonl")
    with pytest.raises(ValidationError):
        StagedJsonlSource.stage(root, "dangling.jsonl", tmp_path / "private", "rows")
    (root / "rows.jsonl").write_bytes(outside.read_bytes())
    source = StagedJsonlSource.stage(root, "rows.jsonl", tmp_path / "private", "rows")
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    (tmp_path / "private" / "out.jsonl").symlink_to(outside)
    with pytest.raises(ValidationError):
        materialize_file(journal, tmp_path / "private", "out.jsonl")
    (tmp_path / "private" / "dangling-out.jsonl").symlink_to(
        tmp_path / "private" / "missing-out.jsonl"
    )
    with pytest.raises(ValidationError):
        materialize_file(journal, tmp_path / "private", "dangling-out.jsonl")
    assert outside.read_bytes() == b'{"key":"a","value":1}\n'


def test_lost_wave_ack_reconciles_receipt_without_duplicate(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n{"key":"a","value":2}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    original = PartitionedFlowSession.apply
    seen = []

    def lose_ack(self, request):
        result = original(self, request)
        seen.append(result)
        if request.cause == "wave":
            raise OSError("simulated lost return after durable COMMIT")
        return result

    monkeypatch.setattr(PartitionedFlowSession, "apply", lose_ack)
    first = run_file_journal(source, journal, max_new_inputs=2, batch_size=2)
    assert first.checkpoint.next_position == 2
    assert not first.checkpoint.source_closed
    assert len(seen) == 1 and seen[0].status == "committed"
    monkeypatch.setattr(PartitionedFlowSession, "apply", original)
    final = run_file_journal(source, journal, max_new_inputs=2)
    assert final.checkpoint.source_closed and final.checkpoint.emitted_records == 2
    output = materialize_file(journal, tmp_path / "private", "out.jsonl")
    assert len(output.read_bytes().splitlines()) == 3


def test_replace_failure_keeps_previous_snapshot(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n{"key":"a","value":2}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    run_file_journal(source, journal, max_new_inputs=1, batch_size=1)
    output = materialize_file(journal, tmp_path / "private", "out.jsonl")
    before = output.read_bytes()
    run_file_journal(source, journal, max_new_inputs=2, batch_size=1)

    def fail_replace(_source, _target):
        raise OSError("simulated sharing violation")

    monkeypatch.setattr(file_connector.os, "replace", fail_replace)
    with pytest.raises(Exception, match="cannot publish output snapshot"):
        materialize_file(journal, tmp_path / "private", "out.jsonl")
    assert output.read_bytes() == before
    assert not list((tmp_path / "private").glob(".output-*.tmp"))


def test_post_replace_lost_ack_is_idempotent(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n{"key":"a","value":2}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    run_file_journal(source, journal, max_new_inputs=1, batch_size=1)
    output = materialize_file(journal, tmp_path / "private", "out.jsonl")
    old = output.read_bytes()
    run_file_journal(source, journal, max_new_inputs=2, batch_size=1)
    real_replace = file_connector.os.replace

    def replace_then_lose_ack(temporary, target):
        real_replace(temporary, target)
        raise OSError("simulated lost return after replacement")

    monkeypatch.setattr(file_connector.os, "replace", replace_then_lose_ack)
    with pytest.raises(Exception, match="cannot publish output snapshot"):
        materialize_file(journal, tmp_path / "private", "out.jsonl")
    newer = output.read_bytes()
    assert newer != old and len(newer.splitlines()) == 3

    def unexpected_replace(_temporary, _target):
        raise AssertionError("identical snapshot should not be replaced")

    monkeypatch.setattr(file_connector.os, "replace", unexpected_replace)
    assert materialize_file(journal, tmp_path / "private", "out.jsonl").read_bytes() == newer


def test_eof_without_new_rows_keeps_materialized_prefix_bytes(tmp_path: Path) -> None:
    source = _source(tmp_path, b'{"key":"a","value":1}\n')
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    first = run_file_journal(source, journal, max_new_inputs=1)
    assert not first.checkpoint.source_closed
    output = materialize_file(journal, tmp_path / "private", "out.jsonl")
    before = output.read_bytes()
    final = run_file_journal(source, journal, max_new_inputs=1)
    assert final.checkpoint.source_closed and final.checkpoint.emitted_records == 1
    assert materialize_file(journal, tmp_path / "private", "out.jsonl").read_bytes() == before


@pytest.mark.parametrize(
    "second_name",
    [
        "out.jsonl",
        pytest.param(
            "OUT.jsonl",
            marks=pytest.mark.skipif(os.name != "nt", reason="Windows name alias"),
        ),
    ],
)
def test_competing_journals_cannot_both_publish_absent_target(
    tmp_path: Path, monkeypatch, second_name: str
) -> None:
    source = _source(tmp_path, b"")
    first = PartitionedFlowJournal(
        tmp_path / "private" / "first.db", _flow(), source.source_id, source.source_digest
    )
    second = PartitionedFlowJournal(
        tmp_path / "private" / "second.db", _flow(), source.source_id, source.source_digest
    )
    first_at_replace = threading.Event()
    second_at_replace = threading.Event()
    release_first = threading.Event()
    label = threading.local()
    real_replace = file_connector.os.replace

    def paused_replace(source_path, target_path):
        if label.name == "first":
            first_at_replace.set()
            if not release_first.wait(8):
                raise AssertionError("first writer was not released")
        else:
            second_at_replace.set()
        return real_replace(source_path, target_path)

    def publish(name, journal, target_name):
        label.name = name
        return materialize_file(journal, tmp_path / "private", target_name)

    monkeypatch.setattr(file_connector.os, "replace", paused_replace)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first_job = pool.submit(publish, "first", first, "out.jsonl")
        assert first_at_replace.wait(8)
        second_job = pool.submit(publish, "second", second, second_name)
        try:
            assert not second_at_replace.wait(2), "second writer passed the incumbent check"
        finally:
            release_first.set()
        assert first_job.result(timeout=8).is_file()
        with pytest.raises(ValidationError, match=r"identity or digest mismatch|case alias"):
            second_job.result(timeout=8)
    assert len(list((tmp_path / "private").glob(".stream-quilt-sink-*.lock"))) == 1


def test_publication_lock_released_after_process_death(tmp_path: Path) -> None:
    source = _source(tmp_path, b"")
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    child = context.Process(
        target=_hold_sink_lock_then_exit, args=(str(tmp_path / "private"), ready)
    )
    child.start()
    try:
        assert ready.wait(15)
        child.join(15)
        assert child.exitcode == 0
        assert materialize_file(journal, tmp_path / "private", "out.jsonl").is_file()
    finally:
        if child.is_alive():
            child.terminate()
            child.join(5)


def test_publication_lock_wait_is_bounded(tmp_path: Path, monkeypatch) -> None:
    source = _source(tmp_path, b"")
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    child = context.Process(
        target=_hold_sink_lock_until_released,
        args=(str(tmp_path / "private"), ready, release),
    )
    monkeypatch.setattr(file_connector, "_LOCK_WAIT_SECONDS", 0.2)
    child.start()
    try:
        assert ready.wait(15)
        with pytest.raises(Exception, match="publication lock timed out"):
            materialize_file(journal, tmp_path / "private", "out.jsonl")
    finally:
        release.set()
        child.join(15)
        if child.is_alive():
            child.terminate()
            child.join(5)
    assert child.exitcode == 0
    assert not (tmp_path / "private" / "out.jsonl").exists()


@pytest.mark.parametrize("raw", [b"x", b"\0trailing"])
def test_malformed_preexisting_publication_lock_fails_closed(tmp_path: Path, raw: bytes) -> None:
    source = _source(tmp_path, b"")
    journal = PartitionedFlowJournal(
        tmp_path / "private" / "run.db", _flow(), source.source_id, source.source_digest
    )
    name = ".stream-quilt-sink-" + hashlib.sha256(b"out.jsonl").hexdigest() + ".lock"
    (tmp_path / "private" / name).write_bytes(raw)
    with pytest.raises(ValidationError, match="publication lock"):
        materialize_file(journal, tmp_path / "private", "out.jsonl")
    assert not (tmp_path / "private" / "out.jsonl").exists()
