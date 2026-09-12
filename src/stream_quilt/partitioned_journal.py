"""SQPJ: one parent SQLite publication boundary over real local worker candidates."""

from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

from ._local_worker import LocalWorkerError, LocalWorkerStatus
from .dataflow import Dataflow, FlowRuntime, _count
from .errors import OutputError, ValidationError
from .flow_journal import _connection_scope, _digest, _finish_connection
from .partitioned_checkpoint import (
    LocalWorkerLimits,
    PartitionedFlowCheckpoint,
    _flow,
    _hex,
    _name,
)
from .partitioned_flow import LocalPartitionedFlow
from .partitioned_journal_types import (
    _BATCH_BYTES,
    _DATABASE_BYTES,
    _FILE_BYTES,
    _HEAD_BYTES,
    _MAX_HISTORY,
    _OUTPUT_BYTES,
    _RECEIPT_BYTES,
    _STORE_BYTES,
    PartitionedFlowReceipt,
    PartitionedFlowRecoveryPoint,
    PartitionedFlowRequest,
    PartitionedJournalOutput,
    PartitionedOutputCursor,
    PartitionedOutputPage,
    _checked,
    _encoded,
    _id,
    _point_progress,
)
from .recovery import RecoveryConflict

_APPLICATION_ID = 0x5351504A
_LABEL = "partitioned journal"
_PAGE_METADATA_BYTES = 8 * 1024 * 1024
_SCHEMA = {
    "partition_head": "CREATE TABLE partition_head (slot INTEGER PRIMARY KEY CHECK(slot=1), "
    "payload TEXT NOT NULL, digest TEXT NOT NULL)",
    "partition_commit": "CREATE TABLE partition_commit (generation INTEGER PRIMARY KEY, "
    "request_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, digest TEXT NOT NULL)",
    "partition_output": "CREATE TABLE partition_output (seq INTEGER PRIMARY KEY, "
    "generation INTEGER NOT NULL REFERENCES partition_commit(generation), "
    "payload TEXT NOT NULL, digest TEXT NOT NULL)",
}
# Native SQLite admits lengths before transferring any stored payload to Python.
# All SQL identifiers are static; request content is passed only as bound parameters.
_HEAD = (
    "SELECT CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' "
    "AND length(CAST(digest AS BLOB))=64 THEN digest END, "
    "length(CAST(payload AS BLOB)) FROM partition_head WHERE slot=1"
)
_RECEIPT = (
    "SELECT generation, CASE WHEN typeof(request_id)='text' "
    "AND length(CAST(request_id AS BLOB))=32 THEN request_id END, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' "
    "AND length(CAST(digest AS BLOB))=64 THEN digest END, "
    "length(CAST(payload AS BLOB)) FROM partition_commit WHERE generation=?"
)
_REQUEST = _RECEIPT.replace("WHERE generation=?", "WHERE request_id=?")
_OUTPUT = (
    "SELECT seq, generation, CASE WHEN typeof(payload)='text' "
    "AND length(CAST(payload AS BLOB))<=? THEN payload END, CASE WHEN typeof(digest)='text' "
    "AND length(CAST(digest AS BLOB))=64 THEN digest END, "
    "length(CAST(payload AS BLOB)) FROM partition_output WHERE seq=?"
)
_OUTPUTS = (
    "SELECT seq, generation, CASE WHEN typeof(payload)='text' "
    "AND length(CAST(payload AS BLOB))<=? THEN payload END, CASE WHEN typeof(digest)='text' "
    "AND length(CAST(digest AS BLOB))=64 THEN digest END, "
    "length(CAST(payload AS BLOB)) FROM partition_output "
    "WHERE seq>=? AND seq<? ORDER BY seq LIMIT ?"
)
_STORAGE = (
    (
        "SELECT count(*) FROM partition_head",
        "SELECT coalesce(sum(length(CAST(payload AS BLOB))+length(CAST(digest AS BLOB))),0) "
        "FROM partition_head",
        1,
    ),
    (
        "SELECT count(*) FROM partition_commit",
        "SELECT coalesce(sum(length(CAST(payload AS BLOB))+length(CAST(digest AS BLOB))"
        "+length(CAST(request_id AS BLOB))),0) FROM partition_commit",
        _MAX_HISTORY,
    ),
    (
        "SELECT count(*) FROM partition_output",
        "SELECT coalesce(sum(length(CAST(payload AS BLOB))+length(CAST(digest AS BLOB))),0) "
        "FROM partition_output",
        _MAX_HISTORY,
    ),
)


class PartitionedFlowJournal:
    """No persistent connection or worker ownership; sessions explicitly own processes."""

    def __init__(
        self,
        path: str | Path,
        flow: Dataflow,
        source_id: str,
        source_digest: str,
        *,
        workers: int = 2,
        limits: LocalWorkerLimits | None = None,
        create: bool = True,
    ) -> None:
        _flow(flow)
        _name(source_id, "source ID")
        _hex(source_digest)
        _count(workers, "workers", 1, 8)
        active = LocalWorkerLimits() if limits is None else limits
        if type(active) is not LocalWorkerLimits or type(create) is not bool:
            raise ValidationError("invalid journal limits/create flag")
        active.__post_init__()
        self._flow_definition = flow
        self._initial_checkpoint = PartitionedFlowCheckpoint(
            flow.identity,
            source_id,
            source_digest,
            workers,
            active,
            0,
            0,
            False,
            tuple(FlowRuntime(flow).checkpoint() for _ in range(workers)),
        )
        self._path = Path(path).absolute()
        self._journal_id: str | None = None
        if not create and not self.path.is_file():
            raise ValidationError("partitioned journal does not exist")
        try:
            if create:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._transaction(create=create) as connection:
                connection.execute("BEGIN IMMEDIATE")
                count = connection.execute(
                    "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                ).fetchone()[0]
                if (
                    count == 0
                    and create
                    and connection.execute("PRAGMA application_id").fetchone() == (0,)
                    and connection.execute("PRAGMA user_version").fetchone() == (0,)
                ):
                    if connection.execute("PRAGMA journal_mode").fetchone() != ("delete",):
                        raise ValidationError("only DELETE journal mode is admitted")
                    connection.execute("PRAGMA application_id = 1397837898")
                    connection.execute("PRAGMA user_version = 1")
                    for sql in _SCHEMA.values():
                        connection.execute(sql)
                    point = self._initial(uuid.uuid4().hex)
                    payload = point.to_json()
                    connection.execute(
                        "INSERT INTO partition_head VALUES (1, ?, ?)", (payload, _digest(payload))
                    )
                point, _, _ = self._latest(connection)
                self._journal_id = point.journal_id
        except (sqlite3.Error, OSError) as exc:
            raise OutputError("cannot initialize partitioned journal") from exc

    @property
    def path(self) -> Path:
        return self._path

    @property
    def flow(self) -> Dataflow:
        return self._flow_definition

    def _initial(self, journal_id: str) -> PartitionedFlowRecoveryPoint:
        return PartitionedFlowRecoveryPoint(journal_id, 0, self._initial_checkpoint)

    def _files(self) -> None:
        total = 0
        for suffix in ("", "-journal", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            try:
                size = candidate.stat().st_size
            except FileNotFoundError:
                continue
            total += size
            if total > _FILE_BYTES or (not suffix and size > _DATABASE_BYTES):
                raise ValidationError("journal file admission exceeded")

    def _connect(self, *, create: bool = False) -> sqlite3.Connection:
        self._files()
        connection = sqlite3.connect(
            self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw"), uri=True, timeout=10
        )
        try:
            # These are connection options; never migrate an existing journal mode/schema.
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
        except BaseException as error:
            _finish_connection(connection, error, label=_LABEL)
            raise
        return connection

    @contextmanager
    def _transaction(self, *, create: bool = False) -> Iterator[sqlite3.Connection]:
        with _connection_scope(self._connect(create=create), label=_LABEL) as connection:
            yield connection

    def _schema(self, connection: sqlite3.Connection) -> None:
        if (
            connection.execute("PRAGMA application_id").fetchone() != (_APPLICATION_ID,)
            or connection.execute("PRAGMA user_version").fetchone() != (1,)
            or connection.execute("PRAGMA journal_mode").fetchone() != ("delete",)
        ):
            raise ValidationError("unsupported partitioned journal schema/mode")
        if connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchone() != (3,):
            raise ValidationError("unexpected journal schema objects")
        rows = connection.execute(
            "SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END, "
            "CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END, "
            "CASE WHEN length(CAST(sql AS BLOB))<=4096 THEN sql END "
            "FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        if any(kind != "table" or _SCHEMA.get(name) != sql for name, kind, sql in rows):
            raise ValidationError("partitioned journal schema changed")
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        pages = connection.execute("PRAGMA page_count").fetchone()[0]
        if page_size * pages > _DATABASE_BYTES:
            raise ValidationError("database page budget exceeded")

    def _storage(self, connection: sqlite3.Connection) -> int:
        total = 0
        for count_sql, size_sql, maximum in _STORAGE:
            count = connection.execute(count_sql).fetchone()[0]
            if count > maximum:
                raise ValidationError("journal retained row count exceeded")
            size = connection.execute(size_sql).fetchone()[0]
            total += size
            if total > _STORE_BYTES:
                raise ValidationError("journal retained text-wire budget exceeded")
        return total

    def _receipt_row(self, row: Any) -> tuple[PartitionedFlowReceipt, str]:
        if row is None:
            raise ValidationError("missing partitioned receipt")
        generation, request_id, payload, digest, size = row
        receipt = PartitionedFlowReceipt.from_dict(
            _checked((payload, digest, size), _RECEIPT_BYTES)
        )
        if (
            receipt.status != "committed"
            or receipt.after_generation != generation
            or receipt.request_id != request_id
        ):
            raise ValidationError("receipt differs from its SQL index")
        return receipt, digest

    def _receipt(
        self, connection: sqlite3.Connection, generation: int
    ) -> tuple[PartitionedFlowReceipt, str]:
        return self._receipt_row(
            connection.execute(_RECEIPT, (_RECEIPT_BYTES, generation)).fetchone()
        )

    def _chain(
        self,
        connection: sqlite3.Connection,
        receipt: PartitionedFlowReceipt,
        point: PartitionedFlowRecoveryPoint,
    ) -> None:
        limits = point.checkpoint.limits
        if (
            receipt.journal_id != point.journal_id
            or receipt.after_generation > point.generation
            or receipt.after[0] > point.checkpoint.next_position
            or receipt.output_stop > point.checkpoint.emitted_records
            or receipt.after[0] - receipt.before[0] > limits.max_batch_inputs
            or receipt.output_stop - receipt.output_start > limits.max_output_records
        ):
            raise ValidationError("receipt lies outside admitted journal prefix")
        if receipt.expected_generation:
            previous, _ = self._receipt(connection, receipt.expected_generation)
            if (
                previous.journal_id != receipt.journal_id
                or previous.after != receipt.before
                or previous.after_head_digest != receipt.before_head_digest
            ):
                raise ValidationError("adjacent receipt boundaries disagree")
        elif receipt.before != (0, 0, False, 0) or receipt.before_head_digest != _digest(
            self._initial(point.journal_id).to_json()
        ):
            raise ValidationError("first receipt must bind initial head")
        if receipt.after_generation < point.generation:
            following, _ = self._receipt(connection, receipt.after_generation + 1)
            if (
                following.journal_id != receipt.journal_id
                or following.before != receipt.after
                or following.before_head_digest != receipt.after_head_digest
            ):
                raise ValidationError("adjacent receipt boundaries disagree")

    def _latest(
        self, connection: sqlite3.Connection
    ) -> tuple[PartitionedFlowRecoveryPoint, str, int]:
        self._schema(connection)
        retained = self._storage(connection)
        row = connection.execute(_HEAD, (_HEAD_BYTES,)).fetchone()
        point = PartitionedFlowRecoveryPoint.from_dict(_checked(row, _HEAD_BYTES))
        initial, cp = self._initial_checkpoint, point.checkpoint
        if (cp.flow_identity, cp.source_id, cp.source_digest, cp.workers, cp.limits) != (
            initial.flow_identity,
            initial.source_id,
            initial.source_digest,
            initial.workers,
            initial.limits,
        ) or (self._journal_id is not None and point.journal_id != self._journal_id):
            raise ValidationError("journal/source/flow/worker identity mismatch")
        cp.validate_for(self.flow)
        for sql, expected, first in (
            (
                "SELECT count(*),min(generation),max(generation) FROM partition_commit",
                point.generation,
                1,
            ),
            ("SELECT count(*),min(seq),max(seq) FROM partition_output", cp.emitted_records, 0),
        ):
            count, low, high = connection.execute(sql).fetchone()
            if count != expected or (count and (low != first or high != first + count - 1)):
                raise ValidationError("journal table prefix contradicts head")
        if point.generation:
            receipt, _ = self._receipt(connection, point.generation)
            self._chain(connection, receipt, point)
            if receipt.after != _point_progress(cp) or receipt.after_head_digest != row[1]:
                raise ValidationError("latest receipt does not bind head")
        elif point != self._initial(point.journal_id):
            raise ValidationError("initial journal must be empty")
        return point, row[1], retained

    def latest(self) -> PartitionedFlowRecoveryPoint:
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _, _ = self._latest(connection)
                point.to_json()
                return point
        except sqlite3.Error as exc:
            raise OutputError("cannot read partitioned recovery point") from exc

    def _request(
        self,
        connection: sqlite3.Connection,
        request_id: str,
        point: PartitionedFlowRecoveryPoint,
        binding: tuple[str, int] | None = None,
    ) -> PartitionedFlowReceipt | None:
        row = connection.execute(_REQUEST, (_RECEIPT_BYTES, request_id)).fetchone()
        if row is None:
            return None
        receipt, _ = self._receipt_row(row)
        self._chain(connection, receipt, point)
        if binding is not None and (receipt.request_digest, receipt.expected_generation) != binding:
            raise RecoveryConflict("request ID already binds different content/generation")
        receipt.to_json()
        return receipt

    def request(self, request_id: str) -> PartitionedFlowReceipt | None:
        _id(request_id)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _, _ = self._latest(connection)
                return self._request(connection, request_id, point)
        except sqlite3.Error as exc:
            raise OutputError("cannot read partitioned receipt") from exc

    def session(self) -> PartitionedFlowSession:
        return PartitionedFlowSession(self)

    def output_cursor(self, *, start: int = 0) -> PartitionedOutputCursor:
        _count(start, "output start", 0, _MAX_HISTORY)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, digest, _ = self._latest(connection)
                if point.generation:
                    _, digest = self._receipt(connection, point.generation)
                result = PartitionedOutputCursor(
                    point.journal_id,
                    point.generation,
                    digest,
                    point.checkpoint.emitted_records,
                    start,
                )
                result.to_json()
                return result
        except sqlite3.Error as exc:
            raise OutputError("cannot capture partitioned output cursor") from exc

    def _output_row(self, row: Any) -> tuple[PartitionedJournalOutput, int]:
        if row is None:
            raise ValidationError("missing partitioned output")
        seq, generation, payload, digest, size = row
        output = PartitionedJournalOutput.from_dict(
            _checked((payload, digest, size), _OUTPUT_BYTES)
        )
        if (output.output.sequence, output.generation) != (
            seq,
            generation,
        ) or output.output.record.byte_size > self.flow.limits.max_record_bytes:
            raise ValidationError("output differs from its index or value limit")
        return output, size

    def read_outputs(
        self,
        cursor: PartitionedOutputCursor,
        *,
        limit: int = 1000,
        max_bytes: int = 16 * 1024 * 1024,
    ) -> PartitionedOutputPage:
        if type(cursor) is not PartitionedOutputCursor:
            raise ValidationError("read_outputs requires PartitionedOutputCursor")
        cursor.__post_init__()
        _count(limit, "page limit", 1, 1000)
        _count(max_bytes, "page bytes", 1, _BATCH_BYTES)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _, _ = self._latest(connection)
                if (
                    cursor.journal_id != point.journal_id
                    or cursor.anchor_generation > point.generation
                ):
                    raise ValidationError("cursor belongs to a different prefix")
                stop, digest = 0, _digest(self._initial(point.journal_id).to_json())
                if cursor.anchor_generation:
                    anchor, digest = self._receipt(connection, cursor.anchor_generation)
                    self._chain(connection, anchor, point)
                    stop = anchor.output_stop
                if cursor.stop_sequence != stop or cursor.anchor_receipt_digest != digest:
                    raise ValidationError("cursor anchor changed")
                receipts: dict[int, PartitionedFlowReceipt] = {}

                def admit(item: PartitionedJournalOutput) -> bool:
                    if item.generation not in receipts:
                        # Reserve the selected receipt and both neighbors before fetching.
                        if (len(receipts) + 1) * 3 * _RECEIPT_BYTES > _PAGE_METADATA_BYTES:
                            return False
                        receipt, _ = self._receipt(connection, item.generation)
                        self._chain(connection, receipt, point)
                        receipts[item.generation] = receipt
                    receipt = receipts[item.generation]
                    if (
                        item.generation > cursor.anchor_generation
                        or receipt.cause != "wave"
                        or not receipt.output_start <= item.output.sequence < receipt.output_stop
                        or not receipt.before[0] <= item.output.source_position < receipt.after[0]
                        or (
                            item.output.sequence == receipt.output_start
                            and item.output.output_index
                        )
                    ):
                        raise ValidationError("output lies outside its wave receipt")
                    return True

                previous = None
                if cursor.next_sequence:
                    previous, _ = self._output_row(
                        connection.execute(
                            _OUTPUT, (_OUTPUT_BYTES, cursor.next_sequence - 1)
                        ).fetchone()
                    )
                    if not admit(previous):
                        raise ValidationError("predecessor receipt exceeds page admission")
                outputs: list[PartitionedJournalOutput] = []
                size = 0
                limited = False
                for row in connection.execute(
                    _OUTPUTS, (_OUTPUT_BYTES, cursor.next_sequence, cursor.stop_sequence, limit)
                ):
                    item, cost = self._output_row(row)
                    if item.output.sequence != cursor.next_sequence + len(outputs):
                        raise ValidationError("noncontiguous output page")
                    if size + cost > max_bytes or not admit(item):
                        if not outputs:
                            raise ValidationError("next output exceeds page admission")
                        limited = True
                        break
                    if previous is not None:
                        a, b = previous.output, item.output
                        if (
                            item.generation < previous.generation
                            or b.source_position < a.source_position
                            or (
                                b.source_position == a.source_position
                                and (
                                    item.generation != previous.generation
                                    or b.output_index != a.output_index + 1
                                    or b.record.key != a.record.key
                                )
                            )
                            or (b.source_position > a.source_position and b.output_index)
                        ):
                            raise ValidationError("output source/ordinal order is invalid")
                    outputs.append(item)
                    size += cost
                    previous = item
                if not limited and len(outputs) != min(
                    limit, cursor.stop_sequence - cursor.next_sequence
                ):
                    raise ValidationError("output query returned incomplete prefix")
                result = PartitionedOutputPage(
                    tuple(outputs),
                    replace(cursor, next_sequence=cursor.next_sequence + len(outputs)),
                )
                result.to_json()
                return result
        except sqlite3.Error as exc:
            raise OutputError("cannot read partitioned output page") from exc


class PartitionedFlowSession:
    """Explicit process ownership; every apply reads durable truth, never cached state."""

    def __init__(self, journal: PartitionedFlowJournal) -> None:
        if type(journal) is not PartitionedFlowJournal:
            raise ValidationError("session requires PartitionedFlowJournal")
        point = journal.latest().checkpoint
        self._journal = journal
        self._runtime = LocalPartitionedFlow(
            journal.flow,
            point.source_id,
            point.source_digest,
            workers=point.workers,
            limits=point.limits,
            checkpoint=point,
        )

    def worker_status(self) -> tuple[LocalWorkerStatus, ...]:
        return self._runtime.worker_status()

    def cancel(self) -> None:
        self._runtime.cancel()

    def close(self) -> None:
        self._runtime.close()

    def __enter__(self) -> PartitionedFlowSession:
        self._runtime.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, traceback: Any) -> None:
        self._runtime.__exit__(exc_type, exc, traceback)

    def apply(self, request: PartitionedFlowRequest) -> PartitionedFlowReceipt:
        runtime, journal = self._runtime, self._journal
        with runtime._operation():
            try:
                deadline = time.monotonic() + runtime.limits.wave_timeout
                if type(request) is not PartitionedFlowRequest:
                    raise ValidationError("apply requires PartitionedFlowRequest")
                binding = (request.digest, request.expected_generation)
                if runtime._pool is None:
                    raise LocalWorkerError("session_contract")
                runtime._pool.check(deadline)
                with journal._transaction() as connection:
                    connection.execute("BEGIN")
                    before, before_digest, retained = journal._latest(connection)
                    if request.journal_id != before.journal_id:
                        raise ValidationError("request belongs to another journal")
                    existing = journal._request(connection, request.request_id, before, binding)
                    if existing is not None:
                        return existing
                if before.generation != request.expected_generation:
                    raise RecoveryConflict("journal generation changed before execution")
                if request.start_position != before.checkpoint.next_position:
                    raise ValidationError("request source position differs from durable head")
                if request.cause == "wave":
                    candidate = runtime._candidate_batch(
                        before.checkpoint, request.start_position, request.records, deadline
                    )
                    checkpoint, outputs = candidate.checkpoint, candidate.outputs
                else:
                    checkpoint, outputs = replace(before.checkpoint, source_closed=True), ()
                changed = checkpoint != before.checkpoint
                after = PartitionedFlowRecoveryPoint(
                    before.journal_id, before.generation + changed, checkpoint
                )
                payload = after.to_json()
                digest = _digest(payload)
                receipt = PartitionedFlowReceipt(
                    before.journal_id,
                    request.request_id,
                    binding[0],
                    "committed" if changed else "no_op",
                    before.generation,
                    request.cause,
                    _point_progress(before.checkpoint),
                    _point_progress(checkpoint),
                    before_digest,
                    digest,
                )
                receipt_payload = receipt.to_json()
                rows = []
                output_size = 0
                for output in outputs:
                    wire = _encoded(
                        PartitionedJournalOutput(after.generation, output).to_dict(), _OUTPUT_BYTES
                    )
                    output_size += len(wire.encode("utf-8"))
                    if output_size > _BATCH_BYTES:
                        raise ValidationError("durable wave output wire exceeded")
                    rows.append((output.sequence, after.generation, wire, _digest(wire)))
                projected = (
                    retained
                    - len(before.to_json().encode("utf-8"))
                    + len(payload.encode("utf-8"))
                    + output_size
                    + 64 * len(rows)
                    + (len(receipt_payload.encode("utf-8")) + 96 if changed else 0)
                )
                if projected > _STORE_BYTES:
                    raise ValidationError("projected journal retained wire exceeded")
                pool = runtime._pool
                if pool is None:
                    raise LocalWorkerError("session_contract")
                # Cancellation linearizes before final admission or after the COMMIT attempt.
                # It cannot undo a COMMIT in progress or make a lost ACK imply rollback.
                with runtime._publication, journal._transaction() as connection:
                    connection.execute("BEGIN IMMEDIATE" if changed else "BEGIN")
                    current, current_digest, _ = journal._latest(connection)
                    existing = journal._request(connection, request.request_id, current, binding)
                    if existing is not None:
                        return existing
                    if current != before or current_digest != before_digest:
                        raise RecoveryConflict("journal changed during candidate execution")
                    pool.check(deadline)
                    pool.check_alive()
                    if changed:
                        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
                        connection.execute(
                            f"PRAGMA max_page_count = {_DATABASE_BYTES // page_size}"
                        )
                        connection.execute(
                            "INSERT INTO partition_commit VALUES (?, ?, ?, ?)",
                            (
                                after.generation,
                                request.request_id,
                                receipt_payload,
                                _digest(receipt_payload),
                            ),
                        )
                        connection.executemany(
                            "INSERT INTO partition_output VALUES (?, ?, ?, ?)", rows
                        )
                        connection.execute(
                            "UPDATE partition_head SET payload=?, digest=? WHERE slot=1",
                            (payload, digest),
                        )
                    # This is the last reversible boundary; COMMIT follows in the scope's exit.
                    pool.check(deadline)
                return receipt
            except sqlite3.Error as exc:
                raise OutputError(
                    "partitioned SQL operation failed; inspect request before replay"
                ) from exc
            except BaseException as primary:
                if runtime._phase != "failed" and (
                    isinstance(primary, LocalWorkerError) or not isinstance(primary, Exception)
                ):
                    # Candidate execution already owns its failure cleanup, including a
                    # cleanup exception. Only failures outside that scope settle here.
                    runtime._phase = "failed"
                    if runtime._pool is not None:
                        runtime._pool.cleanup(primary)
                raise
