"""SQMJ: bounded multi-source operation/state/output publication in local SQLite.

Callbacks run on detached runtimes before CAS. Request receipts make publication
idempotent, not callback effects. Source positions and checksums are not broker
acknowledgements, producer authentication, or proofs of historical execution.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal, cast

from .dataflow import _count
from .errors import OutputError, ValidationError
from .flow_journal import _connection_scope, _digest, _finish_connection
from .multi_graph import FlowJoin, GraphInput, MultiGraphDataflow, MultiGraphRuntime
from .multi_journal_types import (
    _BATCH_BYTES,
    _HEAD_BYTES,
    _MAX_BATCH_OUTPUTS,
    _MAX_COMMANDS,
    _MAX_HISTORY,
    _METADATA_BYTES,
    _OPERATION_BYTES,
    _OUTPUT_BYTES,
    _RECEIPT_BYTES,
    GraphDrain,
    GraphEOF,
    MultiGraphJournalOutput,
    MultiGraphOperation,
    MultiGraphOutputCursor,
    MultiGraphOutputPage,
    MultiGraphReceipt,
    MultiGraphRecoveryPoint,
    MultiGraphRequest,
    _checked,
    _command_document,
    _encoded,
    _hex,
)
from .recovery import RecoveryConflict

_APPLICATION_ID = 0x53514D4A
_LABEL = "multi-source journal"
_PAGE_METADATA_ROWS = 2000
_PAGE_METADATA_BYTES = 32 * 1024 * 1024
_SCHEMA = {
    "multi_head": "CREATE TABLE multi_head (slot INTEGER PRIMARY KEY CHECK(slot=1), "
    "payload TEXT NOT NULL, digest TEXT NOT NULL)",
    "multi_commit": "CREATE TABLE multi_commit (generation INTEGER PRIMARY KEY, "
    "request_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, digest TEXT NOT NULL)",
    "multi_operation": "CREATE TABLE multi_operation (seq INTEGER PRIMARY KEY, "
    "generation INTEGER NOT NULL REFERENCES multi_commit(generation), "
    "command_index INTEGER NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL, "
    "UNIQUE(generation, command_index))",
    "multi_output": "CREATE TABLE multi_output (seq INTEGER PRIMARY KEY, "
    "operation_seq INTEGER NOT NULL REFERENCES multi_operation(seq), "
    "payload TEXT NOT NULL, digest TEXT NOT NULL)",
}
_SELECT_HEAD = (
    "SELECT "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM multi_head WHERE slot=1"
)
_COMMIT_BY_GENERATION = (
    "SELECT generation, CASE WHEN typeof(request_id)='text' "
    "AND length(CAST(request_id AS BLOB))=32 THEN request_id END, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM multi_commit WHERE generation=?"
)
_COMMIT_BY_REQUEST = (
    "SELECT generation, CASE WHEN typeof(request_id)='text' "
    "AND length(CAST(request_id AS BLOB))=32 THEN request_id END, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM multi_commit WHERE request_id=?"
)
_SELECT_OPERATIONS = (
    "SELECT seq, generation, command_index, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) "
    "FROM multi_operation WHERE generation=? ORDER BY seq LIMIT ?"
)
_SELECT_OPERATION = (
    "SELECT seq, generation, command_index, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM multi_operation WHERE seq=?"
)
_SELECT_OUTPUT = (
    "SELECT seq, operation_seq, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM multi_output WHERE seq=?"
)
_SELECT_OUTPUTS = (
    "SELECT seq, operation_seq, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) "
    "FROM multi_output WHERE seq>=? AND seq<? ORDER BY seq LIMIT ?"
)


class MultiGraphJournal:
    """Append-only local journal. Instances own no persistent SQLite connections."""

    def __init__(
        self,
        path: str | Path,
        flow: MultiGraphDataflow,
        source_commitments: Mapping[str, str],
        *,
        create: bool = True,
    ) -> None:
        initial_runtime = MultiGraphRuntime(flow)  # Before touching the path.
        if not isinstance(source_commitments, Mapping) or not 1 <= len(source_commitments) <= 16:
            raise ValidationError("source commitments must be a mapping of 1..16 sources")
        captured: dict[str, str] = {}
        for i, (name, digest) in enumerate(source_commitments.items()):
            if i >= 16 or type(name) is not str or name in captured:
                raise ValidationError("invalid source commitment mapping")
            _hex(digest)
            captured[name] = digest
        names = tuple(entry.source_id for entry in flow.entries)
        if set(captured) != set(names):
            raise ValidationError("commitments must cover exactly the graph sources")
        if type(create) is not bool:
            raise ValidationError("create must be boolean")
        self.flow = flow
        self.source_commitments = tuple((n, captured[n]) for n in names)
        self.path = Path(path).absolute()
        self._initial_checkpoint = initial_runtime.checkpoint()
        self._journal_id: str | None = None
        nonterminal = {edge.source for edge in flow.edges}
        self._terminals = {
            name: rank for rank, name in enumerate(flow.execution_order) if name not in nonterminal
        }
        self._final_joins = {
            n.step_id for n in flow.nodes if type(n) is FlowJoin and n.join.emit_mode == "final"
        }
        if not create and not self.path.is_file():
            raise ValidationError("multi-source journal does not exist")
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
                    and connection.execute("PRAGMA application_id").fetchone() == (0,)
                    and connection.execute("PRAGMA user_version").fetchone() == (0,)
                    and create
                ):
                    connection.execute("PRAGMA application_id = 1397837130")
                    connection.execute("PRAGMA user_version = 1")
                    for sql in _SCHEMA.values():
                        connection.execute(sql)
                    point = self._initial(uuid.uuid4().hex)
                    payload = _encoded(point.to_dict(), _HEAD_BYTES)
                    connection.execute(
                        "INSERT INTO multi_head VALUES (1, ?, ?)", (payload, _digest(payload))
                    )
                point, _ = self._latest(connection)
                self._journal_id = point.journal_id
        except (sqlite3.Error, OSError) as exc:
            raise OutputError("cannot initialize multi-source journal") from exc

    def _initial(self, journal_id: str) -> MultiGraphRecoveryPoint:
        return MultiGraphRecoveryPoint(
            journal_id, self.source_commitments, 0, self._initial_checkpoint
        )

    def _initial_digest(self, journal_id: str) -> str:
        return _digest(_encoded(self._initial(journal_id).to_dict(), _HEAD_BYTES))

    def _connect(self, *, create: bool = False) -> sqlite3.Connection:
        uri = self.path.as_uri() + ("?mode=rwc" if create else "?mode=rw")
        connection = sqlite3.connect(uri, uri=True, timeout=10)
        try:
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
        if connection.execute("PRAGMA application_id").fetchone() != (
            _APPLICATION_ID,
        ) or connection.execute("PRAGMA user_version").fetchone() != (1,):
            raise ValidationError("unsupported or unrelated multi-source journal schema")
        if connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchone() != (4,):
            raise ValidationError("unexpected multi-source journal schema objects")
        rows = connection.execute(
            "SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END, "
            "CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END, "
            "CASE WHEN length(CAST(sql AS BLOB))<=4096 THEN sql END "
            "FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        if any(kind != "table" or _SCHEMA.get(name) != sql for name, kind, sql in rows):
            raise ValidationError("multi-source journal schema definition changed")

    def _receipt_row(self, row: Any) -> tuple[MultiGraphReceipt, str, int]:
        if row is None:
            raise ValidationError("missing committed request receipt")
        generation, request_id, payload, digest, size = row
        receipt = MultiGraphReceipt.from_dict(_checked((payload, digest, size), _RECEIPT_BYTES))
        if (
            receipt.status != "committed"
            or receipt.after_generation != generation
            or receipt.request_id != request_id
        ):
            raise ValidationError("receipt does not match its SQL index")
        return receipt, digest, size

    def _receipt(
        self, connection: sqlite3.Connection, generation: int
    ) -> tuple[MultiGraphReceipt, str, int]:
        return self._receipt_row(
            connection.execute(_COMMIT_BY_GENERATION, (_RECEIPT_BYTES, generation)).fetchone()
        )

    def _chain(
        self,
        connection: sqlite3.Connection,
        receipt: MultiGraphReceipt,
        point: MultiGraphRecoveryPoint,
    ) -> int:
        if (
            tuple(n for n, _, _ in receipt.before_sources)
            != tuple(n for n, _ in self.source_commitments)
            or receipt.journal_id != point.journal_id
            or receipt.after_generation > point.generation
            or receipt.after_operation > point.checkpoint.operation_sequence
            or receipt.output_stop > point.checkpoint.emitted_records
        ):
            raise ValidationError("receipt lies outside its journal prefix")
        if receipt.before_generation:
            previous, _, size = self._receipt(connection, receipt.before_generation)
            if (
                previous.journal_id != receipt.journal_id
                or previous.after_head_digest != receipt.before_head_digest
                or previous.after_operation != receipt.before_operation
                or previous.output_stop != receipt.output_start
                or previous.after_sources != receipt.before_sources
            ):
                raise ValidationError("adjacent receipt boundaries are inconsistent")
            return size
        if (
            receipt.before_operation
            or receipt.output_start
            or receipt.before_sources != self._initial_checkpoint.sources
            or receipt.before_head_digest != self._initial_digest(point.journal_id)
        ):
            raise ValidationError("first receipt does not begin at the initial checkpoint")
        return 0

    def _latest(self, connection: sqlite3.Connection) -> tuple[MultiGraphRecoveryPoint, str]:
        self._schema(connection)
        row = connection.execute(_SELECT_HEAD, (_HEAD_BYTES,)).fetchone()
        if connection.execute("SELECT count(*) FROM multi_head").fetchone() != (1,):
            raise ValidationError("missing or duplicate journal head")
        point = MultiGraphRecoveryPoint.from_dict(_checked(row, _HEAD_BYTES))
        if (
            point.source_commitments != self.source_commitments
            or point.checkpoint.identity != self.flow.identity
            or (self._journal_id is not None and point.journal_id != self._journal_id)
        ):
            raise ValidationError("journal, flow, or source identity mismatch")
        MultiGraphRuntime.from_checkpoint(self.flow, point.checkpoint)
        for sql, expected, first in (
            (
                "SELECT count(*), min(generation), max(generation) FROM multi_commit",
                point.generation,
                1,
            ),
            (
                "SELECT count(*), min(seq), max(seq) FROM multi_operation",
                point.checkpoint.operation_sequence,
                1,
            ),
            (
                "SELECT count(*), min(seq), max(seq) FROM multi_output",
                point.checkpoint.emitted_records,
                0,
            ),
        ):
            count, low, high = connection.execute(sql).fetchone()
            if count != expected or (count and (low != first or high != first + count - 1)):
                raise ValidationError("journal table prefix contradicts current head")
        digest = row[1]
        if point.generation:
            receipt, _, _ = self._receipt(connection, point.generation)
            self._chain(connection, receipt, point)
            if (
                receipt.after_head_digest != digest
                or receipt.after_sources != point.checkpoint.sources
                or receipt.after_operation != point.checkpoint.operation_sequence
                or receipt.output_stop != point.checkpoint.emitted_records
            ):
                raise ValidationError("latest receipt does not bind the current head")
        elif point != self._initial(point.journal_id):
            raise ValidationError("initial journal checkpoint is not empty")
        return point, digest

    def latest(self) -> MultiGraphRecoveryPoint:
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                return point
        except sqlite3.Error as exc:
            raise OutputError("cannot read multi-source journal") from exc

    def _operation_row(self, row: Any) -> tuple[MultiGraphOperation, int]:
        if row is None:
            raise ValidationError("missing journal operation")
        seq, generation, index, payload, digest, size = row
        operation = MultiGraphOperation.from_dict(
            _checked((payload, digest, size), _OPERATION_BYTES)
        )
        if (operation.sequence, operation.generation, operation.command_index) != (
            seq,
            generation,
            index,
        ):
            raise ValidationError("operation does not match its SQL index")
        return operation, size

    def _verify_commit(
        self,
        connection: sqlite3.Connection,
        receipt: MultiGraphReceipt,
        point: MultiGraphRecoveryPoint,
        receipt_bytes: int,
    ) -> tuple[tuple[MultiGraphOperation, ...], int]:
        extra = self._chain(connection, receipt, point)
        expected = receipt.after_operation - receipt.before_operation
        if connection.execute(
            "SELECT count(*) FROM multi_operation WHERE generation=?", (receipt.after_generation,)
        ).fetchone() != (expected,):
            raise ValidationError("receipt operation count is inconsistent")
        sources = {name: (position, closed) for name, position, closed in receipt.before_sources}
        ops: list[MultiGraphOperation] = []
        size = receipt_bytes
        last_index = -1
        output = receipt.output_start
        for row in connection.execute(
            _SELECT_OPERATIONS, (_OPERATION_BYTES, receipt.after_generation, _MAX_COMMANDS + 1)
        ):
            op, byte_size = self._operation_row(row)
            size += byte_size
            if size > _METADATA_BYTES or len(ops) >= expected:
                raise ValidationError("commit metadata exceeds its bound")
            if (
                op.sequence != receipt.before_operation + len(ops) + 1
                or op.generation != receipt.after_generation
                or not last_index < op.command_index < receipt.command_count
                or op.output_start != output
            ):
                raise ValidationError(
                    "operation sequence, command index, or output range is inconsistent"
                )
            if op.cause == "drain":
                if op.join_id not in self._final_joins:
                    raise ValidationError("drain operation names no configured final join")
            else:
                if op.source_id not in sources:
                    raise ValidationError("operation names an unknown source")
                position, closed = sources[op.source_id]
                if closed or op.position != position:
                    raise ValidationError("operation contradicts its source position or EOF")
                sources[op.source_id] = (position + (op.cause == "process"), op.cause == "eof")
            output, last_index = op.output_stop, op.command_index
            ops.append(op)
        after = tuple((name, *sources[name]) for name, _, _ in receipt.before_sources)
        if len(ops) != expected or after != receipt.after_sources or output != receipt.output_stop:
            raise ValidationError("operation metadata does not reproduce receipt boundaries")
        return tuple(ops), size + extra

    def _request(
        self,
        connection: sqlite3.Connection,
        request_id: str,
        point: MultiGraphRecoveryPoint,
        expected: tuple[str, int] | None = None,
    ) -> MultiGraphReceipt | None:
        row = connection.execute(_COMMIT_BY_REQUEST, (_RECEIPT_BYTES, request_id)).fetchone()
        if row is None:
            return None
        receipt, _, size = self._receipt_row(row)
        if expected is not None and (
            receipt.request_digest != expected[0] or receipt.before_generation != expected[1]
        ):
            raise RecoveryConflict("request ID is already bound to different command content")
        self._verify_commit(connection, receipt, point, size)
        return receipt

    def request(self, request_id: str) -> MultiGraphReceipt | None:
        """None means absent at this snapshot, not proof that no writer is in flight."""
        _hex(request_id, 32)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                return self._request(connection, request_id, point)
        except sqlite3.Error as exc:
            raise OutputError("cannot inspect committed request") from exc

    def operation(self, sequence: int) -> MultiGraphOperation:
        """Read one cause after verifying its bounded commit metadata, without replay."""
        _count(sequence, "operation sequence", 1, _MAX_HISTORY)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                op, _ = self._operation_row(
                    connection.execute(_SELECT_OPERATION, (_OPERATION_BYTES, sequence)).fetchone()
                )
                receipt, _, size = self._receipt(connection, op.generation)
                operations, _ = self._verify_commit(connection, receipt, point, size)
                if op not in operations:
                    raise ValidationError("operation is outside its commit")
                return op
        except sqlite3.Error as exc:
            raise OutputError("cannot read journal operation") from exc

    def apply(self, request: MultiGraphRequest) -> MultiGraphReceipt:
        """Publish one complete request; no callback retry and no implicit source pull."""
        if type(request) is not MultiGraphRequest:
            raise ValidationError("apply requires MultiGraphRequest")
        request_digest = request.digest  # Content work precedes any SQL transaction.
        binding = (request_digest, request.expected_generation)
        if request.journal_id != self._journal_id:
            raise ValidationError("request targets a different journal")
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                before, before_digest = self._latest(connection)
                existing = self._request(connection, request.request_id, before, binding)
                if existing is not None:
                    return existing
            if before.generation != request.expected_generation:
                raise RecoveryConflict("journal generation changed before processing")
            runtime = MultiGraphRuntime.from_checkpoint(self.flow, before.checkpoint)
            operation_sequence = before.checkpoint.operation_sequence
            operation_rows: list[tuple[int, int, int, str, str]] = []
            output_rows: list[tuple[int, int, str, str]] = []
            output_size = metadata_size = 0
            for index, command in enumerate(request.commands):
                selected = (
                    runtime.ready_joins[0]
                    if type(command) is GraphDrain and runtime.ready_joins
                    else None
                )
                if type(command) is GraphInput:
                    batch = runtime.process(command)
                elif type(command) is GraphEOF:
                    batch = runtime.close(command.source_id, next_position=command.next_position)
                else:
                    batch = runtime.drain(max_keys=cast(GraphDrain, command).max_keys)
                if batch.operation_sequence == operation_sequence:
                    continue
                if (
                    batch.operation_sequence != operation_sequence + 1
                    or batch.operation_sequence > _MAX_HISTORY
                ):
                    raise ValidationError("operation history capacity exceeded")
                start = before.checkpoint.emitted_records + len(output_rows)
                if (
                    len(output_rows) + len(batch.outputs) > _MAX_BATCH_OUTPUTS
                    or start + len(batch.outputs) > _MAX_HISTORY
                ):
                    raise ValidationError("journal transaction output capacity exceeded")
                for output in batch.outputs:
                    item = MultiGraphJournalOutput(
                        before.checkpoint.emitted_records + len(output_rows),
                        batch.operation_sequence,
                        output.step_id,
                        output.record,
                    )
                    payload = _encoded(item.to_dict(), _OUTPUT_BYTES)
                    output_size += len(payload.encode("utf-8"))
                    if output_size > _BATCH_BYTES:
                        raise ValidationError("journal transaction output bytes exceeded")
                    output_rows.append(
                        (item.sequence, item.operation_sequence, payload, _digest(payload))
                    )
                cause: Literal["process", "eof", "drain"] = (
                    "process"
                    if type(command) is GraphInput
                    else "eof"
                    if type(command) is GraphEOF
                    else "drain"
                )
                op = MultiGraphOperation(
                    batch.operation_sequence,
                    before.generation + 1,
                    index,
                    cause,
                    command.source_id if isinstance(command, (GraphInput, GraphEOF)) else None,
                    command.position
                    if type(command) is GraphInput
                    else command.next_position
                    if type(command) is GraphEOF
                    else None,
                    selected,
                    command.max_keys if type(command) is GraphDrain else None,
                    _digest(_encoded(_command_document(command), 16 * 1024 * 1024))
                    if type(command) is GraphInput
                    else None,
                    start,
                    before.checkpoint.emitted_records + len(output_rows),
                )
                payload = _encoded(op.to_dict(), _OPERATION_BYTES)
                metadata_size += len(payload.encode("utf-8"))
                if metadata_size > _METADATA_BYTES:
                    raise ValidationError("transaction operation metadata exceeded")
                operation_rows.append(
                    (op.sequence, op.generation, index, payload, _digest(payload))
                )
                operation_sequence = batch.operation_sequence
            changed = bool(operation_rows)
            after = MultiGraphRecoveryPoint(
                before.journal_id,
                self.source_commitments,
                before.generation + changed,
                runtime.checkpoint(),
            )
            # The checkpoint is nested as an object. Count the complete actual UTF-8 envelope.
            head_payload = _encoded(after.to_dict(), _HEAD_BYTES)
            head_digest = _digest(head_payload)
            receipt = MultiGraphReceipt(
                before.journal_id,
                request.request_id,
                request_digest,
                "committed" if changed else "no_op",
                len(request.commands),
                before.generation,
                after.generation,
                before.checkpoint.operation_sequence,
                after.checkpoint.operation_sequence,
                before.checkpoint.emitted_records,
                after.checkpoint.emitted_records,
                before.checkpoint.sources,
                after.checkpoint.sources,
                before_digest,
                head_digest,
            )
            receipt_payload = _encoded(receipt.to_dict(), _RECEIPT_BYTES)
            if metadata_size + len(receipt_payload.encode("utf-8")) > _METADATA_BYTES:
                raise ValidationError("transaction receipt and operation metadata exceeded")
            if not changed:
                # All allocations, including the no-op result, precede the final full-head check.
                with self._transaction() as connection:
                    connection.execute("BEGIN")
                    current, digest = self._latest(connection)
                    if current != before or digest != before_digest:
                        raise RecoveryConflict("journal changed during no-op request")
                return receipt
            with self._transaction() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current, digest = self._latest(connection)
                existing = self._request(connection, request.request_id, current, binding)
                if existing is not None:
                    return existing  # Same-ID race: publication once; callbacks may have run twice.
                if current != before or digest != before_digest:
                    raise RecoveryConflict("journal changed during processing; no result committed")
                connection.execute(
                    "INSERT INTO multi_commit VALUES (?, ?, ?, ?)",
                    (
                        after.generation,
                        request.request_id,
                        receipt_payload,
                        _digest(receipt_payload),
                    ),
                )
                connection.executemany(
                    "INSERT INTO multi_operation VALUES (?, ?, ?, ?, ?)", operation_rows
                )
                connection.executemany("INSERT INTO multi_output VALUES (?, ?, ?, ?)", output_rows)
                connection.execute(
                    "UPDATE multi_head SET payload=?, digest=? WHERE slot=1",
                    (head_payload, head_digest),
                )
            return receipt
        except sqlite3.Error as exc:
            raise OutputError(
                "multi-source journal SQL operation failed; inspect request receipt before retry"
            ) from exc

    def output_cursor(self, *, start: int = 0) -> MultiGraphOutputCursor:
        _count(start, "output start", 0, _MAX_HISTORY)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                digest = (
                    self._receipt(connection, point.generation)[1]
                    if point.generation
                    else self._initial_digest(point.journal_id)
                )
                cursor = MultiGraphOutputCursor(
                    point.journal_id,
                    point.generation,
                    digest,
                    point.checkpoint.emitted_records,
                    start,
                )
                cursor.to_json()  # Check the actual complete cursor envelope before return.
                return cursor
        except sqlite3.Error as exc:
            raise OutputError("cannot capture output cursor") from exc

    def _output_row(self, row: Any) -> tuple[MultiGraphJournalOutput, int]:
        if row is None:
            raise ValidationError("missing journal output")
        seq, op_seq, payload, digest, size = row
        output = MultiGraphJournalOutput.from_dict(_checked((payload, digest, size), _OUTPUT_BYTES))
        if (output.sequence, output.operation_sequence) != (seq, op_seq):
            raise ValidationError("output does not match its SQL index")
        if (
            output.step_id not in self._terminals
            or output.record.byte_size > self.flow.limits.graph.operator_limits.max_record_bytes
        ):
            raise ValidationError("output violates its configured terminal/record contract")
        return output, size

    def read_outputs(
        self,
        cursor: MultiGraphOutputCursor,
        *,
        limit: int = 1000,
        max_bytes: int = 16 * 1024 * 1024,
    ) -> MultiGraphOutputPage:
        """Detach one bounded page; its cursor is a read position, never a consumer ack."""
        if type(cursor) is not MultiGraphOutputCursor:
            raise ValidationError("read_outputs requires MultiGraphOutputCursor")
        cursor.__post_init__()
        _count(limit, "page limit", 1, 1000)
        _count(max_bytes, "page bytes", 1, _BATCH_BYTES)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                if (
                    cursor.journal_id != point.journal_id
                    or cursor.anchor_generation > point.generation
                ):
                    raise ValidationError("cursor does not belong to the retained journal prefix")
                if cursor.anchor_generation:
                    anchor, digest, _ = self._receipt(connection, cursor.anchor_generation)
                    self._chain(connection, anchor, point)
                    stop = anchor.output_stop
                else:
                    digest, stop = self._initial_digest(point.journal_id), 0
                if cursor.anchor_receipt_digest != digest or cursor.stop_sequence != stop:
                    raise ValidationError("cursor anchor or fixed stop changed")
                outputs: list[MultiGraphJournalOutput] = []
                metadata: dict[int, MultiGraphOperation] = {}
                checked_generations: set[int] = set()
                metadata_bytes = metadata_count = output_bytes = 0
                previous: MultiGraphJournalOutput | None = None
                if cursor.next_sequence:
                    previous, _ = self._output_row(
                        connection.execute(
                            _SELECT_OUTPUT, (_OUTPUT_BYTES, cursor.next_sequence - 1)
                        ).fetchone()
                    )
                rows = connection.execute(
                    _SELECT_OUTPUTS,
                    (_OUTPUT_BYTES, cursor.next_sequence, cursor.stop_sequence, limit),
                )
                budget_limited = False
                for row in rows:
                    item, byte_size = self._output_row(row)
                    if item.sequence != cursor.next_sequence + len(outputs):
                        raise ValidationError("output page is not contiguous")
                    if output_bytes + byte_size > max_bytes:
                        if not outputs:
                            raise ValidationError("next output exceeds the page byte budget")
                        budget_limited = True
                        break
                    op, _ = self._operation_row(
                        connection.execute(
                            _SELECT_OPERATION, (_OPERATION_BYTES, item.operation_sequence)
                        ).fetchone()
                    )
                    if op.generation > cursor.anchor_generation:
                        raise ValidationError("output lies beyond cursor generation")
                    if op.generation not in checked_generations:
                        receipt, _, size = self._receipt(connection, op.generation)
                        # Bound the prospective scan before fetching its operation rows.
                        count = receipt.after_operation - receipt.before_operation
                        if (
                            metadata_count + count > _PAGE_METADATA_ROWS
                            or metadata_bytes + _METADATA_BYTES + _RECEIPT_BYTES
                            > _PAGE_METADATA_BYTES
                        ):
                            if outputs:
                                budget_limited = True
                                break
                            raise ValidationError("commit metadata exceeds page admission")
                        operations, size = self._verify_commit(connection, receipt, point, size)
                        metadata.update((operation.sequence, operation) for operation in operations)
                        checked_generations.add(op.generation)
                        metadata_count += count
                        metadata_bytes += size
                    if (
                        op != metadata.get(op.sequence)
                        or not op.output_start <= item.sequence < op.output_stop
                    ):
                        raise ValidationError("output does not belong to its operation range")
                    if previous is not None and (
                        item.operation_sequence < previous.operation_sequence
                        or (
                            item.operation_sequence == previous.operation_sequence
                            and self._terminals[item.step_id] < self._terminals[previous.step_id]
                        )
                    ):
                        raise ValidationError("output operation or terminal order is invalid")
                    outputs.append(item)
                    output_bytes += byte_size
                    previous = item
                if not budget_limited and len(outputs) != min(
                    limit, cursor.stop_sequence - cursor.next_sequence
                ):
                    raise ValidationError("output query returned an incomplete prefix")
                result = MultiGraphOutputPage(
                    tuple(outputs),
                    replace(cursor, next_sequence=cursor.next_sequence + len(outputs)),
                )
            return result
        except sqlite3.Error as exc:
            raise OutputError("cannot read multi-source output page") from exc


__all__ = [
    "GraphDrain",
    "GraphEOF",
    "MultiGraphJournal",
    "MultiGraphJournalOutput",
    "MultiGraphOperation",
    "MultiGraphOutputCursor",
    "MultiGraphOutputPage",
    "MultiGraphReceipt",
    "MultiGraphRecoveryPoint",
    "MultiGraphRequest",
]
