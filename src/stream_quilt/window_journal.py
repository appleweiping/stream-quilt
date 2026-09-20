"""Atomic local source, window-graph state, operation and output publication.

User callbacks run on a detached runtime before the SQLite write transaction.
The journal never claims to own a source, a sink, or callback side effects.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal

from .dataflow import _count
from .errors import OutputError, ValidationError
from .flow_journal import _connection_scope, _digest, _finish_connection
from .recovery import RecoveryConflict, _input_id
from .window_graph import WindowGraphDataflow, WindowGraphInput, WindowGraphRuntime
from .window_graph_checkpoint import WindowGraphCheckpoint
from .window_journal_types import (
    _BATCH_BYTES,
    _HEAD_BYTES,
    _MAX_BATCH_OUTPUTS,
    _MAX_COMMANDS,
    _MAX_HISTORY,
    _METADATA_BYTES,
    _OPERATION_BYTES,
    _OUTPUT_BYTES,
    _RECEIPT_BYTES,
    _REQUEST_BYTES,
    WindowGraphDrain,
    WindowGraphFinish,
    WindowGraphJournalOutput,
    WindowGraphOperation,
    WindowGraphOutputCursor,
    WindowGraphOutputPage,
    WindowGraphReceipt,
    WindowGraphRecoveryPoint,
    WindowGraphRequest,
    WindowGraphWatermark,
    _checked,
    _command_document,
    _encoded,
    _hex,
)

_APPLICATION_ID = 0x5351574A  # SQWJ; never reinterpret an existing journal schema.
_LABEL = "window graph journal"
_PAGE_METADATA_ROWS = 2_000
_PAGE_METADATA_BYTES = 32 * 1024 * 1024
_SCHEMA = {
    "window_head": "CREATE TABLE window_head (slot INTEGER PRIMARY KEY CHECK(slot=1), "
    "payload TEXT NOT NULL, digest TEXT NOT NULL)",
    "window_commit": "CREATE TABLE window_commit (generation INTEGER PRIMARY KEY, "
    "request_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL, digest TEXT NOT NULL)",
    "window_operation": "CREATE TABLE window_operation (seq INTEGER PRIMARY KEY, "
    "generation INTEGER NOT NULL REFERENCES window_commit(generation), "
    "command_index INTEGER NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL, "
    "UNIQUE(generation, command_index))",
    "window_output": "CREATE TABLE window_output (seq INTEGER PRIMARY KEY, "
    "operation_seq INTEGER NOT NULL REFERENCES window_operation(seq), "
    "payload TEXT NOT NULL, digest TEXT NOT NULL)",
}
_SELECT_HEAD = (
    "SELECT CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM window_head WHERE slot=1"
)
_SELECT_COMMIT = (
    "SELECT generation, CASE WHEN typeof(request_id)='text' "
    "AND length(CAST(request_id AS BLOB))=32 THEN request_id END, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM window_commit WHERE generation=?"
)
_SELECT_REQUEST = (
    "SELECT generation, CASE WHEN typeof(request_id)='text' "
    "AND length(CAST(request_id AS BLOB))=32 THEN request_id END, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM window_commit WHERE request_id=?"
)
_SELECT_OPERATIONS = (
    "SELECT seq, generation, command_index, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) "
    "FROM window_operation WHERE generation=? ORDER BY seq LIMIT ?"
)
_SELECT_OPERATION = (
    "SELECT seq, generation, command_index, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM window_operation WHERE seq=?"
)
_SELECT_OUTPUT = (
    "SELECT seq, operation_seq, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) FROM window_output WHERE seq=?"
)
_SELECT_OUTPUTS = (
    "SELECT seq, operation_seq, "
    "CASE WHEN typeof(payload)='text' AND length(CAST(payload AS BLOB))<=? "
    "THEN payload END, CASE WHEN typeof(digest)='text' AND length(CAST(digest AS BLOB))=64 "
    "THEN digest END, length(CAST(payload AS BLOB)) "
    "FROM window_output WHERE seq>=? AND seq<? ORDER BY seq LIMIT ?"
)


def _progress(
    checkpoint: WindowGraphCheckpoint,
) -> tuple[int, int, bool, int | None, int, int, int]:
    body = checkpoint.to_dict()["body"]
    counters = body["counters"]
    return (
        body["operation_sequence"],
        body["next_position"],
        body["source_finished"],
        body["window"]["body"]["watermark"],
        counters["watermark_advances"],
        counters["drain_operations"],
        counters["emitted_records"],
    )


class WindowGraphJournal:
    """One durable explicit-watermark graph; no persistent connection is held."""

    def __init__(
        self,
        path: str | Path,
        flow: WindowGraphDataflow,
        source_id: str,
        source_commitment: str,
        *,
        create: bool = True,
    ) -> None:
        initial_runtime = WindowGraphRuntime(flow)  # Validate before touching the path.
        _input_id(source_id)
        _hex(source_commitment)
        if type(create) is not bool:
            raise ValidationError("create must be boolean")
        self.flow = flow
        self.source_id = source_id
        self.source_commitment = source_commitment
        self.path = Path(path).absolute()
        self._initial_checkpoint = initial_runtime.checkpoint()
        self._journal_id: str | None = None
        nonterminal = {edge.source for edge in flow.edges}
        self._terminals = {node.step_id for node in flow.nodes if node.step_id not in nonterminal}
        if not create and not self.path.is_file():
            raise ValidationError("window graph journal does not exist")
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
                    connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
                    connection.execute("PRAGMA user_version = 1")
                    for statement in _SCHEMA.values():
                        connection.execute(statement)
                    point = self._initial(uuid.uuid4().hex)
                    payload = _encoded(point.to_dict(), _HEAD_BYTES)
                    connection.execute(
                        "INSERT INTO window_head VALUES (1, ?, ?)", (payload, _digest(payload))
                    )
                point, _ = self._latest(connection)
                self._journal_id = point.journal_id
        except (sqlite3.Error, OSError) as error:
            raise OutputError("cannot initialize window graph journal") from error

    def _initial(self, journal_id: str) -> WindowGraphRecoveryPoint:
        return WindowGraphRecoveryPoint(
            journal_id, self.source_id, self.source_commitment, 0, self._initial_checkpoint
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
            raise ValidationError("unrelated or unsupported window graph journal")
        if connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchone() != (4,):
            raise ValidationError("unexpected window graph journal schema objects")
        rows = connection.execute(
            "SELECT CASE WHEN length(CAST(name AS BLOB))<=64 THEN name END, "
            "CASE WHEN length(CAST(type AS BLOB))<=16 THEN type END, "
            "CASE WHEN length(CAST(sql AS BLOB))<=4096 THEN sql END "
            "FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        if any(kind != "table" or _SCHEMA.get(name) != sql for name, kind, sql in rows):
            raise ValidationError("window graph journal schema definition changed")

    def _receipt_row(self, row: Any) -> tuple[WindowGraphReceipt, str, int]:
        if row is None:
            raise ValidationError("missing committed window request receipt")
        generation, request_id, payload, digest, size = row
        receipt = WindowGraphReceipt.from_dict(_checked((payload, digest, size), _RECEIPT_BYTES))
        if (
            receipt.status != "committed"
            or receipt.after_generation != generation
            or receipt.request_id != request_id
        ):
            raise ValidationError("window receipt contradicts its SQL key")
        return receipt, digest, size

    def _receipt(
        self, connection: sqlite3.Connection, generation: int
    ) -> tuple[WindowGraphReceipt, str, int]:
        return self._receipt_row(
            connection.execute(_SELECT_COMMIT, (_RECEIPT_BYTES, generation)).fetchone()
        )

    def _chain(
        self,
        connection: sqlite3.Connection,
        receipt: WindowGraphReceipt,
        point: WindowGraphRecoveryPoint,
    ) -> int:
        if (
            receipt.journal_id != point.journal_id
            or not 1 <= receipt.after_generation <= point.generation
            or receipt.before_generation != receipt.after_generation - 1
        ):
            raise ValidationError("window receipt does not belong to this journal prefix")
        if receipt.before_generation:
            previous, _, size = self._receipt(connection, receipt.before_generation)
            if (
                previous.after_head_digest != receipt.before_head_digest
                or previous.after_operation != receipt.before_operation
                or previous.after_position != receipt.before_position
                or previous.after_finished != receipt.before_finished
                or previous.after_watermark != receipt.before_watermark
                or previous.after_watermarks != receipt.before_watermarks
                or previous.after_drains != receipt.before_drains
                or previous.output_stop != receipt.output_start
            ):
                raise ValidationError("adjacent window receipts are inconsistent")
            return size
        initial = _progress(self._initial_checkpoint)
        if (
            receipt.before_head_digest != self._initial_digest(point.journal_id)
            or (
                receipt.before_operation,
                receipt.before_position,
                receipt.before_finished,
                receipt.before_watermark,
                receipt.before_watermarks,
                receipt.before_drains,
                receipt.output_start,
            )
            != initial
        ):
            raise ValidationError("first window receipt does not begin at the initial state")
        return 0

    def _latest(self, connection: sqlite3.Connection) -> tuple[WindowGraphRecoveryPoint, str]:
        self._schema(connection)
        row = connection.execute(_SELECT_HEAD, (_HEAD_BYTES,)).fetchone()
        if connection.execute("SELECT count(*) FROM window_head").fetchone() != (1,):
            raise ValidationError("missing or duplicate window journal head")
        point = WindowGraphRecoveryPoint.from_dict(_checked(row, _HEAD_BYTES))
        if (
            point.source_id != self.source_id
            or point.source_commitment != self.source_commitment
            or point.checkpoint.to_dict()["body"]["identity"] != self.flow.identity
            or (self._journal_id is not None and point.journal_id != self._journal_id)
        ):
            raise ValidationError("window journal source, flow or lineage mismatch")
        WindowGraphRuntime.from_checkpoint(self.flow, point.checkpoint)
        for sql, expected, first in (
            (
                "SELECT count(*), min(generation), max(generation) FROM window_commit",
                point.generation,
                1,
            ),
            (
                "SELECT count(*), min(seq), max(seq) FROM window_operation",
                _progress(point.checkpoint)[0],
                1,
            ),
            (
                "SELECT count(*), min(seq), max(seq) FROM window_output",
                _progress(point.checkpoint)[6],
                0,
            ),
        ):
            count, low, high = connection.execute(sql).fetchone()
            if count != expected or (count and (low != first or high != first + count - 1)):
                raise ValidationError("window journal table prefix contradicts current head")
        digest = row[1]
        if point.generation:
            receipt, _, _ = self._receipt(connection, point.generation)
            self._chain(connection, receipt, point)
            if receipt.after_head_digest != digest or (
                receipt.after_operation,
                receipt.after_position,
                receipt.after_finished,
                receipt.after_watermark,
                receipt.after_watermarks,
                receipt.after_drains,
                receipt.output_stop,
            ) != _progress(point.checkpoint):
                raise ValidationError("latest receipt does not bind the window head")
        elif point != self._initial(point.journal_id):
            raise ValidationError("initial window journal checkpoint is not empty")
        return point, digest

    def latest(self) -> WindowGraphRecoveryPoint:
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                return point
        except sqlite3.Error as error:
            raise OutputError("cannot read window graph journal") from error

    @property
    def journal_id(self) -> str:
        if self._journal_id is None:
            raise ValidationError("window journal has no initialized lineage")
        return self._journal_id

    def _operation_row(self, row: Any) -> tuple[WindowGraphOperation, int]:
        if row is None:
            raise ValidationError("missing window journal operation")
        sequence, generation, index, payload, digest, size = row
        operation = WindowGraphOperation.from_dict(
            _checked((payload, digest, size), _OPERATION_BYTES)
        )
        if (operation.sequence, operation.generation, operation.command_index) != (
            sequence,
            generation,
            index,
        ):
            raise ValidationError("window operation contradicts its SQL key")
        return operation, size

    def _verify_commit(
        self,
        connection: sqlite3.Connection,
        receipt: WindowGraphReceipt,
        point: WindowGraphRecoveryPoint,
        receipt_bytes: int,
    ) -> tuple[tuple[WindowGraphOperation, ...], int]:
        extra = self._chain(connection, receipt, point)
        expected = receipt.after_operation - receipt.before_operation
        if connection.execute(
            "SELECT count(*) FROM window_operation WHERE generation=?",
            (receipt.after_generation,),
        ).fetchone() != (expected,):
            raise ValidationError("window receipt operation count is inconsistent")
        position = receipt.before_position
        finished = receipt.before_finished
        watermark = receipt.before_watermark
        watermarks = receipt.before_watermarks
        drains = receipt.before_drains
        output = receipt.output_start
        previous_index = -1
        size = receipt_bytes + extra
        operations: list[WindowGraphOperation] = []
        for row in connection.execute(
            _SELECT_OPERATIONS, (_OPERATION_BYTES, receipt.after_generation, _MAX_COMMANDS + 1)
        ):
            operation, byte_size = self._operation_row(row)
            size += byte_size
            if size > _METADATA_BYTES or len(operations) >= expected:
                raise ValidationError("window commit metadata exceeds its byte/row bound")
            if (
                operation.sequence != receipt.before_operation + len(operations) + 1
                or operation.generation != receipt.after_generation
                or not previous_index < operation.command_index < receipt.command_count
                or operation.output_start != output
            ):
                raise ValidationError("window operation sequence/index/output boundary is invalid")
            if operation.cause == "process":
                if finished or operation.position != position:
                    raise ValidationError("window operation contradicts source position or EOF")
                position += 1
            elif operation.cause == "watermark":
                if (
                    finished
                    or operation.position != position
                    or operation.timestamp is None
                    or (watermark is not None and operation.timestamp <= watermark)
                ):
                    raise ValidationError("window watermark operation is not an advance")
                watermark = operation.timestamp
                watermarks += 1
            elif operation.cause == "finish":
                if finished or operation.position != position:
                    raise ValidationError("window finish operation contradicts source prefix")
                finished = True
            else:
                drains += 1
            output = operation.output_stop
            previous_index = operation.command_index
            operations.append(operation)
        if (
            len(operations) != expected
            or position != receipt.after_position
            or finished != receipt.after_finished
            or watermark != receipt.after_watermark
            or watermarks != receipt.after_watermarks
            or drains != receipt.after_drains
            or output != receipt.output_stop
        ):
            raise ValidationError("window operations do not reproduce receipt boundaries")
        return tuple(operations), size

    def _request(
        self,
        connection: sqlite3.Connection,
        request_id: str,
        point: WindowGraphRecoveryPoint,
        expected: tuple[str, int] | None = None,
    ) -> WindowGraphReceipt | None:
        row = connection.execute(_SELECT_REQUEST, (_RECEIPT_BYTES, request_id)).fetchone()
        if row is None:
            return None
        receipt, _, size = self._receipt_row(row)
        if expected is not None and (
            receipt.request_digest != expected[0] or receipt.before_generation != expected[1]
        ):
            raise RecoveryConflict("window request ID is bound to different content")
        self._verify_commit(connection, receipt, point, size)
        return receipt

    def request(self, request_id: str) -> WindowGraphReceipt | None:
        """Return a persisted commit, or absence observed at this read snapshot."""
        _hex(request_id, 32)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                return self._request(connection, request_id, point)
        except sqlite3.Error as error:
            raise OutputError("cannot inspect window graph request") from error

    def operation(self, sequence: int) -> WindowGraphOperation:
        _count(sequence, "window operation sequence", 1, _MAX_HISTORY)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                operation, _ = self._operation_row(
                    connection.execute(_SELECT_OPERATION, (_OPERATION_BYTES, sequence)).fetchone()
                )
                receipt, _, size = self._receipt(connection, operation.generation)
                operations, _ = self._verify_commit(connection, receipt, point, size)
                if operation not in operations:
                    raise ValidationError("window operation is outside its commit")
                return operation
        except sqlite3.Error as error:
            raise OutputError("cannot read window graph operation") from error

    def apply(self, request: WindowGraphRequest) -> WindowGraphReceipt:
        """Publish a complete command request without retrying callback execution."""
        if type(request) is not WindowGraphRequest:
            raise ValidationError("apply requires WindowGraphRequest")
        request_digest = request.digest  # All canonical input admission precedes SQL.
        binding = (request_digest, request.expected_generation)
        if request.journal_id != self.journal_id:
            raise ValidationError("request belongs to a different window journal")
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                before, before_digest = self._latest(connection)
                existing = self._request(connection, request.request_id, before, binding)
                if existing is not None:
                    return existing
            if before.generation != request.expected_generation:
                raise RecoveryConflict("window journal generation changed before processing")
            runtime = WindowGraphRuntime.from_checkpoint(self.flow, before.checkpoint)
            before_progress = _progress(before.checkpoint)
            operation_sequence = before_progress[0]
            operation_rows: list[tuple[int, int, int, str, str]] = []
            output_rows: list[tuple[int, int, str, str]] = []
            output_size = metadata_size = 0
            for index, command in enumerate(request.commands):
                # A guaranteed effective command must fit before any of its
                # user callbacks can run. Equal watermarks, repeated EOF and
                # empty drains remain admissible observations at the ceiling.
                effective = (
                    type(command) is WindowGraphInput
                    or (
                        type(command) is WindowGraphWatermark
                        and command.timestamp != runtime.status.watermark
                    )
                    or (type(command) is WindowGraphFinish and not runtime.status.finished)
                    or (type(command) is WindowGraphDrain and bool(runtime.status.pending_windows))
                )
                if effective and (
                    before.generation >= _MAX_HISTORY or operation_sequence >= _MAX_HISTORY
                ):
                    raise ValidationError("window journal operation history capacity exceeded")
                if type(command) is WindowGraphInput:
                    batch = runtime.process(command)
                elif type(command) is WindowGraphWatermark:
                    batch = runtime.advance_watermark(
                        command.timestamp, next_position=command.next_position
                    )
                elif type(command) is WindowGraphFinish:
                    batch = runtime.finish(next_position=command.next_position)
                elif type(command) is WindowGraphDrain:
                    batch = runtime.drain(max_windows=command.max_windows)
                else:
                    raise ValidationError("invalid window graph command")
                if batch.operation_sequence == operation_sequence:
                    continue
                if (
                    batch.operation_sequence != operation_sequence + 1
                    or batch.operation_sequence > _MAX_HISTORY
                ):
                    raise ValidationError("window journal operation history capacity exceeded")
                start = before_progress[6] + len(output_rows)
                if (
                    len(output_rows) + len(batch.outputs) > _MAX_BATCH_OUTPUTS
                    or start + len(batch.outputs) > _MAX_HISTORY
                ):
                    raise ValidationError("window transaction output capacity exceeded")
                for output in batch.outputs:
                    item = WindowGraphJournalOutput(
                        before_progress[6] + len(output_rows),
                        batch.operation_sequence,
                        output.step_id,
                        output.record,
                    )
                    payload = _encoded(item.to_dict(), _OUTPUT_BYTES)
                    output_size += len(payload.encode("utf-8"))
                    if output_size > _BATCH_BYTES:
                        raise ValidationError("window transaction output bytes exceeded")
                    output_rows.append(
                        (item.sequence, item.operation_sequence, payload, _digest(payload))
                    )
                cause: Literal["process", "watermark", "finish", "drain"]
                position: int | None
                timestamp: int | None
                max_windows: int | None
                input_digest: str | None
                if type(command) is WindowGraphInput:
                    cause = "process"
                    position = command.position
                    timestamp = command.timestamp
                    max_windows = None
                    input_digest = _digest(_encoded(_command_document(command), _REQUEST_BYTES))
                elif type(command) is WindowGraphWatermark:
                    cause = "watermark"
                    position = command.next_position
                    timestamp = command.timestamp
                    max_windows = None
                    input_digest = None
                elif type(command) is WindowGraphFinish:
                    cause = "finish"
                    position = command.next_position
                    timestamp = None
                    max_windows = None
                    input_digest = None
                elif type(command) is WindowGraphDrain:
                    cause = "drain"
                    position = None
                    timestamp = None
                    max_windows = command.max_windows
                    input_digest = None
                else:
                    raise ValidationError("invalid window graph command")
                operation = WindowGraphOperation(
                    batch.operation_sequence,
                    before.generation + 1,
                    index,
                    cause,
                    position,
                    timestamp,
                    max_windows,
                    input_digest,
                    batch.drained_windows,
                    start,
                    before_progress[6] + len(output_rows),
                )
                payload = _encoded(operation.to_dict(), _OPERATION_BYTES)
                metadata_size += len(payload.encode("utf-8"))
                if metadata_size > _METADATA_BYTES:
                    raise ValidationError("window transaction operation metadata exceeded")
                operation_rows.append(
                    (operation.sequence, operation.generation, index, payload, _digest(payload))
                )
                operation_sequence = batch.operation_sequence
            changed = bool(operation_rows)
            after = WindowGraphRecoveryPoint(
                self.journal_id,
                self.source_id,
                self.source_commitment,
                before.generation + changed,
                runtime.checkpoint(),
            )
            after_progress = _progress(after.checkpoint)
            head_payload = _encoded(after.to_dict(), _HEAD_BYTES)
            head_digest = _digest(head_payload)
            receipt = WindowGraphReceipt(
                self.journal_id,
                request.request_id,
                request_digest,
                "committed" if changed else "no_op",
                len(request.commands),
                before.generation,
                after.generation,
                before_progress[0],
                after_progress[0],
                before_progress[1],
                after_progress[1],
                before_progress[2],
                after_progress[2],
                before_progress[3],
                after_progress[3],
                before_progress[4],
                after_progress[4],
                before_progress[5],
                after_progress[5],
                before_progress[6],
                after_progress[6],
                before_digest,
                head_digest,
            )
            receipt_payload = _encoded(receipt.to_dict(), _RECEIPT_BYTES)
            if metadata_size + len(receipt_payload.encode("utf-8")) > _METADATA_BYTES:
                raise ValidationError("window transaction receipt/operation metadata exceeded")
            if not changed:
                with self._transaction() as connection:
                    connection.execute("BEGIN")
                    current, digest = self._latest(connection)
                    if current != before or digest != before_digest:
                        raise RecoveryConflict("window journal changed during no-op observation")
                return receipt
            with self._transaction() as connection:
                connection.execute("BEGIN IMMEDIATE")
                current, digest = self._latest(connection)
                existing = self._request(connection, request.request_id, current, binding)
                if existing is not None:
                    return existing
                if current != before or digest != before_digest:
                    raise RecoveryConflict("window journal changed during processing")
                connection.execute(
                    "INSERT INTO window_commit VALUES (?, ?, ?, ?)",
                    (
                        after.generation,
                        request.request_id,
                        receipt_payload,
                        _digest(receipt_payload),
                    ),
                )
                connection.executemany(
                    "INSERT INTO window_operation VALUES (?, ?, ?, ?, ?)", operation_rows
                )
                connection.executemany("INSERT INTO window_output VALUES (?, ?, ?, ?)", output_rows)
                connection.execute(
                    "UPDATE window_head SET payload=?, digest=? WHERE slot=1",
                    (head_payload, head_digest),
                )
            return receipt
        except sqlite3.Error as error:
            raise OutputError(
                "window journal SQL operation failed; inspect request receipt before retry"
            ) from error

    def output_cursor(self, *, start: int = 0) -> WindowGraphOutputCursor:
        _count(start, "window output start", 0, _MAX_HISTORY)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                digest = (
                    self._receipt(connection, point.generation)[1]
                    if point.generation
                    else self._initial_digest(point.journal_id)
                )
                emitted = _progress(point.checkpoint)[6]
                if start > emitted:
                    raise ValidationError("window output start lies beyond the current prefix")
                return WindowGraphOutputCursor(
                    point.journal_id, point.generation, digest, emitted, start
                )
        except sqlite3.Error as error:
            raise OutputError("cannot create window output cursor") from error

    def _output_row(self, row: Any) -> tuple[WindowGraphJournalOutput, int]:
        if row is None:
            raise ValidationError("missing window journal output")
        sequence, operation_sequence, payload, digest, size = row
        output = WindowGraphJournalOutput.from_dict(
            _checked((payload, digest, size), _OUTPUT_BYTES)
        )
        if (output.sequence, output.operation_sequence) != (sequence, operation_sequence):
            raise ValidationError("window output contradicts its SQL key")
        if (
            output.step_id not in self._terminals
            or output.record.byte_size > self.flow.limits.graph.operator_limits.max_record_bytes
        ):
            raise ValidationError("window output violates its terminal or record contract")
        return output, size

    def read_outputs(
        self,
        cursor: WindowGraphOutputCursor,
        *,
        limit: int = 1_000,
        max_bytes: int = 16 * 1024 * 1024,
    ) -> WindowGraphOutputPage:
        """Detach one bounded fixed-prefix page; this does not acknowledge a sink."""
        if type(cursor) is not WindowGraphOutputCursor:
            raise ValidationError("read_outputs requires WindowGraphOutputCursor")
        cursor.__post_init__()
        _count(limit, "window output page limit", 1, 1_000)
        _count(max_bytes, "window output page bytes", 1, _BATCH_BYTES)
        try:
            with self._transaction() as connection:
                connection.execute("BEGIN")
                point, _ = self._latest(connection)
                if (
                    cursor.journal_id != point.journal_id
                    or cursor.anchor_generation > point.generation
                ):
                    raise ValidationError("window cursor does not belong to retained prefix")
                if cursor.anchor_generation:
                    anchor, digest, _ = self._receipt(connection, cursor.anchor_generation)
                    self._chain(connection, anchor, point)
                    stop = anchor.output_stop
                else:
                    digest, stop = self._initial_digest(point.journal_id), 0
                if cursor.anchor_receipt_digest != digest or cursor.stop_sequence != stop:
                    raise ValidationError("window cursor anchor or fixed stop changed")
                outputs: list[WindowGraphJournalOutput] = []
                metadata: dict[int, WindowGraphOperation] = {}
                checked_generations: set[int] = set()
                metadata_bytes = metadata_count = output_bytes = 0
                previous: WindowGraphJournalOutput | None = None
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
                        raise ValidationError("window output page is not contiguous")
                    if output_bytes + byte_size > max_bytes:
                        if not outputs:
                            raise ValidationError("next window output exceeds page byte budget")
                        budget_limited = True
                        break
                    operation, _ = self._operation_row(
                        connection.execute(
                            _SELECT_OPERATION, (_OPERATION_BYTES, item.operation_sequence)
                        ).fetchone()
                    )
                    if operation.generation > cursor.anchor_generation:
                        raise ValidationError("window output lies beyond cursor generation")
                    if operation.generation not in checked_generations:
                        receipt, _, size = self._receipt(connection, operation.generation)
                        count = receipt.after_operation - receipt.before_operation
                        if (
                            metadata_count + count > _PAGE_METADATA_ROWS
                            or metadata_bytes + _METADATA_BYTES + _RECEIPT_BYTES
                            > _PAGE_METADATA_BYTES
                        ):
                            if outputs:
                                budget_limited = True
                                break
                            raise ValidationError("window commit metadata exceeds page admission")
                        operations, size = self._verify_commit(connection, receipt, point, size)
                        metadata.update((value.sequence, value) for value in operations)
                        checked_generations.add(operation.generation)
                        metadata_count += count
                        metadata_bytes += size
                    if (
                        operation != metadata.get(operation.sequence)
                        or not operation.output_start <= item.sequence < operation.output_stop
                        or operation.cause != "drain"
                    ):
                        raise ValidationError("window output is outside its drain operation")
                    if (
                        previous is not None
                        and item.operation_sequence < previous.operation_sequence
                    ):
                        raise ValidationError("window output operation order regressed")
                    outputs.append(item)
                    output_bytes += byte_size
                    previous = item
                if not budget_limited and len(outputs) != min(
                    limit, cursor.stop_sequence - cursor.next_sequence
                ):
                    raise ValidationError("window output query returned an incomplete prefix")
                return WindowGraphOutputPage(
                    tuple(outputs),
                    replace(cursor, next_sequence=cursor.next_sequence + len(outputs)),
                )
        except sqlite3.Error as error:
            raise OutputError("cannot read window graph output page") from error


__all__ = ["WindowGraphJournal"]
