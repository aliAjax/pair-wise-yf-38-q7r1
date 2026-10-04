import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def transaction(self):
        """One SERIALIZABLE-ish unit of work over a single connection.

        Saga steps run their read-check-write inside a single BEGIN
        IMMEDIATE transaction so two workers can never both observe the
        pre-step state; the loser sees the bumped version and retries.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS quota_ledger (
                    grant_id TEXT NOT NULL,
                    export_id TEXT NOT NULL,
                    step TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    actor_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(export_id, step)
                );
                CREATE INDEX IF NOT EXISTS idx_quota_grant
                    ON quota_ledger(grant_id);
                CREATE TABLE IF NOT EXISTS receipts (
                    export_id TEXT PRIMARY KEY,
                    grant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version, status FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            next_status = status if status is not None else row["status"]
            cursor = connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (next_status, payload, now, entity_id, current_version),
            )
            if cursor.rowcount != 1:
                raise ConflictError(
                    "version conflict: expected %s for %s"
                    % (current_version, entity_id)
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # ------------------------------------------------------------------
    # Transaction-scoped primitives used by the export saga.
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_entity(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def tx_get(self, connection, entity_id):
        row = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._row_to_entity(row) if row else None

    def tx_update(self, connection, entity_id, expected_version, status, data):
        now = utcnow()
        row = connection.execute(
            "SELECT version, status FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if not row:
            raise NotFoundError("entity not found: " + entity_id)
        current_version = int(row["version"])
        if expected_version is not None and current_version != int(expected_version):
            raise ConflictError(
                "version conflict: expected %s, found %s"
                % (expected_version, current_version)
            )
        next_status = status if status is not None else row["status"]
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        cursor = connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (next_status, payload, now, entity_id, current_version),
        )
        if cursor.rowcount != 1:
            # Lost the optimistic-lock race inside the write transaction.
            raise ConflictError(
                "version conflict: expected %s for %s"
                % (current_version, entity_id)
            )
        return self.tx_get(connection, entity_id)

    def tx_append_audit(self, connection, entity_id, actor_id, actor_role, action,
                        from_status, to_status, detail):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, "
            "from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor_id,
                actor_role,
                action,
                from_status,
                to_status,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def tx_quota_consume(self, connection, grant_id, export_id, step, amount, actor_id):
        """Idempotent quota debit.

        The (export_id, step) primary key makes a retried step a no-op:
        after a crash the second attempt cannot debit quota twice.
        Returns (charged_now, amount_for_this_call).
        """
        existing = connection.execute(
            "SELECT amount FROM quota_ledger WHERE export_id = ? AND step = ?",
            (export_id, step),
        ).fetchone()
        if existing:
            return False, int(existing["amount"])
        connection.execute(
            "INSERT INTO quota_ledger(grant_id, export_id, step, amount, actor_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (grant_id, export_id, step, int(amount), actor_id, utcnow()),
        )
        return True, int(amount)

    def quota_consumed(self, grant_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(SUM(amount), 0) AS total FROM quota_ledger WHERE grant_id = ?",
                (grant_id,),
            ).fetchone()
        return int(row["total"])

    def quota_entries(self, grant_id=None):
        sql = (
            "SELECT grant_id, export_id, step, amount, actor_id, created_at "
            "FROM quota_ledger"
        )
        params = ()
        if grant_id:
            sql += " WHERE grant_id = ?"
            params = (grant_id,)
        sql += " ORDER BY created_at, export_id, step"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [
            {
                "grant_id": row["grant_id"],
                "export_id": row["export_id"],
                "step": row["step"],
                "amount": int(row["amount"]),
                "actor_id": row["actor_id"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def tx_insert_receipt(self, connection, export_id, grant_id, dataset_id,
                          recipient, amount, payload):
        """Insert the one-and-only receipt for an export; no-op if it exists.

        Returns True when a new receipt was written, False on replay so a
        retried final step can never leave a second receipt behind.
        """
        existing = connection.execute(
            "SELECT 1 FROM receipts WHERE export_id = ?", (export_id,)
        ).fetchone()
        if existing:
            return False
        connection.execute(
            "INSERT INTO receipts(export_id, grant_id, dataset_id, recipient, "
            "amount, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                export_id,
                grant_id,
                dataset_id,
                recipient,
                int(amount),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )
        return True

    def get_receipt(self, export_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM receipts WHERE export_id = ?", (export_id,)
            ).fetchone()
        return self._receipt_from_row(row) if row else None

    def list_receipts(self, dataset_id=None):
        sql = "SELECT * FROM receipts"
        params = ()
        if dataset_id:
            sql += " WHERE dataset_id = ?"
            params = (dataset_id,)
        sql += " ORDER BY created_at, export_id"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._receipt_from_row(row) for row in rows]

    @staticmethod
    def _receipt_from_row(row):
        return {
            "export_id": row["export_id"],
            "grant_id": row["grant_id"],
            "dataset_id": row["dataset_id"],
            "recipient": row["recipient"],
            "amount": int(row["amount"]),
            "payload": json.loads(row["payload"]),
            "created_at": row["created_at"],
        }

    def ledger(self):
        """Reconciliation view: quota debits next to their receipts.

        Issued receipts survive restriction/revocation and appear here even
        though the related entities may since have been invalidated.
        """
        return {"quota": self.quota_entries(), "receipts": self.list_receipts()}

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
