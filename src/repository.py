import json
import sqlite3
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
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                current = self._entity_from_row(row)
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version),
                    details={"current": current},
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def conditional_update(self, entity_id, from_status, expected_version, to_status, data_patch=None):
        """Conditionally update status, merged data and version.

        The write only lands when the entity is in ``from_status`` (when given)
        and at ``expected_version`` (when given). Returns the updated entity, or
        ``None`` when the condition failed so the caller can surface a conflict.
        """
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, version, data FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_status = row["status"]
            current_version = int(row["version"])
            if from_status is not None and current_status != from_status:
                return None
            if expected_version is not None and current_version != int(expected_version):
                return None
            data = json.loads(row["data"])
            if data_patch:
                data.update(data_patch)
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ?",
                (to_status if to_status else current_status, payload, now, entity_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def record_step(self, entity_id, step_name, expected_version):
        """Record a durable workflow step on a queued export task.

        Returns the updated entity, or ``None`` when the task is no longer
        queued or the version moved. Already-confirmed steps are skipped by the
        caller, so retries never re-run a confirmed step.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, version, data FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if row["status"] != "queued":
                return None
            if expected_version is not None and current_version != int(expected_version):
                return None
            data = json.loads(row["data"])
            steps = data.setdefault("steps", {})
            steps[step_name] = True
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "UPDATE entities SET version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                (payload, utcnow(), entity_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def deduct_quota(self, grant_id, amount):
        """Conditionally deduct quota from a grant.

        Returns ``True`` when the deduction landed, ``False`` when the grant is
        missing or has insufficient remaining quota.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT data FROM entities WHERE id = ? AND kind = 'grant'", (grant_id,)
            ).fetchone()
            if not row:
                return False
            data = json.loads(row["data"])
            total = int(data.get("quota_total", 0))
            used = int(data.get("quota_used", 0))
            if used + int(amount) > total:
                return False
            data["quota_used"] = used + int(amount)
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "UPDATE entities SET data = ?, updated_at = ? WHERE id = ?",
                (payload, utcnow(), grant_id),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def refund_quota(self, grant_id, amount):
        """Return quota to a grant (used when a later release step fails)."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT data FROM entities WHERE id = ? AND kind = 'grant'", (grant_id,)
            ).fetchone()
            if not row:
                return False
            data = json.loads(row["data"])
            used = int(data.get("quota_used", 0))
            data["quota_used"] = max(0, used - int(amount))
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "UPDATE entities SET data = ?, updated_at = ? WHERE id = ?",
                (payload, utcnow(), grant_id),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def issue_receipt(self, task_id, expected_version, receipt_id, receipt_data):
        """Atomically issue a receipt and mark the export task released.

        The receipt and the task status move in one transaction, so a retry can
        never leave a second receipt or a half-released task. Returns the
        updated task, or ``None`` when the task is no longer queued or the
        version moved.
        """
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, version, data FROM entities WHERE id = ?", (task_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + task_id)
            current_version = int(row["version"])
            if row["status"] != "queued":
                return None
            if expected_version is not None and current_version != int(expected_version):
                return None
            data = json.loads(row["data"])
            steps = data.setdefault("steps", {})
            steps["receipt_issued"] = True
            data["receipt_id"] = receipt_id
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            now = utcnow()
            connection.execute(
                "UPDATE entities SET status = 'released', version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ?",
                (payload, now, task_id),
            )
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, 'receipt', 'issued', 1, ?, 'system', ?, ?)",
                (receipt_id, json.dumps(receipt_data, ensure_ascii=False, sort_keys=True), now, now),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(task_id)

    def list_blocked_tasks(self, dataset_id):
        return [
            entity
            for entity in self.list_entities(kind="export_task")
            if entity["status"] == "blocked" and entity["data"].get("dataset_id") == dataset_id
        ]

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

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
