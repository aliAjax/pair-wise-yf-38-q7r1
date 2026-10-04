from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, InvalidTransition, NotFoundError, utcnow
from .rules import RuleEngine, check_export_release


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        payload = self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        # Check the version before validating the transition so a stale write
        # always observes the latest state, regardless of the action's validity.
        if expected_version is not None and int(expected_version) != entity["version"]:
            raise ConflictError(
                "version conflict: expected %s, found %s"
                % (expected_version, entity["version"]),
                details={"current": entity},
            )
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(
            entity_id, expected, next_status if next_status else entity["status"], merged
        )
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        self._after_transition(actor, entity, action, updated)
        return updated

    def dispatch(self, actor, entity_id, action, data=None, expected_version=None):
        """Route an action to the dedicated release workflow when needed."""
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        if entity["kind"] == "export_task" and action in ("release", "resume"):
            return self.release_export(actor, entity_id, expected_version)
        return self.transition(actor, entity_id, action, data, expected_version)

    def _after_transition(self, actor, entity, action, updated):
        kind = updated["kind"]
        if kind == "dataset" and action == "restrict":
            self._block_tasks(dataset_id=updated["id"], reason="dataset restricted")
        elif kind == "dataset" and action == "publish":
            self._reconfirm_tasks(actor, updated)
        elif kind == "grant" and action in ("revoke", "expire"):
            self._block_tasks(
                grant_id=updated["id"],
                reason="grant %s" % ("revoked" if action == "revoke" else "expired"),
            )

    def _block_tasks(self, dataset_id=None, grant_id=None, reason=None):
        if dataset_id is None and grant_id is None:
            return 0
        if dataset_id is not None:
            tasks = self.repository.find_entities("export_task", "dataset_id", dataset_id)
        else:
            tasks = self.repository.find_entities("export_task", "grant_id", grant_id)
        count = 0
        for task in tasks:
            if task["status"] != "queued":
                continue
            updated = self.repository.conditional_update(
                task["id"],
                "queued",
                None,
                "blocked",
                data_patch={"blocked_reason": reason},
            )
            if updated:
                count += 1
        return count

    def _reconfirm_tasks(self, actor, dataset):
        for task in self.repository.list_blocked_tasks(dataset["id"]):
            application = self.repository.get_entity(task["data"].get("application_id"))
            grant = self.repository.get_entity(task["data"].get("grant_id"))
            ok, reason = check_export_release(task, dataset, application, grant)
            if ok:
                updated = self.repository.conditional_update(
                    task["id"],
                    "blocked",
                    None,
                    "queued",
                    data_patch={"reconfirmed_at": utcnow(), "blocked_reason": None},
                )
                if updated:
                    self.audit.record(
                        task["id"], actor, "reconfirm", "blocked", "queued",
                        {"reason": "reconfirmed against remaining grant validity"},
                    )
            else:
                updated = self.repository.conditional_update(
                    task["id"],
                    "blocked",
                    None,
                    "rejected",
                    data_patch={"reject_reason": reason},
                )
                if updated:
                    self.audit.record(
                        task["id"], actor, "reconfirm", "blocked", "rejected",
                        {"reason": reason},
                    )

    def release_export(self, actor, task_id, expected_version=None):
        """Release an export task against the full consistency ledger.

        The release is a sequence of durable steps (chain check, quota
        deduction, receipt issuance). Each step is recorded before the next
        starts, so a crash resumes at the first unconfirmed step. Confirmed
        steps are skipped, so a retry never deducts quota twice or leaves a
        second receipt.
        """
        task = self.repository.get_entity(task_id)
        if not task:
            raise NotFoundError("export task not found: " + task_id)
        if task["kind"] != "export_task":
            raise ValidationError("not an export task")

        dataset = self.repository.get_entity(task["data"].get("dataset_id"))
        application = self.repository.get_entity(task["data"].get("application_id"))
        grant = self.repository.get_entity(task["data"].get("grant_id"))
        version = int(expected_version) if expected_version is not None else task["version"]

        if task["status"] == "released":
            return task  # idempotent: receipt already issued
        if task["status"] in ("rejected", "cancelled"):
            raise ConflictError(
                "export task is %s" % task["status"],
                details={"current": task},
            )
        if task["status"] == "blocked":
            ok, reason = check_export_release(task, dataset, application, grant)
            return self._reject_task(
                task["id"], version, reason, actor, from_status="blocked"
            )
        if task["status"] != "queued":
            raise InvalidTransition("cannot release export task in status %s" % task["status"])

        steps = dict(task["data"].get("steps") or {})
        quota = int(task["data"].get("quota", 0))

        # Step 1: consistency chain check (durable, resumable)
        if not steps.get("chain_checked"):
            ok, reason = check_export_release(task, dataset, application, grant)
            if not ok:
                return self._reject_task(task["id"], version, reason, actor)
            task = self.repository.record_step(task["id"], "chain_checked", version)
            if task is None:
                raise self._conflict(task["id"])
            version = task["version"]
            steps["chain_checked"] = True

        # Step 2: quota deduction (durable, idempotent)
        if not steps.get("quota_deducted"):
            ok, reason = check_export_release(task, dataset, application, grant)
            if not ok:
                return self._reject_task(task["id"], version, reason, actor)
            if not self.repository.deduct_quota(grant["id"], quota):
                return self._reject_task(task["id"], version, "grant quota exhausted", actor)
            task = self.repository.record_step(task["id"], "quota_deducted", version)
            if task is None:
                raise self._conflict(task["id"])
            version = task["version"]
            steps["quota_deducted"] = True

        # Step 3: receipt issuance (durable, idempotent)
        if not steps.get("receipt_issued"):
            ok, reason = check_export_release(task, dataset, application, grant)
            if not ok:
                if steps.get("quota_deducted"):
                    self.repository.refund_quota(grant["id"], quota)
                return self._reject_task(task["id"], version, reason, actor)
            receipt_id = str(uuid4())
            receipt_data = {
                "task_id": task["id"],
                "dataset_id": task["data"].get("dataset_id"),
                "application_id": task["data"].get("application_id"),
                "grant_id": task["data"].get("grant_id"),
                "recipient": grant["data"].get("recipient"),
                "quota": quota,
                "purpose": task["data"].get("purpose"),
            }
            task = self.repository.issue_receipt(task["id"], version, receipt_id, receipt_data)
            if task is None:
                raise self._conflict(task["id"])
            self.audit.record(
                task["id"], actor, "release", "queued", "released",
                {"receipt_id": receipt_id},
            )
        return task

    def resume_export(self, actor, task_id, expected_version=None):
        """Resume a crashed release at the first unconfirmed step."""
        return self.release_export(actor, task_id, expected_version)

    def _reject_task(self, task_id, expected_version, reason, actor, from_status="queued"):
        updated = self.repository.conditional_update(
            task_id,
            from_status,
            expected_version,
            "rejected",
            data_patch={"reject_reason": reason},
        )
        if updated is None:
            raise self._conflict(task_id)
        self.audit.record(
            task_id, actor, "release", from_status, "rejected", {"reason": reason}
        )
        return updated

    def _conflict(self, entity_id):
        current = self.repository.get_entity(entity_id)
        return ConflictError(
            "version conflict: the export task was modified concurrently",
            details={"current": current},
        )

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
