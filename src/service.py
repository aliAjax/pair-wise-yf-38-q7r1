from datetime import date
from uuid import uuid4

from .audit import AuditTrail
from .domain import (
    ConflictError,
    NotFoundError,
    ReleaseDenied,
    TemporarilyBlocked,
)
from .rules import RuleEngine

# Saga steps for an export. Each step is confirmed by its own durable record
# (quota_ledger row / receipts row) so a crashed worker resumes by looking at
# what is already on disk instead of redoing work.
PULL_STEP = "pull"
RECEIPT_STEP = "receipt"


class DomainService:
    def __init__(self, repository, rules=None, clock=None, crash_hook=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        # Injectable clock ("YYYY-MM-DD") keeps expiry decisions testable.
        self._clock = clock
        # crash_hook(export_id, step) may raise to simulate a worker dying
        # immediately after a step committed. Tests only.
        self.crash_hook = crash_hook

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def today(self):
        return self._clock() if self._clock else date.today().isoformat()

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
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None,
                   idempotency_key=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)

        if idempotency_key:
            replayed = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if replayed:
                replayed_entity = self.repository.get_entity(replayed)
                if replayed_entity:
                    return replayed_entity

        kind = self.rules.normalize_kind(entity["kind"])
        payload = dict(data or {})

        if kind == "export" and action == "continue":
            self._guard_export_action(entity, "continue")
            if entity["status"] == "receipted":
                # Idempotent replay: steps and receipt are already confirmed.
                result = entity
            else:
                # Validates role/ownership and the starting status here; the
                # saga itself performs the per-step state moves.
                self.rules.validate_transition(
                    actor, entity, action, payload, self._lookup
                )
                result = self._continue_export(actor, entity, expected_version)
        elif kind == "export" and action == "cancel":
            self._guard_export_action(entity, "cancel")
            result = self._cancel_export(actor, entity, payload, expected_version)
        else:
            next_status, patch = self.rules.validate_transition(
                actor, entity, action, payload, self._lookup
            )
            merged = dict(entity["data"])
            merged.update(patch)
            expected = (
                int(expected_version)
                if expected_version is not None
                else entity["version"]
            )
            result = self.repository.update_entity(entity_id, expected, next_status, merged)
            self.audit.record(
                entity_id,
                actor,
                action,
                entity["status"],
                result["status"],
                {"patch": patch},
            )
            self._cascade(actor, entity, result, action)

        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, result["id"])
        return result

    # ------------------------------------------------------------------
    # Export saga
    # ------------------------------------------------------------------
    @staticmethod
    def _guard_export_action(export, action):
        """Turn a request against a settled export into its recorded outcome.

        A rejected export answers a retried continue with the persisted
        denial (not a bare state-machine error); receipted/cancelled exports
        report a conflict carrying the latest version so the other side of a
        race can re-read the truth.
        """
        status = export["status"]
        if status == "rejected":
            reason = export["data"].get("rejection", {}).get(
                "reason", "export was rejected"
            )
            raise ReleaseDenied(reason)
        if status == "receipted":
            if action == "continue":
                # The work is already confirmed; this is an idempotent replay.
                return
            raise ConflictError(
                "export %s is already receipted at version %s"
                % (export["id"], export["version"])
            )
        if status == "cancelled":
            raise ConflictError(
                "export %s was cancelled at version %s"
                % (export["id"], export["version"])
            )

    def _export_chain(self, connection, export):
        dataset = self.repository.tx_get(connection, export["data"]["dataset_id"])
        grant = self.repository.tx_get(connection, export["data"]["grant_id"])
        application = (
            self.repository.tx_get(connection, grant["data"]["application_id"])
            if grant
            else None
        )
        return dataset, application, grant

    @staticmethod
    def _quota_remaining(grant, consumed):
        limit = grant["data"].get("quota_limit")
        if limit is None:
            return None
        return int(limit) - int(consumed)

    def _continue_export(self, actor, export, expected_version):
        """Drive the export saga forward, confirming each step at most once.

        Steps are durable: a crashed worker leaves the quota debit (pull) and
        the receipt row on disk, so a retry picks up at the first unconfirmed
        step instead of redoing anything. A version clash with a competing
        worker means the other worker already confirmed some step; re-read and
        resume from there rather than failing the request.
        """
        for _attempt in range(2):
            try:
                if export["status"] == "queued":
                    export = self._step_pull(actor, export, expected_version)
                elif export["status"] == "pulling":
                    export = self._resume_receipt(actor, export)
                break
            except ConflictError:
                latest = self.repository.get_entity(export["id"])
                progressed = latest["version"] > export["version"]
                if not progressed or latest["status"] not in ("pulling", "receipted"):
                    # A cancel/reject won the race, or nothing actually moved;
                    # the caller must re-read the latest version instead.
                    raise
                export = latest
                # Loop: re-enter at the step the winning worker reached.

        if export["status"] != "receipted":
            # A racing cancel/reject moved the export out from under us; the
            # caller must re-read the latest version instead of retrying blind.
            latest = self.repository.get_entity(export["id"])
            raise ConflictError(
                "export %s is now %s at version %s"
                % (export["id"], latest["status"], latest["version"])
            )
        return export

    def _step_pull(self, actor, export, expected_version):
        pulled = self._commit_pull(actor, export)
        if self.crash_hook:
            # Simulate the worker dying after the pull committed (quota
            # debited, export in 'pulling') but before the receipt step.
            self.crash_hook(pulled["id"], PULL_STEP)
        return self._resume_receipt(actor, pulled)

    def _commit_pull(self, actor, export):
        result = []
        deferred = []
        with self.repository.transaction() as connection:
            current = self.repository.tx_get(connection, export["id"])
            if current["status"] not in ("queued", "pulling"):
                raise ConflictError(
                    "export %s is now %s at version %s"
                    % (current["id"], current["status"], current["version"])
                )
            if current["status"] == "pulling":
                # Another worker already confirmed the pull; there is nothing
                # new to charge in this transaction.
                return current

            dataset, application, grant = self._export_chain(connection, current)
            consumed = self.repository.quota_consumed(current["data"]["grant_id"])
            remaining = self._quota_remaining(grant, consumed) if grant else None
            try:
                confirmation = self.rules.release_gate(
                    current, dataset, application, grant, self.today(), remaining
                )
            except TemporarilyBlocked:
                # Nothing confirmed: re-queued exactly as it was.
                raise
            except ReleaseDenied as denied:
                # Persist the terminal rejection in this same transaction,
                # then raise once it has committed.
                deferred.append(
                    self._tx_reject(connection, actor, current, str(denied), "gate")
                )
            else:
                amount = int(current["data"].get("amount", 1))
                # The ledger primary key makes this debit exactly-once on retry.
                charged, charged_amount = self.repository.tx_quota_consume(
                    connection, grant["id"], current["id"], PULL_STEP, amount, actor.user_id
                )
                next_status = self.rules.validate_internal_transition(current, "pull")
                data = dict(current["data"])
                data["steps"] = sorted(set(data.get("steps", [])) | {PULL_STEP})
                data["pull"] = confirmation
                # Optimistic lock against the version observed INSIDE this
                # transaction: two racing continues cannot both pass, since
                # BEGIN IMMEDIATE serializes them and the loser sees a bumped
                # version.
                updated = self.repository.tx_update(
                    connection, current["id"], current["version"], next_status, data
                )
                self.repository.tx_append_audit(
                    connection,
                    current["id"],
                    actor.user_id,
                    actor.role,
                    "pull",
                    current["status"],
                    updated["status"],
                    {"charged": charged, "amount": charged_amount, "gate": confirmation},
                )
                result.append(updated)
        if deferred:
            raise deferred[0]
        return result[0]

    def _resume_receipt(self, actor, export):
        done = self._step_receipt(actor, export, export["version"])
        if self.crash_hook:
            self.crash_hook(done["id"], RECEIPT_STEP)
        return done

    def _step_receipt(self, actor, export, expected_version):
        result = []
        deferred = []
        with self.repository.transaction() as connection:
            current = self.repository.tx_get(connection, export["id"])
            if current["status"] == "receipted":
                return current
            if current["status"] != "pulling":
                raise ConflictError(
                    "export %s is now %s at version %s"
                    % (current["id"], current["status"], current["version"])
                )
            dataset, application, grant = self._export_chain(connection, current)
            # Re-check at the moment of issuing the receipt: a revoke landing
            # between pull and receipt must still be refused.
            try:
                confirmation = self.rules.release_gate(
                    current, dataset, application, grant, self.today()
                )
            except TemporarilyBlocked:
                raise
            except ReleaseDenied as denied:
                deferred.append(
                    self._tx_reject(connection, actor, current, str(denied), "gate")
                )
            else:
                payload = {
                    "dataset_id": dataset["id"],
                    "grant_id": grant["id"],
                    "recipient": grant["data"].get("recipient"),
                    "amount": int(current["data"].get("amount", 1)),
                    "confirmed": confirmation,
                }
                issued = self.repository.tx_insert_receipt(
                    connection,
                    current["id"],
                    grant["id"],
                    dataset["id"],
                    grant["data"].get("recipient", current["data"].get("requester_id")),
                    payload["amount"],
                    payload,
                )
                data = dict(current["data"])
                data["steps"] = sorted(set(data.get("steps", [])) | {RECEIPT_STEP})
                data["receipt_confirmed"] = confirmation
                updated = self.repository.tx_update(
                    connection, current["id"], current["version"], "receipted", data
                )
                self.repository.tx_append_audit(
                    connection,
                    current["id"],
                    actor.user_id,
                    actor.role,
                    "continue",
                    current["status"],
                    updated["status"],
                    {"receipt_issued": issued, "gate": confirmation},
                )
                result.append(updated)
        if deferred:
            raise deferred[0]
        return result[0]

    def _cancel_export(self, actor, export, payload, expected_version):
        """Cancel while the optimistic version lock is held.

        A continue and a cancel submitted concurrently serialize on the same
        row: exactly one commits; the loser gets a conflict and the latest
        version to re-read.
        """
        next_status, patch = self.rules.validate_transition(
            actor, export, "cancel", payload, self._lookup
        )
        expected = (
            int(expected_version) if expected_version is not None else export["version"]
        )
        with self.repository.transaction() as connection:
            current = self.repository.tx_get(connection, export["id"])
            if current["status"] not in ("queued", "pulling"):
                raise ConflictError(
                    "export %s is now %s at version %s"
                    % (current["id"], current["status"], current["version"])
                )
            merged = dict(current["data"])
            merged.update(patch)
            updated = self.repository.tx_update(
                connection, current["id"], expected, next_status, merged
            )
            self.repository.tx_append_audit(
                connection,
                current["id"],
                actor.user_id,
                actor.role,
                "cancel",
                current["status"],
                updated["status"],
                {"patch": patch},
            )
            return updated

    def _tx_reject(self, connection, actor, export, reason, source):
        """Record a terminal rejection inside the open saga transaction.

        Does not raise: callers commit first, then raise the returned
        ReleaseDenied, so the rejection is durable when the error surfaces.
        """
        next_status = self.rules.validate_internal_transition(export, "reject")
        data = dict(export["data"])
        data["rejection"] = {"reason": reason, "source": source, "at": self.today()}
        updated = self.repository.tx_update(
            connection, export["id"], export["version"], next_status, data
        )
        self.repository.tx_append_audit(
            connection,
            export["id"],
            actor.user_id,
            actor.role,
            "reject",
            export["status"],
            updated["status"],
            {"reason": reason, "source": source},
        )
        return ReleaseDenied(reason)

    # ------------------------------------------------------------------
    # Cascades: keep the four ledgers consistent after a governing action.
    # ------------------------------------------------------------------
    def _cascade(self, actor, before, after, action):
        if before["kind"] == "dataset" and action == "publish":
            self._cascade_dataset_publish(actor, after)
        elif before["kind"] == "grant" and action in ("revoke", "expire"):
            self._cascade_grant_invalidated(actor, after, action)
        elif before["kind"] == "application" and action == "withdraw":
            self._cascade_application_withdrawn(actor, after)
        elif before["kind"] == "application" and action == "amend":
            self._cascade_application_amended(actor, before, after)

    def _open_exports_for(self, dataset_id=None, grant_id=None):
        exports = self.repository.list_entities(kind="export")
        result = []
        for export in exports:
            if export["status"] not in ("queued", "pulling"):
                continue
            if dataset_id is not None and export["data"].get("dataset_id") != dataset_id:
                continue
            if grant_id is not None and export["data"].get("grant_id") != grant_id:
                continue
            result.append(export)
        return result

    def _cascade_dataset_publish(self, actor, dataset):
        """Re-confirm every queued export against the original credential.

        Recovery must ride on the grant's remaining window; expired grants or
        drifted purposes reject the export instead of re-releasing it.
        """
        for export in self._open_exports_for(dataset_id=dataset["id"]):
            with self.repository.transaction() as connection:
                current = self.repository.tx_get(connection, export["id"])
                if current["status"] != "queued":
                    continue
                _dataset, application, grant = self._export_chain(connection, current)
                decision = self.rules.reconfirm_for_publish(
                    current, dataset, application, grant, self.today()
                )
                if decision.get("allowed"):
                    data = dict(current["data"])
                    data["reconfirmed"] = decision
                    updated = self.repository.tx_update(
                        connection, current["id"], current["version"], None, data
                    )
                    self.repository.tx_append_audit(
                        connection,
                        current["id"],
                        actor.user_id,
                        actor.role,
                        "reconfirm",
                        current["status"],
                        updated["status"],
                        decision,
                    )
                else:
                    # Persist the rejection and swallow its deny signal: the
                    # publish action itself still succeeds for other exports.
                    self._tx_reject(
                        connection,
                        actor,
                        current,
                        decision.get("reason", "failed reconfirmation on publish"),
                        "publish",
                    )

    def _cascade_grant_invalidated(self, actor, grant, action):
        reason = "grant %s" % ("revoked" if action == "revoke" else "expired")
        for export in self._open_exports_for(grant_id=grant["id"]):
            self._reject_out_of_band(actor, export, reason, action)

    def _cascade_application_withdrawn(self, actor, application):
        grants = self._lookup("grant", "application_id", application["id"]) or []
        for grant in grants:
            for export in self._open_exports_for(grant_id=grant["id"]):
                self._reject_out_of_band(
                    actor, export, "application %s withdrawn" % application["id"], "withdraw"
                )

    def _cascade_application_amended(self, actor, before, application):
        new_purpose = application["data"].get("purpose")
        grants = self._lookup("grant", "application_id", application["id"]) or []
        for grant in grants:
            for export in self._open_exports_for(grant_id=grant["id"]):
                recorded = export["data"].get("purpose_at_request")
                if recorded is not None and recorded != new_purpose:
                    self._reject_out_of_band(
                        actor,
                        export,
                        "application purpose changed since export was queued",
                        "amend",
                    )

    def _reject_out_of_band(self, actor, export, reason, source):
        with self.repository.transaction() as connection:
            current = self.repository.tx_get(connection, export["id"])
            if current["status"] not in ("queued", "pulling"):
                return
            # The governing action already committed; the denial is recorded
            # here but not propagated to the admin's transition response.
            self._tx_reject(connection, actor, current, reason, source)

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def receipt(self, export_id):
        receipt = self.repository.get_receipt(export_id)
        if not receipt:
            raise NotFoundError("no receipt for export: " + export_id)
        return receipt

    def receipts(self, dataset_id=None):
        return self.repository.list_receipts(dataset_id=dataset_id)

    def ledger(self):
        return self.repository.ledger()

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
