import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path

from src.domain import (
    Actor,
    ConflictError,
    InvalidTransition,
    ReleaseDenied,
    TemporarilyBlocked,
)
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class SimulatedCrash(Exception):
    """Stand-in for an export worker dying right after a step committed."""


class ExportConsistencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.today = date.today().isoformat()
        self.future = (date.today() + timedelta(days=365)).isoformat()
        self.past = (date.today() - timedelta(days=2)).isoformat()
        self.service = DomainService(self.repo, RuleEngine(), clock=lambda: self.today)
        self.admin = Actor("admin", "admin")
        self.user = Actor("researcher-1", "applicant")
        self.other = Actor("researcher-2", "applicant")

    def tearDown(self):
        self.tmp.cleanup()

    def _provision(self, quota_limit=None, grant_expiry=None, purpose="variant analysis",
                   publish=True):
        dataset = self.service.create(
            self.admin, "dataset", {"name": "D", "access_policy": "controlled"}
        )
        if publish:
            self.service.transition(
                self.admin, dataset["id"], "restrict", {"reason": "baseline review"}
            )
            self.service.transition(self.admin, dataset["id"], "publish", {})
        application = self.service.create(
            self.user,
            "application",
            {
                "dataset_id": dataset["id"],
                "applicant_id": "researcher-1",
                "purpose": purpose,
            },
        )
        self.service.transition(self.user, application["id"], "submit", {})
        self.service.transition(
            self.admin, application["id"], "review", {"committee_id": "c1"}
        )
        self.service.transition(
            self.admin,
            application["id"],
            "approve",
            {"approvals": ["r1", "r2", "r3"], "terms": "nc", "expires_at": self.future},
        )
        grant_data = {
            "application_id": application["id"],
            "dataset_id": dataset["id"],
            "recipient": "researcher-1",
        }
        if quota_limit is not None:
            grant_data["quota_limit"] = quota_limit
        grant = self.service.create(self.admin, "grant", grant_data)
        self.service.transition(
            self.admin,
            grant["id"],
            "activate",
            {"starts_at": self.past, "expires_at": grant_expiry or self.future},
        )
        return dataset, application, grant

    def _export(self, dataset, grant, amount=1):
        return self.service.create(
            self.user,
            "export",
            {"dataset_id": dataset["id"], "grant_id": grant["id"], "amount": amount},
        )

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------
    def test_queued_export_runs_pull_then_receipt(self):
        dataset, application, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)

        done = self.service.transition(self.user, export["id"], "continue", {})

        self.assertEqual(done["status"], "receipted")
        self.assertEqual(done["data"]["steps"], ["pull", "receipt"])
        receipt = self.service.receipt(export["id"])
        self.assertEqual(receipt["grant_id"], grant["id"])
        self.assertEqual(receipt["amount"], 1)
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)

    def test_other_applicant_cannot_operate_export(self):
        from src.domain import PermissionDenied

        dataset, _app, grant = self._provision()
        export = self._export(dataset, grant)
        with self.assertRaises(PermissionDenied):
            self.service.transition(self.other, export["id"], "continue", {})
        # Positive control: the real requester still proceeds.
        done = self.service.transition(self.user, export["id"], "continue", {})
        self.assertEqual(done["status"], "receipted")

    # ------------------------------------------------------------------
    # Restriction / revocation / withdrawal must block the data pull
    # ------------------------------------------------------------------
    def test_restricted_dataset_blocks_pull_without_side_effects(self):
        dataset, _app, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)

        self.service.transition(
            self.admin, dataset["id"], "restrict", {"reason": "incident"}
        )
        with self.assertRaises(TemporarilyBlocked):
            self.service.transition(self.user, export["id"], "continue", {})

        # The queued task stays queued, no quota was debited, no receipt.
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "queued")
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 0)
        self.assertIsNone(self.repo.get_receipt(export["id"]))

    def test_publish_reconfirms_and_export_can_finish(self):
        dataset, _app, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)
        self.service.transition(
            self.admin, dataset["id"], "restrict", {"reason": "incident"}
        )
        with self.assertRaises(TemporarilyBlocked):
            self.service.transition(self.user, export["id"], "continue", {})

        self.service.transition(self.admin, dataset["id"], "publish", {})
        done = self.service.transition(self.user, export["id"], "continue", {})

        self.assertEqual(done["status"], "receipted")
        self.assertIn("reconfirmed", done["data"])
        self.assertGreater(done["data"]["reconfirmed"]["grant_remaining"], 0)

    def test_revoked_grant_rejects_queued_export(self):
        dataset, _app, grant = self._provision()
        export = self._export(dataset, grant)

        self.service.transition(
            self.admin, grant["id"], "revoke", {"reason": "misuse"}
        )
        # The cascade already marked the export rejected; the queued request
        # is refused with the persisted denial rather than re-attempted.
        with self.assertRaises(ReleaseDenied):
            self.service.transition(self.user, export["id"], "continue", {})

        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")
        self.assertIsNone(self.repo.get_receipt(export["id"]))
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 0)

    def test_withdrawn_application_rejects_queued_export(self):
        dataset, application, grant = self._provision()
        export = self._export(dataset, grant)

        self.service.transition(
            self.user, application["id"], "withdraw", {"reason": "no longer needed"}
        )
        with self.assertRaises(ReleaseDenied):
            self.service.transition(self.user, export["id"], "continue", {})
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")

    def test_expired_grant_cannot_be_re_released_on_publish(self):
        dataset, application, grant = self._provision(grant_expiry=self.past)
        export = self._export(dataset, grant)
        # The grant expires out of band: the open export is rejected.
        self.service.transition(
            self.admin, grant["id"], "expire", {"expired_at": self.past}
        )
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")

        # Even after re-publishing the dataset, the dead export stays dead.
        self.service.transition(
            self.admin, dataset["id"], "restrict", {"reason": "x"}
        )
        self.service.transition(self.admin, dataset["id"], "publish", {})
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")

    def test_purpose_change_after_queue_rejects_export(self):
        dataset, application, grant = self._provision()
        export = self._export(dataset, grant)
        self.service.transition(
            self.user, application["id"], "amend", {"purpose": "commercial resale"}
        )
        # The recorded purpose no longer matches: the queued export is
        # rejected now and cannot be re-released by a later publish cycle.
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")
        self.service.transition(
            self.admin, dataset["id"], "restrict", {"reason": "review"}
        )
        self.service.transition(self.admin, dataset["id"], "publish", {})
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")
        with self.assertRaises(ReleaseDenied):
            self.service.transition(self.user, export["id"], "continue", {})

    def test_publish_reconfirmation_uses_remaining_grant_window(self):
        # Grant still formally active while the dataset is restricted, but it
        # lapses during the lockdown; republish must not re-release the task.
        near_expiry = (date.today() + timedelta(days=5)).isoformat()
        dataset, _app, grant = self._provision(grant_expiry=near_expiry)
        export = self._export(dataset, grant)

        self.service.transition(
            self.admin, dataset["id"], "restrict", {"reason": "lockdown"}
        )
        with self.assertRaises(TemporarilyBlocked):
            self.service.transition(self.user, export["id"], "continue", {})

        # Time passes past the original credential's expiry; force the grant
        # into expired without the export watching (simulates an external job).
        grant_entity = self.repo.get_entity(grant["id"])
        grant_data = dict(grant_entity["data"])
        self.repo.update_entity(grant["id"], grant_entity["version"], "expired", grant_data)

        self.service.transition(self.admin, dataset["id"], "publish", {})
        self.assertEqual(self.repo.get_entity(export["id"])["status"], "rejected")
        reason = self.repo.get_entity(export["id"])["data"]["rejection"]["reason"]
        self.assertIn("expired", reason)
        with self.assertRaises(ReleaseDenied):
            self.service.transition(self.user, export["id"], "continue", {})

    def test_issued_receipt_is_kept_for_reconciliation_after_revoke(self):
        dataset, _app, grant = self._provision()
        export = self._export(dataset, grant)
        self.service.transition(self.user, export["id"], "continue", {})
        self.assertIsNotNone(self.repo.get_receipt(export["id"]))

        self.service.transition(
            self.admin, grant["id"], "revoke", {"reason": "later misuse"}
        )
        # The already-issued receipt survives and stays visible in the ledger.
        self.assertIsNotNone(self.repo.get_receipt(export["id"]))
        receipts = self.service.ledger()["receipts"]
        self.assertIn(export["id"], {row["export_id"] for row in receipts})

    # ------------------------------------------------------------------
    # Quota
    # ------------------------------------------------------------------
    def test_quota_exhaustion_blocks_but_does_not_debit(self):
        dataset, _app, grant = self._provision(quota_limit=1)
        first = self._export(dataset, grant)
        second = self._export(dataset, grant)

        self.service.transition(self.user, first["id"], "continue", {})
        with self.assertRaises(TemporarilyBlocked):
            self.service.transition(self.user, second["id"], "continue", {})

        self.assertEqual(self.repo.get_entity(second["id"])["status"], "queued")
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)
        self.assertEqual(len(self.repo.quota_entries(grant["id"])), 1)

    # ------------------------------------------------------------------
    # Crash recovery: only unconfirmed steps are redone
    # ------------------------------------------------------------------
    def test_crash_after_pull_resumes_without_double_quota_or_double_receipt(self):
        dataset, _app, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)

        fired = {"on": False}

        def crash_after_pull(eid, step):
            if step == "pull" and not fired["on"]:
                fired["on"] = True
                raise SimulatedCrash("worker died")

        self.service.crash_hook = crash_after_pull
        with self.assertRaises(SimulatedCrash):
            self.service.transition(self.user, export["id"], "continue", {})
        self.service.crash_hook = None

        mid = self.repo.get_entity(export["id"])
        self.assertEqual(mid["status"], "pulling")
        self.assertEqual(mid["data"]["steps"], ["pull"])
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)

        # A second worker (fresh retry) resumes from the unconfirmed step.
        resumed = self.service.transition(
            self.user, export["id"], "continue", {}, expected_version=mid["version"]
        )
        self.assertEqual(resumed["status"], "receipted")
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)
        self.assertEqual(len(self.repo.quota_entries(grant["id"])), 1)
        self.assertEqual(
            len([r for r in self.service.receipts() if r["export_id"] == export["id"]]),
            1,
        )

    def test_repeated_continue_after_receipt_does_not_duplicate(self):
        dataset, _app, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)
        self.service.transition(self.user, export["id"], "continue", {})

        # A retried request after completion is an idempotent replay: same
        # entity, no second quota debit, no second receipt.
        replay = self.service.transition(self.user, export["id"], "continue", {})
        self.assertEqual(replay["status"], "receipted")
        self.assertEqual(replay["version"], 3)

        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)
        self.assertEqual(
            len([r for r in self.service.receipts() if r["export_id"] == export["id"]]),
            1,
        )

    # ------------------------------------------------------------------
    # Concurrency: continue vs cancel on the same queued export
    # ------------------------------------------------------------------
    def test_concurrent_continue_and_cancel_only_one_wins(self):
        dataset, _app, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)

        # Hold one worker inside its step transaction so the second action is
        # forced to race against the version lock instead of trivially serializing.
        can_finish = threading.Event()
        entered_pull = threading.Event()

        original_step_pull = self.service._step_pull

        def slow_pull(actor, entity, expected_version):
            entered_pull.set()
            self.assertTrue(can_finish.wait(timeout=5))
            return original_step_pull(actor, entity, expected_version)

        self.service._step_pull = slow_pull
        outcomes = {}

        def run_continue():
            try:
                outcomes["continue"] = self.service.transition(
                    self.user, export["id"], "continue", {}
                )["status"]
            except Exception as exc:  # the loser records why
                outcomes["continue"] = "error:" + type(exc).__name__

        thread = threading.Thread(target=run_continue)
        thread.start()
        self.assertTrue(entered_pull.wait(timeout=5))

        # Admin cancels while the pull is in flight (waits on the write lock).
        cancel_result = {}

        def run_cancel():
            try:
                entity = self.service.transition(
                    self.admin,
                    export["id"],
                    "cancel",
                    {"reason": "duplicate request"},
                    expected_version=export["version"],
                )
                cancel_result["status"] = entity["status"]
            except Exception as exc:
                cancel_result["status"] = "error:" + type(exc).__name__

        cancel_thread = threading.Thread(target=run_cancel)
        cancel_thread.start()
        # Give the cancel thread time to block on BEGIN IMMEDIATE.
        import time
        time.sleep(0.2)
        can_finish.set()
        thread.join(timeout=5)
        cancel_thread.join(timeout=5)

        final = self.repo.get_entity(export["id"])
        self.assertIn(final["status"], ("receipted", "cancelled"))
        if final["status"] == "receipted":
            self.assertEqual(outcomes["continue"], "receipted")
            self.assertTrue(cancel_result["status"].startswith("error:"))
            self.assertIsNotNone(self.repo.get_receipt(export["id"]))
        else:
            self.assertEqual(cancel_result["status"], "cancelled")
            self.assertTrue(outcomes["continue"].startswith("error:"))
            self.assertIsNone(self.repo.get_receipt(export["id"]))
            self.assertEqual(self.repo.quota_consumed(grant["id"]), 0)
        # Whoever lost can read the newest version and sees what really happened.
        self.assertEqual(
            final["version"], 3 if final["status"] == "receipted" else 2
        )
        self.assertEqual(
            len([r for r in self.service.receipts() if r["export_id"] == export["id"]]),
            1 if final["status"] == "receipted" else 0,
        )

    def test_two_concurrent_continues_do_not_double_debit(self):
        dataset, _app, grant = self._provision(quota_limit=10)
        export = self._export(dataset, grant)
        outcomes = []
        conflicts = []
        lock = threading.Lock()

        def run():
            try:
                result = self.service.transition(self.user, export["id"], "continue", {})
                with lock:
                    outcomes.append(result["status"])
            except ConflictError as exc:
                with lock:
                    outcomes.append("error:ConflictError")
                    conflicts.append(str(exc))

        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        final = self.repo.get_entity(export["id"])
        self.assertEqual(final["status"], "receipted")
        # Both callers converge on the same fact; what matters is that the
        # work happened exactly once (one debit, one receipt).
        self.assertTrue(all(o in ("receipted", "error:ConflictError") for o in outcomes))
        self.assertIn("receipted", outcomes)
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)
        self.assertEqual(len(self.repo.quota_entries(grant["id"])), 1)
        self.assertEqual(
            len([r for r in self.service.receipts() if r["export_id"] == export["id"]]),
            1,
        )
        # A loser that saw a conflict learns the newest version; replaying
        # against it is an idempotent no-op (still one debit, one receipt).
        for message in conflicts:
            self.assertIn("version %s" % final["version"], message)
        replay = self.service.transition(self.user, export["id"], "continue", {})
        self.assertEqual(replay["version"], final["version"])
        self.assertEqual(self.repo.quota_consumed(grant["id"]), 1)


if __name__ == "__main__":
    unittest.main()
