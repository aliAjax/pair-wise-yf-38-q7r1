import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


ADMIN = Actor("admin", "admin")
APPLICANT = Actor("applicant-1", "applicant")
COMMITTEE = Actor("committee-1", "committee")


class ExportLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_chain(self, grant_quota=100, grant_starts="2026-01-01",
                     grant_expires="2099-12-31", app_expires="2099-12-31"):
        dataset = self.service.create(
            ADMIN, "dataset", {"name": "Rare Disease Cohort", "access_policy": "controlled"}
        )
        self.service.transition(ADMIN, dataset["id"], "publish", {})
        application = self.service.create(
            ADMIN, "application",
            {"dataset_id": dataset["id"], "applicant_id": "APP-1", "purpose": "variant analysis"},
        )
        self.service.transition(ADMIN, application["id"], "submit", {})
        self.service.transition(ADMIN, application["id"], "review", {"committee_id": "committee-a"})
        self.service.transition(
            ADMIN, application["id"], "approve",
            {"approvals": ["r1", "r2", "r3"], "terms": "noncommercial", "expires_at": app_expires},
        )
        grant = self.service.create(
            ADMIN, "grant",
            {"application_id": application["id"], "dataset_id": dataset["id"],
             "recipient": "researcher-1", "quota_total": grant_quota, "quota_used": 0},
        )
        self.service.transition(
            ADMIN, grant["id"], "activate",
            {"starts_at": grant_starts, "expires_at": grant_expires},
        )
        return dataset, application, grant

    def _create_task(self, dataset, application, grant, quota=10, purpose="variant analysis"):
        return self.service.create(
            APPLICANT, "export_task",
            {"dataset_id": dataset["id"], "application_id": application["id"],
             "grant_id": grant["id"], "purpose": purpose, "quota": quota},
        )

    def _receipt(self, task):
        return self.repo.get_entity(task["data"]["receipt_id"])

    # --- happy path -------------------------------------------------------

    def test_release_valid_chain(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)
        self.assertEqual(task["status"], "queued")

        released = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(released["status"], "released")
        self.assertTrue(released["data"]["steps"]["chain_checked"])
        self.assertTrue(released["data"]["steps"]["quota_deducted"])
        self.assertTrue(released["data"]["steps"]["receipt_issued"])

        receipt = self._receipt(released)
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt["kind"], "receipt")
        self.assertEqual(receipt["status"], "issued")
        self.assertEqual(receipt["data"]["task_id"], task["id"])
        self.assertEqual(receipt["data"]["quota"], 10)

        updated_grant = self.repo.get_entity(grant["id"])
        self.assertEqual(updated_grant["data"]["quota_used"], 10)

    def test_release_twice_is_idempotent(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        first = self.service.release_export(APPLICANT, task["id"])
        second = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(second["status"], "released")

        # Exactly one receipt, quota deducted exactly once.
        receipts = [e for e in self.repo.list_entities(kind="receipt")]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["data"]["task_id"], task["id"])
        updated_grant = self.repo.get_entity(grant["id"])
        self.assertEqual(updated_grant["data"]["quota_used"], 10)

    def test_resume_after_crash_completes_pending_steps(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant, quota=5)

        # Simulate a crash after the chain check was recorded but before quota
        # was deducted: only the chain_checked step is confirmed.
        v1 = task["version"]
        self.repo.record_step(task["id"], "chain_checked", v1)

        resumed = self.service.resume_export(APPLICANT, task["id"])
        self.assertEqual(resumed["status"], "released")
        self.assertTrue(resumed["data"]["steps"]["quota_deducted"])
        self.assertTrue(resumed["data"]["steps"]["receipt_issued"])

        updated_grant = self.repo.get_entity(grant["id"])
        self.assertEqual(updated_grant["data"]["quota_used"], 5)
        receipts = [e for e in self.repo.list_entities(kind="receipt")]
        self.assertEqual(len(receipts), 1)

    def test_resume_after_crash_skips_confirmed_quota_step(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant, quota=5)

        # Simulate a crash after quota was deducted but before the receipt was
        # issued: chain_checked and quota_deducted are confirmed.
        v1 = task["version"]
        self.repo.record_step(task["id"], "chain_checked", v1)
        self.repo.deduct_quota(grant["id"], 5)
        v2 = self.repo.get_entity(task["id"])["version"]
        self.repo.record_step(task["id"], "quota_deducted", v2)

        resumed = self.service.resume_export(APPLICANT, task["id"])
        self.assertEqual(resumed["status"], "released")

        # Quota must not be deducted a second time.
        updated_grant = self.repo.get_entity(grant["id"])
        self.assertEqual(updated_grant["data"]["quota_used"], 5)
        receipts = [e for e in self.repo.list_entities(kind="receipt")]
        self.assertEqual(len(receipts), 1)

    # --- rejection paths --------------------------------------------------

    def test_release_after_dataset_restrict_rejected(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        self.service.transition(ADMIN, dataset["id"], "restrict", {"reason": "review"})
        blocked = self.repo.get_entity(task["id"])
        self.assertEqual(blocked["status"], "blocked")

        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("dataset is not published", rejected["data"]["reject_reason"])

        # No receipt, no quota deduction.
        self.assertEqual(len(self.repo.list_entities(kind="receipt")), 0)
        updated_grant = self.repo.get_entity(grant["id"])
        self.assertEqual(updated_grant["data"]["quota_used"], 0)

    def test_release_after_grant_revoke_rejected(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        self.service.transition(ADMIN, grant["id"], "revoke", {"reason": "purpose changed"})
        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("revoked", rejected["data"]["reject_reason"])
        self.assertEqual(len(self.repo.list_entities(kind="receipt")), 0)

    def test_release_after_grant_expire_rejected(self):
        dataset, application, grant = self._setup_chain(
            grant_starts="2020-01-01", grant_expires="2020-12-31"
        )
        task = self._create_task(dataset, application, grant)

        self.service.transition(ADMIN, grant["id"], "expire", {"expired_at": "2026-10-01"})
        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("expired", rejected["data"]["reject_reason"])
        self.assertEqual(len(self.repo.list_entities(kind="receipt")), 0)

    def test_release_with_expired_grant_window_rejected(self):
        dataset, application, grant = self._setup_chain(
            grant_starts="2020-01-01", grant_expires="2020-12-31"
        )
        task = self._create_task(dataset, application, grant)

        # Grant is still active but its validity window has elapsed.
        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("grant has expired", rejected["data"]["reject_reason"])

    def test_release_with_expired_application_rejected(self):
        dataset, application, grant = self._setup_chain(app_expires="2020-12-31")
        task = self._create_task(dataset, application, grant)

        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("application has expired", rejected["data"]["reject_reason"])

    def test_release_with_purpose_change_rejected(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        self.service.transition(
            APPLICANT, application["id"], "amend",
            {"purpose": "secondary analysis", "reason": "scope expanded"},
        )
        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("application purpose has changed", rejected["data"]["reject_reason"])

    def test_release_with_insufficient_quota_rejected(self):
        dataset, application, grant = self._setup_chain(grant_quota=3)
        task = self._create_task(dataset, application, grant, quota=10)

        rejected = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("grant quota insufficient", rejected["data"]["reject_reason"])
        self.assertEqual(len(self.repo.list_entities(kind="receipt")), 0)

    # --- re-confirmation on re-publication -------------------------------

    def test_republish_reconfirms_and_allows_release(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        self.service.transition(ADMIN, dataset["id"], "restrict", {"reason": "review"})
        self.assertEqual(self.repo.get_entity(task["id"])["status"], "blocked")

        self.service.transition(ADMIN, dataset["id"], "publish", {})
        self.assertEqual(self.repo.get_entity(task["id"])["status"], "queued")

        released = self.service.release_export(APPLICANT, task["id"])
        self.assertEqual(released["status"], "released")
        self.assertIsNotNone(self._receipt(released))

    def test_republish_with_expired_grant_rejected(self):
        dataset, application, grant = self._setup_chain(
            grant_starts="2020-01-01", grant_expires="2020-12-31"
        )
        task = self._create_task(dataset, application, grant)

        self.service.transition(ADMIN, dataset["id"], "restrict", {"reason": "review"})
        self.service.transition(ADMIN, dataset["id"], "publish", {})

        rejected = self.repo.get_entity(task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("grant has expired", rejected["data"]["reject_reason"])

    def test_republish_with_purpose_change_rejected(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        self.service.transition(ADMIN, dataset["id"], "restrict", {"reason": "review"})
        self.service.transition(
            APPLICANT, application["id"], "amend",
            {"purpose": "secondary analysis", "reason": "scope expanded"},
        )
        self.service.transition(ADMIN, dataset["id"], "publish", {})

        rejected = self.repo.get_entity(task["id"])
        self.assertEqual(rejected["status"], "rejected")
        self.assertIn("application purpose has changed", rejected["data"]["reject_reason"])

    # --- receipts are preserved ------------------------------------------

    def test_receipts_preserved_after_restriction(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)
        released = self.service.release_export(APPLICANT, task["id"])
        receipt_id = released["data"]["receipt_id"]

        self.service.transition(ADMIN, dataset["id"], "restrict", {"reason": "review"})

        # The receipt is immutable and still present for reconciliation.
        receipt = self.repo.get_entity(receipt_id)
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt["status"], "issued")
        self.assertEqual(receipt["data"]["task_id"], task["id"])

    # --- cancellation -----------------------------------------------------

    def test_cancel_queued_task(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)

        cancelled = self.service.transition(APPLICANT, task["id"], "cancel", {"reason": "no longer needed"})
        self.assertEqual(cancelled["status"], "cancelled")
        with self.assertRaises(ConflictError):
            self.service.release_export(APPLICANT, task["id"])

    # --- concurrency: continue vs cancel ---------------------------------

    def test_release_and_cancel_concurrently_one_wins(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)
        v1 = task["version"]

        # Continue wins: the release lands first.
        released = self.service.release_export(APPLICANT, task["id"], expected_version=v1)
        self.assertEqual(released["status"], "released")

        # Cancel arrives with the stale version and must see the latest state.
        with self.assertRaises(ConflictError) as ctx:
            self.service.transition(APPLICANT, task["id"], "cancel", {"reason": "too late"}, expected_version=v1)
        current = ctx.exception.details.get("current")
        self.assertIsNotNone(current)
        self.assertEqual(current["status"], "released")
        self.assertEqual(current["version"], released["version"])

    def test_cancel_and_release_concurrently_one_wins(self):
        dataset, application, grant = self._setup_chain()
        task = self._create_task(dataset, application, grant)
        v1 = task["version"]

        # Cancel wins.
        cancelled = self.service.transition(
            APPLICANT, task["id"], "cancel", {"reason": "no longer needed"}, expected_version=v1
        )
        self.assertEqual(cancelled["status"], "cancelled")

        # Release arrives with the stale version and must see the latest state.
        with self.assertRaises(ConflictError) as ctx:
            self.service.release_export(APPLICANT, task["id"], expected_version=v1)
        current = ctx.exception.details.get("current")
        self.assertIsNotNone(current)
        self.assertEqual(current["status"], "cancelled")
        self.assertEqual(current["version"], cancelled["version"])


if __name__ == "__main__":
    unittest.main()
