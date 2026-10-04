from datetime import datetime, timedelta

from .domain import (
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
    utcnow,
)


def _validate_dataset(actor, data, lookup):
    if len(data.get("access_policy", "")) < 3:
        raise ValidationError("access_policy is required")


def _validate_application(actor, data, lookup):
    dataset = _find_one(lookup, "dataset", "id", data.get("dataset_id"))
    if not dataset:
        raise ValidationError("dataset does not exist")
    if not data.get("purpose", "").strip():
        raise ValidationError("purpose is required")


def _validate_approve(actor, entity, data, lookup):
    approvals = data.get("approvals") or []
    if len(set(approvals)) < 3:
        raise ValidationError("at least three distinct committee approvals are required")
    if data.get("conflict_of_interest"):
        raise PermissionDenied("conflicted reviewer cannot approve access")


def valid_grant_window(expires_at, as_of):
    return str(expires_at) >= str(as_of)


def _validate_grant_activate(actor, entity, data, lookup):
    if data.get("expires_at") < data.get("starts_at"):
        raise ValidationError("grant expiry must be after start")
    return {"activated_by": actor.user_id}


def _validate_export_task(actor, data, lookup):
    dataset = _find_one(lookup, "dataset", "id", data.get("dataset_id"))
    if not dataset:
        raise ValidationError("dataset does not exist")
    application = _find_one(lookup, "application", "id", data.get("application_id"))
    if not application:
        raise ValidationError("application does not exist")
    grant = _find_one(lookup, "grant", "id", data.get("grant_id"))
    if not grant:
        raise ValidationError("grant does not exist")
    if application["data"].get("dataset_id") != dataset["id"]:
        raise ValidationError("application does not reference dataset")
    if grant["data"].get("application_id") != application["id"]:
        raise ValidationError("grant does not reference application")
    if grant["data"].get("dataset_id") != dataset["id"]:
        raise ValidationError("grant does not reference dataset")
    if not str(data.get("purpose", "")).strip():
        raise ValidationError("purpose is required")
    quota = data.get("quota")
    if quota is None or int(quota) <= 0:
        raise ValidationError("quota must be a positive integer")
    # Snapshot the approved purpose so a later purpose change can be detected.
    return {
        "purpose_snapshot": application["data"].get("purpose"),
        "steps": {},
    }


def check_export_release(task, dataset, application, grant, now=None):
    """Return ``(ok, reason)`` for releasing an export task.

    The dataset, application, grant and export task form a single consistency
    ledger: a release is valid only when the dataset is published, the
    application is approved and unexpired with an unchanged purpose, and the
    grant is active inside its validity window with enough remaining quota.
    """
    now = str(now or utcnow())
    if dataset is None or application is None or grant is None:
        return False, "referenced entity is missing"
    if dataset["status"] != "published":
        return False, "dataset is not published"
    if application["status"] != "approved":
        return False, "application is not approved"
    if application["data"].get("dataset_id") != dataset["id"]:
        return False, "application does not reference dataset"
    if str(application["data"].get("expires_at", "")) < now:
        return False, "application has expired"
    if task["data"].get("purpose_snapshot") != application["data"].get("purpose"):
        return False, "application purpose has changed"
    if grant["status"] == "revoked":
        return False, "grant has been revoked"
    if grant["status"] == "expired":
        return False, "grant has expired"
    if grant["status"] != "active":
        return False, "grant is not active"
    if grant["data"].get("application_id") != application["id"]:
        return False, "grant does not reference application"
    if grant["data"].get("dataset_id") != dataset["id"]:
        return False, "grant does not reference dataset"
    if str(grant["data"].get("starts_at", "")) > now:
        return False, "grant is not yet valid"
    if str(grant["data"].get("expires_at", "")) < now:
        return False, "grant has expired"
    remaining = int(grant["data"].get("quota_total", 0)) - int(grant["data"].get("quota_used", 0))
    if int(task["data"].get("quota", 0)) > remaining:
        return False, "grant quota insufficient"
    return True, None


CUSTOM_CREATE = {
    'dataset': _validate_dataset,
    'application': _validate_application,
    'export_task': _validate_export_task,
}
CUSTOM_TRANSITIONS = {
    ('application', 'approve'): _validate_approve,
    ('grant', 'activate'): _validate_grant_activate,
}


class RuleEngine:
    ALIASES = {
        'datasets': 'dataset',
        'applications': 'application',
        'grants': 'grant',
        'export_tasks': 'export_task',
        'receipts': 'receipt',
    }
    INITIAL_STATUS = {
        'dataset': 'registered',
        'application': 'draft',
        'grant': 'issued',
        'export_task': 'queued',
        'receipt': 'issued',
    }
    TRANSITIONS = {
        'dataset': {
            'restrict': (('registered', 'published'), 'restricted'),
            'publish': (('registered', 'restricted'), 'published'),
        },
        'application': {
            'submit': (('draft',), 'submitted'),
            'review': (('submitted',), 'under_review'),
            'approve': (('under_review',), 'approved'),
            'reject': (('under_review',), 'rejected'),
            'withdraw': (('submitted', 'under_review'), 'withdrawn'),
            'amend': (('draft', 'submitted', 'under_review', 'approved'), None),
        },
        'grant': {
            'activate': (('issued',), 'active'),
            'revoke': (('active',), 'revoked'),
            'expire': (('active',), 'expired'),
        },
        'export_task': {
            'cancel': (('queued', 'blocked'), 'cancelled'),
        },
        'receipt': {},
    }
    CREATE_REQUIRED = {
        'dataset': ('name', 'access_policy'),
        'application': ('dataset_id', 'applicant_id', 'purpose'),
        'grant': ('application_id', 'dataset_id', 'recipient'),
        'export_task': ('dataset_id', 'application_id', 'grant_id', 'purpose', 'quota'),
    }
    ACTION_REQUIRED = {
        ('dataset', 'restrict'): ('reason',),
        ('application', 'review'): ('committee_id',),
        ('application', 'approve'): ('approvals', 'terms', 'expires_at'),
        ('application', 'reject'): ('reason',),
        ('application', 'withdraw'): ('reason',),
        ('application', 'amend'): ('purpose', 'reason'),
        ('grant', 'activate'): ('starts_at', 'expires_at'),
        ('grant', 'revoke'): ('reason',),
        ('grant', 'expire'): ('expired_at',),
        ('export_task', 'cancel'): ('reason',),
    }
    CREATE_ROLES = {
        'dataset': ('admin', 'committee'),
        'application': ('admin', 'applicant'),
        'grant': ('admin', 'committee'),
        'export_task': ('admin', 'applicant'),
        'receipt': (),
    }
    ROLE_ACTIONS = {
        'restrict': ('admin', 'committee'),
        'publish': ('admin', 'committee'),
        'submit': ('admin', 'applicant'),
        'review': ('admin', 'committee'),
        'approve': ('admin', 'committee'),
        'reject': ('admin', 'committee'),
        'withdraw': ('admin', 'applicant'),
        'amend': ('admin', 'applicant'),
        'activate': ('admin', 'committee'),
        'revoke': ('admin', 'committee'),
        'expire': ('admin', 'committee'),
        'cancel': ('admin', 'applicant'),
    }

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if "*" not in allowed and actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        if custom:
            extra = custom(actor, data, lookup) or {}
            data = dict(data)
            data.update(extra)
        return data

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        allowed_roles = self.ROLE_ACTIONS.get(
            (kind, action), self.ROLE_ACTIONS.get(action, ("admin",))
        )
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()
