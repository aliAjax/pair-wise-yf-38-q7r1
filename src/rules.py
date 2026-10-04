from datetime import datetime

from .domain import (
    InvalidTransition,
    PermissionDenied,
    ReleaseDenied,
    TemporarilyBlocked,
    ValidationError,
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
    if str(data.get("expires_at")) < str(data.get("starts_at")):
        raise ValidationError("grant expiry must be after start")
    return {"activated_by": actor.user_id}


def _validate_application_amend(actor, entity, data, lookup):
    # Applicants may amend their own application; the recorded purpose is the
    # anchor of the consistency ledger, so a changed purpose re-checks every
    # in-flight export downstream.
    if actor.role == "applicant" and entity["data"].get("applicant_id") != actor.user_id:
        raise PermissionDenied("only the original applicant may amend this application")
    if not str(data.get("purpose", "")).strip():
        raise ValidationError("purpose is required")
    return {}


def _validate_export(actor, data, lookup):
    dataset = _find_one(lookup, "dataset", "id", data.get("dataset_id"))
    if not dataset:
        raise ValidationError("dataset does not exist")
    grant = _find_one(lookup, "grant", "id", data.get("grant_id"))
    if not grant:
        raise ValidationError("grant does not exist")
    application = _find_one(lookup, "application", "id", grant["data"].get("application_id"))
    if not application:
        raise ValidationError("grant is not linked to an application")
    if grant["data"].get("dataset_id") != data.get("dataset_id"):
        raise ValidationError("grant does not cover this dataset")
    if str(application["id"]) != str(grant["data"].get("application_id")):
        raise ValidationError("grant and application do not match")
    if not data.get("requester_id"):
        data["requester_id"] = actor.user_id
    try:
        amount = int(data.get("amount", 1))
    except (TypeError, ValueError):
        raise ValidationError("amount must be an integer")
    if amount < 1:
        raise ValidationError("amount must be positive")
    data["amount"] = amount
    data["purpose_at_request"] = application["data"].get("purpose")
    data["grant_expires_at"] = grant["data"].get("expires_at")
    return dict(data)


def _validate_export_continue(actor, entity, data, lookup):
    _ensure_export_actor(actor, entity)


def _validate_export_cancel(actor, entity, data, lookup):
    _ensure_export_actor(actor, entity)
    if not str(data.get("reason", "")).strip():
        raise ValidationError("reason is required")


def _ensure_export_actor(actor, export):
    if actor.role == "admin":
        return
    if actor.role != "applicant" or export["data"].get("requester_id") != actor.user_id:
        raise PermissionDenied("only the requester or an admin may operate this export")


CUSTOM_CREATE = {
    'dataset': _validate_dataset,
    'application': _validate_application,
    'export': _validate_export,
}
CUSTOM_TRANSITIONS = {
    ('application', 'approve'): _validate_approve,
    ('application', 'amend'): _validate_application_amend,
    ('grant', 'activate'): _validate_grant_activate,
    ('export', 'continue'): _validate_export_continue,
    ('export', 'cancel'): _validate_export_cancel,
}


class RuleEngine:
    ALIASES = {
        'datasets': 'dataset',
        'applications': 'application',
        'grants': 'grant',
        'exports': 'export',
    }
    INITIAL_STATUS = {
        'dataset': 'registered',
        'application': 'draft',
        'grant': 'issued',
        'export': 'queued',
    }
    TRANSITIONS = {
        'dataset': {
            'restrict': (('registered', 'published'), 'restricted'),
            'publish': (('restricted',), 'published'),
        },
        'application': {
            'submit': (('draft',), 'submitted'),
            'review': (('submitted',), 'under_review'),
            'approve': (('under_review',), 'approved'),
            'reject': (('under_review',), 'rejected'),
            'withdraw': (('submitted', 'under_review', 'approved'), 'withdrawn'),
            # Amend is status-preserving: it updates the purpose anchor without
            # silently promoting a draft; downstream gates see the new purpose.
            'amend': (('draft', 'submitted', 'under_review', 'approved'), None),
        },
        'grant': {
            'activate': (('issued',), 'active'),
            'revoke': (('active',), 'revoked'),
            'expire': (('active',), 'expired'),
        },
        'export': {
            'continue': (('queued', 'pulling'), 'receipted'),
            'cancel': (('queued', 'pulling'), 'cancelled'),
        },
    }
    # Internal transitions driven by the saga machinery, never from the API.
    INTERNAL_TRANSITIONS = {
        ('export', 'pull'): (('queued',), 'pulling'),
        ('export', 'reject'): (('queued', 'pulling'), 'rejected'),
    }
    CREATE_REQUIRED = {
        'dataset': ('name', 'access_policy'),
        'application': ('dataset_id', 'applicant_id', 'purpose'),
        'grant': ('application_id', 'dataset_id', 'recipient'),
        'export': ('dataset_id', 'grant_id'),
    }
    ACTION_REQUIRED = {
        ('dataset', 'restrict'): ('reason',),
        ('application', 'review'): ('committee_id',),
        ('application', 'approve'): ('approvals', 'terms', 'expires_at'),
        ('application', 'reject'): ('reason',),
        ('application', 'withdraw'): ('reason',),
        ('application', 'amend'): ('purpose',),
        ('grant', 'activate'): ('starts_at', 'expires_at'),
        ('grant', 'revoke'): ('reason',),
        ('grant', 'expire'): ('expired_at',),
        ('export', 'cancel'): ('reason',),
    }
    CREATE_ROLES = {
        'dataset': ('admin', 'committee'),
        'application': ('admin', 'applicant'),
        'grant': ('admin', 'committee'),
        'export': ('admin', 'applicant'),
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
        'continue': ('admin', 'applicant'),
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
            custom(actor, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        return self._validate(actor, entity, action, data, lookup, internal=False)

    def validate_internal_transition(self, entity, action):
        transition = self.INTERNAL_TRANSITIONS.get((self.normalize_kind(entity["kind"]), action))
        if not transition:
            raise InvalidTransition("unknown internal action %s" % action)
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        return next_status

    def _validate(self, actor, entity, action, data, lookup, internal):
        kind = self.normalize_kind(entity["kind"])
        table = self.INTERNAL_TRANSITIONS if internal else self.TRANSITIONS.get(kind, {})
        transition = table.get((kind, action)) if internal else table.get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        if not internal:
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

    # ------------------------------------------------------------------
    # Consistency gate: dataset + application + grant must all agree.
    # ------------------------------------------------------------------
    def release_gate(self, export, dataset, application, grant, as_of, quota_remaining=None):
        """Evaluate whether one export pull is allowed to take data.

        Returns a list of confirmed release attributes. Raises
        TemporarilyBlocked while the dataset is restricted or quota is
        exhausted, and ReleaseDenied when the chain is terminally broken
        (withdrawn/expired/revoked/purpose drift).
        """
        if dataset is None:
            raise ReleaseDenied("dataset no longer exists")
        if grant is None:
            raise ReleaseDenied("grant %s no longer exists" % export["data"].get("grant_id"))
        if application is None:
            raise ReleaseDenied("application no longer exists")

        if dataset["status"] == "restricted":
            raise TemporarilyBlocked("dataset %s is restricted" % dataset["id"])
        if dataset["status"] != "published":
            raise TemporarilyBlocked(
                "dataset %s is not published (status %s)" % (dataset["id"], dataset["status"])
            )

        if application["status"] == "withdrawn":
            raise ReleaseDenied("application %s was withdrawn" % application["id"])
        if application["status"] != "approved":
            raise ReleaseDenied(
                "application %s is not approved (status %s)"
                % (application["id"], application["status"])
            )
        approved_until = application["data"].get("expires_at")
        if approved_until is not None and str(approved_until) < str(as_of):
            raise ReleaseDenied(
                "application %s expired at %s" % (application["id"], approved_until)
            )

        recorded_purpose = export["data"].get("purpose_at_request")
        current_purpose = application["data"].get("purpose")
        if recorded_purpose is not None and current_purpose != recorded_purpose:
            raise ReleaseDenied(
                "application purpose changed since export was queued: %r != %r"
                % (recorded_purpose, current_purpose)
            )

        if grant["status"] == "revoked":
            raise ReleaseDenied("grant %s was revoked" % grant["id"])
        if grant["status"] == "expired":
            raise ReleaseDenied("grant %s has expired" % grant["id"])
        if grant["status"] != "active":
            raise ReleaseDenied(
                "grant %s is not active (status %s)" % (grant["id"], grant["status"])
            )
        if not valid_grant_window(grant["data"].get("expires_at"), as_of):
            raise ReleaseDenied(
                "grant %s expired at %s" % (grant["id"], grant["data"].get("expires_at"))
            )
        if grant["data"].get("dataset_id") != dataset["id"]:
            raise ReleaseDenied("grant does not cover dataset %s" % dataset["id"])

        amount = int(export["data"].get("amount", 1))
        if quota_remaining is not None and quota_remaining < amount:
            raise TemporarilyBlocked(
                "quota exhausted: remaining %s, need %s" % (quota_remaining, amount)
            )

        return {
            "confirmed_at": str(as_of),
            "grant_expires_at": grant["data"].get("expires_at"),
            "grant_remaining": self.grant_remaining(
                grant["data"].get("expires_at"), as_of
            ),
            "purpose": current_purpose,
        }

    @staticmethod
    def grant_remaining(expires_at, as_of):
        """Whole days of validity left on the original credential."""
        try:
            expiry = _date_ordinal(expires_at)
        except (TypeError, ValueError):
            return 0
        return expiry - _date_ordinal(as_of)

    def reconfirm_for_publish(self, export, dataset, application, grant, as_of):
        """Re-check a queued export when a dataset is published again.

        Re-release only against the *original* grant's remaining window;
        expired grants or drifted purposes are terminally rejected instead
        of being silently re-allowed.
        """
        try:
            confirmed = self.release_gate(
                export, dataset, application, grant, as_of
            )
        except ReleaseDenied as denied:
            return {"allowed": False, "reason": str(denied)}
        except TemporarilyBlocked as blocked:
            return {"allowed": False, "reason": str(blocked)}
        remaining = self.grant_remaining(grant["data"].get("expires_at"), as_of)
        if remaining <= 0:
            return {
                "allowed": False,
                "reason": "grant has no remaining validity as of %s" % as_of,
            }
        confirmed["allowed"] = True
        confirmed["reconfirmed_at"] = str(as_of)
        return confirmed


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()
