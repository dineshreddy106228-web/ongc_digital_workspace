"""Shared rules for the password-reset request flow.

The login screen, the administration queue and the change-password page all
need the same answers — what counts as a CPF, what a temporary password may
be, when a request goes stale — so they live here rather than in three routes.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone

from flask import current_app
from sqlalchemy.exc import OperationalError, ProgrammingError

from app.extensions import db
from app.models.core.password_reset_request import (
    STATUS_EXPIRED,
    STATUS_PENDING,
    PasswordResetRequest,
)
from app.models.core.user import User


# Characters a person can read out over a bad phone line without being asked
# "was that a one or an ell?".
_UPPER = "ABCDEFGHJKLMNPQRSTUVWXYZ"
_LOWER = "abcdefghijkmnopqrstuvwxyz"
_DIGITS = "23456789"


def normalize_cpf(raw: str | None) -> str:
    """Strip the decoration people add to a CPF number when typing it."""
    if not raw:
        return ""
    return "".join(ch for ch in str(raw) if ch.isalnum()).upper()


def find_user_by_identifier(raw_value: str | None) -> User | None:
    """Resolve what someone typed on the forgot-password form to an account.

    A CPF number for ordinary staff, or a username — the administrator
    accounts are second accounts held by people who already use their CPF on
    their own login, so giving those accounts a CPF too would make the number
    ambiguous and could reset the wrong one.
    """
    if not raw_value:
        return None

    cpf = normalize_cpf(raw_value)
    if cpf:
        candidates = {cpf, cpf.lstrip("0")}
        candidates.discard("")
        for candidate in candidates:
            user = User.query.filter(
                db.func.upper(db.func.trim(User.employee_code)) == candidate
            ).first()
            if user is not None:
                return user

    # Usernames carry characters normalize_cpf strips, so they are matched on
    # the raw value rather than the normalised one.
    username = str(raw_value).strip().lower()
    if not username:
        return None
    return User.query.filter(
        db.func.lower(db.func.trim(User.username)) == username
    ).first()


def temp_password_ttl_hours() -> int:
    return max(int(current_app.config.get("PASSWORD_RESET_TEMP_TTL_HOURS", 3)), 1)


def request_ttl_hours() -> int:
    return max(int(current_app.config.get("PASSWORD_RESET_REQUEST_TTL_HOURS", 24)), 1)


def preset_passwords() -> list[str]:
    return list(current_app.config.get("PASSWORD_RESET_PRESETS", []))


def password_min_length() -> int:
    return max(int(current_app.config.get("PASSWORD_MIN_LENGTH", 8)), 8)


def generate_temporary_password() -> str:
    """A one-off password that is unguessable but still dictatable by phone."""
    body = [secrets.choice(_LOWER) for _ in range(4)]
    tail = [secrets.choice(_DIGITS) for _ in range(4)]
    return f"{secrets.choice(_UPPER)}{''.join(body)}@{''.join(tail)}"


def validate_chosen_password(password: str) -> str | None:
    """Return an error message when *password* may not be issued or kept."""
    if not password:
        return "A password is required."
    minimum = password_min_length()
    if len(password) < minimum:
        return f"Password must be at least {minimum} characters."
    if len(password) > 128:
        return "Password must be 128 characters or fewer."
    return None


def is_preset_password(password: str) -> bool:
    """True when the value is one of the shortlist admins dictate by phone."""
    if not password:
        return False
    folded = password.casefold()
    return any(preset.casefold() == folded for preset in preset_passwords())


def expire_stale_requests() -> int:
    """Mark unhandled requests older than the request TTL as expired."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=request_ttl_hours())
    try:
        stale = (
            PasswordResetRequest.query.filter(
                PasswordResetRequest.status == STATUS_PENDING,
                PasswordResetRequest.created_at < cutoff,
            ).all()
        )
    except (ProgrammingError, OperationalError):
        # The table arrives with a migration; a pre-migration process must not
        # 500 on the login screen because of it.
        db.session.rollback()
        return 0

    for entry in stale:
        entry.status = STATUS_EXPIRED
        entry.handled_note = "Expired before an administrator handled it."
    if stale:
        db.session.commit()
    return len(stale)


def pending_request_count() -> int:
    """Number of reset requests waiting on an administrator."""
    try:
        return PasswordResetRequest.query.filter_by(status=STATUS_PENDING).count()
    except (ProgrammingError, OperationalError):
        db.session.rollback()
        return 0
