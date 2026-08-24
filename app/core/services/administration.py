"""Administration access and its audit trail, shared by every module.

An administration page changes how a module behaves for everyone, so only a
superuser may change it. Everyone else can still open it and read what the
current settings are — hiding it made the settings invisible to the people
working under them.

Every change is written to the audit log with the same shape, so each module's
administration page can show its own trail without inventing a format.
"""
from __future__ import annotations

from flask_login import current_user

from app.extensions import db
from app.core.utils.activity import log_activity
from app.core.utils.request_meta import get_user_agent
from app.models.core.audit_log import AuditLog
from app.models.core.user import User

# One action per module, so a trail can be read back per module.
ADMIN_ACTIONS = {
    "inventory": "INVENTORY_ADMIN_UPDATED",
    "quality_control": "QC_ADMIN_UPDATED",
}


def can_edit_administration(user=None) -> bool:
    """Only a superuser may change a module's administration settings."""
    user = user or current_user
    return bool(user and getattr(user, "is_authenticated", False) and user.is_super_user())


def record_admin_change(module_key: str, summary: str, entity_id: str = "", ip_address: str = "") -> None:
    """Write one administration change to the audit log.

    Flushed with the caller's transaction, so a change and its record either
    both land or neither does.
    """
    AuditLog.log(
        action=ADMIN_ACTIONS.get(module_key, "MODULE_ADMIN_UPDATED"),
        user_id=getattr(current_user, "id", None),
        entity_type="ModuleAdministration",
        entity_id=entity_id or module_key,
        details=summary,
        ip_address=ip_address,
        user_agent=get_user_agent(),
    )
    log_activity(
        getattr(current_user, "username", ""),
        "module_admin_updated",
        "administration",
        module_key,
        details=summary,
    )


def administration_trail(module_key: str, limit: int = 20) -> list[dict]:
    """Recent administration changes for one module, newest first.

    The actor's name is resolved for display but the row itself keeps only the
    id, exactly as the audit log stores it.
    """
    action = ADMIN_ACTIONS.get(module_key, "MODULE_ADMIN_UPDATED")
    entries = (
        AuditLog.query.filter(AuditLog.action == action)
        .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
        .limit(limit)
        .all()
    )
    if not entries:
        return []

    user_ids = {entry.user_id for entry in entries if entry.user_id}
    names = {}
    if user_ids:
        for user in User.query.filter(User.id.in_(user_ids)).all():
            names[user.id] = user.full_name or user.username

    return [
        {
            "actor": names.get(entry.user_id, "Unknown user"),
            "details": entry.details or "",
            "created_at": entry.created_at,
        }
        for entry in entries
    ]
