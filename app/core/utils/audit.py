"""Convenience helper to write audit log entries."""

from app.extensions import db
from app.models.core.audit_log import AuditLog
from app.core.utils.request_meta import get_client_ip, get_user_agent


def log_action(
    action: str,
    user_id: int = None,
    entity_type: str = None,
    entity_id: str = None,
    details: str = None,
):
    """Write an audit log row and flush (caller should commit the session)."""
    entry = AuditLog(
        user_id=user_id,
        action=action,
        entity_type=entity_type,
        entity_id=str(entity_id) if entity_id is not None else None,
        details=details,
        ip_address=get_client_ip(),
        user_agent=get_user_agent(),
    )
    db.session.add(entry)
