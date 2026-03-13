"""Convenience helper to write human-readable activity log entries.

Usage:
    from app.utils.activity import log_activity

    log_activity("admin", "user_created", "user", "Gaurav", details="role=user")
    db.session.commit()   # caller commits

The entry is added to the session but NOT committed — the caller is
responsible for committing (so it participates in the same transaction).
"""

import logging
from app.extensions import db
from app.models.activity_log import ActivityLog

logger = logging.getLogger(__name__)


def log_activity(
    actor_username: str | None,
    action_type: str,
    entity_type: str = "",
    entity_name: str = "",
    details: str | None = None,
) -> None:
    """Create an ActivityLog row and add it to the current session.

    Safe to call inside any request context. If the session is later
    rolled back (e.g. due to a DB error), the activity row is discarded
    with it — which is the desired behaviour.
    """
    try:
        entry = ActivityLog(
            actor_username=actor_username,
            action_type=action_type,
            entity_type=entity_type,
            entity_name=entity_name,
            details=details,
        )
        db.session.add(entry)
    except Exception:
        # Never let activity logging break the primary operation.
        logger.exception("Failed to write activity log entry")
