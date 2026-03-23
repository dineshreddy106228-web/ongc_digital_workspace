"""Reusable notification service helpers.

All helpers participate in the caller's current SQLAlchemy transaction.
They add/flush changes but do not commit the session.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.extensions import cache, db
from app.models.core.notification import Notification, NOTIFICATION_SEVERITIES
from app.models.core.user import User


VALID_NOTIFICATION_SEVERITIES = set(NOTIFICATION_SEVERITIES)
WELCOME_NOTIFICATION_TITLE = "Welcome to your workspace"
WELCOME_NOTIFICATION_MESSAGE = "We're glad you're here. Your workspace is ready whenever you are."
WELCOME_NOTIFICATION_LINK = "/dashboard"
WELCOME_NOTIFICATION_SEVERITY = "success"
LEGACY_WELCOME_NOTIFICATION_TITLES = (
    WELCOME_NOTIFICATION_TITLE,
    "Welcome to ONGC Digital Workspace",
)


def _coerce_user_id(user_id):
    try:
        return int(user_id)
    except (TypeError, ValueError):
        return None


def _normalize_limit(limit: int, default: int) -> int:
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        return default
    return max(limit, 1)


def _normalize_severity(severity: str | None) -> str:
    candidate = (severity or "info").strip().lower()
    if candidate not in VALID_NOTIFICATION_SEVERITIES:
        return "info"
    return candidate


def _get_notifications_by_ids(notification_ids: list[int]) -> list[Notification]:
    if not notification_ids:
        return []

    rows = Notification.query.filter(Notification.id.in_(notification_ids)).all()
    rows_by_id = {row.id: row for row in rows}
    return [rows_by_id[row_id] for row_id in notification_ids if row_id in rows_by_id]


def create_notification(user_id, title, message, severity="info", link=None):
    """Create a notification row for a user and flush it into the current session."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return None

    if not db.session.get(User, user_id):
        return None

    clean_title = (title or "").strip()
    clean_message = (message or "").strip()
    if not clean_title or not clean_message:
        return None

    notification = Notification(
        user_id=user_id,
        title=clean_title,
        message=clean_message,
        severity=_normalize_severity(severity),
        link=(link or "").strip() or None,
        is_read=False,
    )
    db.session.add(notification)
    db.session.flush()
    invalidate_notification_cache(user_id)
    return notification


def get_welcome_notifications_for_user(user_id):
    """Return welcome notifications for a user, including legacy welcome titles."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return []
    return (
        Notification.query
        .filter(
            Notification.user_id == user_id,
            Notification.title.in_(LEGACY_WELCOME_NOTIFICATION_TITLES),
        )
        .order_by(Notification.created_at.asc(), Notification.id.asc())
        .all()
    )


def ensure_welcome_notification(user_id, *, is_read: bool | None = None):
    """Ensure one normalized welcome notification exists for the user."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return None

    user = db.session.get(User, user_id)
    if user is None:
        return None

    welcome_notifications = get_welcome_notifications_for_user(user_id)
    notification = welcome_notifications[0] if welcome_notifications else None

    if notification is None:
        notification = create_notification(
            user_id=user_id,
            title=WELCOME_NOTIFICATION_TITLE,
            message=WELCOME_NOTIFICATION_MESSAGE,
            severity=WELCOME_NOTIFICATION_SEVERITY,
            link=WELCOME_NOTIFICATION_LINK,
        )
        if notification is None:
            return None
    else:
        notification.title = WELCOME_NOTIFICATION_TITLE
        notification.message = WELCOME_NOTIFICATION_MESSAGE
        notification.severity = WELCOME_NOTIFICATION_SEVERITY
        notification.link = WELCOME_NOTIFICATION_LINK
        notification.updated_at = datetime.now(timezone.utc)
        db.session.flush()
        invalidate_notification_cache(user_id)

    for duplicate in welcome_notifications[1:]:
        db.session.delete(duplicate)

    if is_read is not None and notification.is_read != bool(is_read):
        notification.is_read = bool(is_read)
        notification.updated_at = datetime.now(timezone.utc)

    db.session.flush()
    invalidate_notification_cache(user_id)
    return notification


def reset_all_notifications_to_welcome_state() -> dict[str, int]:
    """Collapse every user's notifications down to one normalized welcome notification."""
    user_ids = {
        int(user_id)
        for (user_id,) in User.query.with_entities(User.id).all()
        if _coerce_user_id(user_id) is not None
    }
    user_ids.update(
        int(user_id)
        for (user_id,) in Notification.query.with_entities(Notification.user_id).distinct().all()
        if _coerce_user_id(user_id) is not None
    )

    removed = 0
    normalized = 0
    created = 0

    for user_id in sorted(user_ids):
        existing_welcome_ids = {
            notification.id
            for notification in get_welcome_notifications_for_user(user_id)
        }
        if existing_welcome_ids:
            notification = ensure_welcome_notification(user_id)
            if notification is not None:
                normalized += 1
            keep_id = notification.id if notification is not None else None
        else:
            keep_id = None

        query = Notification.query.filter(Notification.user_id == user_id)
        if keep_id is not None:
            deleted = query.filter(Notification.id != keep_id).delete(synchronize_session=False)
        else:
            deleted = query.delete(synchronize_session=False)
        removed += int(deleted or 0)

        if keep_id is None:
            notification = ensure_welcome_notification(user_id)
            if notification is not None:
                created += 1

        invalidate_notification_cache(user_id)

    return {
        "user_count": len(user_ids),
        "removed_count": removed,
        "normalized_count": normalized,
        "created_count": created,
    }


def get_notification(notification_id, user_id=None):
    """Return a single notification, optionally scoped to a user."""
    query = Notification.query.filter_by(id=notification_id)
    if user_id is not None:
        query = query.filter_by(user_id=user_id)
    return query.first()


def mark_notification_read(notification_id, user_id=None):
    """Mark a single notification as read if it exists in scope."""
    notification = get_notification(notification_id, user_id=user_id)
    if notification is None or notification.is_read:
        return notification

    notification.is_read = True
    notification.updated_at = datetime.now(timezone.utc)
    db.session.flush()
    invalidate_notification_cache(notification.user_id)
    return notification


def mark_all_notifications_read(user_id):
    """Mark all unread notifications as read for a user."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return 0

    now = datetime.now(timezone.utc)
    updated = (
        Notification.query
        .filter_by(user_id=user_id, is_read=False)
        .update(
            {
                Notification.is_read: True,
                Notification.updated_at: now,
            },
            synchronize_session=False,
        )
    )
    if updated:
        db.session.flush()
        invalidate_notification_cache(user_id)
    return updated


@cache.memoize(timeout=300)
def _get_unread_notification_ids(user_id, limit=10) -> list[int]:
    """Return cached unread notification ids for a user."""
    return [
        row.id
        for row in (
            Notification.query
            .filter_by(user_id=user_id, is_read=False)
            .order_by(Notification.created_at.desc())
            .limit(_normalize_limit(limit, 10))
            .all()
        )
    ]


def get_unread_notifications(user_id, limit=10):
    """Return the latest unread notifications for a user."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return []
    return _get_notifications_by_ids(_get_unread_notification_ids(user_id, limit))


@cache.memoize(timeout=300)
def _get_recent_notification_ids(user_id, limit=10) -> list[int]:
    """Return cached recent notification ids for a user."""
    return [
        row.id
        for row in (
            Notification.query
            .filter_by(user_id=user_id)
            .order_by(Notification.created_at.desc())
            .limit(_normalize_limit(limit, 10))
            .all()
        )
    ]


def get_recent_notifications(user_id, limit=10):
    """Return the latest notifications, regardless of read state."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return []
    return _get_notifications_by_ids(_get_recent_notification_ids(user_id, limit))


@cache.memoize(timeout=300)
def get_unread_notification_count(user_id):
    """Return the unread notification count for a user."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return 0
    return Notification.query.filter_by(user_id=user_id, is_read=False).count()


def invalidate_unread_notification_count(user_id) -> None:
    """Clear the cached unread count for a user."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return
    cache.delete_memoized(get_unread_notification_count, user_id)


def invalidate_notification_cache(user_id) -> None:
    """Clear all cached notification reads for a user."""
    user_id = _coerce_user_id(user_id)
    if user_id is None:
        return

    invalidate_unread_notification_count(user_id)
    cache.delete_memoized(_get_unread_notification_ids, user_id, 5)
    cache.delete_memoized(_get_unread_notification_ids, user_id, 10)
    cache.delete_memoized(_get_recent_notification_ids, user_id, 5)
    cache.delete_memoized(_get_recent_notification_ids, user_id, 10)
    cache.delete_memoized(_get_recent_notification_ids, user_id, 100)
