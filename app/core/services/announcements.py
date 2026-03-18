"""Announcement delivery, polling, and recipient helpers."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from types import SimpleNamespace

from flask import url_for
from sqlalchemy import or_

from app.core.roles import SUPERUSER_ROLE, canonicalize_role_name
from app.core.services.notifications import create_notification
from app.extensions import db
from app.models.core.announcement import (
    ANNOUNCEMENT_STATUSES,
    ANNOUNCEMENT_TYPES,
    Announcement,
    AnnouncementOption,
    AnnouncementRecipient,
    AnnouncementVote,
)
from app.models.core.user import User


VALID_ANNOUNCEMENT_TYPES = set(ANNOUNCEMENT_TYPES)
VALID_ANNOUNCEMENT_STATUSES = set(ANNOUNCEMENT_STATUSES)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _is_active_for_delivery(announcement: Announcement) -> bool:
    now = _now_utc()
    if announcement.status != "PUBLISHED":
        return False
    if announcement.published_at and announcement.published_at > now:
        return False
    if announcement.expires_at and announcement.expires_at < now:
        return False
    if announcement.closed_at:
        return False
    return True


def get_target_announcement_users() -> list[User]:
    """Return all active users — broadcasts are delivered to every user in the app."""
    return (
        User.query
        .filter(User.is_active.is_(True))
        .order_by(User.full_name.asc(), User.username.asc())
        .all()
    )


def validate_announcement_payload(
    title: str,
    body: str,
    announcement_type: str,
    expires_at: datetime | None,
    option_texts: list[str] | None = None,
) -> list[str]:
    errors = []
    clean_type = (announcement_type or "").strip().upper()
    if clean_type not in VALID_ANNOUNCEMENT_TYPES:
        errors.append("Select a valid announcement type.")
    if not (title or "").strip():
        errors.append("Title is required.")
    if len((title or "").strip()) > 255:
        errors.append("Title cannot exceed 255 characters.")
    if not (body or "").strip():
        errors.append("Body is required.")
    if len((body or "").strip()) > 10000:
        errors.append("Body cannot exceed 10000 characters.")
    if expires_at and expires_at <= _now_utc():
        errors.append("Expiry date must be in the future.")

    normalized_options = normalize_poll_options(option_texts or [])
    if clean_type == "POLL":
        if len(normalized_options) < 2:
            errors.append("Polls require at least two answer options.")
        if len(normalized_options) > 6:
            errors.append("Polls support up to six answer options.")
    elif normalized_options:
        errors.append("Poll options are only allowed for poll announcements.")

    return errors


def normalize_poll_options(option_texts: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for raw_option in option_texts:
        clean_option = " ".join((raw_option or "").split())
        if not clean_option:
            continue
        option_key = clean_option.casefold()
        if option_key in seen:
            continue
        normalized.append(clean_option)
        seen.add(option_key)
    return normalized


def replace_poll_options(announcement: Announcement, option_texts: list[str]) -> None:
    AnnouncementOption.query.filter_by(announcement_id=announcement.id).delete(
        synchronize_session=False
    )
    db.session.flush()
    for index, option_text in enumerate(normalize_poll_options(option_texts), start=1):
        db.session.add(
            AnnouncementOption(
                announcement_id=announcement.id,
                option_text=option_text,
                display_order=index,
            )
        )
    db.session.flush()


def publish_announcement(announcement: Announcement) -> int:
    announcement.status = "PUBLISHED"
    if announcement.published_at is None:
        announcement.published_at = _now_utc()
    announcement.closed_at = None
    db.session.flush()

    recipients_created = 0
    for user in get_target_announcement_users():
        recipient = AnnouncementRecipient.query.filter_by(
            announcement_id=announcement.id,
            user_id=user.id,
        ).first()
        if recipient is None:
            recipient = AnnouncementRecipient(
                announcement_id=announcement.id,
                user_id=user.id,
            )
            db.session.add(recipient)
            recipients_created += 1
        if user.id == announcement.created_by:
            continue
        create_notification(
            user_id=user.id,
            title=f"{'Poll' if announcement.announcement_type == 'POLL' else 'Announcement'}: {announcement.title}",
            message=announcement.summary or announcement.body[:240],
            severity=announcement.severity,
            link=url_for("notifications.announcement_detail", announcement_id=announcement.id),
        )
    db.session.flush()
    return recipients_created


def close_announcement(announcement: Announcement) -> None:
    announcement.status = "CLOSED"
    announcement.closed_at = _now_utc()
    db.session.flush()


def get_announcement_for_user(announcement_id: int, user_id: int, include_superuser: bool = False):
    user = db.session.get(User, int(user_id)) if user_id else None
    announcement = Announcement.query.filter_by(id=announcement_id).first()
    if announcement is None:
        return None, None

    recipient = AnnouncementRecipient.query.filter_by(
        announcement_id=announcement.id,
        user_id=user_id,
    ).first()

    if recipient is None and announcement.status == "PUBLISHED":
        recipient = AnnouncementRecipient(
            announcement_id=announcement.id,
            user_id=user_id,
            is_read=False,
        )
        db.session.add(recipient)
        db.session.flush()

    user_is_super = bool(user and canonicalize_role_name(user.role.name if user.role else None) == SUPERUSER_ROLE)
    if recipient is None and not (include_superuser and user_is_super):
        return None, None
    return announcement, recipient


def get_announcement_recipients_for_user(user_id: int) -> list[AnnouncementRecipient]:
    return (
        AnnouncementRecipient.query
        .filter_by(user_id=user_id)
        .join(Announcement, Announcement.id == AnnouncementRecipient.announcement_id)
        .order_by(
            Announcement.published_at.desc(),
            Announcement.created_at.desc(),
        )
        .all()
    )


def mark_announcement_read(recipient: AnnouncementRecipient | None) -> AnnouncementRecipient | None:
    if recipient is None or recipient.is_read:
        return recipient
    recipient.is_read = True
    recipient.read_at = _now_utc()
    recipient.updated_at = _now_utc()
    db.session.flush()
    return recipient


def get_latest_login_announcement_for_user(user_id: int) -> SimpleNamespace | None:
    """Return the latest unread published broadcast for a user.

    Returns a SimpleNamespace (not an ORM object) so that zero ORM
    relationship loading happens — no eager joins, no lazy triggers, and no
    risk of the f405 duplicate-alias error that occurs when lazy="joined"
    relationships auto-generate a second LEFT OUTER JOIN to a table that is
    already INNER JOINed for filtering.  The namespace exposes exactly the
    fields the popup template and polling endpoint need.
    """
    now = _now_utc()
    row = (
        db.session.query(
            Announcement.id.label("announcement_id"),
            Announcement.title,
            Announcement.summary,
            Announcement.body,
            Announcement.announcement_type,
            Announcement.published_at,
            Announcement.created_at,
        )
        .outerjoin(
            AnnouncementRecipient,
            (Announcement.id == AnnouncementRecipient.announcement_id) &
            (AnnouncementRecipient.user_id == user_id)
        )
        .filter(
            Announcement.status == "PUBLISHED",
            Announcement.created_by != user_id,
            or_(Announcement.expires_at.is_(None), Announcement.expires_at >= now),
            Announcement.closed_at.is_(None),
            or_(AnnouncementRecipient.id.is_(None), AnnouncementRecipient.is_read.is_(False))
        )
        .order_by(
            Announcement.published_at.desc(),
            Announcement.created_at.desc(),
        )
        .first()
    )
    if row is None:
        return None
    ann = SimpleNamespace(
        id=row.announcement_id,
        title=row.title,
        summary=row.summary,
        body=row.body,
        announcement_type=row.announcement_type,
        published_at=row.published_at,
        created_at=row.created_at,
    )
    return SimpleNamespace(
        announcement_id=row.announcement_id,
        announcement=ann,
    )


def get_user_vote_for_announcement(announcement_id: int, user_id: int) -> AnnouncementVote | None:
    return AnnouncementVote.query.filter_by(
        announcement_id=announcement_id,
        user_id=user_id,
    ).first()


def can_vote_on_announcement(announcement: Announcement) -> bool:
    return announcement.announcement_type == "POLL" and _is_active_for_delivery(announcement)


def cast_vote(announcement: Announcement, option_id: int, user_id: int) -> AnnouncementVote:
    existing_vote = get_user_vote_for_announcement(announcement.id, user_id)
    if existing_vote is not None:
        return existing_vote

    option = AnnouncementOption.query.filter_by(
        id=option_id,
        announcement_id=announcement.id,
    ).first()
    if option is None:
        raise ValueError("Selected poll option was not found.")

    vote = AnnouncementVote(
        announcement_id=announcement.id,
        option_id=option.id,
        user_id=user_id,
    )
    db.session.add(vote)
    db.session.flush()
    return vote


def get_poll_result_rows(announcement: Announcement) -> list[dict[str, object]]:
    counts = Counter(
        vote.option_id
        for vote in AnnouncementVote.query.filter_by(announcement_id=announcement.id).all()
    )
    total_votes = sum(counts.values())
    rows = []
    for option in announcement.options:
        option_votes = counts.get(option.id, 0)
        rows.append(
            {
                "option": option,
                "votes": option_votes,
                "percent": round((option_votes / total_votes * 100), 1) if total_votes else 0,
            }
        )
    return rows
