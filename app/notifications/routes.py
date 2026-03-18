from __future__ import annotations

"""Per-user notification, announcement, and poll routes."""

from datetime import datetime
from zoneinfo import ZoneInfo

from flask import (
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required
from sqlalchemy.exc import SQLAlchemyError

from app.core.roles import SUPERUSER_ROLE, canonicalize_role_name
from app.core.services.announcements import (
    Announcement,
    AnnouncementRecipient,
    can_vote_on_announcement,
    cast_vote,
    close_announcement,
    get_announcement_for_user,
    get_latest_login_announcement_for_user,
    get_poll_result_rows,
    get_target_announcement_users,
    get_user_vote_for_announcement,
    mark_announcement_read,
    normalize_poll_options,
    publish_announcement,
    replace_poll_options,
    validate_announcement_payload,
)
from app.core.services.notifications import (
    get_notification,
    get_recent_notifications,
    get_unread_notification_count,
    mark_all_notifications_read,
    mark_notification_read,
)
from app.core.utils.activity import log_activity
from app.core.utils.decorators import superuser_required
from app.extensions import db
from app.models.core.audit_log import AuditLog
from app.core.utils.request_meta import get_client_ip, get_user_agent
from app.notifications import notifications_bp


INDIA_TIMEZONE = ZoneInfo("Asia/Kolkata")
ANNOUNCEMENT_SEVERITIES = ("info", "success", "warning", "danger")


def _safe_redirect_target(target: str | None):
    if target and target.startswith("/"):
        return target
    return url_for("notifications.index")


def _parse_expiry_utc(raw_value: str | None):
    clean_value = (raw_value or "").strip()
    if not clean_value:
        return None
    try:
        local_dt = datetime.strptime(clean_value, "%Y-%m-%dT%H:%M").replace(
            tzinfo=INDIA_TIMEZONE
        )
    except ValueError:
        return "invalid"
    return local_dt.astimezone(ZoneInfo("UTC"))


def _format_expiry_for_form(dt_value):
    if not dt_value:
        return ""
    aware_dt = dt_value
    if aware_dt.tzinfo is None:
        aware_dt = aware_dt.replace(tzinfo=ZoneInfo("UTC"))
    return aware_dt.astimezone(INDIA_TIMEZONE).strftime("%Y-%m-%dT%H:%M")


def _announcement_option_values(form_data, announcement: Announcement | None = None) -> list[str]:
    if hasattr(form_data, "getlist"):
        submitted = [value for value in form_data.getlist("poll_options")]
        if submitted:
            return submitted
    if announcement and announcement.options:
        return [option.option_text for option in announcement.options]
    return ["", ""]


def _superuser_audit(action: str, entity_id: str, details: str) -> None:
    db.session.add(
        AuditLog(
            user_id=current_user.id,
            action=action,
            entity_type="Announcement",
            entity_id=entity_id,
            details=details,
            ip_address=get_client_ip(),
            user_agent=get_user_agent(),
        )
    )


def _is_superuser() -> bool:
    return canonicalize_role_name(current_user.role.name if current_user.role else None) == SUPERUSER_ROLE


def _announcement_stats(announcement: Announcement) -> dict[str, int]:
    recipient_count = announcement.recipients.count()
    read_count = announcement.recipients.filter_by(is_read=True).count()
    vote_count = announcement.votes.count() if announcement.announcement_type == "POLL" else 0
    return {
        "recipients": recipient_count,
        "read_count": read_count,
        "unread_count": max(recipient_count - read_count, 0),
        "vote_count": vote_count,
    }


@notifications_bp.route("/")
@login_required
def index():
    notifications = get_recent_notifications(current_user.id, limit=100)
    unread_count = get_unread_notification_count(current_user.id)
    return render_template(
        "notifications/index.html",
        notifications=notifications,
        unread_count=unread_count,
        can_manage_announcements=_is_superuser(),
    )


@notifications_bp.route("/<int:notification_id>/open")
@login_required
def open_notification(notification_id):
    notification = get_notification(notification_id, user_id=current_user.id)
    if notification is None:
        flash("Notification not found.", "warning")
        return redirect(url_for("notifications.index"))

    mark_notification_read(notification.id, user_id=current_user.id)
    db.session.commit()

    if notification.link and notification.link.startswith("/"):
        return redirect(notification.link)
    return redirect(url_for("notifications.index"))


@notifications_bp.route("/<int:notification_id>/read", methods=["POST"])
@login_required
def mark_read(notification_id):
    notification = mark_notification_read(notification_id, user_id=current_user.id)
    if notification is None:
        flash("Notification not found.", "warning")
    else:
        db.session.commit()
        flash("Notification marked as read.", "success")

    return redirect(_safe_redirect_target(request.form.get("next", "").strip()))


@notifications_bp.route("/read-all", methods=["POST"])
@login_required
def read_all():
    updated = mark_all_notifications_read(current_user.id)
    if updated:
        db.session.commit()
        flash("All notifications marked as read.", "success")
    else:
        flash("No unread notifications found.", "info")
    return redirect(url_for("notifications.index"))



@notifications_bp.route("/announcements/<int:announcement_id>")
@login_required
def announcement_detail(announcement_id):
    announcement, recipient = get_announcement_for_user(
        announcement_id,
        current_user.id,
        include_superuser=True,
    )
    if announcement is None:
        flash("Announcement not found.", "warning")
        return redirect(url_for("notifications.index"))

    session.pop("dismissed_broadcast", None)
    recipient_was_unread = bool(recipient and not recipient.is_read)
    if recipient_was_unread:
        mark_announcement_read(recipient)

    user_vote = get_user_vote_for_announcement(announcement.id, current_user.id)
    can_vote = bool(recipient) and can_vote_on_announcement(announcement) and user_vote is None
    show_results = _is_superuser() or user_vote is not None or not can_vote_on_announcement(announcement)
    poll_results = get_poll_result_rows(announcement) if announcement.announcement_type == "POLL" else []
    if recipient_was_unread:
        db.session.commit()

    return render_template(
        "notifications/announcement_detail.html",
        announcement=announcement,
        recipient=recipient,
        user_vote=user_vote,
        can_vote=can_vote,
        show_results=show_results,
        poll_results=poll_results,
        announcement_stats=_announcement_stats(announcement),
        can_manage_announcements=_is_superuser(),
    )


@notifications_bp.route("/announcements/<int:announcement_id>/vote", methods=["POST"])
@login_required
def vote_announcement(announcement_id):
    announcement, recipient = get_announcement_for_user(
        announcement_id,
        current_user.id,
        include_superuser=False,
    )
    if announcement is None or recipient is None:
        flash("Announcement not found.", "warning")
        return redirect(url_for("notifications.index"))
    if not can_vote_on_announcement(announcement):
        flash("This poll is no longer accepting responses.", "warning")
        return redirect(url_for("notifications.announcement_detail", announcement_id=announcement.id))

    option_raw = request.form.get("option_id", "").strip()
    if not option_raw.isdigit():
        flash("Select a valid poll option.", "danger")
        return redirect(url_for("notifications.announcement_detail", announcement_id=announcement.id))

    try:
        vote = cast_vote(announcement, int(option_raw), current_user.id)
        mark_announcement_read(recipient)
        session.pop("dismissed_broadcast", None)
        log_activity(
            current_user.username,
            "announcement_vote_cast",
            "announcement",
            announcement.title,
            details=f"option_id={vote.option_id}",
        )
        db.session.commit()
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("notifications.announcement_detail", announcement_id=announcement.id))
    except SQLAlchemyError:
        db.session.rollback()
        flash("Could not record your vote due to a database error.", "danger")
        return redirect(url_for("notifications.announcement_detail", announcement_id=announcement.id))

    flash("Your vote has been recorded.", "success")
    return redirect(url_for("notifications.announcement_detail", announcement_id=announcement.id))


@notifications_bp.route("/login-announcement-dismiss", methods=["POST"])
@login_required
def dismiss_login_announcement():
    raw_id = request.form.get("announcement_id", "").strip()
    if raw_id.isdigit():
        session["dismissed_broadcast"] = int(raw_id)
    session.pop("show_login_announcement", None)  # backwards compat
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    return redirect(request.referrer or url_for("main.dashboard"))


@notifications_bp.route("/latest-broadcast")
@login_required
def latest_broadcast_api():
    """JSON endpoint polled by JS to surface new broadcasts in real time."""
    recipient = get_latest_login_announcement_for_user(current_user.id)
    if recipient is None:
        return jsonify({"broadcast": None})
    dismissed = session.get("dismissed_broadcast")
    if dismissed is not None and dismissed == recipient.announcement_id:
        return jsonify({"broadcast": None})
    ann = recipient.announcement
    return jsonify({
        "broadcast": {
            "id": ann.id,
            "title": ann.title,
            "summary": ann.summary or ann.body[:280],
            "type": ann.announcement_type,
            "url": url_for("notifications.announcement_detail", announcement_id=ann.id),
        }
    })


@notifications_bp.route("/announcements/manage")
@login_required
@superuser_required
def manage_announcements():
    announcements = Announcement.query.order_by(
        Announcement.created_at.desc(),
        Announcement.id.desc(),
    ).all()
    stats = {announcement.id: _announcement_stats(announcement) for announcement in announcements}
    return render_template(
        "notifications/manage_announcements.html",
        announcements=announcements,
        announcement_stats=stats,
        target_user_count=len(get_target_announcement_users()),
    )


@notifications_bp.route("/announcements/manage/create", methods=["GET", "POST"])
@login_required
@superuser_required
def create_announcement():
    if request.method == "POST":
        title = request.form.get("title", "").strip()
        summary = request.form.get("summary", "").strip()
        body = request.form.get("body", "").strip()
        announcement_type = request.form.get("announcement_type", "").strip().upper()
        severity = request.form.get("severity", "info").strip().lower()
        expires_at = _parse_expiry_utc(request.form.get("expires_at"))
        option_texts = _announcement_option_values(request.form)
        action = request.form.get("submit_action", "draft").strip().lower()

        errors = []
        if expires_at == "invalid":
            errors.append("Expiry date must use a valid date and time.")
            expires_at = None
        errors.extend(
            validate_announcement_payload(
                title=title,
                body=body,
                announcement_type=announcement_type,
                expires_at=expires_at,
                option_texts=option_texts,
            )
        )
        if severity not in ANNOUNCEMENT_SEVERITIES:
            errors.append("Select a valid announcement severity.")
        if action not in {"draft", "publish"}:
            errors.append("Select a valid announcement action.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "notifications/announcement_form.html",
                form_mode="create",
                announcement=None,
                option_values=option_texts,
                expires_at_value=request.form.get("expires_at", ""),
                form_data=request.form,
            )

        announcement = Announcement(
            title=title,
            summary=summary or None,
            body=body,
            announcement_type=announcement_type,
            severity=severity,
            status="DRAFT",
            created_by=current_user.id,
            expires_at=expires_at,
        )

        try:
            db.session.add(announcement)
            db.session.flush()
            replace_poll_options(announcement, option_texts)
            if action == "publish":
                publish_announcement(announcement)
                log_activity(
                    current_user.username,
                    "announcement_published",
                    "announcement",
                    announcement.title,
                    details=f"type={announcement.announcement_type}",
                )
                _superuser_audit(
                    "ANNOUNCEMENT_PUBLISHED",
                    str(announcement.id),
                    f"Published announcement '{announcement.title}'.",
                )
                success_message = "Announcement published successfully."
            else:
                log_activity(
                    current_user.username,
                    "announcement_draft_created",
                    "announcement",
                    announcement.title,
                    details=f"type={announcement.announcement_type}",
                )
                _superuser_audit(
                    "ANNOUNCEMENT_DRAFT_CREATED",
                    str(announcement.id),
                    f"Created announcement draft '{announcement.title}'.",
                )
                success_message = "Announcement draft created successfully."
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            flash("Could not save the announcement due to a database error.", "danger")
            return render_template(
                "notifications/announcement_form.html",
                form_mode="create",
                announcement=None,
                option_values=option_texts,
                expires_at_value=request.form.get("expires_at", ""),
                form_data=request.form,
            )

        flash(success_message, "success")
        return redirect(url_for("notifications.manage_announcements"))

    return render_template(
        "notifications/announcement_form.html",
        form_mode="create",
        announcement=None,
        option_values=["", ""],
        expires_at_value="",
        form_data={},
    )


@notifications_bp.route("/announcements/manage/<int:announcement_id>/edit", methods=["GET", "POST"])
@login_required
@superuser_required
def edit_announcement(announcement_id):
    announcement = Announcement.query.get_or_404(announcement_id)
    if announcement.status != "DRAFT":
        flash("Only draft announcements can be edited.", "warning")
        return redirect(url_for("notifications.manage_announcements"))

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        summary = request.form.get("summary", "").strip()
        body = request.form.get("body", "").strip()
        announcement_type = request.form.get("announcement_type", "").strip().upper()
        severity = request.form.get("severity", "info").strip().lower()
        expires_at = _parse_expiry_utc(request.form.get("expires_at"))
        option_texts = _announcement_option_values(request.form)

        errors = []
        if expires_at == "invalid":
            errors.append("Expiry date must use a valid date and time.")
            expires_at = None
        errors.extend(
            validate_announcement_payload(
                title=title,
                body=body,
                announcement_type=announcement_type,
                expires_at=expires_at,
                option_texts=option_texts,
            )
        )
        if severity not in ANNOUNCEMENT_SEVERITIES:
            errors.append("Select a valid announcement severity.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "notifications/announcement_form.html",
                form_mode="edit",
                announcement=announcement,
                option_values=option_texts,
                expires_at_value=request.form.get("expires_at", ""),
                form_data=request.form,
            )

        try:
            announcement.title = title
            announcement.summary = summary or None
            announcement.body = body
            announcement.announcement_type = announcement_type
            announcement.severity = severity
            announcement.expires_at = expires_at
            replace_poll_options(announcement, option_texts)
            log_activity(
                current_user.username,
                "announcement_draft_updated",
                "announcement",
                announcement.title,
            )
            _superuser_audit(
                "ANNOUNCEMENT_DRAFT_UPDATED",
                str(announcement.id),
                f"Updated draft announcement '{announcement.title}'.",
            )
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            flash("Could not update the announcement due to a database error.", "danger")
            return render_template(
                "notifications/announcement_form.html",
                form_mode="edit",
                announcement=announcement,
                option_values=option_texts,
                expires_at_value=request.form.get("expires_at", ""),
                form_data=request.form,
            )

        flash("Announcement draft updated successfully.", "success")
        return redirect(url_for("notifications.manage_announcements"))

    return render_template(
        "notifications/announcement_form.html",
        form_mode="edit",
        announcement=announcement,
        option_values=_announcement_option_values({}, announcement=announcement),
        expires_at_value=_format_expiry_for_form(announcement.expires_at),
        form_data={},
    )


@notifications_bp.route("/announcements/manage/<int:announcement_id>/publish", methods=["POST"])
@login_required
@superuser_required
def publish_announcement_route(announcement_id):
    announcement = Announcement.query.get_or_404(announcement_id)
    if announcement.status != "DRAFT":
        flash("Only draft announcements can be published.", "warning")
        return redirect(url_for("notifications.manage_announcements"))

    try:
        publish_announcement(announcement)
        log_activity(
            current_user.username,
            "announcement_published",
            "announcement",
            announcement.title,
            details=f"type={announcement.announcement_type}",
        )
        _superuser_audit(
            "ANNOUNCEMENT_PUBLISHED",
            str(announcement.id),
            f"Published announcement '{announcement.title}'.",
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        flash("Could not publish the announcement due to a database error.", "danger")
        return redirect(url_for("notifications.manage_announcements"))

    flash("Announcement published successfully.", "success")
    return redirect(url_for("notifications.manage_announcements"))


@notifications_bp.route("/announcements/manage/<int:announcement_id>/close", methods=["POST"])
@login_required
@superuser_required
def close_announcement_route(announcement_id):
    announcement = Announcement.query.get_or_404(announcement_id)
    if announcement.status != "PUBLISHED":
        flash("Only published announcements can be closed.", "warning")
        return redirect(url_for("notifications.manage_announcements"))

    try:
        close_announcement(announcement)
        log_activity(
            current_user.username,
            "announcement_closed",
            "announcement",
            announcement.title,
        )
        _superuser_audit(
            "ANNOUNCEMENT_CLOSED",
            str(announcement.id),
            f"Closed announcement '{announcement.title}'.",
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        flash("Could not close the announcement due to a database error.", "danger")
        return redirect(url_for("notifications.manage_announcements"))

    flash("Announcement closed successfully.", "success")
    return redirect(url_for("notifications.manage_announcements"))
