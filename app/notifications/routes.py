from __future__ import annotations

"""Per-user in-app notification routes."""

from flask import flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.extensions import db
from app.notifications import notifications_bp
from app.services.notifications import (
    get_notification,
    get_recent_notifications,
    get_unread_notification_count,
    mark_all_notifications_read,
    mark_notification_read,
)


def _safe_redirect_target(target: str | None):
    if target and target.startswith("/"):
        return target
    return url_for("notifications.index")


@notifications_bp.route("/")
@login_required
def index():
    notifications = get_recent_notifications(current_user.id, limit=100)
    unread_count = get_unread_notification_count(current_user.id)
    return render_template(
        "notifications/index.html",
        notifications=notifications,
        unread_count=unread_count,
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
