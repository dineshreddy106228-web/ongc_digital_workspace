"""Main routes – dashboard and root redirect."""

from datetime import datetime, timezone

from flask import render_template, redirect, url_for
from flask_login import login_required, current_user
from app.features import get_dashboard_module_cards
from app.main import main_bp
from app.models.activity_log import ActivityLog
from app.services.dashboard import (
    get_dashboard_summary_metrics,
    get_superuser_dashboard_analytics,
)
from app.services.notifications import (
    get_recent_notifications,
    get_unread_notifications,
)


@main_bp.route("/")
def index():
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))
    return redirect(url_for("auth.login"))


@main_bp.route("/dashboard")
@login_required
def dashboard():
    module_cards = get_dashboard_module_cards(current_user)
    task_module_active = current_user.has_module_access("tasks")
    metrics = get_dashboard_summary_metrics("global") if task_module_active else None
    modules_enabled_count = 1 if task_module_active else 0
    last_refreshed = datetime.now(timezone.utc)
    system_status = [
        {"label": "Database", "value": "Connected", "tone": "success"},
        {"label": "App Mode", "value": "Pilot", "tone": "neutral"},
        {
            "label": "Modules Enabled",
            "value": str(modules_enabled_count),
            "tone": "info",
        },
        {"label": "Status", "value": "Operational", "tone": "success"},
    ]

    has_clickable_modules = any(card["clickable"] for card in module_cards)

    # Activity feed – super users see the latest 10 platform-wide entries.
    recent_activity = []
    is_super = current_user.is_super_user()
    dashboard_analytics = None
    if is_super:
        recent_activity = (
            ActivityLog.query
            .order_by(ActivityLog.created_at.desc())
            .limit(10)
            .all()
        )
        if task_module_active:
            dashboard_analytics = get_superuser_dashboard_analytics()

    recent_notifications = get_unread_notifications(current_user.id, limit=5)
    alerts_panel_title = "Unread Alerts"
    if not recent_notifications:
        recent_notifications = get_recent_notifications(current_user.id, limit=5)
        alerts_panel_title = "Recent Alerts"

    return render_template(
        "main/dashboard.html",
        user=current_user,
        metrics=metrics,
        module_cards=module_cards,
        task_module_active=task_module_active,
        has_clickable_modules=has_clickable_modules,
        recent_activity=recent_activity,
        is_super=is_super,
        recent_notifications=recent_notifications,
        alerts_panel_title=alerts_panel_title,
        system_status=system_status,
        modules_enabled_count=modules_enabled_count,
        last_refreshed=last_refreshed,
        dashboard_analytics=dashboard_analytics,
    )
