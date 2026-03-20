"""Main routes – dashboard and root redirect."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import render_template, redirect, url_for
from flask_login import login_required, current_user
from app.modules.dashboard import dashboard_bp
from app.models.core.activity_log import ActivityLog
from app.core.services.dashboard import (
    get_dashboard_workspace_context,
    get_superuser_dashboard_analytics,
)
from app.core.services.notifications import (
    get_unread_notifications,
)

INDIA_TIMEZONE = ZoneInfo("Asia/Kolkata")


@dashboard_bp.route("/")
def index():
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))
    return redirect(url_for("auth.login"))


@dashboard_bp.route("/dashboard")
@login_required
def dashboard():
    workspace_context = get_dashboard_workspace_context(current_user)
    task_module_active = workspace_context["task_module_active"]
    modules_enabled_count = workspace_context["modules_enabled_count"]
    last_refreshed = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    is_admin = current_user.is_admin_user()
    system_status = []
    if is_admin:
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

    # Activity feed – admins see the latest 10 platform-wide entries.
    recent_activity = []
    is_super = current_user.is_super_user()
    dashboard_analytics = None
    if is_admin:
        recent_activity = (
            ActivityLog.query
            .order_by(ActivityLog.created_at.desc())
            .limit(10)
            .all()
        )
    if is_super:
        if task_module_active:
            dashboard_analytics = get_superuser_dashboard_analytics()

    recent_notifications = get_unread_notifications(current_user.id, limit=5)
    alerts_panel_title = "Unread Alerts"

    return render_template(
        "main/dashboard.html",
        user=current_user,
        workspace_context=workspace_context,
        task_module_active=task_module_active,
        recent_activity=recent_activity,
        is_super=is_super,
        recent_notifications=recent_notifications,
        alerts_panel_title=alerts_panel_title,
        system_status=system_status,
        is_admin=is_admin,
        modules_enabled_count=modules_enabled_count,
        last_refreshed=last_refreshed,
        dashboard_analytics=dashboard_analytics,
    )
