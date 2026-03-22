"""Main routes – dashboard and root redirect."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import render_template, redirect, url_for, abort, flash
from flask_login import login_required, current_user
from sqlalchemy.orm import joinedload
from app.modules.dashboard import dashboard_bp
from app.models.core.activity_log import ActivityLog
from app.models.core.user import User
from app.core.services.dashboard import (
    get_dashboard_workspace_context,
    get_superuser_dashboard_briefing,
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
    return _render_dashboard_page(workspace_context)


@dashboard_bp.route("/dashboard/control-center")
@login_required
def control_center():
    if current_user.is_super_user():
        return redirect(url_for("main.superuser_dashboard"))
    if getattr(current_user, "is_power_user", False) and getattr(current_user, "office_id", None):
        return redirect(url_for("main.power_user_dashboard"))
    flash("Power User Dashboard is not available for this account.", "danger")
    return redirect(url_for("main.dashboard"))


@dashboard_bp.route("/dashboard/power-user")
@dashboard_bp.route("/dashboard/power-user/")
@login_required
def power_user_dashboard():
    if not (getattr(current_user, "is_active", False) and getattr(current_user, "is_power_user", False)):
        flash("Power User Dashboard is not available for this account.", "danger")
        return redirect(url_for("main.dashboard"))
    if getattr(current_user, "office_id", None) is None:
        flash("Power User Dashboard requires an office mapping.", "danger")
        return redirect(url_for("main.dashboard"))

    workspace_context = get_dashboard_workspace_context(current_user, mode="power_user")
    last_refreshed = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    return render_template(
        "main/power_user_dashboard.html",
        user=current_user,
        workspace_context=workspace_context,
        last_refreshed=last_refreshed,
    )


@dashboard_bp.route("/dashboard/superuser")
@login_required
def superuser_dashboard():
    if not current_user.is_super_user():
        abort(403)

    from app.modules.admin.routes import _build_user_organogram

    briefing = get_superuser_dashboard_briefing()
    reporting_users = (
        User.query
        .options(
            joinedload(User.role),
            joinedload(User.office),
            joinedload(User.controlling_officer),
            joinedload(User.reviewing_officer),
            joinedload(User.accepting_officer),
        )
        .order_by(User.created_at.desc())
        .all()
    )
    reporting_organogram = _build_user_organogram(reporting_users)
    reporting_organogram = [
        office
        for office in reporting_organogram
        if int(office.get("mapped_users") or 0) > 0
    ]
    last_refreshed = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    return render_template(
        "main/superuser_dashboard.html",
        briefing=briefing,
        reporting_organogram=reporting_organogram,
        last_refreshed=last_refreshed,
        user=current_user,
    )


def _render_dashboard_page(workspace_context):
    task_module_active = workspace_context["task_module_active"]
    modules_enabled_count = workspace_context["modules_enabled_count"]
    last_refreshed = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    is_admin = current_user.is_admin_user()
    control_center_mode = workspace_context.get("control_center_mode", False)
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
        control_center_mode=control_center_mode,
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
