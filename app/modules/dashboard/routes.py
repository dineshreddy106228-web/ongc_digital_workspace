"""Main routes – adaptive home page, analytics, and root redirect."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import jsonify, render_template, redirect, request, url_for, flash
from flask_login import login_required, current_user
from app.core.utils.decorators import superuser_required
from sqlalchemy.orm import joinedload
from app.modules.dashboard import dashboard_bp
from app.models.core.activity_log import ActivityLog
from app.models.core.user import User
from app.core.services.dashboard import (
    get_dashboard_briefing,
    get_superuser_dashboard_briefing,
)
from app.core.services.home import (
    SCOPE_OFFICE,
    SCOPE_WORKSPACE,
    get_home_context,
    resolve_scope,
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
    """The single adaptive home page. Scope is chosen with ``?scope=``."""
    home = get_home_context(current_user, request.args.get("scope"))

    is_admin = current_user.is_admin_user()
    recent_activity = []
    system_status = []
    if is_admin:
        recent_activity = (
            ActivityLog.query
            .order_by(ActivityLog.created_at.desc())
            .limit(8)
            .all()
        )
        system_status = [
            {"label": "Database", "value": "Connected", "tone": "success"},
            {"label": "App Mode", "value": "Pilot", "tone": "neutral"},
            {
                "label": "Modules Enabled",
                "value": str(home["modules_enabled_count"]),
                "tone": "info",
            },
            {"label": "Status", "value": "Operational", "tone": "success"},
        ]

    return render_template(
        "main/home.html",
        user=current_user,
        home=home,
        is_admin=is_admin,
        recent_activity=recent_activity,
        system_status=system_status,
    )


# ── Legacy dashboard URLs ─────────────────────────────────────────
# The superuser and power-user dashboards are now scopes of the home page.


@dashboard_bp.route("/dashboard/superuser")
@login_required
def superuser_dashboard():
    return redirect(url_for("main.dashboard", scope=SCOPE_WORKSPACE))


@dashboard_bp.route("/dashboard/power-user")
@dashboard_bp.route("/dashboard/power-user/")
@login_required
def power_user_dashboard():
    return redirect(url_for("main.dashboard", scope=SCOPE_OFFICE))


@dashboard_bp.route("/dashboard/control-center")
@login_required
def control_center():
    return redirect(url_for("main.dashboard"))


@dashboard_bp.route("/dashboard/analytics")
@login_required
def analytics():
    """Portfolio charts, task drilldowns, and the reporting organogram."""
    if not (current_user.is_super_user() or current_user.is_office_power_user()):
        flash("Portfolio Analytics is not available for this account.", "danger")
        return redirect(url_for("main.dashboard"))

    from app.modules.admin.routes import _build_user_organogram

    scope = resolve_scope(current_user, request.args.get("scope"))
    if scope not in (SCOPE_OFFICE, SCOPE_WORKSPACE):
        scope = SCOPE_WORKSPACE if current_user.is_super_user() else SCOPE_OFFICE

    office_id = getattr(current_user, "office_id", None)
    if scope == SCOPE_OFFICE and office_id is None:
        scope = SCOPE_WORKSPACE

    briefing = get_dashboard_briefing(
        scope, office_id if scope == SCOPE_OFFICE else None
    )

    reporting_organogram = []
    default_organogram_office_key = None
    if current_user.is_super_user():
        reporting_organogram, default_organogram_office_key = _reporting_organograms(
            _build_user_organogram
        )

    scope_label = "Workspace" if scope == SCOPE_WORKSPACE else (
        getattr(getattr(current_user, "office", None), "office_name", None) or "My Office"
    )
    scope_options = []
    if current_user.is_super_user() and office_id is not None:
        scope_options = [
            {
                "key": SCOPE_WORKSPACE,
                "label": "Workspace",
                "href": url_for("main.analytics", scope=SCOPE_WORKSPACE),
                "is_active": scope == SCOPE_WORKSPACE,
            },
            {
                "key": SCOPE_OFFICE,
                "label": getattr(current_user.office, "office_name", "My Office"),
                "href": url_for("main.analytics", scope=SCOPE_OFFICE),
                "is_active": scope == SCOPE_OFFICE,
            },
        ]

    last_refreshed = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    return render_template(
        "main/analytics.html",
        briefing=briefing,
        scope=scope,
        scope_label=scope_label,
        scope_options=scope_options,
        reporting_organogram=reporting_organogram,
        default_organogram_office_key=default_organogram_office_key,
        last_refreshed=last_refreshed,
        user=current_user,
    )


@dashboard_bp.route("/api/briefing-data")
@login_required
@superuser_required
def briefing_data_api():
    """JSON endpoint for lazy-loading the heavy briefing drilldown."""
    return jsonify(get_superuser_dashboard_briefing())


def _reporting_organograms(build_user_organogram):
    """Return the per-office reporting trees with the viewer's office first."""
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
    organogram = [
        {
            **office,
            "office_key": str(office["office_id"]) if office.get("office_id") is not None else "unassigned",
            "is_current_user_office": office.get("office_id") == getattr(current_user, "office_id", None),
        }
        for office in build_user_organogram(reporting_users)
        if office.get("nodes") or int(office.get("total_users") or 0) > 0
    ]

    preferred_office_key = (
        str(current_user.office_id)
        if getattr(current_user, "office_id", None) is not None
        else None
    )
    organogram.sort(
        key=lambda office: (
            office["office_key"] != preferred_office_key,
            (office.get("office_name") or "").lower(),
            office.get("office_id") or 0,
        )
    )
    default_key = (
        preferred_office_key
        if preferred_office_key
        and any(office["office_key"] == preferred_office_key for office in organogram)
        else (organogram[0]["office_key"] if organogram else None)
    )
    return organogram, default_key
