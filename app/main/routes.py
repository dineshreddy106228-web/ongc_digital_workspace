"""Main routes – dashboard and root redirect."""

from datetime import date, datetime, timedelta
from flask import render_template, redirect, url_for
from flask_login import login_required, current_user
from app.features import get_dashboard_module_cards
from app.main import main_bp
from app.models.task import Task


CLOSED_TASK_STATUSES = ["Completed", "Cancelled"]


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
    metrics = None

    if task_module_active:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        month_start = datetime(today.year, today.month, 1)
        if today.month == 12:
            next_month_start = datetime(today.year + 1, 1, 1)
        else:
            next_month_start = datetime(today.year, today.month + 1, 1)

        metrics = {
            "open_tasks": Task.query.filter(
                Task.is_active.is_(True),
                Task.status.notin_(CLOSED_TASK_STATUSES),
            ).count(),
            "overdue_tasks": Task.query.filter(
                Task.is_active.is_(True),
                Task.due_date.isnot(None),
                Task.due_date < today,
                Task.status.notin_(CLOSED_TASK_STATUSES),
            ).count(),
            "due_this_week": Task.query.filter(
                Task.is_active.is_(True),
                Task.due_date.isnot(None),
                Task.due_date >= week_start,
                Task.due_date <= week_end,
                Task.status.notin_(CLOSED_TASK_STATUSES),
            ).count(),
            "completed_this_month": Task.query.filter(
                Task.status == "Completed",
                Task.updated_at >= month_start,
                Task.updated_at < next_month_start,
            ).count(),
        }

    has_clickable_modules = any(card["clickable"] for card in module_cards) or current_user.has_module_access(
        "admin_users"
    )
    return render_template(
        "main/dashboard.html",
        user=current_user,
        metrics=metrics,
        module_cards=module_cards,
        task_module_active=task_module_active,
        has_clickable_modules=has_clickable_modules,
    )
