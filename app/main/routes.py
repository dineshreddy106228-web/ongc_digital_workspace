"""Main routes – dashboard and root redirect."""

from datetime import date, datetime, timedelta
from flask import render_template, redirect, url_for
from flask_login import login_required, current_user
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
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    month_start = datetime(today.year, today.month, 1)
    if today.month == 12:
        next_month_start = datetime(today.year + 1, 1, 1)
    else:
        next_month_start = datetime(today.year, today.month + 1, 1)

    open_tasks = Task.query.filter(
        Task.is_active.is_(True),
        Task.status.notin_(CLOSED_TASK_STATUSES),
    ).count()

    overdue_tasks = Task.query.filter(
        Task.is_active.is_(True),
        Task.due_date.isnot(None),
        Task.due_date < today,
        Task.status.notin_(CLOSED_TASK_STATUSES),
    ).count()

    due_this_week = Task.query.filter(
        Task.is_active.is_(True),
        Task.due_date.isnot(None),
        Task.due_date >= week_start,
        Task.due_date <= week_end,
        Task.status.notin_(CLOSED_TASK_STATUSES),
    ).count()

    completed_this_month = Task.query.filter(
        Task.status == "Completed",
        Task.updated_at >= month_start,
        Task.updated_at < next_month_start,
    ).count()

    metrics = {
        "open_tasks": open_tasks,
        "overdue_tasks": overdue_tasks,
        "due_this_week": due_this_week,
        "completed_this_month": completed_this_month,
    }
    return render_template("main/dashboard.html", user=current_user, metrics=metrics)
