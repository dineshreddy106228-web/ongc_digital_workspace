from __future__ import annotations

"""Cached dashboard service helpers."""

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import url_for
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import selectinload

from app.core.module_registry import (
    get_module_definition,
    is_module_enabled,
    is_module_registered,
    user_can_access_module,
)
from app.extensions import cache
from app.models.core.user import User
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator


CLOSED_TASK_STATUSES = ("Completed", "Cancelled")
PENDING_UPDATE_STATUSES = ("Not Started", "On Hold")
SHOWCASE_MODULE_KEYS = ("office_management", "csc_workflow", "inventory")
MODULE_ICON_MAP = {
    "office_management": "bi-list-task",
    "csc_workflow": "bi-diagram-3",
    "inventory": "bi-box-seam",
}
INDIA_TIMEZONE = ZoneInfo("Asia/Kolkata")


@cache.memoize(timeout=300)
def get_dashboard_summary_metrics(scope: str = "global"):
    """Return dashboard task metrics for the requested scope."""
    if scope != "global":
        return None

    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    month_start = datetime(today.year, today.month, 1)
    if today.month == 12:
        next_month_start = datetime(today.year + 1, 1, 1)
    else:
        next_month_start = datetime(today.year, today.month + 1, 1)

    return {
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


def invalidate_dashboard_summary_metrics(scope: str = "global") -> None:
    """Clear cached dashboard metrics after task writes."""
    cache.delete_memoized(get_dashboard_summary_metrics, scope)
    cache.delete_memoized(get_superuser_dashboard_analytics)


@cache.memoize(timeout=300)
def get_superuser_dashboard_analytics():
    """Return executive analytics for the main dashboard."""
    today = date.today()
    all_active_tasks = (
        Task.query
        .filter(Task.is_active.is_(True))
        .order_by(Task.created_at.desc())
        .all()
    )
    open_tasks = [task for task in all_active_tasks if task.status not in CLOSED_TASK_STATUSES]
    completed_tasks = [task for task in all_active_tasks if task.status == "Completed"]

    overdue_tasks = [
        task for task in open_tasks
        if task.due_date and task.due_date < today
    ]
    due_next_7_days = [
        task for task in open_tasks
        if task.due_date and today <= task.due_date <= today + timedelta(days=7)
    ]
    critical_open = [task for task in open_tasks if task.priority == "Critical"]
    unassigned_open = [task for task in open_tasks if task.owner is None]
    completion_pct = round(
        (len(completed_tasks) / len(all_active_tasks) * 100) if all_active_tasks else 0
    )

    owner_counts = Counter(
        (task.owner.full_name or task.owner.username) if task.owner else "Unassigned"
        for task in open_tasks
    )
    top_owners = sorted(
        owner_counts.items(),
        key=lambda item: (-item[1], item[0].lower()),
    )[:7]

    due_horizon = {
        "Overdue": len(overdue_tasks),
        "Next 7 Days": len(due_next_7_days),
        "Next 30 Days": sum(
            1 for task in open_tasks
            if task.due_date and today + timedelta(days=8) <= task.due_date <= today + timedelta(days=30)
        ),
        "No Due Date": sum(1 for task in open_tasks if task.due_date is None),
        "Stable": sum(
            1 for task in open_tasks
            if task.due_date and task.due_date > today + timedelta(days=30)
        ),
    }
    aging = {
        "0-7 Days": sum(1 for task in open_tasks if (today - task.created_at.date()).days <= 7),
        "8-30 Days": sum(1 for task in open_tasks if 8 <= (today - task.created_at.date()).days <= 30),
        "31-60 Days": sum(1 for task in open_tasks if 31 <= (today - task.created_at.date()).days <= 60),
        "60+ Days": sum(1 for task in open_tasks if (today - task.created_at.date()).days > 60),
    }

    return {
        "signals": {
            "open_tasks": len(open_tasks),
            "completed_tasks": len(completed_tasks),
            "completion_pct": completion_pct,
            "overdue_tasks": len(overdue_tasks),
            "critical_open": len(critical_open),
            "unassigned_open": len(unassigned_open),
        },
        "status": dict(Counter(task.status for task in all_active_tasks)),
        "priority": dict(Counter(task.priority for task in open_tasks)),
        "owner_workload": {
            "labels": [label for label, _ in top_owners],
            "values": [value for _, value in top_owners],
        },
        "due_horizon": due_horizon,
        "aging": aging,
    }


def _task_visibility_query_for_user(user):
    """Mirror Office Management visibility rules for dashboard summaries."""
    base = Task.query

    if user.is_super_user():
        return base

    conds = [
        Task.task_scope == "GLOBAL",
        Task.owner_id == user.id,
    ]

    collaborator_task_ids_subq = (
        TaskCollaborator.query
        .with_entities(TaskCollaborator.task_id)
        .filter_by(user_id=user.id)
        .subquery()
    )
    conds.append(Task.id.in_(select(collaborator_task_ids_subq.c.task_id)))

    controlled_ids_subq = (
        User.query
        .with_entities(User.id)
        .filter_by(controlling_officer_id=user.id, is_active=True)
        .subquery()
    )
    controlled_collaborator_task_ids_subq = (
        TaskCollaborator.query
        .with_entities(TaskCollaborator.task_id)
        .filter(TaskCollaborator.user_id.in_(select(controlled_ids_subq.c.id)))
        .subquery()
    )
    conds.append(
        and_(
            Task.task_scope.in_(["MY", "TEAM"]),
            or_(
                Task.owner_id.in_(select(controlled_ids_subq.c.id)),
                Task.id.in_(select(controlled_collaborator_task_ids_subq.c.task_id)),
            ),
        )
    )

    return base.filter(or_(*conds))


def _display_first_name(user) -> str:
    full_name = (getattr(user, "full_name", "") or "").strip()
    if full_name:
        return full_name.split()[0]

    username = (getattr(user, "username", "") or "").strip()
    if not username:
        return "User"

    return username.split(".")[0].replace("_", " ").title()


def _time_of_day_greeting(now_local: datetime) -> str:
    if now_local.hour < 12:
        return "Good Morning"
    if now_local.hour < 17:
        return "Good Afternoon"
    return "Good Evening"


def _task_updated_date(task: Task) -> date | None:
    if not getattr(task, "updated_at", None):
        return None
    return task.updated_at.date()


def _latest_update_date(task: Task) -> date | None:
    update_dates = [
        update.created_at.date()
        for update in getattr(task, "updates", [])
        if getattr(update, "created_at", None)
    ]
    return max(update_dates) if update_dates else None


def _build_welcome_summary(open_tasks, due_today_count: int, pending_update_count: int) -> str:
    active_count = len(open_tasks)
    if active_count == 0:
        return "No active tasks require attention right now."

    return (
        f"You have {active_count} active tasks"
        f" • {due_today_count} due today"
        f" • {pending_update_count} pending update"
    )


def _build_module_showcase(user, app=None) -> list[dict]:
    cards = []

    for key in SHOWCASE_MODULE_KEYS:
        definition = get_module_definition(key)
        if definition is None:
            continue

        if key == "inventory":
            cards.append(
                {
                    "key": key,
                    "label": definition.name,
                    "description": definition.description,
                    "icon_class": MODULE_ICON_MAP.get(key, "bi-grid"),
                    "badge": "Under Development",
                    "eyebrow": "Coming Soon",
                    "clickable": False,
                    "href": None,
                    "state": "coming-soon",
                    "is_available": False,
                }
            )
            continue

        feature_ready = is_module_enabled(key, app) and is_module_registered(key, app)
        clickable = user_can_access_module(key, user, app)
        cards.append(
            {
                "key": key,
                "label": definition.name,
                "description": definition.description,
                "icon_class": MODULE_ICON_MAP.get(key, "bi-grid"),
                "badge": "Active" if clickable else ("Access Required" if feature_ready else "Unavailable"),
                "eyebrow": "Live Module" if feature_ready else "Configuration Required",
                "clickable": clickable,
                "href": url_for(definition.endpoint) if clickable and definition.endpoint else None,
                "state": "active" if feature_ready else "restricted",
                "is_available": feature_ready,
            }
        )

    return cards


def get_dashboard_workspace_context(user, app=None) -> dict:
    """Build the main workspace dashboard payload for the logged-in user."""
    now_local = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    today = now_local.date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    recent_completion_start = today - timedelta(days=2)
    stale_update_threshold = today - timedelta(days=3)
    task_module_active = user_can_access_module("office_management", user, app)

    open_tasks = []
    due_today = []
    overdue = []
    pending_updates = []
    recently_completed = []
    due_this_week = []
    completed_this_week = []
    pending_across_team = 0
    module_cards = _build_module_showcase(user, app)

    if task_module_active:
        visible_tasks = (
            _task_visibility_query_for_user(user)
            .options(selectinload(Task.updates))
            .order_by(Task.created_at.desc())
            .all()
        )

        open_tasks = [
            task for task in visible_tasks
            if task.is_active and task.status not in CLOSED_TASK_STATUSES
        ]
        due_today = [task for task in open_tasks if task.due_date == today]
        overdue = [task for task in open_tasks if task.due_date and task.due_date < today]
        due_this_week = [
            task for task in open_tasks
            if task.due_date and week_start <= task.due_date <= week_end
        ]
        pending_updates = [
            task for task in open_tasks
            if (
                task.status in PENDING_UPDATE_STATUSES
                or (
                    task.status == "In Progress"
                    and (
                        (_latest_update_date(task) is None)
                        or (_latest_update_date(task) <= stale_update_threshold)
                    )
                )
            )
        ]
        recently_completed = [
            task for task in visible_tasks
            if task.status == "Completed"
            and (_task_updated_date(task) is not None)
            and (_task_updated_date(task) >= recent_completion_start)
        ]
        completed_this_week = [
            task for task in visible_tasks
            if task.status == "Completed"
            and (_task_updated_date(task) is not None)
            and (week_start <= _task_updated_date(task) <= week_end)
        ]
        pending_across_team = sum(
            1 for task in open_tasks
            if task.status in PENDING_UPDATE_STATUSES
        )

    task_list_href = url_for("tasks.list_tasks") if task_module_active else None

    return {
        "identity": {
            "title": "Corporate Chemistry Digital Workspace",
            "subtitle": "Centralized coordination. Real-time visibility. Structured execution.",
        },
        "welcome": {
            "greeting": _time_of_day_greeting(now_local),
            "name": _display_first_name(user),
            "summary": _build_welcome_summary(open_tasks, len(due_today), len(pending_updates)),
        },
        "task_module_active": task_module_active,
        "immediate_actions": [
            {
                "label": "Tasks Due Today",
                "value": len(due_today),
                "detail": "Scheduled for action before close of day.",
                "tone": "primary",
                "icon": "bi-calendar2-day",
                "href": task_list_href,
            },
            {
                "label": "Overdue Tasks",
                "value": len(overdue),
                "detail": "Requires immediate follow-up.",
                "tone": "danger",
                "icon": "bi-exclamation-diamond",
                "href": task_list_href,
            },
            {
                "label": "Pending Update",
                "value": len(pending_updates),
                "detail": "Awaiting progress confirmation or fresh remarks.",
                "tone": "warning",
                "icon": "bi-arrow-repeat",
                "href": task_list_href,
            },
            {
                "label": "Recently Completed",
                "value": len(recently_completed),
                "detail": "Closed within the last 72 hours.",
                "tone": "success",
                "icon": "bi-check2-circle",
                "href": task_list_href,
            },
        ],
        "visibility_metrics": [
            {
                "label": "Total Active Tasks",
                "value": len(open_tasks),
                "detail": "Open workload in your visible operating scope.",
            },
            {
                "label": "Tasks Completed This Week",
                "value": len(completed_this_week),
                "detail": "Closed since the start of the current week.",
            },
            {
                "label": "Pending Across Team",
                "value": pending_across_team,
                "detail": "Not started or on hold across visible tasks.",
            },
            {
                "label": "Tasks Due This Week",
                "value": len(due_this_week),
                "detail": "Upcoming commitments through week-end.",
            },
        ],
        "modules": module_cards,
        "guidance_message": "All updates are reflected in real time. Keep your tasks current.",
        "modules_enabled_count": sum(1 for card in module_cards if card["is_available"]),
    }
