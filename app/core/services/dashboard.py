from __future__ import annotations

"""Cached dashboard service helpers."""

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from flask import url_for
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import joinedload, selectinload

from app.core.module_registry import (
    get_module_definition,
    is_module_enabled,
    is_module_registered,
    user_can_access_module,
)
from app.core.services.task_visibility import task_visibility_query
from app.extensions import cache
from app.models.core.user import User
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator
from app.models.tasks.task_office import TaskOffice


CLOSED_TASK_STATUSES = ("Completed", "Cancelled")
PENDING_UPDATE_STATUSES = ("Not Started", "On Hold")
SHOWCASE_MODULE_KEYS = (
    "office_management",
    "inventory",
    "quality_control",
    "csc_workflow",
)
ADMIN_SHOWCASE_MODULE_KEYS = ("admin_users", "admin_backups")
MODULE_ICON_MAP = {
    "office_management": "bi-kanban-fill",
    "csc_workflow": "bi-file-earmark-ruled",
    "inventory": "bi-boxes",
    "quality_control": "bi-flask",
    "admin_users": "bi-person-gear",
    "admin_backups": "bi-database",
    "forecasting": "bi-graph-up-arrow",
}
# Each module carries its own accent so the home grid reads as a set of
# distinct destinations rather than one undifferentiated block.
MODULE_ACCENT_MAP = {
    "office_management": "office",
    "csc_workflow": "csc",
    "inventory": "inventory",
    "quality_control": "qc",
    "admin_users": "admin",
    "admin_backups": "admin",
    "forecasting": "forecasting",
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
    cache.delete_memoized(get_dashboard_briefing)


@cache.memoize(timeout=300)
def get_superuser_dashboard_analytics():
    """Return executive analytics for the main dashboard."""
    today = date.today()
    all_active_tasks = (
        Task.query
        .options(joinedload(Task.owner), joinedload(Task.office))
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


def _serialize_dashboard_task(task: Task) -> dict:
    owner_name = "Unassigned"
    if getattr(task, "owner", None) is not None:
        owner_name = _display_user_name(task.owner)

    office_names = []
    if getattr(task, "office", None) is not None and getattr(task.office, "office_name", None):
        office_names.append(task.office.office_name)
    if getattr(task, "tagged_offices", None):
        office_names.extend(
            office.office_name
            for office in task.tagged_offices
            if getattr(office, "office_name", None)
        )
    office_names = list(dict.fromkeys(name for name in office_names if name))

    return {
        "id": task.id,
        "title": task.task_title or f"Task #{task.id}",
        "status": task.status or "Not Started",
        "priority": task.priority or "Medium",
        "owner": owner_name,
        "scope": (task.task_scope or "MY").upper(),
        "office": ", ".join(office_names) if office_names else "Unassigned Office",
        "due_date": task.due_date.strftime("%d %b %Y") if task.due_date else "No due date",
        "detail_url": url_for("tasks.task_detail", task_id=task.id),
        "summary_url": url_for("tasks.task_summary", task_id=task.id),
        "command_summary_url": url_for("tasks.task_command_summary", task_id=task.id),
    }


def _bucket_task_list(tasks: list[Task], limit: int = 40) -> dict:
    sorted_tasks = sorted(
        tasks,
        key=lambda item: (
            item.due_date is None,
            item.due_date or date.max,
            (item.priority or "").lower(),
            item.id,
        ),
    )
    serialized = [_serialize_dashboard_task(task) for task in sorted_tasks[:limit]]
    return {
        "count": len(sorted_tasks),
        "items": serialized,
        "has_more": len(sorted_tasks) > limit,
    }


@cache.memoize(timeout=300)
def get_dashboard_briefing(scope: str = "workspace", office_id: int | None = None):
    """Return presentation-grade drilldown analytics for the analytics page.

    ``scope`` is ``workspace`` for the full portfolio, or ``office`` to restrict
    the briefing to the tasks owned by or tagged into ``office_id``.
    """
    from app.core.services.home import _office_task_query

    today = date.today()
    base_query = (
        _office_task_query(office_id)
        if scope == "office" and office_id is not None
        else Task.query
    )
    all_active_tasks = (
        base_query
        .options(
            selectinload(Task.owner),
            selectinload(Task.office),
            selectinload(Task.tagged_offices),
        )
        .filter(Task.is_active.is_(True))
        .order_by(Task.created_at.desc())
        .all()
    )
    open_tasks = [task for task in all_active_tasks if task.status not in CLOSED_TASK_STATUSES]
    completed_tasks = [task for task in all_active_tasks if task.status == "Completed"]

    def _group(tasks: list[Task], key_builder):
        grouped: dict[str, list[Task]] = {}
        for task in tasks:
            grouped.setdefault(key_builder(task), []).append(task)
        return grouped

    overdue_tasks = [task for task in open_tasks if task.due_date and task.due_date < today]
    critical_tasks = [task for task in open_tasks if task.priority == "Critical"]
    due_next_7_tasks = [
        task for task in open_tasks
        if task.due_date and today <= task.due_date <= today + timedelta(days=7)
    ]
    stale_tasks = [
        task for task in open_tasks
        if not getattr(task, "updated_at", None) or task.updated_at.date() <= today - timedelta(days=7)
    ]
    unassigned_tasks = [task for task in open_tasks if task.owner is None]

    completion_pct = round(
        (len(completed_tasks) / len(all_active_tasks) * 100) if all_active_tasks else 0
    )

    office_groups = _group(
        open_tasks,
        lambda task: (
            task.office.office_name
            if getattr(task, "office", None) and getattr(task.office, "office_name", None)
            else (
                ", ".join(
                    office.office_name
                    for office in getattr(task, "tagged_offices", [])
                    if getattr(office, "office_name", None)
                )
                or "Workspace Wide"
            )
        ),
    )
    owner_groups = _group(
        open_tasks,
        lambda task: _display_user_name(task.owner) if getattr(task, "owner", None) else "Unassigned",
    )
    status_groups = _group(all_active_tasks, lambda task: task.status or "Not Started")
    priority_groups = _group(open_tasks, lambda task: task.priority or "Medium")

    due_horizon_groups = {
        "Overdue": overdue_tasks,
        "Next 7 Days": due_next_7_tasks,
        "Next 30 Days": [
            task for task in open_tasks
            if task.due_date and today + timedelta(days=8) <= task.due_date <= today + timedelta(days=30)
        ],
        "No Due Date": [task for task in open_tasks if task.due_date is None],
        "Stable": [
            task for task in open_tasks
            if task.due_date and task.due_date > today + timedelta(days=30)
        ],
    }
    aging_groups = {
        "0-7 Days": [task for task in open_tasks if (today - task.created_at.date()).days <= 7],
        "8-30 Days": [task for task in open_tasks if 8 <= (today - task.created_at.date()).days <= 30],
        "31-60 Days": [task for task in open_tasks if 31 <= (today - task.created_at.date()).days <= 60],
        "60+ Days": [task for task in open_tasks if (today - task.created_at.date()).days > 60],
    }

    def _top_group_rows(grouped: dict[str, list[Task]], limit: int = 8) -> tuple[list[str], list[int], dict]:
        ordered = sorted(
            grouped.items(),
            key=lambda item: (-len(item[1]), item[0].lower()),
        )
        top_rows = ordered[:limit]
        overflow = ordered[limit:]
        labels = [label for label, _ in top_rows]
        values = [len(tasks) for _, tasks in top_rows]
        drilldown = {label: _bucket_task_list(tasks) for label, tasks in top_rows}
        if overflow:
            overflow_tasks = []
            for _, tasks in overflow:
                overflow_tasks.extend(tasks)
            labels.append("Others")
            values.append(len(overflow_tasks))
            drilldown["Others"] = _bucket_task_list(overflow_tasks)
        return labels, values, drilldown

    office_labels, office_values, office_drilldown = _top_group_rows(office_groups, limit=8)
    owner_labels, owner_values, owner_drilldown = _top_group_rows(owner_groups, limit=8)

    status_drilldown = {label: _bucket_task_list(tasks) for label, tasks in status_groups.items()}
    priority_drilldown = {label: _bucket_task_list(tasks) for label, tasks in priority_groups.items()}
    due_drilldown = {label: _bucket_task_list(tasks) for label, tasks in due_horizon_groups.items()}
    aging_drilldown = {label: _bucket_task_list(tasks) for label, tasks in aging_groups.items()}

    watchlist = {
        "critical": _bucket_task_list(critical_tasks, limit=12),
        "overdue": _bucket_task_list(overdue_tasks, limit=12),
        "stale": _bucket_task_list(stale_tasks, limit=12),
    }

    return {
        "signals": {
            "open_tasks": len(open_tasks),
            "completed_tasks": len(completed_tasks),
            "completion_pct": completion_pct,
            "overdue_tasks": len(overdue_tasks),
            "critical_open": len(critical_tasks),
            "unassigned_open": len(unassigned_tasks),
            "due_next_7_days": len(due_next_7_tasks),
            "stale_updates": len(stale_tasks),
        },
        "watchlist": watchlist,
        "charts": {
            "office_workload": {
                "title": "Open Tasks by Office",
                "labels": office_labels,
                "values": office_values,
                "drilldown": office_drilldown,
            },
            "owner_workload": {
                "title": "Open Tasks by Owner",
                "labels": owner_labels,
                "values": owner_values,
                "drilldown": owner_drilldown,
            },
            "status_mix": {
                "title": "Portfolio Status Mix",
                "labels": list(status_groups.keys()),
                "values": [len(status_groups[label]) for label in status_groups.keys()],
                "drilldown": status_drilldown,
            },
            "priority_pressure": {
                "title": "Priority Pressure",
                "labels": list(priority_groups.keys()),
                "values": [len(priority_groups[label]) for label in priority_groups.keys()],
                "drilldown": priority_drilldown,
            },
            "due_horizon": {
                "title": "Due Horizon",
                "labels": list(due_horizon_groups.keys()),
                "values": [len(due_horizon_groups[label]) for label in due_horizon_groups.keys()],
                "drilldown": due_drilldown,
            },
            "aging": {
                "title": "Open Task Aging",
                "labels": list(aging_groups.keys()),
                "values": [len(aging_groups[label]) for label in aging_groups.keys()],
                "drilldown": aging_drilldown,
            },
        },
    }


def get_superuser_dashboard_briefing():
    """Workspace-wide briefing (backward-compatible alias)."""
    return get_dashboard_briefing("workspace", None)


def _task_visibility_query_for_user(user):
    """Task query for *user* (see app.core.services.task_visibility)."""
    return task_visibility_query(user)


def _control_center_task_query_for_user(user):
    """Return the task query behind the command dashboard."""
    if user.is_super_user():
        return Task.query

    return _task_visibility_query_for_user(user)


def task_visible_in_command_dashboard(user, task_id: int) -> bool:
    """Return True when the task is visible in the user's command-dashboard scope."""
    if not user.is_super_user():
        return False

    return (
        _control_center_task_query_for_user(user)
        .filter(Task.id == task_id)
        .first()
        is not None
    )


def _display_first_name(user) -> str:
    full_name = (getattr(user, "full_name", "") or "").strip()
    if full_name:
        return full_name.split()[0]

    username = (getattr(user, "username", "") or "").strip()
    if not username:
        return "User"

    return username.split(".")[0].replace("_", " ").title()


def _display_user_name(user) -> str:
    full_name = (getattr(user, "full_name", "") or "").strip()
    if full_name:
        return full_name

    username = (getattr(user, "username", "") or "").strip()
    return username or "Unassigned"


def _build_officer_chain(user) -> list[dict]:
    officers = [
        ("Controlling Officer", getattr(user, "controlling_officer", None)),
        ("Reviewing Officer", getattr(user, "reviewing_officer", None)),
        ("Accepting Officer", getattr(user, "accepting_officer", None)),
    ]
    return [
        {"label": label, "name": _display_user_name(officer)}
        for label, officer in officers
        if officer is not None
    ]


def _time_of_day_greeting(now_local: datetime) -> str:
    if now_local.hour < 12:
        return "Good Morning"
    if now_local.hour < 17:
        return "Good Afternoon"
    return "Good Evening"


def _showcase_module_keys_for_user(user) -> tuple[str, ...]:
    if user.is_admin_user() and not user.is_super_user():
        return ADMIN_SHOWCASE_MODULE_KEYS
    return SHOWCASE_MODULE_KEYS


def _build_module_showcase(user, app=None) -> list[dict]:
    cards = []

    for key in _showcase_module_keys_for_user(user):
        definition = get_module_definition(key)
        if definition is None:
            continue

        feature_ready = is_module_enabled(key, app) and is_module_registered(key, app)

        if key == "inventory" and not feature_ready:
            cards.append(
                {
                    "key": key,
                    "label": definition.name,
                    "description": definition.description,
                    "icon_class": MODULE_ICON_MAP.get(key, "bi-grid"),
                    "accent": MODULE_ACCENT_MAP.get(key, "default"),
                    "badge": "Under Development",
                    "eyebrow": "Coming Soon",
                    "clickable": False,
                    "href": None,
                    "state": "coming-soon",
                    "is_available": False,
                }
            )
            continue

        if not feature_ready:
            continue

        clickable = user_can_access_module(key, user, app)
        cards.append(
            {
                "key": key,
                "label": definition.name,
                "description": definition.description,
                "icon_class": MODULE_ICON_MAP.get(key, "bi-grid"),
                "accent": MODULE_ACCENT_MAP.get(key, "default"),
                "badge": "Active" if clickable else ("Access Required" if feature_ready else "Unavailable"),
                "eyebrow": "Live Module" if feature_ready else "Configuration Required",
                "clickable": clickable,
                "href": url_for(definition.endpoint) if clickable and definition.endpoint else None,
                "state": "active" if feature_ready else "restricted",
                "is_available": feature_ready,
            }
        )

    return cards
