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
from app.extensions import cache
from app.models.committee.committee_task import CommitteeTask
from app.models.core.user import User
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator
from app.models.tasks.task_office import TaskOffice


CLOSED_TASK_STATUSES = ("Completed", "Cancelled")
PENDING_UPDATE_STATUSES = ("Not Started", "On Hold")
COMMITTEE_CLOSED_STATUSES = ("done",)
SHOWCASE_MODULE_KEYS = ("office_management", "csc_workflow", "inventory", "manpower_planning")
ADMIN_SHOWCASE_MODULE_KEYS = ("admin_users",)
MODULE_ICON_MAP = {
    "office_management": "bi-list-task",
    "csc_workflow": "bi-diagram-3",
    "inventory": "bi-box-seam",
    "manpower_planning": "bi-people-fill",
    "admin_users": "bi-people",
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
    cache.delete_memoized(get_superuser_dashboard_briefing)


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


def _can_access_committee_workspace(user) -> bool:
    return bool(
        user.is_admin_user()
        or user.is_super_user()
        or user.has_module_access("committee")
    )


def _committee_visibility_query_for_user(user):
    if not _can_access_committee_workspace(user):
        return None

    base = CommitteeTask.query.options(selectinload(CommitteeTask.members))
    if user.is_super_user() or user.is_admin_user():
        return base

    if getattr(user, "office_id", None) is None:
        return base.filter(CommitteeTask.members.any(user_id=user.id))

    return base.filter(
        or_(
            CommitteeTask.office_id == user.office_id,
            CommitteeTask.members.any(user_id=user.id),
        )
    )


def _serialize_committee_task(task: CommitteeTask) -> dict:
    committee_head = "Not assigned"
    assignee_count = 0

    for member in getattr(task, "members", []) or []:
        if member.role == "committee_head" and getattr(member, "user", None):
            committee_head = member.user.full_name or member.user.username or committee_head
        elif member.role == "assignee":
            assignee_count += 1

    return {
        "id": task.id,
        "title": task.title or f"Committee Task #{task.id}",
        "status": (task.status or "open").replace("_", " ").title(),
        "priority": (task.priority or "medium").title(),
        "status_key": (task.status or "open").lower(),
        "priority_key": (task.priority or "medium").lower(),
        "office": task.office.office_name if getattr(task, "office", None) else "Unassigned Office",
        "committee_head": committee_head,
        "member_count": assignee_count,
        "due_date": task.due_date.strftime("%d %b %Y") if task.due_date else "No due date",
        "detail_url": url_for("committee.task_detail", task_id=task.id),
        "summary_url": url_for("committee.task_summary", task_id=task.id),
    }


def _build_committee_workspace_context(user) -> dict | None:
    query = _committee_visibility_query_for_user(user)
    if query is None:
        return None

    today = date.today()
    visible_tasks = query.order_by(CommitteeTask.created_at.desc()).all()
    open_tasks = [task for task in visible_tasks if task.status not in COMMITTEE_CLOSED_STATUSES]
    queued_tasks = [task for task in open_tasks if task.status == "open"]
    overdue_tasks = [
        task for task in open_tasks
        if task.due_date and task.due_date < today
    ]
    due_next_7_days = [
        task for task in open_tasks
        if task.due_date and today <= task.due_date <= today + timedelta(days=7)
    ]
    in_progress_tasks = [task for task in open_tasks if task.status == "in_progress"]
    completed_tasks = [task for task in visible_tasks if task.status == "done"]

    preview_tasks = sorted(
        open_tasks,
        key=lambda item: (
            item.due_date is None,
            item.due_date or date.max,
            (item.priority or "").lower(),
            item.id,
        ),
    )[:6]

    if user.is_super_user() or user.is_admin_user():
        scope_label = "All Offices"
        subtitle = "Committee assignments across the full workspace."
    else:
        office_name = getattr(getattr(user, "office", None), "office_name", None) or "Your office"
        scope_label = office_name
        subtitle = f"Committee assignments visible within {office_name}."
    can_create = bool(user.is_super_user())

    return {
        "available": True,
        "title": "Committee Management",
        "subtitle": subtitle,
        "scope_label": scope_label,
        "list_url": url_for("committee.list_tasks"),
        "create_url": url_for("committee.create_task"),
        "can_create": can_create,
        "items": [_serialize_committee_task(task) for task in preview_tasks],
        "actions": [
            {
                "label": "Open Tasks",
                "value": len(queued_tasks),
                "detail": "New committee work waiting to be picked up.",
                "tone": "primary",
                "href": url_for("committee.list_tasks", status="open"),
            },
            {
                "label": "In Progress",
                "value": len(in_progress_tasks),
                "detail": "Assignments currently being worked.",
                "tone": "warning",
                "href": url_for("committee.list_tasks", status="in_progress"),
            },
            {
                "label": "Overdue",
                "value": len(overdue_tasks),
                "detail": "Past due date and still open.",
                "tone": "danger",
                "href": url_for("committee.list_tasks"),
            },
            {
                "label": "Completed",
                "value": len(completed_tasks),
                "detail": "Tasks closed in the visible committee scope.",
                "tone": "success",
                "href": url_for("committee.list_tasks", status="done"),
            },
        ],
        "empty_state": {
            "title": "No committee tasks in this scope",
            "body": "Newly created committee tasks will appear here for quick access from the landing dashboard.",
        },
        "summary": {
            "total": len(visible_tasks),
            "due_next_7_days": len(due_next_7_days),
        },
    }


@cache.memoize(timeout=300)
def get_superuser_dashboard_briefing():
    """Return presentation-grade drilldown analytics for the superuser briefing page."""
    today = date.today()
    all_active_tasks = (
        Task.query
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

    # ── Committee Task Statistics ──────────────────────────────────
    all_committee_tasks = (
        CommitteeTask.query
        .order_by(CommitteeTask.created_at.desc())
        .all()
    )
    committee_open = [t for t in all_committee_tasks if t.status not in COMMITTEE_CLOSED_STATUSES]
    committee_done = [t for t in all_committee_tasks if t.status == "done"]
    committee_overdue = [
        t for t in committee_open
        if t.due_date and t.due_date < today
    ]
    committee_in_progress = [t for t in committee_open if t.status == "in_progress"]
    committee_high_priority = [t for t in committee_open if t.priority == "high"]
    committee_completion_pct = round(
        (len(committee_done) / len(all_committee_tasks) * 100) if all_committee_tasks else 0
    )

    committee_status_counts = dict(Counter(t.status for t in all_committee_tasks))
    committee_priority_counts = dict(Counter(t.priority for t in committee_open))
    committee_office_counts = dict(Counter(
        t.office.office_name if t.office else "Unknown"
        for t in committee_open
    ))

    committee_stats = {
        "total": len(all_committee_tasks),
        "open": len(committee_open),
        "done": len(committee_done),
        "overdue": len(committee_overdue),
        "in_progress": len(committee_in_progress),
        "high_priority": len(committee_high_priority),
        "completion_pct": committee_completion_pct,
        "by_status": committee_status_counts,
        "by_priority": committee_priority_counts,
        "by_office": committee_office_counts,
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
        "committee": committee_stats,
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


def _control_center_task_query_for_user(user):
    """Return the task query for dedicated power/superuser dashboard mode."""
    if user.is_super_user():
        return Task.query

    if user.is_office_power_user() and user.office_id is not None:
        tagged_task_ids_subq = (
            TaskOffice.query
            .with_entities(TaskOffice.task_id)
            .filter(TaskOffice.office_id == user.office_id)
            .subquery()
        )
        return Task.query.filter(
            or_(
                and_(
                    Task.task_scope.in_(["MY", "TEAM"]),
                    Task.office_id == user.office_id,
                    Task.is_private_self_task.is_(False),
                ),
                and_(
                    Task.task_scope == "GLOBAL",
                    or_(
                        Task.office_id == user.office_id,
                        Task.id.in_(select(tagged_task_ids_subq.c.task_id)),
                    ),
                ),
            )
        )

    return _task_visibility_query_for_user(user)


def task_visible_in_command_dashboard(user, task_id: int) -> bool:
    """Return True when the task is visible in the user's command-dashboard scope."""
    if not (user.is_super_user() or user.is_office_power_user()):
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
                    "badge": "Under Development",
                    "eyebrow": "Coming Soon",
                    "clickable": False,
                    "href": None,
                    "state": "coming-soon",
                    "is_available": False,
                }
            )
            continue

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


def _dashboard_identity_title(user) -> str:
    if user.is_admin_user():
        return "Admin Dashboard"
    if getattr(user, "office", None) and getattr(user.office, "office_name", None):
        return user.office.office_name
    return "Corporate Chemistry Digital Workspace"


def _dashboard_control_center(user) -> dict | None:
    if user.is_super_user():
        return {
            "label": "Super User Dashboard",
            "detail": "Global workspace mission control.",
            "href": url_for("main.superuser_dashboard"),
        }

    if user.is_office_power_user():
        return {
            "label": "Power User Dashboard",
            "detail": "Office-wide command view.",
            "href": url_for("main.power_user_dashboard"),
        }

    return None


def get_dashboard_workspace_context(user, app=None, mode: str = "default") -> dict:
    """Build the main workspace dashboard payload for the logged-in user."""
    now_local = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    today = now_local.date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    recent_completion_start = today - timedelta(days=2)
    stale_update_threshold = today - timedelta(days=3)
    control_center_mode = mode in {"control_center", "power_user"}
    power_user_mode = mode == "power_user"
    task_links_active = user_can_access_module("office_management", user, app)
    task_module_active = task_links_active or (
        control_center_mode and (user.is_super_user() or user.is_office_power_user())
    )

    open_tasks = []
    due_today = []
    overdue = []
    pending_updates = []
    recently_completed = []
    due_this_week = []
    completed_this_week = []
    pending_across_team = 0
    module_cards = _build_module_showcase(user, app)
    committee_workspace = _build_committee_workspace_context(user)

    if task_module_active:
        task_query = (
            _control_center_task_query_for_user(user)
            if control_center_mode
            else _task_visibility_query_for_user(user)
        )
        visible_tasks = (
            task_query
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

    task_list_href = url_for("tasks.list_tasks") if task_links_active else None
    identity_title = _dashboard_identity_title(user)
    identity_subtitle = "Aligned execution. Clear accountability. Real-time operational visibility."
    guidance_message = "All updates are reflected in real time. Keep your tasks current."
    if control_center_mode:
        if user.is_super_user():
            identity_title = "Super User Dashboard"
            identity_subtitle = "Global workspace mission control across all offices."
            guidance_message = "Global command visibility is active across the full workspace."
        elif power_user_mode and user.is_office_power_user():
            office_name = getattr(getattr(user, "office", None), "office_name", "") or "Office"
            identity_title = "Power User Dashboard"
            identity_subtitle = f"{office_name} office mission control."
            guidance_message = f"Only tasks within the {office_name} office scope are shown here."

    return {
        "identity": {
            "title": identity_title,
            "subtitle": identity_subtitle,
        },
        "welcome": {
            "greeting": _time_of_day_greeting(now_local),
            "name": _display_first_name(user),
            "summary": _build_welcome_summary(open_tasks, len(due_today), len(pending_updates)),
        },
        "officer_chain": _build_officer_chain(user),
        "task_module_active": task_module_active,
        "task_links_active": task_links_active,
        "control_center_mode": control_center_mode,
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
        "committee_workspace": committee_workspace,
        "modules": module_cards,
        "guidance_message": guidance_message,
        "modules_enabled_count": sum(1 for card in module_cards if card["is_available"]),
        "control_center": _dashboard_control_center(user),
    }
