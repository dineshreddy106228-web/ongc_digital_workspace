"""Cached dashboard service helpers."""

from collections import Counter
from datetime import date, datetime, timedelta

from app.extensions import cache
from app.models.tasks.task import Task


CLOSED_TASK_STATUSES = ("Completed", "Cancelled")


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
