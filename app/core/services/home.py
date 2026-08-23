"""Home page context builder.

The home page is a single adaptive surface. Superusers switch its data scope
with the scope selector; everyone else sees ``my`` scope only. Deep analytics
live on their own page and are linked from here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from flask import url_for
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import joinedload, selectinload

from app.core.services.dashboard import (
    CLOSED_TASK_STATUSES,
    INDIA_TIMEZONE,
    PENDING_UPDATE_STATUSES,
    _build_module_showcase,
    _build_officer_chain,
    _display_first_name,
    _display_user_name,
    _time_of_day_greeting,
)
from app.core.module_registry import user_can_access_module
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator
from app.models.tasks.task_office import TaskOffice

SCOPE_MY = "my"
SCOPE_OFFICE = "office"
SCOPE_WORKSPACE = "workspace"
DEFAULT_SCOPE = SCOPE_MY

SCOPE_LABELS = {
    SCOPE_MY: "My Work",
    SCOPE_OFFICE: "My Office",
    SCOPE_WORKSPACE: "Workspace",
}
SCOPE_DETAILS = {
    SCOPE_MY: "Tasks you own or collaborate on.",
    SCOPE_OFFICE: "Every task inside your mapped office.",
    SCOPE_WORKSPACE: "Every task across all mapped offices.",
}

# Focus keys are shared with the Office Management task list so that every tile
# on this page opens a list showing exactly the tasks it counted.
FOCUS_OVERDUE = "overdue"
FOCUS_TODAY = "today"
FOCUS_WEEK = "week"
FOCUS_PENDING = "pending"
FOCUS_CRITICAL = "critical"
FOCUS_UNASSIGNED = "unassigned"


def available_scopes(user) -> list[str]:
    """Return the scope keys this user is entitled to switch between."""
    scopes = [SCOPE_MY]

    has_office = getattr(user, "office_id", None) is not None
    if user.is_super_user():
        if has_office:
            scopes.append(SCOPE_OFFICE)
        scopes.append(SCOPE_WORKSPACE)

    return scopes


def resolve_scope(user, requested: str | None) -> str:
    """Coerce a requested scope to one the user may actually view."""
    allowed = available_scopes(user)
    candidate = (requested or "").strip().lower()
    if candidate in allowed:
        return candidate
    return DEFAULT_SCOPE


def _office_task_query(office_id: int):
    """Every task belonging to, or tagged into, one office."""
    tagged_task_ids = (
        TaskOffice.query
        .with_entities(TaskOffice.task_id)
        .filter(TaskOffice.office_id == office_id)
        .subquery()
    )
    return Task.query.filter(
        or_(
            and_(
                Task.office_id == office_id,
                Task.is_private_self_task.is_(False),
            ),
            Task.id.in_(select(tagged_task_ids.c.task_id)),
        )
    )


def _my_work_query(user):
    """Only the tasks this user personally owns or collaborates on.

    Deliberately narrower than the RBAC visibility rule: "My Work" answers
    "what is on my desk", not "what am I allowed to read".
    """
    collaborating_task_ids = (
        TaskCollaborator.query
        .with_entities(TaskCollaborator.task_id)
        .filter(TaskCollaborator.user_id == user.id)
        .subquery()
    )
    return Task.query.filter(
        or_(
            Task.owner_id == user.id,
            Task.id.in_(select(collaborating_task_ids.c.task_id)),
        )
    )


def scoped_task_query(user, scope: str):
    """Return the base task query backing the given home scope.

    Entitlement is settled by ``resolve_scope`` before this is called, so an
    unentitled scope has already been downgraded to ``my``.
    """
    if scope == SCOPE_WORKSPACE and user.is_super_user():
        # Every office in the workspace.
        return Task.query

    if scope == SCOPE_OFFICE and getattr(user, "office_id", None) is not None:
        return _office_task_query(user.office_id)

    return _my_work_query(user)


def _due_label(task: Task, today: date) -> tuple[str, str]:
    """Return a human due-date label and the tone it should be rendered in."""
    if task.due_date is None:
        return "No due date", "neutral"

    delta = (task.due_date - today).days
    if delta < 0:
        days = abs(delta)
        return f"{days} day{'' if days == 1 else 's'} overdue", "danger"
    if delta == 0:
        return "Due today", "warning"
    if delta == 1:
        return "Due tomorrow", "warning"
    if delta <= 7:
        return f"Due {task.due_date.strftime('%a %d %b')}", "info"
    return f"Due {task.due_date.strftime('%d %b %Y')}", "neutral"


def _task_office_name(task: Task) -> str:
    office = getattr(task, "office", None)
    if office is not None and getattr(office, "office_name", None):
        return office.office_name

    tagged = [
        tagged_office.office_name
        for tagged_office in getattr(task, "tagged_offices", [])
        if getattr(tagged_office, "office_name", None)
    ]
    return tagged[0] if tagged else "Unassigned office"


def _focus_rank(task: Task, today: date) -> tuple:
    """Order tasks so the most pressing work sits at the top of the focus list."""
    if task.due_date is not None and task.due_date < today:
        band = 0
    elif task.due_date == today:
        band = 1
    elif task.priority == "Critical":
        band = 2
    elif task.due_date is not None and task.due_date <= today + timedelta(days=7):
        band = 3
    elif task.status in PENDING_UPDATE_STATUSES:
        band = 4
    else:
        band = 5

    priority_rank = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}.get(
        task.priority, 2
    )
    return band, task.due_date or date.max, priority_rank, task.id


def _serialize_focus_task(task: Task, today: date, show_owner: bool) -> dict:
    due_label, due_tone = _due_label(task, today)
    return {
        "id": task.id,
        "title": task.task_title or f"Task #{task.id}",
        "status": task.status or "Not Started",
        "priority": task.priority or "Medium",
        "owner": _display_user_name(task.owner) if getattr(task, "owner", None) else "Unassigned",
        "show_owner": show_owner,
        "office": _task_office_name(task),
        "due_label": due_label,
        "due_tone": due_tone,
        "href": url_for("tasks.task_detail", task_id=task.id),
    }


def _tile(label, value, detail, tone, icon, href):
    return {
        "label": label,
        "value": value,
        "detail": detail,
        "tone": tone,
        "icon": icon,
        "href": href,
    }


def get_home_context(user, scope: str | None = None, app=None) -> dict:
    """Build the full payload for the adaptive home page."""
    scope = resolve_scope(user, scope)
    now_local = datetime.now(timezone.utc).astimezone(INDIA_TIMEZONE)
    today = now_local.date()
    week_end = today + timedelta(days=(6 - today.weekday()))

    task_links_active = user_can_access_module("office_management", user, app)
    # Office and workspace scopes are entitlement-driven, so a superuser keeps
    # command visibility even without an explicit module grant.
    tasks_visible = task_links_active or scope in (SCOPE_OFFICE, SCOPE_WORKSPACE)

    open_tasks: list[Task] = []
    if tasks_visible:
        visible_tasks = (
            scoped_task_query(user, scope)
            .options(
                joinedload(Task.owner),
                joinedload(Task.office),
                selectinload(Task.tagged_offices),
            )
            .filter(Task.is_active.is_(True))
            .all()
        )
        open_tasks = [
            task for task in visible_tasks if task.status not in CLOSED_TASK_STATUSES
        ]

    overdue = [task for task in open_tasks if task.due_date and task.due_date < today]
    due_today = [task for task in open_tasks if task.due_date == today]
    due_this_week = [
        task for task in open_tasks
        if task.due_date and today <= task.due_date <= week_end
    ]
    pending_update = [
        task for task in open_tasks if task.status in PENDING_UPDATE_STATUSES
    ]
    critical_open = [task for task in open_tasks if task.priority == "Critical"]
    unassigned_open = [task for task in open_tasks if task.owner_id is None]

    def focus_href(focus_key: str) -> str | None:
        if not task_links_active:
            return None
        # ``view`` re-applies this scope on the list, so the tile count and the
        # list it opens always describe the same set of tasks.
        return url_for("tasks.list_tasks", focus=focus_key, view=scope)

    tiles = [
        _tile(
            "Overdue", len(overdue), "Past the committed date.",
            "danger", "bi-exclamation-diamond", focus_href(FOCUS_OVERDUE),
        ),
        _tile(
            "Due Today", len(due_today), "Close these before end of day.",
            "primary", "bi-calendar2-day", focus_href(FOCUS_TODAY),
        ),
        _tile(
            "Due This Week", len(due_this_week), "Commitments through week-end.",
            "info", "bi-calendar3-week", focus_href(FOCUS_WEEK),
        ),
        _tile(
            "Awaiting Progress", len(pending_update), "Not started or on hold.",
            "warning", "bi-arrow-repeat", focus_href(FOCUS_PENDING),
        ),
    ]
    if scope in (SCOPE_OFFICE, SCOPE_WORKSPACE):
        tiles.append(
            _tile(
                "Critical Open", len(critical_open), "Highest severity still running.",
                "danger", "bi-shield-exclamation", focus_href(FOCUS_CRITICAL),
            )
        )
        tiles.append(
            _tile(
                "Unassigned", len(unassigned_open), "Open with no owner mapped.",
                "neutral", "bi-person-dash", focus_href(FOCUS_UNASSIGNED),
            )
        )

    show_owner = scope in (SCOPE_OFFICE, SCOPE_WORKSPACE)
    focus_tasks = [
        _serialize_focus_task(task, today, show_owner)
        for task in sorted(open_tasks, key=lambda task: _focus_rank(task, today))[:8]
    ]

    # The work queue is collapsed by default, so its summary line has to carry
    # enough signal for someone to decide whether to open it.
    if not open_tasks:
        focus_summary = "No open tasks in scope."
        focus_summary_tone = "muted"
    else:
        parts = []
        if overdue:
            parts.append(f"{len(overdue)} overdue")
        if due_today:
            parts.append(f"{len(due_today)} due today")
        if critical_open:
            parts.append(f"{len(critical_open)} critical")
        # Only claim urgency when something is actually pressing.
        focus_summary_tone = "danger" if parts else "muted"
        if not parts:
            parts.append(f"{len(open_tasks)} open")
        focus_summary = " · ".join(parts)

    office_name = getattr(getattr(user, "office", None), "office_name", None)

    def scope_detail(key: str) -> str:
        if key == SCOPE_OFFICE and office_name:
            return f"Every task in {office_name}."
        return SCOPE_DETAILS[key]

    scope_options = [
        {
            "key": key,
            "label": SCOPE_LABELS[key],
            "detail": scope_detail(key),
            "href": url_for("main.dashboard", scope=key),
            "is_active": key == scope,
        }
        for key in available_scopes(user)
    ]

    analytics = None
    if user.is_super_user():
        analytics = {
            "label": "Organograms",
            "detail": "Reporting lines by office, alongside portfolio charts and drilldowns.",
            # Lands on the organogram tab; the portfolio charts stay one tab away.
            "href": url_for("main.analytics", scope=scope, _anchor="organogram"),
        }

    module_cards = _build_module_showcase(user, app)

    return {
        "scope": scope,
        "scope_label": SCOPE_LABELS[scope],
        "scope_detail": scope_detail(scope),
        "scope_options": scope_options if len(scope_options) > 1 else [],
        "greeting": _time_of_day_greeting(now_local),
        "first_name": _display_first_name(user),
        "identity": {
            "name": _display_user_name(user),
            "role": getattr(getattr(user, "role", None), "name", None),
            "office": getattr(getattr(user, "office", None), "office_name", None)
            or "Unassigned office",
            "officer_chain": _build_officer_chain(user),
        },
        "tasks_visible": tasks_visible,
        "task_links_active": task_links_active,
        "task_list_href": (
            url_for("tasks.list_tasks", view=scope) if task_links_active else None
        ),
        "open_task_count": len(open_tasks),
        "tiles": tiles,
        "focus_tasks": focus_tasks,
        "focus_summary": focus_summary,
        "focus_summary_tone": focus_summary_tone,
        "modules": module_cards,
        "modules_enabled_count": sum(1 for card in module_cards if card["is_available"]),
        # What this user can actually open, which is what the home page reports.
        "modules_accessible_count": sum(1 for card in module_cards if card["clickable"]),
        "modules_total_count": len(module_cards),
        "analytics": analytics,
        "last_refreshed": now_local,
    }
