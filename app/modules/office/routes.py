from __future__ import annotations

"""Task tracker routes – Office Management module with Governance V2 visibility model."""

from collections import Counter
from datetime import datetime, date as date_type
from flask import render_template, redirect, url_for, flash, request, abort, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import or_, and_, select
from sqlalchemy.exc import SQLAlchemyError
from app.modules.office import office_bp
from app.extensions import db
from app.models.tasks.recurring_task_collaborator import RecurringTaskCollaborator
from app.models.tasks.recurring_task_template import (
    RECURRENCE_TYPES,
    RECURRENCE_WEEKDAYS,
    RecurringTaskTemplate,
)
from app.models.core.user import User
from app.models.office.office import Office
from app.models.tasks.task import Task, TASK_SCOPES
from app.models.tasks.task_collaborator import TaskCollaborator
from app.models.tasks.task_office import TaskOffice
from app.models.tasks.task_update import TaskUpdate
from app.core.services.dashboard import invalidate_dashboard_summary_metrics
from app.core.services.notifications import create_notification
from app.core.services.recurring_tasks import (
    create_initial_task_for_template,
    decode_weekday_codes,
    encode_weekday_codes,
    first_occurrence_date,
    next_scheduled_occurrence_for_template,
    occurrence_dates_in_window,
    normalize_weekday_codes,
    recurrence_summary,
)
from app.core.services.rich_text import rich_text_visible_text, sanitize_rich_text
from app.core.utils.audit import log_action
from app.core.utils.activity import log_activity
from app.core.utils.decorators import module_access_required, superuser_required
from app.core.permissions import (
    can_view_task,
    can_edit_task,
    can_close_task,
    can_add_update,
    can_create_global_task,
)
from app.core.roles import ADMIN_ROLE


TASK_STATUSES = [
    "Not Started",
    "In Progress",
    "On Hold",
    "Completed",
    "Cancelled",
]

TASK_PRIORITIES = [
    "Low",
    "Medium",
    "High",
    "Critical",
]

MAX_TASK_TITLE_LEN = 255
MAX_TASK_ORIGIN_LEN = 100
MAX_TASK_DESC_LEN = 5000
MAX_TASK_UPDATE_LEN = 5000
TASK_SCHEDULE_MODES = ["ONE_TIME", "RECURRING"]


# ── Permission helpers ────────────────────────────────────────────

def _is_privileged():
    """True for super_user or admin – full task visibility and management access."""
    return current_user.is_super_user() or current_user.has_role(ADMIN_ROLE)


def _normalize_task_scope(scope: str | None, default: str = "") -> str:
    raw_scope = (scope or "").strip().upper()
    if not raw_scope:
        return default
    if raw_scope in ("LOCAL", "MY", "TEAM"):
        return "MY"
    return raw_scope


def _scope_filter_condition(scope: str):
    normalized_scope = _normalize_task_scope(scope)
    if normalized_scope == "MY":
        return Task.task_scope.in_(["MY", "TEAM"])
    return Task.task_scope == normalized_scope


def _task_collaborator_user_ids(task: Task) -> set[int]:
    return {
        int(link.user_id)
        for link in getattr(task, "collaborator_links", [])
        if getattr(link, "user_id", None)
    }


def _is_task_collaborator(task: Task, user_id: int) -> bool:
    return int(user_id) in _task_collaborator_user_ids(task)


def _recurring_template_collaborator_user_ids(template: RecurringTaskTemplate) -> set[int]:
    return {
        int(link.user_id)
        for link in getattr(template, "collaborator_links", [])
        if getattr(link, "user_id", None)
    }


def _is_recurring_template_collaborator(template: RecurringTaskTemplate, user_id: int) -> bool:
    return int(user_id) in _recurring_template_collaborator_user_ids(template)


def _participant_and_controller_ids(task: Task) -> set[int]:
    recipient_ids = set()

    participant_users = []
    if task.owner:
        participant_users.append(task.owner)
    participant_users.extend(
        link.user for link in getattr(task, "collaborator_links", []) if getattr(link, "user", None)
    )

    for user in participant_users:
        if user and user.id:
            recipient_ids.add(int(user.id))
        if user and user.controlling_officer_id:
            recipient_ids.add(int(user.controlling_officer_id))

    return recipient_ids


def _recurring_template_participant_and_controller_ids(
    template: RecurringTaskTemplate,
) -> set[int]:
    recipient_ids = set()

    participant_users = []
    if template.owner:
        participant_users.append(template.owner)
    participant_users.extend(
        link.user for link in getattr(template, "collaborator_links", []) if getattr(link, "user", None)
    )

    for user in participant_users:
        if user and user.id:
            recipient_ids.add(int(user.id))
        if user and user.controlling_officer_id:
            recipient_ids.add(int(user.controlling_officer_id))

    return recipient_ids


def _active_task_user_options(exclude_user_ids=None):
    excluded_ids = {int(user_id) for user_id in (exclude_user_ids or set()) if user_id}
    return [
        user
        for user in _active_owner_options()
        if int(user.id) not in excluded_ids
    ]


def _selected_collaborator_ids(form_data, allowed_user_ids: set[str]) -> list[str]:
    if not hasattr(form_data, "getlist"):
        return []

    selected = []
    seen = set()
    for raw_user_id in form_data.getlist("collaborator_ids"):
        clean_user_id = (raw_user_id or "").strip()
        if clean_user_id and clean_user_id in allowed_user_ids and clean_user_id not in seen:
            selected.append(clean_user_id)
            seen.add(clean_user_id)
    return selected


def _submitted_collaborator_ids(form_data) -> list[str]:
    if not hasattr(form_data, "getlist"):
        return []
    return [
        clean_user_id
        for raw_user_id in form_data.getlist("collaborator_ids")
        if (clean_user_id := (raw_user_id or "").strip())
    ]


def _collaborator_count(form_data, selected_ids: list[str], max_slots: int) -> int:
    if max_slots == 0:
        return 0
    if hasattr(form_data, "get"):
        raw_count = (form_data.get("collaborator_count", "") or "").strip()
        if raw_count.isdigit():
            return max(0, min(int(raw_count), max_slots))
    return max(0, min(len(selected_ids), max_slots))


def _can_view_task(task: Task) -> bool:
    """Delegate to centralized permission engine."""
    return can_view_task(current_user, task)


def _can_view_recurring_template(template: RecurringTaskTemplate) -> bool:
    """Recurring template visibility — mirrors can_view_task logic for templates."""
    if _is_privileged():
        return True
    if template.owner_id == current_user.id:
        return True
    if _is_recurring_template_collaborator(template, current_user.id):
        return True
    scope = _normalize_task_scope(template.task_scope)
    if scope == "GLOBAL":
        return True
    if scope == "MY" and current_user.id in _recurring_template_participant_and_controller_ids(template):
        return True
    return False


def _can_edit_task(task: Task) -> bool:
    """Delegate to centralized permission engine."""
    return can_edit_task(current_user, task)


def _can_edit_recurring_template(template: RecurringTaskTemplate) -> bool:
    """Recurring template edit — mirrors can_edit_task logic for templates."""
    if _is_privileged():
        return True
    return template.owner_id == current_user.id or template.created_by == current_user.id


def _can_add_update(task: Task) -> bool:
    """Delegate to centralized permission engine."""
    return can_add_update(current_user, task)


def _can_close_task(task: Task) -> bool:
    """Delegate to centralized permission engine. Close = terminal status change."""
    return can_close_task(current_user, task)


def _can_create_global_task() -> bool:
    """Delegate to centralized permission engine. Superuser and Admin only."""
    return can_create_global_task(current_user)


def _active_owner_options():
    base_query = User.query.filter_by(is_active=True)
    if current_user.office_id:
        same_office = base_query.filter_by(office_id=current_user.office_id).order_by(
            User.full_name, User.username
        ).all()
        other_offices = (
            base_query.filter(User.office_id != current_user.office_id)
            .order_by(User.full_name, User.username)
            .all()
        )
        return same_office + other_offices
    return base_query.order_by(User.full_name, User.username).all()


def _parse_due_date(raw_due_date: str):
    if not raw_due_date:
        return None
    try:
        return datetime.strptime(raw_due_date, "%Y-%m-%d").date()
    except ValueError:
        return None


def _normalize_schedule_mode(mode: str | None, default: str = "ONE_TIME") -> str:
    raw_mode = (mode or "").strip().upper()
    if raw_mode in TASK_SCHEDULE_MODES:
        return raw_mode
    return default


def _selected_recurrence_weekdays(form_data) -> list[str]:
    if not hasattr(form_data, "getlist"):
        return []
    return normalize_weekday_codes(form_data.getlist("recurrence_weekdays"))


def _parse_monthly_day(raw_monthly_day: str | None) -> int | None:
    clean_value = (raw_monthly_day or "").strip()
    if not clean_value:
        return None
    if clean_value.isdigit():
        return int(clean_value)
    return None


def _recurrence_form_context(form_data, task: Task | None = None) -> dict[str, object]:
    template = task.recurring_template if task else None
    if task and template:
        default_start_date = template.start_date.strftime("%Y-%m-%d") if template.start_date else ""
        default_end_date = template.end_date.strftime("%Y-%m-%d") if template.end_date else ""
        default_monthly_day = str(template.monthly_day) if template.monthly_day else ""
        default_weekdays = decode_weekday_codes(template.weekly_days)
        default_recurrence_type = template.recurrence_type
        default_schedule_mode = "RECURRING"
    else:
        default_start_date = ""
        default_end_date = ""
        default_monthly_day = ""
        default_weekdays = []
        default_recurrence_type = "DAILY"
        default_schedule_mode = "ONE_TIME"

    return {
        "schedule_mode": _normalize_schedule_mode(
            form_data.get("schedule_mode") if hasattr(form_data, "get") else None,
            default=default_schedule_mode,
        ),
        "recurrence_type": (
            (form_data.get("recurrence_type") if hasattr(form_data, "get") else None)
            or default_recurrence_type
        ),
        "recurrence_start_date": (
            (form_data.get("recurrence_start_date") if hasattr(form_data, "get") else None)
            or default_start_date
        ),
        "recurrence_end_date": (
            (form_data.get("recurrence_end_date") if hasattr(form_data, "get") else None)
            or default_end_date
        ),
        "recurrence_month_day": (
            (form_data.get("recurrence_month_day") if hasattr(form_data, "get") else None)
            or default_monthly_day
        ),
        "selected_recurrence_weekdays": (
            _selected_recurrence_weekdays(form_data)
            if hasattr(form_data, "getlist")
            else default_weekdays
        ),
    }


def _task_visibility_query():
    """
    Build a Task query that respects the RBAC visibility model:

    Privileged (super_user or admin):
        → all tasks

    Standard users:
        → GLOBAL tasks (task_scope == 'GLOBAL')
        → personal tasks where owner_id == current_user.id
        → tasks where user is a collaborator
        → MY-scope tasks of users whose controlling_officer_id == current_user.id
    """
    base = Task.query

    # Privileged roles see everything
    if _is_privileged():
        return base

    conds = []

    # 1. All GLOBAL tasks
    conds.append(Task.task_scope == "GLOBAL")

    # 2. Tasks directly assigned to me (including any legacy scope values)
    conds.append(Task.owner_id == current_user.id)

    collaborator_task_ids_subq = (
        db.session.query(TaskCollaborator.task_id)
        .filter_by(user_id=current_user.id)
        .subquery()
    )
    conds.append(Task.id.in_(select(collaborator_task_ids_subq.c.task_id)))

    # 3. Personal-scope tasks for users I control (subquery avoids large Python lists)
    controlled_ids_subq = (
        db.session.query(User.id)
        .filter_by(controlling_officer_id=current_user.id, is_active=True)
        .subquery()
    )
    controlled_collaborator_task_ids_subq = (
        db.session.query(TaskCollaborator.task_id)
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


def _recurring_template_visibility_query():
    base = RecurringTaskTemplate.query

    if _is_privileged():
        return base

    conds = []
    conds.append(RecurringTaskTemplate.task_scope == "GLOBAL")
    conds.append(RecurringTaskTemplate.owner_id == current_user.id)

    collaborator_template_ids_subq = (
        db.session.query(RecurringTaskCollaborator.template_id)
        .filter_by(user_id=current_user.id)
        .subquery()
    )
    conds.append(RecurringTaskTemplate.id.in_(select(collaborator_template_ids_subq.c.template_id)))

    controlled_ids_subq = (
        db.session.query(User.id)
        .filter_by(controlling_officer_id=current_user.id, is_active=True)
        .subquery()
    )
    controlled_collaborator_template_ids_subq = (
        db.session.query(RecurringTaskCollaborator.template_id)
        .filter(RecurringTaskCollaborator.user_id.in_(select(controlled_ids_subq.c.id)))
        .subquery()
    )
    conds.append(
        and_(
            RecurringTaskTemplate.task_scope.in_(["MY", "TEAM"]),
            or_(
                RecurringTaskTemplate.owner_id.in_(select(controlled_ids_subq.c.id)),
                RecurringTaskTemplate.id.in_(
                    select(controlled_collaborator_template_ids_subq.c.template_id)
                ),
            ),
        )
    )

    return base.filter(or_(*conds))


def _db_error(message: str):
    db.session.rollback()
    current_app.logger.exception("Task module database operation failed")
    flash(message, "danger")


def _notify_task_owner(
    task: Task,
    title: str,
    message: str,
    severity: str = "info",
    skip_user_id: int | None = None,
):
    if not task.owner_id or task.owner_id == skip_user_id:
        return None

    return create_notification(
        user_id=task.owner_id,
        title=title,
        message=message,
        severity=severity,
        link=url_for("tasks.task_detail", task_id=task.id),
    )


def _notify_task_participants(
    task: Task,
    title: str,
    message: str,
    severity: str = "info",
    skip_user_ids=None,
):
    skip_ids = {int(user_id) for user_id in (skip_user_ids or set()) if user_id is not None}
    recipient_ids = {int(task.owner_id)} if task.owner_id else set()
    recipient_ids.update(_task_collaborator_user_ids(task))
    recipient_ids -= skip_ids

    for user_id in recipient_ids:
        create_notification(
            user_id=user_id,
            title=title,
            message=message,
            severity=severity,
            link=url_for("tasks.task_detail", task_id=task.id),
        )


def _display_name(user) -> str:
    if user is None:
        return "A user"
    return (user.full_name or user.username or "A user").strip()


def _format_remark_message(author_name: str, task_title: str, remark_text: str) -> str:
    remark = " ".join((remark_text or "").split())
    if len(remark) > 240:
        remark = f"{remark[:237].rstrip()}..."
    return (
        f"{author_name} added a remark on '{task_title}'. "
        f"Remark: \"{remark}\""
    )


def _notify_task_remark_stakeholders(task: Task, remark_text: str):
    if not task:
        return

    recipients = _participant_and_controller_ids(task)
    recipients.discard(int(current_user.id))
    if not recipients:
        return

    author_name = _display_name(current_user)
    title = "New remark on task"
    message = _format_remark_message(author_name, task.task_title, remark_text)

    for user_id in recipients:
        create_notification(
            user_id=user_id,
            title=title,
            message=message,
            severity="info",
            link=url_for("tasks.task_detail", task_id=task.id),
        )


def _month_bounds(year: int, month: int):
    start = date_type(year, month, 1)
    if month == 12:
        end = date_type(year + 1, 1, 1)
    else:
        end = date_type(year, month + 1, 1)
    return start, end


def _split_tasks_for_list(all_tasks):
    """Single-pass partition to avoid O(n^2) list membership checks."""
    global_tasks = []
    my_tasks = []

    for task in all_tasks:
        if _normalize_task_scope(task.task_scope) == "GLOBAL":
            global_tasks.append(task)
        else:
            my_tasks.append(task)

    return global_tasks, my_tasks


def _split_tasks_for_dashboard(all_tasks):
    """Dashboard grouping keeps all visible personal tasks in 'My Tasks'."""
    global_tasks = []
    my_tasks = []

    for task in all_tasks:
        if _normalize_task_scope(task.task_scope) == "GLOBAL":
            global_tasks.append(task)
        else:
            my_tasks.append(task)

    return global_tasks, my_tasks


def _is_archived_task(task: Task) -> bool:
    return (not task.is_active) or task.status in ("Completed", "Cancelled")


# ── List Tasks ────────────────────────────────────────────────────
@office_bp.route("")
@office_bp.route("/")
@login_required
@module_access_required("tasks")
def list_tasks():
    status_filter = request.args.get("status", "").strip()
    priority_filter = request.args.get("priority", "").strip()
    owner_filter = request.args.get("owner", "").strip()
    scope_filter = _normalize_task_scope(request.args.get("scope", "").strip())

    query = _task_visibility_query()

    if status_filter in TASK_STATUSES:
        query = query.filter(Task.status == status_filter)
    if priority_filter in TASK_PRIORITIES:
        query = query.filter(Task.priority == priority_filter)
    if owner_filter.isdigit():
        query = query.filter(Task.owner_id == int(owner_filter))
    if scope_filter in TASK_SCOPES:
        query = query.filter(_scope_filter_condition(scope_filter))

    all_tasks = query.order_by(Task.created_at.desc()).all()
    active_tasks = [task for task in all_tasks if not _is_archived_task(task)]
    archived_tasks = [task for task in all_tasks if _is_archived_task(task)]
    global_tasks, my_tasks = _split_tasks_for_list(active_tasks)
    is_privileged = _is_privileged()
    owners = _active_owner_options() if is_privileged else []

    task_permissions = {
        task.id: {
            "can_edit": _can_edit_task(task),
            "can_add_update": _can_add_update(task),
            "can_close": _can_close_task(task),
        }
        for task in all_tasks
    }

    return render_template(
        "tasks/list.html",
        tasks=all_tasks,
        active_tasks=active_tasks,
        global_tasks=global_tasks,
        my_tasks=my_tasks,
        owners=owners,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        task_scopes=TASK_SCOPES,
        filters={
            "status": status_filter,
            "priority": priority_filter,
            "owner": owner_filter,
            "scope": scope_filter,
        },
        archived_tasks=archived_tasks,
        task_permissions=task_permissions,
        can_create_global=_can_create_global_task(),
        is_privileged=is_privileged,
        recurrence_summary=recurrence_summary,
    )


# ── Task Dashboard ────────────────────────────────────────────────
@office_bp.route("/dashboard")
@login_required
@module_access_required("tasks")
@superuser_required
def task_dashboard():
    """Sectioned summary + calendar view for superusers only."""
    all_tasks = (
        _task_visibility_query()
        .filter_by(is_active=True)
        .order_by(Task.created_at.desc())
        .all()
    )

    # ── Summary counts ──────────────────────────────────────────
    global_tasks, my_tasks = _split_tasks_for_dashboard(all_tasks)
    # ── Recent activity (latest 5 updates) ──────────────────────
    visible_task_ids_subq = _task_visibility_query().with_entities(Task.id).subquery()
    recent_updates = (
        TaskUpdate.query
        .filter(TaskUpdate.task_id.in_(select(visible_task_ids_subq.c.id)))
        .order_by(TaskUpdate.created_at.desc())
        .limit(5)
        .all()
    )

    # ── Due-soon tasks ─────────────────────────────────────────────
    # Built directly from calendar_tasks (the already-serialised dicts) so it is
    # guaranteed to be identical to what the calendar dots show.  ISO date strings
    # sort lexicographically, so string comparison gives correct date order.
    due_soon_rows = (
        _task_visibility_query()
        .filter(
            Task.is_active.is_(True),
            Task.due_date.isnot(None),
            Task.status.notin_(("Completed", "Cancelled")),
        )
        .order_by(Task.due_date.asc())
        .limit(8)
        .all()
    )
    due_soon_tasks = [
        {
            "id": t.id,
            "title": t.task_title,
            "scope": _normalize_task_scope(t.task_scope),
            "due_date": t.due_date.strftime("%Y-%m-%d"),
            "status": t.status,
        }
        for t in due_soon_rows
        if t.due_date
    ]

    # ── Super-user analytics ─────────────────────────────────────
    analytics = None
    if _is_privileged():
        today = date_type.today()
        status_counts   = dict(Counter(t.status       for t in all_tasks))
        priority_counts = dict(Counter(t.priority     for t in all_tasks))
        scope_counts    = dict(Counter(_normalize_task_scope(t.task_scope) for t in all_tasks))
        overdue_count   = sum(
            1 for t in all_tasks
            if t.due_date and t.due_date < today
            and t.status not in ("Completed", "Cancelled")
        )
        # Completion rate
        completed = sum(1 for t in all_tasks if t.status == "Completed")
        total     = len(all_tasks)
        completion_pct = round((completed / total * 100) if total else 0)

        analytics = {
            "status":         status_counts,
            "priority":       priority_counts,
            "scope":          scope_counts,
            "overdue":        overdue_count,
            "total":          total,
            "completed":      completed,
            "completion_pct": completion_pct,
        }

    return render_template(
        "tasks/dashboard.html",
        global_tasks=global_tasks,
        my_tasks=my_tasks,
        recent_updates=recent_updates,
        due_soon_tasks=due_soon_tasks,
        is_privileged=_is_privileged(),
        analytics=analytics,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        calendar_initial={"year": date_type.today().year, "month": date_type.today().month},
    )


@office_bp.route("/calendar-data")
@login_required
@module_access_required("tasks")
def calendar_data():
    """Return task due-date items for a specific month in the user's visibility scope."""
    year_raw = request.args.get("year", "").strip()
    month_raw = request.args.get("month", "").strip()

    if not (year_raw.isdigit() and month_raw.isdigit()):
        return jsonify({"error": "year and month must be numeric"}), 400

    year = int(year_raw)
    month = int(month_raw)
    if year < 2000 or year > 2100 or month < 1 or month > 12:
        return jsonify({"error": "year/month out of allowed range"}), 400

    start_date, end_date = _month_bounds(year, month)
    rows = (
        _task_visibility_query()
        .filter(
            Task.is_active.is_(True),
            Task.due_date.isnot(None),
            Task.due_date >= start_date,
            Task.due_date < end_date,
        )
        .order_by(Task.due_date.asc(), Task.id.asc())
        .all()
    )

    items = [
        {
            "id": int(t.id),
            "template_id": int(t.recurring_template_id) if t.recurring_template_id else None,
            "title": t.task_title or "",
            "scope": _normalize_task_scope(t.task_scope),
            "due_date": t.due_date.strftime("%Y-%m-%d"),
            "status": t.status or "Not Started",
            "is_projected": False,
        }
        for t in rows
        if t.due_date
    ]

    existing_recurring_occurrences = {
        (int(task.recurring_template_id), task.occurrence_date)
        for task in rows
        if task.recurring_template_id and task.occurrence_date
    }
    visible_templates = (
        _recurring_template_visibility_query()
        .filter(
            RecurringTaskTemplate.is_active.is_(True),
            RecurringTaskTemplate.next_generation_date.isnot(None),
            RecurringTaskTemplate.next_generation_date < end_date,
            or_(
                RecurringTaskTemplate.end_date.is_(None),
                RecurringTaskTemplate.end_date >= start_date,
            ),
        )
        .order_by(RecurringTaskTemplate.next_generation_date.asc(), RecurringTaskTemplate.id.asc())
        .all()
    )
    for template in visible_templates:
        for occurrence_date in occurrence_dates_in_window(template, start_date, end_date):
            occurrence_key = (int(template.id), occurrence_date)
            if occurrence_key in existing_recurring_occurrences:
                continue
            items.append(
                {
                    "id": None,
                    "template_id": int(template.id),
                    "title": template.task_title or "",
                    "scope": _normalize_task_scope(template.task_scope),
                    "due_date": occurrence_date.strftime("%Y-%m-%d"),
                    "status": template.status or "Not Started",
                    "is_projected": True,
                }
            )

    items.sort(
        key=lambda item: (
            item["due_date"],
            0 if not item["is_projected"] else 1,
            (item["title"] or "").lower(),
        )
    )
    return jsonify({"year": year, "month": month, "items": items})


# ── Create Task ───────────────────────────────────────────────────
@office_bp.route("/create", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def create_task():
    owners = _active_owner_options()

    def _render_create(form_data):
        current_scope = _normalize_task_scope(
            form_data.get("task_scope") if hasattr(form_data, "get") else None,
            default="GLOBAL",
        )
        selected_owner_id = (
            (form_data.get("owner_id", "") if hasattr(form_data, "get") else "") or ""
        ).strip()
        local_collaborator_options = _active_task_user_options({current_user.id})
        global_collaborator_options = _active_task_user_options(
            {int(selected_owner_id)} if selected_owner_id.isdigit() else set()
        )
        collaborator_options = (
            global_collaborator_options if current_scope == "GLOBAL" else local_collaborator_options
        )
        collaborator_option_ids = {str(user.id) for user in collaborator_options}
        selected_collaborator_ids = _selected_collaborator_ids(
            form_data, collaborator_option_ids
        )
        max_collaborator_slots = min(len(collaborator_options), 10)
        recurrence_context = _recurrence_form_context(form_data)
        return render_template(
            "tasks/create.html",
            owners=owners,
            collaborator_options=collaborator_options,
            selected_collaborator_ids=selected_collaborator_ids,
            collaborator_count=_collaborator_count(
                form_data, selected_collaborator_ids, max_collaborator_slots
            ),
            max_collaborator_slots=max_collaborator_slots,
            task_statuses=TASK_STATUSES,
            task_priorities=TASK_PRIORITIES,
            task_scopes=TASK_SCOPES,
            task_schedule_modes=TASK_SCHEDULE_MODES,
            recurrence_types=RECURRENCE_TYPES,
            recurrence_weekdays=RECURRENCE_WEEKDAYS,
            can_create_global=_can_create_global_task(),
            current_scope=current_scope,
            all_offices=Office.query.filter_by(is_active=True).order_by(Office.office_name).all(),
            selected_tagged_offices=(
                form_data.getlist("tagged_offices")
                if hasattr(form_data, "getlist") else []
            ),
            self_task_visible_to_co=(
                form_data.get("self_task_visible_to_controlling_officer", "")
                if hasattr(form_data, "get") else ""
            ),
            form_data=form_data,
            **recurrence_context,
        )

    if request.method == "POST":
        task_title = request.form.get("task_title", "").strip()
        task_description = sanitize_rich_text(request.form.get("task_description", ""))
        task_origin = request.form.get("task_origin", "").strip()
        status = request.form.get("status", "").strip()
        priority = request.form.get("priority", "").strip()
        due_date_raw = request.form.get("due_date", "").strip()
        owner_id_raw = request.form.get("owner_id", "").strip()
        task_scope = _normalize_task_scope(request.form.get("task_scope"), default="GLOBAL")
        schedule_mode = _normalize_schedule_mode(
            request.form.get("schedule_mode"), default="ONE_TIME"
        )
        recurrence_type = (request.form.get("recurrence_type", "DAILY") or "").strip().upper()
        recurrence_start_date_raw = request.form.get("recurrence_start_date", "").strip()
        recurrence_end_date_raw = request.form.get("recurrence_end_date", "").strip()
        selected_recurrence_weekdays = _selected_recurrence_weekdays(request.form)
        recurrence_month_day = _parse_monthly_day(
            request.form.get("recurrence_month_day", "")
        )

        # ── New RBAC fields ──────────────────────────────────────
        self_task_visible_to_co = request.form.get(
            "self_task_visible_to_controlling_officer"
        ) == "on"
        tagged_office_ids_raw = request.form.getlist("tagged_offices")

        errors = []
        if not task_title:
            errors.append("Task title is required.")
        if len(task_title) > MAX_TASK_TITLE_LEN:
            errors.append(f"Task title cannot exceed {MAX_TASK_TITLE_LEN} characters.")
        if len(task_origin) > MAX_TASK_ORIGIN_LEN:
            errors.append(f"Task origin cannot exceed {MAX_TASK_ORIGIN_LEN} characters.")
        if len(rich_text_visible_text(task_description)) > MAX_TASK_DESC_LEN:
            errors.append(f"Task description cannot exceed {MAX_TASK_DESC_LEN} characters.")
        if status not in TASK_STATUSES:
            errors.append("Please choose a valid status.")
        if priority not in TASK_PRIORITIES:
            errors.append("Please choose a valid priority.")
        if task_scope not in TASK_SCOPES:
            errors.append("Please choose a valid task scope.")
        if schedule_mode not in TASK_SCHEDULE_MODES:
            errors.append("Please choose a valid task schedule.")

        # ── RBAC enforcement: block Users from creating GLOBAL tasks
        if task_scope == "GLOBAL" and not _can_create_global_task():
            abort(403)

        owner = None
        if task_scope == "GLOBAL":
            if not owner_id_raw:
                errors.append("Task owner is required for Global Task.")
            elif not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")
        else:
            owner = current_user

        # ── Validate tagged offices for GLOBAL tasks ─────────────
        tagged_offices = []
        if task_scope == "GLOBAL" and tagged_office_ids_raw:
            valid_ids = [
                int(oid) for oid in tagged_office_ids_raw
                if oid.strip().isdigit()
            ]
            if valid_ids:
                tagged_offices = (
                    Office.query.filter(
                        Office.id.in_(valid_ids),
                        Office.is_active.is_(True),
                    ).all()
                )
                if len(tagged_offices) != len(valid_ids):
                    errors.append(
                        "One or more selected offices are invalid or inactive."
                    )

        # self_task_visible flag only applies to self-tasks (zero collaborators)
        if self_task_visible_to_co and task_scope == "GLOBAL":
            self_task_visible_to_co = False

        due_date = None
        recurrence_start_date = None
        recurrence_end_date = None
        next_generation_date = None

        if schedule_mode == "ONE_TIME":
            due_date = _parse_due_date(due_date_raw)
            if due_date_raw and due_date is None:
                errors.append("Due date must be in YYYY-MM-DD format.")
        else:
            if recurrence_type not in RECURRENCE_TYPES:
                errors.append("Please choose a valid recurrence type.")

            recurrence_start_date = _parse_due_date(recurrence_start_date_raw)
            recurrence_end_date = _parse_due_date(recurrence_end_date_raw)

            if not recurrence_start_date_raw:
                errors.append("Start date is required for a recurring task.")
            elif recurrence_start_date is None:
                errors.append("Start date must be in YYYY-MM-DD format.")
            elif recurrence_start_date < date_type.today():
                errors.append("Start date for a recurring task cannot be in the past.")

            if recurrence_end_date_raw and recurrence_end_date is None:
                errors.append("End date must be in YYYY-MM-DD format.")
            if (
                recurrence_start_date
                and recurrence_end_date
                and recurrence_end_date < recurrence_start_date
            ):
                errors.append("End date cannot be earlier than the start date.")

            if recurrence_type == "WEEKLY" and not selected_recurrence_weekdays:
                errors.append("Select at least one weekday for a weekly recurring task.")
            if recurrence_type == "MONTHLY":
                if recurrence_month_day is None:
                    errors.append("Select a day of month for a monthly recurring task.")
                elif recurrence_month_day < 1 or recurrence_month_day > 28:
                    errors.append("Monthly recurring tasks support days 1 to 28.")
            else:
                recurrence_month_day = None

            if recurrence_type != "WEEKLY":
                selected_recurrence_weekdays = []

            if not errors and recurrence_start_date:
                try:
                    next_generation_date = first_occurrence_date(
                        recurrence_type,
                        recurrence_start_date,
                        weekly_days=selected_recurrence_weekdays,
                        monthly_day=recurrence_month_day,
                    )
                except ValueError as exc:
                    errors.append(str(exc))

        collaborator_options = _active_task_user_options(
            {owner.id} if owner and owner.id else ({current_user.id} if task_scope == "MY" else set())
        )
        collaborator_option_ids = {str(user.id) for user in collaborator_options}
        selected_collaborator_ids = _selected_collaborator_ids(
            request.form, collaborator_option_ids
        )
        max_collaborator_slots = min(len(collaborator_options), 10)
        requested_collaborator_count = _collaborator_count(
            request.form, selected_collaborator_ids, max_collaborator_slots
        )
        submitted_collaborator_ids = _submitted_collaborator_ids(request.form)
        duplicate_collaborator_ids = [
            user_id for user_id, count in Counter(submitted_collaborator_ids).items() if count > 1
        ]

        if len(selected_collaborator_ids) != len(submitted_collaborator_ids):
            errors.append("One or more selected collaborators are invalid.")
        if duplicate_collaborator_ids:
            errors.append("A collaborator cannot be selected more than once.")
        if owner and str(owner.id) in submitted_collaborator_ids:
            errors.append("Task owner cannot be added as a collaborator.")
        if len(selected_collaborator_ids) < requested_collaborator_count:
            errors.append(
                f"Select {requested_collaborator_count} collaborator"
                f"{'' if requested_collaborator_count == 1 else 's'}."
            )

        collaborator_users = []
        if selected_collaborator_ids:
            collaborators_by_id = {
                str(user.id): user
                for user in User.query
                .filter(User.id.in_([int(user_id) for user_id in selected_collaborator_ids]))
                .filter_by(is_active=True)
                .all()
            }
            if len(collaborators_by_id) != len(selected_collaborator_ids):
                errors.append("One or more selected collaborators are inactive or unavailable.")
            else:
                collaborator_users = [
                    collaborators_by_id[user_id] for user_id in selected_collaborator_ids
                ]

        if errors:
            for err in errors:
                flash(err, "danger")
            return _render_create(request.form)

        try:
            collaborator_names = ", ".join(
                user.full_name or user.username for user in collaborator_users
            ) or "none"
            scope_label = "GLOBAL" if task_scope == "GLOBAL" else "LOCAL"
            created_task = None
            if schedule_mode == "RECURRING":
                template = RecurringTaskTemplate(
                    task_title=task_title,
                    task_description=task_description or None,
                    task_origin=task_origin or None,
                    status=status,
                    priority=priority,
                    owner_id=owner.id if owner else None,
                    created_by=current_user.id,
                    office_id=current_user.office_id if current_user.office_id else None,
                    is_active=True,
                    task_scope="GLOBAL" if task_scope == "GLOBAL" else "MY",
                    recurrence_type=recurrence_type,
                    weekly_days=encode_weekday_codes(selected_recurrence_weekdays),
                    monthly_day=recurrence_month_day,
                    start_date=recurrence_start_date,
                    end_date=recurrence_end_date,
                    next_generation_date=next_generation_date,
                )
                db.session.add(template)
                db.session.flush()

                for collaborator in collaborator_users:
                    db.session.add(
                        RecurringTaskCollaborator(
                            template_id=template.id,
                            user_id=collaborator.id,
                        )
                    )

                db.session.flush()

                created_task = create_initial_task_for_template(template)

                summary = recurrence_summary(template)
                log_action(
                    action="RECURRING_TASK_TEMPLATE_CREATED",
                    user_id=current_user.id,
                    entity_type="RecurringTaskTemplate",
                    entity_id=str(template.id),
                    details=(
                        f"{'Global' if task_scope == 'GLOBAL' else 'Local'} recurring task "
                        f"'{template.task_title}' created with owner "
                        f"'{owner.full_name or owner.username if owner else '-'}', "
                        f"collaborators [{collaborator_names}], schedule '{summary}', "
                        f"start_date '{template.start_date}', end_date '{template.end_date or '-'}', "
                        f"and priority '{template.priority}'."
                    ),
                )
                log_activity(
                    current_user.username,
                    "recurring_task_created",
                    "task",
                    template.task_title,
                    details=(
                        f"schedule={summary}, scope={scope_label}, owner="
                        f"{owner.username if owner else '-'}, collaborators="
                        f"{','.join(user.username for user in collaborator_users) or 'none'}"
                    ),
                )
                if created_task is not None:
                    _notify_task_participants(
                        created_task,
                        title="New recurring task assigned",
                        message=(
                            f"{current_user.full_name or current_user.username} created "
                            f"the recurring task '{created_task.task_title}' and added you "
                            f"as an owner/collaborator."
                        ),
                        severity="info",
                        skip_user_ids={current_user.id},
                    )
            else:
                task = Task(
                    task_title=task_title,
                    task_description=task_description or None,
                    task_origin=task_origin or None,
                    status=status,
                    priority=priority,
                    due_date=due_date,
                    owner_id=owner.id if owner else None,
                    created_by=current_user.id,
                    office_id=current_user.office_id if current_user.office_id else None,
                    is_active=True,
                    task_scope="GLOBAL" if task_scope == "GLOBAL" else "MY",
                    self_task_visible_to_controlling_officer=self_task_visible_to_co,
                )
                db.session.add(task)
                db.session.flush()

                for collaborator in collaborator_users:
                    db.session.add(
                        TaskCollaborator(task_id=task.id, user_id=collaborator.id)
                    )

                # ── Persist tagged offices for GLOBAL tasks ──────
                for office in tagged_offices:
                    db.session.add(
                        TaskOffice(task_id=task.id, office_id=office.id)
                    )

                db.session.flush()

                log_action(
                    action="TASK_CREATED",
                    user_id=current_user.id,
                    entity_type="Task",
                    entity_id=str(task.id),
                    details=(
                        f"{'Global' if task_scope == 'GLOBAL' else 'Local'} task "
                        f"'{task.task_title}' created with owner "
                        f"'{owner.full_name or owner.username if owner else '-'}', "
                        f"collaborators [{collaborator_names}], status '{task.status}', "
                        f"and priority '{task.priority}'."
                    ),
                )
                log_activity(
                    current_user.username,
                    "task_created",
                    "task",
                    task.task_title,
                    details=(
                        f"priority={task.priority}, scope={scope_label}, owner="
                        f"{owner.username if owner else '-'}, collaborators="
                        f"{','.join(user.username for user in collaborator_users) or 'none'}"
                    ),
                )
                _notify_task_participants(
                    task,
                    title="New task assigned",
                    message=(
                        f"{current_user.full_name or current_user.username} created "
                        f"'{task.task_title}' and added you as an owner/collaborator."
                    ),
                    severity="info",
                    skip_user_ids={current_user.id},
                )
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not create task due to a database error.")
            return _render_create(request.form)

        if schedule_mode == "RECURRING":
            if created_task is not None:
                flash(
                    "Recurring task series created successfully. The first occurrence is now available in the task list.",
                    "success",
                )
            else:
                flash(
                    "Recurring task series created successfully.",
                    "success",
                )
            return redirect(url_for("tasks.list_tasks"))
        if task_scope == "GLOBAL":
            flash("Global task created successfully.", "success")
        elif not collaborator_users:
            flash("Local self-task created successfully.", "success")
        else:
            flash(
                f"Local task created with {len(collaborator_users)} collaborator"
                f"{'' if len(collaborator_users) == 1 else 's'}.",
                "success",
            )
        return redirect(url_for("tasks.list_tasks"))

    return _render_create({})


# ── Recurring Series Detail / Edit ───────────────────────────────
@office_bp.route("/series/<int:template_id>")
@login_required
@module_access_required("tasks")
def recurring_series_detail(template_id):
    template = RecurringTaskTemplate.query.get_or_404(template_id)
    if not _can_view_recurring_template(template):
        abort(403)

    generated_tasks = (
        template.generated_tasks
        .order_by(Task.due_date.desc(), Task.id.desc())
        .limit(12)
        .all()
    )

    return render_template(
        "tasks/series_detail.html",
        template=template,
        generated_tasks=generated_tasks,
        recurrence_summary=recurrence_summary,
        can_edit_series=_can_edit_recurring_template(template),
    )


@office_bp.route("/series/<int:template_id>/edit", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def edit_recurring_series(template_id):
    template = RecurringTaskTemplate.query.get_or_404(template_id)
    if not _can_edit_recurring_template(template):
        abort(403)

    def _render_edit_series(form_data):
        return render_template(
            "tasks/series_edit.html",
            template=template,
            form_data=form_data,
            recurrence_summary=recurrence_summary,
        )

    if request.method == "POST":
        recurrence_end_date_raw = request.form.get("recurrence_end_date", "").strip()
        recurrence_end_date = _parse_due_date(recurrence_end_date_raw)

        errors = []
        if recurrence_end_date_raw and recurrence_end_date is None:
            errors.append("End date must be in YYYY-MM-DD format.")
        if recurrence_end_date and recurrence_end_date < template.start_date:
            errors.append("End date cannot be earlier than the recurring start date.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return _render_edit_series(request.form)

        previous_end_date = template.end_date
        template.end_date = recurrence_end_date

        latest_generated_task = (
            template.generated_tasks
            .order_by(Task.occurrence_date.desc(), Task.id.desc())
            .first()
        )
        if recurrence_end_date and template.next_generation_date and template.next_generation_date > recurrence_end_date:
            template.next_generation_date = None
            template.is_active = False
        elif (
            template.next_generation_date is None
            and recurrence_end_date != previous_end_date
        ):
            after_date = (
                latest_generated_task.occurrence_date
                if latest_generated_task and latest_generated_task.occurrence_date
                else None
            )
            resumed_next_date = next_scheduled_occurrence_for_template(
                template,
                after_date=after_date,
            )
            if resumed_next_date is not None:
                template.next_generation_date = resumed_next_date
                template.is_active = True

        try:
            db.session.flush()
            log_action(
                action="RECURRING_TASK_TEMPLATE_UPDATED",
                user_id=current_user.id,
                entity_type="RecurringTaskTemplate",
                entity_id=str(template.id),
                details=(
                    f"Recurring task series '{template.task_title}' updated. "
                    f"end_date '{previous_end_date or '-'}' -> '{template.end_date or '-'}'."
                ),
            )
            log_activity(
                current_user.username,
                "recurring_task_updated",
                "task",
                template.task_title,
                details=f"end_date={template.end_date or '-'}",
            )
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not update recurring task series due to a database error.")
            return _render_edit_series(request.form)

        flash("Recurring task series updated successfully.", "success")
        return redirect(url_for("tasks.recurring_series_detail", template_id=template.id))

    return _render_edit_series({})


# ── Task Detail ───────────────────────────────────────────────────
@office_bp.route("/<int:task_id>")
@login_required
@module_access_required("tasks")
def task_detail(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_view_task(task):
        abort(403)

    updates = TaskUpdate.query.filter_by(task_id=task.id).order_by(
        TaskUpdate.created_at.desc()
    ).all()

    return render_template(
        "tasks/detail.html",
        task=task,
        updates=updates,
        can_edit=_can_edit_task(task),
        can_close=_can_close_task(task),
        can_add_update=_can_add_update(task),
        can_edit_series=_can_edit_recurring_template(task.recurring_template)
        if task.recurring_template else False,
        recurrence_summary=recurrence_summary,
    )


@office_bp.route("/<int:task_id>/summary")
@login_required
@module_access_required("tasks")
def task_summary(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_view_task(task):
        abort(403)

    updates = (
        TaskUpdate.query.filter_by(task_id=task.id)
        .order_by(TaskUpdate.created_at.desc())
        .all()
    )

    return render_template(
        "tasks/_task_summary_panel.html",
        task=task,
        updates=updates,
        can_edit=_can_edit_task(task),
        can_close=_can_close_task(task),
        can_add_update=_can_add_update(task),
        can_edit_series=_can_edit_recurring_template(task.recurring_template)
        if task.recurring_template else False,
        recurrence_summary=recurrence_summary,
        is_privileged=_is_privileged(),
    )


@office_bp.route("/<int:task_id>/delete", methods=["POST"])
@login_required
@module_access_required("tasks")
def delete_task(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_edit_task(task):
        abort(403)

    if not task.is_active:
        flash("Task is already inactive.", "info")
        return redirect(url_for("tasks.list_tasks"))

    try:
        task.is_active = False
        db.session.flush()

        log_action(
            action="TASK_DEACTIVATED",
            user_id=current_user.id,
            entity_type="Task",
            entity_id=str(task.id),
            details=(
                f"Task '{task.task_title}' was removed from the active tracker by "
                f"'{current_user.username}'."
            ),
        )
        log_activity(
            current_user.username,
            "task_deactivated",
            "task",
            task.task_title,
            details="marked inactive",
        )
        _notify_task_participants(
            task,
            title="Task archived",
            message=(
                f"{current_user.full_name or current_user.username} removed "
                f"'{task.task_title}' from the active tracker."
            ),
            severity="warning",
            skip_user_ids={current_user.id},
        )
        invalidate_dashboard_summary_metrics()
        db.session.commit()
    except SQLAlchemyError:
        _db_error("Could not remove task from the active tracker due to a database error.")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    flash("Task removed from the active tracker.", "success")
    return redirect(url_for("tasks.list_tasks"))


# ── Edit Task ─────────────────────────────────────────────────────
@office_bp.route("/<int:task_id>/edit", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def edit_task(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_edit_task(task):
        abort(403)

    owners = _active_owner_options()
    existing_collaborator_ids = [
        str(link.user_id)
        for link in task.collaborator_links
        if getattr(link, "user_id", None)
    ]

    def _render_edit(form_data):
        restore_required = _is_archived_task(task)
        current_scope = _normalize_task_scope(
            form_data.get("task_scope") if hasattr(form_data, "get") else task.task_scope,
            default=_normalize_task_scope(task.task_scope, default="MY"),
        )
        selected_owner_id = (
            (form_data.get("owner_id") if hasattr(form_data, "get") else None)
            or (str(task.owner_id) if task.owner_id else "")
        )
        selected_owner_ids = {int(selected_owner_id)} if str(selected_owner_id).isdigit() else set()
        if current_scope != "GLOBAL":
            selected_owner_ids = {int(task.owner_id)} if task.owner_id else set()

        collaborator_options = _active_task_user_options(selected_owner_ids)
        collaborator_option_ids = {str(user.id) for user in collaborator_options}
        selected_collaborator_ids = _selected_collaborator_ids(
            form_data, collaborator_option_ids
        ) if hasattr(form_data, "getlist") and form_data else existing_collaborator_ids
        max_collaborator_slots = min(len(collaborator_options), 10)

        # Tagged offices: prefer form submission, fall back to existing
        if hasattr(form_data, "getlist") and form_data:
            edit_selected_tagged = form_data.getlist("tagged_offices")
        else:
            edit_selected_tagged = [
                str(o.id) for o in getattr(task, "tagged_offices", [])
            ]

        # self_task_visible: prefer form submission, fall back to existing
        if hasattr(form_data, "get") and form_data:
            edit_self_task_vis = form_data.get(
                "self_task_visible_to_controlling_officer", ""
            )
        else:
            edit_self_task_vis = (
                "on" if task.self_task_visible_to_controlling_officer else ""
            )

        return render_template(
            "tasks/edit.html",
            task=task,
            owners=owners,
            collaborator_options=collaborator_options,
            selected_collaborator_ids=selected_collaborator_ids,
            collaborator_count=_collaborator_count(
                form_data if hasattr(form_data, "get") else {},
                selected_collaborator_ids,
                max_collaborator_slots,
            ),
            max_collaborator_slots=max_collaborator_slots,
            task_statuses=TASK_STATUSES,
            task_priorities=TASK_PRIORITIES,
            task_scopes=TASK_SCOPES,
            can_create_global=_can_create_global_task(),
            all_offices=Office.query.filter_by(is_active=True).order_by(Office.office_name).all(),
            selected_tagged_offices=edit_selected_tagged,
            self_task_visible_to_co=edit_self_task_vis,
            form_data=form_data,
            recurrence_summary=recurrence_summary,
            restore_required=restore_required,
        )

    if request.method == "POST":
        restore_required = _is_archived_task(task)
        task_title = request.form.get("task_title", "").strip()
        task_description = sanitize_rich_text(request.form.get("task_description", ""))
        task_origin = request.form.get("task_origin", "").strip()
        status = request.form.get("status", "").strip()
        priority = request.form.get("priority", "").strip()
        due_date_raw = request.form.get("due_date", "").strip()
        owner_id_raw = request.form.get("owner_id", "").strip()
        task_scope = _normalize_task_scope(
            request.form.get("task_scope", task.task_scope),
            default=_normalize_task_scope(task.task_scope, default="MY"),
        )

        # ── New RBAC fields (edit) ───────────────────────────────
        edit_self_task_visible_to_co = request.form.get(
            "self_task_visible_to_controlling_officer"
        ) == "on"
        edit_tagged_office_ids_raw = request.form.getlist("tagged_offices")

        # RBAC enforcement: block Users from switching to GLOBAL
        if task_scope == "GLOBAL" and not _can_create_global_task():
            abort(403)

        errors = []
        if not task_title:
            errors.append("Task title is required.")
        if len(task_title) > MAX_TASK_TITLE_LEN:
            errors.append(f"Task title cannot exceed {MAX_TASK_TITLE_LEN} characters.")
        if len(task_origin) > MAX_TASK_ORIGIN_LEN:
            errors.append(f"Task origin cannot exceed {MAX_TASK_ORIGIN_LEN} characters.")
        if len(rich_text_visible_text(task_description)) > MAX_TASK_DESC_LEN:
            errors.append(f"Task description cannot exceed {MAX_TASK_DESC_LEN} characters.")
        if status not in TASK_STATUSES:
            errors.append("Please choose a valid status.")
        if priority not in TASK_PRIORITIES:
            errors.append("Please choose a valid priority.")
        if task_scope not in TASK_SCOPES:
            errors.append("Please choose a valid task scope.")

        # ── Validate tagged offices for GLOBAL tasks (edit) ──
        edit_tagged_offices = []
        if task_scope == "GLOBAL" and edit_tagged_office_ids_raw:
            valid_ids = [
                int(oid) for oid in edit_tagged_office_ids_raw
                if oid.strip().isdigit()
            ]
            if valid_ids:
                edit_tagged_offices = (
                    Office.query.filter(
                        Office.id.in_(valid_ids),
                        Office.is_active.is_(True),
                    ).all()
                )
                if len(edit_tagged_offices) != len(valid_ids):
                    errors.append(
                        "One or more selected offices are invalid or inactive."
                    )

        # self_task_visible only applies to MY-scope self-tasks
        if edit_self_task_visible_to_co and task_scope == "GLOBAL":
            edit_self_task_visible_to_co = False

        due_date = _parse_due_date(due_date_raw)
        if due_date_raw and due_date is None:
            errors.append("Due date must be in YYYY-MM-DD format.")
        if restore_required and due_date is None:
            errors.append("A new due date is required to restore an archived task.")
        if restore_required and status in ("Completed", "Cancelled"):
            errors.append("Choose an active task status to restore this task.")

        owner = None
        if task_scope == "GLOBAL":
            if not owner_id_raw:
                errors.append("Task owner is required for Global Task.")
            elif not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")
        elif owner_id_raw:
            if not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")
        elif task.owner_id:
            owner = task.owner
        else:
            owner = current_user

        collaborator_options = _active_task_user_options({owner.id} if owner and owner.id else set())
        collaborator_option_ids = {str(user.id) for user in collaborator_options}
        selected_collaborator_ids = _selected_collaborator_ids(
            request.form, collaborator_option_ids
        )
        max_collaborator_slots = min(len(collaborator_options), 10)
        requested_collaborator_count = _collaborator_count(
            request.form, selected_collaborator_ids, max_collaborator_slots
        )
        submitted_collaborator_ids = _submitted_collaborator_ids(request.form)
        duplicate_collaborator_ids = [
            user_id for user_id, count in Counter(submitted_collaborator_ids).items() if count > 1
        ]
        if len(selected_collaborator_ids) != len(submitted_collaborator_ids):
            errors.append("One or more selected collaborators are invalid.")
        if duplicate_collaborator_ids:
            errors.append("A collaborator cannot be selected more than once.")
        if owner and str(owner.id) in submitted_collaborator_ids:
            errors.append("Task owner cannot be added as a collaborator.")
        if len(selected_collaborator_ids) < requested_collaborator_count:
            errors.append(
                f"Select {requested_collaborator_count} collaborator"
                f"{'' if requested_collaborator_count == 1 else 's'}."
            )

        collaborator_users = []
        if selected_collaborator_ids:
            collaborators_by_id = {
                str(user.id): user
                for user in User.query
                .filter(User.id.in_([int(user_id) for user_id in selected_collaborator_ids]))
                .filter_by(is_active=True)
                .all()
            }
            if len(collaborators_by_id) != len(selected_collaborator_ids):
                errors.append("One or more selected collaborators are inactive or unavailable.")
            else:
                collaborator_users = [
                    collaborators_by_id[user_id] for user_id in selected_collaborator_ids
                ]

        if errors:
            for err in errors:
                flash(err, "danger")
            return _render_edit(request.form)

        changed_fields = []
        previous_owner_id = task.owner_id
        new_owner_id = owner.id if owner else None
        previous_collaborator_ids = _task_collaborator_user_ids(task)
        new_collaborator_ids = {user.id for user in collaborator_users}

        if task.task_title != task_title:
            changed_fields.append(f"title '{task.task_title}' -> '{task_title}'")
            task.task_title = task_title
        if (task.task_description or "") != task_description:
            changed_fields.append("description updated")
            task.task_description = task_description or None
        if (task.task_origin or "") != task_origin:
            changed_fields.append(f"type '{task.task_origin or '-'}' -> '{task_origin or '-'}'")
            task.task_origin = task_origin or None
        if task.status != status:
            changed_fields.append(f"status '{task.status}' -> '{status}'")
            task.status = status
        if task.priority != priority:
            changed_fields.append(f"priority '{task.priority}' -> '{priority}'")
            task.priority = priority
        if task.due_date != due_date:
            changed_fields.append(f"due_date '{task.due_date or '-'}' -> '{due_date or '-'}'")
            task.due_date = due_date
        if task.owner_id != new_owner_id:
            changed_fields.append(f"owner_id '{task.owner_id or '-'}' -> '{new_owner_id or '-'}'")
            task.owner_id = new_owner_id
        if task.task_scope != task_scope:
            changed_fields.append(f"scope '{task.task_scope}' -> '{task_scope}'")
            task.task_scope = task_scope
        if previous_collaborator_ids != new_collaborator_ids:
            changed_fields.append(
                f"collaborators '{sorted(previous_collaborator_ids)}' -> '{sorted(new_collaborator_ids)}'"
            )

        # ── Track RBAC field changes ─────────────────────────────
        if task.self_task_visible_to_controlling_officer != edit_self_task_visible_to_co:
            changed_fields.append(
                f"self_task_visible_to_co '{task.self_task_visible_to_controlling_officer}'"
                f" -> '{edit_self_task_visible_to_co}'"
            )
            task.self_task_visible_to_controlling_officer = edit_self_task_visible_to_co

        prev_tagged_ids = sorted(o.id for o in getattr(task, "tagged_offices", []))
        new_tagged_ids = sorted(o.id for o in edit_tagged_offices)
        if prev_tagged_ids != new_tagged_ids:
            changed_fields.append(
                f"tagged_offices '{prev_tagged_ids}' -> '{new_tagged_ids}'"
            )

        if restore_required:
            task.is_active = True
            changed_fields.append("restored to active tracker")

        try:
            TaskCollaborator.query.filter_by(task_id=task.id).delete(synchronize_session=False)
            for collaborator in collaborator_users:
                db.session.add(TaskCollaborator(task_id=task.id, user_id=collaborator.id))

            # ── Sync tagged offices ──────────────────────────────
            TaskOffice.query.filter_by(task_id=task.id).delete(synchronize_session=False)
            for office in edit_tagged_offices:
                db.session.add(TaskOffice(task_id=task.id, office_id=office.id))
            db.session.flush()
            log_action(
                action="TASK_UPDATED",
                user_id=current_user.id,
                entity_type="Task",
                entity_id=str(task.id),
                details=(
                    f"Task '{task.task_title}' updated. "
                    f"Changes: {'; '.join(changed_fields) if changed_fields else 'no field changes'}."
                ),
            )
            if changed_fields:
                log_activity(current_user.username, "task_updated", "task",
                             task.task_title,
                             details="; ".join(changed_fields))
            if changed_fields:
                title = "Task assignment updated" if (
                    new_owner_id and previous_owner_id != new_owner_id
                ) or previous_collaborator_ids != new_collaborator_ids else "Task updated"
                if restore_required:
                    title = "Task restored"
                _notify_task_participants(
                    task,
                    title=title,
                    message=(
                        (
                            f"{current_user.full_name or current_user.username} restored "
                            f"'{task.task_title}' to the active tracker."
                        )
                        if restore_required else (
                            f"{current_user.full_name or current_user.username} updated "
                            f"'{task.task_title}'."
                        )
                    ),
                    severity="warning" if title == "Task updated" else "info",
                    skip_user_ids={current_user.id},
                )
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not update task due to a database error.")
            return _render_edit(request.form)

        flash("Task updated successfully.", "success")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    return _render_edit({})


# ── Add Task Update ───────────────────────────────────────────────
@office_bp.route("/<int:task_id>/add-update", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def add_task_update(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_add_update(task):
        abort(403)

    task_can_close = _can_close_task(task)

    if request.method == "POST":
        update_text = request.form.get("update_text", "").strip()
        new_status = request.form.get("new_status", "").strip()

        errors = []
        if not update_text:
            errors.append("Update text is required.")
        if new_status and new_status not in TASK_STATUSES:
            errors.append("Please select a valid status value.")
        if len(update_text) > MAX_TASK_UPDATE_LEN:
            errors.append(f"Update text cannot exceed {MAX_TASK_UPDATE_LEN} characters.")

        # ── RBAC: terminal status requires can_close permission ──
        if new_status in ("Completed", "Cancelled") and not task_can_close:
            errors.append(
                "You do not have permission to close this task. "
                "Only the task owner, creator, or an admin can set the status to "
                f"'{new_status}'."
            )

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "tasks/add_update.html",
                task=task,
                task_statuses=TASK_STATUSES,
                can_close=task_can_close,
                form_data=request.form,
            )

        try:
            old_status = None
            status_for_log = "status unchanged"
            saved_new_status = None
            if new_status:
                old_status = task.status
                task.status = new_status
                saved_new_status = new_status
                status_for_log = f"status '{old_status}' -> '{new_status}'"

            db.session.add(
                TaskUpdate(
                    task_id=task.id,
                    update_text=update_text,
                    old_status=old_status,
                    new_status=saved_new_status,
                    updated_by=current_user.id,
                )
            )
            db.session.flush()

            log_action(
                action="TASK_UPDATE_ADDED",
                user_id=current_user.id,
                entity_type="Task",
                entity_id=str(task.id),
                details=(
                    f"Task '{task.task_title}' update added ({status_for_log}). "
                    f"Note: {update_text[:250]}"
                ),
            )
            log_activity(current_user.username, "task_update_added", "task",
                         task.task_title, details=status_for_log)
            _notify_task_remark_stakeholders(task, update_text)
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not add task update due to a database error.")
            return render_template(
                "tasks/add_update.html",
                task=task,
                task_statuses=TASK_STATUSES,
                can_close=task_can_close,
                form_data=request.form,
            )

        flash("Task update added successfully.", "success")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    return render_template(
        "tasks/add_update.html",
        task=task,
        task_statuses=TASK_STATUSES,
        can_close=task_can_close,
        form_data={},
    )


# ── Context Processor ─────────────────────────────────────────────
# Expose permission helper functions to all templates rendered by the
# office (tasks) blueprint.  Templates can call these directly, e.g.:
#   {% if task_perms.can_view(task) %}
#   {% if task_perms.can_edit(task) %}
#   {% if task_perms.can_close(task) %}
#   {% if task_perms.can_add_update(task) %}
#   {% if task_perms.can_create_global() %}
#   {% if task_perms.is_privileged() %}

class _TaskPermissionProxy:
    """
    Lazy proxy that binds permission checks to the current request user.

    Instantiated once per request by the context processor.  Each method
    delegates to the centralized permission engine in
    ``app.core.permissions.task_permissions``.
    """

    __slots__ = ()

    @staticmethod
    def can_view(task: Task) -> bool:
        return can_view_task(current_user, task)

    @staticmethod
    def can_edit(task: Task) -> bool:
        return can_edit_task(current_user, task)

    @staticmethod
    def can_close(task: Task) -> bool:
        return can_close_task(current_user, task)

    @staticmethod
    def can_add_update(task: Task) -> bool:
        return can_add_update(current_user, task)

    @staticmethod
    def can_create_global() -> bool:
        return can_create_global_task(current_user)

    @staticmethod
    def is_privileged() -> bool:
        return _is_privileged()


_task_permission_proxy = _TaskPermissionProxy()


@office_bp.context_processor
def inject_task_permissions():
    """Make the task permission helpers available in every task template."""
    return {
        "task_perms": _task_permission_proxy,
    }
