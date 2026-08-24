from __future__ import annotations

"""Task tracker routes – Office Management module with Governance V2 visibility model."""

from collections import Counter
from datetime import datetime, date as date_type, timedelta, timezone
from functools import wraps
from flask import render_template, redirect, url_for, flash, request, abort, current_app, jsonify, session
from flask_login import login_required, current_user
from sqlalchemy import or_, and_, case, func, select
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
from app.core.services.dashboard import (
    CLOSED_TASK_STATUSES,
    PENDING_UPDATE_STATUSES,
    invalidate_dashboard_summary_metrics,
    task_visible_in_command_dashboard,
)
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
from app.core.services.task_visibility import task_visibility_query
from app.core.services.home import resolve_scope, scoped_task_query
from app.core.utils.audit import log_action
from app.core.utils.activity import log_activity
from app.core.utils.decorators import module_access_required
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
TASK_UPDATE_EDIT_WINDOW_HOURS = 12
TASK_SCHEDULE_MODES = ["ONE_TIME", "RECURRING"]


# ── Permission helpers ────────────────────────────────────────────

def _is_privileged():
    """True for super_user or admin – full task visibility and management access."""
    return current_user.is_super_user() or current_user.has_role(ADMIN_ROLE)


def _can_access_command_dashboard():
    """Read-only command dashboard access — superusers only."""
    return current_user.is_super_user()


def _can_reorder_tasks():
    """Shared task ordering is restricted to superusers."""
    return current_user.is_super_user()


def _task_read_access_required(fn):
    """Allow read access through the task module grant or the command-dashboard grant."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            flash("Please log in to access this page.", "warning")
            return redirect(url_for("auth.login"))
        if not current_user.is_active:
            flash("Your account has been deactivated.", "danger")
            return redirect(url_for("auth.login"))
        if current_user.has_module_access("tasks") or _can_access_command_dashboard():
            return fn(*args, **kwargs)
        flash(
            "You do not have access to this task workspace.",
            "danger",
        )
        abort(403)

    return wrapper


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
    if scope == "MY":
        if (
            getattr(template, "is_private_self_task", False)
            and len(_recurring_template_collaborator_user_ids(template)) == 0
        ):
            if (
                getattr(template, "self_task_visible_to_controlling_officer", False)
                and template.owner
                and template.owner.controlling_officer_id == current_user.id
            ):
                return True
            return False
        if template.office_id and current_user.office_id == template.office_id:
            return True
        if current_user.id in _recurring_template_participant_and_controller_ids(template):
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


def _can_edit_task_update(update: TaskUpdate) -> bool:
    """Only the original updater may edit, and only within the edit window."""
    if not update:
        return False
    if getattr(update, "task", None) is not None and not _can_view_task(update.task):
        return False
    return update.is_editable_by(
        current_user,
        edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
    )


def _can_create_global_task() -> bool:
    """Delegate to centralized permission engine. Superuser and Admin only."""
    return can_create_global_task(current_user)


def _is_task_selectable_user(user: User | None) -> bool:
    return bool(
        user
        and getattr(user, "is_active", False)
        and not user.is_admin_user()
    )


def _active_owner_options():
    base_query = User.query.filter_by(is_active=True)
    if current_user.office_id:
        same_office = [
            user
            for user in base_query.filter_by(office_id=current_user.office_id)
            .order_by(User.full_name, User.username)
            .all()
            if _is_task_selectable_user(user)
        ]
        other_offices = (
            base_query.filter(User.office_id != current_user.office_id)
            .order_by(User.full_name, User.username)
            .all()
        )
        other_offices = [user for user in other_offices if _is_task_selectable_user(user)]
        return same_office + other_offices
    return [
        user
        for user in base_query.order_by(User.full_name, User.username).all()
        if _is_task_selectable_user(user)
    ]


def _active_local_task_user_options(exclude_user_ids=None, office_id: int | None = None):
    """Return active users from the provided office for Local Tasks."""
    excluded_ids = {int(user_id) for user_id in (exclude_user_ids or set()) if user_id}

    target_office_id = current_user.office_id if office_id is None else office_id
    if target_office_id:
        candidates = (
            User.query.filter_by(is_active=True, office_id=target_office_id)
            .order_by(User.full_name, User.username)
            .all()
        )
    else:
        candidates = [current_user] if getattr(current_user, "is_active", False) else []

    return [
        user
        for user in candidates
        if _is_task_selectable_user(user) and int(user.id) not in excluded_ids
    ]


def _is_user_in_office(user: User | None, office_id: int | None) -> bool:
    """Return True when the user belongs to the provided office id."""
    if user is None:
        return False
    if office_id is None:
        return user.office_id is None
    return user.office_id == office_id


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


def _is_ajax_request() -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


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
    """Task query for the signed-in user (shared with the home page counts)."""
    return task_visibility_query(current_user)


def _task_dashboard_query():
    """Return the task query backing the command dashboard for the current user.

    A super user sees every task their visibility query allows; everybody else
    sees exactly what they can see anywhere else in the module.
    """
    return _task_visibility_query()


def _task_dashboard_context() -> dict[str, object]:
    if current_user.is_super_user():
        return {
            "title": "Super User Dashboard",
            "subtitle": "Global workspace mission control across all offices and task streams.",
            "eyebrow": "Task Management",
            "badge": "Global Workspace",
            "local_tasks_label": "Office Tasks",
            "local_tasks_hint": "All visible office-scoped work items across the workspace.",
        }

    office_name = getattr(getattr(current_user, "office", None), "office_name", "") or "Office"
    return {
        "title": f"Task Dashboard · {office_name}",
        "subtitle": "Your task position across the mapped workspace.",
        "eyebrow": "Task Management",
        "badge": office_name,
        "local_tasks_label": "Office Tasks",
        "local_tasks_hint": "Local office tasks visible across this office command view.",
    }


def _office_task_condition(office_id: int):
    """Tasks that belong to one office — those it owns plus the GLOBAL tasks tagged to it.

    Shared by the office navigator counts and the register's office filter, so a
    marker's number and the rows its link produces can never describe different
    sets of tasks.
    """
    tagged_task_ids_subq = (
        db.session.query(TaskOffice.task_id)
        .filter(TaskOffice.office_id == office_id)
        .subquery()
    )
    return or_(
        Task.office_id == office_id,
        and_(
            Task.task_scope == "GLOBAL",
            Task.id.in_(select(tagged_task_ids_subq.c.task_id)),
        ),
    )


def _open_task_condition():
    """Active work in a status the workspace has not closed."""
    return and_(
        Task.is_active.is_(True),
        Task.status.notin_(CLOSED_TASK_STATUSES),
    )


def _overdue_case():
    """1 when the task is past its committed date, 0 otherwise."""
    today = date_type.today()
    return case((and_(Task.due_date.isnot(None), Task.due_date < today), 1), else_=0)


def _visible_task_ids_condition():
    """Restrict a count to the tasks this user is already allowed to see.

    A super user sees the whole workspace, so their counts need no extra clause;
    everybody else counts only their own visible tasks, which keeps the map from
    reporting work the visibility model hides from them.
    """
    if current_user.is_super_user():
        return None
    visible_task_ids_subq = _task_visibility_query().with_entities(Task.id).subquery()
    return Task.id.in_(select(visible_task_ids_subq.c.id))


def _offices_with_open_tasks(office_ids: list[int] | None = None) -> list[dict[str, object]]:
    """Every active office, heaviest open load first, for the office navigator.

    An office with no open work still appears, reporting zero — the map is the
    organisation, not a list of today's busy locations. A deactivated office is
    not a place work runs from, so it is the one thing left out.
    An office's open tasks are the ones it owns unioned with the open GLOBAL
    tasks tagged to it; a task that is both owned and tagged counts once.
    Aggregated in one grouped query over that union, never one query per office.
    Pass office_ids to narrow the map to the offices a user may look at.

    DELIBERATE EXCEPTION TO THE VISIBILITY MODEL. These counts are taken over
    the whole workspace, not over the tasks the reader may open. Corporate
    Chemistry shows how the office works to everyone in it: a reader sees that
    another location is carrying late work without being able to see what that
    work is. Restricting the counts to a reader's own visible tasks would make
    every other location report zero, which is worse than showing nothing.
    Do not "fix" this back — test_office_navigator_visibility covers it.

    Tasks a user marked private are left out entirely, so a public number never
    reveals that a private task exists.
    """
    conditions = [_open_task_condition(), Task.is_private_self_task.is_(False)]
    overdue_flag = _overdue_case().label("is_overdue")

    owned_tasks = select(
        Task.office_id.label("office_id"),
        Task.id.label("task_id"),
        overdue_flag,
    ).where(*conditions, Task.office_id.isnot(None))
    tagged_tasks = (
        select(
            TaskOffice.office_id.label("office_id"),
            Task.id.label("task_id"),
            overdue_flag,
        )
        .join(Task, Task.id == TaskOffice.task_id)
        .where(*conditions, Task.task_scope == "GLOBAL")
    )
    # UNION drops the duplicate row for a task an office both owns and tags,
    # so a plain count is already a count of distinct task ids.
    office_tasks = owned_tasks.union(tagged_tasks).subquery()

    open_count = func.count(office_tasks.c.task_id)
    statement = (
        select(
            Office.id,
            Office.office_code,
            Office.office_name,
            Office.location,
            Office.secondary_location,
            Office.latitude,
            Office.longitude,
            Office.secondary_latitude,
            Office.secondary_longitude,
            open_count.label("open_count"),
            func.coalesce(func.sum(office_tasks.c.is_overdue), 0).label("overdue_count"),
        )
        # Outer join: every active office is on the map, whether or not it is
        # carrying open work. An office with none reports zero rather than
        # disappearing, so the map shows the organisation, not just today's load.
        .outerjoin(office_tasks, office_tasks.c.office_id == Office.id)
        # A deactivated office is not a place work is running from, so it drops
        # off the navigator map and the panel beside it.
        .where(Office.is_active.is_(True))
        .group_by(
            Office.id, Office.office_code, Office.office_name,
            Office.location, Office.secondary_location,
            Office.latitude, Office.longitude,
            Office.secondary_latitude, Office.secondary_longitude,
        )
        # The navigator reports status, so the locations needing attention lead;
        # open load breaks ties, then name.
        .order_by(
            func.coalesce(func.sum(office_tasks.c.is_overdue), 0).desc(),
            open_count.desc(),
            Office.office_name,
        )
    )
    if office_ids is not None:
        if not office_ids:
            return []
        statement = statement.where(Office.id.in_(office_ids))

    return [
        {
            "id": row.id,
            "office_code": row.office_code or "",
            "office_name": row.office_name,
            "location": row.location or "",
            # An office working from two places is pinned at both, so the map
            # shows where the work actually happens. Each point carries its own
            # coordinates, so an office the template has never seen still pins.
            "locations": [
                place for place in (row.location, row.secondary_location)
                if place and place.strip()
            ],
            "points": [
                point for point in (
                    {"label": (row.location or "").strip(), "lat": row.latitude, "lng": row.longitude},
                    {
                        "label": (row.secondary_location or "").strip(),
                        "lat": row.secondary_latitude,
                        "lng": row.secondary_longitude,
                    },
                )
                if point["lat"] is not None and point["lng"] is not None
            ],
            "open_count": int(row.open_count or 0),
            "overdue_count": int(row.overdue_count or 0),
        }
        for row in db.session.execute(statement).all()
    ]


def _global_task_counts() -> dict[str, int]:
    """Open and overdue counts for GLOBAL-scope work, which belongs to no one office.

    Shown beside the map rather than on it: a global task is the workspace's,
    not a location's, even when it is tagged to offices.
    """
    conditions = [_open_task_condition(), Task.task_scope == "GLOBAL"]
    visibility = _visible_task_ids_condition()
    if visibility is not None:
        conditions.append(visibility)

    row = db.session.execute(
        select(
            func.count(Task.id),
            func.coalesce(func.sum(_overdue_case()), 0),
        ).where(*conditions)
    ).one()
    return {"open_count": int(row[0] or 0), "overdue_count": int(row[1] or 0)}


REGISTER_OFFICE_SESSION_KEY = "task_register_office_id"


def _register_office_condition(office_id: int):
    """Tasks the Task Register shows when it is pointed at one office.

    The office's own tasks and the GLOBAL tasks tagged to it — the same union
    the office navigator counts with — plus the GLOBAL tasks tagged to no office
    at all. Workspace-wide work belongs on every office's register; scoping it
    away would leave it visible on none of them.
    """
    untagged_global = ~Task.id.in_(select(db.session.query(TaskOffice.task_id).subquery().c.task_id))
    return or_(
        _office_task_condition(office_id),
        and_(Task.task_scope == "GLOBAL", untagged_global),
    )


def _selectable_register_offices() -> list[Office]:
    """The offices a user may point the register at, in name order."""
    if current_user.is_super_user():
        return Office.query.filter(Office.is_active.is_(True)).order_by(Office.office_name).all()
    if current_user.office_id is None:
        return []
    office = db.session.get(Office, current_user.office_id)
    return [office] if office is not None else []


def _resolve_register_office(requested_office_id: str = "") -> Office | None:
    """Resolve the single office whose tasks the register renders.

    The register always shows one office at a time. A regular user is pinned to
    their own office and cannot switch. A super user picks any active office;
    the choice is remembered for the session so the register does not snap back
    on the next visit, and falls back to their own office, then to the first
    active office.
    """
    if not current_user.is_super_user():
        if current_user.office_id is None:
            return None
        return db.session.get(Office, current_user.office_id)

    selectable = _selectable_register_offices()
    if not selectable:
        return None
    by_id = {office.id: office for office in selectable}

    if requested_office_id.isdigit():
        chosen = by_id.get(int(requested_office_id))
        if chosen is None:
            # An inactive or deleted office reached us from a stale link.
            chosen = db.session.get(Office, int(requested_office_id))
        if chosen is not None:
            session[REGISTER_OFFICE_SESSION_KEY] = chosen.id
            return chosen

    remembered = session.get(REGISTER_OFFICE_SESSION_KEY)
    if isinstance(remembered, int) and remembered in by_id:
        return by_id[remembered]

    if current_user.office_id in by_id:
        return by_id[current_user.office_id]
    return selectable[0]


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

    if current_user.office_id is not None:
        conds.append(
            and_(
                RecurringTaskTemplate.task_scope.in_(["MY", "TEAM"]),
                RecurringTaskTemplate.office_id == current_user.office_id,
                RecurringTaskTemplate.is_private_self_task.is_(False),
            )
        )

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
                and_(
                    RecurringTaskTemplate.owner_id.in_(select(controlled_ids_subq.c.id)),
                    or_(
                        RecurringTaskTemplate.is_private_self_task.is_(False),
                        RecurringTaskTemplate.self_task_visible_to_controlling_officer.is_(True),
                    ),
                ),
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


def _build_recent_task_activity(all_tasks, limit: int = 5):
    """Summarize the latest visible event for each active task."""
    active_tasks = [task for task in all_tasks if task.is_active]
    if not active_tasks:
        return []

    latest_updates = {}
    task_ids = [task.id for task in active_tasks]
    for update in (
        TaskUpdate.query
        .filter(TaskUpdate.task_id.in_(task_ids))
        .order_by(TaskUpdate.created_at.desc())
        .all()
    ):
        if update.task_id not in latest_updates:
            latest_updates[update.task_id] = update

    items = []
    for task in active_tasks:
        created_at = task.created_at
        updated_at = task.updated_at or created_at
        latest_update = latest_updates.get(task.id)
        latest_update_at = latest_update.created_at if latest_update is not None else None
        event_at = max(
            moment
            for moment in (created_at, updated_at, latest_update_at)
            if moment is not None
        )

        if latest_update_at is not None and event_at == latest_update_at:
            actor = latest_update.updater
            event_label = "Update added"
            old_status = latest_update.old_status
            new_status = latest_update.new_status
        elif updated_at and updated_at > created_at:
            actor = task.creator
            event_label = "Task updated"
            old_status = None
            new_status = task.status
        else:
            actor = task.creator
            event_label = "Task created"
            old_status = None
            new_status = task.status

        items.append(
            {
                "task_id": task.id,
                "title": task.task_title,
                "actor_name": (
                    (actor.full_name or actor.username)
                    if actor is not None else "—"
                ),
                "event_label": event_label,
                "created_at": event_at,
                "old_status": old_status,
                "new_status": new_status,
            }
        )

    items.sort(key=lambda item: item["created_at"] or datetime.min, reverse=True)
    return items[:limit]


def _is_archived_task(task: Task) -> bool:
    return (not task.is_active) or task.status in ("Completed", "Cancelled")


def _task_display_sort_key(task: Task) -> tuple:
    """Return the shared persisted task ordering, falling back to recent-first."""
    if task.display_order is not None:
        return (0, int(task.display_order), 0.0, int(task.id or 0))
    created_ts = (
        task.created_at.astimezone(timezone.utc).timestamp()
        if getattr(task, "created_at", None) is not None
        else 0.0
    )
    return (1, 0, -created_ts, -(int(task.id or 0)))


def _order_task_collection(tasks: list[Task]) -> list[Task]:
    """Sort tasks using the shared display order visible to all users."""
    return sorted(tasks, key=_task_display_sort_key)


def _normalize_global_task_display_order() -> bool:
    """Materialize a full shared order for all tasks before any reorder action.

    Uses a lightweight column-only SELECT and bulk UPDATE to avoid loading
    full ORM objects for an unbounded table scan.
    """
    rows = (
        db.session.query(Task.id, Task.display_order, Task.created_at, Task.is_active)
        .order_by(
            Task.display_order.asc().nullslast(),
            Task.created_at.desc(),
            Task.id.desc(),
        )
        .all()
    )
    changed = False
    for index, row in enumerate(rows, start=1):
        if row.display_order != index:
            db.session.query(Task).filter(Task.id == row.id).update(
                {"display_order": index}, synchronize_session=False
            )
            changed = True
    return changed


def _next_task_display_order() -> int:
    """Return the next shared task order slot for newly created tasks."""
    max_order = db.session.query(db.func.max(Task.display_order)).scalar()
    return int(max_order or 0) + 1


def _assignable_posts():
    """Active posts, office first, offered alongside individual owners."""
    from app.models.office.office_post import OfficePost

    return (
        OfficePost.query.filter_by(is_active=True)
        .join(Office, OfficePost.office_id == Office.id)
        .order_by(Office.office_name, OfficePost.post_title)
        .all()
    )


def _resolve_assigned_post(raw_value: str):
    """The post a form selected, or None. Only active posts may be assigned."""
    from app.models.office.office_post import OfficePost

    if not raw_value or not raw_value.isdigit():
        return None
    post = db.session.get(OfficePost, int(raw_value))
    if post is None or not post.is_active:
        return None
    return post


# Focus shortcuts used by the home page tiles. Each key must select exactly the
# tasks its tile counted, so the two views never disagree.
TASK_FOCUS_KEYS = ("open", "overdue", "today", "week", "pending", "critical", "unassigned")


def _focus_filter_condition(focus: str):
    """Return the filter clause for a home-page focus shortcut."""
    today = date_type.today()
    week_end = today + timedelta(days=(6 - today.weekday()))

    if focus == "open":
        # The caller already restricts to live, unclosed work — which is exactly
        # what "open" means, so this shortcut adds no further condition.
        return Task.id.isnot(None)
    if focus == "overdue":
        return and_(Task.due_date.isnot(None), Task.due_date < today)
    if focus == "today":
        return Task.due_date == today
    if focus == "week":
        return and_(
            Task.due_date.isnot(None),
            Task.due_date >= today,
            Task.due_date <= week_end,
        )
    if focus == "pending":
        return Task.status.in_(PENDING_UPDATE_STATUSES)
    if focus == "critical":
        return Task.priority == "Critical"
    if focus == "unassigned":
        return Task.owner_id.is_(None)
    return None


def _filtered_task_query_from_request():
    """Return the register query, its filters, and the office it is scoped to.

    The register renders one office at a time, so the resolved office travels
    back with the query and the filters — every view built on this helper then
    describes the same office as the rows it shows.
    """
    status_filter = request.args.get("status", "").strip()
    priority_filter = request.args.get("priority", "").strip()
    owner_filter = request.args.get("owner", "").strip()
    office_filter = request.args.get("office", "").strip()
    scope_filter = _normalize_task_scope(request.args.get("scope", "").strip())
    focus_filter = request.args.get("focus", "").strip().lower()
    view_raw = request.args.get("view", "").strip().lower()

    if view_raw:
        # Opened from a home page tile. Reuse the home scope query verbatim so
        # the list can never disagree with the count that linked here.
        # resolve_scope downgrades any scope this user is not entitled to.
        view_filter = resolve_scope(current_user, view_raw)
        query = scoped_task_query(current_user, view_filter)
    else:
        view_filter = ""
        query = _task_visibility_query()

    if status_filter in TASK_STATUSES:
        query = query.filter(Task.status == status_filter)
    if priority_filter in TASK_PRIORITIES:
        query = query.filter(Task.priority == priority_filter)
    if owner_filter.isdigit():
        query = query.filter(Task.owner_id == int(owner_filter))
    # The register is always pointed at exactly one office — including for super
    # users, who choose which one rather than seeing every office pooled together.
    register_office = _resolve_register_office(office_filter)
    if register_office is not None:
        query = query.filter(_register_office_condition(register_office.id))
        office_filter = str(register_office.id)
    else:
        office_filter = ""
    if scope_filter in TASK_SCOPES:
        query = query.filter(_scope_filter_condition(scope_filter))
    if focus_filter in TASK_FOCUS_KEYS:
        # Every focus shortcut describes live work, so archived and closed
        # tasks drop out — matching how the home page counted them.
        query = query.filter(Task.is_active.is_(True))
        query = query.filter(Task.status.notin_(CLOSED_TASK_STATUSES))
        query = query.filter(_focus_filter_condition(focus_filter))
    else:
        focus_filter = ""

    return query, {
        "status": status_filter,
        "priority": priority_filter,
        "owner": owner_filter,
        "office": office_filter,
        "scope": scope_filter,
        "focus": focus_filter,
        "view": view_filter,
    }, register_office


# ── List Tasks ────────────────────────────────────────────────────
@office_bp.route("")
@office_bp.route("/")
@login_required
@_task_read_access_required
def list_tasks():
    query, filters, register_office = _filtered_task_query_from_request()
    all_tasks = _order_task_collection(query.all())
    active_tasks = [task for task in all_tasks if not _is_archived_task(task)]
    # Closed work splits two ways: tasks finished or cancelled while still on
    # the active tracker, and tasks lifted off it. Both collapse in the register,
    # but a user looking for a finished task should not have to guess which.
    completed_tasks = [
        task for task in all_tasks
        if _is_archived_task(task) and task.is_active
    ]
    archived_tasks = [task for task in all_tasks if not task.is_active]
    global_tasks, my_tasks = _split_tasks_for_list(active_tasks)
    is_privileged = _is_privileged()
    owners = _active_owner_options() if is_privileged else []
    register_offices = _selectable_register_offices() if current_user.is_super_user() else []

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
        register_office=register_office,
        register_offices=register_offices,
        can_switch_office=current_user.is_super_user(),
        completed_tasks=completed_tasks,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        task_scopes=TASK_SCOPES,
        filters=filters,
        archived_tasks=archived_tasks,
        task_permissions=task_permissions,
        can_create_global=_can_create_global_task(),
        is_privileged=is_privileged,
        can_reorder_tasks=_can_reorder_tasks(),
        recurrence_summary=recurrence_summary,
        today=date_type.today(),
    )


@office_bp.route("/<int:task_id>/reorder", methods=["POST"])
@login_required
@_task_read_access_required
def reorder_task(task_id: int):
    """Move a visible task up or down in the shared list order."""
    if not _can_reorder_tasks():
        abort(403)

    direction = (request.form.get("direction") or "").strip().lower()
    if direction not in {"up", "down"}:
        flash("Choose a valid reorder direction.", "danger")
        return redirect(url_for("tasks.list_tasks", **request.args.to_dict()))

    query, filters, _register_office = _filtered_task_query_from_request()
    target_task = query.filter(Task.id == task_id).first()
    if target_task is None or not _can_view_task(target_task):
        flash("Task not found in the current visible list.", "warning")
        return redirect(url_for("tasks.list_tasks", **filters))
    if _is_archived_task(target_task):
        flash("Archived tasks cannot be reordered.", "warning")
        return redirect(url_for("tasks.list_tasks", **filters))

    try:
        _normalize_global_task_display_order()
        visible_tasks = _order_task_collection([
            task for task in query.all()
            if not _is_archived_task(task)
        ])
        ordered_ids = [task.id for task in visible_tasks]
        if task_id not in ordered_ids:
            flash("Task not found in the current visible list.", "warning")
            return redirect(url_for("tasks.list_tasks", **filters))

        current_index = ordered_ids.index(task_id)
        swap_index = current_index - 1 if direction == "up" else current_index + 1
        if swap_index < 0 or swap_index >= len(visible_tasks):
            flash(
                "Task is already at the top of the current list." if direction == "up"
                else "Task is already at the bottom of the current list.",
                "info",
            )
            return redirect(url_for("tasks.list_tasks", **filters))

        # Re-fetch with row-level locks to reduce the race condition window
        # between the normalize pass and the actual swap commit.
        current_task = db.session.query(Task).filter_by(
            id=visible_tasks[current_index].id
        ).with_for_update().first()
        adjacent_task = db.session.query(Task).filter_by(
            id=visible_tasks[swap_index].id
        ).with_for_update().first()
        if current_task is None or adjacent_task is None:
            flash("Task not found in the current visible list.", "warning")
            return redirect(url_for("tasks.list_tasks", **filters))
        current_task.display_order, adjacent_task.display_order = (
            adjacent_task.display_order,
            current_task.display_order,
        )

        log_action(
            action="TASK_REORDERED",
            user_id=current_user.id,
            entity_type="Task",
            entity_id=str(current_task.id),
            details=(
                f"Task '{current_task.task_title}' moved {direction} in shared task order "
                f"by swapping with task '{adjacent_task.task_title}'."
            ),
        )
        log_activity(
            current_user.username,
            "task_reordered",
            "task",
            current_task.task_title,
            details=f"direction={direction}, swap_with={adjacent_task.task_title}",
        )
        db.session.commit()
        flash("Task order saved.", "success")
    except SQLAlchemyError:
        db.session.rollback()
        flash("Could not save the task order.", "danger")

    return redirect(url_for("tasks.list_tasks", **filters))


# ── Task Dashboard ────────────────────────────────────────────────
@office_bp.route("/dashboard")
@login_required
@_task_read_access_required
def task_dashboard():
    """Sectioned command dashboard for superusers."""
    dashboard_context = _task_dashboard_context()
    all_tasks = (
        _task_dashboard_query()
        .filter_by(is_active=True)
        .order_by(Task.created_at.desc())
        .all()
    )

    # ── Summary counts ──────────────────────────────────────────
    global_tasks, my_tasks = _split_tasks_for_dashboard(all_tasks)
    # ── Recent activity (latest event per active visible task) ───────────────
    recent_activity = _build_recent_task_activity(all_tasks, limit=5)

    # ── Location navigator ───────────────────────────────────────
    # Everyone sees the map. A super user sees every office; anybody else sees
    # only their own, counted against the tasks they are allowed to see.
    # Everyone sees the whole map: Corporate Chemistry shows how the office
    # works to everyone in it. Opening a location's register is a separate
    # question — a reader may only open their own, so the rest are shown but
    # not clickable rather than being made to look available.
    office_navigator = _offices_with_open_tasks()
    for office in office_navigator:
        office["can_open"] = bool(
            current_user.is_super_user() or office["id"] == current_user.office_id
        )
    navigator_scope = "every ONGC location"
    global_task_counts = _global_task_counts()
    show_office_navigator = bool(office_navigator) or global_task_counts["open_count"] > 0

    # ── Super-user analytics ─────────────────────────────────────
    analytics = None
    if _can_access_command_dashboard():
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

        # Every dimension the register can filter on, so each slice below has a
        # drilldown. Counted over the same all_tasks the charts describe.
        week_end = today + timedelta(days=(6 - today.weekday()))
        due_window = Counter()
        for t in all_tasks:
            if t.status in ("Completed", "Cancelled"):
                due_window["Closed"] += 1
            elif t.due_date is None:
                due_window["No date"] += 1
            elif t.due_date < today:
                due_window["Overdue"] += 1
            elif t.due_date == today:
                due_window["Due today"] += 1
            elif t.due_date <= week_end:
                due_window["Due this week"] += 1
            else:
                due_window["Later"] += 1

        # Owners and offices are filtered by id, so counts are kept keyed by id
        # and only rendered by name — two people can share a display name.
        owner_rows = Counter(
            (t.owner.id, t.owner.full_name or t.owner.username) if t.owner else (None, "Unassigned")
            for t in all_tasks
        )
        office_rows = Counter(
            (t.office.id, t.office.office_name) if t.office else (None, "No office")
            for t in all_tasks
        )
        open_tasks = sum(1 for t in all_tasks if t.status not in ("Completed", "Cancelled"))
        critical_open = sum(
            1 for t in all_tasks
            if t.priority == "Critical" and t.status not in ("Completed", "Cancelled")
        )
        unassigned_open = sum(
            1 for t in all_tasks
            if t.owner_id is None and t.status not in ("Completed", "Cancelled")
        )
        pending_count = sum(1 for t in all_tasks if t.status in PENDING_UPDATE_STATUSES)

        # Ordered link targets, aligned with the chart label order below.
        owners_for_links = [
            {"id": owner_id, "name": name, "count": count}
            for (owner_id, name), count in owner_rows.most_common(8)
            if owner_id is not None
        ]
        offices_for_links = [
            {"id": office_id, "name": name, "count": count}
            for (office_id, name), count in office_rows.most_common()
            if office_id is not None
        ]

        analytics = {
            "status":         status_counts,
            "priority":       priority_counts,
            "scope":          scope_counts,
            "due_window":     dict(due_window),
            "overdue":        overdue_count,
            "total":          total,
            "open":           open_tasks,
            "completed":      completed,
            "completion_pct": completion_pct,
            "critical_open":  critical_open,
            "unassigned":     unassigned_open,
            "pending":        pending_count,
        }
    else:
        owners_for_links = []
        offices_for_links = []

    return render_template(
        "tasks/dashboard.html",
        dashboard_context=dashboard_context,
        global_tasks=global_tasks,
        my_tasks=my_tasks,
        recent_activity=recent_activity,
        is_privileged=_can_access_command_dashboard(),
        show_office_navigator=show_office_navigator,
        office_navigator=office_navigator,
        navigator_scope=navigator_scope,
        global_task_counts=global_task_counts,
        show_task_actions=current_user.has_module_access("tasks"),
        analytics=analytics,
        owners_for_links=owners_for_links,
        offices_for_links=offices_for_links,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        calendar_initial={"year": date_type.today().year, "month": date_type.today().month},
    )


@office_bp.route("/calendar-data")
@login_required
@_task_read_access_required
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
        _task_dashboard_query()
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

    # Work that is already late travels with every month, because looking at a
    # day ahead should still show what is outstanding from before it. Overdue is
    # measured against today, not against the day being viewed: a task due later
    # this week has not been missed yet.
    today = date_type.today()
    overdue_rows = (
        _task_dashboard_query()
        .filter(
            Task.is_active.is_(True),
            Task.due_date.isnot(None),
            Task.due_date < today,
            Task.status.notin_(CLOSED_TASK_STATUSES),
        )
        .order_by(Task.due_date.asc(), Task.id.asc())
        .all()
    )
    overdue = [
        {
            "id": int(t.id),
            "template_id": int(t.recurring_template_id) if t.recurring_template_id else None,
            "title": t.task_title or "",
            "scope": _normalize_task_scope(t.task_scope),
            "due_date": t.due_date.strftime("%Y-%m-%d"),
            "status": t.status or "Not Started",
            "is_projected": False,
        }
        for t in overdue_rows
    ]

    return jsonify({"year": year, "month": month, "items": items, "overdue": overdue})


# ── Create Task ───────────────────────────────────────────────────
@office_bp.route("/create", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def create_task():
    owners = _active_owner_options()

    def _render_create(form_data):
        can_assign_local_owner = _is_privileged()
        current_scope = _normalize_task_scope(
            form_data.get("task_scope") if hasattr(form_data, "get") else None,
            default="GLOBAL",
        )
        collaborator_option_ids = {str(user.id) for user in owners}
        selected_collaborator_ids = _selected_collaborator_ids(
            form_data, collaborator_option_ids
        )
        max_collaborator_slots = min(len(owners), 10)
        recurrence_context = _recurrence_form_context(form_data)
        return render_template(
            "tasks/create.html",
            owners=owners,
            posts=_assignable_posts(),
            collaborator_options=owners,
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
            can_assign_local_owner=can_assign_local_owner,
            current_office_id=current_user.office_id,
            current_scope=current_scope,
            all_offices=Office.query.filter_by(is_active=True).order_by(Office.office_name).all(),
            selected_tagged_offices=(
                form_data.getlist("tagged_offices")
                if hasattr(form_data, "getlist") else []
            ),
            is_private_self_task=(
                form_data.get("is_private_self_task", "")
                if hasattr(form_data, "get") else ""
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
        assigned_post = _resolve_assigned_post(request.form.get("assigned_post_id", "").strip())
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
        is_private_self_task = request.form.get("is_private_self_task") == "on"
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
                elif owner.is_admin_user():
                    errors.append("Admin users cannot be selected as task owners.")
        elif owner_id_raw:
            if not _is_privileged():
                errors.append("Only admin or superuser can assign another owner on a Local Task.")
            elif not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")
                elif owner.is_admin_user():
                    errors.append("Admin users cannot be selected as task owners.")
        else:
            owner = current_user

        local_office_id = current_user.office_id
        if task_scope != "GLOBAL" and not _is_user_in_office(owner, local_office_id):
            errors.append("Local Task owner must belong to your office.")

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

        collaborator_options = (
            _active_task_user_options({owner.id} if owner and owner.id else set())
            if task_scope == "GLOBAL"
            else _active_local_task_user_options(
                {owner.id} if owner and owner.id else set(),
                office_id=local_office_id,
            )
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
                if _is_task_selectable_user(user)
            }
            if len(collaborators_by_id) != len(selected_collaborator_ids):
                errors.append("One or more selected collaborators are inactive, unavailable, or not eligible for task assignment.")
            else:
                collaborator_users = [
                    collaborators_by_id[user_id] for user_id in selected_collaborator_ids
                ]

        if task_scope != "GLOBAL":
            invalid_office_collaborators = [
                user.full_name or user.username
                for user in collaborator_users
                if not _is_user_in_office(user, local_office_id)
            ]
            if invalid_office_collaborators:
                errors.append("Local Task collaborators must belong to your office.")

        if task_scope == "GLOBAL" or collaborator_users:
            is_private_self_task = False
            self_task_visible_to_co = False
        elif not is_private_self_task:
            self_task_visible_to_co = False

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
                    office_id=local_office_id,
                    is_active=True,
                    task_scope="GLOBAL" if task_scope == "GLOBAL" else "MY",
                    is_private_self_task=is_private_self_task,
                    self_task_visible_to_controlling_officer=self_task_visible_to_co,
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
                if created_task is not None and created_task.display_order is None:
                    created_task.display_order = _next_task_display_order()

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
            else:
                task = Task(
                    task_title=task_title,
                    task_description=task_description or None,
                    task_origin=task_origin or None,
                    status=status,
                    priority=priority,
                    display_order=_next_task_display_order(),
                    due_date=due_date,
                    # A post-assigned task owns through its current holder, so
                    # visibility and permissions keep working unchanged.
                    owner_id=(assigned_post.holder_user_id if assigned_post else (owner.id if owner else None)),
                    assigned_post_id=assigned_post.id if assigned_post else None,
                    created_by=current_user.id,
                    office_id=local_office_id if task_scope != "GLOBAL" else (current_user.office_id if current_user.office_id else None),
                    is_active=True,
                    task_scope="GLOBAL" if task_scope == "GLOBAL" else "MY",
                    is_private_self_task=is_private_self_task,
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
@_task_read_access_required
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
@_task_read_access_required
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
        task_update_edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
        can_edit_series=_can_edit_recurring_template(task.recurring_template)
        if task.recurring_template else False,
        recurrence_summary=recurrence_summary,
    )


@office_bp.route("/<int:task_id>/summary")
@login_required
@_task_read_access_required
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
        task_update_edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
        can_edit_series=_can_edit_recurring_template(task.recurring_template)
        if task.recurring_template else False,
        recurrence_summary=recurrence_summary,
        is_privileged=_is_privileged(),
        allow_full_page=True,
        show_manage_actions=True,
    )


@office_bp.route("/<int:task_id>/command-summary")
@login_required
def task_command_summary(task_id):
    if not _can_access_command_dashboard():
        abort(403)

    task = Task.query.get_or_404(task_id)
    if not task_visible_in_command_dashboard(current_user, task_id):
        abort(403)

    updates = (
        TaskUpdate.query.filter_by(task_id=task.id)
        .order_by(TaskUpdate.created_at.desc())
        .all()
    )
    can_open_full_page = _can_view_task(task)

    return render_template(
        "tasks/_task_summary_panel.html",
        task=task,
        updates=updates,
        can_edit=_can_edit_task(task) if can_open_full_page else False,
        can_close=_can_close_task(task) if can_open_full_page else False,
        can_add_update=_can_add_update(task) if can_open_full_page else False,
        task_update_edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
        can_edit_series=(
            _can_edit_recurring_template(task.recurring_template)
            if task.recurring_template and can_open_full_page else False
        ),
        recurrence_summary=recurrence_summary,
        is_privileged=_is_privileged(),
        allow_full_page=can_open_full_page,
        show_manage_actions=can_open_full_page,
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
        collaborator_option_ids = {str(user.id) for user in owners}
        selected_collaborator_ids = _selected_collaborator_ids(
            form_data, collaborator_option_ids
        ) if hasattr(form_data, "getlist") and form_data else existing_collaborator_ids
        max_collaborator_slots = min(len(owners), 10)

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
            posts=_assignable_posts(),
            collaborator_options=owners,
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
            current_office_id=task.office_id,
            all_offices=Office.query.filter_by(is_active=True).order_by(Office.office_name).all(),
            selected_tagged_offices=edit_selected_tagged,
            is_private_self_task=(
                form_data.get("is_private_self_task", "")
                if hasattr(form_data, "get") and form_data
                else ("on" if task.is_private_self_task else "")
            ),
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
        edit_assigned_post = _resolve_assigned_post(request.form.get("assigned_post_id", "").strip())
        task_scope = _normalize_task_scope(
            request.form.get("task_scope", task.task_scope),
            default=_normalize_task_scope(task.task_scope, default="MY"),
        )

        # ── New RBAC fields (edit) ───────────────────────────────
        edit_is_private_self_task = request.form.get("is_private_self_task") == "on"
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
                elif owner.is_admin_user():
                    errors.append("Admin users cannot be selected as task owners.")
        elif owner_id_raw:
            if not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")
                elif owner.is_admin_user():
                    errors.append("Admin users cannot be selected as task owners.")
        elif task.owner_id:
            owner = task.owner
        else:
            owner = current_user

        local_office_id = (
            owner.office_id
            if task_scope != "GLOBAL" and owner and owner.office_id is not None
            else (task.office_id if task_scope != "GLOBAL" else task.office_id)
        )
        if task_scope != "GLOBAL" and not _is_user_in_office(owner, local_office_id):
            errors.append("Local Task owner must belong to your office.")

        collaborator_options = (
            _active_task_user_options({owner.id} if owner and owner.id else set())
            if task_scope == "GLOBAL"
            else _active_local_task_user_options(
                {owner.id} if owner and owner.id else set(),
                office_id=local_office_id,
            )
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
                if _is_task_selectable_user(user)
            }
            if len(collaborators_by_id) != len(selected_collaborator_ids):
                errors.append("One or more selected collaborators are inactive, unavailable, or not eligible for task assignment.")
            else:
                collaborator_users = [
                    collaborators_by_id[user_id] for user_id in selected_collaborator_ids
                ]

        if task_scope != "GLOBAL":
            invalid_office_collaborators = [
                user.full_name or user.username
                for user in collaborator_users
                if not _is_user_in_office(user, local_office_id)
            ]
            if invalid_office_collaborators:
                errors.append("Local Task collaborators must belong to your office.")

        if task_scope == "GLOBAL" or collaborator_users:
            edit_is_private_self_task = False
            edit_self_task_visible_to_co = False
        elif not edit_is_private_self_task:
            edit_self_task_visible_to_co = False

        if errors:
            for err in errors:
                flash(err, "danger")
            return _render_edit(request.form)

        changed_fields = []
        previous_owner_id = task.owner_id
        task.assigned_post_id = edit_assigned_post.id if edit_assigned_post else None
        new_owner_id = (
            edit_assigned_post.holder_user_id if edit_assigned_post
            else (owner.id if owner else None)
        )
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
        if task.office_id != local_office_id and task_scope != "GLOBAL":
            changed_fields.append(f"office_id '{task.office_id or '-'}' -> '{local_office_id or '-'}'")
            task.office_id = local_office_id
        if task.is_private_self_task != edit_is_private_self_task:
            changed_fields.append(
                f"is_private_self_task '{task.is_private_self_task}' -> '{edit_is_private_self_task}'"
            )
            task.is_private_self_task = edit_is_private_self_task
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
            if _is_ajax_request():
                return jsonify({"ok": False, "errors": errors}), 400
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
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            if _is_ajax_request():
                return jsonify(
                    {
                        "ok": False,
                        "errors": ["Could not add task update due to a database error."],
                    }
                ), 500
            _db_error("Could not add task update due to a database error.")
            return render_template(
                "tasks/add_update.html",
                task=task,
                task_statuses=TASK_STATUSES,
                can_close=task_can_close,
                form_data=request.form,
            )

        if _is_ajax_request():
            return jsonify(
                {
                    "ok": True,
                    "message": "Task update added successfully.",
                    "task_id": task.id,
                    "task_status": task.status,
                }
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


@office_bp.route("/<int:task_id>/updates/<int:update_id>/edit", methods=["GET", "POST"])
@login_required
@_task_read_access_required
def edit_task_update(task_id, update_id):
    task = Task.query.get_or_404(task_id)
    if not _can_view_task(task):
        abort(403)

    update = TaskUpdate.query.filter_by(id=update_id, task_id=task.id).first_or_404()
    if not _can_edit_task_update(update):
        flash(
            f"Only the original updater can edit an entry, and only within "
            f"{TASK_UPDATE_EDIT_WINDOW_HOURS} hours of posting.",
            "danger",
        )
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    if request.method == "POST":
        update_text = request.form.get("update_text", "").strip()
        errors = []

        if not update_text:
            errors.append("Update text is required.")
        if len(update_text) > MAX_TASK_UPDATE_LEN:
            errors.append(f"Update text cannot exceed {MAX_TASK_UPDATE_LEN} characters.")
        if not update.is_within_edit_window(TASK_UPDATE_EDIT_WINDOW_HOURS):
            errors.append(
                f"This update can no longer be edited because the "
                f"{TASK_UPDATE_EDIT_WINDOW_HOURS}-hour edit window has closed."
            )

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "tasks/edit_update.html",
                task=task,
                update=update,
                form_data=request.form,
                task_update_edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
            )

        try:
            update.update_text = update_text
            update.edited_at = datetime.now(timezone.utc)
            update.edited_by = current_user.id
            db.session.flush()

            log_action(
                action="TASK_UPDATE_EDITED",
                user_id=current_user.id,
                entity_type="TaskUpdate",
                entity_id=str(update.id),
                details=(
                    f"Task '{task.task_title}' update edited. "
                    f"Note: {update_text[:250]}"
                ),
            )
            log_activity(
                current_user.username,
                "task_update_edited",
                "task",
                task.task_title,
                details=f"update_id={update.id}",
            )
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not edit task update due to a database error.")
            return render_template(
                "tasks/edit_update.html",
                task=task,
                update=update,
                form_data=request.form,
                task_update_edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
            )

        flash("Task update edited successfully.", "success")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    return render_template(
        "tasks/edit_update.html",
        task=task,
        update=update,
        form_data={},
        task_update_edit_window_hours=TASK_UPDATE_EDIT_WINDOW_HOURS,
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
