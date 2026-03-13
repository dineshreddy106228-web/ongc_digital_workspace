"""Task tracker routes – Office Management module with Governance V2 visibility model."""

from collections import Counter
from datetime import datetime, date as date_type
from flask import render_template, redirect, url_for, flash, request, abort, current_app, jsonify
from flask_login import login_required, current_user
from sqlalchemy import or_, and_, select
from sqlalchemy.exc import SQLAlchemyError
from app.tasks import tasks_bp
from app.extensions import db
from app.models.user import User
from app.models.task import Task, TASK_SCOPES
from app.models.task_update import TaskUpdate
from app.services.dashboard import invalidate_dashboard_summary_metrics
from app.services.notifications import create_notification
from app.utils.audit import log_action
from app.utils.activity import log_activity
from app.utils.decorators import module_access_required


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


# ── Permission helpers ────────────────────────────────────────────

def _is_privileged():
    """True for super_user – full task visibility and management access."""
    return current_user.is_super_user()


def _can_view_task(task: Task) -> bool:
    if _is_privileged():
        return True
    # Task owner can always view their own task
    if task.owner_id == current_user.id:
        return True
    # GLOBAL tasks – visible to all tasks-module users (already filtered in query)
    if task.task_scope == "GLOBAL":
        return True
    # MY tasks – controlling officer can view subordinate's tasks
    if task.task_scope == "MY" and task.owner:
        if task.owner.controlling_officer_id == current_user.id:
            return True
    # TEAM tasks – reviewing officer can view
    if task.task_scope in ("TEAM", "MY") and task.owner:
        if task.owner.reviewing_officer_id == current_user.id:
            return True
    return False


def _can_edit_task(task: Task) -> bool:
    """Editing rights: privileged roles OR task owner."""
    if _is_privileged():
        return True
    return task.owner_id == current_user.id


def _can_update_status(task: Task) -> bool:
    if _is_privileged():
        return True
    return task.owner_id == current_user.id


def _can_add_update(task: Task) -> bool:
    if _is_privileged():
        return True
    if task.owner_id == current_user.id:
        return True
    # Controlling officer can add remarks on subordinate tasks
    if task.owner and task.owner.controlling_officer_id == current_user.id:
        return True
    return False


def _can_create_global_task() -> bool:
    """Only privileged roles can create GLOBAL-scope tasks."""
    return _is_privileged()


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


def _task_visibility_query():
    """
    Build a Task query that respects the Governance V2 visibility model:

    Privileged (super_user):
        → all tasks

    Standard users:
        → GLOBAL tasks (task_scope == 'GLOBAL')
        → MY tasks where owner_id == current_user.id
        → MY tasks of users whose controlling_officer_id == current_user.id
        → TEAM (and MY) tasks of users whose reviewing_officer_id == current_user.id
    """
    base = Task.query

    # Privileged roles see everything
    if _is_privileged():
        return base

    conds = []

    # 1. All GLOBAL tasks
    conds.append(Task.task_scope == "GLOBAL")

    # 2. Tasks directly assigned to me (any scope)
    conds.append(Task.owner_id == current_user.id)

    # 3. MY-scope tasks for users I control (subquery avoids large Python lists)
    controlled_ids_subq = (
        db.session.query(User.id)
        .filter_by(controlling_officer_id=current_user.id, is_active=True)
        .subquery()
    )
    conds.append(
        and_(
            Task.task_scope == "MY",
            Task.owner_id.in_(select(controlled_ids_subq.c.id)),
        )
    )

    # 4. TEAM-scope (and MY-scope) tasks for users I review
    reviewed_ids_subq = (
        db.session.query(User.id)
        .filter_by(reviewing_officer_id=current_user.id, is_active=True)
        .subquery()
    )
    conds.append(
        and_(
            Task.task_scope.in_(["TEAM", "MY"]),
            Task.owner_id.in_(select(reviewed_ids_subq.c.id)),
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
    team_tasks = []
    uid = current_user.id

    for task in all_tasks:
        if task.task_scope == "GLOBAL":
            global_tasks.append(task)
        elif task.task_scope == "MY" and task.owner_id == uid:
            my_tasks.append(task)
        else:
            team_tasks.append(task)

    return global_tasks, my_tasks, team_tasks


def _split_tasks_for_dashboard(all_tasks):
    """Dashboard grouping keeps all personally assigned tasks in 'My Tasks'."""
    global_tasks = []
    my_tasks = []
    team_tasks = []
    uid = current_user.id

    for task in all_tasks:
        if task.task_scope == "GLOBAL":
            global_tasks.append(task)
        elif task.owner_id == uid:
            my_tasks.append(task)
        else:
            team_tasks.append(task)

    return global_tasks, my_tasks, team_tasks


# ── List Tasks ────────────────────────────────────────────────────
@tasks_bp.route("")
@tasks_bp.route("/")
@login_required
@module_access_required("tasks")
def list_tasks():
    status_filter = request.args.get("status", "").strip()
    priority_filter = request.args.get("priority", "").strip()
    owner_filter = request.args.get("owner", "").strip()
    scope_filter = request.args.get("scope", "").strip()

    query = _task_visibility_query()

    if status_filter in TASK_STATUSES:
        query = query.filter(Task.status == status_filter)
    if priority_filter in TASK_PRIORITIES:
        query = query.filter(Task.priority == priority_filter)
    if owner_filter.isdigit():
        query = query.filter(Task.owner_id == int(owner_filter))
    if scope_filter in TASK_SCOPES:
        query = query.filter(Task.task_scope == scope_filter)

    all_tasks = query.order_by(Task.created_at.desc()).all()
    global_tasks, my_tasks, team_tasks = _split_tasks_for_list(all_tasks)
    is_privileged = _is_privileged()
    owners = _active_owner_options() if is_privileged else []

    task_permissions = {
        task.id: {
            "can_edit": _can_edit_task(task),
            "can_update_status": _can_update_status(task),
            "can_add_update": _can_add_update(task),
        }
        for task in all_tasks
    }

    return render_template(
        "tasks/list.html",
        tasks=all_tasks,
        global_tasks=global_tasks,
        my_tasks=my_tasks,
        team_tasks=team_tasks,
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
        task_permissions=task_permissions,
        can_create_global=_can_create_global_task(),
        is_privileged=is_privileged,
    )


# ── Task Dashboard ────────────────────────────────────────────────
@tasks_bp.route("/dashboard")
@login_required
@module_access_required("tasks")
def task_dashboard():
    """Sectioned summary + calendar view. Super users also get analytics."""
    all_tasks = (
        _task_visibility_query()
        .filter_by(is_active=True)
        .order_by(Task.created_at.desc())
        .all()
    )

    # ── Summary counts ──────────────────────────────────────────
    global_tasks, my_tasks, team_tasks = _split_tasks_for_dashboard(all_tasks)
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
            "scope": t.task_scope,
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
        scope_counts    = dict(Counter(t.task_scope   for t in all_tasks))
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
        team_tasks=team_tasks,
        recent_updates=recent_updates,
        due_soon_tasks=due_soon_tasks,
        is_privileged=_is_privileged(),
        analytics=analytics,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        calendar_initial={"year": date_type.today().year, "month": date_type.today().month},
    )


@tasks_bp.route("/calendar-data")
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
            "title": t.task_title or "",
            "scope": t.task_scope or "MY",
            "due_date": t.due_date.strftime("%Y-%m-%d"),
            "status": t.status or "Not Started",
        }
        for t in rows
        if t.due_date
    ]
    return jsonify({"year": year, "month": month, "items": items})


# ── Create Task ───────────────────────────────────────────────────
@tasks_bp.route("/create", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def create_task():
    owners = _active_owner_options()

    if request.method == "POST":
        task_title = request.form.get("task_title", "").strip()
        task_description = request.form.get("task_description", "").strip()
        task_origin = request.form.get("task_origin", "").strip()
        status = request.form.get("status", "").strip()
        priority = request.form.get("priority", "").strip()
        due_date_raw = request.form.get("due_date", "").strip()
        owner_id_raw = request.form.get("owner_id", "").strip()
        task_scope = request.form.get("task_scope", "MY").strip()

        errors = []
        if not task_title:
            errors.append("Task title is required.")
        if len(task_title) > MAX_TASK_TITLE_LEN:
            errors.append(f"Task title cannot exceed {MAX_TASK_TITLE_LEN} characters.")
        if len(task_origin) > MAX_TASK_ORIGIN_LEN:
            errors.append(f"Task origin cannot exceed {MAX_TASK_ORIGIN_LEN} characters.")
        if len(task_description) > MAX_TASK_DESC_LEN:
            errors.append(f"Task description cannot exceed {MAX_TASK_DESC_LEN} characters.")
        if status not in TASK_STATUSES:
            errors.append("Please choose a valid status.")
        if priority not in TASK_PRIORITIES:
            errors.append("Please choose a valid priority.")
        if task_scope not in TASK_SCOPES:
            errors.append("Please choose a valid task scope.")

        # Enforce: only privileged users can create GLOBAL tasks
        if task_scope == "GLOBAL" and not _can_create_global_task():
            errors.append("You do not have permission to create Global-scope tasks.")
            task_scope = "MY"

        due_date = _parse_due_date(due_date_raw)
        if due_date_raw and due_date is None:
            errors.append("Due date must be in YYYY-MM-DD format.")

        owner = None
        if owner_id_raw:
            if not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "tasks/create.html",
                owners=owners,
                task_statuses=TASK_STATUSES,
                task_priorities=TASK_PRIORITIES,
                task_scopes=TASK_SCOPES,
                can_create_global=_can_create_global_task(),
                form_data=request.form,
            )

        try:
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
                task_scope=task_scope,
            )
            db.session.add(task)
            db.session.flush()

            log_action(
                action="TASK_CREATED",
                user_id=current_user.id,
                entity_type="Task",
                entity_id=str(task.id),
                details=(
                    f"Task '{task.task_title}' created with status '{task.status}', "
                    f"priority '{task.priority}', scope '{task.task_scope}'."
                ),
            )
            log_activity(current_user.username, "task_created", "task",
                         task.task_title,
                         details=f"priority={task.priority}, scope={task.task_scope}")
            if owner:
                _notify_task_owner(
                    task,
                    title="New task assigned",
                    message=(
                        f"{current_user.full_name or current_user.username} assigned "
                        f"'{task.task_title}' to you."
                    ),
                    severity="info",
                    skip_user_id=current_user.id,
                )
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not create task due to a database error.")
            return render_template(
                "tasks/create.html",
                owners=owners,
                task_statuses=TASK_STATUSES,
                task_priorities=TASK_PRIORITIES,
                task_scopes=TASK_SCOPES,
                can_create_global=_can_create_global_task(),
                form_data=request.form,
            )

        flash("Task created successfully.", "success")
        return redirect(url_for("tasks.list_tasks"))

    return render_template(
        "tasks/create.html",
        owners=owners,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        task_scopes=TASK_SCOPES,
        can_create_global=_can_create_global_task(),
        form_data={},
    )


# ── Task Detail ───────────────────────────────────────────────────
@tasks_bp.route("/<int:task_id>")
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
        can_update_status=_can_update_status(task),
        can_add_update=_can_add_update(task),
    )


# ── Edit Task ─────────────────────────────────────────────────────
@tasks_bp.route("/<int:task_id>/edit", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def edit_task(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_edit_task(task):
        abort(403)

    owners = _active_owner_options()

    if request.method == "POST":
        task_title = request.form.get("task_title", "").strip()
        task_description = request.form.get("task_description", "").strip()
        task_origin = request.form.get("task_origin", "").strip()
        status = request.form.get("status", "").strip()
        priority = request.form.get("priority", "").strip()
        due_date_raw = request.form.get("due_date", "").strip()
        owner_id_raw = request.form.get("owner_id", "").strip()
        is_active = request.form.get("is_active") == "on"
        task_scope = request.form.get("task_scope", task.task_scope).strip()

        errors = []
        if not task_title:
            errors.append("Task title is required.")
        if len(task_title) > MAX_TASK_TITLE_LEN:
            errors.append(f"Task title cannot exceed {MAX_TASK_TITLE_LEN} characters.")
        if len(task_origin) > MAX_TASK_ORIGIN_LEN:
            errors.append(f"Task origin cannot exceed {MAX_TASK_ORIGIN_LEN} characters.")
        if len(task_description) > MAX_TASK_DESC_LEN:
            errors.append(f"Task description cannot exceed {MAX_TASK_DESC_LEN} characters.")
        if status not in TASK_STATUSES:
            errors.append("Please choose a valid status.")
        if priority not in TASK_PRIORITIES:
            errors.append("Please choose a valid priority.")
        if task_scope not in TASK_SCOPES:
            errors.append("Please choose a valid task scope.")

        if task_scope == "GLOBAL" and not _can_create_global_task():
            errors.append("You do not have permission to set a Global scope.")
            task_scope = task.task_scope

        due_date = _parse_due_date(due_date_raw)
        if due_date_raw and due_date is None:
            errors.append("Due date must be in YYYY-MM-DD format.")

        owner = None
        if owner_id_raw:
            if not owner_id_raw.isdigit():
                errors.append("Selected owner is invalid.")
            else:
                owner = User.query.filter_by(id=int(owner_id_raw), is_active=True).first()
                if owner is None:
                    errors.append("Selected owner was not found or is inactive.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "tasks/edit.html",
                task=task,
                owners=owners,
                task_statuses=TASK_STATUSES,
                task_priorities=TASK_PRIORITIES,
                task_scopes=TASK_SCOPES,
                can_create_global=_can_create_global_task(),
                form_data=request.form,
            )

        changed_fields = []
        previous_owner_id = task.owner_id
        new_owner_id = owner.id if owner else None

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
        if task.is_active != is_active:
            changed_fields.append(f"is_active '{task.is_active}' -> '{is_active}'")
            task.is_active = is_active
        if task.task_scope != task_scope:
            changed_fields.append(f"scope '{task.task_scope}' -> '{task_scope}'")
            task.task_scope = task_scope

        try:
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
            if new_owner_id and previous_owner_id != new_owner_id:
                _notify_task_owner(
                    task,
                    title="Task assignment updated",
                    message=(
                        f"{current_user.full_name or current_user.username} assigned "
                        f"'{task.task_title}' to you."
                    ),
                    severity="info",
                    skip_user_id=current_user.id,
                )
            elif changed_fields:
                _notify_task_owner(
                    task,
                    title="Task updated",
                    message=(
                        f"{current_user.full_name or current_user.username} updated "
                        f"'{task.task_title}'."
                    ),
                    severity="warning",
                    skip_user_id=current_user.id,
                )
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not update task due to a database error.")
            return render_template(
                "tasks/edit.html",
                task=task,
                owners=owners,
                task_statuses=TASK_STATUSES,
                task_priorities=TASK_PRIORITIES,
                task_scopes=TASK_SCOPES,
                can_create_global=_can_create_global_task(),
                form_data=request.form,
            )

        flash("Task updated successfully.", "success")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    return render_template(
        "tasks/edit.html",
        task=task,
        owners=owners,
        task_statuses=TASK_STATUSES,
        task_priorities=TASK_PRIORITIES,
        task_scopes=TASK_SCOPES,
        can_create_global=_can_create_global_task(),
        form_data={},
    )


# ── Update Status ─────────────────────────────────────────────────
@tasks_bp.route("/<int:task_id>/update-status", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def update_task_status(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_update_status(task):
        abort(403)

    if request.method == "POST":
        new_status = request.form.get("new_status", "").strip()
        update_text = request.form.get("update_text", "").strip()

        errors = []
        if new_status not in TASK_STATUSES:
            errors.append("Please select a valid new status.")
        if not update_text:
            errors.append("Remarks are required.")
        if len(update_text) > MAX_TASK_UPDATE_LEN:
            errors.append(f"Remarks cannot exceed {MAX_TASK_UPDATE_LEN} characters.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "tasks/update_status.html",
                task=task,
                task_statuses=TASK_STATUSES,
                form_data=request.form,
            )

        try:
            old_status = task.status
            task.status = new_status
            db.session.add(
                TaskUpdate(
                    task_id=task.id,
                    update_text=update_text,
                    old_status=old_status,
                    new_status=new_status,
                    updated_by=current_user.id,
                )
            )

            db.session.flush()
            log_action(
                action="TASK_STATUS_UPDATED",
                user_id=current_user.id,
                entity_type="Task",
                entity_id=str(task.id),
                details=(
                    f"Task '{task.task_title}' status changed from '{old_status}' "
                    f"to '{new_status}'. Remarks: {update_text[:250]}"
                ),
            )
            log_activity(current_user.username, "task_status_changed", "task",
                         task.task_title,
                         details=f"{old_status} → {new_status}")
            _notify_task_owner(
                task,
                title="Task status changed",
                message=(
                    f"{current_user.full_name or current_user.username} changed "
                    f"'{task.task_title}' from '{old_status}' to '{new_status}'."
                ),
                severity="warning",
                skip_user_id=current_user.id,
            )
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not update task status due to a database error.")
            return render_template(
                "tasks/update_status.html",
                task=task,
                task_statuses=TASK_STATUSES,
                form_data=request.form,
            )

        flash("Task status updated successfully.", "success")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    return render_template(
        "tasks/update_status.html",
        task=task,
        task_statuses=TASK_STATUSES,
        form_data={},
    )


# ── Add Task Update ───────────────────────────────────────────────
@tasks_bp.route("/<int:task_id>/add-update", methods=["GET", "POST"])
@login_required
@module_access_required("tasks")
def add_task_update(task_id):
    task = Task.query.get_or_404(task_id)
    if not _can_add_update(task):
        abort(403)

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

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "tasks/add_update.html",
                task=task,
                task_statuses=TASK_STATUSES,
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
            _notify_task_owner(
                task,
                title="Task update added",
                message=(
                    f"{current_user.full_name or current_user.username} added an update "
                    f"to '{task.task_title}'."
                ),
                severity="info",
                skip_user_id=current_user.id,
            )
            invalidate_dashboard_summary_metrics()
            db.session.commit()
        except SQLAlchemyError:
            _db_error("Could not add task update due to a database error.")
            return render_template(
                "tasks/add_update.html",
                task=task,
                task_statuses=TASK_STATUSES,
                form_data=request.form,
            )

        flash("Task update added successfully.", "success")
        return redirect(url_for("tasks.task_detail", task_id=task.id))

    return render_template(
        "tasks/add_update.html",
        task=task,
        task_statuses=TASK_STATUSES,
        form_data={},
    )
