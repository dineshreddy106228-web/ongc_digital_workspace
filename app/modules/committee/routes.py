"""Committee Task routes — blueprint with url_prefix=/committee."""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required
from sqlalchemy import or_
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename

from app.extensions import db
from app.core.services.csc_committee_directory import get_committee_directory
from app.models.committee.committee_task import CommitteeTask
from app.models.committee.committee_task_member import CommitteeTaskMember
from app.models.committee.task_comment import TaskComment
from app.models.core.user import User
from app.models.office.office import Office
from app.modules.committee import committee_bp as bp
from app.modules.committee.decorators import committee_access_required

COMMITTEE_PRIORITIES = ["low", "medium", "high"]
COMMITTEE_STATUSES = ["open", "in_progress", "done"]
MAX_TITLE_LEN = 255
MAX_DESC_LEN = 5000
MAX_COMMENT_LEN = 5000
ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg", "doc", "docx", "xls", "xlsx", "txt", "csv"}


# ── Helpers ───────────────────────────────────────────────────────

def _role_name() -> str:
    return (current_user.role.name if current_user.role else "").lower()


def _is_admin() -> bool:
    return _role_name() == "admin"


def _is_superuser() -> bool:
    return _role_name() == "superuser"


def _is_committee_member() -> bool:
    """True for user-role users who hold the committee module permission."""
    return _role_name() == "user" and current_user.has_module_access("committee")


def _can_write() -> bool:
    """True for roles that may create, comment, and change status."""
    return _is_admin() or _is_superuser() or _is_committee_member()


def _can_access_all_offices() -> bool:
    return _is_admin() or _is_superuser()


def _is_member_of_task(task: CommitteeTask) -> bool:
    return any(m.user_id == current_user.id for m in task.members)


def _can_create_task() -> bool:
    return _is_superuser()


def _can_edit_task() -> bool:
    return _is_superuser()


def _can_change_task_status() -> bool:
    return _is_superuser()


def _can_add_task_update(task: CommitteeTask) -> bool:
    return _is_superuser() or _is_member_of_task(task)


def _is_committee_task_selectable_user(user: User | None) -> bool:
    return bool(
        user
        and getattr(user, "is_active", False)
        and not user.is_admin_user()
    )


def _upload_dir() -> str:
    base = current_app.config.get(
        "COMMITTEE_UPLOAD_DIR",
        os.path.join(current_app.instance_path, "committee_uploads"),
    )
    os.makedirs(base, exist_ok=True)
    return base


def _allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _save_attachment(file_storage) -> str | None:
    """Save an uploaded file and return the relative path, or None."""
    if not file_storage or not file_storage.filename:
        return None
    if not _allowed_file(file_storage.filename):
        return None
    filename = secure_filename(file_storage.filename)
    unique_name = f"{uuid.uuid4().hex}_{filename}"
    dest = os.path.join(_upload_dir(), unique_name)
    file_storage.save(dest)
    return unique_name


def _resolve_office_id(office_id_raw: str | None, fallback: int | None = None) -> int | None:
    raw = str(office_id_raw or "").strip()
    if _can_access_all_offices() and raw.isdigit():
        return int(raw)
    return fallback if fallback is not None else getattr(current_user, "office_id", None)


def _load_active_users_by_ids(member_ids_raw) -> tuple[list[User], list[str]]:
    member_ids = sorted({int(mid) for mid in member_ids_raw if str(mid).strip().isdigit()})
    if not member_ids:
        return [], []

    member_users = (
        User.query.filter(
            User.id.in_(member_ids),
            User.is_active.is_(True),
        )
        .all()
    )

    errors = []
    if len(member_users) != len(member_ids) or any(
        not _is_committee_task_selectable_user(user) for user in member_users
    ):
        errors.append("One or more selected members are invalid or inactive.")
        return [], errors

    return member_users, errors


def _load_committee_head_user(head_user_id_raw: str | None) -> tuple[User | None, list[str]]:
    raw = str(head_user_id_raw or "").strip()
    if not raw:
        return None, ["Committee head is required."]
    if not raw.isdigit():
        return None, ["Selected committee head is invalid."]

    committee_head_user = User.query.filter_by(id=int(raw), is_active=True).first()
    if committee_head_user is None:
        return None, ["Selected committee head is invalid or inactive."]
    if not _is_committee_task_selectable_user(committee_head_user):
        return None, ["Admin users cannot be selected as committee heads."]
    return committee_head_user, []


def _sync_task_members(
    *,
    task_id: int,
    creator_id: int,
    member_users: list[User],
    committee_head_user: User,
) -> None:
    added_user_ids: set[int] = set()
    selected_member_ids = {user.id for user in member_users}

    db.session.add(
        CommitteeTaskMember(
            task_id=task_id,
            user_id=committee_head_user.id,
            role="committee_head",
        )
    )
    added_user_ids.add(committee_head_user.id)

    if creator_id not in added_user_ids:
        db.session.add(
            CommitteeTaskMember(
                task_id=task_id,
                user_id=creator_id,
                role="assignee" if creator_id in selected_member_ids else "creator",
            )
        )
        added_user_ids.add(creator_id)

    for user in member_users:
        if user.id in added_user_ids:
            continue
        db.session.add(
            CommitteeTaskMember(
                task_id=task_id,
                user_id=user.id,
                role="assignee",
            )
        )
        added_user_ids.add(user.id)


def _build_list_summary(tasks: list[CommitteeTask]) -> dict[str, int]:
    today = date.today()
    open_statuses = {"open", "in_progress"}
    return {
        "total": len(tasks),
        "open": sum(1 for task in tasks if task.status in open_statuses),
        "in_progress": sum(1 for task in tasks if task.status == "in_progress"),
        "overdue": sum(
            1 for task in tasks
            if task.status in open_statuses and task.due_date and task.due_date < today
        ),
        "done": sum(1 for task in tasks if task.status == "done"),
    }


def _display_user_name(user: User | None, fallback: str | None = None) -> str:
    if user is not None:
        return (user.full_name or user.username or fallback or "—").strip()
    return str(fallback or "—").strip() or "—"


def _username_key(value: str | None) -> str:
    return str(value or "").strip().lower()


def _committee_is_mapped_to_username(committee: dict[str, object], username: str | None) -> bool:
    username_key = _username_key(username)
    if not username_key:
        return False
    if _username_key(committee.get("committee_head")) == username_key:
        return True
    for assignment in committee.get("committee_users") or []:
        if _username_key((assignment or {}).get("username")) == username_key:
            return True
    return False


def _build_committee_directory_context() -> tuple[list[dict], dict[str, int]]:
    directory = get_committee_directory()
    mapped_usernames = set()

    for committee in directory:
        committee_head = str(committee.get("committee_head") or "").strip()
        if committee_head:
            mapped_usernames.add(committee_head)
        for assignment in committee.get("committee_users") or []:
            username = str((assignment or {}).get("username") or "").strip()
            if username:
                mapped_usernames.add(username)

    user_lookup = {}
    if mapped_usernames:
        mapped_users = (
            User.query.filter(
                User.username.in_(sorted(mapped_usernames)),
            )
            .all()
        )
        user_lookup = {
            (user.username or "").strip().lower(): user
            for user in mapped_users
            if (user.username or "").strip()
        }

    committee_cards = []
    mapped_heads = 0
    mapped_members = 0
    linked_orders = 0

    for committee in directory:
        committee_head_username = str(committee.get("committee_head") or "").strip()
        committee_head_user = user_lookup.get(committee_head_username.lower()) if committee_head_username else None
        if committee_head_username:
            mapped_heads += 1

        subset_labels = [
            str((subset or {}).get("label") or (subset or {}).get("code") or "").strip()
            for subset in (committee.get("subsets") or [])
            if str((subset or {}).get("label") or (subset or {}).get("code") or "").strip()
        ]

        member_assignments = []
        for assignment in committee.get("committee_users") or []:
            username = str((assignment or {}).get("username") or "").strip()
            if not username:
                continue
            member_user = user_lookup.get(username.lower())
            member_assignments.append(
                {
                    "name": _display_user_name(member_user, username),
                    "subset_codes": [
                        str(code).strip().upper()
                        for code in ((assignment or {}).get("subset_codes") or [])
                        if str(code).strip()
                    ],
                }
            )

        mapped_members += len(member_assignments)
        office_orders = [
            str(order_slug).strip()
            for order_slug in (committee.get("office_orders") or [])
            if str(order_slug).strip()
        ]
        is_mapped_to_current_user = _committee_is_mapped_to_username(
            committee,
            getattr(current_user, "username", ""),
        )
        linked_orders += len(office_orders)

        committee_cards.append(
            {
                "slug": committee.get("slug") or "",
                "title": committee.get("title") or "Committee",
                "kind": committee.get("kind") or "Committee",
                "tone": committee.get("tone") or "indigo",
                "summary": committee.get("summary") or "",
                "head_name": _display_user_name(committee_head_user, committee_head_username or "Not assigned"),
                "member_assignments": member_assignments,
                "member_count": len(member_assignments),
                "subset_labels": subset_labels,
                "subset_count": len(subset_labels),
                "office_order_count": len(office_orders),
                "office_orders": office_orders,
                "is_mapped_to_current_user": is_mapped_to_current_user,
            }
        )

    return committee_cards, {
        "total": len(committee_cards),
        "mapped_heads": mapped_heads,
        "mapped_members": mapped_members,
        "linked_orders": linked_orders,
    }


def _scoped_committee_directory_view(
    committee_directory: list[dict],
    selected_slug: str | None,
) -> tuple[list[dict], dict[str, int], list[dict], dict | None, str]:
    can_view_all = _is_superuser() or _is_admin()
    visible_committees = (
        list(committee_directory)
        if can_view_all
        else [committee for committee in committee_directory if committee.get("is_mapped_to_current_user")]
    )
    visible_committees.sort(key=lambda committee: str(committee.get("title") or "").lower())

    visible_summary = {
        "total": len(visible_committees),
        "mapped_heads": sum(1 for committee in visible_committees if committee.get("head_name") and committee.get("head_name") != "Not assigned"),
        "mapped_members": sum(int(committee.get("member_count") or 0) for committee in visible_committees),
        "linked_orders": sum(int(committee.get("office_order_count") or 0) for committee in visible_committees),
    }

    filter_options = [
        {
            "slug": str(committee.get("slug") or ""),
            "title": str(committee.get("title") or "Committee"),
            "kind": str(committee.get("kind") or "Committee"),
        }
        for committee in visible_committees
        if str(committee.get("slug") or "").strip()
    ]

    selected_slug = str(selected_slug or "").strip()
    valid_slugs = {option["slug"] for option in filter_options}
    if selected_slug not in valid_slugs and filter_options:
        selected_slug = filter_options[0]["slug"]

    selected_committee = next(
        (committee for committee in visible_committees if committee.get("slug") == selected_slug),
        None,
    )

    return visible_committees, visible_summary, filter_options, selected_committee, selected_slug


# ── List Tasks ────────────────────────────────────────────────────

@bp.route("/")
@bp.route("")
@login_required
@committee_access_required()
def list_tasks():
    committee_filter = request.args.get("committee", "").strip()
    status_filter = request.args.get("status", "").strip().lower()
    priority_filter = request.args.get("priority", "").strip().lower()
    office_filter = request.args.get("office_id", "").strip()
    due_from = request.args.get("due_from", "").strip()
    due_to = request.args.get("due_to", "").strip()

    query = CommitteeTask.query

    # Scoping: committee members see their office tasks plus tasks assigned to them.
    if _is_committee_member():
        query = query.filter(
            or_(
                CommitteeTask.office_id == current_user.office_id,
                CommitteeTask.members.any(CommitteeTaskMember.user_id == current_user.id),
            )
        )
    elif office_filter and office_filter.isdigit():
        query = query.filter(CommitteeTask.office_id == int(office_filter))

    if status_filter in COMMITTEE_STATUSES:
        query = query.filter(CommitteeTask.status == status_filter)
    if priority_filter in COMMITTEE_PRIORITIES:
        query = query.filter(CommitteeTask.priority == priority_filter)

    if due_from:
        try:
            query = query.filter(
                CommitteeTask.due_date >= datetime.strptime(due_from, "%Y-%m-%d").date()
            )
        except ValueError:
            pass
    if due_to:
        try:
            query = query.filter(
                CommitteeTask.due_date <= datetime.strptime(due_to, "%Y-%m-%d").date()
            )
        except ValueError:
            pass

    tasks = query.order_by(CommitteeTask.created_at.desc()).all()
    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()
    committee_directory, directory_summary = _build_committee_directory_context()
    (
        visible_committee_directory,
        visible_directory_summary,
        committee_filter_options,
        selected_committee,
        selected_committee_slug,
    ) = _scoped_committee_directory_view(
        committee_directory,
        committee_filter,
    )

    return render_template(
        "committee/list.html",
        tasks=tasks,
        summary=_build_list_summary(tasks),
        committee_directory=visible_committee_directory,
        directory_summary=visible_directory_summary,
        directory_summary_all=directory_summary,
        committee_filter_options=committee_filter_options,
        selected_committee=selected_committee,
        offices=offices,
        statuses=COMMITTEE_STATUSES,
        priorities=COMMITTEE_PRIORITIES,
        filters={
            "committee": selected_committee_slug,
            "status": status_filter,
            "priority": priority_filter,
            "office_id": office_filter,
            "due_from": due_from,
            "due_to": due_to,
        },
        can_create=_can_create_task(),
        is_admin=_is_admin(),
        is_superuser=_is_superuser(),
        can_view_all_committees=_is_superuser() or _is_admin(),
    )


# ── Create Task ───────────────────────────────────────────────────

@bp.route("/create", methods=["GET", "POST"])
@login_required
@committee_access_required()
def create_task():
    if not _can_create_task():
        abort(403)

    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        priority = request.form.get("priority", "medium").strip().lower()
        due_date_raw = request.form.get("due_date", "").strip()
        office_id_raw = request.form.get("office_id", "").strip()
        committee_head_id_raw = request.form.get("committee_head_id", "").strip()
        member_ids_raw = request.form.getlist("member_ids")

        errors = []
        if not title:
            errors.append("Title is required.")
        if len(title) > MAX_TITLE_LEN:
            errors.append(f"Title cannot exceed {MAX_TITLE_LEN} characters.")
        if len(description) > MAX_DESC_LEN:
            errors.append(f"Description cannot exceed {MAX_DESC_LEN} characters.")
        if priority not in COMMITTEE_PRIORITIES:
            errors.append("Invalid priority.")

        due_date = None
        if due_date_raw:
            try:
                due_date = datetime.strptime(due_date_raw, "%Y-%m-%d").date()
            except ValueError:
                errors.append("Due date must be YYYY-MM-DD.")

        # Office resolution
        office_id = _resolve_office_id(office_id_raw)

        if not office_id:
            errors.append("An office must be associated with the task.")

        committee_head_user, committee_head_errors = _load_committee_head_user(committee_head_id_raw)
        errors.extend(committee_head_errors)

        member_users, member_errors = _load_active_users_by_ids(member_ids_raw)
        errors.extend(member_errors)

        # Attachment
        attachment_path = None
        file = request.files.get("attachment")
        if file and file.filename:
            if not _allowed_file(file.filename):
                errors.append("Unsupported file type.")
            else:
                attachment_path = _save_attachment(file)

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "committee/create.html",
                offices=offices,
                priorities=COMMITTEE_PRIORITIES,
                is_admin=_is_admin(),
                is_superuser=_is_superuser(),
                form_data=request.form,
            )

        try:
            task = CommitteeTask(
                title=title,
                description=description or None,
                priority=priority,
                status="open",
                due_date=due_date,
                office_id=office_id,
                created_by=current_user.id,
            )
            db.session.add(task)
            db.session.flush()

            _sync_task_members(
                task_id=task.id,
                creator_id=current_user.id,
                member_users=member_users,
                committee_head_user=committee_head_user,
            )

            # If attachment was provided, add as first comment
            if attachment_path:
                db.session.add(
                    TaskComment(
                        task_id=task.id,
                        user_id=current_user.id,
                        body="Attachment uploaded with task creation.",
                        attachment_path=attachment_path,
                    )
                )

            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            current_app.logger.exception("Committee task creation failed")
            flash("Could not create task due to a database error.", "danger")
            return render_template(
                "committee/create.html",
                offices=offices,
                priorities=COMMITTEE_PRIORITIES,
                is_admin=_is_admin(),
                is_superuser=_is_superuser(),
                form_data=request.form,
            )

        flash("Committee task created successfully.", "success")
        return redirect(url_for("committee.list_tasks"))

    return render_template(
        "committee/create.html",
        offices=offices,
        priorities=COMMITTEE_PRIORITIES,
        is_admin=_is_admin(),
        is_superuser=_is_superuser(),
        form_data={},
    )


# ── Task Detail ───────────────────────────────────────────────────

@bp.route("/<int:task_id>")
@login_required
@committee_access_required()
def task_detail(task_id):
    task = CommitteeTask.query.get_or_404(task_id)

    # Visibility check
    if _is_committee_member() and task.office_id != current_user.office_id:
        if not _is_member_of_task(task):
            abort(403)

    comments = (
        TaskComment.query.filter_by(task_id=task.id)
        .order_by(TaskComment.created_at.asc())
        .all()
    )

    can_edit = _can_edit_task()
    can_change_status = _can_change_task_status()
    can_add_update = _can_add_task_update(task)

    return render_template(
        "committee/detail.html",
        task=task,
        comments=comments,
        can_edit=can_edit,
        can_change_status=can_change_status,
        can_add_update=can_add_update,
        is_superuser=_is_superuser(),
        statuses=COMMITTEE_STATUSES,
    )


@bp.route("/<int:task_id>/summary")
@login_required
@committee_access_required()
def task_summary(task_id):
    task = CommitteeTask.query.get_or_404(task_id)

    if _is_committee_member() and task.office_id != current_user.office_id:
        if not _is_member_of_task(task):
            abort(403)

    comments = (
        TaskComment.query.filter_by(task_id=task.id)
        .order_by(TaskComment.created_at.desc())
        .all()
    )

    can_edit = _can_edit_task()
    can_change_status = _can_change_task_status()
    can_add_update = _can_add_task_update(task)

    return render_template(
        "committee/_committee_summary_panel.html",
        task=task,
        comments=comments,
        can_edit=can_edit,
        can_change_status=can_change_status,
        can_add_update=can_add_update,
    )


# ── Add Comment ───────────────────────────────────────────────────

@bp.route("/<int:task_id>/comment", methods=["POST"])
@login_required
@committee_access_required()
def add_comment(task_id):
    task = CommitteeTask.query.get_or_404(task_id)

    if not _can_add_task_update(task):
        abort(403)

    body = request.form.get("body", "").strip()
    if not body:
        flash("Comment body is required.", "danger")
        return redirect(url_for("committee.task_detail", task_id=task.id))
    if len(body) > MAX_COMMENT_LEN:
        flash(f"Comment cannot exceed {MAX_COMMENT_LEN} characters.", "danger")
        return redirect(url_for("committee.task_detail", task_id=task.id))

    attachment_path = None
    file = request.files.get("attachment")
    if file and file.filename:
        if _allowed_file(file.filename):
            attachment_path = _save_attachment(file)
        else:
            flash("Unsupported attachment file type.", "danger")
            return redirect(url_for("committee.task_detail", task_id=task.id))

    try:
        db.session.add(
            TaskComment(
                task_id=task.id,
                user_id=current_user.id,
                body=body,
                attachment_path=attachment_path,
            )
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        current_app.logger.exception("Committee comment creation failed")
        flash("Could not add comment.", "danger")

    return redirect(url_for("committee.task_detail", task_id=task.id))


# ── Update Status ─────────────────────────────────────────────────

@bp.route("/<int:task_id>/status", methods=["POST"])
@login_required
@committee_access_required()
def update_status(task_id):
    task = CommitteeTask.query.get_or_404(task_id)

    if not _can_change_task_status():
        abort(403)

    new_status = request.form.get("status", "").strip().lower()
    if new_status not in COMMITTEE_STATUSES:
        flash("Invalid status.", "danger")
        return redirect(url_for("committee.task_detail", task_id=task.id))

    try:
        task.status = new_status
        db.session.commit()
        flash(f"Status updated to {new_status.replace('_', ' ').title()}.", "success")
    except SQLAlchemyError:
        db.session.rollback()
        current_app.logger.exception("Committee status update failed")
        flash("Could not update status.", "danger")

    return redirect(url_for("committee.task_detail", task_id=task.id))


# ── Edit Task ─────────────────────────────────────────────────────

@bp.route("/<int:task_id>/edit", methods=["GET", "POST"])
@login_required
@committee_access_required()
def edit_task(task_id):
    task = CommitteeTask.query.get_or_404(task_id)

    if not _can_edit_task():
        abort(403)

    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        priority = request.form.get("priority", "medium").strip().lower()
        status = request.form.get("status", task.status).strip().lower()
        due_date_raw = request.form.get("due_date", "").strip()
        office_id_raw = request.form.get("office_id", "").strip()
        committee_head_id_raw = request.form.get("committee_head_id", "").strip()
        member_ids_raw = request.form.getlist("member_ids")

        errors = []
        if not title:
            errors.append("Title is required.")
        if len(title) > MAX_TITLE_LEN:
            errors.append(f"Title cannot exceed {MAX_TITLE_LEN} characters.")
        if len(description) > MAX_DESC_LEN:
            errors.append(f"Description cannot exceed {MAX_DESC_LEN} characters.")
        if priority not in COMMITTEE_PRIORITIES:
            errors.append("Invalid priority.")
        if status not in COMMITTEE_STATUSES:
            errors.append("Invalid status.")

        due_date = None
        if due_date_raw:
            try:
                due_date = datetime.strptime(due_date_raw, "%Y-%m-%d").date()
            except ValueError:
                errors.append("Due date must be YYYY-MM-DD.")

        office_id = _resolve_office_id(office_id_raw, fallback=task.office_id)
        committee_head_user, committee_head_errors = _load_committee_head_user(committee_head_id_raw)
        errors.extend(committee_head_errors)

        member_users, member_errors = _load_active_users_by_ids(member_ids_raw)
        errors.extend(member_errors)

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "committee/edit.html",
                task=task,
                offices=offices,
                priorities=COMMITTEE_PRIORITIES,
                statuses=COMMITTEE_STATUSES,
                is_admin=_is_admin(),
                is_superuser=_is_superuser(),
                form_data=request.form,
            )

        try:
            task.title = title
            task.description = description or None
            task.priority = priority
            task.status = status
            task.due_date = due_date
            task.office_id = office_id

            # Rebuild members
            CommitteeTaskMember.query.filter_by(task_id=task.id).delete(
                synchronize_session=False
            )
            _sync_task_members(
                task_id=task.id,
                creator_id=task.created_by,
                member_users=member_users,
                committee_head_user=committee_head_user,
            )

            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            current_app.logger.exception("Committee task edit failed")
            flash("Could not update task.", "danger")
            return render_template(
                "committee/edit.html",
                task=task,
                offices=offices,
                priorities=COMMITTEE_PRIORITIES,
                statuses=COMMITTEE_STATUSES,
                is_admin=_is_admin(),
                is_superuser=_is_superuser(),
                form_data=request.form,
            )

        flash("Committee task updated.", "success")
        return redirect(url_for("committee.task_detail", task_id=task.id))

    return render_template(
        "committee/edit.html",
        task=task,
        offices=offices,
        priorities=COMMITTEE_PRIORITIES,
        statuses=COMMITTEE_STATUSES,
        is_admin=_is_admin(),
        is_superuser=_is_superuser(),
        form_data={},
    )


# ── API: Office Members (for JS member picker) ───────────────────

@bp.route("/api/office_members/<int:office_id>")
@login_required
@committee_access_required()
def api_office_members(office_id):
    query = User.query.filter_by(is_active=True)
    if office_id:
        query = query.filter_by(office_id=office_id)
    users = [
        user
        for user in query.order_by(User.full_name, User.username).all()
        if _is_committee_task_selectable_user(user)
    ]
    return jsonify(
        [
            {
                "id": u.id,
                "name": u.full_name or u.username,
                "username": u.username,
                "office_id": u.office_id,
                "office_name": u.office.office_name if getattr(u, "office", None) else "Unassigned Office",
            }
            for u in users
        ]
    )


# ── Attachment download ──────────────────────────────────────────

@bp.route("/attachment/<path:filename>")
@login_required
@committee_access_required()
def download_attachment(filename):
    from flask import send_from_directory

    return send_from_directory(_upload_dir(), filename)
