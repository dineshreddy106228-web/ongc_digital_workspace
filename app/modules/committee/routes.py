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
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.utils import secure_filename

from app.extensions import db
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


def _load_member_users_for_office(member_ids_raw, office_id: int | None) -> tuple[list[User], list[str]]:
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
    if len(member_users) != len(member_ids):
        errors.append("One or more selected members are invalid or inactive.")
        return [], errors

    if office_id is not None:
        invalid_members = [user for user in member_users if user.office_id != office_id]
        if invalid_members:
            errors.append("Assigned members must belong to the selected office.")
            return [], errors

    return member_users, errors


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


# ── List Tasks ────────────────────────────────────────────────────

@bp.route("/")
@bp.route("")
@login_required
@committee_access_required()
def list_tasks():
    status_filter = request.args.get("status", "").strip().lower()
    priority_filter = request.args.get("priority", "").strip().lower()
    office_filter = request.args.get("office_id", "").strip()
    due_from = request.args.get("due_from", "").strip()
    due_to = request.args.get("due_to", "").strip()

    query = CommitteeTask.query

    # Scoping: committee_member sees own office only
    if _is_committee_member():
        query = query.filter(CommitteeTask.office_id == current_user.office_id)
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

    return render_template(
        "committee/list.html",
        tasks=tasks,
        summary=_build_list_summary(tasks),
        offices=offices,
        statuses=COMMITTEE_STATUSES,
        priorities=COMMITTEE_PRIORITIES,
        filters={
            "status": status_filter,
            "priority": priority_filter,
            "office_id": office_filter,
            "due_from": due_from,
            "due_to": due_to,
        },
        can_write=_can_write(),
        is_admin=_is_admin(),
        is_superuser=_is_superuser(),
    )


# ── Create Task ───────────────────────────────────────────────────

@bp.route("/create", methods=["GET", "POST"])
@login_required
@committee_access_required()
def create_task():
    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        priority = request.form.get("priority", "medium").strip().lower()
        due_date_raw = request.form.get("due_date", "").strip()
        office_id_raw = request.form.get("office_id", "").strip()
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

        member_users, member_errors = _load_member_users_for_office(member_ids_raw, office_id)
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

            # Creator as 'creator' member
            db.session.add(
                CommitteeTaskMember(
                    task_id=task.id,
                    user_id=current_user.id,
                    role="creator",
                )
            )
            # Assigned members
            for user in member_users:
                if user.id == current_user.id:
                    continue  # already added as creator
                db.session.add(
                    CommitteeTaskMember(
                        task_id=task.id,
                        user_id=user.id,
                        role="assignee",
                    )
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

    is_creator = task.created_by == current_user.id
    can_edit = is_creator or _is_admin() or _is_superuser()
    can_interact = _can_write() and (_is_member_of_task(task) or _is_admin() or _is_superuser())

    return render_template(
        "committee/detail.html",
        task=task,
        comments=comments,
        can_edit=can_edit,
        can_interact=can_interact,
        is_superuser=_is_superuser(),
        statuses=COMMITTEE_STATUSES,
    )


# ── Add Comment ───────────────────────────────────────────────────

@bp.route("/<int:task_id>/comment", methods=["POST"])
@login_required
@committee_access_required()
def add_comment(task_id):
    task = CommitteeTask.query.get_or_404(task_id)

    if not (_is_member_of_task(task) or _is_admin() or _is_superuser()):
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

    if not (_is_member_of_task(task) or _is_admin() or _is_superuser()):
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

    is_creator = task.created_by == current_user.id
    if not (is_creator or _is_admin() or _is_superuser()):
        abort(403)

    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        description = request.form.get("description", "").strip()
        priority = request.form.get("priority", "medium").strip().lower()
        status = request.form.get("status", task.status).strip().lower()
        due_date_raw = request.form.get("due_date", "").strip()
        office_id_raw = request.form.get("office_id", "").strip()
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
        member_users, member_errors = _load_member_users_for_office(member_ids_raw, office_id)
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
            # Always keep creator
            db.session.add(
                CommitteeTaskMember(
                    task_id=task.id,
                    user_id=task.created_by,
                    role="creator",
                )
            )
            for user in member_users:
                if user.id == task.created_by:
                    continue
                db.session.add(
                    CommitteeTaskMember(
                        task_id=task.id,
                        user_id=user.id,
                        role="assignee",
                    )
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
    users = (
        User.query.filter_by(office_id=office_id, is_active=True)
        .order_by(User.full_name, User.username)
        .all()
    )
    return jsonify(
        [
            {
                "id": u.id,
                "name": u.full_name or u.username,
                "username": u.username,
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
