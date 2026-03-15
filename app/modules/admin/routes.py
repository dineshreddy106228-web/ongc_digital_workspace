"""Admin blueprint routes – User Management with User Governance V2.

Governance V2 additions:
  - Reporting hierarchy assignment
  - Module-level access permissions (checkboxes)
  - Audit logging for governance changes
  - User category reflected via role assignment
"""

from flask import render_template, redirect, url_for, flash, request, send_file
from flask_login import login_required, current_user
from sqlalchemy.exc import SQLAlchemyError
from app.modules.admin import admin_bp
from app.extensions import db
from app.features import get_admin_module_options
from app.models.core.backup_snapshot import BackupSnapshot
from app.models.core.user import User
from app.models.core.role import Role
from app.models.office.office import Office
from app.models.core.audit_log import AuditLog
from app.models.core.activity_log import ActivityLog
from app.models.core.notification import Notification
from app.models.tasks.recurring_task_collaborator import RecurringTaskCollaborator
from app.models.tasks.recurring_task_template import RecurringTaskTemplate
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator
from app.models.tasks.task_update import TaskUpdate
from app.models.core.user_module_permission import (
    UserModulePermission,
    SUPER_USER_MODULES,
)
from app.core.utils.decorators import roles_required
from app.core.utils.request_meta import get_client_ip, get_user_agent
from app.core.utils.activity import log_activity
from app.core.module_registry import invalidate_user_module_access_cache
from app.core.services.notifications import create_notification
from app.core.services.backups import (
    BackupError,
    create_database_backup,
    get_runtime_environment_name,
)
from app.core.roles import (
    ADMIN_ROLE,
    ROLE_DESCRIPTIONS,
    SUPERUSER_ROLE,
    USER_ROLE,
    ROLE_REGISTRY,
    canonicalize_role_name,
)


# ── Helper ──────────────────────────────────────────────────────
def _client_ip():
    return get_client_ip()


def _set_module_permissions(user: User, selected_codes: list, role: Role):
    """
    Clear existing module permissions for *user* and set fresh ones.

    superuser → all business modules
    admin     → admin_users only
    user      → exactly what is in selected_codes (admin-controlled)
    """
    UserModulePermission.query.filter_by(user_id=user.id).delete()
    db.session.flush()

    canonical_role = canonicalize_role_name(role.name) if role else None

    if canonical_role == SUPERUSER_ROLE:
        codes_to_save = list(SUPER_USER_MODULES)
    elif canonical_role == ADMIN_ROLE:
        codes_to_save = ["admin_users"]
    else:
        codes_to_save = list(selected_codes)

    for code in codes_to_save:
        db.session.add(
            UserModulePermission(
                user_id=user.id,
                module_code=code,
                can_access=True,
            )
        )
    db.session.flush()


def _module_options():
    """Expose module permissions with app-level feature status for admin forms."""
    return get_admin_module_options()


def _assignable_roles():
    """Return active role choices using canonical labels with legacy fallback."""
    roles = Role.query.filter(Role.is_active == True).order_by(Role.name).all()
    canonical_roles = {}

    for role in roles:
        canonical_name = canonicalize_role_name(role.name)
        if canonical_name not in ROLE_REGISTRY:
            continue

        existing = canonical_roles.get(canonical_name)
        if existing is None or role.name == canonical_name:
            canonical_roles[canonical_name] = {
                "id": role.id,
                "name": canonical_name,
                "description": ROLE_DESCRIPTIONS.get(canonical_name, role.description),
            }

    return [
        canonical_roles[role_name]
        for role_name in ROLE_REGISTRY
        if role_name in canonical_roles
    ]


# ── Users List ───────────────────────────────────────────────────
@admin_bp.route("/users")
@login_required
@roles_required(ADMIN_ROLE)
def users():
    all_users = User.query.order_by(User.created_at.desc()).all()
    return render_template("admin/users.html", users=all_users)


# ── Create User ──────────────────────────────────────────────────
@admin_bp.route("/users/create", methods=["GET", "POST"])
@login_required
@roles_required(ADMIN_ROLE)
def create_user():
    roles = _assignable_roles()
    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()
    # Officers dropdown – all active users except the one being created
    officers = User.query.filter_by(is_active=True).order_by(User.full_name, User.username).all()

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        role_id = request.form.get("role_id", "").strip()
        office_id = request.form.get("office_id", "").strip()
        designation = request.form.get("designation", "").strip()
        employee_code = request.form.get("employee_code", "").strip()
        temp_password = request.form.get("temporary_password", "")
        is_active = request.form.get("is_active") == "on"
        controlling_officer_id_raw = request.form.get("controlling_officer_id", "").strip()
        reviewing_officer_id_raw = request.form.get("reviewing_officer_id", "").strip()
        selected_modules = request.form.getlist("module_access")

        # ── Validation ────────────────────────────────────────────
        errors = []

        if not username:
            errors.append("Username is required.")
        if not full_name:
            errors.append("Full name is required.")
        if not email:
            errors.append("Email is required.")
        if not role_id:
            errors.append("Role is required.")
        if not office_id:
            errors.append("Office is required.")
        if not temp_password:
            errors.append("Temporary password is required.")
        elif len(temp_password) < 6:
            errors.append("Password must be at least 6 characters.")

        if username and User.query.filter_by(username=username).first():
            errors.append(f"Username '{username}' is already taken.")
        if email and User.query.filter(User.email == email).first():
            errors.append(f"Email '{email}' is already registered.")

        role = None
        office = None
        if role_id:
            role = Role.query.filter_by(id=role_id, is_active=True).first()
            if not role:
                errors.append("Selected role is invalid or inactive.")
        if office_id:
            office = Office.query.filter_by(id=office_id, is_active=True).first()
            if not office:
                errors.append("Selected office is invalid or inactive.")

        # Validate officer references
        controlling_officer_id = None
        reviewing_officer_id = None
        if controlling_officer_id_raw and controlling_officer_id_raw.isdigit():
            co = User.query.filter_by(id=int(controlling_officer_id_raw), is_active=True).first()
            if co:
                controlling_officer_id = co.id
        if reviewing_officer_id_raw and reviewing_officer_id_raw.isdigit():
            ro = User.query.filter_by(id=int(reviewing_officer_id_raw), is_active=True).first()
            if ro:
                reviewing_officer_id = ro.id

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "admin/create_user.html",
                roles=roles,
                offices=offices,
                officers=officers,
                supported_modules=_module_options(),
                form_data=request.form,
                selected_modules=selected_modules,
            )

        # ── Create user ────────────────────────────────────────────
        new_user = User(
            username=username,
            full_name=full_name,
            email=email,
            role_id=int(role_id),
            office_id=int(office_id),
            designation=designation,
            employee_code=employee_code,
            is_active=is_active,
            must_change_password=True,
            controlling_officer_id=controlling_officer_id,
            reviewing_officer_id=reviewing_officer_id,
        )
        new_user.set_password(temp_password)
        db.session.add(new_user)
        db.session.flush()  # get new_user.id

        # ── Set module permissions ─────────────────────────────────
        _set_module_permissions(new_user, selected_modules, role)

        # ── Audit log ──────────────────────────────────────────────
        AuditLog.log(
            action="USER_CREATED",
            user_id=current_user.id,
            entity_type="User",
            entity_id=str(new_user.id),
            details=(
                f"Admin '{current_user.username}' created user '{username}' "
                f"(role={role.name}, office={office.office_name}, "
                f"modules={','.join(selected_modules) or 'auto'})"
            ),
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )

        log_activity(current_user.username, "user_created", "user", username,
                     details=f"role={role.name}, office={office.office_name}")
        create_notification(
            user_id=new_user.id,
            title="Your account is ready",
            message=(
                f"{current_user.full_name or current_user.username} created your account "
                f"with the role '{role.name}' for {office.office_name}."
            ),
            severity="success",
            link="/dashboard",
        )
        invalidate_user_module_access_cache(new_user.id)
        db.session.commit()

        flash(
            f"User '{username}' created successfully. "
            "They must change password on first login.",
            "success",
        )
        return redirect(url_for("admin.users"))

    return render_template(
        "admin/create_user.html",
        roles=roles,
        offices=offices,
        officers=officers,
        supported_modules=_module_options(),
        form_data={},
        selected_modules=[],
    )


# ── Edit User ────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@login_required
@roles_required(ADMIN_ROLE)
def edit_user(user_id):
    target = User.query.get_or_404(user_id)
    roles = _assignable_roles()
    offices = Office.query.filter_by(is_active=True).order_by(Office.office_name).all()
    # Exclude the user being edited from officer dropdowns
    officers = (
        User.query.filter(User.is_active == True, User.id != user_id)
        .order_by(User.full_name, User.username)
        .all()
    )
    # Current module permissions for this user
    current_module_codes = [
        p.module_code
        for p in UserModulePermission.query.filter_by(user_id=user_id, can_access=True).all()
    ]

    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        role_id = request.form.get("role_id", "").strip()
        office_id = request.form.get("office_id", "").strip()
        designation = request.form.get("designation", "").strip()
        employee_code = request.form.get("employee_code", "").strip()
        is_active = request.form.get("is_active") == "on"
        controlling_officer_id_raw = request.form.get("controlling_officer_id", "").strip()
        reviewing_officer_id_raw = request.form.get("reviewing_officer_id", "").strip()
        selected_modules = request.form.getlist("module_access")

        # ── Validation ────────────────────────────────────────────
        errors = []

        if not full_name:
            errors.append("Full name is required.")
        if not email:
            errors.append("Email is required.")
        if not role_id:
            errors.append("Role is required.")
        if not office_id:
            errors.append("Office is required.")

        if email:
            conflict = User.query.filter(User.email == email, User.id != user_id).first()
            if conflict:
                errors.append(f"Email '{email}' is already registered to another user.")

        role = None
        office = None
        if role_id:
            role = Role.query.filter_by(id=role_id, is_active=True).first()
            if not role:
                errors.append("Selected role is invalid or inactive.")
        if office_id:
            office = Office.query.filter_by(id=office_id, is_active=True).first()
            if not office:
                errors.append("Selected office is invalid or inactive.")

        # Validate officer references
        controlling_officer_id = None
        reviewing_officer_id = None
        if controlling_officer_id_raw and controlling_officer_id_raw.isdigit():
            co = User.query.filter_by(id=int(controlling_officer_id_raw), is_active=True).first()
            if co and co.id != user_id:
                controlling_officer_id = co.id
        if reviewing_officer_id_raw and reviewing_officer_id_raw.isdigit():
            ro = User.query.filter_by(id=int(reviewing_officer_id_raw), is_active=True).first()
            if ro and ro.id != user_id:
                reviewing_officer_id = ro.id

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template(
                "admin/edit_user.html",
                target=target,
                roles=roles,
                offices=offices,
                officers=officers,
                supported_modules=_module_options(),
                current_module_codes=selected_modules,
                form_data=request.form,
            )

        # ── Apply changes ──────────────────────────────────────────
        changed_fields = []
        previous_role_name = target.role.name if target.role else "No role"
        role_changed = str(target.role_id) != str(role_id)
        if target.full_name != full_name:
            changed_fields.append(f"full_name: '{target.full_name}' → '{full_name}'")
            target.full_name = full_name
        if target.email != email:
            changed_fields.append(f"email: '{target.email}' → '{email}'")
            target.email = email
        if str(target.role_id) != str(role_id):
            changed_fields.append(f"role_id: {target.role_id} → {role_id}")
            target.role_id = int(role_id)
        if str(target.office_id) != str(office_id):
            changed_fields.append(f"office_id: {target.office_id} → {office_id}")
            target.office_id = int(office_id)
        if target.designation != designation:
            changed_fields.append(f"designation: '{target.designation}' → '{designation}'")
            target.designation = designation
        if target.employee_code != employee_code:
            changed_fields.append(f"employee_code: '{target.employee_code}' → '{employee_code}'")
            target.employee_code = employee_code
        if target.is_active != is_active:
            changed_fields.append(f"is_active: {target.is_active} → {is_active}")
            target.is_active = is_active

        # Hierarchy changes
        old_co = target.controlling_officer_id
        old_ro = target.reviewing_officer_id
        target.controlling_officer_id = controlling_officer_id
        target.reviewing_officer_id = reviewing_officer_id
        if old_co != controlling_officer_id:
            changed_fields.append(f"controlling_officer_id: {old_co} → {controlling_officer_id}")
        if old_ro != reviewing_officer_id:
            changed_fields.append(f"reviewing_officer_id: {old_ro} → {reviewing_officer_id}")

        db.session.flush()

        # ── Update module permissions ──────────────────────────────
        _set_module_permissions(target, selected_modules, role)

        # ── Audit log ──────────────────────────────────────────────
        AuditLog.log(
            action="USER_UPDATED",
            user_id=current_user.id,
            entity_type="User",
            entity_id=str(target.id),
            details=(
                f"Admin '{current_user.username}' updated user '{target.username}'. "
                f"Changes: {'; '.join(changed_fields) if changed_fields else 'none'}"
            ),
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )

        # Separate audit entries for governance changes
        if old_co != controlling_officer_id or old_ro != reviewing_officer_id:
            AuditLog.log(
                action="USER_HIERARCHY_UPDATED",
                user_id=current_user.id,
                entity_type="User",
                entity_id=str(target.id),
                details=(
                    f"Hierarchy updated for '{target.username}': "
                    f"controlling_officer_id={controlling_officer_id}, "
                    f"reviewing_officer_id={reviewing_officer_id}"
                ),
                ip_address=_client_ip(),
                user_agent=get_user_agent(),
            )

        if sorted(current_module_codes) != sorted(selected_modules):
            AuditLog.log(
                action="USER_MODULE_ACCESS_UPDATED",
                user_id=current_user.id,
                entity_type="User",
                entity_id=str(target.id),
                details=(
                    f"Module access updated for '{target.username}': "
                    f"modules={','.join(selected_modules) or 'none'}"
                ),
                ip_address=_client_ip(),
                user_agent=get_user_agent(),
            )

        if changed_fields:
            log_activity(current_user.username, "user_updated", "user",
                         target.username,
                         details="; ".join(changed_fields))
        if sorted(current_module_codes) != sorted(selected_modules):
            log_activity(current_user.username, "role_changed", "user",
                         target.username,
                         details=f"modules={','.join(selected_modules) or 'none'}")
        if role_changed:
            create_notification(
                user_id=target.id,
                title="Your role has been updated",
                message=(
                    f"{current_user.full_name or current_user.username} changed your role "
                    f"from '{previous_role_name}' to '{role.name}'."
                ),
                severity="warning",
                link="/dashboard",
            )
        invalidate_user_module_access_cache(target.id)
        db.session.commit()

        flash(f"User '{target.username}' updated successfully.", "success")
        return redirect(url_for("admin.users"))

    return render_template(
        "admin/edit_user.html",
        target=target,
        roles=roles,
        offices=offices,
        officers=officers,
        supported_modules=_module_options(),
        current_module_codes=current_module_codes,
        form_data={},
    )


# ── Reset Password ───────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/reset-password", methods=["GET", "POST"])
@login_required
@roles_required(ADMIN_ROLE)
def reset_user_password(user_id):
    target = User.query.get_or_404(user_id)

    if target.id == current_user.id:
        flash("Use /change-password to update your own password.", "warning")
        return redirect(url_for("admin.users"))

    if request.method == "POST":
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")

        errors = []
        if not new_password:
            errors.append("New password is required.")
        elif len(new_password) < 6:
            errors.append("Password must be at least 6 characters.")
        if new_password and new_password != confirm_password:
            errors.append("Passwords do not match.")

        if errors:
            for err in errors:
                flash(err, "danger")
            return render_template("admin/reset_user_password.html", target=target)

        target.set_password(new_password)
        target.must_change_password = True
        db.session.flush()

        AuditLog.log(
            action="USER_PASSWORD_RESET",
            user_id=current_user.id,
            entity_type="User",
            entity_id=str(target.id),
            details=(
                f"Admin '{current_user.username}' reset password for user '{target.username}'. "
                f"must_change_password set to True."
            ),
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )

        log_activity(current_user.username, "password_reset", "user",
                     target.username)
        db.session.commit()

        flash(
            f"Password for '{target.username}' has been reset. "
            "They will be required to change it on next login.",
            "success",
        )
        return redirect(url_for("admin.users"))

    return render_template("admin/reset_user_password.html", target=target)


# ── Toggle Active ────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/toggle-active", methods=["POST"])
@login_required
@roles_required(ADMIN_ROLE)
def toggle_user_active(user_id):
    target = User.query.get_or_404(user_id)

    if target.id == current_user.id:
        flash("You cannot deactivate your own account.", "warning")
        return redirect(url_for("admin.users"))

    if target.is_active:
        target.is_active = False
        action = "USER_DEACTIVATED"
        verb = "deactivated"
    else:
        target.is_active = True
        action = "USER_ACTIVATED"
        verb = "activated"

    db.session.flush()

    AuditLog.log(
        action=action,
        user_id=current_user.id,
        entity_type="User",
        entity_id=str(target.id),
        details=f"Admin '{current_user.username}' {verb} user '{target.username}'.",
        ip_address=_client_ip(),
        user_agent=get_user_agent(),
    )
    invalidate_user_module_access_cache(target.id)

    activity_action = "user_deactivated" if verb == "deactivated" else "user_activated"
    log_activity(current_user.username, activity_action, "user", target.username)
    db.session.commit()

    flash(f"User '{target.username}' has been {verb}.", "success")
    return redirect(url_for("admin.users"))


@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@roles_required(ADMIN_ROLE)
def delete_user(user_id):
    target = User.query.get_or_404(user_id)

    if target.id == current_user.id:
        flash("You cannot delete your own account.", "warning")
        return redirect(url_for("admin.users"))

    if target.is_active:
        flash("Deactivate the user before deleting the account.", "warning")
        return redirect(url_for("admin.users"))

    target_username = target.username

    try:
        User.query.filter_by(controlling_officer_id=target.id).update(
            {User.controlling_officer_id: None},
            synchronize_session=False,
        )
        User.query.filter_by(reviewing_officer_id=target.id).update(
            {User.reviewing_officer_id: None},
            synchronize_session=False,
        )
        Task.query.filter_by(owner_id=target.id).update(
            {Task.owner_id: None},
            synchronize_session=False,
        )
        Task.query.filter_by(created_by=target.id).update(
            {Task.created_by: None},
            synchronize_session=False,
        )
        RecurringTaskTemplate.query.filter_by(owner_id=target.id).update(
            {RecurringTaskTemplate.owner_id: None},
            synchronize_session=False,
        )
        RecurringTaskTemplate.query.filter_by(created_by=target.id).update(
            {RecurringTaskTemplate.created_by: None},
            synchronize_session=False,
        )
        TaskUpdate.query.filter_by(updated_by=target.id).update(
            {TaskUpdate.updated_by: None},
            synchronize_session=False,
        )
        TaskCollaborator.query.filter_by(user_id=target.id).delete(
            synchronize_session=False
        )
        RecurringTaskCollaborator.query.filter_by(user_id=target.id).delete(
            synchronize_session=False
        )
        UserModulePermission.query.filter_by(user_id=target.id).delete(
            synchronize_session=False
        )
        Notification.query.filter_by(user_id=target.id).delete(
            synchronize_session=False
        )

        db.session.delete(target)
        db.session.flush()

        db.session.add(
            AuditLog(
                action="USER_DELETED",
                user_id=current_user.id,
                entity_type="User",
                entity_id=str(user_id),
                details=(
                    f"Admin '{current_user.username}' deleted user "
                    f"'{target_username}' after deactivation."
                ),
                ip_address=AuditLog._normalize_ip(_client_ip()),
                user_agent=AuditLog._normalize_user_agent(get_user_agent()),
            )
        )
        log_activity(current_user.username, "user_deleted", "user", target_username)
        invalidate_user_module_access_cache(user_id)
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        flash(f"Could not delete user '{target_username}' due to a database error.", "danger")
        return redirect(url_for("admin.users"))

    flash(f"User '{target_username}' has been deleted.", "success")
    return redirect(url_for("admin.users"))


# ── Activity History ────────────────────────────────────────────
@admin_bp.route("/activity")
@login_required
@roles_required(SUPERUSER_ROLE)
def activity_history():
    page = request.args.get("page", 1, type=int)
    per_page = 30
    action_filter = request.args.get("action", "").strip()
    actor_filter = request.args.get("actor", "").strip()

    query = ActivityLog.query

    if action_filter:
        query = query.filter(ActivityLog.action_type == action_filter)
    if actor_filter:
        query = query.filter(ActivityLog.actor_username == actor_filter)

    pagination = (
        query
        .order_by(ActivityLog.created_at.desc())
        .paginate(page=page, per_page=per_page, error_out=False)
    )

    # Distinct action types and actors for filter dropdowns
    action_types = [
        r[0] for r in
        db.session.query(ActivityLog.action_type)
        .distinct()
        .order_by(ActivityLog.action_type)
        .all()
    ]
    actors = [
        r[0] for r in
        db.session.query(ActivityLog.actor_username)
        .filter(ActivityLog.actor_username.isnot(None))
        .distinct()
        .order_by(ActivityLog.actor_username)
        .all()
    ]

    return render_template(
        "admin/activity_history.html",
        pagination=pagination,
        activities=pagination.items,
        action_types=action_types,
        actors=actors,
        filters={"action": action_filter, "actor": actor_filter},
    )


@admin_bp.route("/backups")
@login_required
@roles_required(ADMIN_ROLE)
def backup_center():
    snapshots = []
    history_available = True
    try:
        snapshots = (
            BackupSnapshot.query
            .order_by(BackupSnapshot.created_at.desc())
            .limit(20)
            .all()
        )
    except SQLAlchemyError:
        db.session.rollback()
        history_available = False

    return render_template(
        "admin/backups.html",
        snapshots=snapshots,
        history_available=history_available,
        environment_name=get_runtime_environment_name(),
    )


@admin_bp.route("/backups/export", methods=["POST"])
@login_required
@roles_required(ADMIN_ROLE)
def export_backup():
    try:
        artifact = create_database_backup()
    except BackupError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("admin.backup_center"))

    try:
        db.session.add(
            BackupSnapshot(
                filename=artifact.download_name,
                created_by_username=current_user.username,
                environment=get_runtime_environment_name(),
                notes="Generated from the admin Backup Center and streamed immediately.",
            )
        )
        db.session.add(
            AuditLog(
                action="DATABASE_BACKUP_EXPORTED",
                user_id=current_user.id,
                entity_type="BackupSnapshot",
                entity_id=artifact.download_name,
                details=(
                    f"Admin '{current_user.username}' exported database backup "
                    f"'{artifact.download_name}'."
                ),
                ip_address=AuditLog._normalize_ip(_client_ip()),
                user_agent=AuditLog._normalize_user_agent(get_user_agent()),
            )
        )
        log_activity(
            current_user.username,
            "backup_exported",
            "backup",
            artifact.download_name,
            details=f"environment={get_runtime_environment_name()}",
        )
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()

    response = send_file(
        artifact.temp_path,
        as_attachment=True,
        download_name=artifact.download_name,
        mimetype="application/gzip",
        max_age=0,
    )
    response.call_on_close(artifact.cleanup)
    return response
