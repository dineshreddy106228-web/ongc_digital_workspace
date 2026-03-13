"""Admin blueprint routes – User Management with User Governance V2.

Governance V2 additions:
  - Controlling Officer / Reviewing Officer assignment
  - Module-level access permissions (checkboxes)
  - Audit logging for governance changes
  - User category reflected via role assignment
"""

from flask import render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app.admin import admin_bp
from app.extensions import db
from app.models.user import User
from app.models.role import Role
from app.models.office import Office
from app.models.audit_log import AuditLog
from app.models.user_module_permission import (
    UserModulePermission,
    SUPPORTED_MODULES,
    SUPER_USER_MODULES,
)
from app.utils.decorators import roles_required
from app.utils.request_meta import get_client_ip, get_user_agent


# ── Helper ──────────────────────────────────────────────────────
def _client_ip():
    return get_client_ip()


def _set_module_permissions(user: User, selected_codes: list, role: Role):
    """
    Clear existing module permissions for *user* and set fresh ones.

    super_user → all SUPER_USER_MODULES (runtime bypass, but persisted for UI)
    user       → exactly what is in selected_codes (admin-controlled)
    """
    UserModulePermission.query.filter_by(user_id=user.id).delete()
    db.session.flush()

    if role and role.name == "super_user":
        codes_to_save = list(SUPER_USER_MODULES)
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


# ── Users List ───────────────────────────────────────────────────
@admin_bp.route("/users")
@login_required
@roles_required("super_user")
def users():
    all_users = User.query.order_by(User.created_at.desc()).all()
    return render_template("admin/users.html", users=all_users)


# ── Create User ──────────────────────────────────────────────────
@admin_bp.route("/users/create", methods=["GET", "POST"])
@login_required
@roles_required("super_user")
def create_user():
    roles = Role.query.filter(Role.is_active == True, Role.name.in_(["user", "super_user"])).order_by(Role.name).all()
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
                supported_modules=SUPPORTED_MODULES,
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
        supported_modules=SUPPORTED_MODULES,
        form_data={},
        selected_modules=[],
    )


# ── Edit User ────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@login_required
@roles_required("super_user")
def edit_user(user_id):
    target = User.query.get_or_404(user_id)
    roles = Role.query.filter(Role.is_active == True, Role.name.in_(["user", "super_user"])).order_by(Role.name).all()
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
                supported_modules=SUPPORTED_MODULES,
                current_module_codes=selected_modules,
                form_data=request.form,
            )

        # ── Apply changes ──────────────────────────────────────────
        changed_fields = []
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

        flash(f"User '{target.username}' updated successfully.", "success")
        return redirect(url_for("admin.users"))

    return render_template(
        "admin/edit_user.html",
        target=target,
        roles=roles,
        offices=offices,
        officers=officers,
        supported_modules=SUPPORTED_MODULES,
        current_module_codes=current_module_codes,
        form_data={},
    )


# ── Reset Password ───────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/reset-password", methods=["GET", "POST"])
@login_required
@roles_required("super_user")
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
@roles_required("super_user")
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

    flash(f"User '{target.username}' has been {verb}.", "success")
    return redirect(url_for("admin.users"))
