"""Custom decorators for route protection."""

from functools import wraps
from flask import abort, flash, redirect, url_for
from flask_login import current_user
from app.features import is_module_enabled
from app.core.permissions import user_has_permission

def roles_required(*role_names):
    """Restrict a view to users whose role name is in *role_names*.
    Preserved for backwards compatibility with existing routes.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated:
                flash("Please log in to access this page.", "warning")
                return redirect(url_for("auth.login"))
            if not current_user.is_active:
                flash("Your account has been deactivated.", "danger")
                return redirect(url_for("auth.login"))
            if not current_user.has_role(*role_names):
                flash("You do not have permission to access this page.", "danger")
                abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def active_user_required(fn):
    """Reject deactivated accounts even if a session cookie persists."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_active:
            flash("Your account has been deactivated.", "danger")
            return redirect(url_for("auth.login"))
        return fn(*args, **kwargs)
    return wrapper


def module_access_required(module_code: str):
    """
    Restrict a view to users who have been granted access to *module_code*.

    Access is granted when ANY of these is true:
      - The module is enabled in the current environment.
      - User is super_user (has access to all modules automatically).
      - User has an explicit UserModulePermission entry with can_access=True.

    Usage:
        @module_access_required("tasks")
        @module_access_required("csc")
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated:
                flash("Please log in to access this page.", "warning")
                return redirect(url_for("auth.login"))
            if not current_user.is_active:
                flash("Your account has been deactivated.", "danger")
                return redirect(url_for("auth.login"))
            # Route guards still enforce feature flags even if a URL is guessed.
            if not is_module_enabled(module_code):
                abort(404)
            if current_user.has_module_access(module_code):
                return fn(*args, **kwargs)
            flash(
                f"You do not have access to this module. "
                "Contact your administrator if you believe this is an error.",
                "danger",
            )
            abort(403)
        return wrapper
    return decorator


def superuser_required(fn):
    """Restrict a view to users with the 'superuser' role.

    Intended to gate pages that only superusers should access (e.g. the
    Master Data editor in Inventory Intelligence).  Combines authentication
    + active-account checks with the superuser role check.

    Usage::

        @inventory_bp.route("/master-data")
        @login_required
        @superuser_required
        def master_data():
            ...
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            flash("Please log in to access this page.", "warning")
            return redirect(url_for("auth.login"))
        if not current_user.is_active:
            flash("Your account has been deactivated.", "danger")
            return redirect(url_for("auth.login"))
        if not current_user.is_super_user():
            flash(
                "This page is restricted to superusers.",
                "danger",
            )
            abort(403)
        return fn(*args, **kwargs)
    return wrapper


def module_admin_required(module_code: str):
    """Restrict a view to users assigned as admins for the given module."""

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated:
                flash("Please log in to access this page.", "warning")
                return redirect(url_for("auth.login"))
            if not current_user.is_active:
                flash("Your account has been deactivated.", "danger")
                return redirect(url_for("auth.login"))
            if not is_module_enabled(module_code):
                abort(404)
            if not current_user.is_module_admin(module_code):
                flash(
                    "This page is restricted to the assigned module admins.",
                    "danger",
                )
                abort(403)
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def require_permission(permission_name: str):
    """Restrict a view to users whose role grants *permission_name*.

    Permission→role mappings live in app.core.roles.ROLE_PERMISSIONS and are
    resolved at request time — no DB query is needed beyond what Flask-Login
    already loaded for current_user.

    Usage::

        from app.core.utils.decorators import require_permission

        @admin_bp.route("/users/create")
        @login_required
        @require_permission("manage_users")
        def create_user():
            ...

    Precedence: authentication → account active → permission check.
    A 403 is raised (not a redirect) so AJAX callers get a proper status code.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user.is_authenticated:
                flash("Please log in to access this page.", "warning")
                return redirect(url_for("auth.login"))
            if not current_user.is_active:
                flash("Your account has been deactivated.", "danger")
                return redirect(url_for("auth.login"))
            if not user_has_permission(current_user, permission_name):
                flash(
                    "You do not have permission to perform this action.",
                    "danger",
                )
                abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return decorator
