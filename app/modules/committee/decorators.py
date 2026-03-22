"""RBAC decorator for Committee Task routes."""

from functools import wraps
from flask import abort, flash, redirect, request, url_for
from flask_login import current_user


def committee_access_required():
    """Restrict access to committee-eligible roles.

    Access rules:
        admin                                    — full access (user-admin role)
        superuser                                — full access across all offices
        user (with committee module permission)  — full access (committee member equivalent)

    Returns 403 for any other role or user without committee module access.

    Note:
        There is no separate ``committee_member`` database role.  In this
        application, "committee member" means a ``user``-role user whose admin
        has granted the ``committee`` module permission via User Management.
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

            role_name = (
                current_user.role.name if current_user.role else ""
            ).lower()

            # Admin role — full platform administration, full committee access
            if role_name == "admin":
                return fn(*args, **kwargs)

            # Superuser — full access across all offices
            if role_name == "superuser":
                return fn(*args, **kwargs)

            # User role — requires explicit committee module permission
            if role_name == "user" and current_user.has_module_access("committee"):
                return fn(*args, **kwargs)

            flash(
                "You do not have permission to access committee tasks.",
                "danger",
            )
            abort(403)

        return wrapper

    return decorator
