"""
app/core/permissions/__init__.py — Single source of truth for permissions logic.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from app.core.roles import ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE, canonicalize_role_name

if TYPE_CHECKING:
    from app.models.core.user import User

# ── Permission registry ───────────────────────────────────────────────────────
# Permissions are simple strings: "<verb>_<noun>", lowercase + underscores.

ROLE_PERMISSIONS: dict[str, list[str]] = {
    ADMIN_ROLE: [
        "manage_users",
        "manage_roles",
        "reset_passwords",
    ],
    SUPERUSER_ROLE: [
        "view_dashboard",
        "manage_tasks",
        "view_activity",
    ],
    USER_ROLE: [
        "view_dashboard",
    ],
}

_ALL_PERMISSIONS: frozenset[str] = frozenset(
    p for perms in ROLE_PERMISSIONS.values() for p in perms
)

def get_permissions_for_role(role_name: str) -> list[str]:
    """Return the permission list for *role_name*."""
    return list(ROLE_PERMISSIONS.get(canonicalize_role_name(role_name), []))

def role_has_permission(role_name: str, permission_name: str) -> bool:
    """Return True if the *role_name* role grants *permission_name*."""
    return permission_name in ROLE_PERMISSIONS.get(canonicalize_role_name(role_name), [])

def user_has_permission(user: "User", permission_name: str) -> bool:
    """Return True if *user* holds the given permission through their role."""
    if user is None:
        return False
    if not getattr(user, "is_active", False):
        return False
    role = getattr(user, "role", None)
    if role is None:
        return False
    return role_has_permission(role.name, permission_name)

def get_all_permissions() -> list[str]:
    """Return a sorted list of every permission string defined in the registry."""
    return sorted(_ALL_PERMISSIONS)


# ── Task-level permission functions (re-exported for convenience) ────────────
# Canonical implementations live in task_permissions.py within this package.
# Import here so consumers can do:
#   from app.core.permissions import can_view_task, can_edit_task, ...

from app.core.permissions.task_permissions import (  # noqa: E402, F401
    can_view_task,
    can_edit_task,
    can_close_task,
    can_add_update,
    can_create_global_task,
    can_manage_offices,
)
