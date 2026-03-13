"""
app/core/roles.py — Single source of truth for roles and permissions.

This module owns:
  • Role name constants         (ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE)
  • Ordered role registry       (ROLE_REGISTRY)
  • Human-readable descriptions (ROLE_DESCRIPTIONS) — stored in DB Role.description
  • Legacy-name rename map      (ROLE_RENAME_MAP)   — used by `flask normalize-roles`
  • Permission sets per role    (ROLE_PERMISSIONS)
  • Helper utilities            (get_all_roles, get_permissions_for_role,
                                  user_has_permission, role_has_permission)

Do NOT import Flask, SQLAlchemy, or any app models here.
This module must be importable before the app is fully initialised
(e.g. inside CLI commands and migration scripts).

Future extension points
-----------------------
• Add richer Permission objects (description, category, module tag) by
  replacing the plain string lists — all call-sites use get_permissions_for_role()
  so no other code needs changing.
• Add feature-flag-aware permission checks by extending user_has_permission()
  without touching any decorator or route.
• Scope permissions to modules by nesting under a module key rather than a
  flat list; the helper functions absorb that change transparently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Avoid a runtime circular import: User model imports app.features which
    # bootstraps the whole app.  We only need the type for static analysis.
    from app.models.user import User


# ── Role name constants ────────────────────────────────────────────────────────

ADMIN_ROLE     = "admin"       # User management only
SUPERUSER_ROLE = "superuser"   # Full platform access, no admin panel
USER_ROLE      = "user"        # Explicit module access controlled by admin

# ── Ordered registry ──────────────────────────────────────────────────────────
# Authority descends from index 0 (highest) to last (lowest).
# Used by: bootstrap commands, normalize-roles, admin dropdowns.

ROLE_REGISTRY: list[str] = [
    ADMIN_ROLE,
    SUPERUSER_ROLE,
    USER_ROLE,
]

# ── Human-readable descriptions (persisted to Role.description in DB) ─────────

ROLE_DESCRIPTIONS: dict[str, str] = {
    ADMIN_ROLE:     "User management access only — manage users, roles, and password resets",
    SUPERUSER_ROLE: "Full platform access — all modules, all task actions, activity history",
    USER_ROLE:      "Explicit module access only — controlled by admin",
}

# ── Legacy → canonical rename map ─────────────────────────────────────────────
# Used exclusively by `flask normalize-roles`.
# Key   = legacy DB role name to be eliminated.
# Value = canonical role name it maps to.

ROLE_RENAME_MAP: dict[str, str] = {
    "super_user": SUPERUSER_ROLE,
}

# ── Permission registry ───────────────────────────────────────────────────────
# Permissions are simple strings: "<verb>_<noun>", lowercase + underscores.
#
# Convention used throughout the app:
#   manage_*  → create / edit / delete the resource
#   view_*    → read-only access
#   reset_*   → destructive-but-scoped (e.g. password reset, cache flush)
#
# Note: module-level access (tasks, inventory, csc …) is governed by the
# separate UserModulePermission DB table.  The permissions below control
# coarser, route-level access decisions.

ROLE_PERMISSIONS: dict[str, list[str]] = {
    ADMIN_ROLE: [
        # --- User & role administration ---
        "manage_users",        # create / edit / deactivate user accounts
        "manage_roles",        # assign roles to users
        "reset_passwords",     # reset another user's password
    ],
    SUPERUSER_ROLE: [
        "view_dashboard",
        "manage_tasks",        # full task CRUD across all scopes
        "view_activity",       # activity timeline and audit history page
    ],
    USER_ROLE: [
        "view_dashboard",
    ],
}

# Derived: flat set of every known permission string (for validation helpers)
_ALL_PERMISSIONS: frozenset[str] = frozenset(
    p for perms in ROLE_PERMISSIONS.values() for p in perms
)


def canonicalize_role_name(role_name: str | None) -> str | None:
    """Map legacy role names to the canonical registry name."""
    if not role_name:
        return role_name
    return ROLE_RENAME_MAP.get(role_name, role_name)


# ── Public helpers ─────────────────────────────────────────────────────────────

def get_all_roles() -> list[str]:
    """Return the ordered list of canonical role names.

    >>> get_all_roles()
    ['admin', 'superuser', 'user']
    """
    return list(ROLE_REGISTRY)


def get_permissions_for_role(role_name: str) -> list[str]:
    """Return the permission list for *role_name*.

    Returns an empty list for unknown role names — never raises.

    >>> get_permissions_for_role("admin")
    ['manage_users', 'manage_roles', ...]
    >>> get_permissions_for_role("ghost")
    []
    """
    return list(ROLE_PERMISSIONS.get(canonicalize_role_name(role_name), []))


def role_has_permission(role_name: str, permission_name: str) -> bool:
    """Return True if the *role_name* role grants *permission_name*.

    >>> role_has_permission("admin", "manage_users")
    True
    >>> role_has_permission("user", "manage_users")
    False
    """
    return permission_name in ROLE_PERMISSIONS.get(canonicalize_role_name(role_name), [])


def user_has_permission(user: "User", permission_name: str) -> bool:
    """Return True if *user* holds the given permission through their role.

    Resolves permissions from ROLE_PERMISSIONS via the user's DB role name.
    Returns False for:
      • None / unauthenticated proxies
      • Inactive users
      • Users with no role assigned
      • Unknown permission names

    This function is pure Python — no DB queries, no Flask context needed.

    >>> user_has_permission(admin_user, "manage_users")
    True
    >>> user_has_permission(regular_user, "manage_users")
    False
    """
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


def is_known_role(role_name: str) -> bool:
    """Return True if *role_name* is a canonical role in ROLE_REGISTRY."""
    return canonicalize_role_name(role_name) in ROLE_REGISTRY
