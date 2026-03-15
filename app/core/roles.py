"""
app/core/roles.py — Single source of truth for roles and permissions.

This module owns:
  • Role name constants         (ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE)
  • Ordered role registry       (ROLE_REGISTRY)
  • Human-readable descriptions (ROLE_DESCRIPTIONS) — stored in DB Role.description
  • Legacy-name rename map      (ROLE_RENAME_MAP)   — used by `flask normalize-roles`

Do NOT import Flask, SQLAlchemy, or any app models here.
This module must be importable before the app is fully initialised
(e.g. inside CLI commands and migration scripts).
"""

from __future__ import annotations




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


def is_known_role(role_name: str) -> bool:
    """Return True if *role_name* is a canonical role in ROLE_REGISTRY."""
    return canonicalize_role_name(role_name) in ROLE_REGISTRY
