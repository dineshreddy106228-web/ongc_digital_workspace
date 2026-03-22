"""
app/core/permissions/task_permissions.py — Centralised task permission engine.

╔══════════════════════════════════════════════════════════════════════╗
║  DENY-BY-DEFAULT PHILOSOPHY                                        ║
║                                                                    ║
║  Every function in this module returns ``False`` unless an         ║
║  explicit rule grants access.  There are no catch-all              ║
║  ``return True`` branches — each grant is intentional and          ║
║  auditable.                                                        ║
║                                                                    ║
║  Permission denials are logged at WARNING level so that            ║
║  security audits can trace rejected access attempts without        ║
║  application errors.                                               ║
║                                                                    ║
║  PURE FUNCTIONS — no DB writes, no side-effects.  These may be    ║
║  called from routes, templates (via context processor), CLI, and   ║
║  background jobs without concern for transaction boundaries.       ║
╚══════════════════════════════════════════════════════════════════════╝

Permission Matrix
─────────────────
Scenario                          │ Rule
──────────────────────────────────┼──────────────────────────────────────────
Admin (role)                      │ Always True for all functions
Owner (any role)                  │ Can view, edit, close own tasks
Self-task (no collaborators)      │ Owner only; controlling officer if
                                  │   task.self_task_visible_to_controlling_officer
MY-scope task                     │ Owner + collaborators + same-office users
                                  │   + office Superusers/Admins
GLOBAL-scope task                 │ Owner + collaborators + users in tagged
                                  │   offices + Superusers of tagged offices
                                  │   + Admins
Create global task                │ Superuser and Admin only
Manage offices                    │ Admin only
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.core.roles import ADMIN_ROLE, SUPERUSER_ROLE

if TYPE_CHECKING:
    from app.models.core.user import User
    from app.models.tasks.task import Task

logger = logging.getLogger(__name__)


# ─── Internal Helpers ────────────────────────────────────────────────────────

def _is_admin(user: "User") -> bool:
    """Return True when *user* holds the Admin role."""
    return bool(user and getattr(user, "is_active", False) and user.has_role(ADMIN_ROLE))


def _is_superuser(user: "User") -> bool:
    """Return True when *user* holds the Superuser role."""
    return bool(user and getattr(user, "is_active", False) and user.has_role(SUPERUSER_ROLE))


def _is_owner(user: "User", task: "Task") -> bool:
    """Return True when *user* is the task owner."""
    if not user or not task:
        return False
    return (
        task.owner_id is not None
        and int(task.owner_id) == int(user.id)
    )


def _is_creator(user: "User", task: "Task") -> bool:
    """Return True when *user* originally created the task."""
    if not user or not task:
        return False
    return (
        task.created_by is not None
        and int(task.created_by) == int(user.id)
    )


def _is_collaborator(user: "User", task: "Task") -> bool:
    """Return True when *user* is an explicit collaborator on *task*."""
    if not user or not task:
        return False
    uid = int(user.id)
    for link in getattr(task, "collaborator_links", []):
        if getattr(link, "user_id", None) is not None and int(link.user_id) == uid:
            return True
    return False


def _collaborator_user_ids(task: "Task") -> set[int]:
    """Return the set of user IDs who are collaborators on *task*."""
    return {
        int(link.user_id)
        for link in getattr(task, "collaborator_links", [])
        if getattr(link, "user_id", None) is not None
    }


def _is_self_task(task: "Task") -> bool:
    """A self-task is explicitly private and has zero collaborators."""
    return bool(
        getattr(task, "is_private_self_task", False)
        and len(_collaborator_user_ids(task)) == 0
    )


def _same_office(user: "User", task: "Task") -> bool:
    """Return True when *user* and *task* share the same office."""
    if not user or not task:
        return False
    return (
        user.office_id is not None
        and task.office_id is not None
        and int(user.office_id) == int(task.office_id)
    )


def _is_controlling_officer_of_owner(user: "User", task: "Task") -> bool:
    """Return True when *user* is the controlling officer of the task owner."""
    if not user or not task:
        return False
    owner = getattr(task, "owner", None)
    if owner is None:
        return False
    return (
        owner.controlling_officer_id is not None
        and int(owner.controlling_officer_id) == int(user.id)
    )


def _is_in_participant_hierarchy(user: "User", task: "Task") -> bool:
    """
    Return True when *user* is the controlling officer of *any* participant
    (owner or collaborator) on the task.
    """
    if not user or not task:
        return False

    uid = int(user.id)

    # Check owner's controlling officer
    owner = getattr(task, "owner", None)
    if owner and getattr(owner, "controlling_officer_id", None) is not None:
        if int(owner.controlling_officer_id) == uid:
            return True

    # Check each collaborator's controlling officer
    for link in getattr(task, "collaborator_links", []):
        collab_user = getattr(link, "user", None)
        if collab_user and getattr(collab_user, "controlling_officer_id", None) is not None:
            if int(collab_user.controlling_officer_id) == uid:
                return True

    return False


def _user_in_tagged_offices(user: "User", task: "Task") -> bool:
    """
    Return True when *user*'s office is among the offices tagged on a
    GLOBAL task via the task_offices junction table.

    Gracefully returns False if the ``tagged_offices`` relationship is
    not yet available (pre-migration).
    """
    if not user or not task:
        return False
    if user.office_id is None:
        return False

    tagged = getattr(task, "tagged_offices", None)
    if tagged is None:
        return False

    uid_office = int(user.office_id)
    for office in tagged:
        if getattr(office, "id", None) is not None and int(office.id) == uid_office:
            return True
    return False


def _superuser_of_tagged_office(user: "User", task: "Task") -> bool:
    """
    Return True when *user* is a Superuser whose office is among the
    offices tagged on a GLOBAL task.
    """
    return _is_superuser(user) and _user_in_tagged_offices(user, task)


def _deny(func_name: str, user: "User", task: "Task | None" = None) -> bool:
    """Log a permission denial and return False."""
    user_repr = getattr(user, "username", "?") if user else "anonymous"
    task_repr = f"Task#{getattr(task, 'id', '?')}" if task else "N/A"
    logger.warning(
        "PERMISSION DENIED: %s | user=%s | task=%s",
        func_name,
        user_repr,
        task_repr,
    )
    return False


# ─── Public Permission Functions ─────────────────────────────────────────────

def can_view_task(user: "User", task: "Task") -> bool:
    """
    Determine whether *user* may view *task*.

    Grant chain (first match wins):
      1. Admin                          → True
      2. Superuser                      → True
      3. Owner                          → True
      4. Collaborator                   → True
      5. Local self-task + CO flag on   → controlling officer of owner
      6. Local self-task                → deny (owner-only, already covered above)
      7. MY-scope: same office          → True
      8. MY-scope: controlling officer
         of any participant             → True
      9. GLOBAL-scope: any Superuser    → True
     10. GLOBAL-scope: user in tagged
         offices                        → True
     11. GLOBAL-scope: superuser of
         tagged office                  → True  (subset of 8, explicit for clarity)
    """
    if not user or not task:
        return _deny("can_view_task", user, task)

    # 1. Admin — full access
    if _is_admin(user):
        return True

    # 2. Superuser — full business access
    if _is_superuser(user):
        return True

    # 3. Owner — always sees own task
    if _is_owner(user, task):
        return True

    # 4. Collaborator
    if _is_collaborator(user, task):
        return True

    scope = (getattr(task, "task_scope", "") or "").strip().upper()

    # 4–7. MY-scope (local) visibility
    if scope in ("MY", "LOCAL", "TEAM", ""):
        if _is_self_task(task):
            visible_to_co = getattr(task, "self_task_visible_to_controlling_officer", False)
            if visible_to_co and _is_controlling_officer_of_owner(user, task):
                return True
            return _deny("can_view_task", user, task)
        if _same_office(user, task):
            return True
        if _is_in_participant_hierarchy(user, task):
            return True
        # Superusers of the task's office
        if _is_superuser(user) and _same_office(user, task):
            return True
        return _deny("can_view_task", user, task)

    # 8–10. GLOBAL-scope visibility
    if scope == "GLOBAL":
        if _is_superuser(user):
            return True
        if _user_in_tagged_offices(user, task):
            return True
        if _same_office(user, task):
            return True
        if _is_in_participant_hierarchy(user, task):
            return True
        return _deny("can_view_task", user, task)

    return _deny("can_view_task", user, task)


def can_edit_task(user: "User", task: "Task") -> bool:
    """
    Determine whether *user* may edit *task*.

    Grant chain:
      1. Admin              → True
      2. Superuser          → True
      3. Owner              → True
      4. Creator            → True
      5. Otherwise          → deny
    """
    if not user or not task:
        return _deny("can_edit_task", user, task)

    if _is_admin(user):
        return True
    if _is_superuser(user):
        return True
    if _is_owner(user, task):
        return True
    if _is_creator(user, task):
        return True

    return _deny("can_edit_task", user, task)


def can_close_task(user: "User", task: "Task") -> bool:
    """
    Determine whether *user* may close (complete / cancel) *task*.

    Same grant chain as edit — closing is a terminal edit.
    """
    if not user or not task:
        return _deny("can_close_task", user, task)

    if _is_admin(user):
        return True
    if _is_superuser(user):
        return True
    if _is_owner(user, task):
        return True
    if _is_creator(user, task):
        return True

    return _deny("can_close_task", user, task)


def can_add_update(user: "User", task: "Task") -> bool:
    """
    Determine whether *user* may add a remark / status update to *task*.

    Grant chain:
      1. Admin                          → True
      2. Superuser                      → True
      3. Owner                          → True
      4. Collaborator                   → True
      5. Controlling officer of any
         participant                    → True
      6. Otherwise                      → deny
    """
    if not user or not task:
        return _deny("can_add_update", user, task)

    if _is_admin(user):
        return True
    if _is_superuser(user):
        return True
    if _is_owner(user, task):
        return True
    if _is_collaborator(user, task):
        return True
    if _is_in_participant_hierarchy(user, task):
        return True

    return _deny("can_add_update", user, task)


def can_create_global_task(user: "User") -> bool:
    """
    Determine whether *user* may create a GLOBAL-scope task.

    Only Superuser and Admin roles are permitted.
    """
    if not user:
        return _deny("can_create_global_task", user)

    if _is_admin(user):
        return True
    if _is_superuser(user):
        return True

    return _deny("can_create_global_task", user)


def can_manage_offices(user: "User") -> bool:
    """
    Determine whether *user* may create, edit, or deactivate offices.

    Admin only.
    """
    if not user:
        return _deny("can_manage_offices", user)

    if _is_admin(user):
        return True

    return _deny("can_manage_offices", user)
