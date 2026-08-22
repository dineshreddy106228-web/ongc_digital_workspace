"""Single source of truth for which tasks a user is allowed to see.

Both the Office Management task list and the home page counts read from here.
When they disagree, a home tile shows a number the user cannot reconcile with
the list it links to — so this rule must have exactly one definition.
"""

from __future__ import annotations

from sqlalchemy import and_, or_, select

from app.core.roles import ADMIN_ROLE
from app.extensions import db
from app.models.core.user import User
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator


def is_privileged(user) -> bool:
    """True for superusers and admins – full task visibility."""
    return user.is_super_user() or user.has_role(ADMIN_ROLE)


def task_visibility_query(user):
    """Build a Task query restricted to what *user* may see.

    Privileged (superuser or admin):
        → all tasks

    Standard users:
        → GLOBAL tasks
        → tasks they own
        → tasks they collaborate on
        → non-private MY/TEAM tasks inside their own office
        → MY/TEAM tasks of users they control, unless marked private
    """
    base = Task.query

    if is_privileged(user):
        return base

    conds = [
        Task.task_scope == "GLOBAL",
        Task.owner_id == user.id,
    ]

    collaborator_task_ids = (
        db.session.query(TaskCollaborator.task_id)
        .filter_by(user_id=user.id)
        .subquery()
    )
    conds.append(Task.id.in_(select(collaborator_task_ids.c.task_id)))

    if user.office_id is not None:
        conds.append(
            and_(
                Task.task_scope.in_(["MY", "TEAM"]),
                Task.office_id == user.office_id,
                Task.is_private_self_task.is_(False),
            )
        )

    controlled_ids = (
        db.session.query(User.id)
        .filter_by(controlling_officer_id=user.id, is_active=True)
        .subquery()
    )
    controlled_collaborator_task_ids = (
        db.session.query(TaskCollaborator.task_id)
        .filter(TaskCollaborator.user_id.in_(select(controlled_ids.c.id)))
        .subquery()
    )
    conds.append(
        and_(
            Task.task_scope.in_(["MY", "TEAM"]),
            or_(
                and_(
                    Task.owner_id.in_(select(controlled_ids.c.id)),
                    or_(
                        Task.is_private_self_task.is_(False),
                        Task.self_task_visible_to_controlling_officer.is_(True),
                    ),
                ),
                Task.id.in_(select(controlled_collaborator_task_ids.c.task_id)),
            ),
        )
    )

    return base.filter(or_(*conds))
