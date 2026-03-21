"""add private self-task flags for local task visibility

Revision ID: e4f5a6b7c8d9
Revises: c9f4e1a2b3d6
Create Date: 2026-03-21 18:10:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e4f5a6b7c8d9"
down_revision = "c9f4e1a2b3d6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "is_private_self_task",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )

    with op.batch_alter_table("recurring_task_templates", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "is_private_self_task",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )
        batch_op.add_column(
            sa.Column(
                "self_task_visible_to_controlling_officer",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )

    # Preserve existing visibility semantics for already-created local tasks/templates
    # that have zero collaborators: before this revision they behaved as self-tasks.
    op.execute(
        """
        UPDATE tasks
        SET is_private_self_task = 1
        WHERE task_scope IN ('MY', 'TEAM', 'LOCAL')
          AND NOT EXISTS (
            SELECT 1
            FROM task_collaborators
            WHERE task_collaborators.task_id = tasks.id
          )
        """
    )

    op.execute(
        """
        UPDATE recurring_task_templates
        SET is_private_self_task = 1,
            self_task_visible_to_controlling_officer = 0
        WHERE task_scope IN ('MY', 'TEAM', 'LOCAL')
          AND NOT EXISTS (
            SELECT 1
            FROM recurring_task_collaborators
            WHERE recurring_task_collaborators.template_id = recurring_task_templates.id
          )
        """
    )


def downgrade():
    with op.batch_alter_table("recurring_task_templates", schema=None) as batch_op:
        batch_op.drop_column("self_task_visible_to_controlling_officer")
        batch_op.drop_column("is_private_self_task")

    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_column("is_private_self_task")
