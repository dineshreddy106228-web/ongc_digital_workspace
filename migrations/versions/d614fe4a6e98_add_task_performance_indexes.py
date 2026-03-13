"""add task performance indexes

Revision ID: d614fe4a6e98
Revises: 4b7c30d91f2a
Create Date: 2026-03-13 07:06:02.695291

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'd614fe4a6e98'
down_revision = '4b7c30d91f2a'
branch_labels = None
depends_on = None


def upgrade():
    # Add task-related indexes only; keep existing FK semantics unchanged.
    with op.batch_alter_table('task_updates', schema=None) as batch_op:
        batch_op.create_index('ix_task_updates_task_id_created_at', ['task_id', 'created_at'], unique=False)

    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.create_index('ix_tasks_active_status_due', ['is_active', 'status', 'due_date'], unique=False)
        batch_op.create_index('ix_tasks_created_at', ['created_at'], unique=False)
        batch_op.create_index('ix_tasks_due_date', ['due_date'], unique=False)
        batch_op.create_index('ix_tasks_is_active', ['is_active'], unique=False)
        batch_op.create_index('ix_tasks_priority', ['priority'], unique=False)
        batch_op.create_index('ix_tasks_status', ['status'], unique=False)
        batch_op.create_index('ix_tasks_task_scope', ['task_scope'], unique=False)


def downgrade():
    # Drop only indexes added by this revision.
    with op.batch_alter_table('tasks', schema=None) as batch_op:
        batch_op.drop_index('ix_tasks_task_scope')
        batch_op.drop_index('ix_tasks_status')
        batch_op.drop_index('ix_tasks_priority')
        batch_op.drop_index('ix_tasks_is_active')
        batch_op.drop_index('ix_tasks_due_date')
        batch_op.drop_index('ix_tasks_created_at')
        batch_op.drop_index('ix_tasks_active_status_due')

    with op.batch_alter_table('task_updates', schema=None) as batch_op:
        batch_op.drop_index('ix_task_updates_task_id_created_at')
