"""add committee task tables

Revision ID: a8b9c0d1e2f3
Revises: f1a2b3c4d5e7
Create Date: 2026-03-22 14:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a8b9c0d1e2f3'
down_revision = 'f1a2b3c4d5e7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'committee_tasks',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('title', sa.String(length=255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('priority', sa.String(length=50), nullable=False, server_default='medium'),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='open'),
        sa.Column('due_date', sa.Date(), nullable=True),
        sa.Column('office_id', sa.BigInteger(), nullable=False),
        sa.Column('created_by', sa.BigInteger(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['office_id'], ['offices.id']),
        sa.ForeignKeyConstraint(['created_by'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_committee_tasks_office_id', 'committee_tasks', ['office_id'])
    op.create_index('ix_committee_tasks_created_by', 'committee_tasks', ['created_by'])
    op.create_index('ix_committee_tasks_status', 'committee_tasks', ['status'])
    op.create_index('ix_committee_tasks_priority', 'committee_tasks', ['priority'])
    op.create_index('ix_committee_tasks_due_date', 'committee_tasks', ['due_date'])

    op.create_table(
        'committee_task_members',
        sa.Column('task_id', sa.BigInteger(), nullable=False),
        sa.Column('user_id', sa.BigInteger(), nullable=False),
        sa.Column('role', sa.String(length=50), nullable=False, server_default='assignee'),
        sa.ForeignKeyConstraint(['task_id'], ['committee_tasks.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('task_id', 'user_id'),
    )

    op.create_table(
        'task_comments',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('task_id', sa.BigInteger(), nullable=False),
        sa.Column('user_id', sa.BigInteger(), nullable=False),
        sa.Column('body', sa.Text(), nullable=False),
        sa.Column('attachment_path', sa.String(length=512), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['task_id'], ['committee_tasks.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_task_comments_task_id', 'task_comments', ['task_id'])


def downgrade():
    op.drop_index('ix_task_comments_task_id', table_name='task_comments')
    op.drop_table('task_comments')
    op.drop_table('committee_task_members')
    op.drop_index('ix_committee_tasks_due_date', table_name='committee_tasks')
    op.drop_index('ix_committee_tasks_priority', table_name='committee_tasks')
    op.drop_index('ix_committee_tasks_status', table_name='committee_tasks')
    op.drop_index('ix_committee_tasks_created_by', table_name='committee_tasks')
    op.drop_index('ix_committee_tasks_office_id', table_name='committee_tasks')
    op.drop_table('committee_tasks')
