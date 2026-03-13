"""user governance v2 – hierarchy fields, module permissions, task scope

Revision ID: 3f9a21c85d4e
Revises: 2a215c8379ab
Create Date: 2026-03-13 00:00:00.000000

Changes:
  1. users table  – ADD controlling_officer_id, reviewing_officer_id
  2. tasks table  – ADD task_scope (default 'MY')
  3. NEW TABLE     – user_module_permissions
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3f9a21c85d4e'
down_revision = '2a215c8379ab'
branch_labels = None
depends_on = None


def upgrade():
    # ── 1. Add hierarchy columns to users ──────────────────────────
    op.add_column(
        'users',
        sa.Column('controlling_officer_id', sa.BigInteger(), nullable=True),
    )
    op.add_column(
        'users',
        sa.Column('reviewing_officer_id', sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        'fk_users_controlling_officer',
        'users', 'users',
        ['controlling_officer_id'], ['id'],
        ondelete='SET NULL',
    )
    op.create_foreign_key(
        'fk_users_reviewing_officer',
        'users', 'users',
        ['reviewing_officer_id'], ['id'],
        ondelete='SET NULL',
    )

    # ── 2. Add task_scope to tasks ─────────────────────────────────
    op.add_column(
        'tasks',
        sa.Column(
            'task_scope',
            sa.String(length=50),
            nullable=False,
            server_default='MY',
        ),
    )

    # ── 3. Create user_module_permissions table ─────────────────────
    op.create_table(
        'user_module_permissions',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('user_id', sa.BigInteger(), nullable=False),
        sa.Column('module_code', sa.String(length=100), nullable=False),
        sa.Column('can_access', sa.Boolean(), nullable=False, server_default=sa.text('1')),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ['user_id'], ['users.id'],
            name='fk_ump_user',
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'module_code', name='uq_user_module'),
    )
    op.create_index(
        'ix_user_module_permissions_user_id',
        'user_module_permissions',
        ['user_id'],
    )


def downgrade():
    # Drop in reverse order
    op.drop_index('ix_user_module_permissions_user_id', table_name='user_module_permissions')
    op.drop_table('user_module_permissions')

    op.drop_column('tasks', 'task_scope')

    op.drop_constraint('fk_users_reviewing_officer', 'users', type_='foreignkey')
    op.drop_constraint('fk_users_controlling_officer', 'users', type_='foreignkey')
    op.drop_column('users', 'reviewing_officer_id')
    op.drop_column('users', 'controlling_officer_id')
