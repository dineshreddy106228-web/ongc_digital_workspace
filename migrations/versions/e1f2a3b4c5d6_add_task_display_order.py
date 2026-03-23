"""add shared task display order

Revision ID: e1f2a3b4c5d6
Revises: 188c95ba1fed
Create Date: 2026-03-23 08:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e1f2a3b4c5d6"
down_revision = "188c95ba1fed"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.add_column(sa.Column("display_order", sa.Integer(), nullable=True))
        batch_op.create_index("ix_tasks_display_order", ["display_order"], unique=False)


def downgrade():
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.drop_index("ix_tasks_display_order")
        batch_op.drop_column("display_order")
