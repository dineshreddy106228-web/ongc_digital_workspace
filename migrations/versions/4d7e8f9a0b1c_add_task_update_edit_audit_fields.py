"""add task update edit audit fields

Revision ID: 4d7e8f9a0b1c
Revises: 2a3b4c5d6e7f
Create Date: 2026-03-27 10:15:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4d7e8f9a0b1c"
down_revision = "2a3b4c5d6e7f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("task_updates", schema=None) as batch_op:
        batch_op.add_column(sa.Column("edited_by", sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column("edited_at", sa.DateTime(), nullable=True))
        batch_op.create_foreign_key(
            "fk_task_updates_edited_by_users",
            "users",
            ["edited_by"],
            ["id"],
        )


def downgrade():
    with op.batch_alter_table("task_updates", schema=None) as batch_op:
        batch_op.drop_constraint("fk_task_updates_edited_by_users", type_="foreignkey")
        batch_op.drop_column("edited_at")
        batch_op.drop_column("edited_by")
