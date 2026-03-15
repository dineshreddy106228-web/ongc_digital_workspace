"""add task collaborators table

Revision ID: f7b8d2a9c1e4
Revises: d614fe4a6e98
Create Date: 2026-03-14 10:20:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f7b8d2a9c1e4"
down_revision = "d614fe4a6e98"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "task_collaborators",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "user_id", name="uq_task_collaborators_task_user"),
    )
    op.create_index(
        "ix_task_collaborators_user_id",
        "task_collaborators",
        ["user_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_task_collaborators_user_id", table_name="task_collaborators")
    op.drop_table("task_collaborators")
