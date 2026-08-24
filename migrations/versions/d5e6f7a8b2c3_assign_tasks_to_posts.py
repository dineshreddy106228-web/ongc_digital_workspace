"""Let a task be assigned to a post rather than to a person.

owner_id keeps tracking whoever currently holds the post, so visibility and
permissions are unchanged, and a handover moves the open work across on its
own. What each person already wrote stays attributed to them.

Revision ID: d5e6f7a8b2c3
Revises: c4d5e6f7a8b1
"""
import sqlalchemy as sa
from alembic import op

revision = "d5e6f7a8b2c3"
down_revision = "c4d5e6f7a8b1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("tasks", sa.Column("assigned_post_id", sa.BigInteger(), nullable=True))
    op.create_index("ix_tasks_assigned_post_id", "tasks", ["assigned_post_id"])
    op.create_foreign_key(
        "fk_tasks_assigned_post_id", "tasks", "office_posts", ["assigned_post_id"], ["id"]
    )


def downgrade():
    op.drop_constraint("fk_tasks_assigned_post_id", "tasks", type_="foreignkey")
    op.drop_index("ix_tasks_assigned_post_id", table_name="tasks")
    op.drop_column("tasks", "assigned_post_id")
