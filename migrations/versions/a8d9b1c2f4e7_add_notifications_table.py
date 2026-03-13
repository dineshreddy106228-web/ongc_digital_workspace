"""add notifications table

Revision ID: a8d9b1c2f4e7
Revises: 188c95ba1fed
Create Date: 2026-03-13 22:05:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a8d9b1c2f4e7"
down_revision = "188c95ba1fed"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "notifications",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("link", sa.String(length=255), nullable=True),
        sa.Column("is_read", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    with op.batch_alter_table("notifications", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_notifications_created_at"),
            ["created_at"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_notifications_is_read"),
            ["is_read"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_notifications_user_id"),
            ["user_id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_notifications_user_created_at",
            ["user_id", "created_at"],
            unique=False,
        )
        batch_op.create_index(
            "ix_notifications_user_is_read",
            ["user_id", "is_read"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("notifications", schema=None) as batch_op:
        batch_op.drop_index("ix_notifications_user_is_read")
        batch_op.drop_index("ix_notifications_user_created_at")
        batch_op.drop_index(batch_op.f("ix_notifications_user_id"))
        batch_op.drop_index(batch_op.f("ix_notifications_is_read"))
        batch_op.drop_index(batch_op.f("ix_notifications_created_at"))

    op.drop_table("notifications")
