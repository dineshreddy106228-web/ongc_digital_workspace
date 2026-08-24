"""add password reset requests and temporary password expiry

Revision ID: a1c3e5f7b9d2
Revises: e6f7a8b3c4d5
Create Date: 2026-08-24 17:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a1c3e5f7b9d2"
down_revision = "e6f7a8b3c4d5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "password_reset_requests",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("submitted_identifier", sa.String(length=80), nullable=False, server_default=""),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("request_ip", sa.String(length=45), nullable=False, server_default=""),
        sa.Column("request_user_agent", sa.Text(), nullable=True),
        sa.Column("handled_by_id", sa.BigInteger(), nullable=True),
        sa.Column("handled_at", sa.DateTime(), nullable=True),
        sa.Column("handled_note", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("temp_password_expires_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["handled_by_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_password_reset_requests_user_id", "password_reset_requests", ["user_id"]
    )
    op.create_index(
        "ix_password_reset_requests_status", "password_reset_requests", ["status"]
    )
    op.create_index(
        "ix_password_reset_requests_created_at", "password_reset_requests", ["created_at"]
    )
    op.create_index(
        "ix_password_reset_requests_status_created",
        "password_reset_requests",
        ["status", "created_at"],
    )

    op.add_column(
        "users", sa.Column("temp_password_expires_at", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "users", sa.Column("temp_password_used_at", sa.DateTime(), nullable=True)
    )


def downgrade():
    op.drop_column("users", "temp_password_used_at")
    op.drop_column("users", "temp_password_expires_at")
    # The table's indexes go with it.  Dropping them first fails on MySQL,
    # which refuses to drop an index a foreign key still needs.
    op.drop_table("password_reset_requests")
