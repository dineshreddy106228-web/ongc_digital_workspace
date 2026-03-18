"""add announcements and polls

Revision ID: 5e4f6a7b8c9d
Revises: f2c4a8b9d1e6
Create Date: 2026-03-18 09:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5e4f6a7b8c9d"
down_revision = "f2c4a8b9d1e6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "announcements",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("summary", sa.String(length=500), nullable=True),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("announcement_type", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("audience_scope", sa.String(length=50), nullable=False),
        sa.Column("created_by", sa.BigInteger(), nullable=False),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_announcements_status_published_at",
        "announcements",
        ["status", "published_at"],
        unique=False,
    )
    op.create_index(
        "ix_announcements_created_by",
        "announcements",
        ["created_by"],
        unique=False,
    )

    op.create_table(
        "announcement_options",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("announcement_id", sa.BigInteger(), nullable=False),
        sa.Column("option_text", sa.String(length=255), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["announcement_id"], ["announcements.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "announcement_id",
            "display_order",
            name="uq_announcement_options_order",
        ),
    )
    op.create_index(
        "ix_announcement_options_announcement_id",
        "announcement_options",
        ["announcement_id"],
        unique=False,
    )

    op.create_table(
        "announcement_recipients",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("announcement_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("is_read", sa.Boolean(), nullable=False),
        sa.Column("read_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["announcement_id"], ["announcements.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "announcement_id",
            "user_id",
            name="uq_announcement_recipients_announcement_user",
        ),
    )
    op.create_index(
        "ix_announcement_recipients_user_read",
        "announcement_recipients",
        ["user_id", "is_read"],
        unique=False,
    )
    op.create_index(
        "ix_announcement_recipients_announcement_id",
        "announcement_recipients",
        ["announcement_id"],
        unique=False,
    )
    op.create_index(
        "ix_announcement_recipients_user_id",
        "announcement_recipients",
        ["user_id"],
        unique=False,
    )

    op.create_table(
        "announcement_votes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("announcement_id", sa.BigInteger(), nullable=False),
        sa.Column("option_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["announcement_id"], ["announcements.id"]),
        sa.ForeignKeyConstraint(["option_id"], ["announcement_options.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "announcement_id",
            "user_id",
            name="uq_announcement_votes_announcement_user",
        ),
    )
    op.create_index(
        "ix_announcement_votes_option_id",
        "announcement_votes",
        ["option_id"],
        unique=False,
    )
    op.create_index(
        "ix_announcement_votes_announcement_id",
        "announcement_votes",
        ["announcement_id"],
        unique=False,
    )
    op.create_index(
        "ix_announcement_votes_user_id",
        "announcement_votes",
        ["user_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_announcement_votes_user_id", table_name="announcement_votes")
    op.drop_index("ix_announcement_votes_announcement_id", table_name="announcement_votes")
    op.drop_index("ix_announcement_votes_option_id", table_name="announcement_votes")
    op.drop_table("announcement_votes")

    op.drop_index("ix_announcement_recipients_user_id", table_name="announcement_recipients")
    op.drop_index("ix_announcement_recipients_announcement_id", table_name="announcement_recipients")
    op.drop_index("ix_announcement_recipients_user_read", table_name="announcement_recipients")
    op.drop_table("announcement_recipients")

    op.drop_index("ix_announcement_options_announcement_id", table_name="announcement_options")
    op.drop_table("announcement_options")

    op.drop_index("ix_announcements_created_by", table_name="announcements")
    op.drop_index("ix_announcements_status_published_at", table_name="announcements")
    op.drop_table("announcements")
