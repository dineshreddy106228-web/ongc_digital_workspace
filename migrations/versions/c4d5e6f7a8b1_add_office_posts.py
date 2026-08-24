"""Office posts: the designation, held separately from the person.

Reusing one user account for a post ("Head RGL") and renaming it on handover
retroactively rewrites every historical attribution, because task updates,
approvals and audit rows resolve the display name live. These tables keep the
post as its own record with a succession of holders, so a handover changes who
holds the post without touching what anyone wrote.

Revision ID: c4d5e6f7a8b1
Revises: b3c4d5e6f7a9
"""
import sqlalchemy as sa
from alembic import op

revision = "c4d5e6f7a8b1"
down_revision = "b3c4d5e6f7a9"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "office_posts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("office_id", sa.BigInteger(), nullable=False),
        sa.Column("post_code", sa.String(length=50), nullable=False),
        sa.Column("post_title", sa.String(length=150), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("holder_user_id", sa.BigInteger(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["office_id"], ["offices.id"]),
        sa.ForeignKeyConstraint(["holder_user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_office_posts_office_id", "office_posts", ["office_id"])
    op.create_index("ix_office_posts_holder_user_id", "office_posts", ["holder_user_id"])
    op.create_index("ix_office_posts_post_code", "office_posts", ["post_code"], unique=True)

    op.create_table(
        "office_post_assignments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("post_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("holder_name", sa.String(length=150), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("ended_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["post_id"], ["office_posts.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_office_post_assignments_post_id", "office_post_assignments", ["post_id"])
    op.create_index("ix_office_post_assignments_user_id", "office_post_assignments", ["user_id"])
    op.create_index(
        "ix_office_post_assignments_post_started",
        "office_post_assignments",
        ["post_id", "started_at"],
    )


def downgrade():
    # Dropping the table takes its indexes with it. Dropping them first fails on
    # MySQL, which needs the index that backs each foreign key to stay put.
    # Assignments go first: they reference office_posts.
    op.drop_table("office_post_assignments")
    op.drop_table("office_posts")
