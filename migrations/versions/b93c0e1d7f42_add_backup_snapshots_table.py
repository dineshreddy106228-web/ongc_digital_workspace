"""add backup snapshots table

Revision ID: b93c0e1d7f42
Revises: a8d9b1c2f4e7
Create Date: 2026-03-13 23:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b93c0e1d7f42"
down_revision = "a8d9b1c2f4e7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "backup_snapshots",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("created_by_username", sa.String(length=80), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("environment", sa.String(length=50), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    with op.batch_alter_table("backup_snapshots", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_backup_snapshots_created_at"),
            ["created_at"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_backup_snapshots_created_by_username"),
            ["created_by_username"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("backup_snapshots", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_backup_snapshots_created_by_username"))
        batch_op.drop_index(batch_op.f("ix_backup_snapshots_created_at"))

    op.drop_table("backup_snapshots")
