"""Add module admin assignments and CSC review-stage fields

Revision ID: 6b1c2d3e4f5a
Revises: f3a1b2c4d5e6
Create Date: 2026-03-21 10:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "6b1c2d3e4f5a"
down_revision = "f3a1b2c4d5e6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "module_admin_assignments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("module_code", sa.String(length=100), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "module_code", name="uq_module_admin_assignment"),
    )
    op.create_index(
        op.f("ix_module_admin_assignments_module_code"),
        "module_admin_assignments",
        ["module_code"],
        unique=False,
    )
    op.create_index(
        op.f("ix_module_admin_assignments_user_id"),
        "module_admin_assignments",
        ["user_id"],
        unique=False,
    )

    with op.batch_alter_table("csc_revisions", schema=None) as batch_op:
        batch_op.add_column(sa.Column("committee_head_reviewed_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("committee_head_notes", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("committee_head_user_id", sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column("module_admin_reviewed_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("module_admin_notes", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("module_admin_user_id", sa.BigInteger(), nullable=True))
        batch_op.create_foreign_key(
            "fk_csc_revisions_committee_head_user_id_users",
            "users",
            ["committee_head_user_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_csc_revisions_module_admin_user_id_users",
            "users",
            ["module_admin_user_id"],
            ["id"],
        )


def downgrade():
    with op.batch_alter_table("csc_revisions", schema=None) as batch_op:
        batch_op.drop_constraint("fk_csc_revisions_module_admin_user_id_users", type_="foreignkey")
        batch_op.drop_constraint("fk_csc_revisions_committee_head_user_id_users", type_="foreignkey")
        batch_op.drop_column("module_admin_user_id")
        batch_op.drop_column("module_admin_notes")
        batch_op.drop_column("module_admin_reviewed_at")
        batch_op.drop_column("committee_head_user_id")
        batch_op.drop_column("committee_head_notes")
        batch_op.drop_column("committee_head_reviewed_at")

    op.drop_index(op.f("ix_module_admin_assignments_user_id"), table_name="module_admin_assignments")
    op.drop_index(op.f("ix_module_admin_assignments_module_code"), table_name="module_admin_assignments")
    op.drop_table("module_admin_assignments")
