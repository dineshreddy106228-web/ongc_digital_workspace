"""Assign an optional SAP QC laboratory scope to each user.

Revision ID: qcctrl20260828_user_lab_scope
Revises: qcctrl20260827_timing
"""

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260828_user_lab_scope"
down_revision = "qcctrl20260827_timing"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("users", sa.Column("quality_control_lab_code", sa.String(length=64), nullable=True))
    op.create_index(
        "ix_users_quality_control_lab_code", "users", ["quality_control_lab_code"], unique=False,
    )


def downgrade():
    op.drop_index("ix_users_quality_control_lab_code", table_name="users")
    op.drop_column("users", "quality_control_lab_code")
