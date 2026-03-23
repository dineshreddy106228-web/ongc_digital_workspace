"""add composite index on committee_tasks(office_id, status)

Revision ID: fb01c2d3e4f5
Revises: fa12b3c4d5e6
Create Date: 2026-03-23 11:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "fb01c2d3e4f5"
down_revision = "fa12b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_committee_tasks_office_status",
        "committee_tasks",
        ["office_id", "status"],
    )


def downgrade():
    op.drop_index("ix_committee_tasks_office_status", table_name="committee_tasks")
