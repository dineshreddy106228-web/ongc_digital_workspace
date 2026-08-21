"""Repair batches created by the initial self-supersession defect.

Revision ID: d6e7f8a9b0c
Revises: c5d6e7f8a9b
"""
from alembic import op

revision = "d6e7f8a9b0c"
down_revision = "c5d6e7f8a9b"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "UPDATE inventory_monitoring_upload_batches "
        "SET is_superseded = 0, superseded_by_id = NULL "
        "WHERE id = superseded_by_id"
    )


def downgrade():
    pass
