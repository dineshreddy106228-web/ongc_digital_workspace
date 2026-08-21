"""Repair inventory batches recorded as superseding themselves.

A batch marked as its own successor hides every record it carries — for the
affected workbooks that removed a whole material group from the reviews. The
earlier repair (d6e7f8a9b0c) ran before the affected imports, so the defective
rows are cleared again here; imports now self-heal the same way.

Revision ID: f8a9b0c1d2e
Revises: e7f8a9b0c1d
"""
from alembic import op

revision = "f8a9b0c1d2e"
down_revision = "e7f8a9b0c1d"
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
