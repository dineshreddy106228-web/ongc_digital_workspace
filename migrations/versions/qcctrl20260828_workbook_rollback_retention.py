"""Track expiry of stored Inventory and QC workbook payloads.

Revision ID: qcctrl20260828_wb_retention
Revises: qcctrl20260828_user_lab_scope
"""

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260828_wb_retention"
down_revision = "qcctrl20260828_user_lab_scope"
branch_labels = None
depends_on = None


def upgrade():
    # MySQL DDL is not transactional. The idempotent checks also recover
    # cleanly if an interrupted upgrade added a column before Alembic recorded
    # the revision marker.
    inspector = sa.inspect(op.get_bind())
    for table_name in (
        "inventory_monitoring_upload_batches",
        "qc_upload_batches",
        "qc_sap_upload_batches",
    ):
        columns = {column["name"] for column in inspector.get_columns(table_name)}
        if "source_purged_at" not in columns:
            op.add_column(table_name, sa.Column("source_purged_at", sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column("qc_sap_upload_batches", "source_purged_at")
    op.drop_column("qc_upload_batches", "source_purged_at")
    op.drop_column("inventory_monitoring_upload_batches", "source_purged_at")
