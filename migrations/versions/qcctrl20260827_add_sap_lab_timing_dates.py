"""Capture laboratory sampling and actual-start dates against SAP action items.

Revision ID: qcctrl20260827_timing
Revises: qcctrl20260827_dispositions
"""

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260827_timing"
down_revision = "qcctrl20260827_dispositions"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("qc_sap_lab_updates", sa.Column("sampling_date", sa.Date(), nullable=True))
    op.add_column("qc_sap_lab_updates", sa.Column("actual_start_date", sa.Date(), nullable=True))


def downgrade():
    op.drop_column("qc_sap_lab_updates", "actual_start_date")
    op.drop_column("qc_sap_lab_updates", "sampling_date")
