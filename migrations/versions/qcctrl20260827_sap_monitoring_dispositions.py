"""Add auditable SAP QC monitoring dispositions.

Revision ID: qcctrl20260827_dispositions
Revises: qcctrl20260827
"""

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260827_dispositions"
down_revision = "qcctrl20260827"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "qc_sap_monitoring_dispositions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("record_id", sa.BigInteger(), nullable=False),
        sa.Column("decision", sa.String(length=24), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("official_status_at_decision", sa.String(length=32), nullable=False),
        sa.Column("work_center_at_decision", sa.String(length=160), nullable=True),
        sa.Column("recorded_by", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["record_id"], ["qc_sap_records.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["recorded_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_qc_sap_monitoring_dispositions_record_created",
        "qc_sap_monitoring_dispositions", ["record_id", "created_at"],
    )


def downgrade():
    op.drop_table("qc_sap_monitoring_dispositions")
