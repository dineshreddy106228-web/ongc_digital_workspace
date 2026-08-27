"""Add Corporate Chemistry's controlled non-SAP QC exception register.

Revision ID: qcctrl20260827
Revises: a0b1c2d3e4f5
"""

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260827"
down_revision = "a0b1c2d3e4f5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "qc_non_sap_samples",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("lab_code", sa.String(length=64), nullable=False),
        sa.Column("sample_reference", sa.String(length=120), nullable=False),
        sa.Column("chemical_name", sa.String(length=255), nullable=False),
        sa.Column("material_code", sa.String(length=100), nullable=True),
        sa.Column("sample_receipt_date", sa.Date(), nullable=True),
        sa.Column("current_status", sa.String(length=48), nullable=False),
        sa.Column("expected_completion_date", sa.Date(), nullable=True),
        sa.Column("action_owner", sa.String(length=160), nullable=True),
        sa.Column("delay_reason", sa.Text(), nullable=True),
        sa.Column("update_note", sa.Text(), nullable=True),
        sa.Column("reported_outcome", sa.String(length=24), nullable=True),
        sa.Column("created_by", sa.BigInteger(), nullable=True),
        sa.Column("updated_by", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lab_code", "sample_reference", name="uq_qc_non_sap_lab_reference"),
    )
    op.create_index("ix_qc_non_sap_samples_lab_status", "qc_non_sap_samples", ["lab_code", "current_status"])

    op.create_table(
        "qc_non_sap_sample_updates",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sample_id", sa.BigInteger(), nullable=False),
        sa.Column("current_status", sa.String(length=48), nullable=False),
        sa.Column("expected_completion_date", sa.Date(), nullable=True),
        sa.Column("action_owner", sa.String(length=160), nullable=True),
        sa.Column("delay_reason", sa.Text(), nullable=True),
        sa.Column("update_note", sa.Text(), nullable=True),
        sa.Column("reported_outcome", sa.String(length=24), nullable=True),
        sa.Column("updated_by", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["sample_id"], ["qc_non_sap_samples.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_qc_non_sap_sample_updates_sample_created",
        "qc_non_sap_sample_updates", ["sample_id", "created_at"],
    )


def downgrade():
    op.drop_table("qc_non_sap_sample_updates")
    op.drop_table("qc_non_sap_samples")
