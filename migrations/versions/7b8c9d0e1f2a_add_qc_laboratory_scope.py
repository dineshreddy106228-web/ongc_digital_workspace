"""Scope QC batches and samples to a laboratory.

Revision ID: 7b8c9d0e1f2a
Revises: 6a7b8c9d0e1f
"""

from alembic import op
import sqlalchemy as sa


revision = "7b8c9d0e1f2a"
down_revision = "6a7b8c9d0e1f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "qc_upload_batches",
        sa.Column("lab_code", sa.String(length=64), nullable=False, server_default="rgl_panvel"),
    )
    op.add_column(
        "qc_upload_batches",
        sa.Column("lab_name", sa.String(length=160), nullable=False, server_default="RGL Panvel"),
    )
    op.drop_constraint("uq_qc_upload_batches_period", "qc_upload_batches", type_="unique")
    op.drop_index("ix_qc_upload_batches_period", table_name="qc_upload_batches")
    op.create_index("ix_qc_upload_batches_period", "qc_upload_batches", ["lab_code", "week_start", "week_end"])
    op.create_unique_constraint("uq_qc_upload_batches_lab_period", "qc_upload_batches", ["lab_code", "week_start", "week_end"])

    op.add_column(
        "qc_samples",
        sa.Column("lab_code", sa.String(length=64), nullable=False, server_default="rgl_panvel"),
    )
    op.create_index("ix_qc_samples_lab", "qc_samples", ["lab_code"])


def downgrade():
    op.drop_index("ix_qc_samples_lab", table_name="qc_samples")
    op.drop_column("qc_samples", "lab_code")
    op.drop_constraint("uq_qc_upload_batches_lab_period", "qc_upload_batches", type_="unique")
    op.drop_index("ix_qc_upload_batches_period", table_name="qc_upload_batches")
    op.create_index("ix_qc_upload_batches_period", "qc_upload_batches", ["week_start", "week_end"])
    op.create_unique_constraint("uq_qc_upload_batches_period", "qc_upload_batches", ["week_start", "week_end"])
    op.drop_column("qc_upload_batches", "lab_name")
    op.drop_column("qc_upload_batches", "lab_code")
