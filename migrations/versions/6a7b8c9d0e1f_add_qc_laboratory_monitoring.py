"""Add QC laboratory weekly uploads and sample history.

Revision ID: 6a7b8c9d0e1f
Revises: 5a6b7c8d9e0f
"""

from alembic import op
import sqlalchemy as sa


revision = "6a7b8c9d0e1f"
down_revision = "5a6b7c8d9e0f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "qc_upload_batches",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("report_label", sa.String(length=160), nullable=False),
        sa.Column("week_start", sa.Date(), nullable=False),
        sa.Column("week_end", sa.Date(), nullable=False),
        sa.Column("source_filename", sa.String(length=255), nullable=False),
        sa.Column("source_content_type", sa.String(length=120), nullable=False),
        sa.Column("source_file_size", sa.BigInteger(), nullable=False),
        sa.Column("source_data", sa.LargeBinary(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("imported_count", sa.Integer(), nullable=False),
        sa.Column("updated_count", sa.Integer(), nullable=False),
        sa.Column("summary_json", sa.Text(), nullable=False),
        sa.Column("uploaded_by", sa.BigInteger(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["uploaded_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("week_start", "week_end", name="uq_qc_upload_batches_period"),
    )
    op.create_index("ix_qc_upload_batches_period", "qc_upload_batches", ["week_start", "week_end"])
    op.create_index("ix_qc_upload_batches_uploaded_at", "qc_upload_batches", ["uploaded_at"])
    op.create_table(
        "qc_samples",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_key", sa.String(length=64), nullable=False),
        sa.Column("serial_number", sa.Integer(), nullable=True),
        sa.Column("chemical_name", sa.String(length=255), nullable=False),
        sa.Column("specification_no", sa.String(length=255), nullable=True),
        sa.Column("supply_type", sa.String(length=50), nullable=True),
        sa.Column("po_number", sa.String(length=100), nullable=True),
        sa.Column("lot_stack", sa.String(length=100), nullable=True),
        sa.Column("notification_no", sa.String(length=100), nullable=True),
        sa.Column("result_status", sa.String(length=30), nullable=False),
        sa.Column("sample_receipt_date", sa.Date(), nullable=True),
        sa.Column("report_issue_date", sa.Date(), nullable=True),
        sa.Column("turnaround_days", sa.Integer(), nullable=True),
        sa.Column("delay_reason", sa.Text(), nullable=True),
        sa.Column("first_seen_batch_id", sa.BigInteger(), nullable=True),
        sa.Column("last_seen_batch_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["first_seen_batch_id"], ["qc_upload_batches.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["last_seen_batch_id"], ["qc_upload_batches.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_key", name="uq_qc_samples_source_key"),
    )
    op.create_index("ix_qc_samples_status", "qc_samples", ["result_status"])
    op.create_index("ix_qc_samples_chemical", "qc_samples", ["chemical_name"])
    op.create_index("ix_qc_samples_receipt_date", "qc_samples", ["sample_receipt_date"])
    op.create_index("ix_qc_samples_last_seen", "qc_samples", ["last_seen_batch_id"])


def downgrade():
    op.drop_table("qc_samples")
    op.drop_table("qc_upload_batches")
