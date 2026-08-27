"""Add SAP-first RGL Panvel quality monitoring.

Revision ID: a0b1c2d3e4f5
Revises: f6a7b8c9d0e1
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


revision = "a0b1c2d3e4f5"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "qc_sap_upload_batches",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("lab_code", sa.String(length=64), nullable=False),
        sa.Column("plant_code", sa.String(length=32), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("inspection_filename", sa.String(length=255), nullable=False),
        sa.Column("inspection_content_type", sa.String(length=120), nullable=False),
        sa.Column("inspection_file_size", sa.BigInteger(), nullable=False),
        sa.Column("inspection_source_data", mysql.LONGBLOB(), nullable=False),
        sa.Column("notification_filename", sa.String(length=255), nullable=False),
        sa.Column("notification_content_type", sa.String(length=120), nullable=False),
        sa.Column("notification_file_size", sa.BigInteger(), nullable=False),
        sa.Column("notification_source_data", mysql.LONGBLOB(), nullable=False),
        sa.Column("inspection_lot_count", sa.Integer(), nullable=False),
        sa.Column("notification_count", sa.Integer(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("unmatched_inspection_count", sa.Integer(), nullable=False),
        sa.Column("unmatched_notification_count", sa.Integer(), nullable=False),
        sa.Column("summary_json", sa.Text(), nullable=False),
        sa.Column("uploaded_by", sa.BigInteger(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["uploaded_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_qc_sap_upload_batches_lab_date", "qc_sap_upload_batches", ["lab_code", "as_of_date"])
    op.create_index("ix_qc_sap_upload_batches_uploaded_at", "qc_sap_upload_batches", ["uploaded_at"])

    op.create_table(
        "qc_sap_records",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_key", sa.String(length=180), nullable=False),
        sa.Column("lab_code", sa.String(length=64), nullable=False),
        sa.Column("source_completeness", sa.String(length=32), nullable=False),
        sa.Column("inspection_lot_number", sa.String(length=100), nullable=True),
        sa.Column("notification_no", sa.String(length=100), nullable=True),
        sa.Column("plant_code", sa.String(length=32), nullable=False),
        sa.Column("material_code", sa.String(length=100), nullable=True),
        sa.Column("material_description", sa.String(length=500), nullable=True),
        sa.Column("po_number", sa.String(length=100), nullable=True),
        sa.Column("po_item", sa.String(length=40), nullable=True),
        sa.Column("work_center", sa.String(length=160), nullable=True),
        sa.Column("sap_system_status", sa.String(length=255), nullable=True),
        sa.Column("sap_lot_status", sa.String(length=255), nullable=True),
        sa.Column("sap_notification_status", sa.String(length=255), nullable=True),
        sa.Column("usage_decision_code", sa.String(length=80), nullable=True),
        sa.Column("official_status", sa.String(length=32), nullable=False),
        sa.Column("start_inspection_date", sa.Date(), nullable=True),
        sa.Column("end_inspection_date", sa.Date(), nullable=True),
        sa.Column("notification_start_date", sa.Date(), nullable=True),
        sa.Column("planned_end_date", sa.Date(), nullable=True),
        sa.Column("completion_date", sa.Date(), nullable=True),
        sa.Column("sap_delay_days", sa.Integer(), nullable=True),
        sa.Column("first_seen_batch_id", sa.BigInteger(), nullable=True),
        sa.Column("last_seen_batch_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["first_seen_batch_id"], ["qc_sap_upload_batches.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["last_seen_batch_id"], ["qc_sap_upload_batches.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_key", name="uq_qc_sap_records_source_key"),
    )
    op.create_index("ix_qc_sap_records_lab_batch", "qc_sap_records", ["lab_code", "last_seen_batch_id"])
    op.create_index("ix_qc_sap_records_lot", "qc_sap_records", ["inspection_lot_number"])
    op.create_index("ix_qc_sap_records_notification", "qc_sap_records", ["notification_no"])
    op.create_index("ix_qc_sap_records_status", "qc_sap_records", ["official_status"])
    op.create_index("ix_qc_sap_records_work_center", "qc_sap_records", ["work_center"])

    op.create_table(
        "qc_sap_lab_updates",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("record_id", sa.BigInteger(), nullable=False),
        sa.Column("activity_status", sa.String(length=48), nullable=False),
        sa.Column("expected_completion_date", sa.Date(), nullable=True),
        sa.Column("action_owner", sa.String(length=160), nullable=True),
        sa.Column("delay_reason", sa.Text(), nullable=True),
        sa.Column("update_note", sa.Text(), nullable=True),
        sa.Column("updated_by", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["record_id"], ["qc_sap_records.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_qc_sap_lab_updates_record_created", "qc_sap_lab_updates", ["record_id", "created_at"])


def downgrade():
    op.drop_table("qc_sap_lab_updates")
    op.drop_table("qc_sap_records")
    op.drop_table("qc_sap_upload_batches")
