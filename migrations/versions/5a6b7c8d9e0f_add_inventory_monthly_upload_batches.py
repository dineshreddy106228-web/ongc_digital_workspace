"""add inventory monthly upload batches

Revision ID: 5a6b7c8d9e0f
Revises: 4d7e8f9a0b1c
Create Date: 2026-03-26 13:45:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


revision = "5a6b7c8d9e0f"
down_revision = "4d7e8f9a0b1c"
branch_labels = None
depends_on = None


def upgrade():
    blob_type = sa.LargeBinary().with_variant(mysql.LONGBLOB(), "mysql")

    op.create_table(
        "inventory_monthly_upload_batches",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("report_year", sa.Integer(), nullable=False),
        sa.Column("report_month", sa.Integer(), nullable=False),
        sa.Column("report_label", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("mb51_filename", sa.String(length=255), nullable=False),
        sa.Column("me2m_filename", sa.String(length=255), nullable=False),
        sa.Column("mc9_filename", sa.String(length=255), nullable=False),
        sa.Column("mb51_row_count", sa.Integer(), nullable=False),
        sa.Column("me2m_row_count", sa.Integer(), nullable=False),
        sa.Column("mc9_row_count", sa.Integer(), nullable=False),
        sa.Column("summary_json", sa.Text(), nullable=False),
        sa.Column("excel_filename", sa.String(length=255), nullable=False),
        sa.Column("excel_content_type", sa.String(length=120), nullable=False),
        sa.Column("excel_file_size", sa.BigInteger(), nullable=False),
        sa.Column("excel_data", blob_type, nullable=False),
        sa.Column("pdf_filename", sa.String(length=255), nullable=False),
        sa.Column("pdf_content_type", sa.String(length=120), nullable=False),
        sa.Column("pdf_file_size", sa.BigInteger(), nullable=False),
        sa.Column("pdf_data", blob_type, nullable=False),
        sa.Column("uploaded_by", sa.BigInteger(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["uploaded_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("report_year", "report_month", name="uq_inv_monthly_batches_period"),
    )
    op.create_index(
        "ix_inv_monthly_batches_period",
        "inventory_monthly_upload_batches",
        ["report_year", "report_month"],
        unique=False,
    )
    op.create_index(
        "ix_inv_monthly_batches_uploaded_at",
        "inventory_monthly_upload_batches",
        ["uploaded_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_inv_monthly_batches_uploaded_at", table_name="inventory_monthly_upload_batches")
    op.drop_index("ix_inv_monthly_batches_period", table_name="inventory_monthly_upload_batches")
    op.drop_table("inventory_monthly_upload_batches")
