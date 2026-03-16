"""add normalized inventory seed tables

Revision ID: c7d8e9f0a1b2
Revises: b2c3d4e5f6a1
Create Date: 2026-03-16 20:40:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "c7d8e9f0a1b2"
down_revision = "b2c3d4e5f6a1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "inventory_consumption_seed_rows",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("import_batch", sa.String(length=36), nullable=False),
        sa.Column("source_filename", sa.String(length=255), nullable=True),
        sa.Column("imported_at", sa.DateTime(), nullable=False),
        sa.Column("imported_by", sa.BigInteger(), nullable=True),
        sa.Column("material_code", sa.String(length=50), nullable=True),
        sa.Column("material_desc", sa.String(length=255), nullable=True),
        sa.Column("plant", sa.String(length=50), nullable=True),
        sa.Column("reporting_plant", sa.String(length=50), nullable=True),
        sa.Column("storage_location", sa.String(length=50), nullable=True),
        sa.Column("movement_type", sa.String(length=20), nullable=True),
        sa.Column("posting_date", sa.DateTime(), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("month", sa.Integer(), nullable=True),
        sa.Column("month_raw", sa.String(length=20), nullable=True),
        sa.Column("financial_year", sa.String(length=16), nullable=True),
        sa.Column("usage_qty", sa.Numeric(precision=18, scale=3), nullable=True),
        sa.Column("usage_value", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("uom", sa.String(length=20), nullable=True),
        sa.Column("currency", sa.String(length=10), nullable=True),
        sa.Column("po_number", sa.String(length=50), nullable=True),
        sa.Column("material_document", sa.String(length=50), nullable=True),
        sa.Column("material_document_item", sa.String(length=20), nullable=True),
        sa.ForeignKeyConstraint(["imported_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_inv_cons_seed_reporting_plant", "inventory_consumption_seed_rows", ["reporting_plant"], unique=False)
    op.create_index("ix_inv_cons_seed_material_desc", "inventory_consumption_seed_rows", ["material_desc"], unique=False)
    op.create_index("ix_inv_cons_seed_material_code", "inventory_consumption_seed_rows", ["material_code"], unique=False)
    op.create_index("ix_inv_cons_seed_posting_date", "inventory_consumption_seed_rows", ["posting_date"], unique=False)
    op.create_index("ix_inv_cons_seed_year_month", "inventory_consumption_seed_rows", ["year", "month"], unique=False)
    op.create_index("ix_inv_cons_seed_batch", "inventory_consumption_seed_rows", ["import_batch"], unique=False)

    op.create_table(
        "inventory_procurement_seed_rows",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("import_batch", sa.String(length=36), nullable=False),
        sa.Column("source_filename", sa.String(length=255), nullable=True),
        sa.Column("imported_at", sa.DateTime(), nullable=False),
        sa.Column("imported_by", sa.BigInteger(), nullable=True),
        sa.Column("material_code", sa.String(length=50), nullable=True),
        sa.Column("material_desc", sa.String(length=255), nullable=True),
        sa.Column("plant", sa.String(length=50), nullable=True),
        sa.Column("reporting_plant", sa.String(length=50), nullable=True),
        sa.Column("vendor", sa.String(length=255), nullable=True),
        sa.Column("po_number", sa.String(length=50), nullable=True),
        sa.Column("item", sa.String(length=20), nullable=True),
        sa.Column("doc_date", sa.DateTime(), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("month", sa.Integer(), nullable=True),
        sa.Column("financial_year", sa.String(length=16), nullable=True),
        sa.Column("order_qty", sa.Numeric(precision=18, scale=3), nullable=True),
        sa.Column("order_unit", sa.String(length=20), nullable=True),
        sa.Column("net_price", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("unit_price", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("price_unit", sa.Numeric(precision=18, scale=3), nullable=True),
        sa.Column("effective_value", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("currency", sa.String(length=10), nullable=True),
        sa.Column("release_indicator", sa.String(length=50), nullable=True),
        sa.ForeignKeyConstraint(["imported_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_inv_proc_seed_reporting_plant", "inventory_procurement_seed_rows", ["reporting_plant"], unique=False)
    op.create_index("ix_inv_proc_seed_material_desc", "inventory_procurement_seed_rows", ["material_desc"], unique=False)
    op.create_index("ix_inv_proc_seed_material_code", "inventory_procurement_seed_rows", ["material_code"], unique=False)
    op.create_index("ix_inv_proc_seed_doc_date", "inventory_procurement_seed_rows", ["doc_date"], unique=False)
    op.create_index("ix_inv_proc_seed_vendor", "inventory_procurement_seed_rows", ["vendor"], unique=False)
    op.create_index("ix_inv_proc_seed_batch", "inventory_procurement_seed_rows", ["import_batch"], unique=False)


def downgrade():
    op.drop_index("ix_inv_proc_seed_batch", table_name="inventory_procurement_seed_rows")
    op.drop_index("ix_inv_proc_seed_vendor", table_name="inventory_procurement_seed_rows")
    op.drop_index("ix_inv_proc_seed_doc_date", table_name="inventory_procurement_seed_rows")
    op.drop_index("ix_inv_proc_seed_material_code", table_name="inventory_procurement_seed_rows")
    op.drop_index("ix_inv_proc_seed_material_desc", table_name="inventory_procurement_seed_rows")
    op.drop_index("ix_inv_proc_seed_reporting_plant", table_name="inventory_procurement_seed_rows")
    op.drop_table("inventory_procurement_seed_rows")

    op.drop_index("ix_inv_cons_seed_batch", table_name="inventory_consumption_seed_rows")
    op.drop_index("ix_inv_cons_seed_year_month", table_name="inventory_consumption_seed_rows")
    op.drop_index("ix_inv_cons_seed_posting_date", table_name="inventory_consumption_seed_rows")
    op.drop_index("ix_inv_cons_seed_material_code", table_name="inventory_consumption_seed_rows")
    op.drop_index("ix_inv_cons_seed_material_desc", table_name="inventory_consumption_seed_rows")
    op.drop_index("ix_inv_cons_seed_reporting_plant", table_name="inventory_consumption_seed_rows")
    op.drop_table("inventory_consumption_seed_rows")
