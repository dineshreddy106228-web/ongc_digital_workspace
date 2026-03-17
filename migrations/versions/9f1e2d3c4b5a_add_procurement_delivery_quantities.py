"""add procurement delivery quantity columns

Revision ID: 9f1e2d3c4b5a
Revises: c7d8e9f0a1b2
Create Date: 2026-03-17 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "9f1e2d3c4b5a"
down_revision = "c7d8e9f0a1b2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "inventory_procurement_seed_rows",
        sa.Column("still_to_be_delivered_qty", sa.Numeric(precision=18, scale=3), nullable=True),
    )
    op.add_column(
        "inventory_procurement_seed_rows",
        sa.Column("procured_qty", sa.Numeric(precision=18, scale=3), nullable=True),
    )
    op.execute(
        """
        UPDATE inventory_procurement_seed_rows
        SET still_to_be_delivered_qty = 0,
            procured_qty = order_qty
        WHERE procured_qty IS NULL
        """
    )


def downgrade():
    op.drop_column("inventory_procurement_seed_rows", "procured_qty")
    op.drop_column("inventory_procurement_seed_rows", "still_to_be_delivered_qty")
