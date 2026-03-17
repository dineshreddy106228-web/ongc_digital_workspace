"""drop redundant procurement net price column

Revision ID: ab12cd34ef56
Revises: 9f1e2d3c4b5a
Create Date: 2026-03-17 00:30:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "ab12cd34ef56"
down_revision = "9f1e2d3c4b5a"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("inventory_procurement_seed_rows", "net_price")


def downgrade():
    op.add_column(
        "inventory_procurement_seed_rows",
        sa.Column("net_price", sa.Numeric(precision=18, scale=2), nullable=True),
    )
