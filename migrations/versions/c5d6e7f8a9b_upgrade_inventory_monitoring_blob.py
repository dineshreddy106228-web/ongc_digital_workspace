"""Allow full source Excel files in Inventory Monitoring batches.

Revision ID: c5d6e7f8a9b
Revises: b4d5e6f7a8b
"""
from alembic import op
from sqlalchemy.dialects import mysql

revision = "c5d6e7f8a9b"
down_revision = "b4d5e6f7a8b"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "inventory_monitoring_upload_batches", "source_data",
        existing_type=mysql.BLOB(), type_=mysql.LONGBLOB(), existing_nullable=False,
    )


def downgrade():
    # Do not shrink stored workbooks automatically: that could truncate valid files.
    pass
