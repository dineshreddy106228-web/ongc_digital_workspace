"""add super-user review fields to Inventory Monitoring exceptions

Revision ID: e7f8a9b0c1d
Revises: d6e7f8a9b0c
"""
from alembic import op
import sqlalchemy as sa

revision = "e7f8a9b0c1d"
down_revision = "d6e7f8a9b0c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("inventory_monitoring_exceptions", sa.Column("review_status", sa.String(24), nullable=False, server_default="not_required"))
    op.add_column("inventory_monitoring_exceptions", sa.Column("reviewed_by", sa.BigInteger(), nullable=True))
    op.add_column("inventory_monitoring_exceptions", sa.Column("reviewed_at", sa.DateTime(), nullable=True))
    op.add_column("inventory_monitoring_exceptions", sa.Column("review_note", sa.String(500), nullable=True))
    op.create_foreign_key("fk_inventory_monitoring_exceptions_reviewer", "inventory_monitoring_exceptions", "users", ["reviewed_by"], ["id"], ondelete="SET NULL")
    op.execute("UPDATE inventory_monitoring_exceptions SET review_status = 'pending' WHERE exception_type IN ('held_not_mapped', 'unknown_mapping')")


def downgrade():
    op.drop_constraint("fk_inventory_monitoring_exceptions_reviewer", "inventory_monitoring_exceptions", type_="foreignkey")
    op.drop_column("inventory_monitoring_exceptions", "review_note")
    op.drop_column("inventory_monitoring_exceptions", "reviewed_at")
    op.drop_column("inventory_monitoring_exceptions", "reviewed_by")
    op.drop_column("inventory_monitoring_exceptions", "review_status")
