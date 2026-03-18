"""Add material code to CSC drafts

Revision ID: 91c4e2f7b8aa
Revises: 7a9c5f4d2e31
Create Date: 2026-03-17 19:45:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "91c4e2f7b8aa"
down_revision = "7a9c5f4d2e31"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("csc_drafts", schema=None) as batch_op:
        batch_op.add_column(sa.Column("material_code", sa.String(length=50), nullable=True))
        batch_op.create_index("ix_csc_drafts_material_code", ["material_code"], unique=False)


def downgrade():
    with op.batch_alter_table("csc_drafts", schema=None) as batch_op:
        batch_op.drop_index("ix_csc_drafts_material_code")
        batch_op.drop_column("material_code")
