"""add additional packing slots to material master

Revision ID: 1d2e3f4a5b6c
Revises: fb01c2d3e4f5
Create Date: 2026-03-10 18:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "1d2e3f4a5b6c"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("material_master", schema=None) as batch_op:
        batch_op.add_column(sa.Column("container_type_2", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("container_capacity_2", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("container_type_3", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("container_capacity_3", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("container_type_4", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("container_capacity_4", sa.String(length=100), nullable=True))


def downgrade():
    with op.batch_alter_table("material_master", schema=None) as batch_op:
        batch_op.drop_column("container_capacity_4")
        batch_op.drop_column("container_type_4")
        batch_op.drop_column("container_capacity_3")
        batch_op.drop_column("container_type_3")
        batch_op.drop_column("container_capacity_2")
        batch_op.drop_column("container_type_2")
