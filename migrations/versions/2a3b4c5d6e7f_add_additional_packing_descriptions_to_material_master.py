"""add additional packing descriptions to material master

Revision ID: 2a3b4c5d6e7f
Revises: 1d2e3f4a5b6c
Create Date: 2026-03-25 08:40:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2a3b4c5d6e7f"
down_revision = "1d2e3f4a5b6c"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("material_master", schema=None) as batch_op:
        batch_op.add_column(sa.Column("container_description_2", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("container_description_3", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("container_description_4", sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table("material_master", schema=None) as batch_op:
        batch_op.drop_column("container_description_4")
        batch_op.drop_column("container_description_3")
        batch_op.drop_column("container_description_2")
