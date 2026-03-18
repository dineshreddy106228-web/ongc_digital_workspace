"""Widen CSC parameter name column

Revision ID: 7a9c5f4d2e31
Revises: 33b1d640498b
Create Date: 2026-03-17 16:25:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7a9c5f4d2e31"
down_revision = "33b1d640498b"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("csc_parameters", schema=None) as batch_op:
        batch_op.alter_column(
            "parameter_name",
            existing_type=sa.String(length=500),
            type_=sa.Text(),
            existing_nullable=False,
        )


def downgrade():
    with op.batch_alter_table("csc_parameters", schema=None) as batch_op:
        batch_op.alter_column(
            "parameter_name",
            existing_type=sa.Text(),
            type_=sa.String(length=500),
            existing_nullable=False,
        )
