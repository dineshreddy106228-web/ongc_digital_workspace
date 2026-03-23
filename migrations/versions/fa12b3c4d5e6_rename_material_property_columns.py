"""rename material property columns for ambient and refrigeration semantics

Revision ID: fa12b3c4d5e6
Revises: f9e8d7c6b5a4
Create Date: 2026-03-23 10:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "fa12b3c4d5e6"
down_revision = "f9e8d7c6b5a4"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "material_master",
        "volatility",
        new_column_name="volatility_ambient_temperature",
        existing_type=sa.String(length=100),
        existing_nullable=True,
    )
    op.alter_column(
        "material_master",
        "sunlight_sensitivity",
        new_column_name="sunlight_sensitivity_up_to_50c",
        existing_type=sa.String(length=100),
        existing_nullable=True,
    )
    op.alter_column(
        "material_master",
        "temperature_sensitivity",
        new_column_name="refrigeration_required",
        existing_type=sa.String(length=100),
        existing_nullable=True,
    )


def downgrade():
    op.alter_column(
        "material_master",
        "refrigeration_required",
        new_column_name="temperature_sensitivity",
        existing_type=sa.String(length=100),
        existing_nullable=True,
    )
    op.alter_column(
        "material_master",
        "sunlight_sensitivity_up_to_50c",
        new_column_name="sunlight_sensitivity",
        existing_type=sa.String(length=100),
        existing_nullable=True,
    )
    op.alter_column(
        "material_master",
        "volatility_ambient_temperature",
        new_column_name="volatility",
        existing_type=sa.String(length=100),
        existing_nullable=True,
    )
