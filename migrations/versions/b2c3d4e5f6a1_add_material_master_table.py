"""add material_master table

Revision ID: b2c3d4e5f6a1
Revises: a1b2c3d4e5f6
Create Date: 2026-03-15 12:00:00.000000

Stores per-material master data (from Master_data.xlsx).
Primary key: ``material`` (matches the SAP field name used across all
inventory data, enabling direct joins).

All 20 columns from the current workbook schema are stored as proper
SQL columns.  Any future columns that appear in uploads but are not
listed here fall into the ``extra_data`` JSON bucket automatically.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b2c3d4e5f6a1'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'material_master',

        # ── Primary key ───────────────────────────────────────────
        sa.Column('material',  sa.String(length=50),  nullable=False),

        # ── Core identification ───────────────────────────────────
        sa.Column('short_text',     sa.String(length=255), nullable=True),
        sa.Column('group',          sa.String(length=100), nullable=True),
        sa.Column('material_type',  sa.String(length=100), nullable=True),
        sa.Column('centralization', sa.String(length=50),  nullable=True),
        sa.Column('physical_state', sa.String(length=50),  nullable=True),

        # ── Physical / chemical properties ────────────────────────
        sa.Column('volatility',               sa.String(length=100), nullable=True),
        sa.Column('sunlight_sensitivity',     sa.String(length=100), nullable=True),
        sa.Column('moisture_sensitivity',     sa.String(length=100), nullable=True),
        sa.Column('temperature_sensitivity',  sa.String(length=100), nullable=True),
        sa.Column('reactivity',               sa.String(length=100), nullable=True),
        sa.Column('flammable',                sa.String(length=50),  nullable=True),
        sa.Column('toxic',                    sa.String(length=50),  nullable=True),
        sa.Column('corrosive',                sa.String(length=50),  nullable=True),

        # ── Storage ───────────────────────────────────────────────
        sa.Column('storage_conditions_general',     sa.String(length=255), nullable=True),
        sa.Column('storage_conditions_special',     sa.String(length=255), nullable=True),
        sa.Column('container_type',                 sa.String(length=100), nullable=True),
        sa.Column('container_capacity',             sa.String(length=100), nullable=True),
        sa.Column('container_description',          sa.String(length=255), nullable=True),
        sa.Column('primary_storage_classification', sa.String(length=100), nullable=True),

        # ── Flexible bucket for truly unknown future columns ──────
        sa.Column('extra_data',  sa.JSON(),       nullable=True),

        # ── Audit ─────────────────────────────────────────────────
        sa.Column('updated_at',  sa.DateTime(),   nullable=False),
        sa.Column('updated_by',  sa.BigInteger(), nullable=True),

        sa.ForeignKeyConstraint(['updated_by'], ['users.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('material'),
    )
    op.create_index(
        'ix_material_master_group',
        'material_master',
        ['group'],
        unique=False,
    )
    op.create_index(
        'ix_material_master_material_type',
        'material_master',
        ['material_type'],
        unique=False,
    )


def downgrade():
    op.drop_index('ix_material_master_material_type', table_name='material_master')
    op.drop_index('ix_material_master_group', table_name='material_master')
    op.drop_table('material_master')
