"""add inventory tables

Revision ID: a1b2c3d4e5f6
Revises: e8b1c2d3f4a5
Create Date: 2026-03-14 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'e8b1c2d3f4a5'
branch_labels = None
depends_on = None


def upgrade():
    # ── inventory_uploads ─────────────────────────────────────────
    op.create_table(
        'inventory_uploads',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('original_filename', sa.String(length=255), nullable=False),
        sa.Column('row_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='success'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('uploaded_by', sa.BigInteger(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['uploaded_by'], ['users.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_inv_uploads_uploaded_by', 'inventory_uploads', ['uploaded_by'], unique=False)
    op.create_index('ix_inv_uploads_created_at', 'inventory_uploads', ['created_at'], unique=False)
    op.create_index('ix_inv_uploads_status', 'inventory_uploads', ['status'], unique=False)

    # ── inventory_records ─────────────────────────────────────────
    op.create_table(
        'inventory_records',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('upload_id', sa.BigInteger(), nullable=False),
        sa.Column('plant', sa.String(length=50), nullable=True),
        sa.Column('material', sa.String(length=100), nullable=True),
        sa.Column('storage_location', sa.String(length=50), nullable=True),
        sa.Column('material_type', sa.String(length=50), nullable=True),
        sa.Column('material_group', sa.String(length=100), nullable=True),
        sa.Column('business_area', sa.String(length=50), nullable=True),
        sa.Column('division', sa.String(length=50), nullable=True),
        sa.Column('month_raw', sa.String(length=20), nullable=True),
        sa.Column('period_year', sa.SmallInteger(), nullable=True),
        sa.Column('period_month', sa.SmallInteger(), nullable=True),
        sa.Column('total_usage', sa.Numeric(precision=18, scale=3), nullable=True),
        sa.Column('usage_uom', sa.String(length=20), nullable=True),
        sa.Column('total_usage_value', sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column('currency', sa.String(length=10), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['upload_id'], ['inventory_uploads.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_inv_records_upload_id', 'inventory_records', ['upload_id'], unique=False)
    op.create_index('ix_inv_records_plant', 'inventory_records', ['plant'], unique=False)
    op.create_index('ix_inv_records_material', 'inventory_records', ['material'], unique=False)
    op.create_index('ix_inv_records_material_group', 'inventory_records', ['material_group'], unique=False)
    op.create_index('ix_inv_records_business_area', 'inventory_records', ['business_area'], unique=False)
    op.create_index('ix_inv_records_material_type', 'inventory_records', ['material_type'], unique=False)
    op.create_index('ix_inv_records_division', 'inventory_records', ['division'], unique=False)
    op.create_index('ix_inv_records_period', 'inventory_records', ['period_year', 'period_month'], unique=False)


def downgrade():
    op.drop_index('ix_inv_records_period', table_name='inventory_records')
    op.drop_index('ix_inv_records_division', table_name='inventory_records')
    op.drop_index('ix_inv_records_material_type', table_name='inventory_records')
    op.drop_index('ix_inv_records_business_area', table_name='inventory_records')
    op.drop_index('ix_inv_records_material_group', table_name='inventory_records')
    op.drop_index('ix_inv_records_material', table_name='inventory_records')
    op.drop_index('ix_inv_records_plant', table_name='inventory_records')
    op.drop_index('ix_inv_records_upload_id', table_name='inventory_records')
    op.drop_table('inventory_records')

    op.drop_index('ix_inv_uploads_status', table_name='inventory_uploads')
    op.drop_index('ix_inv_uploads_created_at', table_name='inventory_uploads')
    op.drop_index('ix_inv_uploads_uploaded_by', table_name='inventory_uploads')
    op.drop_table('inventory_uploads')
