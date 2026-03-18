"""add inventory msds documents table

Revision ID: f3a1b2c4d5e6
Revises: ee4884ed2952, e8b1c2d3f4a5
Create Date: 2026-03-18 19:25:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f3a1b2c4d5e6"
down_revision = ("ee4884ed2952", "e8b1c2d3f4a5")
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "inventory_msds_documents",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("material_code", sa.String(length=50), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("stored_filename", sa.String(length=255), nullable=False),
        sa.Column("storage_path", sa.String(length=512), nullable=False),
        sa.Column("mime_type", sa.String(length=100), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256_hex", sa.String(length=64), nullable=False),
        sa.Column("uploaded_by", sa.BigInteger(), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["material_code"],
            ["material_master.material"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["uploaded_by"],
            ["users.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "material_code",
            name="uq_inventory_msds_documents_material_code",
        ),
    )
    with op.batch_alter_table("inventory_msds_documents", schema=None) as batch_op:
        batch_op.create_index(
            "ix_inventory_msds_documents_uploaded_at",
            ["uploaded_at"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("inventory_msds_documents", schema=None) as batch_op:
        batch_op.drop_index("ix_inventory_msds_documents_uploaded_at")

    op.drop_table("inventory_msds_documents")
