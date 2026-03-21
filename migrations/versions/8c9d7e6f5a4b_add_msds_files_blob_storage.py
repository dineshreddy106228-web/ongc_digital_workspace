"""add msds_files blob storage table

Revision ID: 8c9d7e6f5a4b
Revises: 6b1c2d3e4f5a
Create Date: 2026-03-20 12:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8c9d7e6f5a4b"
down_revision = "6b1c2d3e4f5a"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "msds_files",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("material_code", sa.String(length=50), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("content_type", sa.String(length=100), nullable=False),
        sa.Column("file_size", sa.BigInteger(), nullable=False),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=False),
        sa.ForeignKeyConstraint(
            ["material_code"],
            ["material_master.material"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("msds_files", schema=None) as batch_op:
        batch_op.create_index("ix_msds_files_material_code", ["material_code"], unique=False)
        batch_op.create_index("ix_msds_files_uploaded_at", ["uploaded_at"], unique=False)


def downgrade():
    with op.batch_alter_table("msds_files", schema=None) as batch_op:
        batch_op.drop_index("ix_msds_files_uploaded_at")
        batch_op.drop_index("ix_msds_files_material_code")

    op.drop_table("msds_files")
