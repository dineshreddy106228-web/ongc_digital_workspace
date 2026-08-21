"""add QC testing standards"""
from alembic import op
import sqlalchemy as sa
revision = "8c9d0e1f2a3b"
down_revision = "7b8c9d0e1f2a"
branch_labels = depends_on = None
def upgrade():
    op.create_table("qc_testing_standards", sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True), sa.Column("chemical_name", sa.String(255), nullable=False), sa.Column("normalized_name", sa.String(255), nullable=False, unique=True), sa.Column("specification_no", sa.String(255)), sa.Column("material_code", sa.String(100)), sa.Column("standard_days", sa.Integer()), sa.Column("remarks", sa.Text()), sa.Column("updated_by", sa.BigInteger(), sa.ForeignKey("users.id", ondelete="SET NULL")), sa.Column("updated_at", sa.DateTime(), nullable=False))
def downgrade(): op.drop_table("qc_testing_standards")
