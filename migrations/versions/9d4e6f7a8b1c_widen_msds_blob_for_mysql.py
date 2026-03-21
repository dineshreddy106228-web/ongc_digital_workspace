"""widen msds blob storage for mysql

Revision ID: 9d4e6f7a8b1c
Revises: 8c9d7e6f5a4b
Create Date: 2026-03-21 09:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = "9d4e6f7a8b1c"
down_revision = "8c9d7e6f5a4b"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name == "mysql":
        with op.batch_alter_table("msds_files", schema=None) as batch_op:
            batch_op.alter_column(
                "data",
                existing_type=sa.LargeBinary(),
                type_=mysql.LONGBLOB(),
                existing_nullable=False,
            )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "mysql":
        with op.batch_alter_table("msds_files", schema=None) as batch_op:
            batch_op.alter_column(
                "data",
                existing_type=mysql.LONGBLOB(),
                type_=sa.LargeBinary(),
                existing_nullable=False,
            )
