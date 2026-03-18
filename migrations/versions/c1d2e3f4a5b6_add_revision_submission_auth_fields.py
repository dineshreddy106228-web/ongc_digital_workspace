"""Add CSC revision submission authorization fields

Revision ID: c1d2e3f4a5b6
Revises: 91c4e2f7b8aa
Create Date: 2026-03-18 04:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "c1d2e3f4a5b6"
down_revision = "91c4e2f7b8aa"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("csc_revisions", schema=None) as batch_op:
        batch_op.add_column(sa.Column("authorization_confirmed", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("authorized_by_name", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("subcommittee_head_name", sa.String(length=255), nullable=True))


def downgrade():
    with op.batch_alter_table("csc_revisions", schema=None) as batch_op:
        batch_op.drop_column("subcommittee_head_name")
        batch_op.drop_column("authorized_by_name")
        batch_op.drop_column("authorization_confirmed")
