"""Add checklist state to CSC impact analysis

Revision ID: d2a6c1b4f9e7
Revises: c1d2e3f4a5b6
Create Date: 2026-03-18 08:30:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "d2a6c1b4f9e7"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("csc_impact_analysis", schema=None) as batch_op:
        batch_op.add_column(sa.Column("checklist_state_json", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("csc_impact_analysis", schema=None) as batch_op:
        batch_op.drop_column("checklist_state_json")
