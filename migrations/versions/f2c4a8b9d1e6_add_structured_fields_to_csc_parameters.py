"""Add structured fields to CSC parameters

Revision ID: f2c4a8b9d1e6
Revises: d2a6c1b4f9e7, 7a9c5f4d2e31
Create Date: 2026-03-18 16:45:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "f2c4a8b9d1e6"
down_revision = ("d2a6c1b4f9e7", "7a9c5f4d2e31")
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("csc_parameters", schema=None) as batch_op:
        batch_op.add_column(sa.Column("unit_of_measure", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("parameter_conditions", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("required_value_type", sa.String(length=20), nullable=False, server_default="text"))
        batch_op.add_column(sa.Column("required_value_text", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("required_value_operator_1", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("required_value_value_1", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("required_value_operator_2", sa.String(length=10), nullable=True))
        batch_op.add_column(sa.Column("required_value_value_2", sa.String(length=100), nullable=True))


def downgrade():
    with op.batch_alter_table("csc_parameters", schema=None) as batch_op:
        batch_op.drop_column("required_value_value_2")
        batch_op.drop_column("required_value_operator_2")
        batch_op.drop_column("required_value_value_1")
        batch_op.drop_column("required_value_operator_1")
        batch_op.drop_column("required_value_text")
        batch_op.drop_column("required_value_type")
        batch_op.drop_column("parameter_conditions")
        batch_op.drop_column("unit_of_measure")
