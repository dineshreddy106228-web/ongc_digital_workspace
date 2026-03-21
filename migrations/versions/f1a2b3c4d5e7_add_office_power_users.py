"""add office power-user configuration

Revision ID: f1a2b3c4d5e7
Revises: e4f5a6b7c8d9
Create Date: 2026-03-21 18:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f1a2b3c4d5e7"
down_revision = "e4f5a6b7c8d9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "offices",
        sa.Column(
            "power_user_limit",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "users",
        sa.Column(
            "is_power_user",
            sa.Boolean(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade():
    op.drop_column("users", "is_power_user")
    op.drop_column("offices", "power_user_limit")
