"""Give an office a second location: Head Corporate Chemistry works from Mumbai and Dehradun.

An office has always carried exactly one location, but Head Corporate Chemistry
operates from both Mumbai and Dehradun while remaining one office with one task
register. A second location column records that without splitting the office in
two, which would have forked its register.

Revision ID: b3c4d5e6f7a9
Revises: a2b3c4d5e6f8
"""
import sqlalchemy as sa
from alembic import op

revision = "b3c4d5e6f7a9"
down_revision = "a2b3c4d5e6f8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "offices",
        sa.Column("secondary_location", sa.String(length=150), nullable=True, server_default=""),
    )
    # Offices already working from two places had both crammed into the single
    # free-text location, separated by a pipe. Split those into the two columns
    # so the second location is real data rather than a formatting convention.
    op.execute(
        "UPDATE offices SET "
        "  secondary_location = TRIM(SUBSTRING_INDEX(location, '|', -1)), "
        "  location = TRIM(SUBSTRING_INDEX(location, '|', 1)) "
        "WHERE location LIKE '%|%'"
    )
    # Head Corporate Chemistry functions from Dehradun as well as Mumbai, whether
    # or not the pipe convention was ever applied to its row.
    op.execute(
        "UPDATE offices SET secondary_location = 'Dehradun' "
        "WHERE office_code = 'CORP_CHEM' AND COALESCE(TRIM(secondary_location), '') = ''"
    )


def downgrade():
    # Fold the second location back into the single field it came from, so the
    # information survives the rollback in the pipe form the data used before.
    op.execute(
        "UPDATE offices SET location = CONCAT(location, ' | ', secondary_location) "
        "WHERE COALESCE(TRIM(secondary_location), '') <> ''"
    )
    op.drop_column("offices", "secondary_location")
