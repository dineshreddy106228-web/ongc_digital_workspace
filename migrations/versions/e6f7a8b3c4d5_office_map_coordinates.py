"""Put an office's map coordinates on its own record.

The location navigator read coordinates from a table keyed by office_code that
was hardcoded inside the dashboard template. Any office whose code was not in
that table was silently dropped from the map — it appeared in the list beside
the map but was never pinned. Deployments with different office codes therefore
showed an almost empty map.

Coordinates now live on the office. The backfill seeds every location the app
already knew about, matched by code and then by name, so existing deployments
keep their pins without anyone re-entering them.

Revision ID: e6f7a8b3c4d5
Revises: d5e6f7a8b2c3
"""
import sqlalchemy as sa
from alembic import op

revision = "e6f7a8b3c4d5"
down_revision = "d5e6f7a8b2c3"
branch_labels = None
depends_on = None

# Coordinates the templates already carried, by office_code.
BY_CODE = {
    "DFS_AHMD": (23.03, 72.58),
    "ST_MHSN": (23.60, 72.40),
    "QPCL": (21.12, 72.65),
    "DFS_MUMBAI": (19.00, 72.85),
    "MH_ASSET": (19.50, 71.60),
    "DFS_ASSAM": (26.98, 94.64),
    "ST_RJY": (17.00, 81.78),
    "CORP_CHEM": (19.08, 72.88),
}

# The QC laboratories, matched on name for deployments whose offices are the
# laboratories themselves. Coordinates come from the QC lab navigator.
BY_NAME = {
    "RGL Panvel": (18.99, 73.11),
    "RGL Vadodara": (22.31, 73.18),
    "RGL Jorhat": (26.75, 94.22),
    "RGL Rajahmundry": (17.00, 81.78),
    "RGL Chennai": (13.08, 80.27),
    "IDWE Dehradun": (30.32, 78.03),
}


def upgrade():
    for column in ("latitude", "longitude", "secondary_latitude", "secondary_longitude"):
        op.add_column("offices", sa.Column(column, sa.Float(), nullable=True))

    offices = sa.table(
        "offices",
        sa.column("office_code", sa.String),
        sa.column("office_name", sa.String),
        sa.column("latitude", sa.Float),
        sa.column("longitude", sa.Float),
        sa.column("secondary_latitude", sa.Float),
        sa.column("secondary_longitude", sa.Float),
    )

    for code, (lat, lng) in BY_CODE.items():
        op.execute(
            offices.update().where(offices.c.office_code == code)
            .values(latitude=lat, longitude=lng)
        )

    for name, (lat, lng) in BY_NAME.items():
        op.execute(
            offices.update()
            .where(sa.and_(offices.c.office_name == name, offices.c.latitude.is_(None)))
            .values(latitude=lat, longitude=lng)
        )

    # Head Corporate Chemistry works from Dehradun as well as Mumbai.
    op.execute(
        offices.update().where(offices.c.office_code == "CORP_CHEM")
        .values(secondary_latitude=30.32, secondary_longitude=78.03)
    )


def downgrade():
    for column in ("secondary_longitude", "secondary_latitude", "longitude", "latitude"):
        op.drop_column("offices", column)
