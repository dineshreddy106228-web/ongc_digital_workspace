"""Backfill the LPG/20 document designation on databases already migrated.

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
"""
from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from alembic import op

revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    metadata = sa.MetaData()
    standards = sa.Table("qc_testing_standards", metadata, autoload_with=bind)
    authorised_labs = sa.Table("csc_authorized_labs", metadata, autoload_with=bind)
    specification_no = "ONGC/LPG/20/2026"
    chemical_name = "Strong Base Anion Exchange Resin"
    record = bind.execute(
        sa.select(standards.c.id).where(standards.c.specification_no == specification_no)
    ).first()
    if record is None:
        result = bind.execute(
            standards.insert().values(
                chemical_name=chemical_name,
                normalized_name="strongbaseanionexchangeresin",
                specification_no=specification_no,
                material_code=None,
                standard_days=None,
                remarks="Designated laboratory source; Standard Testing Time not supplied.",
                updated_at=datetime.utcnow(),
            )
        )
        record_id = result.inserted_primary_key[0]
    else:
        record_id = record.id

    entry_ref = f"r-{record_id}"
    held = set(bind.execute(
        sa.select(authorised_labs.c.lab_code).where(authorised_labs.c.entry_ref == entry_ref)
    ).scalars())
    now = datetime.utcnow()
    inserts = [
        {"entry_ref": entry_ref, "lab_code": lab_code, "updated_at": now}
        for lab_code in ("rgl_panvel", "rgl_vadodara") if lab_code not in held
    ]
    if inserts:
        bind.execute(authorised_labs.insert(), inserts)


def downgrade():
    pass
