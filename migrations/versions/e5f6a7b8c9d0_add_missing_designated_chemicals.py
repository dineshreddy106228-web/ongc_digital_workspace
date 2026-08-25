"""Add document-designated chemicals absent from the current QC register.

The laboratory source names four chemicals which do not yet have a register
row. They are added without a Standard Testing Time or material code because
the source supplies neither; their laboratory designations are then recorded.

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
"""
from __future__ import annotations

import re
from datetime import datetime

import sqlalchemy as sa
from alembic import op

revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


_MISSING_DOCUMENT_STANDARDS = (
    (
        "ONGC/WIC/08/2026", "Corrosion Inhibitor (Kalol/Nawagam)",
        ("rgl_vadodara", "rgl_rajahmundry"),
    ),
    ("ONGC/WIC/26/2025", "THPS", ("rgl_panvel", "rgl_chennai")),
    ("", "CMC (HVT) API Grade", ("rgl_vadodara", "idwe_dehradun", "rgl_chennai")),
    ("", "CMC (LVT) API Grade", ("rgl_vadodara", "idwe_dehradun", "rgl_chennai")),
    ("ONGC/LPG/20/2026", "Strong Base Anion Exchange Resin", ("rgl_panvel", "rgl_vadodara")),
)


def _normal(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def upgrade():
    bind = op.get_bind()
    metadata = sa.MetaData()
    standards = sa.Table("qc_testing_standards", metadata, autoload_with=bind)
    authorised_labs = sa.Table("csc_authorized_labs", metadata, autoload_with=bind)
    now = datetime.utcnow()

    for specification_no, chemical_name, lab_codes in _MISSING_DOCUMENT_STANDARDS:
        record = bind.execute(
            sa.select(standards.c.id).where(
                standards.c.specification_no == specification_no
                if specification_no
                else standards.c.normalized_name == _normal(chemical_name)
            )
        ).first()
        if record is None:
            result = bind.execute(
                standards.insert().values(
                    chemical_name=chemical_name,
                    normalized_name=_normal(chemical_name),
                    specification_no=specification_no or None,
                    material_code=None,
                    standard_days=None,
                    remarks="Designated laboratory source; Standard Testing Time not supplied.",
                    updated_at=now,
                )
            )
            record_id = result.inserted_primary_key[0]
        else:
            record_id = record.id

        entry_ref = f"r-{record_id}"
        held = {
            row.lab_code
            for row in bind.execute(
                sa.select(authorised_labs.c.lab_code).where(
                    authorised_labs.c.entry_ref == entry_ref
                )
            )
        }
        inserts = [
            {"entry_ref": entry_ref, "lab_code": lab_code, "updated_at": now}
            for lab_code in lab_codes if lab_code not in held
        ]
        if inserts:
            bind.execute(authorised_labs.insert(), inserts)


def downgrade():
    # Preserve manually maintained register data if a deployment is rolled back.
    pass
