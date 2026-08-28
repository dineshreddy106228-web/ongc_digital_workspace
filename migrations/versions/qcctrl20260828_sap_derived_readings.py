"""Materialise the SAP readings the portfolio analytics groups on.

``usage_outcome`` and ``turnaround_days`` are derived from columns already on
the row.  They exist so the whole-load analytics can be counted by the database
instead of by a Python scan over every record ever imported.

The backfill below repeats the derivation rather than importing it from
app.core.services.sap_quality_control: a migration has to keep working against
the code as it is today, not as the service is later changed.  New and updated
rows are derived by a mapper event on QCSAPRecord, so this runs once.

Revision ID: qcctrl20260828_sap_derived
Revises: qcctrl20260828_wb_retention
"""

import re

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260828_sap_derived"
down_revision = "qcctrl20260828_wb_retention"
branch_labels = None
depends_on = None


# Mirrors usage_decision_outcome() and sap_turnaround_days() as at this revision.
_USAGE_DECISION = re.compile(r"(?:^|\b)(?:UD\s*)?([AR])(?:$|\b)")
_OUTCOMES = {"A": "accepted", "R": "rejected"}

_BACKFILL_BATCH = 1000


def _outcome(value):
    match = _USAGE_DECISION.search((value or "").strip().upper())
    return _OUTCOMES.get(match.group(1)) if match else None


def _turnaround(start_inspection_date, notification_start_date, completion_date):
    start_date = start_inspection_date or notification_start_date
    if not start_date or not completion_date:
        return None
    elapsed = (completion_date - start_date).days
    return elapsed if elapsed >= 0 else None


def upgrade():
    op.add_column("qc_sap_records", sa.Column("usage_outcome", sa.String(length=12), nullable=True))
    op.add_column("qc_sap_records", sa.Column("turnaround_days", sa.Integer(), nullable=True))
    op.create_index(
        "ix_qc_sap_records_usage_outcome", "qc_sap_records", ["usage_outcome"], unique=False,
    )

    connection = op.get_bind()
    records = sa.table(
        "qc_sap_records",
        sa.column("id", sa.BigInteger),
        sa.column("usage_decision_code", sa.String),
        sa.column("start_inspection_date", sa.Date),
        sa.column("notification_start_date", sa.Date),
        sa.column("completion_date", sa.Date),
        sa.column("usage_outcome", sa.String),
        sa.column("turnaround_days", sa.Integer),
    )
    rows = connection.execute(
        sa.select(
            records.c.id,
            records.c.usage_decision_code,
            records.c.start_inspection_date,
            records.c.notification_start_date,
            records.c.completion_date,
        )
    ).fetchall()

    # Only rows that actually carry a reading are written, so an untouched
    # portfolio costs one SELECT and nothing else.
    updates = []
    for row in rows:
        outcome = _outcome(row.usage_decision_code)
        turnaround = _turnaround(
            row.start_inspection_date, row.notification_start_date, row.completion_date,
        )
        if outcome is None and turnaround is None:
            continue
        updates.append({"record_id": row.id, "outcome": outcome, "turnaround": turnaround})

    statement = (
        records.update()
        .where(records.c.id == sa.bindparam("record_id"))
        .values(usage_outcome=sa.bindparam("outcome"), turnaround_days=sa.bindparam("turnaround"))
    )
    for start in range(0, len(updates), _BACKFILL_BATCH):
        connection.execute(statement, updates[start:start + _BACKFILL_BATCH])


def downgrade():
    op.drop_index("ix_qc_sap_records_usage_outcome", table_name="qc_sap_records")
    op.drop_column("qc_sap_records", "turnaround_days")
    op.drop_column("qc_sap_records", "usage_outcome")
