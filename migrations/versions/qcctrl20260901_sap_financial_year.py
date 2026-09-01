"""Anchor each SAP monitoring record to its Indian financial year.

A year's base data is imported once from the full SAP notification history.
Every later upload carries only current and newly created notifications, so the
dashboards can no longer read "the records in the latest batch" without losing
the rest of the year.  They read the financial year instead, which this column
records on the row.

The backfill repeats the derivation rather than importing it from
app.core.services.sap_quality_control: a migration has to keep working against
the code as it is today, not as the service is later changed.

Revision ID: qcctrl20260901_sap_fy
Revises: qcctrl20260828_sap_derived
"""

from alembic import op
import sqlalchemy as sa


revision = "qcctrl20260901_sap_fy"
down_revision = "qcctrl20260828_sap_derived"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "qc_sap_records", sa.Column("financial_year", sa.String(length=9), nullable=True),
    )
    op.create_index(
        "ix_qc_sap_records_lab_year", "qc_sap_records", ["lab_code", "financial_year"],
    )
    # An Indian financial year runs 1 April to 31 March and is labelled by both
    # calendar years.  A record belongs to the year its SAP notification was
    # raised in; a lot carrying no notification falls back to its own receipt.
    op.execute(
        """
        UPDATE qc_sap_records
           SET financial_year = CONCAT(
                   CASE WHEN MONTH(COALESCE(notification_start_date, start_inspection_date)) >= 4
                        THEN YEAR(COALESCE(notification_start_date, start_inspection_date))
                        ELSE YEAR(COALESCE(notification_start_date, start_inspection_date)) - 1
                   END,
                   '-',
                   LPAD(
                       MOD(
                           CASE WHEN MONTH(COALESCE(notification_start_date, start_inspection_date)) >= 4
                                THEN YEAR(COALESCE(notification_start_date, start_inspection_date)) + 1
                                ELSE YEAR(COALESCE(notification_start_date, start_inspection_date))
                           END,
                           100
                       ), 2, '0'
                   )
               )
         WHERE COALESCE(notification_start_date, start_inspection_date) IS NOT NULL
        """
    )


def downgrade():
    op.drop_index("ix_qc_sap_records_lab_year", table_name="qc_sap_records")
    op.drop_column("qc_sap_records", "financial_year")
