"""CLI operation for the controlled workbook rollback window."""

from __future__ import annotations

import click
from flask.cli import with_appcontext

from app.core.services.audit_workbook_retention import purge_expired_audit_workbook_payloads
from app.extensions import db


@click.command("prune-audit-workbooks")
@with_appcontext
def prune_audit_workbooks() -> None:
    """Remove source workbook bytes that are outside the 15-day rollback window."""
    counts = purge_expired_audit_workbook_payloads()
    db.session.commit()
    click.echo(
        "Expired workbook payloads cleared — "
        f"Inventory: {counts['inventory']}; "
        f"QC weekly: {counts['qc_weekly']}; "
        f"QC SAP: {counts['qc_sap']}."
    )
