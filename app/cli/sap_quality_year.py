"""Seed one financial year of SAP quality monitoring from a pair of exports.

The daily upload screen deliberately insists on two reports from the same SAP
run.  Opening a financial year is a different act: the notification history and
the inspection-lot register are pulled once, often on different days, and the
result becomes the base every later daily upload merges into.  That is what
this command does, under an explicit as-of date and with nothing hidden.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import click
from flask.cli import with_appcontext

from app.extensions import db


def _as_of(value: str | None, notifications, inspections) -> date:
    if value:
        return datetime.strptime(value, "%d.%m.%Y").date()
    resolved = notifications.as_of_date or inspections.as_of_date
    if resolved is None:
        raise click.ClickException(
            "Could not read an as-of date from either export. Pass --as-of DD.MM.YYYY."
        )
    return resolved


@click.command("seed-sap-financial-year")
@click.option("--inspection", "inspection_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--notifications", "notification_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--as-of", "as_of_value", default=None, help="Snapshot date as DD.MM.YYYY. Defaults to the notification export's own date.")
@click.option("--drop-lot-only", is_flag=True, help="First remove records held only by an inspection lot, with no SAP notification.")
@click.option("--commit", is_flag=True, help="Write the result. Without this the command reports what it would do and rolls back.")
@with_appcontext
def seed_sap_financial_year(inspection_path, notification_path, as_of_value, drop_lot_only, commit):
    """Load a financial year's base data from a paired SAP export."""
    from app.core.services.sap_quality_control import (
        SAP_PLANT_LAB_CODES,
        _persist_sap_lab_snapshot,
        _rows_by_plant,
        _validate_central_sap_plants,
        financial_year_label,
        financial_year_start,
        parse_sap_inspection_workbook,
        parse_sap_notification_workbook,
    )
    from app.models.quality_control.qc_sap_monitoring import QCSAPRecord

    inspection_source = Path(inspection_path).read_bytes()
    notification_source = Path(notification_path).read_bytes()

    inspections = parse_sap_inspection_workbook(
        inspection_source, Path(inspection_path).name,
        expected_plant=None, allow_multiple_plants=True,
    )
    notifications = parse_sap_notification_workbook(
        notification_source, Path(notification_path).name,
        expected_plant=None, allow_multiple_plants=True,
    )
    as_of_date = _as_of(as_of_value, notifications, inspections)
    year = financial_year_label(as_of_date)

    # The notification scope follows the as-of date the operator confirmed,
    # which may differ from the date printed in the export itself.
    year_start = financial_year_start(as_of_date)
    in_year = [row for row in notifications.rows if row["notification_start_date"] >= year_start]
    dropped_out_of_year = len(notifications.rows) - len(in_year)

    _validate_central_sap_plants(inspections.rows, in_year)

    click.echo(f"Financial year {year}, as of {as_of_date:%d.%m.%Y}")
    click.echo(f"  notifications in year      : {len(in_year)}")
    for reason, count in sorted(notifications.excluded_rows.items()):
        click.echo(f"  notifications left out     : {count} ({reason.replace('_', ' ')})")
    if dropped_out_of_year:
        click.echo(f"  notifications left out     : {dropped_out_of_year} (outside the confirmed as-of year)")
    click.echo(f"  laboratory inspection lots : {len(inspections.rows)}")
    for reason, count in sorted(inspections.excluded_rows.items()):
        click.echo(f"  inspection rows left out   : {count} ({reason.replace('_', ' ')})")

    if drop_lot_only:
        removed = QCSAPRecord.query.filter_by(source_completeness="inspection_lot_only").delete()
        click.echo(f"  lot-only records removed   : {removed}")

    inspections_by_plant = _rows_by_plant(inspections.rows)
    notifications_by_plant = _rows_by_plant(in_year)
    for plant_code in sorted(set(inspections_by_plant) | set(notifications_by_plant)):
        batch = _persist_sap_lab_snapshot(
            lab_code=SAP_PLANT_LAB_CODES[plant_code],
            plant_code=plant_code,
            inspection_rows=inspections_by_plant.get(plant_code, []),
            notification_rows=notifications_by_plant.get(plant_code, []),
            as_of_date=as_of_date,
            inspection_source=inspection_source,
            inspection_filename=Path(inspection_path).name,
            notification_source=notification_source,
            notification_filename=Path(notification_path).name,
            uploaded_by=None,
            excluded_rows={**inspections.excluded_rows, **notifications.excluded_rows},
        )
        click.echo(
            f"  {plant_code} {batch.lab_code:<22} {batch.record_count:>5} records "
            f"({batch.record_count - batch.unmatched_inspection_count - batch.unmatched_notification_count} matched, "
            f"{batch.unmatched_notification_count} notification-only, "
            f"{batch.unmatched_inspection_count} lot-only)"
        )

    if commit:
        db.session.commit()
        click.echo("Committed.")
    else:
        db.session.rollback()
        click.echo("Rolled back. Re-run with --commit to write this.")
