from __future__ import annotations

"""Inventory-specific CLI commands."""

import json
from pathlib import Path

import click
from flask import current_app
from flask.cli import with_appcontext

from app.core.services.inventory_seed_audit import (
    LOW_VALUE_THRESHOLD,
    audit_current_seed_files,
    delete_procurement_rows_below_threshold,
    get_procurement_rows_below_threshold,
)
from app.core.services.msds_service import (
    MSDSError,
    get_legacy_msds_storage_root,
    iter_legacy_msds_records,
    msds_file_exists,
    store_msds_bytes,
)
from app.extensions import db


def _print_summary(report: dict) -> None:
    summary = report["summary"]
    click.echo("Inventory seed audit")
    click.echo(f"  Consumption file: {report['files']['consumption']}")
    click.echo(f"  Procurement file: {report['files']['procurement']}")
    click.echo(f"  MC.9 file: {report['files']['mc9']}")
    click.echo("")
    click.echo("Coverage")
    click.echo(
        "  Consumption: "
        f"{summary['consumption']['rows']} rows, "
        f"{summary['consumption']['materials']} materials, "
        f"{summary['consumption']['plants']} plants, "
        f"{summary['consumption']['storage_locations']} storage locations"
    )
    click.echo(
        "  Procurement: "
        f"{summary['procurement']['rows']} rows, "
        f"{summary['procurement']['materials']} materials, "
        f"{summary['procurement']['plants']} plants, "
        f"{summary['procurement']['vendors']} vendors"
    )
    click.echo(
        "  MC.9: "
        f"{summary['mc9']['rows']} rows, "
        f"{summary['mc9']['materials']} materials, "
        f"{summary['mc9']['plants']} plants, "
        f"{summary['mc9']['matched_rows']} matched rows, "
        f"{summary['mc9']['discrepancy_count']} discrepancies"
    )
    click.echo(
        "  Overlap: "
        f"{summary['cross_file']['material_overlap']} material descriptions, "
        f"{summary['cross_file']['plant_overlap']} plants"
    )
    click.echo("")


def _print_findings(report: dict) -> None:
    findings = report["findings"]
    if not findings:
        click.echo("No findings.")
        return
    click.echo("Findings")
    for finding in findings:
        click.echo(
            f"  [{finding['severity'].upper()}] {finding['scope']}.{finding['check']}: "
            f"{finding['count']} row(s) or key(s) - {finding['message']}"
        )
        for row in finding.get("rows", [])[:5]:
            click.echo("    - " + ", ".join(f"{key}={value or '∅'}" for key, value in row.items()))
        if not finding.get("rows"):
            for example in finding.get("examples", [])[:5]:
                click.echo(f"    - {example}")


@click.command("inventory-seed-audit")
@click.option("--json-out", "json_out", type=click.Path(dir_okay=False, writable=True, path_type=str))
@click.option(
    "--fail-on",
    type=click.Choice(["none", "error", "warning"], case_sensitive=False),
    default="error",
    show_default=True,
    help="Exit non-zero when findings at or above this severity are present.",
)
@with_appcontext
def inventory_seed_audit(json_out: str | None, fail_on: str) -> None:
    """Run a sanity audit for the inventory seed workbooks."""
    report = audit_current_seed_files()
    _print_summary(report)
    _print_findings(report)

    if json_out:
        with open(json_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        click.echo("")
        click.echo(f"JSON report written to {json_out}")

    severities = {finding["severity"] for finding in report["findings"]}
    fail_on = fail_on.lower()
    if fail_on == "warning" and report["findings"]:
        raise click.ClickException("Audit reported warning-or-higher findings.")
    if fail_on == "error" and "error" in severities:
        raise click.ClickException("Audit reported error findings.")


@click.command("inventory-prune-procurement-low-value")
@click.option("--threshold", type=float, default=LOW_VALUE_THRESHOLD, show_default=True)
@click.option("--apply", is_flag=True, help="Delete the matching rows after confirmation.")
@with_appcontext
def inventory_prune_procurement_low_value(threshold: float, apply: bool) -> None:
    """Preview or delete procurement rows below the effective-value threshold."""
    preview = get_procurement_rows_below_threshold(threshold)
    click.echo(
        f"Rows below threshold: {preview['count']} "
        f"(threshold: {threshold:,.2f})"
    )
    for row in preview["rows"]:
        click.echo("  - " + ", ".join(f"{column}={row.get(column) or '∅'}" for column in preview["columns"]))

    if not apply:
        click.echo("")
        click.echo("Dry run only. Re-run with --apply to delete these rows.")
        return

    if not preview["rows"]:
        click.echo("No rows to delete.")
        return

    if not click.confirm("Delete these rows from the current procurement seed file?", default=False):
        raise click.ClickException("Deletion cancelled.")

    deleted = delete_procurement_rows_below_threshold(threshold)
    click.echo(f"Deleted {deleted['count']} procurement row(s).")


@click.command("inventory-migrate-legacy-msds")
@click.option(
    "--source-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Directory containing legacy filesystem MSDS PDFs.",
)
@click.option(
    "--strategy",
    type=click.Choice(["legacy-table", "filename"], case_sensitive=False),
    default="legacy-table",
    show_default=True,
    help="Use legacy metadata table mappings or derive material codes from PDF filenames.",
)
@click.option("--dry-run", is_flag=True, help="Preview imports without writing to msds_files.")
@with_appcontext
def inventory_migrate_legacy_msds(
    source_dir: Path | None,
    strategy: str,
    dry_run: bool,
) -> None:
    """Import legacy filesystem-backed MSDS PDFs into the msds_files table."""

    root = source_dir or get_legacy_msds_storage_root()
    if not root.exists():
        raise click.ClickException(f"Legacy MSDS directory was not found: {root}")

    imported = 0
    skipped = 0
    missing = 0
    failed = 0

    try:
        if strategy == "legacy-table":
            records = list(iter_legacy_msds_records())
            if not records:
                click.echo("No legacy MSDS metadata rows were found.")
                return

            for record in records:
                source_path = root / str(record["storage_path"])
                if not source_path.exists():
                    missing += 1
                    click.echo(f"Missing file: {source_path}")
                    continue

                file_bytes = source_path.read_bytes()
                if msds_file_exists(
                    material_code=record["material_code"],
                    filename=record["original_filename"],
                    file_size=len(file_bytes),
                    uploaded_at=record["uploaded_at"],
                ):
                    skipped += 1
                    continue

                if dry_run:
                    imported += 1
                    continue

                store_msds_bytes(
                    material_code=record["material_code"],
                    filename=record["original_filename"],
                    file_bytes=file_bytes,
                    content_type=record["mime_type"],
                    uploaded_at=record["uploaded_at"],
                )
                imported += 1
        else:
            for source_path in sorted(root.rglob("*.pdf")):
                material_code = source_path.stem.strip()
                file_bytes = source_path.read_bytes()
                if msds_file_exists(
                    material_code=material_code,
                    filename=source_path.name,
                    file_size=len(file_bytes),
                ):
                    skipped += 1
                    continue

                if dry_run:
                    imported += 1
                    continue

                store_msds_bytes(
                    material_code=material_code,
                    filename=source_path.name,
                    file_bytes=file_bytes,
                    content_type="application/pdf",
                )
                imported += 1

        if dry_run:
            db.session.rollback()
        else:
            db.session.commit()
    except MSDSError as exc:
        db.session.rollback()
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Failed to migrate legacy MSDS files")
        failed += 1
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Source directory: {root}")
    click.echo(f"Strategy: {strategy}")
    click.echo(f"Imported: {imported}")
    click.echo(f"Skipped duplicates: {skipped}")
    click.echo(f"Missing files: {missing}")
    click.echo(f"Failures: {failed}")
    if dry_run:
        click.echo("Dry run only. No records were written.")
