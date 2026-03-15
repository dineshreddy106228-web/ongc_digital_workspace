from __future__ import annotations

"""Inventory-specific CLI commands."""

import json

import click
from flask.cli import with_appcontext

from app.services.inventory_seed_audit import (
    LOW_VALUE_THRESHOLD,
    audit_current_seed_files,
    delete_procurement_rows_below_threshold,
    get_procurement_rows_below_threshold,
)


def _print_summary(report: dict) -> None:
    summary = report["summary"]
    click.echo("Inventory seed audit")
    click.echo(f"  Consumption file: {report['files']['consumption']}")
    click.echo(f"  Procurement file: {report['files']['procurement']}")
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
