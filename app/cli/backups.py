"""Backup and restore CLI commands."""

from __future__ import annotations

from pathlib import Path

import click
from flask.cli import with_appcontext

from app.extensions import db
from app.services.backups import (
    BackupError,
    resolve_database_connection_settings,
    restore_database_backup,
    validate_backup_file,
)


@click.command("restore-db")
@click.option(
    "--file",
    "backup_file",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
    help="Path to a .sql or .sql.gz backup file.",
)
@click.option(
    "--confirm-restore",
    is_flag=True,
    default=False,
    help="Required safety flag for destructive restore operations.",
)
@with_appcontext
def restore_db(backup_file: Path, confirm_restore: bool) -> None:
    """Restore a MySQL dump into the configured database."""

    if not confirm_restore:
        raise click.ClickException(
            "Restore aborted. Re-run with --confirm-restore. "
            "Full restores are destructive and are not wrapped in a single rollback-safe transaction."
        )

    try:
        settings = resolve_database_connection_settings()
        validation = validate_backup_file(backup_file)
    except BackupError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(
        "Starting restore for "
        f"database '{settings.database}' on host '{settings.host}' "
        f"from '{validation['path']}'."
    )
    click.echo(
        "Warning: mysql imports are not fully transactional. "
        "If the restore fails partway through, partial changes may already be applied."
    )

    db.session.remove()
    db.engine.dispose()

    try:
        result = restore_database_backup(backup_file)
    except BackupError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(
        "Restore completed successfully for "
        f"'{result['database']}' from '{result['path']}'."
    )


@click.command("validate-backup")
@click.option(
    "--file",
    "backup_file",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
    help="Path to a .sql or .sql.gz backup file.",
)
@with_appcontext
def validate_backup(backup_file: Path) -> None:
    """Validate that a backup file exists and is readable."""

    try:
        result = validate_backup_file(backup_file)
    except BackupError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Backup file is valid: {result['path']}")
    click.echo(
        f"Compressed: {'yes' if result['compressed'] else 'no'} | "
        f"Size: {result['size_bytes']} bytes"
    )
    click.echo("Preview:")
    for line in result["preview_lines"]:
        click.echo(f"  {line}")
