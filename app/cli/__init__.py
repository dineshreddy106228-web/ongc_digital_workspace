"""CLI command registration."""

from app.cli.backups import restore_db, validate_backup
from app.cli.cache import clear_cache
from app.cli.seed import (
    list_users,
    seed_admin,
    seed_initial_data,
    seed_module_permissions,
    seed_notifications,
)
from app.cli.normalize import normalize_roles


def register_cli(app):
    """Attach custom CLI commands to the Flask app."""
    app.cli.add_command(restore_db)
    app.cli.add_command(validate_backup)
    app.cli.add_command(clear_cache)
    app.cli.add_command(seed_initial_data)
    app.cli.add_command(seed_module_permissions)
    app.cli.add_command(seed_admin)
    app.cli.add_command(list_users)
    app.cli.add_command(seed_notifications)
    app.cli.add_command(normalize_roles)
