"""CLI command registration."""

from app.cli.backups import restore_db, validate_backup
from app.cli.cache import clear_cache
from app.cli.inventory import inventory_prune_procurement_low_value, inventory_seed_audit
from app.cli.seed import (
    fix_password_hashes,
    list_users,
    seed_admin,
    seed_initial_data,
    seed_module_permissions,
    seed_notifications,
)
from app.cli.normalize import normalize_roles
from app.cli.tasks import generate_recurring_tasks


def register_cli(app):
    """Attach custom CLI commands to the Flask app."""
    app.cli.add_command(restore_db)
    app.cli.add_command(validate_backup)
    app.cli.add_command(clear_cache)
    app.cli.add_command(inventory_seed_audit)
    app.cli.add_command(inventory_prune_procurement_low_value)
    app.cli.add_command(seed_initial_data)
    app.cli.add_command(seed_module_permissions)
    app.cli.add_command(seed_admin)
    app.cli.add_command(list_users)
    app.cli.add_command(seed_notifications)
    app.cli.add_command(normalize_roles)
    app.cli.add_command(generate_recurring_tasks)
    app.cli.add_command(fix_password_hashes)
