"""CLI command registration."""

from app.cli.backups import restore_db, validate_backup
from app.cli.cache import clear_cache
from app.cli.csc import (
    backfill_csc_material_codes_command,
    import_csc_legacy,
    resequence_csc_draft_ids_command,
    sync_csc_master_data_command,
)
from app.cli.seed import (
    fix_password_hashes,
    list_users,
    seed_admin,
    seed_initial_data,
    seed_module_permissions,
    seed_notifications,
    seed_v11_broadcast,
)
from app.cli.normalize import normalize_roles
from app.cli.tasks import generate_recurring_tasks


def register_cli(app):
    """Attach custom CLI commands to the Flask app."""
    app.cli.add_command(restore_db)
    app.cli.add_command(validate_backup)
    app.cli.add_command(clear_cache)
    app.cli.add_command(import_csc_legacy)
    app.cli.add_command(backfill_csc_material_codes_command)
    app.cli.add_command(resequence_csc_draft_ids_command)
    app.cli.add_command(sync_csc_master_data_command)
    app.cli.add_command(seed_initial_data)
    app.cli.add_command(seed_module_permissions)
    app.cli.add_command(seed_admin)
    app.cli.add_command(list_users)
    app.cli.add_command(seed_notifications)
    app.cli.add_command(normalize_roles)
    app.cli.add_command(generate_recurring_tasks)
    app.cli.add_command(fix_password_hashes)
    app.cli.add_command(seed_v11_broadcast)

    # Inventory CLI commands depend on pandas/numpy (heavy optional deps).
    # Import and register them only when ENABLE_INVENTORY is explicitly True
    # so a production instance without pandas can still boot cleanly.
    if app.config.get("ENABLE_INVENTORY", False):
        from app.cli.inventory import (  # noqa: PLC0415
            inventory_migrate_legacy_msds,
            inventory_prune_procurement_low_value,
            inventory_seed_audit,
        )
        app.cli.add_command(inventory_seed_audit)
        app.cli.add_command(inventory_prune_procurement_low_value)
        app.cli.add_command(inventory_migrate_legacy_msds)
