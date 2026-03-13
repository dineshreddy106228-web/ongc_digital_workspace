"""CLI command registration."""

from app.cli.seed import seed_initial_data, seed_module_permissions


def register_cli(app):
    """Attach custom CLI commands to the Flask app."""
    app.cli.add_command(seed_initial_data)
    app.cli.add_command(seed_module_permissions)
