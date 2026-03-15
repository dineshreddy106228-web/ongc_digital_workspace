"""Cache maintenance CLI commands."""

import click
from flask.cli import with_appcontext

from app.extensions import cache


@click.command("clear-cache")
@with_appcontext
def clear_cache():
    """Clear the configured application cache backend."""
    cache.clear()
    click.echo("Application cache cleared.")
