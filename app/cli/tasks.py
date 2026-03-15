from __future__ import annotations

"""Task CLI commands."""

import click
from flask.cli import with_appcontext
from sqlalchemy.exc import SQLAlchemyError

from app.extensions import db
from app.core.services.recurring_tasks import generate_due_recurring_tasks
from app.core.services.dashboard import invalidate_dashboard_summary_metrics


@click.command("generate-recurring-tasks")
@with_appcontext
def generate_recurring_tasks():
    """Generate due task instances from active recurring task templates."""
    try:
        result = generate_due_recurring_tasks()
        invalidate_dashboard_summary_metrics()
        db.session.commit()
        click.echo(
            "Recurring task generation complete: "
            f"{result['tasks']} task(s) created from {result['templates']} template(s)."
        )
    except SQLAlchemyError as exc:
        db.session.rollback()
        raise click.ClickException(
            f"Recurring task generation failed: {exc.__class__.__name__}"
        ) from exc
