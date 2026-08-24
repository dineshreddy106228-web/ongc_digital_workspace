"""Break-glass password recovery from the server shell.

Every other reset in this workspace needs a second person: a user raises a
request, an administrator verifies who is asking and issues the password.  That
breaks down in exactly one case — every administrator locked out at once, with
nobody left to approve.  This command is the way back in, and shell access to
the server is deliberately the bar for using it.

Because it answers to no second person, it writes an audit row naming the
operating-system account and host that ran it.  Usage:

    flask issue-temp-password --username admin_dinesh
    flask issue-temp-password --username admin_dinesh --password 'Chosen@2026'
"""

import getpass
import socket

import click
from flask.cli import with_appcontext

from app.core.services.password_reset import (
    generate_temporary_password,
    temp_password_ttl_hours,
    validate_chosen_password,
)
from app.core.utils.datetime import format_datetime_ist
from app.extensions import db
from app.models.core.audit_log import AuditLog
from app.models.core.password_reset_request import (
    STATUS_APPROVED,
    STATUS_PENDING,
    PasswordResetRequest,
)
from app.models.core.user import User


@click.command("issue-temp-password")
@click.option("--username", required=True, help="Account to issue a temporary password for.")
@click.option(
    "--password",
    default=None,
    help="Password to set. Omit to have one generated, which is preferred.",
)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@with_appcontext
def issue_temp_password(username, password, yes):
    """Issue a temporary password for one account, and record that it happened."""
    target = User.query.filter_by(username=username.strip()).first()
    if target is None:
        raise click.ClickException(f"No user found with username '{username}'.")

    if password is not None:
        error = validate_chosen_password(password)
        if error:
            raise click.ClickException(error)
        new_password = password
    else:
        new_password = generate_temporary_password()

    ttl = temp_password_ttl_hours()
    operator = f"{getpass.getuser()}@{socket.gethostname()}"

    click.echo("")
    click.echo(f"  Account   {target.username} ({target.full_name or 'no name on record'})")
    click.echo(f"  Role      {target.role.name if target.role else 'none'}")
    click.echo(f"  Active    {'yes' if target.is_active else 'no'}")
    click.echo(f"  Valid for {ttl} hour(s), then it stops working")
    click.echo(f"  Recorded  as run by {operator}")
    click.echo("")

    if not yes and not click.confirm("Issue this temporary password?", default=False):
        click.echo("Nothing was changed.")
        return

    expires_at = target.set_temporary_password(new_password, ttl)

    # A break-glass reset answers whatever the user had already asked for.
    pending = PasswordResetRequest.query.filter_by(
        user_id=target.id, status=STATUS_PENDING
    ).all()
    for entry in pending:
        entry.status = STATUS_APPROVED
        entry.handled_note = f"Handled from the server shell by {operator}."
        entry.temp_password_expires_at = expires_at

    db.session.flush()

    AuditLog.log(
        action="PASSWORD_RESET_BREAK_GLASS",
        entity_type="User",
        entity_id=str(target.id),
        details=(
            f"Temporary password issued for '{target.username}' from the server "
            f"shell by {operator}; "
            f"{'generated' if password is None else 'operator-supplied'}; "
            f"expires {expires_at.isoformat()}."
        ),
    )
    db.session.commit()

    click.echo(f"  Temporary password:  {new_password}")
    click.echo(f"  Stops working at:    {format_datetime_ist(expires_at)} IST")
    click.echo("")
    click.echo(
        "  Give it to the holder of the account directly. They must set their own\n"
        "  password on the next sign-in before anything else opens."
    )
    click.echo("")
