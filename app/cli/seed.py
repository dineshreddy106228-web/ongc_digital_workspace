"""Seed / bootstrap data – run once after initial migration.

Usage:
    flask seed-initial-data          # First-time setup: roles, office, admin
    flask seed-module-permissions    # Governance V2: set default module access for all users
"""

import click
from flask import current_app
from flask.cli import with_appcontext
from app.extensions import db
from app.models.role import Role
from app.models.office import Office
from app.models.user import User


# ── Role definitions ─────────────────────────────────────────────
# Two-role model: super_user has full access; user has explicit module access only.
DEFAULT_ROLES = [
    ("super_user", "Full access – all modules, all actions, user management"),
    ("user",       "Explicit module access only – controlled by super_user"),
]


def _seed_roles():
    """Create default roles if they do not already exist."""
    created = 0
    for name, description in DEFAULT_ROLES:
        if not Role.query.filter_by(name=name).first():
            db.session.add(Role(name=name, description=description))
            created += 1
    db.session.commit()
    click.echo(f"  Roles: {created} created, {len(DEFAULT_ROLES) - created} already present.")


def _seed_pilot_office():
    """Create the pilot office from config if missing."""
    code = current_app.config["PILOT_OFFICE_CODE"]
    if not Office.query.filter_by(office_code=code).first():
        db.session.add(Office(
            office_code=code,
            office_name="Corporate Chemistry",
            location="Dehradun",
        ))
        db.session.commit()
        click.echo(f"  Pilot office '{code}' created.")
    else:
        click.echo(f"  Pilot office '{code}' already exists.")


def _seed_bootstrap_admin():
    """Create the bootstrap super_user (system admin) from .env credentials."""
    username = current_app.config["BOOTSTRAP_ADMIN_USERNAME"]
    email = current_app.config["BOOTSTRAP_ADMIN_EMAIL"]
    password = current_app.config["BOOTSTRAP_ADMIN_PASSWORD"]

    if User.query.filter_by(username=username).first():
        click.echo(f"  Bootstrap admin '{username}' already exists – skipping.")
        return

    role = Role.query.filter_by(name="super_user").first()
    office = Office.query.filter_by(
        office_code=current_app.config["PILOT_OFFICE_CODE"]
    ).first()

    if role is None:
        click.echo("  ERROR: super_user role not found. Run role seed first.")
        return

    admin = User(
        username=username,
        full_name="System Administrator",
        email=email,
        role_id=role.id,
        office_id=office.id if office else None,
        must_change_password=True,
    )
    admin.set_password(password)
    db.session.add(admin)
    db.session.commit()
    click.echo(f"  Bootstrap admin '{username}' created (must_change_password=True).")


def _seed_default_module_permissions():
    """
    Set sensible default module permissions for existing users.

    Rules:
      super_user  → all SUPER_USER_MODULES (persisted so admin UI reflects state)
      user        → dashboard only by default (super_user assigns further access)

    Safe to run multiple times – skips users who already have permissions.
    """
    from app.models.user_module_permission import (
        UserModulePermission,
        SUPER_USER_MODULES,
    )

    users = User.query.filter_by(is_active=True).all()
    added = 0

    for user in users:
        if not user.role:
            continue

        role_name = user.role.name

        if role_name == "super_user":
            codes = list(SUPER_USER_MODULES)
        else:
            # role == 'user' (or any unrecognised legacy role) → dashboard only
            codes = ["dashboard"]

        for code in codes:
            exists = UserModulePermission.query.filter_by(
                user_id=user.id, module_code=code
            ).first()
            if not exists:
                db.session.add(
                    UserModulePermission(
                        user_id=user.id,
                        module_code=code,
                        can_access=True,
                    )
                )
                added += 1

    db.session.commit()
    click.echo(f"  Module permissions: {added} new entries added.")


@click.command("seed-initial-data")
@with_appcontext
def seed_initial_data():
    """Create default roles, pilot office, and bootstrap admin."""
    click.echo("Seeding initial data …")
    _seed_roles()
    _seed_pilot_office()
    _seed_bootstrap_admin()
    click.echo("Done.")


@click.command("seed-module-permissions")
@with_appcontext
def seed_module_permissions():
    """Governance V2: seed default module permissions for all existing users.

    Safe to run multiple times. Does not overwrite existing permissions.
    Run this once after applying the Governance V2 migration.
    """
    click.echo("Seeding default module permissions …")
    _seed_roles()   # Ensure super_user role exists first
    _seed_default_module_permissions()
    click.echo("Done.")
