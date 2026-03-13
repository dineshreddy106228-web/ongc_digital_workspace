"""Seed / bootstrap data – run once after initial migration.

Usage:
    flask seed-initial-data          # First-time setup: roles, office, admin
    flask seed-module-permissions    # Governance V2: set default module access for all users
    flask seed-admin                 # Railway-safe bootstrap superuser/admin account
    flask list-users                 # Print current users with role/office status
"""

import os
import click
from flask import current_app
from flask.cli import with_appcontext
from sqlalchemy.exc import SQLAlchemyError
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


def _read_required_bootstrap_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None:
        value = current_app.config.get(name)
    value = (value or "").strip()
    if not value:
        raise click.ClickException(
            f"Missing required environment variable/config: {name}"
        )
    return value


def _read_optional_bootstrap_env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        value = current_app.config.get(name)
    value = (value or "").strip()
    return value or None


def _ensure_office(office_code: str) -> Office:
    if not hasattr(Office, "office_code"):
        raise click.ClickException(
            "Office model does not expose 'office_code'; cannot seed office safely."
        )

    office = Office.query.filter_by(office_code=office_code).first()
    if office:
        click.echo(f"Office found: {office.office_code}")
        return office

    # Fallback defaults aligned with current pilot office conventions.
    office_name = "Corporate Chemistry" if office_code == "CORP_CHEM" else office_code
    location = "Dehradun" if office_code == "CORP_CHEM" else ""

    office_kwargs = {
        "office_code": office_code,
        "office_name": office_name,
    }
    if hasattr(Office, "location"):
        office_kwargs["location"] = location
    if hasattr(Office, "is_active"):
        office_kwargs["is_active"] = True

    office = Office(**office_kwargs)
    db.session.add(office)
    db.session.flush()
    click.echo(f"Office created: {office.office_code}")
    return office


def _ensure_superuser_role() -> Role:
    """
    Ensure a superuser role exists.

    Preference:
      1. superuser (requested role)
      2. super_user (legacy convention fallback)
      3. create superuser only when no conflicting super* naming exists
    """
    role = Role.query.filter_by(name="superuser").first()
    if role:
        click.echo("Role found: superuser")
        return role

    legacy_role = Role.query.filter_by(name="super_user").first()
    if legacy_role:
        click.echo("Role found (legacy convention): super_user")
        return legacy_role

    similar_super_roles = [
        r.name
        for r in Role.query.filter(Role.name.ilike("%super%")).all()
    ]
    if similar_super_roles:
        raise click.ClickException(
            "Role 'superuser' does not exist, and existing super-like roles "
            f"were found ({', '.join(similar_super_roles)}). "
            "Create/align roles explicitly before running seed-admin."
        )

    # No conflicting conventions detected; safe to create requested role.
    role_kwargs = {"name": "superuser"}
    if hasattr(Role, "description"):
        role_kwargs["description"] = "Bootstrap superuser account role"
    if hasattr(Role, "is_active"):
        role_kwargs["is_active"] = True

    role = Role(**role_kwargs)
    db.session.add(role)
    db.session.flush()
    click.echo("Role created: superuser")
    return role


def _upsert_bootstrap_user(
    username: str,
    email: str,
    password: str,
    full_name: str | None,
    office: Office,
    role: Role,
) -> User:
    user = User.query.filter_by(username=username).first()

    if user is None:
        user_kwargs = {"username": username}
        if hasattr(User, "email"):
            user_kwargs["email"] = email
        if full_name and hasattr(User, "full_name"):
            user_kwargs["full_name"] = full_name
        if hasattr(User, "office_id"):
            user_kwargs["office_id"] = office.id
        if hasattr(User, "role_id"):
            user_kwargs["role_id"] = role.id
        if hasattr(User, "is_active"):
            user_kwargs["is_active"] = True
        if hasattr(User, "must_change_password"):
            user_kwargs["must_change_password"] = True

        user = User(**user_kwargs)
        if not hasattr(user, "set_password"):
            raise click.ClickException(
                "User model does not expose set_password(...); cannot seed securely."
            )
        user.set_password(password)
        db.session.add(user)
        db.session.flush()
        click.echo(f"User created: {user.username}")
        return user

    if hasattr(user, "email"):
        user.email = email
    if full_name and hasattr(user, "full_name"):
        user.full_name = full_name
    if hasattr(user, "office_id"):
        user.office_id = office.id
    if hasattr(user, "role_id"):
        user.role_id = role.id
    if hasattr(user, "is_active"):
        user.is_active = True
    if hasattr(user, "must_change_password"):
        user.must_change_password = True

    if not hasattr(user, "set_password"):
        raise click.ClickException(
            "User model does not expose set_password(...); cannot reset password."
        )
    user.set_password(password)
    db.session.flush()
    click.echo(f"User updated: {user.username}")
    return user


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


@click.command("seed-admin")
@with_appcontext
def seed_admin():
    """
    Railway-safe bootstrap command for first-login account creation/update.

    Reads:
      - BOOTSTRAP_ADMIN_USERNAME
      - BOOTSTRAP_ADMIN_EMAIL
      - BOOTSTRAP_ADMIN_PASSWORD
      - PILOT_OFFICE_CODE
      - BOOTSTRAP_ADMIN_FULL_NAME (optional)
    """
    username = _read_required_bootstrap_env("BOOTSTRAP_ADMIN_USERNAME")
    email = _read_required_bootstrap_env("BOOTSTRAP_ADMIN_EMAIL")
    password = _read_required_bootstrap_env("BOOTSTRAP_ADMIN_PASSWORD")
    office_code = _read_required_bootstrap_env("PILOT_OFFICE_CODE")
    full_name = _read_optional_bootstrap_env("BOOTSTRAP_ADMIN_FULL_NAME")

    click.echo("Running seed-admin …")
    try:
        office = _ensure_office(office_code)
        role = _ensure_superuser_role()
        _upsert_bootstrap_user(
            username=username,
            email=email,
            password=password,
            full_name=full_name,
            office=office,
            role=role,
        )
        db.session.commit()
    except click.ClickException:
        db.session.rollback()
        raise
    except SQLAlchemyError as exc:
        db.session.rollback()
        raise click.ClickException(
            f"Database error while running seed-admin: {exc.__class__.__name__}"
        ) from exc

    click.echo("Seed complete.")


@click.command("list-users")
@with_appcontext
def list_users():
    """Print current users with role, office, and active status."""
    users = User.query.order_by(User.username.asc()).all()
    if not users:
        click.echo("No users found.")
        return

    click.echo("username | email | role | office | active")
    for user in users:
        username = getattr(user, "username", "") or ""
        email = getattr(user, "email", "") or "-"
        role_name = user.role.name if getattr(user, "role", None) else "-"
        office_code = user.office.office_code if getattr(user, "office", None) else "-"
        is_active = getattr(user, "is_active", True)
        click.echo(
            f"{username} | {email} | {role_name} | {office_code} | "
            f"{'yes' if is_active else 'no'}"
        )
