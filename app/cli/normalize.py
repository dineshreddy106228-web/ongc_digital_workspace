"""
flask normalize-roles
─────────────────────
One-time (idempotent) role normalization command.

Canonical role set after this command runs:
    admin       – full administrative access
    superuser   – full platform access, all modules
    user        – explicit module access controlled by admin

Legacy → canonical renames applied automatically:
    super_user  →  superuser

Any role that is neither canonical nor a rename source is removed after
its users are migrated to the 'user' role.

Usage:
    flask normalize-roles
    flask normalize-roles --dry-run        # preview changes without writing
"""

import click
from flask.cli import with_appcontext
from sqlalchemy.exc import SQLAlchemyError
from app.extensions import db
from app.models.role import Role
from app.models.user import User
from app.core.roles import (
    ADMIN_ROLE,
    USER_ROLE,
    ROLE_REGISTRY,
    ROLE_DESCRIPTIONS,
    ROLE_RENAME_MAP,
)


# ── Configuration ────────────────────────────────────────────────────────────

# {name: description} dict built from the central registry — single source of truth.
CANONICAL_ROLES: dict[str, str] = {
    name: ROLE_DESCRIPTIONS[name] for name in ROLE_REGISTRY
}

# Legacy name → canonical name (imported from app.core.roles).
RENAME_MAP: dict[str, str] = ROLE_RENAME_MAP

# Fallback role for users whose role is being deleted (imported from app.core.roles).
FALLBACK_ROLE: str = USER_ROLE

# Username that must exist and carry the ADMIN_ROLE.
ADMIN_USERNAME = "admin"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _log(msg: str, *, dry: bool = False, level: str = "info") -> None:
    """Emit a coloured log line."""
    prefix = "[DRY-RUN] " if dry else ""
    colors = {"info": "cyan", "ok": "green", "warn": "yellow", "error": "red"}
    click.echo(click.style(f"  {prefix}{msg}", fg=colors.get(level, "white")))


def _apply_renames(dry: bool) -> dict[str, Role]:
    """
    Rename legacy role names to their canonical equivalents in-place.

    Returns a mapping of all current Role objects keyed by name (post-rename).
    """
    all_roles: list[Role] = Role.query.all()
    role_by_name: dict[str, Role] = {r.name: r for r in all_roles}

    for legacy_name, canonical_name in RENAME_MAP.items():
        if legacy_name not in role_by_name:
            continue  # Already gone or never existed – nothing to do.

        if canonical_name in role_by_name:
            # Target canonical name already exists as a separate row.
            # We cannot rename into an existing name (unique constraint).
            # Users of the legacy role need to be pointed at the canonical one instead.
            legacy_role = role_by_name[legacy_name]
            canonical_role = role_by_name[canonical_name]
            affected = User.query.filter_by(role_id=legacy_role.id).all()
            if affected:
                _log(
                    f"Both '{legacy_name}' and '{canonical_name}' exist – "
                    f"reassigning {len(affected)} user(s) from '{legacy_name}' "
                    f"to existing '{canonical_name}'",
                    dry=dry, level="warn",
                )
                if not dry:
                    for u in affected:
                        u.role_id = canonical_role.id
                    db.session.flush()

            # Delete the now-empty legacy role row.
            _log(f"Deleting duplicate legacy role '{legacy_name}'", dry=dry, level="warn")
            if not dry:
                db.session.delete(legacy_role)
                db.session.flush()
                del role_by_name[legacy_name]
        else:
            # Simple in-place rename: just update the name column.
            _log(f"Renamed role  '{legacy_name}'  →  '{canonical_name}'", dry=dry, level="ok")
            if not dry:
                role_by_name[legacy_name].name = canonical_name
                db.session.flush()
                role_by_name[canonical_name] = role_by_name.pop(legacy_name)

    # Re-fetch to reflect any flushes (avoids stale cache issues).
    if not dry:
        role_by_name = {r.name: r for r in Role.query.all()}

    return role_by_name


def _remove_unknown_roles(role_by_name: dict[str, Role], dry: bool) -> dict[str, Role]:
    """
    Delete any role whose name is not in CANONICAL_ROLES.

    Affected users are reassigned to FALLBACK_ROLE before deletion.
    """
    fallback_role = role_by_name.get(FALLBACK_ROLE)
    # Fallback may not exist yet – it will be created in the next step.
    # We must handle users here safely: if fallback doesn't exist yet,
    # we can only proceed if the unknown role has zero users.

    unknown = [name for name in role_by_name if name not in CANONICAL_ROLES]

    for name in unknown:
        role = role_by_name[name]
        affected_users = User.query.filter_by(role_id=role.id).all()

        if affected_users:
            if fallback_role is None:
                _log(
                    f"Cannot delete unknown role '{name}' yet "
                    f"({len(affected_users)} user(s) affected) – "
                    f"'{FALLBACK_ROLE}' role doesn't exist to migrate them to. "
                    "It will be created next; re-run the command to complete.",
                    dry=dry, level="error",
                )
                continue

            _log(
                f"Reassigning {len(affected_users)} user(s) from '{name}' "
                f"→ '{FALLBACK_ROLE}'",
                dry=dry, level="warn",
            )
            if not dry:
                for u in affected_users:
                    u.role_id = fallback_role.id
                db.session.flush()

        _log(f"Deleted  role '{name}'", dry=dry, level="warn")
        if not dry:
            db.session.delete(role)
            db.session.flush()
            del role_by_name[name]

    return role_by_name


def _ensure_canonical_roles(role_by_name: dict[str, Role], dry: bool) -> dict[str, Role]:
    """Create any canonical role that is missing."""
    for name, description in CANONICAL_ROLES.items():
        if name in role_by_name:
            _log(f"Role '{name}' already exists – OK", dry=dry, level="info")
            continue

        _log(f"Created  role '{name}'", dry=dry, level="ok")
        if not dry:
            new_role = Role(name=name, description=description, is_active=True)
            db.session.add(new_role)
            db.session.flush()
            role_by_name[name] = new_role

    return role_by_name


def _validate_admin_user(role_by_name: dict[str, Role], dry: bool) -> None:
    """
    Ensure the 'admin' user exists and carries the 'admin' role.

    - If the user exists but has a different role, the role is updated.
    - If the user does not exist, a warning is printed (creation is left
      to seed-admin so credentials are not hardcoded here).
    """
    admin_role = role_by_name.get("admin")
    if admin_role is None:
        _log(
            "Cannot validate admin user – 'admin' role was not created "
            "(dry-run may suppress this).",
            dry=dry, level="warn",
        )
        return

    user: User | None = User.query.filter_by(username=ADMIN_USERNAME).first()
    if user is None:
        _log(
            f"User '{ADMIN_USERNAME}' not found – skipping role assignment. "
            "Run 'flask seed-admin' to create the bootstrap user.",
            dry=dry, level="warn",
        )
        return

    if user.role_id == admin_role.id:
        _log(
            f"User '{ADMIN_USERNAME}' already has role 'admin' – OK",
            dry=dry, level="info",
        )
        return

    current_role_name = user.role.name if user.role else "(none)"
    _log(
        f"Assigned role 'admin' to user '{ADMIN_USERNAME}' "
        f"(was: '{current_role_name}')",
        dry=dry, level="ok",
    )
    if not dry:
        user.role_id = admin_role.id
        db.session.flush()


# ── Command ──────────────────────────────────────────────────────────────────

@click.command("normalize-roles")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview changes without writing to the database.",
)
@with_appcontext
def normalize_roles(dry_run: bool):
    """Normalize roles to the canonical set: admin, superuser, user.

    \b
    Actions performed:
      1. Rename legacy role names (e.g. super_user → superuser)
      2. Delete unknown roles (users reassigned to 'user' first)
      3. Create missing canonical roles
      4. Assign role 'admin' to user 'admin' if found

    Safe to run multiple times – already-correct states are left untouched.
    """
    sep = "─" * 52
    click.echo(sep)
    click.echo(click.style("  flask normalize-roles", bold=True))
    if dry_run:
        click.echo(click.style("  MODE: DRY RUN – no changes will be saved", fg="yellow"))
    click.echo(sep)

    try:
        # Step 1: rename legacy role names
        click.echo("\nStep 1 · Rename legacy roles")
        role_by_name = _apply_renames(dry=dry_run)

        # Step 2: remove roles not in the canonical set
        click.echo("\nStep 2 · Remove unknown roles")
        role_by_name = _remove_unknown_roles(role_by_name, dry=dry_run)

        # Step 3: create any canonical role that is still missing
        click.echo("\nStep 3 · Ensure canonical roles exist")
        role_by_name = _ensure_canonical_roles(role_by_name, dry=dry_run)

        # Step 4: validate the admin user
        click.echo("\nStep 4 · Validate admin user")
        _validate_admin_user(role_by_name, dry=dry_run)

        # Commit all changes in a single transaction.
        if not dry_run:
            db.session.commit()
            click.echo(
                click.style(
                    "\n✔  Normalization complete. All changes committed.",
                    fg="green", bold=True,
                )
            )
        else:
            db.session.rollback()
            click.echo(
                click.style(
                    "\n✔  Dry run complete. No changes were written.",
                    fg="yellow", bold=True,
                )
            )

    except SQLAlchemyError as exc:
        db.session.rollback()
        raise click.ClickException(
            f"Database error during normalization – rolled back.\n  {exc}"
        ) from exc
    except click.ClickException:
        db.session.rollback()
        raise
    except Exception as exc:
        db.session.rollback()
        raise click.ClickException(
            f"Unexpected error – rolled back.\n  {exc}"
        ) from exc

    click.echo(sep)
