"""Governance V2b – rename task_type → task_origin; simplify roles to user/super_user.

Revision ID: 4b7c30d91f2a
Revises: 3f9a21c85d4e
Create Date: 2026-03-13

Changes:
  1. Rename tasks.task_type column to tasks.task_origin
  2. Remove legacy roles (super_admin, admin, owner, supporting_officer, viewer)
     that are no longer exposed in the UI.  Users already assigned those roles
     are migrated to the nearest equivalent (super_user or user).
  3. Ensures 'user' and 'super_user' roles exist in the roles table.

IMPORTANT – role migration logic:
  super_admin  → super_user
  admin        → super_user   (had user-management access, promote to super_user)
  owner / supporting_officer / viewer → user
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers
revision = "4b7c30d91f2a"
down_revision = "3f9a21c85d4e"
branch_labels = None
depends_on = None


def upgrade():
    # ── 1. Rename tasks.task_type → tasks.task_origin ─────────────
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.alter_column(
            "task_type",
            new_column_name="task_origin",
            existing_type=sa.String(100),
            existing_nullable=True,
        )

    # ── 2. Ensure new roles exist ──────────────────────────────────
    conn = op.get_bind()

    # Insert 'super_user' if missing
    conn.execute(sa.text("""
        INSERT INTO roles (name, description, is_active, created_at, updated_at)
        SELECT 'super_user',
               'Full access – all modules, all actions, user management',
               1,
               NOW(),
               NOW()
        WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'super_user')
    """))

    # Insert 'user' if missing
    conn.execute(sa.text("""
        INSERT INTO roles (name, description, is_active, created_at, updated_at)
        SELECT 'user',
               'Explicit module access only – controlled by super_user',
               1,
               NOW(),
               NOW()
        WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'user')
    """))

    # ── 3. Migrate users assigned legacy roles ─────────────────────
    # super_admin / admin  →  super_user
    conn.execute(sa.text("""
        UPDATE users
        SET role_id = (SELECT id FROM roles WHERE name = 'super_user')
        WHERE role_id IN (
            SELECT id FROM roles WHERE name IN ('super_admin', 'admin')
        )
    """))

    # owner / supporting_officer / viewer  →  user
    conn.execute(sa.text("""
        UPDATE users
        SET role_id = (SELECT id FROM roles WHERE name = 'user')
        WHERE role_id IN (
            SELECT id FROM roles WHERE name IN ('owner', 'supporting_officer', 'viewer')
        )
    """))

    # ── 4. Deactivate legacy roles (soft-remove, preserving FK integrity) ──
    conn.execute(sa.text("""
        UPDATE roles
        SET is_active = 0
        WHERE name IN ('super_admin', 'admin', 'owner', 'supporting_officer', 'viewer')
    """))


def downgrade():
    # ── Re-activate legacy roles ──────────────────────────────────
    conn = op.get_bind()
    conn.execute(sa.text("""
        UPDATE roles
        SET is_active = 1
        WHERE name IN ('super_admin', 'admin', 'owner', 'supporting_officer', 'viewer')
    """))

    # ── Rename task_origin → task_type ────────────────────────────
    with op.batch_alter_table("tasks", schema=None) as batch_op:
        batch_op.alter_column(
            "task_origin",
            new_column_name="task_type",
            existing_type=sa.String(100),
            existing_nullable=True,
        )
