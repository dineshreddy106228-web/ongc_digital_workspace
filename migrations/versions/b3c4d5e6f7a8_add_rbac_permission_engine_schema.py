"""add RBAC permission engine schema

Adds the schema changes for the centralized RBAC permission engine:
  - users.accepting_officer_id  (FK → users.id)
  - tasks.self_task_visible_to_controlling_officer  (BOOLEAN, default 0)
  - task_offices junction table  (task_id ↔ office_id)

All columns are nullable / have safe defaults so existing rows are
unaffected.  The migration is safe to run on a production MySQL database
with data already in the users, tasks, and offices tables.

Revision ID: b3c4d5e6f7a8
Revises: a1b2c3d4e5f7
Create Date: 2026-03-21 10:05:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b3c4d5e6f7a8"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def upgrade():
    # ── 1. users.accepting_officer_id ─────────────────────────────
    op.add_column(
        "users",
        sa.Column("accepting_officer_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_users_accepting_officer_id",
        "users",
        "users",
        ["accepting_officer_id"],
        ["id"],
    )
    op.create_index(
        "ix_users_accepting_officer_id",
        "users",
        ["accepting_officer_id"],
        unique=False,
    )

    # ── 2. tasks.self_task_visible_to_controlling_officer ─────────
    op.add_column(
        "tasks",
        sa.Column(
            "self_task_visible_to_controlling_officer",
            sa.Boolean(),
            nullable=False,
            server_default="0",
        ),
    )

    # ── 3. task_offices junction table ────────────────────────────
    op.create_table(
        "task_offices",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.BigInteger(), nullable=False),
        sa.Column("office_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["office_id"], ["offices.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id", "office_id", name="uq_task_offices_task_office"),
    )
    op.create_index(
        "ix_task_offices_task_id",
        "task_offices",
        ["task_id"],
        unique=False,
    )
    op.create_index(
        "ix_task_offices_office_id",
        "task_offices",
        ["office_id"],
        unique=False,
    )


def downgrade():
    # ── 3. Drop task_offices ──────────────────────────────────────
    op.drop_index("ix_task_offices_office_id", table_name="task_offices")
    op.drop_index("ix_task_offices_task_id", table_name="task_offices")
    op.drop_table("task_offices")

    # ── 2. Drop tasks.self_task_visible_to_controlling_officer ────
    op.drop_column("tasks", "self_task_visible_to_controlling_officer")

    # ── 1. Drop users.accepting_officer_id ────────────────────────
    op.drop_index("ix_users_accepting_officer_id", table_name="users")
    op.drop_constraint("fk_users_accepting_officer_id", "users", type_="foreignkey")
    op.drop_column("users", "accepting_officer_id")
