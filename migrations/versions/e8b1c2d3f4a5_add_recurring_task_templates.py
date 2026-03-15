"""add recurring task templates

Revision ID: e8b1c2d3f4a5
Revises: c4e7f9a2d1b3
Create Date: 2026-03-14 11:15:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e8b1c2d3f4a5"
down_revision = "c4e7f9a2d1b3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "recurring_task_templates",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("task_title", sa.String(length=255), nullable=False),
        sa.Column("task_description", sa.Text(), nullable=True),
        sa.Column("task_origin", sa.String(length=100), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("priority", sa.String(length=50), nullable=False),
        sa.Column("task_scope", sa.String(length=50), nullable=False),
        sa.Column("owner_id", sa.BigInteger(), nullable=True),
        sa.Column("created_by", sa.BigInteger(), nullable=True),
        sa.Column("office_id", sa.BigInteger(), nullable=True),
        sa.Column("recurrence_type", sa.String(length=20), nullable=False),
        sa.Column("weekly_days", sa.String(length=32), nullable=True),
        sa.Column("monthly_day", sa.Integer(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("next_generation_date", sa.Date(), nullable=True),
        sa.Column("last_generated_at", sa.DateTime(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.ForeignKeyConstraint(["office_id"], ["offices.id"]),
        sa.ForeignKeyConstraint(["owner_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_recurring_task_templates_next_generation_date",
        "recurring_task_templates",
        ["next_generation_date"],
        unique=False,
    )
    op.create_index(
        "ix_recurring_task_templates_owner_id",
        "recurring_task_templates",
        ["owner_id"],
        unique=False,
    )
    op.create_index(
        "ix_recurring_task_templates_task_scope",
        "recurring_task_templates",
        ["task_scope"],
        unique=False,
    )
    op.create_index(
        "ix_recurring_task_templates_is_active",
        "recurring_task_templates",
        ["is_active"],
        unique=False,
    )

    op.create_table(
        "recurring_task_collaborators",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("template_id", sa.BigInteger(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["template_id"], ["recurring_task_templates.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "template_id",
            "user_id",
            name="uq_recurring_task_collaborators_template_user",
        ),
    )
    op.create_index(
        "ix_recurring_task_collaborators_user_id",
        "recurring_task_collaborators",
        ["user_id"],
        unique=False,
    )

    op.add_column(
        "tasks",
        sa.Column("recurring_template_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "tasks",
        sa.Column("occurrence_date", sa.Date(), nullable=True),
    )
    op.create_index(
        "ix_tasks_recurring_template_id",
        "tasks",
        ["recurring_template_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_tasks_recurring_template_id",
        "tasks",
        "recurring_task_templates",
        ["recurring_template_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_tasks_recurring_template_occurrence",
        "tasks",
        ["recurring_template_id", "occurrence_date"],
    )


def downgrade():
    op.drop_constraint(
        "uq_tasks_recurring_template_occurrence",
        "tasks",
        type_="unique",
    )
    op.drop_constraint(
        "fk_tasks_recurring_template_id",
        "tasks",
        type_="foreignkey",
    )
    op.drop_index("ix_tasks_recurring_template_id", table_name="tasks")
    op.drop_column("tasks", "occurrence_date")
    op.drop_column("tasks", "recurring_template_id")

    op.drop_index(
        "ix_recurring_task_collaborators_user_id",
        table_name="recurring_task_collaborators",
    )
    op.drop_table("recurring_task_collaborators")

    op.drop_index(
        "ix_recurring_task_templates_is_active",
        table_name="recurring_task_templates",
    )
    op.drop_index(
        "ix_recurring_task_templates_task_scope",
        table_name="recurring_task_templates",
    )
    op.drop_index(
        "ix_recurring_task_templates_owner_id",
        table_name="recurring_task_templates",
    )
    op.drop_index(
        "ix_recurring_task_templates_next_generation_date",
        table_name="recurring_task_templates",
    )
    op.drop_table("recurring_task_templates")
