"""add slot codes to msds files

Revision ID: c9f4e1a2b3d6
Revises: b3c4d5e6f7a8
Create Date: 2026-03-22 11:30:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c9f4e1a2b3d6"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "msds_files",
        sa.Column("slot_code", sa.String(length=32), nullable=True),
    )
    op.create_index(
        "ix_msds_files_material_slot",
        "msds_files",
        ["material_code", "slot_code"],
        unique=False,
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT id, material_code
            FROM msds_files
            ORDER BY material_code ASC, uploaded_at DESC, id DESC
            """
        )
    ).mappings()

    slot_order = ["standard", "vendor_1", "vendor_2", "vendor_3"]
    per_material_counts: dict[str, int] = {}

    for row in rows:
        material_code = str(row["material_code"] or "").strip()
        count = per_material_counts.get(material_code, 0)
        slot_code = slot_order[count] if count < len(slot_order) else f"legacy_{count - len(slot_order) + 1}"
        bind.execute(
            sa.text(
                """
                UPDATE msds_files
                SET slot_code = :slot_code
                WHERE id = :row_id
                """
            ),
            {"slot_code": slot_code, "row_id": row["id"]},
        )
        per_material_counts[material_code] = count + 1

    op.alter_column(
        "msds_files",
        "slot_code",
        existing_type=sa.String(length=32),
        nullable=False,
        server_default="standard",
    )


def downgrade():
    op.drop_index("ix_msds_files_material_slot", table_name="msds_files")
    op.drop_column("msds_files", "slot_code")
