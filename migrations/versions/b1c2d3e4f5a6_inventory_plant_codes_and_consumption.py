"""SAP plant codes on an asset, an alert for unrecognised ones, and material consumption.

Three things the monitoring module could not record before:

* An asset's SAP plant codes. A merger leaves SAP reporting two codes into one
  asset — N&H and B&S merged into NH-BS while 12A1 and 13A1 stayed apart — so
  the codes belong on the surviving asset, and a retired asset row points at its
  successor through ``merged_into_id`` rather than being deleted.
* Unrecognised plants found during an import. Work centres are not expected to
  change, so a new one is raised for the module admin instead of appearing
  unannounced in the registers.
* Twelve-month consumption per material, and the unit it is measured in. The
  detailed inventory sheet has neither; the workbook's two material summary
  sheets carry quantity with its unit and the value of that consumption. The
  unit is also kept on the material itself, because every table states it beside
  the material code and it is what makes a material a liquid or a solid.

Revision ID: b1c2d3e4f5a6
Revises: a1c3e5f7b9d2
"""
import sqlalchemy as sa
from alembic import op

revision = "b1c2d3e4f5a6"
down_revision = "a1c3e5f7b9d2"
branch_labels = None
depends_on = None


def _has_column(table: str, column: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return column in {item["name"] for item in inspector.get_columns(table)}


def upgrade():
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())

    if "inventory_monitoring_work_centers" in tables:
        with op.batch_alter_table("inventory_monitoring_work_centers") as batch:
            if not _has_column("inventory_monitoring_work_centers", "sap_plant_codes"):
                batch.add_column(sa.Column("sap_plant_codes", sa.String(length=255), nullable=True))
            if not _has_column("inventory_monitoring_work_centers", "merged_into_id"):
                batch.add_column(sa.Column("merged_into_id", sa.BigInteger(), nullable=True))

    if "inventory_monitoring_materials" in tables and not _has_column("inventory_monitoring_materials", "uom"):
        with op.batch_alter_table("inventory_monitoring_materials") as batch:
            batch.add_column(sa.Column("uom", sa.String(length=32), nullable=True))

    if "inventory_monitoring_material_summaries" not in tables:
        op.create_table(
            "inventory_monitoring_material_summaries",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("snapshot_id", sa.BigInteger(), nullable=False),
            sa.Column("batch_id", sa.BigInteger(), nullable=False),
            sa.Column("material_id", sa.BigInteger(), nullable=True),
            sa.Column("material_group", sa.String(length=2), nullable=False),
            sa.Column("material_code", sa.String(length=64), nullable=False),
            sa.Column("material_description", sa.String(length=500), nullable=True),
            sa.Column("stock_qty", sa.Numeric(20, 3), nullable=True),
            sa.Column("uom", sa.String(length=32), nullable=True),
            sa.Column("consumption_qty_12m", sa.Numeric(20, 3), nullable=True),
            sa.Column("consumption_value_inr", sa.Numeric(20, 2), nullable=True),
            sa.Column("inventory_value_inr", sa.Numeric(20, 2), nullable=True),
            sa.Column("stock_months", sa.Numeric(12, 2), nullable=True),
            sa.ForeignKeyConstraint(["snapshot_id"], ["inventory_monitoring_snapshots.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["batch_id"], ["inventory_monitoring_upload_batches.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["material_id"], ["inventory_monitoring_materials.id"], ondelete="SET NULL"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("snapshot_id", "material_code", name="uq_inventory_monitoring_summary_material"),
        )
        op.create_index("ix_inventory_monitoring_summaries_snapshot", "inventory_monitoring_material_summaries", ["snapshot_id"])
        op.create_index("ix_inventory_monitoring_summaries_material", "inventory_monitoring_material_summaries", ["material_id"])

    if "inventory_monitoring_plant_alerts" not in tables:
        op.create_table(
            "inventory_monitoring_plant_alerts",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("plant_code", sa.String(length=32), nullable=True),
            sa.Column("work_center_name", sa.String(length=255), nullable=False),
            sa.Column("batch_id", sa.BigInteger(), nullable=True),
            sa.Column("work_center_id", sa.BigInteger(), nullable=True),
            sa.Column("line_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("inventory_value_inr", sa.Numeric(20, 2), nullable=True),
            sa.Column("status", sa.String(length=24), nullable=False, server_default="open"),
            sa.Column("resolution", sa.String(length=500), nullable=True),
            sa.Column("resolved_by", sa.BigInteger(), nullable=True),
            sa.Column("resolved_at", sa.DateTime(), nullable=True),
            sa.Column("detected_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["batch_id"], ["inventory_monitoring_upload_batches.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["work_center_id"], ["inventory_monitoring_work_centers.id"], ondelete="SET NULL"),
            sa.ForeignKeyConstraint(["resolved_by"], ["users.id"], ondelete="SET NULL"),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index("ix_inventory_monitoring_plant_alerts_status", "inventory_monitoring_plant_alerts", ["status"])


def downgrade():
    inspector = sa.inspect(op.get_bind())
    tables = set(inspector.get_table_names())
    # Dropping the table takes its indexes with it. Dropping them first fails on
    # MySQL, which still needs them for the table's foreign keys.
    if "inventory_monitoring_plant_alerts" in tables:
        op.drop_table("inventory_monitoring_plant_alerts")
    if "inventory_monitoring_material_summaries" in tables:
        op.drop_table("inventory_monitoring_material_summaries")
    if "inventory_monitoring_materials" in tables and _has_column("inventory_monitoring_materials", "uom"):
        with op.batch_alter_table("inventory_monitoring_materials") as batch:
            batch.drop_column("uom")
    if "inventory_monitoring_work_centers" in tables:
        with op.batch_alter_table("inventory_monitoring_work_centers") as batch:
            if _has_column("inventory_monitoring_work_centers", "merged_into_id"):
                batch.drop_column("merged_into_id")
            if _has_column("inventory_monitoring_work_centers", "sap_plant_codes"):
                batch.drop_column("sap_plant_codes")
