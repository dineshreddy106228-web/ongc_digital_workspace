"""Retire inventory pre-mapping: map what the workbooks report, not what was declared.

Work-centre / material mapping used to be declared ahead of the inventory in a mapping
workbook, and every stock line held outside that declaration was raised as a technical
exception for a super-user to clear. Monitoring now maps each stock line to the work
centre reporting it, so the declared pairs are retired, the mapping is restated from the
imported records, and the mapping exceptions they produced are removed.

Revision ID: a2b3c4d5e6f8
Revises: f8a9b0c1d2e
"""
from alembic import op

revision = "a2b3c4d5e6f8"
down_revision = "f8a9b0c1d2e"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "UPDATE inventory_monitoring_work_center_materials SET is_current = 0 "
        "WHERE mapping_batch_id IN ("
        "  SELECT id FROM inventory_monitoring_upload_batches WHERE source_group = 'mapping'"
        ")"
    )
    op.execute(
        "INSERT INTO inventory_monitoring_work_center_materials "
        "(work_center_id, material_id, mapping_batch_id, is_current) "
        "SELECT r.work_center_id, r.material_id, MIN(r.batch_id), 1 "
        "FROM inventory_monitoring_records r "
        "WHERE r.work_center_id IS NOT NULL AND r.material_id IS NOT NULL "
        "  AND COALESCE(r.inventory_value_inr, 0) > 0 "
        "  AND NOT EXISTS ("
        "    SELECT 1 FROM inventory_monitoring_work_center_materials m "
        "    WHERE m.work_center_id = r.work_center_id AND m.material_id = r.material_id AND m.is_current = 1"
        "  ) "
        "GROUP BY r.work_center_id, r.material_id"
    )
    op.execute(
        "DELETE FROM inventory_monitoring_exceptions "
        "WHERE exception_type IN ('held_not_mapped', 'mapped_not_held', 'unknown_mapping')"
    )


def downgrade():
    pass
