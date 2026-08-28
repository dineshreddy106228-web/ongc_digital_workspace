"""Persistent entities for the Inventory Monitoring module.

The models intentionally use a new ``inventory_monitoring_*`` namespace so
legacy Inventory Intelligence data remains recoverable until an administrator
has taken a backup and explicitly retires it.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.dialects import mysql
from sqlalchemy.orm import deferred

from app.extensions import db


INVENTORY_BINARY = db.LargeBinary().with_variant(mysql.LONGBLOB(), "mysql")


class InventoryMonitoringUploadBatch(db.Model):
    __tablename__ = "inventory_monitoring_upload_batches"
    __table_args__ = (
        db.Index("ix_inventory_monitoring_batches_period", "source_group", "reporting_date"),
        db.Index("ix_inventory_monitoring_batches_status", "status"),
        db.Index("ix_inventory_monitoring_batches_uploaded_at", "uploaded_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    source_group = db.Column(db.String(16), nullable=False)  # 09 | 10 | mapping
    reporting_date = db.Column(db.Date, nullable=True)
    source_filename = db.Column(db.String(255), nullable=False)
    source_content_type = db.Column(db.String(120), nullable=False, default="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    source_checksum = db.Column(db.String(64), nullable=False, index=True)
    source_file_size = db.Column(db.BigInteger, nullable=False, default=0)
    source_data = deferred(db.Column(INVENTORY_BINARY, nullable=False))
    # Parsed snapshots and audit metadata remain indefinitely.  Only the
    # recoverable workbook payload is cleared after the rollback window.
    source_purged_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(24), nullable=False, default="imported")
    is_superseded = db.Column(db.Boolean, nullable=False, default=False)
    superseded_by_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_upload_batches.id", ondelete="SET NULL"), nullable=True)
    row_count = db.Column(db.Integer, nullable=False, default=0)
    accepted_count = db.Column(db.Integer, nullable=False, default=0)
    rejected_count = db.Column(db.Integer, nullable=False, default=0)
    duplicate_count = db.Column(db.Integer, nullable=False, default=0)
    warnings_json = db.Column(db.Text, nullable=False, default="[]")
    validation_json = db.Column(db.Text, nullable=False, default="{}")
    uploaded_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    uploaded_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    uploader = db.relationship("User", foreign_keys=[uploaded_by])
    superseded_by = db.relationship("InventoryMonitoringUploadBatch", remote_side=[id])


class InventoryMonitoringSnapshot(db.Model):
    __tablename__ = "inventory_monitoring_snapshots"
    __table_args__ = (
        db.UniqueConstraint("reporting_date", "material_group", "batch_id", name="uq_inventory_monitoring_snapshot_batch"),
        db.Index("ix_inventory_monitoring_snapshots_date_group", "reporting_date", "material_group"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    reporting_date = db.Column(db.Date, nullable=False)
    material_group = db.Column(db.String(2), nullable=False)
    batch_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_upload_batches.id", ondelete="CASCADE"), nullable=False)
    is_published = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    batch = db.relationship("InventoryMonitoringUploadBatch", foreign_keys=[batch_id])


class InventoryMonitoringWorkCenter(db.Model):
    """One asset, whatever SAP still calls it.

    SAP keeps a plant code per legacy asset, so a merger of two assets leaves two
    codes reporting into one place. ``sap_plant_codes`` holds them all on the
    surviving asset — comma separated, e.g. ``12A1,13A1`` — and ``merged_into_id``
    points a retired asset row at its successor so imported stock lands on one
    asset instead of two.
    """

    __tablename__ = "inventory_monitoring_work_centers"
    __table_args__ = (db.UniqueConstraint("normalized_name", name="uq_inventory_monitoring_work_center_name"),)

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)
    normalized_name = db.Column(db.String(255), nullable=False)
    zone = db.Column(db.String(120), nullable=True)
    work_center_type = db.Column(db.String(80), nullable=True)
    sap_plant_codes = db.Column(db.String(255), nullable=True)
    merged_into_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_work_centers.id", ondelete="SET NULL"), nullable=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    merged_into = db.relationship("InventoryMonitoringWorkCenter", remote_side=[id])

    @property
    def plant_codes(self) -> list[str]:
        """The SAP plant codes reporting into this asset, in the order they were entered."""
        return [part.strip().upper() for part in (self.sap_plant_codes or "").split(",") if part.strip()]


class InventoryMonitoringMaterial(db.Model):
    """One material code, with the unit the workbook states against it.

    The detailed inventory sheet carries no unit column; the material summary
    sheets — "09 Oil well cement - Chemical S" and "10 Chemi incl mud chemi -
    Chemi" — do. That unit is read against the material code at import and kept
    here, so every table can state it beside the code and so a material can be
    read as a liquid (L, KL, GAL) or a solid (KG, MT).
    """

    __tablename__ = "inventory_monitoring_materials"
    __table_args__ = (db.UniqueConstraint("material_code", name="uq_inventory_monitoring_material_code"),)

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    material_code = db.Column(db.String(64), nullable=False)
    description = db.Column(db.String(500), nullable=True)
    material_group = db.Column(db.String(2), nullable=True)
    uom = db.Column(db.String(32), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class InventoryMonitoringWorkCenterMaterial(db.Model):
    __tablename__ = "inventory_monitoring_work_center_materials"
    __table_args__ = (
        db.UniqueConstraint("work_center_id", "material_id", "mapping_batch_id", name="uq_inventory_monitoring_mapping_version"),
        db.Index("ix_inventory_monitoring_mapping_material", "material_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    work_center_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_work_centers.id", ondelete="CASCADE"), nullable=False)
    material_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_materials.id", ondelete="CASCADE"), nullable=False)
    mapping_batch_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_upload_batches.id", ondelete="CASCADE"), nullable=False)
    is_current = db.Column(db.Boolean, nullable=False, default=True)

    work_center = db.relationship("InventoryMonitoringWorkCenter")
    material = db.relationship("InventoryMonitoringMaterial")
    mapping_batch = db.relationship("InventoryMonitoringUploadBatch")


class InventoryMonitoringRecord(db.Model):
    __tablename__ = "inventory_monitoring_records"
    __table_args__ = (
        db.Index("ix_inventory_monitoring_records_snapshot", "snapshot_id"),
        db.Index("ix_inventory_monitoring_records_work_center", "work_center_id"),
        db.Index("ix_inventory_monitoring_records_material", "material_id"),
        db.Index("ix_inventory_monitoring_records_group", "material_group"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    snapshot_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_snapshots.id", ondelete="CASCADE"), nullable=False)
    batch_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_upload_batches.id", ondelete="CASCADE"), nullable=False)
    material_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_materials.id", ondelete="SET NULL"), nullable=True)
    work_center_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_work_centers.id", ondelete="SET NULL"), nullable=True)
    material_group = db.Column(db.String(2), nullable=False)
    material_code = db.Column(db.String(64), nullable=False)
    material_description = db.Column(db.String(500), nullable=True)
    work_center_name = db.Column(db.String(255), nullable=False)
    stock_qty = db.Column(db.Numeric(20, 3), nullable=True)
    uom = db.Column(db.String(32), nullable=True)
    inventory_value_inr = db.Column(db.Numeric(20, 2), nullable=True)
    open_po = db.Column(db.Numeric(20, 3), nullable=True)
    open_pr = db.Column(db.Numeric(20, 3), nullable=True)
    stock_months = db.Column(db.Numeric(12, 2), nullable=True)
    source_sheet = db.Column(db.String(255), nullable=False)
    source_row = db.Column(db.Integer, nullable=False)

    snapshot = db.relationship("InventoryMonitoringSnapshot")
    batch = db.relationship("InventoryMonitoringUploadBatch")
    material = db.relationship("InventoryMonitoringMaterial")
    work_center = db.relationship("InventoryMonitoringWorkCenter")


class InventoryMonitoringException(db.Model):
    __tablename__ = "inventory_monitoring_exceptions"
    __table_args__ = (
        db.Index("ix_inventory_monitoring_exceptions_snapshot", "snapshot_id"),
        db.Index("ix_inventory_monitoring_exceptions_type", "exception_type"),
        db.Index("ix_inventory_monitoring_exceptions_work_center", "work_center_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    snapshot_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_snapshots.id", ondelete="CASCADE"), nullable=False)
    record_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_records.id", ondelete="CASCADE"), nullable=True)
    work_center_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_work_centers.id", ondelete="SET NULL"), nullable=True)
    material_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_materials.id", ondelete="SET NULL"), nullable=True)
    exception_type = db.Column(db.String(80), nullable=False)
    severity = db.Column(db.String(20), nullable=False, default="medium")
    details = db.Column(db.Text, nullable=True)
    inventory_value_inr = db.Column(db.Numeric(20, 2), nullable=True)
    stock_months = db.Column(db.Numeric(12, 2), nullable=True)
    review_status = db.Column(db.String(24), nullable=False, default="not_required")
    reviewed_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    review_note = db.Column(db.String(500), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    snapshot = db.relationship("InventoryMonitoringSnapshot")
    record = db.relationship("InventoryMonitoringRecord")
    work_center = db.relationship("InventoryMonitoringWorkCenter")
    material = db.relationship("InventoryMonitoringMaterial")
    reviewer = db.relationship("User", foreign_keys=[reviewed_by])


class InventoryMonitoringThreshold(db.Model):
    __tablename__ = "inventory_monitoring_thresholds"
    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.Numeric(12, 2), nullable=False)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)


class InventoryMonitoringMaterialSummary(db.Model):
    """One material's all-ONGC line from a workbook's material summary sheets.

    The detailed inventory sheet reports stock per work centre and carries no
    consumption. The two material summary sheets in the same workbook do: one
    gives quantity consumed over twelve months with its unit, the other the
    value of that consumption. They are stored here so the review can rank
    materials by what is actually used, not only by what is held.
    """

    __tablename__ = "inventory_monitoring_material_summaries"
    __table_args__ = (
        db.UniqueConstraint("snapshot_id", "material_code", name="uq_inventory_monitoring_summary_material"),
        db.Index("ix_inventory_monitoring_summaries_snapshot", "snapshot_id"),
        db.Index("ix_inventory_monitoring_summaries_material", "material_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    snapshot_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_snapshots.id", ondelete="CASCADE"), nullable=False)
    batch_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_upload_batches.id", ondelete="CASCADE"), nullable=False)
    material_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_materials.id", ondelete="SET NULL"), nullable=True)
    material_group = db.Column(db.String(2), nullable=False)
    material_code = db.Column(db.String(64), nullable=False)
    material_description = db.Column(db.String(500), nullable=True)
    stock_qty = db.Column(db.Numeric(20, 3), nullable=True)
    uom = db.Column(db.String(32), nullable=True)
    consumption_qty_12m = db.Column(db.Numeric(20, 3), nullable=True)
    consumption_value_inr = db.Column(db.Numeric(20, 2), nullable=True)
    inventory_value_inr = db.Column(db.Numeric(20, 2), nullable=True)
    stock_months = db.Column(db.Numeric(12, 2), nullable=True)

    snapshot = db.relationship("InventoryMonitoringSnapshot")
    batch = db.relationship("InventoryMonitoringUploadBatch")
    material = db.relationship("InventoryMonitoringMaterial")


class InventoryMonitoringPlantAlert(db.Model):
    """A plant code or work centre an import reported that no asset claims yet.

    Work centres are not expected to change, so an unrecognised one is news: it
    is raised here for the module admin to attach to an existing asset or to
    open as a new asset, and the stock it carries is never silently dropped.
    """

    __tablename__ = "inventory_monitoring_plant_alerts"
    __table_args__ = (
        db.Index("ix_inventory_monitoring_plant_alerts_status", "status"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    plant_code = db.Column(db.String(32), nullable=True)
    work_center_name = db.Column(db.String(255), nullable=False)
    batch_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_upload_batches.id", ondelete="SET NULL"), nullable=True)
    work_center_id = db.Column(db.BigInteger, db.ForeignKey("inventory_monitoring_work_centers.id", ondelete="SET NULL"), nullable=True)
    line_count = db.Column(db.Integer, nullable=False, default=0)
    inventory_value_inr = db.Column(db.Numeric(20, 2), nullable=True)
    status = db.Column(db.String(24), nullable=False, default="open")  # open | resolved
    resolution = db.Column(db.String(500), nullable=True)
    resolved_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    resolved_at = db.Column(db.DateTime, nullable=True)
    detected_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    batch = db.relationship("InventoryMonitoringUploadBatch")
    work_center = db.relationship("InventoryMonitoringWorkCenter")
    resolver = db.relationship("User", foreign_keys=[resolved_by])
