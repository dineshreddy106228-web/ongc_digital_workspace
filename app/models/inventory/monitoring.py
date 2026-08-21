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
    __tablename__ = "inventory_monitoring_work_centers"
    __table_args__ = (db.UniqueConstraint("normalized_name", name="uq_inventory_monitoring_work_center_name"),)

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)
    normalized_name = db.Column(db.String(255), nullable=False)
    zone = db.Column(db.String(120), nullable=True)
    work_center_type = db.Column(db.String(80), nullable=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)


class InventoryMonitoringMaterial(db.Model):
    __tablename__ = "inventory_monitoring_materials"
    __table_args__ = (db.UniqueConstraint("material_code", name="uq_inventory_monitoring_material_code"),)

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    material_code = db.Column(db.String(64), nullable=False)
    description = db.Column(db.String(500), nullable=True)
    material_group = db.Column(db.String(2), nullable=True)
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
