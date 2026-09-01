"""Authoritative SAP quality-monitoring records for the QC control tower.

The weekly laboratory workbook remains a separate, historic source.  These
models preserve the native SAP exports and maintain a current operational view
without ever allowing a laboratory follow-up to replace an SAP field.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.dialects import mysql
from sqlalchemy.orm import deferred

from app.extensions import db


SAP_BINARY_TYPE = db.LargeBinary().with_variant(mysql.LONGBLOB(), "mysql")


class QCSAPSourceDocument(db.Model):
    """The uploaded pair of SAP workbooks, held once for the whole upload.

    A central upload produces one batch per laboratory, and every one of them
    was made from the same two files.  Storing the pair here rather than on
    each batch keeps one copy instead of six or seven.

    The files backing each laboratory's newest snapshot are kept.  A central
    upload may therefore share one document across several laboratories while
    a newer single-laboratory upload introduces a second active document.
    Documents no latest snapshot references have their bytes cleared and
    ``purged_at`` stamped; the reconciled records and batch metadata remain
    the permanent audit trail.
    """

    __tablename__ = "qc_sap_source_documents"
    __table_args__ = (
        db.Index("ix_qc_sap_source_documents_uploaded_at", "uploaded_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    inspection_filename = db.Column(db.String(255), nullable=False)
    inspection_content_type = db.Column(db.String(120), nullable=False)
    inspection_file_size = db.Column(db.BigInteger, nullable=False, default=0)
    inspection_source_data = deferred(db.Column(SAP_BINARY_TYPE, nullable=False))

    notification_filename = db.Column(db.String(255), nullable=False)
    notification_content_type = db.Column(db.String(120), nullable=False)
    notification_file_size = db.Column(db.BigInteger, nullable=False, default=0)
    notification_source_data = deferred(db.Column(SAP_BINARY_TYPE, nullable=False))

    purged_at = db.Column(db.DateTime, nullable=True)
    uploaded_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    uploaded_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    uploader = db.relationship("User", foreign_keys=[uploaded_by])
    batches = db.relationship("QCSAPUploadBatch", back_populates="source_document")


class QCSAPUploadBatch(db.Model):
    """One paired daily export of SAP inspection lots and notifications."""

    __tablename__ = "qc_sap_upload_batches"
    __table_args__ = (
        db.Index("ix_qc_sap_upload_batches_lab_date", "lab_code", "as_of_date"),
        db.Index("ix_qc_sap_upload_batches_uploaded_at", "uploaded_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    lab_code = db.Column(db.String(64), nullable=False, default="rgl_panvel")
    plant_code = db.Column(db.String(32), nullable=False, default="10R2")
    as_of_date = db.Column(db.Date, nullable=False)

    # The file names and sizes stay on the batch because they describe what
    # this laboratory's position was built from.  The bytes themselves live on
    # the shared document, which every batch of one upload points at.
    inspection_filename = db.Column(db.String(255), nullable=False)
    inspection_content_type = db.Column(db.String(120), nullable=False)
    inspection_file_size = db.Column(db.BigInteger, nullable=False, default=0)

    notification_filename = db.Column(db.String(255), nullable=False)
    notification_content_type = db.Column(db.String(120), nullable=False)
    notification_file_size = db.Column(db.BigInteger, nullable=False, default=0)

    source_document_id = db.Column(
        db.BigInteger, db.ForeignKey("qc_sap_source_documents.id", ondelete="SET NULL"), nullable=True,
    )

    inspection_lot_count = db.Column(db.Integer, nullable=False, default=0)
    notification_count = db.Column(db.Integer, nullable=False, default=0)
    record_count = db.Column(db.Integer, nullable=False, default=0)
    unmatched_inspection_count = db.Column(db.Integer, nullable=False, default=0)
    unmatched_notification_count = db.Column(db.Integer, nullable=False, default=0)
    summary_json = db.Column(db.Text, nullable=False, default="{}")
    uploaded_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    uploaded_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    uploader = db.relationship("User", foreign_keys=[uploaded_by])
    source_document = db.relationship("QCSAPSourceDocument", back_populates="batches")

    @property
    def source_is_available(self) -> bool:
        """Whether this batch's workbooks can still be downloaded."""
        document = self.source_document
        return document is not None and document.purged_at is None


class QCSAPRecord(db.Model):
    """Current canonical row made from the two SAP reports.

    ``official_status`` and every field prefixed ``sap_`` are only written by
    the SAP importer.  The lab update table records a separate, auditable view
    of action and expected completion.
    """

    __tablename__ = "qc_sap_records"
    __table_args__ = (
        db.UniqueConstraint("source_key", name="uq_qc_sap_records_source_key"),
        db.Index("ix_qc_sap_records_lab_batch", "lab_code", "last_seen_batch_id"),
        db.Index("ix_qc_sap_records_lot", "inspection_lot_number"),
        db.Index("ix_qc_sap_records_notification", "notification_no"),
        db.Index("ix_qc_sap_records_status", "official_status"),
        db.Index("ix_qc_sap_records_work_center", "work_center"),
        db.Index("ix_qc_sap_records_lab_year", "lab_code", "financial_year"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    source_key = db.Column(db.String(180), nullable=False)
    lab_code = db.Column(db.String(64), nullable=False, default="rgl_panvel")
    source_completeness = db.Column(db.String(32), nullable=False, default="matched")

    # The Indian financial year the record belongs to, as "2026-27".  A year's
    # base data is loaded once; a daily export then carries only current and
    # newly created notifications, so the year -- not the latest upload batch
    # -- is what the dashboards and the register read.
    financial_year = db.Column(db.String(9), nullable=True)

    inspection_lot_number = db.Column(db.String(100), nullable=True)
    notification_no = db.Column(db.String(100), nullable=True)
    plant_code = db.Column(db.String(32), nullable=False, default="10R2")
    material_code = db.Column(db.String(100), nullable=True)
    material_description = db.Column(db.String(500), nullable=True)
    po_number = db.Column(db.String(100), nullable=True)
    po_item = db.Column(db.String(40), nullable=True)
    work_center = db.Column(db.String(160), nullable=True)

    sap_system_status = db.Column(db.String(255), nullable=True)
    sap_lot_status = db.Column(db.String(255), nullable=True)
    sap_notification_status = db.Column(db.String(255), nullable=True)
    usage_decision_code = db.Column(db.String(80), nullable=True)
    official_status = db.Column(db.String(32), nullable=False, default="open")
    start_inspection_date = db.Column(db.Date, nullable=True)
    end_inspection_date = db.Column(db.Date, nullable=True)
    notification_start_date = db.Column(db.Date, nullable=True)
    planned_end_date = db.Column(db.Date, nullable=True)
    completion_date = db.Column(db.Date, nullable=True)
    sap_delay_days = db.Column(db.Integer, nullable=True)

    # Derived at import from the two columns above so the portfolio analytics
    # can group in SQL. The usage-decision code and the SAP dates remain the
    # authority; these are a materialised reading of them, never an input.
    usage_outcome = db.Column(db.String(12), nullable=True, index=True)
    turnaround_days = db.Column(db.Integer, nullable=True)

    first_seen_batch_id = db.Column(db.BigInteger, db.ForeignKey("qc_sap_upload_batches.id", ondelete="SET NULL"), nullable=True)
    last_seen_batch_id = db.Column(db.BigInteger, db.ForeignKey("qc_sap_upload_batches.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    first_seen_batch = db.relationship("QCSAPUploadBatch", foreign_keys=[first_seen_batch_id])
    last_seen_batch = db.relationship("QCSAPUploadBatch", foreign_keys=[last_seen_batch_id])
    lab_updates = db.relationship("QCSAPLabUpdate", back_populates="record", cascade="all, delete-orphan")
    monitoring_dispositions = db.relationship(
        "QCSAPMonitoringDisposition", back_populates="record", cascade="all, delete-orphan",
    )


class QCSAPLabUpdate(db.Model):
    """A laboratory's follow-up against an SAP record, retained as history."""

    __tablename__ = "qc_sap_lab_updates"
    __table_args__ = (
        db.Index("ix_qc_sap_lab_updates_record_created", "record_id", "created_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    record_id = db.Column(db.BigInteger, db.ForeignKey("qc_sap_records.id", ondelete="CASCADE"), nullable=False)
    activity_status = db.Column(db.String(48), nullable=False)
    sampling_date = db.Column(db.Date, nullable=True)
    actual_start_date = db.Column(db.Date, nullable=True)
    expected_completion_date = db.Column(db.Date, nullable=True)
    action_owner = db.Column(db.String(160), nullable=True)
    delay_reason = db.Column(db.Text, nullable=True)
    update_note = db.Column(db.Text, nullable=True)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    record = db.relationship("QCSAPRecord", back_populates="lab_updates")
    updater = db.relationship("User", foreign_keys=[updated_by])


class QCSAPMonitoringDisposition(db.Model):
    """Immutable QC-admin decisions that exclude or reinstate SAP records.

    The underlying SAP notification is never deleted.  An exclusion records
    the SAP status and work centre that were reviewed, so a later SAP change
    can be returned to Corporate Chemistry for a fresh decision.
    """

    __tablename__ = "qc_sap_monitoring_dispositions"
    __table_args__ = (
        db.Index("ix_qc_sap_monitoring_dispositions_record_created", "record_id", "created_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    record_id = db.Column(db.BigInteger, db.ForeignKey("qc_sap_records.id", ondelete="CASCADE"), nullable=False)
    decision = db.Column(db.String(24), nullable=False)  # exclude_non_actionable | reinstate
    reason_code = db.Column(db.String(64), nullable=True)
    note = db.Column(db.Text, nullable=True)
    official_status_at_decision = db.Column(db.String(32), nullable=False)
    work_center_at_decision = db.Column(db.String(160), nullable=True)
    recorded_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    record = db.relationship("QCSAPRecord", back_populates="monitoring_dispositions")
    recorder = db.relationship("User", foreign_keys=[recorded_by])


class QCNonSAPSample(db.Model):
    """Corporate Chemistry's controlled register for work absent from SAP.

    These rows are intentionally not SAP records.  They carry the laboratory's
    declared status and are displayed separately in every deck, so they
    can close operational gaps without diluting the authority of SAP QM.
    """

    __tablename__ = "qc_non_sap_samples"
    __table_args__ = (
        db.UniqueConstraint("lab_code", "sample_reference", name="uq_qc_non_sap_lab_reference"),
        db.Index("ix_qc_non_sap_samples_lab_status", "lab_code", "current_status"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    lab_code = db.Column(db.String(64), nullable=False)
    sample_reference = db.Column(db.String(120), nullable=False)
    chemical_name = db.Column(db.String(255), nullable=False)
    material_code = db.Column(db.String(100), nullable=True)
    sample_receipt_date = db.Column(db.Date, nullable=True)
    current_status = db.Column(db.String(48), nullable=False, default="awaiting_sample")
    expected_completion_date = db.Column(db.Date, nullable=True)
    action_owner = db.Column(db.String(160), nullable=True)
    delay_reason = db.Column(db.Text, nullable=True)
    update_note = db.Column(db.Text, nullable=True)
    reported_outcome = db.Column(db.String(24), nullable=True)
    created_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    creator = db.relationship("User", foreign_keys=[created_by])
    updater = db.relationship("User", foreign_keys=[updated_by])
    updates = db.relationship("QCNonSAPSampleUpdate", back_populates="sample", cascade="all, delete-orphan")


class QCNonSAPSampleUpdate(db.Model):
    """Audit history for each Corporate-entered non-SAP status update."""

    __tablename__ = "qc_non_sap_sample_updates"
    __table_args__ = (
        db.Index("ix_qc_non_sap_sample_updates_sample_created", "sample_id", "created_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    sample_id = db.Column(db.BigInteger, db.ForeignKey("qc_non_sap_samples.id", ondelete="CASCADE"), nullable=False)
    current_status = db.Column(db.String(48), nullable=False)
    expected_completion_date = db.Column(db.Date, nullable=True)
    action_owner = db.Column(db.String(160), nullable=True)
    delay_reason = db.Column(db.Text, nullable=True)
    update_note = db.Column(db.Text, nullable=True)
    reported_outcome = db.Column(db.String(24), nullable=True)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    sample = db.relationship("QCNonSAPSample", back_populates="updates")
    updater = db.relationship("User", foreign_keys=[updated_by])
