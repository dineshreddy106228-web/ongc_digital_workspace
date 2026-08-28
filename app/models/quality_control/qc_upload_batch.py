"""Weekly QC workbook import batches."""

from datetime import datetime, timezone

from sqlalchemy.dialects import mysql
from sqlalchemy.orm import deferred

from app.extensions import db


QC_BINARY_TYPE = db.LargeBinary().with_variant(mysql.LONGBLOB(), "mysql")


class QCUploadBatch(db.Model):
    __tablename__ = "qc_upload_batches"
    __table_args__ = (
        db.Index("ix_qc_upload_batches_period", "lab_code", "week_start", "week_end"),
        db.Index("ix_qc_upload_batches_uploaded_at", "uploaded_at"),
        db.UniqueConstraint("lab_code", "week_start", "week_end", name="uq_qc_upload_batches_lab_period"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    lab_code = db.Column(db.String(64), nullable=False, default="rgl_panvel")
    lab_name = db.Column(db.String(160), nullable=False, default="RGL Panvel")
    report_label = db.Column(db.String(160), nullable=False)
    week_start = db.Column(db.Date, nullable=False)
    week_end = db.Column(db.Date, nullable=False)
    source_filename = db.Column(db.String(255), nullable=False)
    source_content_type = db.Column(db.String(120), nullable=False)
    source_file_size = db.Column(db.BigInteger, nullable=False, default=0)
    source_data = deferred(db.Column(QC_BINARY_TYPE, nullable=False))
    # The imported samples and workbook metadata are permanent; the binary
    # source is only available for the controlled rollback period.
    source_purged_at = db.Column(db.DateTime, nullable=True)
    row_count = db.Column(db.Integer, nullable=False, default=0)
    imported_count = db.Column(db.Integer, nullable=False, default=0)
    updated_count = db.Column(db.Integer, nullable=False, default=0)
    summary_json = db.Column(db.Text, nullable=False, default="{}")
    uploaded_by = db.Column(db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    uploaded_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    uploader = db.relationship("User", foreign_keys=[uploaded_by])
