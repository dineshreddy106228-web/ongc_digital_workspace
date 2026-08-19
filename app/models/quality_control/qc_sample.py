"""Current canonical view of every quality-control laboratory sample."""

from datetime import date, datetime, timezone

from typing import Optional

from app.extensions import db


class QCSample(db.Model):
    __tablename__ = "qc_samples"
    __table_args__ = (
        db.Index("ix_qc_samples_status", "result_status"),
        db.Index("ix_qc_samples_chemical", "chemical_name"),
        db.Index("ix_qc_samples_receipt_date", "sample_receipt_date"),
        db.Index("ix_qc_samples_last_seen", "last_seen_batch_id"),
        db.Index("ix_qc_samples_lab", "lab_code"),
        db.UniqueConstraint("source_key", name="uq_qc_samples_source_key"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    source_key = db.Column(db.String(64), nullable=False)
    lab_code = db.Column(db.String(64), nullable=False, default="rgl_panvel")
    serial_number = db.Column(db.Integer, nullable=True)
    chemical_name = db.Column(db.String(255), nullable=False)
    specification_no = db.Column(db.String(255), nullable=True)
    supply_type = db.Column(db.String(50), nullable=True)
    po_number = db.Column(db.String(100), nullable=True)
    lot_stack = db.Column(db.String(100), nullable=True)
    notification_no = db.Column(db.String(100), nullable=True)
    result_status = db.Column(db.String(30), nullable=False, default="under_testing")
    sample_receipt_date = db.Column(db.Date, nullable=True)
    report_issue_date = db.Column(db.Date, nullable=True)
    turnaround_days = db.Column(db.Integer, nullable=True)
    delay_reason = db.Column(db.Text, nullable=True)
    first_seen_batch_id = db.Column(db.BigInteger, db.ForeignKey("qc_upload_batches.id", ondelete="SET NULL"), nullable=True)
    last_seen_batch_id = db.Column(db.BigInteger, db.ForeignKey("qc_upload_batches.id", ondelete="SET NULL"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    first_seen_batch = db.relationship("QCUploadBatch", foreign_keys=[first_seen_batch_id])
    last_seen_batch = db.relationship("QCUploadBatch", foreign_keys=[last_seen_batch_id])

    @property
    def days_open(self) -> Optional[int]:
        if self.result_status != "under_testing" or self.sample_receipt_date is None:
            return None
        return max((date.today() - self.sample_receipt_date).days, 0)
