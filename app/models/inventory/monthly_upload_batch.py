"""DB-backed monthly inventory report batches."""

from datetime import datetime, timezone

from sqlalchemy.dialects import mysql
from sqlalchemy.orm import deferred

from app.extensions import db


MONTHLY_BINARY_TYPE = db.LargeBinary().with_variant(mysql.LONGBLOB(), "mysql")


class InventoryMonthlyUploadBatch(db.Model):
    __tablename__ = "inventory_monthly_upload_batches"
    __table_args__ = (
        db.Index("ix_inv_monthly_batches_period", "report_year", "report_month"),
        db.Index("ix_inv_monthly_batches_uploaded_at", "uploaded_at"),
        db.UniqueConstraint("report_year", "report_month", name="uq_inv_monthly_batches_period"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    report_year = db.Column(db.Integer, nullable=False)
    report_month = db.Column(db.Integer, nullable=False)
    report_label = db.Column(db.String(32), nullable=False)
    status = db.Column(db.String(20), nullable=False, default="success")
    error_message = db.Column(db.Text, nullable=True)

    mb51_filename = db.Column(db.String(255), nullable=False)
    me2m_filename = db.Column(db.String(255), nullable=False)
    mc9_filename = db.Column(db.String(255), nullable=False)
    mb51_row_count = db.Column(db.Integer, nullable=False, default=0)
    me2m_row_count = db.Column(db.Integer, nullable=False, default=0)
    mc9_row_count = db.Column(db.Integer, nullable=False, default=0)

    summary_json = db.Column(db.Text, nullable=False, default="{}")

    excel_filename = db.Column(db.String(255), nullable=False)
    excel_content_type = db.Column(
        db.String(120),
        nullable=False,
        default="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    excel_file_size = db.Column(db.BigInteger, nullable=False, default=0)
    excel_data = deferred(db.Column(MONTHLY_BINARY_TYPE, nullable=False))

    pdf_filename = db.Column(db.String(255), nullable=False)
    pdf_content_type = db.Column(
        db.String(120),
        nullable=False,
        default="application/pdf",
    )
    pdf_file_size = db.Column(db.BigInteger, nullable=False, default=0)
    pdf_data = deferred(db.Column(MONTHLY_BINARY_TYPE, nullable=False))

    uploaded_by = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    uploaded_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    uploader = db.relationship(
        "User",
        foreign_keys=[uploaded_by],
        backref=db.backref("inventory_monthly_upload_batches", lazy="dynamic"),
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<InventoryMonthlyUploadBatch id={self.id!r} "
            f"period={self.report_year:04d}-{self.report_month:02d}>"
        )
