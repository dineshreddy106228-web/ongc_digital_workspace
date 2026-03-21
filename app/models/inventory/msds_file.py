"""BLOB-backed MSDS documents for inventory materials."""

from datetime import datetime, timezone

from sqlalchemy.dialects import mysql
from sqlalchemy.orm import deferred

from app.extensions import db


MSDS_BINARY_TYPE = db.LargeBinary().with_variant(mysql.LONGBLOB(), "mysql")

MSDS_SLOT_STANDARD = "standard"
MSDS_SLOT_VENDOR_1 = "vendor_1"
MSDS_SLOT_VENDOR_2 = "vendor_2"
MSDS_SLOT_VENDOR_3 = "vendor_3"

MSDS_SLOT_LABELS = {
    MSDS_SLOT_STANDARD: "ONGC Standard MSDS",
    MSDS_SLOT_VENDOR_1: "Vendor Submitted MSDS 1",
    MSDS_SLOT_VENDOR_2: "Vendor Submitted MSDS 2",
    MSDS_SLOT_VENDOR_3: "Vendor Submitted MSDS 3",
}

MSDS_SLOT_ORDER = [
    MSDS_SLOT_STANDARD,
    MSDS_SLOT_VENDOR_1,
    MSDS_SLOT_VENDOR_2,
    MSDS_SLOT_VENDOR_3,
]


class MSDSFile(db.Model):
    __tablename__ = "msds_files"
    __table_args__ = (
        db.Index("ix_msds_files_material_code", "material_code"),
        db.Index("ix_msds_files_uploaded_at", "uploaded_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    material_code = db.Column(
        db.String(50),
        db.ForeignKey("material_master.material", ondelete="CASCADE"),
        nullable=False,
    )
    slot_code = db.Column(db.String(32), nullable=False, default=MSDS_SLOT_STANDARD)
    filename = db.Column(db.String(255), nullable=False)
    content_type = db.Column(db.String(100), nullable=False, default="application/pdf")
    file_size = db.Column(db.BigInteger, nullable=False, default=0)
    uploaded_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    data = deferred(db.Column(MSDS_BINARY_TYPE, nullable=False))

    material = db.relationship("MaterialMaster", lazy="select")

    @property
    def slot_label(self) -> str:
        return MSDS_SLOT_LABELS.get(self.slot_code, f"Legacy Slot ({self.slot_code})")

    def __repr__(self) -> str:
        return f"<MSDSFile id={self.id!r} material={self.material_code!r} slot={self.slot_code!r}>"
