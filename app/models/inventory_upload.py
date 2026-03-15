"""Inventory Upload model – tracks each SAP export file imported into the system."""

from datetime import datetime, timezone
from app.extensions import db


class InventoryUpload(db.Model):
    """Represents a single SAP usage-export file uploaded by a user."""

    __tablename__ = "inventory_uploads"
    __table_args__ = (
        db.Index("ix_inv_uploads_uploaded_by", "uploaded_by"),
        db.Index("ix_inv_uploads_created_at", "created_at"),
        db.Index("ix_inv_uploads_status", "status"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    original_filename = db.Column(db.String(255), nullable=False)
    row_count = db.Column(db.Integer, nullable=False, default=0)
    # success | error
    status = db.Column(db.String(20), nullable=False, default="success")
    error_message = db.Column(db.Text, nullable=True)
    uploaded_by = db.Column(
        db.BigInteger, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Relationships ─────────────────────────────────────────────
    uploader = db.relationship(
        "User",
        foreign_keys=[uploaded_by],
        backref=db.backref("inventory_uploads", lazy="dynamic"),
    )
    records = db.relationship(
        "InventoryRecord",
        backref="upload",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<InventoryUpload id={self.id} "
            f"file={self.original_filename!r} rows={self.row_count}>"
        )
