"""MSDS document metadata for inventory materials."""

from datetime import datetime, timezone

from app.extensions import db


class MaterialMSDSDocument(db.Model):
    __tablename__ = "inventory_msds_documents"
    __table_args__ = (
        db.UniqueConstraint("material_code", name="uq_inventory_msds_documents_material_code"),
        db.Index("ix_inventory_msds_documents_uploaded_at", "uploaded_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    material_code = db.Column(
        db.String(50),
        db.ForeignKey("material_master.material", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    original_filename = db.Column(db.String(255), nullable=False)
    stored_filename = db.Column(db.String(255), nullable=False)
    storage_path = db.Column(db.String(512), nullable=False)
    mime_type = db.Column(db.String(100), nullable=False, default="application/pdf")
    file_size_bytes = db.Column(db.BigInteger, nullable=False, default=0)
    sha256_hex = db.Column(db.String(64), nullable=False, default="")
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

    material = db.relationship("MaterialMaster", lazy="joined")
    uploader = db.relationship("User", foreign_keys=[uploaded_by], lazy="joined")

    def __repr__(self) -> str:
        return f"<MaterialMSDSDocument material={self.material_code!r}>"
