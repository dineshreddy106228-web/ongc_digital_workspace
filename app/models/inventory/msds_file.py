"""BLOB-backed MSDS documents for inventory materials."""

from datetime import datetime, timezone

from sqlalchemy.orm import deferred

from app.extensions import db


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
    filename = db.Column(db.String(255), nullable=False)
    content_type = db.Column(db.String(100), nullable=False, default="application/pdf")
    file_size = db.Column(db.BigInteger, nullable=False, default=0)
    uploaded_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    data = deferred(db.Column(db.LargeBinary, nullable=False))

    material = db.relationship("MaterialMaster", lazy="select")

    def __repr__(self) -> str:
        return f"<MSDSFile id={self.id!r} material={self.material_code!r}>"
