"""CSCSpecVersion model – version snapshots for CSC specifications."""

from datetime import datetime, timezone
from app.extensions import db
from app.core.services.csc_utils import format_spec_version


class CSCSpecVersion(db.Model):
    """Version snapshot of a CSC specification draft."""

    __tablename__ = "csc_spec_versions"
    __table_args__ = (
        db.UniqueConstraint(
            "draft_id", "spec_version", name="uq_csc_spec_versions_draft_version"
        ),
        db.Index("ix_csc_spec_versions_draft_version", "draft_id", "spec_version"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
    )
    spec_version = db.Column(db.Integer, nullable=False)
    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    created_by = db.Column(db.String(255), nullable=False, default="system")
    source_action = db.Column(db.String(255), nullable=False, default="")
    remarks = db.Column(db.Text, nullable=True)
    payload_json = db.Column(db.Text, nullable=False)

    # ── Relationships ──────────────────────────────────────────
    draft = db.relationship(
        "CSCDraft",
        back_populates="spec_versions",
        lazy="joined",
    )

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "draft_id": self.draft_id,
            "spec_version": self.spec_version,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "created_by": self.created_by,
            "source_action": self.source_action,
            "remarks": self.remarks,
            "payload_json": self.payload_json,
        }

    def __repr__(self):
        return f"<CSCSpecVersion {self.draft_id} v{format_spec_version(self.spec_version)}>"
