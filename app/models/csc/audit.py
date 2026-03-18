"""CSCAudit model – audit trail for CSC specification changes."""

from datetime import datetime, timezone
from app.extensions import db


class CSCAudit(db.Model):
    """Audit trail entry for CSC specification draft modifications."""

    __tablename__ = "csc_audit"
    __table_args__ = (
        db.Index("ix_csc_audit_draft_id", "draft_id"),
        db.Index("ix_csc_audit_action_time", "action_time"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="SET NULL"),
        nullable=True,
    )
    action = db.Column(db.String(255), nullable=False)
    user_name = db.Column(db.String(255), nullable=False)
    action_time = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    old_value = db.Column(db.Text, nullable=True)
    new_value = db.Column(db.Text, nullable=True)
    remarks = db.Column(db.Text, nullable=True)

    # ── Relationships ──────────────────────────────────────────
    draft = db.relationship(
        "CSCDraft",
        back_populates="audit_entries",
        lazy="joined",
    )

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "draft_id": self.draft_id,
            "action": self.action,
            "user_name": self.user_name,
            "action_time": self.action_time.isoformat() if self.action_time else None,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "remarks": self.remarks,
        }

    def __repr__(self):
        return f"<CSCAudit {self.id} - {self.action}>"
