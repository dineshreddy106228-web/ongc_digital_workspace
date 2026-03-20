"""CSCRevision model – links parent draft to revision child drafts."""

from datetime import datetime, timezone
from app.extensions import db


class CSCRevision(db.Model):
    """Links a parent CSC draft to its revision child draft."""

    __tablename__ = "csc_revisions"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    parent_draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
    )
    child_draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    submitted_at = db.Column(db.DateTime, nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(50), nullable=False, default="open")
    reviewer_notes = db.Column(db.Text, nullable=True)
    authorization_confirmed = db.Column(db.Boolean, nullable=True)
    authorized_by_name = db.Column(db.String(255), nullable=True)
    subcommittee_head_name = db.Column(db.String(255), nullable=True)
    committee_head_reviewed_at = db.Column(db.DateTime, nullable=True)
    committee_head_notes = db.Column(db.Text, nullable=True)
    committee_head_user_id = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id"),
        nullable=True,
    )
    module_admin_reviewed_at = db.Column(db.DateTime, nullable=True)
    module_admin_notes = db.Column(db.Text, nullable=True)
    module_admin_user_id = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id"),
        nullable=True,
    )

    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Relationships ──────────────────────────────────────────
    parent_draft = db.relationship(
        "CSCDraft",
        foreign_keys=[parent_draft_id],
        back_populates="revisions_as_parent",
        lazy="joined",
    )
    child_draft = db.relationship(
        "CSCDraft",
        foreign_keys=[child_draft_id],
        lazy="joined",
    )
    committee_head_user = db.relationship(
        "User",
        foreign_keys=[committee_head_user_id],
        lazy="joined",
    )
    module_admin_user = db.relationship(
        "User",
        foreign_keys=[module_admin_user_id],
        lazy="joined",
    )

    # ── Convenience properties (used by templates) ─────────────
    @property
    def parent_spec(self):
        """Alias for parent_draft (templates use revision.parent_spec)."""
        return self.parent_draft

    @property
    def submitted_by(self):
        """Return submitter's username from the child draft creator."""
        if self.child_draft and self.child_draft.creator:
            return self.child_draft.creator.username
        return None

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "parent_draft_id": self.parent_draft_id,
            "child_draft_id": self.child_draft_id,
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "reviewed_at": self.reviewed_at.isoformat() if self.reviewed_at else None,
            "status": self.status,
            "reviewer_notes": self.reviewer_notes,
            "authorization_confirmed": self.authorization_confirmed,
            "authorized_by_name": self.authorized_by_name,
            "subcommittee_head_name": self.subcommittee_head_name,
            "committee_head_reviewed_at": self.committee_head_reviewed_at.isoformat() if self.committee_head_reviewed_at else None,
            "committee_head_notes": self.committee_head_notes,
            "committee_head_user_id": self.committee_head_user_id,
            "module_admin_reviewed_at": self.module_admin_reviewed_at.isoformat() if self.module_admin_reviewed_at else None,
            "module_admin_notes": self.module_admin_notes,
            "module_admin_user_id": self.module_admin_user_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<CSCRevision {self.parent_draft_id} -> {self.child_draft_id}>"
