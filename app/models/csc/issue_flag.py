"""CSCIssueFlag model – issue flags for CSC specifications."""

from app.extensions import db

# Valid issue types for CSC specifications
ISSUE_TYPES = ["operational", "quality", "supply", "testing"]


class CSCIssueFlag(db.Model):
    """Issue flag entry for a CSC specification draft."""

    __tablename__ = "csc_issue_flags"
    __table_args__ = (
        db.Index("ix_csc_issue_flags_draft_sort", "draft_id", "sort_order"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
    )
    issue_type = db.Column(db.String(100), nullable=False)
    is_present = db.Column(db.Boolean, nullable=False, default=False)
    note = db.Column(db.Text, nullable=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)

    # ── Relationships ──────────────────────────────────────────
    draft = db.relationship(
        "CSCDraft",
        back_populates="issue_flags",
        lazy="joined",
    )

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "draft_id": self.draft_id,
            "issue_type": self.issue_type,
            "is_present": self.is_present,
            "note": self.note,
            "sort_order": self.sort_order,
        }

    def __repr__(self):
        return f"<CSCIssueFlag {self.issue_type}>"
