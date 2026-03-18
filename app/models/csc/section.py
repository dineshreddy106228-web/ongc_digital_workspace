"""CSCSection model – specification sections within CSC drafts."""

from app.extensions import db


class CSCSection(db.Model):
    """Section entry for a CSC specification draft."""

    __tablename__ = "csc_sections"
    __table_args__ = (
        db.UniqueConstraint("draft_id", "section_name", name="uq_csc_sections_draft_name"),
        db.Index("ix_csc_sections_draft_sort", "draft_id", "sort_order"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
    )
    section_name = db.Column(db.String(255), nullable=False)
    section_text = db.Column(db.Text, nullable=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)

    # ── Relationships ──────────────────────────────────────────
    draft = db.relationship(
        "CSCDraft",
        back_populates="sections",
        lazy="joined",
    )

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "draft_id": self.draft_id,
            "section_name": self.section_name,
            "section_text": self.section_text,
            "sort_order": self.sort_order,
        }

    def __repr__(self):
        return f"<CSCSection {self.section_name}>"
