"""CSCParameter model – specification parameters within CSC drafts."""

from app.extensions import db


class CSCParameter(db.Model):
    """Parameter entry for a CSC specification draft."""

    __tablename__ = "csc_parameters"
    __table_args__ = (
        db.Index("ix_csc_parameters_draft_sort", "draft_id", "sort_order"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
    )
    parameter_name = db.Column(db.Text, nullable=False)
    parameter_type = db.Column(db.String(50), nullable=False, default="Essential")
    existing_value = db.Column(db.Text, nullable=True)
    proposed_value = db.Column(db.Text, nullable=True)
    test_method = db.Column(db.Text, nullable=True)
    test_procedure_type = db.Column(db.String(100), nullable=False, default="")
    test_procedure_text = db.Column(db.Text, nullable=True)
    justification = db.Column(db.Text, nullable=True)
    remarks = db.Column(db.Text, nullable=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)

    # ── Relationships ──────────────────────────────────────────
    draft = db.relationship(
        "CSCDraft",
        back_populates="parameters",
        lazy="joined",
    )

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "draft_id": self.draft_id,
            "parameter_name": self.parameter_name,
            "parameter_type": self.parameter_type,
            "existing_value": self.existing_value,
            "proposed_value": self.proposed_value,
            "test_method": self.test_method,
            "test_procedure_type": self.test_procedure_type,
            "test_procedure_text": self.test_procedure_text,
            "justification": self.justification,
            "remarks": self.remarks,
            "sort_order": self.sort_order,
        }

    def __repr__(self):
        return f"<CSCParameter {self.parameter_name}>"
