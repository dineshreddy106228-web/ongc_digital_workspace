"""CSCImpactAnalysis model – impact analysis for CSC specifications."""

from datetime import datetime, timezone
from app.extensions import db
from app.core.services.csc_utils import (
    deserialize_impact_checklist_state,
    summarize_impact_checklist_state,
)

# Valid impact grades
IMPACT_GRADES = ["LOW", "MEDIUM", "HIGH", "CRITICAL", "PENDING"]


class CSCImpactAnalysis(db.Model):
    """Impact analysis entry for a CSC specification draft."""

    __tablename__ = "csc_impact_analysis"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    draft_id = db.Column(
        db.BigInteger,
        db.ForeignKey("csc_drafts.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    operational_impact_score = db.Column(
        db.Integer, nullable=False, default=1
    )
    safety_environment_score = db.Column(
        db.Integer, nullable=False, default=1
    )
    supply_risk_score = db.Column(
        db.Integer, nullable=False, default=1
    )
    no_substitute_flag = db.Column(db.Boolean, nullable=False, default=False)
    impact_score_total = db.Column(db.Float, nullable=False, default=0.0)
    impact_grade = db.Column(
        db.String(20), nullable=False, default="LOW"
    )
    checklist_state_json = db.Column(db.Text, nullable=True)
    operational_note = db.Column(db.Text, nullable=True)
    safety_environment_note = db.Column(db.Text, nullable=True)
    supply_risk_note = db.Column(db.Text, nullable=True)
    reviewed_by = db.Column(db.String(255), nullable=True)
    review_date = db.Column(db.String(50), nullable=True)

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
    draft = db.relationship(
        "CSCDraft",
        back_populates="impact_analysis",
        lazy="joined",
    )

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        checklist_state = deserialize_impact_checklist_state(
            self.checklist_state_json,
            getattr(self.draft, "chemical_name", ""),
        )
        return {
            "id": self.id,
            "draft_id": self.draft_id,
            "operational_impact_score": self.operational_impact_score,
            "safety_environment_score": self.safety_environment_score,
            "supply_risk_score": self.supply_risk_score,
            "no_substitute_flag": self.no_substitute_flag,
            "impact_score_total": self.impact_score_total,
            "impact_grade": self.impact_grade,
            "checklist_state_json": self.checklist_state_json,
            "checklist_state": checklist_state,
            "checklist_summary": summarize_impact_checklist_state(checklist_state),
            "operational_note": self.operational_note,
            "safety_environment_note": self.safety_environment_note,
            "supply_risk_note": self.supply_risk_note,
            "reviewed_by": self.reviewed_by,
            "review_date": self.review_date,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<CSCImpactAnalysis {self.draft_id} - {self.impact_grade}>"
