"""CSCDraft model – core specification draft entity for the CSC workflow."""

from datetime import datetime, timezone
from app.extensions import db
from app.core.services.csc_utils import (
    SPEC_SUBSET_LABELS,
    format_spec_version,
    parse_spec_number,
)


class CSCDraft(db.Model):
    """Chemical Specification Committee draft specification."""

    __tablename__ = "csc_drafts"
    __table_args__ = (
        db.Index("ix_csc_drafts_status", "status"),
        db.Index("ix_csc_drafts_spec_number", "spec_number"),
        db.Index("ix_csc_drafts_admin_stage", "admin_stage"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    spec_number = db.Column(db.String(100), nullable=False)
    chemical_name = db.Column(db.String(255), nullable=False)
    committee_name = db.Column(
        db.String(255), nullable=False, default="Corporate Specifiction Committee"
    )
    meeting_date = db.Column(db.String(50), nullable=True)
    prepared_by = db.Column(db.String(255), nullable=False, default="Dinesh Reddy_106228")
    reviewed_by = db.Column(db.String(255), nullable=True)
    material_code = db.Column(db.String(50), nullable=True)
    status = db.Column(db.String(50), nullable=False, default="Published")
    created_by_role = db.Column(db.String(20), nullable=False, default="User")
    is_admin_draft = db.Column(db.Boolean, nullable=False, default=False)
    phase1_locked = db.Column(db.Boolean, nullable=False, default=False)
    parent_draft_id = db.Column(
        db.BigInteger, db.ForeignKey("csc_drafts.id"), nullable=True
    )
    admin_stage = db.Column(db.String(50), nullable=False, default="published")
    spec_version = db.Column(db.Integer, nullable=False, default=0)
    created_by_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)

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
    parameters = db.relationship(
        "CSCParameter",
        back_populates="draft",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    sections = db.relationship(
        "CSCSection",
        back_populates="draft",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    issue_flags = db.relationship(
        "CSCIssueFlag",
        back_populates="draft",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    impact_analysis = db.relationship(
        "CSCImpactAnalysis",
        back_populates="draft",
        uselist=False,
        cascade="all, delete-orphan",
        lazy="joined",
    )
    audit_entries = db.relationship(
        "CSCAudit",
        back_populates="draft",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    revisions_as_parent = db.relationship(
        "CSCRevision",
        foreign_keys="CSCRevision.parent_draft_id",
        back_populates="parent_draft",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    spec_versions = db.relationship(
        "CSCSpecVersion",
        back_populates="draft",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    creator = db.relationship(
        "User",
        foreign_keys=[created_by_id],
        lazy="joined",
    )

    # ── Convenience properties (used by templates) ─────────────
    @property
    def subset(self):
        """Return subset derived from spec_number prefix, or None."""
        subset_code, _, _ = parse_spec_number(self.spec_number or "")
        return subset_code

    @property
    def subset_label(self):
        """Return human-readable subset name."""
        return SPEC_SUBSET_LABELS.get(self.subset, self.subset)

    @property
    def subset_display(self):
        """Return subset code and label for UI display."""
        if not self.subset:
            return None
        if self.subset_label:
            return f"{self.subset} - {self.subset_label}"
        return self.subset

    @property
    def version(self):
        """Alias for spec_version (templates use draft.version)."""
        return format_spec_version(self.spec_version)

    def to_dict(self):
        """Serialize to dictionary for API responses."""
        return {
            "id": self.id,
            "spec_number": self.spec_number,
            "chemical_name": self.chemical_name,
            "subset": self.subset,
            "subset_label": self.subset_label,
            "subset_display": self.subset_display,
            "committee_name": self.committee_name,
            "meeting_date": self.meeting_date,
            "prepared_by": self.prepared_by,
            "reviewed_by": self.reviewed_by,
            "material_code": self.material_code,
            "status": self.status,
            "created_by_role": self.created_by_role,
            "is_admin_draft": self.is_admin_draft,
            "phase1_locked": self.phase1_locked,
            "parent_draft_id": self.parent_draft_id,
            "admin_stage": self.admin_stage,
            "spec_version": self.spec_version,
            "version_display": self.version,
            "created_by_id": self.created_by_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<CSCDraft {self.spec_number} - {self.chemical_name}>"
