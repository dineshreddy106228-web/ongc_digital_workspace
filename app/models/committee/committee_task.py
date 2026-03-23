"""CommitteeTask model — office-scoped task for committee workflows."""

from datetime import datetime, timezone
from app.extensions import db


class CommitteeTask(db.Model):
    __tablename__ = "committee_tasks"
    __table_args__ = (
        db.Index("ix_committee_tasks_office_id", "office_id"),
        db.Index("ix_committee_tasks_created_by", "created_by"),
        db.Index("ix_committee_tasks_status", "status"),
        db.Index("ix_committee_tasks_priority", "priority"),
        db.Index("ix_committee_tasks_due_date", "due_date"),
        db.Index("ix_committee_tasks_office_status", "office_id", "status"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    priority = db.Column(db.String(50), nullable=False, default="medium")
    status = db.Column(db.String(50), nullable=False, default="open")
    due_date = db.Column(db.Date, nullable=True)

    office_id = db.Column(
        db.BigInteger, db.ForeignKey("offices.id"), nullable=False
    )
    created_by = db.Column(
        db.BigInteger, db.ForeignKey("users.id"), nullable=False
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

    # ── Relationships ─────────────────────────────────────────────
    office = db.relationship("Office", lazy="joined")
    creator = db.relationship(
        "User", foreign_keys=[created_by], lazy="joined"
    )
    members = db.relationship(
        "CommitteeTaskMember",
        back_populates="task",
        cascade="all, delete-orphan",
        lazy="select",
    )
    comments = db.relationship(
        "TaskComment",
        back_populates="task",
        cascade="all, delete-orphan",
        lazy="select",
        order_by="TaskComment.created_at.asc()",
    )

    def __repr__(self):
        return f"<CommitteeTask {self.id} {self.title}>"
