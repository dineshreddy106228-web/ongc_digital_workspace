"""Junction table linking GLOBAL-scope tasks to their tagged offices."""

from datetime import datetime, timezone
from app.extensions import db


class TaskOffice(db.Model):
    __tablename__ = "task_offices"
    __table_args__ = (
        db.UniqueConstraint("task_id", "office_id", name="uq_task_offices_task_office"),
        db.Index("ix_task_offices_office_id", "office_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    task_id = db.Column(
        db.BigInteger, db.ForeignKey("tasks.id"), nullable=False, index=True,
    )
    office_id = db.Column(
        db.BigInteger, db.ForeignKey("offices.id"), nullable=False,
    )
    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False,
    )

    # ── Relationships ─────────────────────────────────────────────
    task = db.relationship("Task", back_populates="tagged_office_links", lazy="joined")
    office = db.relationship("Office", lazy="joined")

    def __repr__(self):
        return f"<TaskOffice task={self.task_id} office={self.office_id}>"
