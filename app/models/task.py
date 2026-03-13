"""Task model for Office Management / Task Tracker."""

from datetime import datetime, timezone
from app.extensions import db


# Valid task scope values
TASK_SCOPES = ["MY", "TEAM", "GLOBAL"]


class Task(db.Model):
    __tablename__ = "tasks"
    __table_args__ = (
        db.Index("ix_tasks_status", "status"),
        db.Index("ix_tasks_priority", "priority"),
        db.Index("ix_tasks_due_date", "due_date"),
        db.Index("ix_tasks_task_scope", "task_scope"),
        db.Index("ix_tasks_is_active", "is_active"),
        db.Index("ix_tasks_created_at", "created_at"),
        db.Index("ix_tasks_active_status_due", "is_active", "status", "due_date"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    task_title = db.Column(db.String(255), nullable=False)
    task_description = db.Column(db.Text, nullable=True)
    task_origin = db.Column(db.String(100), nullable=True)
    status = db.Column(db.String(50), nullable=False, default="Not Started")
    priority = db.Column(db.String(50), nullable=False, default="Medium")
    due_date = db.Column(db.Date, nullable=True)
    owner_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    created_by = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    office_id = db.Column(db.BigInteger, db.ForeignKey("offices.id"), nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)

    # ── Task Visibility Scope ─────────────────────────────────────
    # MY     – visible to assignee + their controlling officer
    # TEAM   – visible to assignee's reviewing officer + super roles
    # GLOBAL – visible to all users with tasks module access
    task_scope = db.Column(db.String(50), nullable=False, default="MY")

    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    owner = db.relationship(
        "User", foreign_keys=[owner_id], back_populates="tasks_owned", lazy="joined"
    )
    creator = db.relationship(
        "User", foreign_keys=[created_by], back_populates="tasks_created", lazy="joined"
    )
    office = db.relationship("Office", back_populates="tasks", lazy="joined")
    updates = db.relationship(
        "TaskUpdate",
        back_populates="task",
        cascade="all, delete-orphan",
        lazy="select",
    )

    def __repr__(self):
        return f"<Task {self.id} {self.task_title} [{self.task_scope}]>"
