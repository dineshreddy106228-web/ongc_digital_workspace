"""Recurring task template model for scheduled task generation."""

from datetime import datetime, timezone
from app.extensions import db


RECURRENCE_TYPES = ["DAILY", "WEEKLY", "MONTHLY"]
RECURRENCE_WEEKDAYS = [
    ("MON", "Monday"),
    ("TUE", "Tuesday"),
    ("WED", "Wednesday"),
    ("THU", "Thursday"),
    ("FRI", "Friday"),
    ("SAT", "Saturday"),
    ("SUN", "Sunday"),
]


class RecurringTaskTemplate(db.Model):
    __tablename__ = "recurring_task_templates"
    __table_args__ = (
        db.Index("ix_recurring_task_templates_next_generation_date", "next_generation_date"),
        db.Index("ix_recurring_task_templates_owner_id", "owner_id"),
        db.Index("ix_recurring_task_templates_task_scope", "task_scope"),
        db.Index("ix_recurring_task_templates_is_active", "is_active"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    task_title = db.Column(db.String(255), nullable=False)
    task_description = db.Column(db.Text, nullable=True)
    task_origin = db.Column(db.String(100), nullable=True)
    status = db.Column(db.String(50), nullable=False, default="Not Started")
    priority = db.Column(db.String(50), nullable=False, default="Medium")
    task_scope = db.Column(db.String(50), nullable=False, default="MY")
    is_private_self_task = db.Column(
        db.Boolean, default=False, nullable=False, server_default="0",
    )
    self_task_visible_to_controlling_officer = db.Column(
        db.Boolean, default=False, nullable=False, server_default="0",
    )

    owner_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    created_by = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    office_id = db.Column(db.BigInteger, db.ForeignKey("offices.id"), nullable=True)

    recurrence_type = db.Column(db.String(20), nullable=False)
    weekly_days = db.Column(db.String(32), nullable=True)
    monthly_day = db.Column(db.Integer, nullable=True)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=True)
    next_generation_date = db.Column(db.Date, nullable=True)
    last_generated_at = db.Column(db.DateTime, nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)

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
        "User",
        foreign_keys=[owner_id],
        back_populates="recurring_templates_owned",
        lazy="joined",
    )
    creator = db.relationship(
        "User",
        foreign_keys=[created_by],
        back_populates="recurring_templates_created",
        lazy="joined",
    )
    office = db.relationship("Office", back_populates="recurring_templates", lazy="joined")
    collaborator_links = db.relationship(
        "RecurringTaskCollaborator",
        back_populates="template",
        cascade="all, delete-orphan",
        lazy="select",
    )
    generated_tasks = db.relationship(
        "Task",
        back_populates="recurring_template",
        lazy="dynamic",
    )

    def __repr__(self):
        return f"<RecurringTaskTemplate {self.id} {self.task_title} [{self.recurrence_type}]>"
