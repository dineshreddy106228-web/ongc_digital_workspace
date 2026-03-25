"""TaskUpdate model for task history / status changes."""

from datetime import datetime, timedelta, timezone
from app.extensions import db


class TaskUpdate(db.Model):
    __tablename__ = "task_updates"
    __table_args__ = (
        db.Index("ix_task_updates_task_id_created_at", "task_id", "created_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    task_id = db.Column(db.BigInteger, db.ForeignKey("tasks.id"), nullable=False)
    update_text = db.Column(db.Text, nullable=False)
    old_status = db.Column(db.String(50), nullable=True)
    new_status = db.Column(db.String(50), nullable=True)
    updated_by = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    edited_by = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    edited_at = db.Column(db.DateTime, nullable=True)

    task = db.relationship("Task", back_populates="updates", lazy="joined")
    updater = db.relationship("User", foreign_keys=[updated_by], back_populates="task_updates", lazy="joined")
    editor = db.relationship("User", foreign_keys=[edited_by], back_populates="task_updates_edited", lazy="joined")

    @staticmethod
    def _as_utc(moment):
        if moment is None:
            return None
        if moment.tzinfo is None:
            return moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc)

    def edit_deadline(self, edit_window_hours=12):
        created_at = self._as_utc(self.created_at)
        if created_at is None:
            return None
        return created_at + timedelta(hours=edit_window_hours)

    def is_within_edit_window(self, edit_window_hours=12, now=None):
        deadline = self.edit_deadline(edit_window_hours)
        if deadline is None:
            return False
        comparison_time = self._as_utc(now) or datetime.now(timezone.utc)
        return comparison_time <= deadline

    def is_editable_by(self, user, edit_window_hours=12, now=None):
        if user is None or not getattr(user, "is_authenticated", False):
            return False
        if self.updated_by != getattr(user, "id", None):
            return False
        return self.is_within_edit_window(edit_window_hours, now)

    @property
    def was_edited(self):
        return self.edited_at is not None

    def __repr__(self):
        return f"<TaskUpdate {self.id} task={self.task_id}>"
