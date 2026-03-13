"""TaskUpdate model for task history / status changes."""

from datetime import datetime, timezone
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
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    task = db.relationship("Task", back_populates="updates", lazy="joined")
    updater = db.relationship("User", foreign_keys=[updated_by], back_populates="task_updates", lazy="joined")

    def __repr__(self):
        return f"<TaskUpdate {self.id} task={self.task_id}>"
