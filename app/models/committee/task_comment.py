"""TaskComment — threaded comments on committee tasks with optional attachment."""

from datetime import datetime, timezone
from app.extensions import db


class TaskComment(db.Model):
    __tablename__ = "task_comments"
    __table_args__ = (
        db.Index("ix_task_comments_task_id", "task_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    task_id = db.Column(
        db.BigInteger,
        db.ForeignKey("committee_tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id = db.Column(
        db.BigInteger, db.ForeignKey("users.id"), nullable=False
    )
    body = db.Column(db.Text, nullable=False)
    attachment_path = db.Column(db.String(512), nullable=True)

    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    # ── Relationships ─────────────────────────────────────────────
    task = db.relationship("CommitteeTask", back_populates="comments")
    author = db.relationship("User", lazy="joined")

    def __repr__(self):
        return f"<TaskComment {self.id} task={self.task_id}>"
