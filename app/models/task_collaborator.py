"""Task collaborator model linking users to shared tasks."""

from datetime import datetime, timezone

from app.extensions import db


class TaskCollaborator(db.Model):
    __tablename__ = "task_collaborators"
    __table_args__ = (
        db.UniqueConstraint("task_id", "user_id", name="uq_task_collaborators_task_user"),
        db.Index("ix_task_collaborators_user_id", "user_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    task_id = db.Column(db.BigInteger, db.ForeignKey("tasks.id"), nullable=False)
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False)
    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    task = db.relationship("Task", back_populates="collaborator_links", lazy="joined")
    user = db.relationship("User", back_populates="task_collaborations", lazy="joined")

    def __repr__(self):
        return f"<TaskCollaborator task={self.task_id} user={self.user_id}>"
