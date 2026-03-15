"""Recurring task collaborator model linking users to recurring templates."""

from datetime import datetime, timezone
from app.extensions import db


class RecurringTaskCollaborator(db.Model):
    __tablename__ = "recurring_task_collaborators"
    __table_args__ = (
        db.UniqueConstraint(
            "template_id",
            "user_id",
            name="uq_recurring_task_collaborators_template_user",
        ),
        db.Index("ix_recurring_task_collaborators_user_id", "user_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    template_id = db.Column(
        db.BigInteger, db.ForeignKey("recurring_task_templates.id"), nullable=False
    )
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False)
    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )

    template = db.relationship(
        "RecurringTaskTemplate", back_populates="collaborator_links", lazy="joined"
    )
    user = db.relationship(
        "User", back_populates="recurring_task_collaborations", lazy="joined"
    )

    def __repr__(self):
        return f"<RecurringTaskCollaborator template={self.template_id} user={self.user_id}>"
