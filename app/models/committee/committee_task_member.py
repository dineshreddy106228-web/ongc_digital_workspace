"""CommitteeTaskMember — association with extra role column."""

from app.extensions import db


class CommitteeTaskMember(db.Model):
    __tablename__ = "committee_task_members"

    task_id = db.Column(
        db.BigInteger,
        db.ForeignKey("committee_tasks.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role = db.Column(db.String(50), nullable=False, default="assignee")

    # ── Relationships ─────────────────────────────────────────────
    task = db.relationship("CommitteeTask", back_populates="members")
    user = db.relationship("User", lazy="joined")

    def __repr__(self):
        return f"<CommitteeTaskMember task={self.task_id} user={self.user_id} role={self.role}>"
