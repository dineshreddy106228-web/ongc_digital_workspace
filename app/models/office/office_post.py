"""Office posts — the designation, kept separate from the person holding it.

A post ("Head RGL Rajahmundry") outlives the people who hold it. Recording it
separately from the user account means a handover never rewrites history: the
task updates, approvals and audit entries a person wrote stay attributed to
that person, while the post moves on to whoever holds it next.

OfficePostAssignment keeps the succession, so "who held this post in July 2026"
remains answerable after the holder changes.
"""

from datetime import datetime, timezone

from app.extensions import db


class OfficePost(db.Model):
    __tablename__ = "office_posts"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    office_id = db.Column(db.BigInteger, db.ForeignKey("offices.id"), nullable=False, index=True)
    post_code = db.Column(db.String(50), unique=True, nullable=False, index=True)
    post_title = db.Column(db.String(150), nullable=False)
    description = db.Column(db.String(255), default="")
    # Nullable: a post between holders is vacant, not broken.
    holder_user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True, index=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    office = db.relationship("Office", foreign_keys=[office_id])
    holder = db.relationship("User", foreign_keys=[holder_user_id])
    assignments = db.relationship(
        "OfficePostAssignment",
        back_populates="post",
        order_by="OfficePostAssignment.started_at.desc()",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    @property
    def holder_label(self) -> str:
        """Who holds the post now, or that it is vacant."""
        if self.holder is None:
            return "Vacant"
        return self.holder.full_name or self.holder.username

    @property
    def current_assignment(self):
        """The open assignment, if the post is filled."""
        for assignment in self.assignments:
            if assignment.ended_at is None:
                return assignment
        return None

    @staticmethod
    def _as_utc(moment):
        """Stored timestamps are naive UTC; callers may pass aware ones.

        Same normalisation TaskUpdate does, so the two sides of a comparison
        are never one naive and one aware.
        """
        if moment is None:
            return None
        if moment.tzinfo is None:
            return moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc)

    def holder_on(self, moment: datetime):
        """The user holding this post at *moment* — the point of the history."""
        moment = self._as_utc(moment)
        for assignment in self.assignments:
            started = self._as_utc(assignment.started_at)
            ended = self._as_utc(assignment.ended_at)
            if started is not None and started <= moment and (ended is None or moment < ended):
                return assignment.user
        return None

    def __repr__(self):
        return f"<OfficePost {self.post_code}>"


class OfficePostAssignment(db.Model):
    """One person's tenure in one post. Closed, never deleted, on handover."""

    __tablename__ = "office_post_assignments"
    __table_args__ = (
        db.Index("ix_office_post_assignments_post_started", "post_id", "started_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    post_id = db.Column(db.BigInteger, db.ForeignKey("office_posts.id"), nullable=False, index=True)
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True)
    # The name as it stood when the tenure began: if the user record is later
    # renamed or removed, the succession still reads correctly.
    holder_name = db.Column(db.String(150), default="")
    started_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    ended_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    post = db.relationship("OfficePost", back_populates="assignments")
    user = db.relationship("User", foreign_keys=[user_id])

    @property
    def is_current(self) -> bool:
        return self.ended_at is None

    def __repr__(self):
        return f"<OfficePostAssignment post={self.post_id} user={self.user_id}>"
