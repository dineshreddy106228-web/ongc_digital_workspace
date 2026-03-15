"""Notification model for per-user in-app alerts."""

from datetime import datetime, timezone

from app.extensions import db


NOTIFICATION_SEVERITIES = ("info", "success", "warning", "danger")


class Notification(db.Model):
    __tablename__ = "notifications"
    __table_args__ = (
        db.Index("ix_notifications_user_is_read", "user_id", "is_read"),
        db.Index("ix_notifications_user_created_at", "user_id", "created_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True)
    title = db.Column(db.String(255), nullable=False)
    message = db.Column(db.Text, nullable=False)
    severity = db.Column(db.String(20), nullable=False, default="info")
    link = db.Column(db.String(255), nullable=True)
    is_read = db.Column(db.Boolean, nullable=False, default=False, index=True)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    user = db.relationship("User", back_populates="notifications", lazy="joined")

    def __repr__(self):
        return f"<Notification {self.id} user={self.user_id} severity={self.severity}>"
