"""Password reset requests raised from the login screen.

A user who cannot sign in identifies themselves by CPF number; the request
lands in the administration queue, where an admin verifies who is asking and
issues a temporary password.  The request row is the record of that exchange:
who asked, from where, who handled it, and when the credential it produced
stops working.
"""

from datetime import datetime, timezone

from app.extensions import db


STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"
STATUS_EXPIRED = "expired"

RESET_REQUEST_STATUSES = (
    STATUS_PENDING,
    STATUS_APPROVED,
    STATUS_REJECTED,
    STATUS_EXPIRED,
)


class PasswordResetRequest(db.Model):
    __tablename__ = "password_reset_requests"
    __table_args__ = (
        db.Index("ix_password_reset_requests_status_created", "status", "created_at"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(
        db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True
    )
    # What the caller typed — a CPF number, or a username for the admin
    # accounts, which have no CPF of their own.  Kept verbatim so an approver
    # can see the original even when it needed normalising to find the account.
    submitted_identifier = db.Column(db.String(80), nullable=False, default="")
    status = db.Column(
        db.String(20), nullable=False, default=STATUS_PENDING, index=True
    )

    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    request_ip = db.Column(db.String(45), nullable=False, default="")
    request_user_agent = db.Column(db.Text, nullable=True)

    handled_by_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=True)
    handled_at = db.Column(db.DateTime, nullable=True)
    handled_note = db.Column(db.String(255), nullable=False, default="")
    # Copy of the expiry stamped on the account at approval time, so the queue
    # can still show what happened after the account's own state is cleared.
    temp_password_expires_at = db.Column(db.DateTime, nullable=True)

    user = db.relationship("User", foreign_keys=[user_id], lazy="joined")
    handled_by = db.relationship("User", foreign_keys=[handled_by_id], lazy="joined")

    @property
    def is_pending(self) -> bool:
        return self.status == STATUS_PENDING

    def __repr__(self) -> str:
        return f"<PasswordResetRequest {self.id} user={self.user_id} {self.status}>"
