"""Role model – defines authorisation roles (super_user, user)."""

from datetime import datetime, timezone
from app.extensions import db


class Role(db.Model):
    __tablename__ = "roles"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    name = db.Column(db.String(50), unique=True, nullable=False, index=True)
    description = db.Column(db.String(255), default="")
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationship back to users
    users = db.relationship("User", back_populates="role", lazy="dynamic")

    def __repr__(self):
        return f"<Role {self.name}>"
