"""Office model – represents an ONGC office / lab / section."""

from datetime import datetime, timezone
from app.extensions import db


class Office(db.Model):
    __tablename__ = "offices"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    office_code = db.Column(db.String(50), unique=True, nullable=False, index=True)
    office_name = db.Column(db.String(150), nullable=False)
    location = db.Column(db.String(150), default="")
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    users = db.relationship("User", back_populates="office", lazy="dynamic")
    tasks = db.relationship("Task", back_populates="office", lazy="dynamic")
    recurring_templates = db.relationship(
        "RecurringTaskTemplate", back_populates="office", lazy="dynamic"
    )

    def __repr__(self):
        return f"<Office {self.office_code}>"
