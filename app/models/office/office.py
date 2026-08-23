"""Office model – represents an ONGC office / lab / section."""

from datetime import datetime, timezone
from app.extensions import db


class Office(db.Model):
    __tablename__ = "offices"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    office_code = db.Column(db.String(50), unique=True, nullable=False, index=True)
    office_name = db.Column(db.String(150), nullable=False)
    location = db.Column(db.String(150), default="")
    secondary_location = db.Column(db.String(150), default="")
    power_user_limit = db.Column(db.Integer, default=0, nullable=False, server_default="0")
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

    @property
    def location_label(self) -> str:
        """Every place this office works, as one readable line.

        An office can operate from more than one location while remaining a
        single office with a single task register — Head Corporate Chemistry
        runs from Mumbai and Dehradun — so both are shown wherever the office
        is named.
        """
        parts = [part.strip() for part in (self.location, self.secondary_location) if part and part.strip()]
        return " & ".join(parts)

    def __repr__(self):
        return f"<Office {self.office_code}>"
