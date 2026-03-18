"""Announcement and poll models for workspace-wide communications."""

from datetime import datetime, timezone

from app.extensions import db


ANNOUNCEMENT_TYPES = ("ANNOUNCEMENT", "POLL")
ANNOUNCEMENT_STATUSES = ("DRAFT", "PUBLISHED", "CLOSED")
ANNOUNCEMENT_AUDIENCE_SCOPE = ("MODULE_USERS_AND_SUPERUSERS",)


class Announcement(db.Model):
    __tablename__ = "announcements"
    __table_args__ = (
        db.Index("ix_announcements_status_published_at", "status", "published_at"),
        db.Index("ix_announcements_created_by", "created_by"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    title = db.Column(db.String(255), nullable=False)
    summary = db.Column(db.String(500), nullable=True)
    body = db.Column(db.Text, nullable=False)
    announcement_type = db.Column(db.String(20), nullable=False, default="ANNOUNCEMENT")
    status = db.Column(db.String(20), nullable=False, default="DRAFT")
    severity = db.Column(db.String(20), nullable=False, default="info")
    audience_scope = db.Column(
        db.String(50),
        nullable=False,
        default="MODULE_USERS_AND_SUPERUSERS",
    )
    created_by = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False)
    published_at = db.Column(db.DateTime, nullable=True)
    expires_at = db.Column(db.DateTime, nullable=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    creator = db.relationship("User", foreign_keys=[created_by], lazy="joined")
    options = db.relationship(
        "AnnouncementOption",
        back_populates="announcement",
        cascade="all, delete-orphan",
        order_by="AnnouncementOption.display_order.asc()",
        lazy="select",
    )
    recipients = db.relationship(
        "AnnouncementRecipient",
        back_populates="announcement",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    votes = db.relationship(
        "AnnouncementVote",
        back_populates="announcement",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )

    def __repr__(self):
        return (
            f"<Announcement {self.id} {self.announcement_type} "
            f"{self.status} '{self.title}'>"
        )


class AnnouncementOption(db.Model):
    __tablename__ = "announcement_options"
    __table_args__ = (
        db.UniqueConstraint(
            "announcement_id",
            "display_order",
            name="uq_announcement_options_order",
        ),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    announcement_id = db.Column(
        db.BigInteger,
        db.ForeignKey("announcements.id"),
        nullable=False,
        index=True,
    )
    option_text = db.Column(db.String(255), nullable=False)
    display_order = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    announcement = db.relationship("Announcement", back_populates="options", lazy="joined")
    votes = db.relationship(
        "AnnouncementVote",
        back_populates="option",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )

    def __repr__(self):
        return f"<AnnouncementOption {self.id} announcement={self.announcement_id}>"


class AnnouncementRecipient(db.Model):
    __tablename__ = "announcement_recipients"
    __table_args__ = (
        db.UniqueConstraint(
            "announcement_id",
            "user_id",
            name="uq_announcement_recipients_announcement_user",
        ),
        db.Index(
            "ix_announcement_recipients_user_read",
            "user_id",
            "is_read",
        ),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    announcement_id = db.Column(
        db.BigInteger,
        db.ForeignKey("announcements.id"),
        nullable=False,
        index=True,
    )
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True)
    is_read = db.Column(db.Boolean, nullable=False, default=False)
    read_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    announcement = db.relationship("Announcement", back_populates="recipients", lazy="joined")
    user = db.relationship("User", lazy="joined")

    def __repr__(self):
        return (
            f"<AnnouncementRecipient announcement={self.announcement_id} "
            f"user={self.user_id} read={self.is_read}>"
        )


class AnnouncementVote(db.Model):
    __tablename__ = "announcement_votes"
    __table_args__ = (
        db.UniqueConstraint(
            "announcement_id",
            "user_id",
            name="uq_announcement_votes_announcement_user",
        ),
        db.Index("ix_announcement_votes_option_id", "option_id"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    announcement_id = db.Column(
        db.BigInteger,
        db.ForeignKey("announcements.id"),
        nullable=False,
        index=True,
    )
    option_id = db.Column(
        db.BigInteger,
        db.ForeignKey("announcement_options.id"),
        nullable=False,
    )
    user_id = db.Column(db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    announcement = db.relationship("Announcement", back_populates="votes", lazy="joined")
    option = db.relationship("AnnouncementOption", back_populates="votes", lazy="joined")
    user = db.relationship("User", lazy="joined")

    def __repr__(self):
        return (
            f"<AnnouncementVote announcement={self.announcement_id} "
            f"user={self.user_id} option={self.option_id}>"
        )
