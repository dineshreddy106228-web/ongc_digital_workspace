"""Metadata for backup exports triggered from the admin UI."""

from datetime import datetime, timezone

from app.extensions import db


class BackupSnapshot(db.Model):
    __tablename__ = "backup_snapshots"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    filename = db.Column(db.String(255), nullable=False)
    created_by_username = db.Column(db.String(80), nullable=False, index=True)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    environment = db.Column(db.String(50), nullable=False, default="")
    notes = db.Column(db.Text, nullable=True)

    def __repr__(self) -> str:
        return f"<BackupSnapshot {self.filename}>"
