"""AuditLog model – immutable record of security-relevant events."""

from datetime import datetime, timezone
from app.extensions import db


class AuditLog(db.Model):
    __tablename__ = "audit_logs"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(db.BigInteger, nullable=True)
    action = db.Column(db.String(100), nullable=False, index=True)
    entity_type = db.Column(db.String(100), default="")
    entity_id = db.Column(db.String(100), default="")
    details = db.Column(db.Text, default="")
    ip_address = db.Column(db.String(45), default="")
    user_agent = db.Column(db.Text, default="")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def __repr__(self):
        return f"<AuditLog {self.action} by user_id={self.user_id}>"

    @staticmethod
    def _normalize_ip(ip_address: str) -> str:
        if not ip_address:
            return ""
        return str(ip_address).strip()[:45]

    @staticmethod
    def _normalize_user_agent(user_agent: str) -> str:
        if not user_agent:
            return ""
        return str(user_agent).strip()[:500]

    @classmethod
    def log(cls, action, user_id=None, entity_type="", entity_id="",
            details="", ip_address="", user_agent=""):
        """Convenience factory – create and flush an audit row."""
        entry = cls(
            user_id=user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details,
            ip_address=cls._normalize_ip(ip_address),
            user_agent=cls._normalize_user_agent(user_agent),
        )
        db.session.add(entry)
        db.session.commit()
        return entry
