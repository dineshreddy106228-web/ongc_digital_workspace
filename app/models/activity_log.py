"""ActivityLog model – human-readable activity timeline for the dashboard feed.

Unlike AuditLog (security-focused, immutable, includes IP/user-agent), ActivityLog
stores friendly, display-ready summaries of user actions across the platform.
"""

from datetime import datetime, timezone
from app.extensions import db


class ActivityLog(db.Model):
    __tablename__ = "activity_logs"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    actor_username = db.Column(db.String(80), nullable=True, index=True)
    action_type = db.Column(db.String(50), nullable=False, index=True)
    entity_type = db.Column(db.String(50), nullable=False, default="")
    entity_name = db.Column(db.String(255), nullable=False, default="")
    details = db.Column(db.Text, nullable=True)
    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    def __repr__(self):
        return f"<ActivityLog {self.action_type} '{self.entity_name}' by {self.actor_username}>"

    # ── Display helpers ──────────────────────────────────────────

    ACTION_ICONS = {
        "login": "bi-box-arrow-in-right",
        "logout": "bi-box-arrow-left",
        "user_created": "bi-person-plus",
        "user_updated": "bi-pencil-square",
        "user_deactivated": "bi-person-x",
        "user_activated": "bi-person-check",
        "password_reset": "bi-key",
        "password_changed": "bi-shield-lock",
        "role_changed": "bi-people",
        "backup_exported": "bi-download",
        "task_created": "bi-plus-circle",
        "task_updated": "bi-pencil",
        "task_status_changed": "bi-arrow-repeat",
        "task_update_added": "bi-chat-left-text",
    }

    ACTION_COLORS = {
        "login": "var(--success, #22c55e)",
        "logout": "var(--muted, #94a3b8)",
        "user_created": "var(--primary, #3b82f6)",
        "user_updated": "var(--warning, #f59e0b)",
        "user_deactivated": "var(--danger, #ef4444)",
        "user_activated": "var(--success, #22c55e)",
        "password_reset": "var(--warning, #f59e0b)",
        "password_changed": "var(--primary, #3b82f6)",
        "role_changed": "var(--warning, #f59e0b)",
        "backup_exported": "var(--primary, #3b82f6)",
        "task_created": "var(--primary, #3b82f6)",
        "task_updated": "var(--warning, #f59e0b)",
        "task_status_changed": "var(--primary, #3b82f6)",
        "task_update_added": "var(--muted, #94a3b8)",
    }

    @property
    def icon_class(self):
        return self.ACTION_ICONS.get(self.action_type, "bi-activity")

    @property
    def accent_color(self):
        return self.ACTION_COLORS.get(self.action_type, "var(--muted, #94a3b8)")

    @property
    def summary(self):
        """Build a human-readable sentence like 'admin created user Gaurav'."""
        actor = self.actor_username or "System"
        verb_map = {
            "login": "logged in",
            "logout": "logged out",
            "user_created": f"created {self.entity_type.lower()} {self.entity_name}",
            "user_updated": f"updated {self.entity_type.lower()} {self.entity_name}",
            "user_deactivated": f"deactivated {self.entity_type.lower()} {self.entity_name}",
            "user_activated": f"activated {self.entity_type.lower()} {self.entity_name}",
            "password_reset": f"reset password for {self.entity_name}",
            "password_changed": "changed their password",
            "role_changed": f"changed role for {self.entity_name}",
            "backup_exported": f"exported database backup {self.entity_name}",
            "task_created": f"created task \"{self.entity_name}\"",
            "task_updated": f"updated task \"{self.entity_name}\"",
            "task_status_changed": f"changed status of \"{self.entity_name}\"",
            "task_update_added": f"added update to \"{self.entity_name}\"",
        }
        verb = verb_map.get(self.action_type, f"{self.action_type} {self.entity_name}")
        return f"{actor} {verb}"
