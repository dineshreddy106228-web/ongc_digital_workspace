"""UserModulePermission – per-user module access grants."""

from datetime import datetime, timezone
from app.extensions import db
from app.core.module_registry import MODULE_DEFINITIONS


class UserModulePermission(db.Model):
    __tablename__ = "user_module_permissions"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(
        db.BigInteger, db.ForeignKey("users.id"), nullable=False, index=True
    )
    module_code = db.Column(db.String(100), nullable=False)
    can_access = db.Column(db.Boolean, default=True, nullable=False)

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

    # One user cannot have duplicate entries for the same module
    __table_args__ = (
        db.UniqueConstraint("user_id", "module_code", name="uq_user_module"),
    )

    # Relationship
    user = db.relationship("User", back_populates="module_permissions")

    def __repr__(self):
        return f"<UserModulePermission user_id={self.user_id} module={self.module_code} access={self.can_access}>"


# Supported module codes
SUPPORTED_MODULES = [
    ("dashboard", "Dashboard"),
    *[(module.code, module.name) for module in MODULE_DEFINITIONS],
]

# Modules that super_user gets by default (all business modules)
SUPER_USER_MODULES = {"dashboard"} | {
    module.code for module in MODULE_DEFINITIONS if module.code != "admin_users"
}

# Modules admin gets by default
ADMIN_DEFAULT_MODULES = {"admin_users"}
