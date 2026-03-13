"""User model – authentication, role assignment, office membership, governance."""

from datetime import datetime, timezone
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from app.extensions import db


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    full_name = db.Column(db.String(150), default="")
    email = db.Column(db.String(150), unique=True, nullable=True)
    password_hash = db.Column(db.String(255), nullable=False)

    role_id = db.Column(db.BigInteger, db.ForeignKey("roles.id"), nullable=True)
    office_id = db.Column(db.BigInteger, db.ForeignKey("offices.id"), nullable=True)

    # ── Reporting Hierarchy ───────────────────────────────────────
    # controlling_officer_id: the officer who directly supervises this user
    controlling_officer_id = db.Column(
        db.BigInteger, db.ForeignKey("users.id"), nullable=True
    )
    # reviewing_officer_id: the officer who reviews this user's team tasks
    reviewing_officer_id = db.Column(
        db.BigInteger, db.ForeignKey("users.id"), nullable=True
    )

    designation = db.Column(db.String(150), default="")
    employee_code = db.Column(db.String(50), default="")
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    must_change_password = db.Column(db.Boolean, default=True, nullable=False)
    last_login_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(
        db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Relationships ─────────────────────────────────────────────
    role = db.relationship("Role", back_populates="users", lazy="joined")
    office = db.relationship("Office", back_populates="users", lazy="joined")

    tasks_owned = db.relationship(
        "Task",
        foreign_keys="Task.owner_id",
        back_populates="owner",
        lazy="dynamic",
    )
    tasks_created = db.relationship(
        "Task",
        foreign_keys="Task.created_by",
        back_populates="creator",
        lazy="dynamic",
    )
    task_updates = db.relationship(
        "TaskUpdate",
        foreign_keys="TaskUpdate.updated_by",
        back_populates="updater",
        lazy="dynamic",
    )

    # ── Hierarchy relationships ────────────────────────────────────
    # The user who is this user's controlling officer (many-to-one)
    controlling_officer = db.relationship(
        "User",
        foreign_keys=[controlling_officer_id],
        primaryjoin="User.controlling_officer_id == User.id",
        uselist=False,
        lazy="select",
    )
    # Users for whom this user IS the controlling officer (one-to-many)
    controlled_users = db.relationship(
        "User",
        foreign_keys="User.controlling_officer_id",
        primaryjoin="User.controlling_officer_id == User.id",
        uselist=True,
        lazy="dynamic",
        overlaps="controlling_officer",
    )

    # The user who is this user's reviewing officer (many-to-one)
    reviewing_officer = db.relationship(
        "User",
        foreign_keys=[reviewing_officer_id],
        primaryjoin="User.reviewing_officer_id == User.id",
        uselist=False,
        lazy="select",
    )
    # Users for whom this user IS the reviewing officer (one-to-many)
    reviewed_users = db.relationship(
        "User",
        foreign_keys="User.reviewing_officer_id",
        primaryjoin="User.reviewing_officer_id == User.id",
        uselist=True,
        lazy="dynamic",
        overlaps="reviewing_officer",
    )

    # Module access grants
    module_permissions = db.relationship(
        "UserModulePermission",
        back_populates="user",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    # ── Password helpers ──────────────────────────────────────────
    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    # ── Role helpers (existing – preserved) ──────────────────────
    def has_role(self, *role_names: str) -> bool:
        if self.role is None:
            return False
        return self.role.name in role_names

    # ── Governance helpers (two-role model: super_user / user) ────
    def is_super_user(self) -> bool:
        """True when this user holds the super_user role (full access)."""
        return self.has_role("super_user")

    # Aliases kept so existing call-sites don't break during migration
    def is_super_admin(self) -> bool:
        return self.is_super_user()

    def is_admin_user(self) -> bool:
        return self.is_super_user()

    # ── Module access helper ──────────────────────────────────────
    def has_module_access(self, module_code: str) -> bool:
        """
        Return True if this user may enter the given module.

        Rules:
          super_user → all SUPER_USER_MODULES + admin_users
          user       → only what is in user_module_permissions with can_access=True
        """
        if self.is_super_user():
            from app.models.user_module_permission import SUPER_USER_MODULES
            return module_code in SUPER_USER_MODULES or module_code == "admin_users"

        # Explicit permission row check for plain 'user' role
        from app.models.user_module_permission import UserModulePermission
        perm = UserModulePermission.query.filter_by(
            user_id=self.id,
            module_code=module_code,
            can_access=True,
        ).first()
        return perm is not None

    def get_accessible_module_codes(self) -> list:
        """Return a list of module codes this user can access."""
        from app.models.user_module_permission import SUPPORTED_MODULES, SUPER_USER_MODULES

        if self.is_super_user():
            return [code for code, _ in SUPPORTED_MODULES]

        from app.models.user_module_permission import UserModulePermission
        explicit = UserModulePermission.query.filter_by(
            user_id=self.id, can_access=True
        ).all()
        return [p.module_code for p in explicit]

    def __repr__(self):
        return f"<User {self.username}>"
