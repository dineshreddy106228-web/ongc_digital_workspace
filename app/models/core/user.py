"""User model – authentication, role assignment, office membership, governance."""

from datetime import datetime, timezone
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from app.extensions import db
from app.features import is_module_enabled
from app.core.roles import ADMIN_ROLE, SUPERUSER_ROLE, canonicalize_role_name


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
    # reviewing_officer_id: reserved for future review workflows
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
    task_collaborations = db.relationship(
        "TaskCollaborator",
        foreign_keys="TaskCollaborator.user_id",
        back_populates="user",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )
    recurring_templates_owned = db.relationship(
        "RecurringTaskTemplate",
        foreign_keys="RecurringTaskTemplate.owner_id",
        back_populates="owner",
        lazy="dynamic",
    )
    recurring_templates_created = db.relationship(
        "RecurringTaskTemplate",
        foreign_keys="RecurringTaskTemplate.created_by",
        back_populates="creator",
        lazy="dynamic",
    )
    recurring_task_collaborations = db.relationship(
        "RecurringTaskCollaborator",
        foreign_keys="RecurringTaskCollaborator.user_id",
        back_populates="user",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    # ── Hierarchy relationships ────────────────────────────────────
    # The user who is this user's controlling officer (many-to-one)
    controlling_officer = db.relationship(
        "User",
        foreign_keys=[controlling_officer_id],
        remote_side=[id],
        back_populates="controlled_users",
        uselist=False,
        lazy="select",
        overlaps="reviewing_officer,reviewed_users",
    )
    # Users for whom this user IS the controlling officer (one-to-many)
    controlled_users = db.relationship(
        "User",
        foreign_keys=[controlling_officer_id],
        back_populates="controlling_officer",
        uselist=True,
        lazy="dynamic",
        overlaps="reviewing_officer,reviewed_users",
    )

    # The user who is this user's reviewing officer (many-to-one)
    reviewing_officer = db.relationship(
        "User",
        foreign_keys=[reviewing_officer_id],
        remote_side=[id],
        back_populates="reviewed_users",
        uselist=False,
        lazy="select",
        overlaps="controlling_officer,controlled_users",
    )
    # Users for whom this user IS the reviewing officer (one-to-many)
    reviewed_users = db.relationship(
        "User",
        foreign_keys=[reviewing_officer_id],
        back_populates="reviewing_officer",
        uselist=True,
        lazy="dynamic",
        overlaps="controlling_officer,controlled_users",
    )

    # Module access grants
    module_permissions = db.relationship(
        "UserModulePermission",
        back_populates="user",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )
    notifications = db.relationship(
        "Notification",
        back_populates="user",
        lazy="dynamic",
        cascade="all, delete-orphan",
        order_by="desc(Notification.created_at)",
    )
    module_admin_assignments = db.relationship(
        "ModuleAdminAssignment",
        back_populates="user",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    # ── Password helpers ──────────────────────────────────────────
    def set_password(self, password: str) -> None:
        # Explicitly use pbkdf2:sha256 — Werkzeug 3.x defaults to scrypt,
        # which is unavailable in Python 3.9 on some platforms (macOS).
        self.password_hash = generate_password_hash(password, method="pbkdf2:sha256")

    def check_password(self, password: str) -> bool:
        # Guard against scrypt hashes on platforms where hashlib.scrypt is
        # absent (Python 3.9 + macOS system OpenSSL).  If the stored hash
        # used scrypt and the runtime can't verify it, treat it as a failed
        # authentication so the app never 500s — the admin can reset that
        # user's password via the CLI (flask reset-password).
        try:
            return check_password_hash(self.password_hash, password)
        except (AttributeError, ValueError):
            return False

    # ── Role helpers (existing – preserved) ──────────────────────
    def has_role(self, *role_names: str) -> bool:
        if self.role is None:
            return False
        actual_role = canonicalize_role_name(self.role.name)
        expected_roles = {canonicalize_role_name(role_name) for role_name in role_names}
        return actual_role in expected_roles

    # ── Governance helpers ────────────────────────────────────────
    def is_super_user(self) -> bool:
        """True when this user holds the superuser business-access role."""
        return self.has_role(SUPERUSER_ROLE)

    # Aliases kept so existing call-sites don't break during migration
    def is_super_admin(self) -> bool:
        return self.is_super_user()

    def is_admin_user(self) -> bool:
        return self.has_role(ADMIN_ROLE)

    # ── Module access helper ──────────────────────────────────────
    def has_module_access(self, module_code: str) -> bool:
        """
        Return True if this user may enter the given module.

        Rules:
          disabled module  → denied before user-level permission checks
          admin            → admin_users only
          superuser        → all business modules
          user             → only what is in user_module_permissions with can_access=True
        """
        if not is_module_enabled(module_code):
            return False

        if self.has_role(ADMIN_ROLE):
            return module_code == "admin_users"

        if self.is_super_user():
            from app.models.core.user_module_permission import SUPER_USER_MODULES
            return module_code in SUPER_USER_MODULES

        # Explicit permission row check for plain 'user' role
        from app.models.core.user_module_permission import UserModulePermission
        perm = UserModulePermission.query.filter_by(
            user_id=self.id,
            module_code=module_code,
            can_access=True,
        ).first()
        return perm is not None

    def get_accessible_module_codes(self) -> list:
        """Return a list of module codes this user can access."""
        from app.models.core.user_module_permission import SUPPORTED_MODULES

        if self.has_role(ADMIN_ROLE):
            return ["admin_users"]

        if self.is_super_user():
            return [code for code, _ in SUPPORTED_MODULES if is_module_enabled(code)]

        from app.models.core.user_module_permission import UserModulePermission
        explicit = UserModulePermission.query.filter_by(
            user_id=self.id, can_access=True
        ).all()
        return [p.module_code for p in explicit if is_module_enabled(p.module_code)]

    def is_module_admin(self, module_code: str) -> bool:
        """Return True when this user is assigned as an admin for the module."""
        from app.models.core.module_admin_assignment import ModuleAdminAssignment

        return (
            ModuleAdminAssignment.query.filter_by(
                user_id=self.id,
                module_code=module_code,
            ).first()
            is not None
        )

    def get_administered_module_codes(self) -> list[str]:
        """Return business module codes this user administers."""
        from app.models.core.module_admin_assignment import ModuleAdminAssignment

        return [
            assignment.module_code
            for assignment in self.module_admin_assignments.order_by(ModuleAdminAssignment.module_code).all()
        ]

    def __repr__(self):
        return f"<User {self.username}>"
