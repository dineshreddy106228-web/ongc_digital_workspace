"""ModuleAdminAssignment – per-module admin ownership grants."""

from datetime import datetime, timezone

from app.extensions import db


class ModuleAdminAssignment(db.Model):
    __tablename__ = "module_admin_assignments"

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    user_id = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    module_code = db.Column(db.String(100), nullable=False, index=True)

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

    __table_args__ = (
        db.UniqueConstraint("user_id", "module_code", name="uq_module_admin_assignment"),
    )

    user = db.relationship("User", back_populates="module_admin_assignments")

    def __repr__(self):
        return f"<ModuleAdminAssignment user_id={self.user_id} module={self.module_code}>"
