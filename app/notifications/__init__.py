"""Notifications blueprint."""

from flask import Blueprint

notifications_bp = Blueprint(
    "notifications",
    __name__,
    template_folder="../templates/notifications",
)

from app.notifications import routes  # noqa: E402, F401
