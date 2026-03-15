from flask import Blueprint

auth_bp = Blueprint("auth", __name__, template_folder="../templates/auth")

from app.core.auth import routes  # noqa: E402, F401
