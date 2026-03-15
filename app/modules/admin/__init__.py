from flask import Blueprint

admin_bp = Blueprint("admin", __name__, template_folder="../templates/admin")

from app.modules.admin import routes  # noqa: E402, F401
