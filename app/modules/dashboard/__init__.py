from flask import Blueprint

dashboard_bp = Blueprint("main", __name__, template_folder="../templates/main")

from app.modules.dashboard import routes  # noqa: E402, F401
