from flask import Blueprint

reports_bp = Blueprint("reports", __name__, template_folder="../templates/modules")

from app.modules.reports import routes  # noqa: E402, F401
