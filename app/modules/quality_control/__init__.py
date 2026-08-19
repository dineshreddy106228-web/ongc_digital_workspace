from flask import Blueprint

quality_control_bp = Blueprint("quality_control", __name__, template_folder="../templates")

from app.modules.quality_control import routes  # noqa: E402, F401
