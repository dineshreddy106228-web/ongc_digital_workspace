from flask import Blueprint

office_bp = Blueprint("tasks", __name__, template_folder="../templates/tasks")

from app.modules.office import routes  # noqa: E402, F401
