from flask import Blueprint

tasks_bp = Blueprint("tasks", __name__, template_folder="../templates/tasks")

from app.tasks import routes  # noqa: E402, F401
