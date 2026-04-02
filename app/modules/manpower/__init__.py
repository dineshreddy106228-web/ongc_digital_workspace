from flask import Blueprint

manpower_bp = Blueprint(
    "manpower",
    __name__,
    template_folder="../templates",
)

from app.modules.manpower import routes  # noqa: E402, F401
