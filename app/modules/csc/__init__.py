from flask import Blueprint

csc_bp = Blueprint(
    "csc",
    __name__,
    template_folder="../templates",
)

from app.modules.csc import routes  # noqa: E402, F401
