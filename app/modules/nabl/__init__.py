from flask import Blueprint

nabl_bp = Blueprint(
    "nabl",
    __name__,
    template_folder="../templates",
)

from app.modules.nabl import routes  # noqa: E402, F401
