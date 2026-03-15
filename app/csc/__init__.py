from flask import Blueprint

csc_bp = Blueprint("csc", __name__, template_folder="../templates/modules")

from app.csc import routes  # noqa: E402, F401
