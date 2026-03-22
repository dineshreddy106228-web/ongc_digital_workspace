from flask import Blueprint

committee_bp = Blueprint(
    "committee",
    __name__,
    template_folder="../../templates/committee",
)

from app.modules.committee import routes  # noqa: E402, F401
