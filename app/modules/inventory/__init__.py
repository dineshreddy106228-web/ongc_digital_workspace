from flask import Blueprint

inventory_bp = Blueprint(
    "inventory",
    __name__,
    template_folder="../templates",
)

from app.modules.inventory import msds_routes  # noqa: E402, F401
from app.modules.inventory import routes       # noqa: E402, F401
