from flask import Blueprint

inventory_bp = Blueprint("inventory", __name__, template_folder="../templates/modules")

from app.inventory import routes  # noqa: E402, F401
