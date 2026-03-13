"""Placeholder routes for the Inventory module."""

from flask import render_template
from flask_login import login_required

from app.inventory import inventory_bp
from app.utils.decorators import module_access_required


@inventory_bp.route("/")
@login_required
@module_access_required("inventory")
def index():
    return render_template(
        "modules/placeholder.html",
        module_title="Inventory Control",
        module_summary="Inventory workflows can be developed here and exposed later via feature flags.",
    )
