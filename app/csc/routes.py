"""Placeholder routes for the CSC module."""

from flask import render_template
from flask_login import login_required

from app.csc import csc_bp
from app.utils.decorators import module_access_required


@csc_bp.route("/")
@login_required
@module_access_required("csc")
def index():
    return render_template(
        "modules/placeholder.html",
        module_title="CSC Workflow",
        module_summary="CSC workflows can be developed safely behind the feature-flag gate.",
    )
