"""Placeholder routes for the Reports module."""

from flask import render_template
from flask_login import login_required

from app.reports import reports_bp
from app.utils.decorators import module_access_required


@reports_bp.route("/")
@login_required
@module_access_required("reports")
def index():
    return render_template(
        "modules/placeholder.html",
        module_title="Reports",
        module_summary="Reports can be built in this shared codebase and enabled per environment later.",
    )
