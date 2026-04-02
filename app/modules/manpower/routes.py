"""Routes for the Manpower Planning module."""

from flask import abort, jsonify, render_template
from flask_login import login_required

from app.core.services.manpower_planning import (
    get_manpower_location,
    get_manpower_workspace,
)
from app.core.utils.decorators import module_access_required
from app.modules.manpower import manpower_bp


@manpower_bp.route("/")
@login_required
@module_access_required("manpower")
def landing():
    workspace = get_manpower_workspace()
    return render_template("manpower/landing.html", workspace=workspace)


@manpower_bp.route("/location/<location_slug>")
@login_required
@module_access_required("manpower")
def location_detail(location_slug: str):
    workspace = get_manpower_workspace()
    location = get_manpower_location(location_slug)
    if location is None:
        abort(404)
    return render_template(
        "manpower/location_detail.html",
        workspace=workspace,
        location=location,
    )


@manpower_bp.route("/api/overview")
@login_required
@module_access_required("manpower")
def overview_api():
    workspace = get_manpower_workspace()
    return jsonify(
        {
            "available": workspace["available"],
            "as_of_date": workspace["as_of_date_iso"],
            "total_locations": workspace["total_locations"],
            "total_employees": workspace["total_employees"],
            "average_vintage": workspace["average_vintage"],
            "retirements_within_two_years": workspace["retirements_within_two_years"],
            "locations": [
                {
                    "slug": location["slug"],
                    "name": location["name"],
                    "employee_count": location["employee_count"],
                    "average_vintage": location["average_vintage"],
                    "max_vintage": location["max_vintage"],
                    "retirements_within_two_years": location["retirements_within_two_years"],
                    "primary_region": location["primary_region"],
                    "reported_locations": location["reported_locations"],
                    "top_levels": location["top_levels"],
                    "vintage_bands": location["vintage_bands"],
                }
                for location in workspace["locations"]
            ],
        }
    )


@manpower_bp.route("/api/location/<location_slug>")
@login_required
@module_access_required("manpower")
def location_api(location_slug: str):
    location = get_manpower_location(location_slug)
    if location is None:
        abort(404)
    return jsonify(location)
