"""Routes for the QC Laboratory Monitoring module."""

from io import BytesIO
import logging

from flask import flash, redirect, render_template, request, send_file, session, url_for
from flask_login import current_user, login_required

from app.core.utils.decorators import module_access_required
from app.extensions import db
from app.modules.quality_control import quality_control_bp

logger = logging.getLogger(__name__)


@quality_control_bp.route("/")
@login_required
@module_access_required("quality_control")
def landing():
    from app.core.services.quality_control import laboratory_landing_data
    return render_template("quality_control/landing.html", laboratories=laboratory_landing_data())


@quality_control_bp.route("/labs/<lab_code>", methods=["GET", "POST"])
@login_required
@module_access_required("quality_control")
def laboratory_dashboard(lab_code: str):
    from app.core.services.quality_control import (
        discard_staged_workbook, get_laboratory, import_weekly_qc_workbook, latest_dashboard_data,
        load_staged_workbook, sanity_check_weekly_qc_workbook, stage_validated_workbook,
    )
    try:
        if request.method == "POST":
            if request.form.get("import_action") == "confirm":
                pending = session.get("qc_pending_import") or {}
                if pending.get("lab_code") != lab_code or not pending.get("token"):
                    raise ValueError("This import review is no longer available. Upload the workbook again.")
                source = load_staged_workbook(pending["token"])
                batch, action = import_weekly_qc_workbook(source, pending["filename"], current_user.id, lab_code)
                db.session.commit()
                discard_staged_workbook(pending["token"])
                session.pop("qc_pending_import", None)
                flash(f"{batch.lab_name}: weekly QC data {action}. {batch.imported_count} new and {batch.updated_count} existing samples processed.", "success")
                return redirect(url_for("quality_control.laboratory_dashboard", lab_code=lab_code))
            workbook = request.files.get("qc_workbook")
            if not workbook or not workbook.filename:
                flash("Select the completed weekly QC workbook before importing.", "warning")
                return redirect(url_for("quality_control.laboratory_dashboard", lab_code=lab_code))
            if not workbook.filename.lower().endswith((".xlsx", ".xls")):
                flash("Upload an Excel workbook (.xlsx or .xls).", "warning")
                return redirect(url_for("quality_control.laboratory_dashboard", lab_code=lab_code))
            source = workbook.read()
            payload, sanity_check = sanity_check_weekly_qc_workbook(source, lab_code)
            if not sanity_check["ready"]:
                return render_template(
                    "quality_control/import_review.html", laboratory=get_laboratory(lab_code), payload=payload,
                    sanity_check=sanity_check, filename=workbook.filename, token=None,
                )
            token = stage_validated_workbook(source)
            session["qc_pending_import"] = {"token": token, "lab_code": lab_code, "filename": workbook.filename}
            return render_template(
                "quality_control/import_review.html", laboratory=get_laboratory(lab_code), payload=payload,
                sanity_check=sanity_check, filename=workbook.filename, token=token,
            )
        return render_template("quality_control/dashboard.html", **latest_dashboard_data(lab_code))
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        logger.exception("QC workbook import/dashboard load failed for lab=%s", lab_code)
        flash("The laboratory dashboard could not be loaded. Please try again.", "danger")
    return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/history")
@login_required
@module_access_required("quality_control")
def sample_history():
    from app.core.services.quality_control import history_filter_options, search_samples
    lab_code = (request.args.get("lab") or "").strip()
    chemical_name = (request.args.get("chemical") or "").strip()
    specification_no = (request.args.get("specification") or "").strip()
    status = (request.args.get("status") or "").strip()
    try:
        return render_template(
            "quality_control/samples.html",
            samples=search_samples(lab_code, chemical_name, specification_no, status),
            filters={"lab": lab_code, "chemical": chemical_name, "specification": specification_no, "status": status},
            **history_filter_options(lab_code),
        )
    except ValueError:
        return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/management-review")
@login_required
@module_access_required("quality_control")
def portfolio_management_review():
    from app.core.services.quality_control import portfolio_management_data
    return render_template("quality_control/portfolio_management_review.html", **portfolio_management_data())


@quality_control_bp.route("/labs/<lab_code>/samples")
@login_required
@module_access_required("quality_control")
def samples(lab_code: str):
    return redirect(url_for("quality_control.sample_history", lab=lab_code))


@quality_control_bp.route("/labs/<lab_code>/management-brief")
@login_required
@module_access_required("quality_control")
def management_brief(lab_code: str):
    from app.core.services.quality_control import latest_dashboard_data
    try:
        return render_template("quality_control/management_brief.html", **latest_dashboard_data(lab_code))
    except ValueError:
        return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/uploads/<int:batch_id>/source")
@login_required
@module_access_required("quality_control")
def download_source(batch_id: int):
    from app.core.services.quality_control import get_upload_batch
    batch = get_upload_batch(batch_id, include_source=True)
    if batch is None:
        flash("The requested weekly source workbook is no longer available.", "warning")
        return redirect(url_for("quality_control.landing"))
    return send_file(BytesIO(batch.source_data), mimetype=batch.source_content_type, as_attachment=True, download_name=batch.source_filename, max_age=0)
