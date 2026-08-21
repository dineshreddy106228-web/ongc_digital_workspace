"""Routes for the QC Laboratory Monitoring module."""

from datetime import date
from io import BytesIO
import logging
from pathlib import Path
import subprocess
import sys

from flask import abort, current_app, flash, redirect, render_template, request, send_file, session, url_for
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


@quality_control_bp.route("/idwe-imports")
@login_required
@module_access_required("quality_control")
def idwe_imports():
    from app.core.services.quality_control import laboratory_landing_data
    idwe = next(item for item in laboratory_landing_data() if item["code"] == "idwe_dehradun")
    return render_template("quality_control/idwe_imports.html", laboratory=idwe)


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
            payload, sanity_check = sanity_check_weekly_qc_workbook(source, lab_code, workbook.filename)
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
    requested_week = request.args.get("week_end", "")
    try:
        reporting_week_end = date.fromisoformat(requested_week) if requested_week else None
    except ValueError:
        reporting_week_end = None
        flash("The requested reporting week was not recognised; the latest available week is shown.", "warning")
    return render_template("quality_control/portfolio_management_review.html", **portfolio_management_data(reporting_week_end))


@quality_control_bp.route("/management-review/presentation.pptx")
@login_required
@module_access_required("quality_control")
def download_portfolio_management_presentation():
    from app.core.services.qc_presentation import build_portfolio_management_presentation
    try:
        reporting_week_end = date.fromisoformat(request.args["week_end"]) if request.args.get("week_end") else None
    except ValueError:
        reporting_week_end = None
    try:
        output, filename = build_portfolio_management_presentation(current_app.static_folder, reporting_week_end)
    except ValueError as exc:
        flash(str(exc), "warning")
    except Exception:
        logger.exception("QC portfolio presentation export failed")
        flash("The portfolio presentation could not be generated. Please try again.", "danger")
    else:
        return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation", as_attachment=True, download_name=filename, max_age=0)
    return redirect(url_for("quality_control.portfolio_management_review"))


@quality_control_bp.route("/analytics")
@login_required
@module_access_required("quality_control")
def management_analytics():
    from app.core.services.quality_control import management_analytics_data
    return render_template("quality_control/management_analytics.html", **management_analytics_data())


@quality_control_bp.route("/management-report.pdf")
@login_required
@module_access_required("quality_control")
def download_management_report():
    root = Path(current_app.root_path).parent
    generator = root / "tmp" / "pdfs" / "generate_qc_weekly_management_report.py"
    output = root / "output" / "pdf" / "QC Portfolio Weekly Management Report.pdf"
    try:
        environment = {**__import__("os").environ, "PYTHONPATH": str(root)}
        subprocess.run([sys.executable, str(generator)], cwd=root, env=environment, check=True, timeout=120, capture_output=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        logger.exception("QC management PDF generation failed")
        flash("The management PDF could not be generated. Please try again.", "danger")
        return redirect(url_for("quality_control.portfolio_management_review"))
    return send_file(output, mimetype="application/pdf", as_attachment=True, download_name="QC Portfolio Weekly Management Report.pdf", max_age=0)


@quality_control_bp.route("/testing-standards", methods=["GET", "POST"])
@login_required
@module_access_required("quality_control")
def testing_standards():
    if not current_user.is_super_user():
        abort(403)
    from app.core.services.quality_control import import_testing_standards_workbook
    from app.models.quality_control.qc_testing_standard import QCTestingStandard
    if request.method == "POST":
        workbook = request.files.get("standards_workbook")
        if not workbook or not workbook.filename:
            flash("Select the testing-time standards workbook.", "warning")
        else:
            try:
                created, updated = import_testing_standards_workbook(workbook.read(), current_user.id)
                db.session.commit()
                flash(f"Testing standards updated: {created} new, {updated} revised.", "success")
            except ValueError as exc:
                db.session.rollback(); flash(str(exc), "danger")
        return redirect(url_for("quality_control.testing_standards"))
    return render_template("quality_control/testing_standards.html", standards=QCTestingStandard.query.order_by(QCTestingStandard.chemical_name).all())


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


@quality_control_bp.route("/labs/<lab_code>/performance-presentation.pptx")
@login_required
@module_access_required("quality_control")
def download_lab_presentation(lab_code: str):
    from app.core.services.qc_presentation import build_lab_performance_presentation
    try:
        output, filename = build_lab_performance_presentation(lab_code, current_app.static_folder)
    except ValueError as exc:
        flash(str(exc), "warning")
        return redirect(url_for("quality_control.laboratory_dashboard", lab_code=lab_code))
    except Exception:
        logger.exception("QC presentation export failed for lab=%s", lab_code)
        flash("The presentation could not be generated. Please try again.", "danger")
        return redirect(url_for("quality_control.laboratory_dashboard", lab_code=lab_code))
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation", as_attachment=True, download_name=filename, max_age=0)


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
