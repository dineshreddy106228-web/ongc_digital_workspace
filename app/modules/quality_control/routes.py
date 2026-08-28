"""Routes for the QC Laboratory Monitoring module."""

from __future__ import annotations

from functools import wraps
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


def _can_control_quality_monitoring() -> bool:
    """Only superusers may control SAP imports and QC-admin decisions."""
    return current_user.is_super_user()


def _can_record_lab_follow_up(lab_code: str) -> bool:
    """Return whether the user may record follow-up for this laboratory.

    Superusers have the full Quality Control scope. Standard users may view
    every laboratory dashboard, but can write returned details only for their
    single admin-assigned ``quality_control_lab_code``.
    """
    return bool(
        current_user.is_authenticated
        and current_user.has_module_access("quality_control")
        and (
            current_user.is_super_user()
            or current_user.quality_control_lab_code == lab_code
        )
    )


def _user_lab_scope() -> str | None:
    """The one laboratory this user works, or ``None`` for the full scope.

    A superuser reads Corporate Chemistry's whole portfolio.  Everybody else
    is scoped to the laboratory an admin assigned them; until that assignment
    exists they have no laboratory scope at all, which is not the same as
    having every laboratory.
    """
    if current_user.is_authenticated and current_user.is_super_user():
        return None
    return getattr(current_user, "quality_control_lab_code", None) or ""


def _can_view_laboratory(lab_code: str) -> bool:
    """Whether this user may open a laboratory's own monitoring screens."""
    scope = _user_lab_scope()
    return scope is None or scope == lab_code


def laboratory_view_required(view):
    """Guard a laboratory's screens against readers from another laboratory."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        lab_code = kwargs.get("lab_code") or (args[0] if args else None)
        if not lab_code or not _can_view_laboratory(str(lab_code)):
            flash("You may open only your own laboratory's monitoring views.", "danger")
            abort(403)
        return view(*args, **kwargs)
    return wrapped


def quality_control_admin_required(view):
    """Guard central SAP imports and QC-admin monitoring decisions."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not _can_control_quality_monitoring():
            flash("This action is restricted to Corporate Chemistry Quality Control admins.", "danger")
            abort(403)
        return view(*args, **kwargs)
    return wrapped


def laboratory_follow_up_required(view):
    """Guard laboratory-returned data without granting control-tower rights."""
    @wraps(view)
    def wrapped(*args, **kwargs):
        lab_code = kwargs.get("lab_code") or (args[0] if args else None)
        if not lab_code or not _can_record_lab_follow_up(str(lab_code)):
            flash("You may record laboratory follow-up only for your assigned laboratory.", "danger")
            abort(403)
        return view(*args, **kwargs)
    return wrapped


@quality_control_bp.route("/")
@login_required
@module_access_required("quality_control")
def landing():
    from app.core.services.quality_control import (
        current_monitoring_day, laboratory_landing_data, laboratory_navigator_data,
    )
    from app.core.services.sap_quality_control import sap_open_counts_by_lab
    monitoring_day = current_monitoring_day()
    laboratories = laboratory_landing_data()
    designated_laboratories = [
        laboratory for laboratory in laboratories
        if laboratory.get("is_additional_designated")
    ]
    scope = _user_lab_scope()
    return render_template(
        "quality_control/landing.html",
        laboratories=laboratories,
        designated_laboratories=designated_laboratories,
        mapped_laboratory_total=len(laboratories) - len(designated_laboratories),
        map_laboratories=laboratory_navigator_data(
            laboratories, monitoring_day["date"],
            scope_lab_code=scope, sap_open_counts=sap_open_counts_by_lab(),
        ),
        monitoring_day=monitoring_day,
        is_superuser=current_user.is_super_user(),
    )


@quality_control_bp.route("/data-import")
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def data_import():
    """SAP control-tower entry point plus workbook fallback laboratories."""
    from app.core.services.quality_control import current_monitoring_day, laboratory_import_targets
    from app.core.services.sap_quality_control import SAP_REPLACED_WEEKLY_LAB_CODES, sap_control_data
    sap_data = sap_control_data()
    workbook_fallback_laboratories = [
        laboratory for laboratory in laboratory_import_targets()
        if laboratory["code"] not in SAP_REPLACED_WEEKLY_LAB_CODES
    ]
    return render_template(
        "quality_control/data_import.html",
        laboratories=workbook_fallback_laboratories,
        monitoring_day=current_monitoring_day(),
        sap_control_cards=sap_data["control_cards"],
        sap_laboratories=sap_data["sap_laboratories"],
        sap_plant_mappings=sap_data["sap_plant_mappings"],
        can_control=_can_control_quality_monitoring(),
    )


@quality_control_bp.route("/idwe-imports")
@login_required
@module_access_required("quality_control")
def idwe_imports():
    """Superseded by the module-wide import centre; kept so old links still land."""
    return redirect(url_for("quality_control.data_import"))


@quality_control_bp.route("/labs/<lab_code>", methods=["GET", "POST"])
@login_required
@module_access_required("quality_control")
@laboratory_view_required
def laboratory_dashboard(lab_code: str):
    # RGL and IDWE views are driven by their native SAP daily exports. Historic
    # local records remain available as the fallback source for other labs.
    from app.core.services.sap_quality_control import SAP_REPORTING_LAB_CODES
    if lab_code in SAP_REPORTING_LAB_CODES:
        return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code))
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
                flash(f"{batch.lab_name}: local workbook data {action}. {batch.imported_count} new and {batch.updated_count} existing samples processed.", "success")
                return redirect(url_for("quality_control.laboratory_dashboard", lab_code=lab_code))
            workbook = request.files.get("qc_workbook")
            if not workbook or not workbook.filename:
                flash("Select the completed local status workbook before importing.", "warning")
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


@quality_control_bp.route("/sap-panvel")
@login_required
@module_access_required("quality_control")
def sap_panvel_dashboard():
    """Retain the original Panvel URL while routing it to the control-tower view."""
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code="rgl_panvel"))


@quality_control_bp.route("/sap-control")
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def sap_control():
    """Corporate Chemistry's central daily SAP and exception register."""
    from app.core.services.sap_quality_control import sap_control_data
    try:
        return render_template(
            "quality_control/sap_control.html",
            can_control=_can_control_quality_monitoring(),
            **sap_control_data(),
        )
    except Exception:
        logger.exception("SAP QC control tower could not be loaded")
        flash("The SAP quality control view could not be loaded. Please try again.", "danger")
        return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/sap-control/labs/<lab_code>")
@login_required
@module_access_required("quality_control")
@laboratory_view_required
def sap_lab_dashboard(lab_code: str):
    from app.core.services.sap_quality_control import sap_lab_dashboard_data
    try:
        return render_template(
            "quality_control/sap_panvel_dashboard.html",
            can_control=_can_control_quality_monitoring(),
            can_record_lab_updates=_can_record_lab_follow_up(lab_code),
            **sap_lab_dashboard_data(lab_code),
        )
    except ValueError as exc:
        flash(str(exc), "warning")
    except Exception:
        logger.exception("SAP QC dashboard load failed for lab=%s", lab_code)
        flash("The SAP quality dashboard could not be loaded. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_control"))


def _import_sap_snapshot(lab_code: str):
    from app.core.services.sap_quality_control import (
        SAP_CENTRAL_UPLOAD_CODE,
        import_central_sap_exports,
        import_sap_lab_exports,
    )
    inspection = request.files.get("inspection_workbook")
    notifications = request.files.get("notification_workbook")
    if not inspection or not inspection.filename or not notifications or not notifications.filename:
        raise ValueError("Select both native SAP exports: Inspection Lots and Notifications.")
    supported = (".xlsx", ".xls")
    if not inspection.filename.lower().endswith(supported) or not notifications.filename.lower().endswith(supported):
        raise ValueError("Upload the native SAP Excel exports (.xlsx or .xls).")
    inspection_source = inspection.read()
    notification_source = notifications.read()
    if lab_code == SAP_CENTRAL_UPLOAD_CODE:
        return import_central_sap_exports(
            inspection_source, inspection.filename, notification_source, notifications.filename, current_user.id,
        )
    return import_sap_lab_exports(
        lab_code, inspection_source, inspection.filename, notification_source, notifications.filename, current_user.id,
    )


@quality_control_bp.route("/sap-control/import", methods=["POST"])
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def import_sap_control_exports():
    from app.core.services.sap_quality_control import SAP_CENTRAL_UPLOAD_CODE

    lab_code = (request.form.get("lab_code") or SAP_CENTRAL_UPLOAD_CODE).strip()
    try:
        snapshot = _import_sap_snapshot(lab_code)
        db.session.commit()
        if lab_code == SAP_CENTRAL_UPLOAD_CODE:
            batches = snapshot
            laboratory_count = len(batches)
            record_count = sum(batch.record_count for batch in batches)
            as_of_date = max(batch.as_of_date for batch in batches)
            flash(
                f"Central SAP snapshot for {as_of_date:%d %b %Y} is now live: "
                f"{laboratory_count} laboratories and {record_count} monitoring records refreshed.",
                "success sap-import-completed",
            )
            return redirect(url_for("quality_control.sap_control"))
        batch = snapshot
        flash(
            f"{batch.lab_code.replace('_', ' ').title()} SAP snapshot for {batch.as_of_date:%d %b %Y} is now live: "
            f"{batch.record_count} monitoring records refreshed.", "success sap-import-completed",
        )
        return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code))
    except ValueError as exc:
        db.session.rollback(); flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        logger.exception("SAP QC import failed for lab=%s", lab_code)
        flash("The SAP exports could not be imported. Confirm both files are from the same daily run.", "danger")
    return redirect(url_for("quality_control.sap_control"))


@quality_control_bp.route("/sap-panvel/import", methods=["POST"])
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def import_sap_panvel_exports():
    """Compatibility endpoint for the former Panvel-only upload form."""
    try:
        _import_sap_snapshot("rgl_panvel")
        db.session.commit()
        flash("RGL Panvel SAP snapshot is now live.", "success sap-import-completed")
    except ValueError as exc:
        db.session.rollback(); flash(str(exc), "danger")
    except Exception:
        db.session.rollback(); logger.exception("SAP QC import failed for RGL Panvel")
        flash("The SAP exports could not be imported. Please confirm both reports are from the same daily run.", "danger")
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code="rgl_panvel"))


@quality_control_bp.route("/sap-control/labs/<lab_code>/records/<int:record_id>/lab-update", methods=["POST"])
@login_required
@module_access_required("quality_control")
@laboratory_follow_up_required
def save_sap_lab_update(lab_code: str, record_id: int):
    from app.core.services.sap_quality_control import create_sap_lab_update
    try:
        create_sap_lab_update(record_id, request.form, current_user.id, lab_code=lab_code)
        db.session.commit()
        flash("Returned laboratory follow-up saved. SAP status remains unchanged until the next SAP export confirms it.", "success")
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        logger.exception("SAP QC lab update failed for lab=%s record=%s", lab_code, record_id)
        flash("The laboratory follow-up could not be saved. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code, focus=record_id) + f"#sap-record-{record_id}")


@quality_control_bp.route("/sap-panvel/records/<int:record_id>/lab-update", methods=["POST"])
@login_required
@module_access_required("quality_control")
def save_sap_panvel_lab_update(record_id: int):
    return save_sap_lab_update("rgl_panvel", record_id)


@quality_control_bp.route("/sap-control/labs/<lab_code>/records/<int:record_id>/exclude", methods=["POST"])
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def exclude_sap_record_from_monitoring(lab_code: str, record_id: int):
    from app.core.services.sap_quality_control import exclude_sap_record_from_monitoring as exclude_record
    try:
        disposition = exclude_record(record_id, request.form, current_user.id, lab_code=lab_code)
        db.session.commit()
        flash(
            "SAP notification excluded from the active monitoring list. The SAP record and the QC-admin decision remain in the audit register.",
            "success",
        )
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        logger.exception("SAP monitoring exclusion failed for lab=%s record=%s", lab_code, record_id)
        flash("The SAP notification could not be excluded from monitoring. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code) + "#excluded-sap-records")


@quality_control_bp.route("/sap-control/labs/<lab_code>/records/<int:record_id>/reinstate", methods=["POST"])
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def reinstate_sap_record_for_monitoring(lab_code: str, record_id: int):
    from app.core.services.sap_quality_control import reinstate_sap_record_for_monitoring as reinstate_record
    try:
        reinstate_record(record_id, current_user.id, lab_code=lab_code)
        db.session.commit()
        flash("SAP notification reinstated to the active monitoring list.", "success")
    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
    except Exception:
        db.session.rollback()
        logger.exception("SAP monitoring reinstatement failed for lab=%s record=%s", lab_code, record_id)
        flash("The SAP notification could not be reinstated. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code) + f"#sap-record-{record_id}")


@quality_control_bp.route("/sap-control/labs/<lab_code>/presentation.pptx")
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def download_sap_lab_presentation(lab_code: str):
    from app.core.services.qc_presentation import build_sap_lab_presentation
    try:
        output, filename = build_sap_lab_presentation(lab_code, current_app.static_folder)
    except ValueError as exc:
        flash(str(exc), "warning")
    except Exception:
        logger.exception("SAP QC presentation export failed for lab=%s", lab_code)
        flash("The SAP management presentation could not be generated. Please try again.", "danger")
    else:
        return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation", as_attachment=True, download_name=filename, max_age=0)
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code))


@quality_control_bp.route("/sap-panvel/presentation.pptx")
@login_required
@module_access_required("quality_control")
def download_sap_panvel_presentation():
    return download_sap_lab_presentation("rgl_panvel")


@quality_control_bp.route("/sap-control/labs/<lab_code>/uploads/<int:batch_id>/<source_kind>")
@login_required
@module_access_required("quality_control")
def download_sap_lab_source(lab_code: str, batch_id: int, source_kind: str):
    from sqlalchemy.orm import undefer
    from app.models.quality_control.qc_sap_monitoring import QCSAPUploadBatch

    if source_kind not in {"inspection", "notifications"}:
        abort(404)
    column = (
        QCSAPUploadBatch.inspection_source_data
        if source_kind == "inspection" else QCSAPUploadBatch.notification_source_data
    )
    batch = QCSAPUploadBatch.query.options(undefer(column)).filter_by(id=batch_id, lab_code=lab_code).first()
    if batch is None:
        flash("The requested SAP source export is no longer available.", "warning")
        return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code))
    data = batch.inspection_source_data if source_kind == "inspection" else batch.notification_source_data
    if batch.source_purged_at is not None or not data:
        flash(
            "The source workbook is outside the 15-day rollback window. "
            "Its SAP audit record remains available.",
            "warning",
        )
        return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code))
    filename = batch.inspection_filename if source_kind == "inspection" else batch.notification_filename
    content_type = batch.inspection_content_type if source_kind == "inspection" else batch.notification_content_type
    return send_file(BytesIO(data), mimetype=content_type, as_attachment=True, download_name=filename, max_age=0)


@quality_control_bp.route("/sap-panvel/uploads/<int:batch_id>/<source_kind>")
@login_required
@module_access_required("quality_control")
def download_sap_panvel_source(batch_id: int, source_kind: str):
    return download_sap_lab_source("rgl_panvel", batch_id, source_kind)


@quality_control_bp.route("/sap-control/non-sap", methods=["POST"])
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def create_controlled_non_sap_sample():
    from app.core.services.sap_quality_control import create_non_sap_sample
    try:
        sample = create_non_sap_sample(request.form, current_user.id)
        db.session.commit()
        flash(f"Non-SAP sample {sample.sample_reference} added to the controlled exception register.", "success")
    except ValueError as exc:
        db.session.rollback(); flash(str(exc), "danger")
    except Exception:
        db.session.rollback(); logger.exception("Non-SAP QC sample creation failed")
        flash("The non-SAP sample could not be added. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_control") + "#non-sap-register")


@quality_control_bp.route("/sap-control/non-sap/<int:sample_id>/update", methods=["POST"])
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def update_controlled_non_sap_sample(sample_id: int):
    from app.core.services.sap_quality_control import update_non_sap_sample
    try:
        update_non_sap_sample(sample_id, request.form, current_user.id)
        db.session.commit()
        flash("Non-SAP sample follow-up saved in the Corporate Chemistry register.", "success")
    except ValueError as exc:
        db.session.rollback(); flash(str(exc), "danger")
    except Exception:
        db.session.rollback(); logger.exception("Non-SAP QC sample update failed for id=%s", sample_id)
        flash("The non-SAP sample update could not be saved. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_control") + "#non-sap-register")


@quality_control_bp.route("/sap-control/labs/<lab_code>/non-sap", methods=["POST"])
@login_required
@module_access_required("quality_control")
@laboratory_follow_up_required
def create_lab_non_sap_sample(lab_code: str):
    """Let a reporting laboratory log work that has no SAP record."""
    from app.core.services.sap_quality_control import create_non_sap_sample, get_sap_reporting_laboratory
    try:
        get_sap_reporting_laboratory(lab_code)
        form = request.form.to_dict()
        form["lab_code"] = lab_code
        sample = create_non_sap_sample(form, current_user.id)
        db.session.commit()
        flash(f"Non-SAP sample {sample.sample_reference} added to this laboratory's exception register.", "success")
    except ValueError as exc:
        db.session.rollback(); flash(str(exc), "danger")
    except Exception:
        db.session.rollback(); logger.exception("Non-SAP QC sample creation failed for lab=%s", lab_code)
        flash("The non-SAP sample could not be added. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code) + "#non-sap-register")


@quality_control_bp.route("/sap-control/labs/<lab_code>/non-sap/<int:sample_id>/update", methods=["POST"])
@login_required
@module_access_required("quality_control")
@laboratory_follow_up_required
def update_lab_non_sap_sample(lab_code: str, sample_id: int):
    """Keep a laboratory's own non-SAP exception status auditable."""
    from app.core.services.sap_quality_control import get_sap_reporting_laboratory, update_non_sap_sample
    try:
        get_sap_reporting_laboratory(lab_code)
        update_non_sap_sample(sample_id, request.form, current_user.id, lab_code=lab_code)
        db.session.commit()
        flash("Non-SAP sample follow-up saved.", "success")
    except ValueError as exc:
        db.session.rollback(); flash(str(exc), "danger")
    except Exception:
        db.session.rollback(); logger.exception("Non-SAP QC sample update failed for lab=%s id=%s", lab_code, sample_id)
        flash("The non-SAP sample update could not be saved. Please try again.", "danger")
    return redirect(url_for("quality_control.sap_lab_dashboard", lab_code=lab_code) + "#non-sap-register")


@quality_control_bp.route("/history")
@login_required
@module_access_required("quality_control")
def sample_history():
    """The SAP register, read at the scope the reader actually works.

    Corporate Chemistry reads every reporting laboratory.  A laboratory user
    reads their own, and the requested ``lab`` is ignored rather than being
    honoured or rejected — the register is one screen at two scopes, not a
    corporate screen with a filter a laboratory could widen.
    """
    from app.core.services.sap_quality_control import (
        SAP_REGISTER_STATUS_FILTERS, sap_sample_register_data,
    )

    scope = _user_lab_scope()
    lab_code = (request.args.get("lab") or "").strip() if scope is None else scope
    # Accept the former ``chemical`` key once so existing bookmarks still find
    # their material in the SAP register after the source migration.
    search = (request.args.get("search") or request.args.get("chemical") or "").strip()
    status = (request.args.get("status") or "").strip()
    subgroup = (request.args.get("subgroup") or "").strip()
    if scope == "":
        # No laboratory assigned yet is an empty scope, not the whole portfolio.
        return render_template(
            "quality_control/samples.html",
            filters={"lab": "", "search": "", "status": "", "subgroup": ""},
            entries=[], groups=[], laboratories=[], subgroup_filters=[],
            status_filters=SAP_REGISTER_STATUS_FILTERS,
            can_view_all_laboratories=False, has_lab_scope=False,
        )
    try:
        register = sap_sample_register_data(lab_code, search, status, subgroup)
        return render_template(
            "quality_control/samples.html",
            filters={"lab": lab_code, "search": search, "status": status, "subgroup": subgroup},
            can_view_all_laboratories=scope is None, has_lab_scope=True,
            **register,
        )
    except ValueError:
        # Workbook fallback laboratories retain their lab-scoped archive. They
        # do not belong in the module-wide SAP register.
        if lab_code:
            return redirect(url_for(
                "quality_control.samples", lab_code=lab_code,
                chemical=request.args.get("chemical") or "",
                specification=request.args.get("specification") or "",
                status=status if status in {"pass", "fail", "under_testing", "report_issued"} else "",
            ))
        return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/management-review")
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def portfolio_management_review():
    from app.core.services.sap_quality_control import sap_management_data
    return render_template("quality_control/portfolio_management_review.html", **sap_management_data())


@quality_control_bp.route("/management-review/presentation.pptx")
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def download_portfolio_management_presentation():
    from app.core.services.qc_presentation import build_sap_portfolio_management_presentation
    from app.core.services.sap_quality_control import SAP_REPORTING_LAB_CODES
    lab_codes = None
    requested_lab_codes = [code for code in request.args.getlist("lab") if code]
    # The presentation selector submits one ``lab`` value for a single-lab
    # deck and no value for the all-laboratories deck.  Keep ``scope=labs``
    # compatible with existing bookmarked filtered-deck links.
    if request.args.get("scope") == "labs" or requested_lab_codes:
        lab_codes = {code for code in requested_lab_codes if code in SAP_REPORTING_LAB_CODES}
        if not lab_codes:
            flash("Select at least one SAP laboratory, or choose the all-SAP deck.", "warning")
            return redirect(url_for("quality_control.portfolio_management_review"))
    try:
        output, filename = build_sap_portfolio_management_presentation(current_app.static_folder, lab_codes)
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
@quality_control_admin_required
def management_analytics():
    from app.core.services.sap_quality_control import sap_management_data
    return render_template("quality_control/management_analytics.html", **sap_management_data())


@quality_control_bp.route("/management-report.pdf")
@login_required
@module_access_required("quality_control")
@quality_control_admin_required
def download_management_report():
    root = Path(current_app.root_path).parent
    generator = root / "tmp" / "pdfs" / "generate_qc_weekly_management_report.py"
    output = root / "output" / "pdf" / "QC Portfolio Monitoring Report.pdf"
    try:
        environment = {**__import__("os").environ, "PYTHONPATH": str(root)}
        subprocess.run([sys.executable, str(generator)], cwd=root, env=environment, check=True, timeout=120, capture_output=True)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        # OSError covers a missing generator script or interpreter, which would
        # otherwise surface as an unhandled 500 rather than a flash.
        logger.exception("QC management PDF generation failed")
        flash("The management PDF could not be generated. Please try again.", "danger")
        return redirect(url_for("quality_control.portfolio_management_review"))
    return send_file(output, mimetype="application/pdf", as_attachment=True, download_name="QC Portfolio Monitoring Report.pdf", max_age=0)


@quality_control_bp.route("/testing-standards", methods=["GET", "POST"])
@login_required
@module_access_required("quality_control")
def testing_standards():
    """Standard Testing Times. Everyone with module access may read them; only a
    superuser may change them, and every change is recorded."""
    from app.core.services.administration import (
        administration_trail, can_edit_administration, record_admin_change,
    )
    from app.core.services.quality_control import import_testing_standards_workbook
    from app.models.quality_control.qc_testing_standard import QCTestingStandard

    can_edit = can_edit_administration()
    if request.method == "POST":
        if not can_edit:
            abort(403)
        workbook = request.files.get("standards_workbook")
        if not workbook or not workbook.filename:
            flash("Select the testing-time standards workbook.", "warning")
        else:
            try:
                created, updated = import_testing_standards_workbook(workbook.read(), current_user.id)
                record_admin_change(
                    "quality_control",
                    f"Standard Testing Times imported from '{workbook.filename}' — "
                    f"{created} new, {updated} revised.",
                    entity_id="testing-standards", ip_address=request.remote_addr or "",
                )
                db.session.commit()
                flash(f"Testing standards updated: {created} new, {updated} revised.", "success")
            except ValueError as exc:
                db.session.rollback(); flash(str(exc), "danger")
        return redirect(url_for("quality_control.testing_standards"))
    return render_template(
        "quality_control/testing_standards.html",
        standards=QCTestingStandard.query.order_by(QCTestingStandard.chemical_name).all(),
        can_edit=can_edit, admin_trail=administration_trail("quality_control"),
    )


@quality_control_bp.route("/labs/<lab_code>/samples")
@login_required
@module_access_required("quality_control")
def samples(lab_code: str):
    """One laboratory's own sample register, inside that laboratory's navigation.

    The cross-laboratory page answers a different question; sending a reader
    there filtered dropped them out of the lab they were working in.
    """
    from app.core.services.quality_control import (
        get_laboratory, history_filter_options, latest_dashboard_data, search_samples,
    )
    chemical_name = (request.args.get("chemical") or "").strip()
    specification_no = (request.args.get("specification") or "").strip()
    status = (request.args.get("status") or "").strip()
    try:
        laboratory = get_laboratory(lab_code)
        options = history_filter_options(lab_code)
        return render_template(
            "quality_control/lab_samples.html",
            laboratory=laboratory,
            batch=latest_dashboard_data(lab_code)["batch"],
            samples=search_samples(lab_code, chemical_name, specification_no, status),
            filters={"chemical": chemical_name, "specification": specification_no, "status": status},
            chemicals=options["chemicals"],
        )
    except ValueError:
        return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/labs/<lab_code>/management-brief")
@login_required
@module_access_required("quality_control")
def management_brief(lab_code: str):
    from app.core.services.quality_control import latest_dashboard_data
    try:
        return render_template("quality_control/management_brief.html", **latest_dashboard_data(lab_code))
    except ValueError:
        return redirect(url_for("quality_control.landing"))


@quality_control_bp.route("/labs/<lab_code>/management-brief.pptx")
@login_required
@module_access_required("quality_control")
def download_brief_presentation(lab_code: str):
    from app.core.services.qc_presentation import build_lab_brief_presentation
    try:
        output, filename = build_lab_brief_presentation(lab_code, current_app.static_folder)
    except ValueError as exc:
        flash(str(exc), "warning")
        return redirect(url_for("quality_control.management_brief", lab_code=lab_code))
    except Exception:
        logger.exception("QC management brief export failed for lab=%s", lab_code)
        flash("The presentation could not be generated. Please try again.", "danger")
        return redirect(url_for("quality_control.management_brief", lab_code=lab_code))
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation", as_attachment=True, download_name=filename, max_age=0)


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
        flash("The requested source workbook was not found.", "warning")
        return redirect(url_for("quality_control.landing"))
    if batch.source_purged_at is not None or not batch.source_data:
        flash(
            "The source workbook is outside the 15-day rollback window. "
            "Its imported QC records remain available.",
            "warning",
        )
        return redirect(url_for("quality_control.landing"))
    return send_file(BytesIO(batch.source_data), mimetype=batch.source_content_type, as_attachment=True, download_name=batch.source_filename, max_age=0)
