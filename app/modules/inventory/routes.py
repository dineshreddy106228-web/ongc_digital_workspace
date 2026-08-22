"""Routes for Inventory Monitoring."""
from __future__ import annotations

import logging
from datetime import date

from flask import abort, current_app, flash, redirect, render_template, request, send_file, session, url_for
from flask_login import current_user, login_required

from app.core.utils.decorators import module_access_required
from app.extensions import db
from app.modules.inventory import inventory_bp

logger = logging.getLogger(__name__)


def _reporting_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError("Reporting date must be in YYYY-MM-DD format.")


@inventory_bp.route("/")
@login_required
@module_access_required("inventory")
def landing():
    from app.core.services.inventory_monitoring import landing_data
    return render_template("inventory/landing.html", **landing_data())


@inventory_bp.route("/portfolio")
@login_required
@module_access_required("inventory")
def portfolio():
    from app.core.services.inventory_monitoring import portfolio_data
    selected = compare = None
    try:
        selected = _reporting_date(request.args.get("reporting_date"))
        compare = _reporting_date(request.args.get("compare_date"))
    except ValueError as exc:
        flash(str(exc), "warning")
    data = portfolio_data(selected, compare)
    if compare and data["previous_date"] != compare:
        flash("That comparison period is not published or is not earlier than the selected date; the closest earlier period is shown.", "warning")
    return render_template("inventory/portfolio.html", **data)


@inventory_bp.route("/management-review/presentation.pptx")
@login_required
@module_access_required("inventory")
def download_management_presentation():
    from app.core.services.inventory_presentation import build_management_review_presentation
    selected = compare = None
    try:
        selected = _reporting_date(request.args.get("reporting_date"))
        compare = _reporting_date(request.args.get("compare_date"))
    except ValueError:
        flash("The requested reporting date was not recognised; the latest published period is used.", "warning")
    try:
        output, filename = build_management_review_presentation(current_app.static_folder, selected, compare)
    except ValueError as exc:
        flash(str(exc), "warning")
    except Exception:
        logger.exception("Inventory management presentation export failed")
        flash("The management presentation could not be generated. Please try again or contact an administrator with the server log reference.", "danger")
    else:
        return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation", as_attachment=True, download_name=filename, max_age=0)
    return redirect(url_for("inventory.portfolio", reporting_date=selected.isoformat() if selected else None, compare_date=compare.isoformat() if compare else None))


@inventory_bp.route("/health")
@login_required
@module_access_required("inventory")
def health():
    from app.core.services.inventory_monitoring import inventory_health_data
    return render_template("inventory/health.html", **inventory_health_data())


@inventory_bp.route("/imports", methods=["GET", "POST"])
@login_required
@module_access_required("inventory")
def imports():
    from app.core.services.inventory_monitoring import (
        discard_staged_workbook, import_workbook, load_staged_workbook, stage_workbook, validate_workbook,
    )
    if request.method == "POST":
        try:
            if request.form.get("import_action") == "confirm":
                pending = session.get("inventory_monitoring_pending_import") or {}
                token = pending.get("token")
                if not token:
                    raise ValueError("This import review is no longer available. Upload the workbook again.")
                source = load_staged_workbook(token)
                selected_date = _reporting_date(request.form.get("reporting_date"))
                batch = import_workbook(source, pending["filename"], pending["source_group"], selected_date or _reporting_date(pending.get("reporting_date")), current_user.id)
                db.session.commit(); discard_staged_workbook(token); session.pop("inventory_monitoring_pending_import", None)
                flash(f"{batch.source_group} workbook imported with {batch.accepted_count} accepted rows.", "success")
                return redirect(url_for("inventory.import_history"))
            workbook = request.files.get("workbook")
            source_group = request.form.get("source_group", "")
            if not workbook or not workbook.filename:
                raise ValueError("Select a workbook to validate.")
            if not workbook.filename.lower().endswith((".xlsx", ".xls")):
                raise ValueError("Upload an Excel workbook (.xlsx or .xls).")
            source = workbook.read()
            review = validate_workbook(source, workbook.filename, source_group)
            token = stage_workbook(source)
            session["inventory_monitoring_pending_import"] = {"token": token, "filename": workbook.filename, "source_group": source_group, "reporting_date": review["reporting_date"].isoformat() if review.get("reporting_date") else None}
            return render_template("inventory/import_review.html", review=review, filename=workbook.filename, token=token)
        except ValueError as exc:
            db.session.rollback(); flash(str(exc), "danger")
        except Exception:
            db.session.rollback(); logger.exception("Inventory Monitoring import failed")
            flash("The import could not be completed. Please try again or contact an administrator with the server log reference.", "danger")
    return render_template("inventory/imports.html")


@inventory_bp.route("/import-history")
@login_required
@module_access_required("inventory")
def import_history():
    from app.models.inventory.monitoring import InventoryMonitoringUploadBatch
    batches = InventoryMonitoringUploadBatch.query.order_by(InventoryMonitoringUploadBatch.uploaded_at.desc()).limit(100).all()
    active_batches = [batch for batch in batches if not batch.is_superseded]
    primary_sources = (
        ("mapping", "Mapping baseline", "Work-centre and material mapping", "diagram-3"),
        ("09", "Group 09", "Oil well cement inventory", "boxes"),
        ("10", "Group 10", "Chemical and mud-chemical inventory", "beaker"),
    )
    latest_by_source = {}
    for batch in active_batches:
        if batch.source_group in {item[0] for item in primary_sources}:
            latest_by_source.setdefault(batch.source_group, batch)
    source_status = [
        {
            "key": key,
            "label": label,
            "description": description,
            "icon": icon,
            "batch": latest_by_source.get(key),
        }
        for key, label, description, icon in primary_sources
    ]
    inventory_dates = {}
    for batch in active_batches:
        if batch.source_group in {"09", "10"} and batch.reporting_date:
            inventory_dates.setdefault(batch.reporting_date, set()).add(batch.source_group)
    published_periods = sum(groups == {"09", "10"} for groups in inventory_dates.values())
    return render_template(
        "inventory/import_history.html",
        batches=batches,
        active_batches=active_batches,
        source_status=source_status,
        published_periods=published_periods,
        accepted_active_rows=sum(batch.accepted_count or 0 for batch in active_batches),
    )


@inventory_bp.route("/work-centres/<int:work_center_id>")
@login_required
@module_access_required("inventory")
def work_centre(work_center_id: int):
    from app.core.services.inventory_monitoring import work_center_review_data
    try:
        compare = _reporting_date(request.args.get("compare_date"))
    except ValueError as exc:
        flash(str(exc), "warning"); compare = None
    try:
        data = work_center_review_data(work_center_id, request.args.get("unit"), compare)
    except ValueError:
        abort(404)
    if compare and data["previous_date"] != compare:
        flash("That comparison date has no records for this work centre; the closest earlier date is shown.", "warning")
    return render_template("inventory/work_centre.html", **data)


@inventory_bp.route("/work-centres/<int:work_center_id>/presentation.pptx")
@login_required
@module_access_required("inventory")
def download_work_centre_presentation(work_center_id: int):
    from app.core.services.inventory_presentation import build_work_centre_review_presentation
    unit = request.args.get("unit")
    try:
        compare = _reporting_date(request.args.get("compare_date"))
    except ValueError:
        compare = None
    try:
        output, filename = build_work_centre_review_presentation(current_app.static_folder, work_center_id, unit, compare)
    except ValueError as exc:
        flash(str(exc), "warning")
    except Exception:
        logger.exception("Inventory work-centre presentation export failed for centre=%s", work_center_id)
        flash("The work-centre presentation could not be generated. Please try again or contact an administrator with the server log reference.", "danger")
    else:
        return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation", as_attachment=True, download_name=filename, max_age=0)
    return redirect(url_for("inventory.work_centre", work_center_id=work_center_id, unit=unit, compare_date=compare.isoformat() if compare else None))


@inventory_bp.route("/materials")
@login_required
@module_access_required("inventory")
def materials():
    term = (request.args.get("q") or "").strip()
    from app.core.services.inventory_monitoring import material_mapping_register_data
    return render_template(
        "inventory/materials.html",
        **material_mapping_register_data(term, request.args.get("category", "")),
    )


@inventory_bp.route("/materials/<int:material_id>")
@login_required
@module_access_required("inventory")
def material(material_id: int):
    from app.models.inventory.monitoring import InventoryMonitoringException, InventoryMonitoringMaterial, InventoryMonitoringRecord, InventoryMonitoringWorkCenter, InventoryMonitoringWorkCenterMaterial
    item = db.session.get(InventoryMonitoringMaterial, material_id)
    if item is None: abort(404)
    return render_template("inventory/material.html", material=item, records=InventoryMonitoringRecord.query.filter_by(material_id=item.id).order_by(InventoryMonitoringRecord.id.desc()).limit(300).all(), exceptions=InventoryMonitoringException.query.filter_by(material_id=item.id).order_by(InventoryMonitoringException.id.desc()).limit(100).all(), mappings=InventoryMonitoringWorkCenterMaterial.query.filter_by(material_id=item.id, is_current=True).join(InventoryMonitoringWorkCenter).order_by(InventoryMonitoringWorkCenter.zone, InventoryMonitoringWorkCenter.name).all())


@inventory_bp.route("/administration", methods=["GET", "POST"])
@login_required
@module_access_required("inventory")
def administration():
    if not current_user.is_super_user(): abort(403)
    from app.models.inventory.monitoring import InventoryMonitoringThreshold
    if request.method == "POST":
        try:
            for key in ("critical_low_stock_months", "low_stock_months", "slow_moving_months", "excess_stock_months"):
                value = request.form.get(key, "").strip()
                if value:
                    item = db.session.get(InventoryMonitoringThreshold, key) or InventoryMonitoringThreshold(key=key)
                    item.value, item.updated_by = value, current_user.id; db.session.add(item)
            db.session.commit(); flash("Monitoring thresholds updated.", "success")
        except Exception:
            db.session.rollback(); flash("Thresholds must be valid numbers.", "danger")
    thresholds = {item.key: item for item in InventoryMonitoringThreshold.query.all()}
    return render_template("inventory/administration.html", thresholds=thresholds)


@inventory_bp.route("/administration/fill-units", methods=["POST"])
@login_required
@module_access_required("inventory")
def fill_missing_units():
    if not current_user.is_super_user(): abort(403)
    from app.core.services.inventory_monitoring import backfill_missing_uom
    try:
        updated = backfill_missing_uom()
        db.session.commit()
        flash(
            f"{updated:,} stock lines were given a unit of measure from consumption history."
            if updated else "No further unit of measure could be resolved from consumption history.",
            "success" if updated else "warning",
        )
    except Exception:
        db.session.rollback(); logger.exception("Inventory unit-of-measure backfill failed")
        flash("Units of measure could not be filled. Please try again or contact an administrator with the server log reference.", "danger")
    return redirect(url_for("inventory.administration"))
