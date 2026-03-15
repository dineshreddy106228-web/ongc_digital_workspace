"""Inventory Intelligence Module – Routes.

Endpoints:
    GET  /inventory/                          → Dashboard (KPI + interactive materials table)
    GET  /inventory/api/materials              → JSON: all materials for table
    GET  /inventory/api/consumption/<material> → JSON: monthly consumption detail
    GET  /inventory/api/procurement/<material> → JSON: procurement history
    GET  /inventory/api/chart/<material>       → JSON: chart data for material
"""

from __future__ import annotations

import json
import logging

from flask import flash, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_login import login_required

from app.inventory import inventory_bp
from app.services.inventory_intelligence import (
    get_data_store,
    get_seed_upload_schema,
)
from app.services.inventory_seed_audit import (
    LOW_VALUE_THRESHOLD,
    apply_staged_seed_upload,
    create_staged_seed_upload,
    delete_rows_from_staged_seed_upload,
    delete_procurement_rows_below_threshold,
    get_procurement_rows_below_threshold,
    get_staged_seed_report,
    ignore_rows_from_staged_seed_upload,
    normalize_price_unit_rows_in_staged_seed_upload,
    discard_staged_seed_upload,
)
from app.utils.decorators import module_access_required

logger = logging.getLogger(__name__)


def _get_pending_token() -> str | None:
    return session.get("inventory_seed_pending_token")


def _set_pending_token(token: str | None) -> None:
    if token:
        session["inventory_seed_pending_token"] = token
    else:
        session.pop("inventory_seed_pending_token", None)


def _render_seed_files_page(
    audit_report: dict | None = None,
    upload_error: str | None = None,
    pending_token: str | None = None,
):
    token = pending_token if pending_token is not None else _get_pending_token()
    if pending_token is not None:
        _set_pending_token(pending_token)
    if token and audit_report is None:
        try:
            audit_report = get_staged_seed_report(token)
        except ValueError:
            _set_pending_token(None)
            token = None
    return render_template(
        "inventory/seed_upload.html",
        schema=get_seed_upload_schema(),
        audit_report=audit_report,
        upload_error=upload_error,
        cleanup_preview=get_procurement_rows_below_threshold(LOW_VALUE_THRESHOLD),
        cleanup_threshold=LOW_VALUE_THRESHOLD,
        pending_token=token,
    )


# ── Dashboard ─────────────────────────────────────────────────────────────────

@inventory_bp.route("/")
@login_required
@module_access_required("inventory")
def index():
    """Inventory Intelligence dashboard — KPIs + interactive materials table."""
    store = get_data_store()
    payload = store.get_dashboard_payload()

    return render_template(
        "inventory/dashboard.html",
        kpis=payload["kpis"],
        materials_count=len(payload["materials"]),
        dashboard_json=json.dumps(payload, default=str),
        filter_options=payload["filters"],
    )


@inventory_bp.route("/seed-files", methods=["GET", "POST"])
@login_required
@module_access_required("inventory")
def seed_files():
    """Upload the paired seed workbooks used by inventory intelligence."""
    if request.method == "POST":
        previous_token = _get_pending_token()
        consumption_file = request.files.get("consumption_seed")
        procurement_file = request.files.get("procurement_seed")

        if not consumption_file or not consumption_file.filename:
            return _render_seed_files_page(upload_error="Select the consumption seed file before uploading.")
        if not procurement_file or not procurement_file.filename:
            return _render_seed_files_page(upload_error="Select the procurement seed file before uploading.")

        consumption_bytes = consumption_file.read()
        procurement_bytes = procurement_file.read()

        try:
            if previous_token:
                discard_staged_seed_upload(previous_token)
                _set_pending_token(None)
            token, audit_report = create_staged_seed_upload(
                consumption_bytes=consumption_bytes,
                consumption_filename=consumption_file.filename,
                procurement_bytes=procurement_bytes,
                procurement_filename=procurement_file.filename,
            )
            if audit_report["has_failures"]:
                _set_pending_token(token)
                return _render_seed_files_page(
                    audit_report=audit_report,
                    upload_error="Sanity Check Failed. Please scroll down to review and clean the flagged rows.",
                    pending_token=token,
                )

            apply_staged_seed_upload(token)
            _set_pending_token(None)
        except ValueError as exc:
            return _render_seed_files_page(upload_error=str(exc))
        except Exception:
            logger.exception("Failed to upload inventory seed workbooks")
            return _render_seed_files_page(upload_error="Could not update the seed files. Check the files and try again.")

        flash("Seed files updated. Inventory analysis has been reloaded from the uploaded workbooks.", "success")
        return redirect(url_for("inventory.index"))

    return _render_seed_files_page()


@inventory_bp.route("/seed-files/staged-delete", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_delete_rows():
    token = request.form.get("pending_token") or _get_pending_token()
    seed_type = request.form.get("seed_type", "").strip().lower()
    finding_check = request.form.get("finding_check", "").strip()
    excel_rows = request.form.getlist("excel_rows")
    if not token:
        return _render_seed_files_page(upload_error="The staged upload is no longer available. Upload the files again.")

    try:
        report = delete_rows_from_staged_seed_upload(token, seed_type, finding_check, [int(row) for row in excel_rows])
        _set_pending_token(token)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc), pending_token=token)
    except Exception:
        logger.exception("Failed to delete selected staged seed rows")
        return _render_seed_files_page(upload_error="Could not delete the selected staged rows.", pending_token=token)

    return _render_seed_files_page(audit_report=report, pending_token=token)


@inventory_bp.route("/seed-files/staged-keep", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_keep_rows():
    token = request.form.get("pending_token") or _get_pending_token()
    seed_type = request.form.get("seed_type", "").strip().lower()
    finding_check = request.form.get("finding_check", "").strip()
    excel_rows = request.form.getlist("excel_rows")
    if not token:
        return _render_seed_files_page(upload_error="The staged upload is no longer available. Upload the files again.")

    try:
        report = ignore_rows_from_staged_seed_upload(token, seed_type, finding_check, [int(row) for row in excel_rows])
        _set_pending_token(token)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc), pending_token=token)
    except Exception:
        logger.exception("Failed to keep selected staged seed rows")
        return _render_seed_files_page(upload_error="Could not keep the selected staged rows.", pending_token=token)

    return _render_seed_files_page(audit_report=report, pending_token=token)


@inventory_bp.route("/seed-files/staged-normalize-price-unit", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_normalize_price_unit():
    token = request.form.get("pending_token") or _get_pending_token()
    excel_rows = request.form.getlist("excel_rows")
    if not token:
        return _render_seed_files_page(upload_error="The staged upload is no longer available. Upload the files again.")

    try:
        report = normalize_price_unit_rows_in_staged_seed_upload(token, [int(row) for row in excel_rows])
        _set_pending_token(token)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc), pending_token=token)
    except Exception:
        logger.exception("Failed to normalize selected staged price-unit rows")
        return _render_seed_files_page(upload_error="Could not normalize the selected price-unit rows.", pending_token=token)

    return _render_seed_files_page(audit_report=report, pending_token=token)


@inventory_bp.route("/seed-files/staged-apply", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_apply():
    token = request.form.get("pending_token") or _get_pending_token()
    if not token:
        return _render_seed_files_page(upload_error="The staged upload is no longer available. Upload the files again.")

    try:
        report = get_staged_seed_report(token)
        if report["has_failures"]:
            return _render_seed_files_page(
                audit_report=report,
                upload_error="Sanity Check Failed. Please scroll down to review and clean the flagged rows.",
                pending_token=token,
            )
        apply_staged_seed_upload(token)
        _set_pending_token(None)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc))
    except Exception:
        logger.exception("Failed to apply staged seed upload")
        return _render_seed_files_page(upload_error="Could not apply the staged seed files.", pending_token=token)

    flash("Cleaned staged seed files applied. Inventory analysis has been reloaded.", "success")
    return redirect(url_for("inventory.index"))


@inventory_bp.route("/seed-files/staged-discard", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_discard():
    token = request.form.get("pending_token") or _get_pending_token()
    if token:
        discard_staged_seed_upload(token)
    _set_pending_token(None)
    flash("Staged seed upload discarded.", "info")
    return redirect(url_for("inventory.seed_files"))


@inventory_bp.route("/seed-files/procurement-low-value-delete", methods=["POST"])
@login_required
@module_access_required("inventory")
def procurement_low_value_delete():
    """Delete currently ingested procurement rows below the configured value threshold."""
    try:
        preview = delete_procurement_rows_below_threshold(LOW_VALUE_THRESHOLD)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc))
    except Exception:
        logger.exception("Failed to delete low-value procurement rows")
        return _render_seed_files_page(upload_error="Could not delete the low-value procurement rows.")

    flash(
        f"Deleted {preview['count']} procurement row(s) below ₹ {LOW_VALUE_THRESHOLD:,.2f} from the ingested seed file.",
        "success",
    )
    return redirect(url_for("inventory.seed_files"))


# ── Demand Forecast submodule ──────────────────────────────────────────────────

@inventory_bp.route("/forecast")
@login_required
@module_access_required("inventory")
def forecast_page():
    """Demand Forecast submodule — probabilistic 6-month P5/P10/P50/P90/P95 view."""
    return render_template("inventory/forecast.html")


# ── API: Filtered dashboard payload ───────────────────────────────────────────

@inventory_bp.route("/api/overview")
@login_required
@module_access_required("inventory")
def api_overview():
    """Return the filtered dashboard payload."""
    store = get_data_store()
    plant = request.args.get("plant")
    return jsonify(store.get_dashboard_payload(plant=plant))


# ── API: Materials list ───────────────────────────────────────────────────────

@inventory_bp.route("/api/materials")
@login_required
@module_access_required("inventory")
def api_materials():
    """Return all materials as JSON for client-side table rendering."""
    store = get_data_store()
    plant = request.args.get("plant")
    return jsonify(store.get_materials_table(plant=plant))


# ── API: Monthly consumption detail ──────────────────────────────────────────

@inventory_bp.route("/api/consumption/<path:material>")
@login_required
@module_access_required("inventory")
def api_consumption(material: str):
    """Return monthly consumption breakdown for a specific material."""
    store = get_data_store()
    plant = request.args.get("plant")
    detail = store.get_monthly_consumption_detail(material, plant=plant)
    return jsonify(detail)


# ── API: Procurement history ──────────────────────────────────────────────────

@inventory_bp.route("/api/procurement/<path:material>")
@login_required
@module_access_required("inventory")
def api_procurement(material: str):
    """Return procurement history for a specific material."""
    store = get_data_store()
    plant = request.args.get("plant")
    history = store.get_procurement_history(material, plant=plant)
    return jsonify(history)


# ── API: Chart data ───────────────────────────────────────────────────────────

@inventory_bp.route("/api/chart/<path:material>")
@login_required
@module_access_required("inventory")
def api_chart(material: str):
    """Return chart-ready consumption data for a specific material."""
    store = get_data_store()
    plant = request.args.get("plant")
    chart = store.get_monthly_consumption(material, plant=plant)

    # Also include procurement timeline
    proc_history = store.get_procurement_history(material, plant=plant)
    proc_dates = []
    proc_values = []
    for p in proc_history:
        if p.get("doc_date"):
            proc_dates.append(p["doc_date"])
            proc_values.append(p["effective_value"])

    return jsonify({
        "consumption": chart,
        "procurement": {
            "dates": proc_dates,
            "values": proc_values,
        },
    })


@inventory_bp.route("/api/analytics/<path:material>")
@login_required
@module_access_required("inventory")
def api_analytics(material: str):
    """Return the material analytics payload for the dashboard modal."""
    store = get_data_store()
    plant = request.args.get("plant")
    return jsonify(store.get_material_analytics(material, plant=plant))


@inventory_bp.route("/export")
@login_required
@module_access_required("inventory")
def export_excel():
    """Download the filtered dashboard data as an Excel workbook."""
    store = get_data_store()
    plant = request.args.get("plant")
    query = request.args.get("q", "")
    stream, filename = store.build_export_workbook(plant=plant, query=query)
    return send_file(
        stream,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
