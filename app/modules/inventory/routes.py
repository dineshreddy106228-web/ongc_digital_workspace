"""Inventory Intelligence Module – Routes.

Endpoints:
    GET  /inventory/                          → Landing dashboard (module overview cards)
    GET  /inventory/materials                 → Materials Intelligence (KPI + interactive table)
    GET  /inventory/forecast                  → Demand Forecast (probabilistic 6-month view)
    GET  /inventory/seed-files                → Data Management (seed file upload & audit)
    GET  /inventory/master-data               → Master Data viewer/editor  [superuser only]
    POST /inventory/master-data/import        → Upload Master_data.xlsx    [superuser only]
    GET  /inventory/api/overview              → JSON: filtered dashboard payload
    GET  /inventory/api/materials             → JSON: all materials for table
    GET  /inventory/api/consumption/<material>→ JSON: monthly consumption detail
    GET  /inventory/api/procurement/<material>→ JSON: procurement history
    GET  /inventory/api/chart/<material>      → JSON: chart data for material
    GET  /inventory/api/analytics/<material>  → JSON: material analytics for modal
    GET  /inventory/api/master-data           → JSON: full master data list (all inventory users)
    POST /inventory/api/master-data/<code>    → JSON: update a single master record [superuser]
    GET  /inventory/export                    → Download Excel workbook
"""

from __future__ import annotations

import json
import logging

from flask import flash, jsonify, redirect, render_template, request, send_file, session, url_for
from flask_login import login_required

from app.modules.inventory import inventory_bp
from app.core.utils.decorators import module_access_required, superuser_required

logger = logging.getLogger(__name__)


def _get_inventory_store():
    from app.core.services.inventory_intelligence import get_data_store

    return get_data_store()


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
    target_fy: str | None = None,
):
    from app.core.services.inventory_intelligence import (
        get_seed_upload_schema,
        get_seed_upload_status,
    )
    from app.core.services.inventory_seed_audit import (
        LOW_VALUE_THRESHOLD,
        get_procurement_rows_below_threshold,
        get_staged_seed_report,
    )

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
        fy_status=get_seed_upload_status(),
        audit_report=audit_report,
        upload_error=upload_error,
        cleanup_preview=get_procurement_rows_below_threshold(LOW_VALUE_THRESHOLD, token=token),
        cleanup_threshold=LOW_VALUE_THRESHOLD,
        pending_token=token,
        target_fy=(target_fy if target_fy is not None else request.args.get("target_fy", "")).strip(),
    )


# ── Landing Dashboard ─────────────────────────────────────────────────────────

@inventory_bp.route("/")
@login_required
@module_access_required("inventory")
def landing():
    """Inventory Intelligence landing dashboard — module overview with cards."""
    return render_template("inventory/landing.html")


# ── Materials Intelligence ────────────────────────────────────────────────────

@inventory_bp.route("/materials")
@login_required
@module_access_required("inventory")
def materials_page():
    """Materials Intelligence — KPIs + interactive materials table."""
    return render_template(
        "inventory/dashboard.html",
        initial_plant=(request.args.get("plant") or "").strip(),
    )


@inventory_bp.route("/seed-files", methods=["GET", "POST"])
@login_required
@module_access_required("inventory")
def seed_files():
    """Upload the paired seed workbooks used by inventory intelligence."""
    from app.core.services.inventory_seed_audit import (
        create_staged_seed_upload,
        discard_staged_seed_upload,
    )

    if request.method == "POST":
        previous_token = _get_pending_token()
        target_fy = request.form.get("target_fy", "").strip()
        consumption_file = request.files.get("consumption_seed")
        procurement_file = request.files.get("procurement_seed")
        mc9_file = request.files.get("mc9_seed")

        if not consumption_file or not consumption_file.filename:
            return _render_seed_files_page(upload_error="Select the consumption seed file before uploading.")
        if not procurement_file or not procurement_file.filename:
            return _render_seed_files_page(upload_error="Select the procurement seed file before uploading.")
        if not mc9_file or not mc9_file.filename:
            return _render_seed_files_page(upload_error="Select the MC.9 monthly stock and consumption seed file before uploading.")

        consumption_bytes = consumption_file.read()
        procurement_bytes = procurement_file.read()
        mc9_bytes = mc9_file.read()

        try:
            if previous_token:
                discard_staged_seed_upload(previous_token)
                _set_pending_token(None)
            token, audit_report = create_staged_seed_upload(
                consumption_bytes=consumption_bytes,
                consumption_filename=consumption_file.filename,
                procurement_bytes=procurement_bytes,
                procurement_filename=procurement_file.filename,
                mc9_bytes=mc9_bytes,
                mc9_filename=mc9_file.filename,
                expected_fy=target_fy or None,
            )
            _set_pending_token(token)
            return _render_seed_files_page(
                audit_report=audit_report,
                upload_error=(
                    "Sanity Check Failed. Please scroll down to review and clean the flagged rows."
                    if audit_report["has_failures"]
                    else None
                ),
                pending_token=token,
                target_fy=target_fy,
            )
        except ValueError as exc:
            return _render_seed_files_page(upload_error=str(exc), target_fy=target_fy)
        except Exception:
            logger.exception("Failed to upload inventory seed workbooks")
            return _render_seed_files_page(
                upload_error="Could not update the seed files. Check the files and try again.",
                target_fy=target_fy,
            )

    return _render_seed_files_page()


@inventory_bp.route("/seed-files/staged-delete", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_delete_rows():
    from app.core.services.inventory_seed_audit import delete_rows_from_staged_seed_upload

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
    from app.core.services.inventory_seed_audit import ignore_rows_from_staged_seed_upload

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


@inventory_bp.route("/seed-files/staged-apply", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_apply():
    from app.core.services.inventory_seed_audit import (
        apply_staged_seed_upload,
        get_staged_seed_report,
    )

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
    return redirect(url_for("inventory.landing"))


@inventory_bp.route("/seed-files/staged-discard", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_discard():
    from app.core.services.inventory_seed_audit import discard_staged_seed_upload

    token = request.form.get("pending_token") or _get_pending_token()
    if token:
        discard_staged_seed_upload(token)
    _set_pending_token(None)
    flash("Staged seed upload discarded.", "info")
    return redirect(url_for("inventory.seed_files"))


@inventory_bp.route("/seed-files/staged-bulk-apply", methods=["POST"])
@login_required
@module_access_required("inventory")
def staged_seed_bulk_apply():
    """Accept one-shot keep/delete decisions for all sanity-check rows, then apply.

    Form fields (for each selectable row in every finding):
        dec__{scope}__{check}__{excel_row}  → "delete" | "keep"
    """
    from app.core.services.inventory_seed_audit import (
        apply_staged_seed_upload,
        bulk_cleanup_staged_seed_upload,
    )

    token = request.form.get("pending_token") or _get_pending_token()
    if not token:
        return _render_seed_files_page(
            upload_error="The staged upload is no longer available. Upload the files again."
        )

    # ── Parse all row decisions from form ─────────────────────────────────
    delete_decisions: dict[str, list[int]] = {}
    keep_decisions: dict[str, list[int]] = {}

    for field_name, value in request.form.items():
        if not field_name.startswith("dec__"):
            continue
        # Name format: dec__{scope}__{check}__{excel_row}
        # Use rsplit with maxsplit=2 from the left-stripped prefix to get 3 parts.
        remainder = field_name[5:]  # strip leading "dec__"
        parts = remainder.split("__")
        if len(parts) < 3:
            continue
        # excel_row is always the last segment; scope may contain "_" but not "__"
        excel_row_str = parts[-1]
        check = parts[-2]
        scope = "__".join(parts[:-2])  # handles "cross_file" or similar
        try:
            excel_row = int(excel_row_str)
        except ValueError:
            continue
        dict_key = f"{scope}:{check}"
        if value == "delete":
            delete_decisions.setdefault(dict_key, []).append(excel_row)
        elif value == "keep":
            keep_decisions.setdefault(dict_key, []).append(excel_row)

    try:
        report = bulk_cleanup_staged_seed_upload(token, delete_decisions, keep_decisions)
        _set_pending_token(token)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc), pending_token=token)
    except Exception:
        logger.exception("Failed to apply bulk staged-seed cleanup")
        return _render_seed_files_page(
            upload_error="Could not apply the cleanup decisions. Please try again.",
            pending_token=token,
        )

    # ── If errors remain, re-render the review page ────────────────────────
    if report["has_failures"]:
        return _render_seed_files_page(
            audit_report=report,
            upload_error=(
                "Some error-level findings still remain after cleanup. "
                "Please review and resolve all errors before uploading."
            ),
            pending_token=token,
        )

    # ── All clear — apply to the seed store ───────────────────────────────
    try:
        apply_staged_seed_upload(token)
        _set_pending_token(None)
    except Exception:
        logger.exception("Failed to apply staged seed upload after bulk cleanup")
        return _render_seed_files_page(
            upload_error="Cleanup succeeded but the upload could not be applied. Please try again.",
            pending_token=token,
        )

    flash(
        "Seed files cleaned and uploaded successfully. Inventory analysis has been reloaded.",
        "success",
    )
    return redirect(url_for("inventory.landing"))


@inventory_bp.route("/seed-files/procurement-low-value-delete", methods=["POST"])
@login_required
@module_access_required("inventory")
def procurement_low_value_delete():
    """Delete procurement rows below the configured value threshold."""
    from app.core.services.inventory_seed_audit import (
        LOW_VALUE_THRESHOLD,
        delete_procurement_rows_below_threshold,
    )

    token = request.form.get("pending_token") or _get_pending_token()
    try:
        preview = delete_procurement_rows_below_threshold(LOW_VALUE_THRESHOLD, token=token)
        if token:
            _set_pending_token(token)
    except ValueError as exc:
        return _render_seed_files_page(upload_error=str(exc), pending_token=token)
    except Exception:
        logger.exception("Failed to delete low-value procurement rows")
        return _render_seed_files_page(
            upload_error="Could not delete the low-value procurement rows.",
            pending_token=token,
        )

    flash(
        f"Deleted {preview['count']} procurement row(s) below ₹ {LOW_VALUE_THRESHOLD:,.2f} from the {preview['source_label']}.",
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


# ── Forecast Excel Export ─────────────────────────────────────────────────────

@inventory_bp.route("/forecast/export")
@login_required
@module_access_required("inventory")
def forecast_export():
    """Download a multi-sheet forecast workbook for all materials."""
    from app.core.services.inventory_forecast_export import build_forecast_workbook

    store = _get_inventory_store()
    plant = request.args.get("plant")
    payload = store.get_dashboard_payload(plant=plant)
    materials = payload.get("materials", [])

    # Build forecast for each material
    materials_forecast = []
    for mat in materials:
        try:
            analytics = store.get_material_analytics(mat["material"], plant=plant)
            materials_forecast.append({
                "material": mat["material"],
                "forecast": analytics.get("forecast", {}),
                "consumption": analytics.get("consumption", {}),
                "summary": analytics.get("summary", {}),
            })
        except Exception:
            continue

    plant_label = plant or "All Plants"
    stream, filename = build_forecast_workbook(materials_forecast, plant_label)
    return send_file(
        stream,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ── API: Filtered dashboard payload ───────────────────────────────────────────

@inventory_bp.route("/api/overview")
@login_required
@module_access_required("inventory")
def api_overview():
    """Return the filtered dashboard payload."""
    store = _get_inventory_store()
    plant = request.args.get("plant")
    return jsonify(store.get_overview_payload(plant=plant))


# ── API: Materials list ───────────────────────────────────────────────────────

@inventory_bp.route("/api/materials")
@login_required
@module_access_required("inventory")
def api_materials():
    """Return all materials as JSON for client-side table rendering."""
    store = _get_inventory_store()
    plant = request.args.get("plant")
    return jsonify(store.get_materials_table(plant=plant))


# ── API: Monthly consumption detail ──────────────────────────────────────────

@inventory_bp.route("/api/consumption/<path:material>")
@login_required
@module_access_required("inventory")
def api_consumption(material: str):
    """Return monthly consumption breakdown for a specific material."""
    store = _get_inventory_store()
    plant = request.args.get("plant")
    detail = store.get_monthly_consumption_detail(material, plant=plant)
    return jsonify(detail)


# ── API: Procurement history ──────────────────────────────────────────────────

@inventory_bp.route("/api/procurement/<path:material>")
@login_required
@module_access_required("inventory")
def api_procurement(material: str):
    """Return procurement history for a specific material."""
    store = _get_inventory_store()
    plant = request.args.get("plant")
    history = store.get_procurement_history(material, plant=plant)
    return jsonify(history)


# ── API: Chart data ───────────────────────────────────────────────────────────

@inventory_bp.route("/api/chart/<path:material>")
@login_required
@module_access_required("inventory")
def api_chart(material: str):
    """Return chart-ready consumption data for a specific material."""
    store = _get_inventory_store()
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
    store = _get_inventory_store()
    plant = request.args.get("plant")
    section = (request.args.get("section") or "").strip().lower()
    if section:
        try:
            payload = store.get_material_analytics_section(material, section, plant=plant)
        except ValueError:
            return jsonify({"error": "Unsupported analytics section."}), 400
        return jsonify(payload)
    return jsonify(store.get_material_analytics_overview(material, plant=plant))


@inventory_bp.route("/export")
@login_required
@module_access_required("inventory")
def export_excel():
    """Download the filtered dashboard data as an Excel workbook."""
    store = _get_inventory_store()
    plant = request.args.get("plant")
    query = request.args.get("q", "")
    stream, filename = store.build_export_workbook(plant=plant, query=query)
    return send_file(
        stream,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ── Master Data ───────────────────────────────────────────────────────────────

@inventory_bp.route("/master-data")
@login_required
@superuser_required
def master_data_page():
    """Master Data viewer/editor – superusers only."""
    from app.core.services.master_data import get_all_master_data, get_extra_column_keys

    rows = get_all_master_data()
    extra_keys = get_extra_column_keys()
    return render_template(
        "inventory/master_data.html",
        rows=rows,
        extra_keys=extra_keys,
        total=len(rows),
        read_only=True,
    )


@inventory_bp.route("/master-data/import", methods=["POST"])
@login_required
@superuser_required
def master_data_import():
    """Upload a Master_data.xlsx workbook and upsert all rows."""
    flash("Inventory Master Data import is disabled. Edit master data from CSC Manage Drafts.", "warning")
    return redirect(url_for("inventory.master_data_page"))


# ── API: Delete a single master data row (superuser only) ────────────────────

@inventory_bp.route("/api/master-data/<path:material>/delete", methods=["POST"])
@login_required
@superuser_required
def api_master_data_delete(material: str):
    """Permanently delete a MaterialMaster row. Returns {ok: true} on success."""
    return jsonify({
        "error": "Inventory Master Data is read-only. Delete or edit from CSC Manage Drafts."
    }), 403


# ── API: Master data list (all inventory users) ───────────────────────────────

@inventory_bp.route("/api/master-data")
@login_required
@module_access_required("inventory")
def api_master_data():
    """Return the full master data list as JSON."""
    from app.core.services.master_data import get_all_master_data, get_extra_column_keys

    rows = get_all_master_data()
    extra_keys = get_extra_column_keys()
    return jsonify({"rows": rows, "extra_keys": extra_keys})


# ── API: Update a single master data row (superuser only) ─────────────────────

@inventory_bp.route("/api/master-data/<path:material>", methods=["POST"])
@login_required
@superuser_required
def api_master_data_update(material: str):
    """Update an existing MaterialMaster row.  Returns updated record as JSON."""
    return jsonify({
        "error": "Inventory Master Data is read-only. Update master data from CSC Manage Drafts."
    }), 403


# ── API: Plant Grouping ───────────────────────────────────────────────────────

@inventory_bp.route("/api/plant-grouping")
@login_required
@module_access_required("inventory")
def api_plant_grouping_get():
    """Return the current plant grouping configuration plus all known plants."""
    import json as _json
    from pathlib import Path

    base = Path(__file__).resolve().parents[3]
    cfg_path = base / "inventory_plant_grouping.json"

    if cfg_path.exists():
        cfg = _json.loads(cfg_path.read_text())
    else:
        cfg = {"prefix_groups": {}, "explicit_groups": {}, "group_members": {}}

    # Collect all raw and reporting plants from the normalized seed data so
    # the grouping modal can remap every available plant, not only the
    # already-grouped reporting parents shown in dashboard summaries.
    store = _get_inventory_store()
    try:
        all_plants_set = set()
        for frame_name in ("cons", "proc"):
            frame = getattr(store, frame_name, None)
            if frame is None or frame.empty:
                continue
            for column in ("plant", "reporting_plant"):
                if column in frame.columns:
                    values = (
                        frame[column]
                        .dropna()
                        .astype(str)
                        .str.strip()
                    )
                    all_plants_set.update(value for value in values.tolist() if value)
    except Exception:
        all_plants_set = set()

    # Also add all plants declared in group_members
    members = cfg.get("group_members", {})
    for parent, children in members.items():
        all_plants_set.add(parent)
        all_plants_set.update(children)

    # Plant type descriptions (static lookup)
    plant_descriptions = {
        "11A1": "UT Offshore — Mumbai High North",
        "12A1": "UT Offshore — Heera / Ratna & R-Series",
        "13A1": "UT Offshore — Mumbai High South",
        "22A1": "Onshore — Ahmedabad / Ankleshwar",
        "25A1": "Onshore — Rajasthan",
        "50A1": "Onshore — Tripura",
        "51A1": "Onshore — Assam",
    }

    return jsonify({
        **cfg,
        "all_plants": sorted(all_plants_set),
        "plant_descriptions": plant_descriptions,
    })


@inventory_bp.route("/api/plant-grouping", methods=["POST"])
@login_required
@module_access_required("inventory")
def api_plant_grouping_post():
    """Save updated plant grouping and reload the data store."""
    import json as _json
    from pathlib import Path

    base = Path(__file__).resolve().parents[3]
    cfg_path = base / "inventory_plant_grouping.json"

    data = request.get_json(silent=True) or {}
    new_members: dict = data.get("group_members", {})

    if not isinstance(new_members, dict):
        return jsonify({"ok": False, "error": "Invalid payload: group_members must be a dict."}), 400

    # Load existing config to preserve prefix_groups / explicit_groups
    if cfg_path.exists():
        cfg = _json.loads(cfg_path.read_text())
    else:
        cfg = {"prefix_groups": {}, "explicit_groups": {}}

    cfg["group_members"] = new_members

    try:
        cfg_path.write_text(_json.dumps(cfg, indent=2))
    except Exception as exc:
        logger.exception("Failed to write plant grouping config")
        return jsonify({"ok": False, "error": str(exc)}), 500

    # Invalidate the data store cache so the new grouping takes effect
    try:
        store = _get_inventory_store()
        store._dashboard_cache.clear()
        store._loaded = False
    except Exception:
        pass

    return jsonify({"ok": True, "group_members": new_members})


# ══════════════════════════════════════════════════════════════════════════════
# SAP Ingestion & Analytics API  (Phase 2)
# ══════════════════════════════════════════════════════════════════════════════
# These routes handle direct SAP file uploads (MB51 / ME2M / MC.9), computed
# table rebuilds, and structured analytics queries.  They are additive —
# existing routes above remain unchanged.
# ══════════════════════════════════════════════════════════════════════════════

import io as _io
from datetime import date as _date

MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

_VALID_SOURCES = {"MB51", "ME2M", "MC9"}


def _validate_fiscal_year(raw: str | None) -> int | None:
    """Return an int fiscal year or None.  Raise ValueError on junk input."""
    if not raw:
        return None
    try:
        fy = int(raw)
    except (TypeError, ValueError):
        raise ValueError("fiscal_year must be an integer")
    if fy < 2000 or fy > 2099:
        raise ValueError("fiscal_year must be between 2000 and 2099")
    return fy


def _json_error(message: str, status: int = 400):
    return jsonify({"status": "error", "message": message}), status


# ── POST /inventory/upload ────────────────────────────────────────────────────

@inventory_bp.route("/upload", methods=["POST"])
@login_required
@module_access_required("inventory")
def sap_upload():
    """Upload an SAP export file (MB51 / ME2M / MC.9) for ingestion."""
    from flask_login import current_user
    from app.modules.inventory.ingestion.mb51_loader import MB51Loader
    from app.modules.inventory.ingestion.me2m_loader import ME2MLoader
    from app.modules.inventory.ingestion.mc9_loader import MC9Loader
    from app.modules.inventory.analytics.stock_register import build_stock_register
    from app.modules.inventory.analytics.vendor_grading import build_vendor_scorecard
    from app.modules.inventory.ingestion.base_loader import ValidationError

    source = (request.form.get("source") or "").strip().upper()
    if source not in _VALID_SOURCES:
        return _json_error(f"source must be one of {', '.join(sorted(_VALID_SOURCES))}")

    file = request.files.get("file")
    if not file or not file.filename:
        return _json_error("No file provided")

    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in ("xlsx", "csv"):
        return _json_error("Only .xlsx and .csv files are accepted")

    # Read file into memory
    raw_bytes = file.read()
    if len(raw_bytes) > MAX_UPLOAD_BYTES:
        return _json_error(f"File exceeds the {MAX_UPLOAD_BYTES // (1024*1024)} MB limit", 413)

    try:
        raw_fy = request.form.get("fiscal_year")
        fiscal_year = _validate_fiscal_year(raw_fy)
    except ValueError as exc:
        return _json_error(str(exc))

    loader_map = {"MB51": MB51Loader, "ME2M": ME2MLoader, "MC9": MC9Loader}
    loader = loader_map[source]()

    buf = _io.BytesIO(raw_bytes)
    buf.name = file.filename

    try:
        result = loader.load(buf, fiscal_year=fiscal_year, loaded_by=getattr(current_user, "username", None))
    except ValidationError as exc:
        return _json_error(str(exc))
    except Exception:
        logger.exception("SAP upload failed for source=%s", source)
        return _json_error("File processing failed. Check the file format and try again.", 500)

    # Trigger recompute after successful load
    warnings = list(result.warnings)
    if fiscal_year and result.rows_loaded > 0:
        try:
            if source in ("MC9", "MB51"):
                build_stock_register(fiscal_year)
            if source == "ME2M":
                build_vendor_scorecard(fiscal_year)
        except Exception:
            logger.exception("Post-upload recompute failed")
            warnings.append("Data loaded but post-upload recompute encountered an error.")

    return jsonify({
        "status": result.status,
        "rows_loaded": result.rows_loaded,
        "rows_rejected": result.rows_rejected,
        "warnings": warnings,
    })


# ── POST /inventory/recompute ─────────────────────────────────────────────────

@inventory_bp.route("/recompute", methods=["POST"])
@login_required
@module_access_required("inventory")
def sap_recompute():
    """Trigger a rebuild of computed tables."""
    from app.modules.inventory.analytics.stock_register import build_stock_register
    from app.modules.inventory.analytics.vendor_grading import build_vendor_scorecard

    data = request.get_json(silent=True) or {}
    raw_fy = data.get("fiscal_year")
    scope = (data.get("scope") or "all").strip().lower()

    try:
        fiscal_year = _validate_fiscal_year(str(raw_fy) if raw_fy else None)
    except ValueError as exc:
        return _json_error(str(exc))
    if not fiscal_year:
        return _json_error("fiscal_year is required")
    if scope not in ("stock_register", "vendor_scorecard", "all"):
        return _json_error("scope must be stock_register, vendor_scorecard, or all")

    summary: dict = {}
    try:
        if scope in ("stock_register", "all"):
            summary["stock_register"] = build_stock_register(fiscal_year)
        if scope in ("vendor_scorecard", "all"):
            summary["vendor_scorecard"] = build_vendor_scorecard(fiscal_year)
    except Exception:
        logger.exception("Recompute failed for scope=%s fy=%s", scope, fiscal_year)
        return _json_error("Recompute failed. Check logs for details.", 500)

    return jsonify({"status": "success", "summary": summary})


# ── GET /inventory/stock-register ──────────────────────────────────────────────

@inventory_bp.route("/stock-register")
@login_required
@module_access_required("inventory")
def sap_stock_register():
    """Return the monthly stock register as JSON or Excel download."""
    from app.modules.inventory.analytics.queries import get_stock_register

    raw_fy = request.args.get("fiscal_year")
    try:
        fiscal_year = _validate_fiscal_year(raw_fy)
    except ValueError as exc:
        return _json_error(str(exc))
    if not fiscal_year:
        return _json_error("fiscal_year is required")

    plant = request.args.get("plant")
    material_no = request.args.get("material_no")
    fmt = (request.args.get("format") or "json").strip().lower()

    rows = get_stock_register(fiscal_year, plant=plant, material_no=material_no)

    if fmt == "excel":
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Stock Register"
        if rows:
            ws.append(list(rows[0].keys()))
            for r in rows:
                ws.append(list(r.values()))
        buf = _io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f"stock_register_FY{fiscal_year}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return jsonify(rows)


# ── GET /inventory/analytics/yoy-spend ─────────────────────────────────────────

@inventory_bp.route("/analytics/yoy-spend")
@login_required
@module_access_required("inventory")
def sap_yoy_spend():
    from app.modules.inventory.analytics.queries import get_yoy_spend

    raw_fys = request.args.get("fiscal_years", "")
    try:
        fiscal_years = [int(x.strip()) for x in raw_fys.split(",") if x.strip()]
    except ValueError:
        return _json_error("fiscal_years must be comma-separated integers")
    if not fiscal_years:
        return _json_error("fiscal_years is required")

    plant = request.args.get("plant")
    material_group = request.args.get("material_group")
    return jsonify(get_yoy_spend(fiscal_years, plant=plant, material_group=material_group))


# ── GET /inventory/analytics/abc ───────────────────────────────────────────────

@inventory_bp.route("/analytics/abc")
@login_required
@module_access_required("inventory")
def sap_abc():
    from app.modules.inventory.analytics.queries import get_abc_analysis

    raw_fy = request.args.get("fiscal_year")
    try:
        fiscal_year = _validate_fiscal_year(raw_fy)
    except ValueError as exc:
        return _json_error(str(exc))
    if not fiscal_year:
        return _json_error("fiscal_year is required")

    plant = request.args.get("plant")
    return jsonify(get_abc_analysis(fiscal_year, plant=plant))


# ── GET /inventory/analytics/vendor-scorecard ──────────────────────────────────

@inventory_bp.route("/analytics/vendor-scorecard")
@login_required
@module_access_required("inventory")
def sap_vendor_scorecard():
    from app.modules.inventory.analytics.queries import get_vendor_scorecard

    raw_fy = request.args.get("fiscal_year")
    try:
        fiscal_year = _validate_fiscal_year(raw_fy)
    except ValueError as exc:
        return _json_error(str(exc))
    if not fiscal_year:
        return _json_error("fiscal_year is required")

    grade = request.args.get("grade")
    if grade and grade.upper() not in ("A", "B", "C", "D"):
        return _json_error("grade must be A, B, C, or D")

    return jsonify(get_vendor_scorecard(fiscal_year, grade=grade))


# ── GET /inventory/analytics/dead-stock ────────────────────────────────────────

@inventory_bp.route("/analytics/dead-stock")
@login_required
@module_access_required("inventory")
def sap_dead_stock():
    from app.modules.inventory.analytics.queries import get_dead_stock

    plant = request.args.get("plant")
    try:
        months = int(request.args.get("months", 6))
    except (TypeError, ValueError):
        return _json_error("months must be an integer")

    return jsonify(get_dead_stock(plant=plant, no_movement_months=months))


# ── GET /inventory/analytics/open-po-aging ─────────────────────────────────────

@inventory_bp.route("/analytics/open-po-aging")
@login_required
@module_access_required("inventory")
def sap_open_po_aging():
    from app.modules.inventory.analytics.queries import get_open_po_aging

    plant = request.args.get("plant")
    return jsonify(get_open_po_aging(plant=plant))


# ── GET /inventory/analytics/price-evolution ───────────────────────────────────

@inventory_bp.route("/analytics/price-evolution")
@login_required
@module_access_required("inventory")
def sap_price_evolution():
    from app.modules.inventory.analytics.queries import get_price_evolution

    material_no = request.args.get("material_no")
    plant = request.args.get("plant")
    if not material_no or not plant:
        return _json_error("material_no and plant are required")

    return jsonify(get_price_evolution(material_no, plant))


# ── GET /inventory/analytics/consumption-trend ─────────────────────────────────

@inventory_bp.route("/analytics/consumption-trend")
@login_required
@module_access_required("inventory")
def sap_consumption_trend():
    from app.modules.inventory.analytics.queries import get_material_consumption_trend

    material_no = request.args.get("material_no")
    plant = request.args.get("plant")
    if not material_no or not plant:
        return _json_error("material_no and plant are required")

    try:
        months = int(request.args.get("months", 24))
    except (TypeError, ValueError):
        return _json_error("months must be an integer")

    return jsonify(get_material_consumption_trend(material_no, plant, months=months))


# ── GET /inventory/analytics/forecast ──────────────────────────────────────────

@inventory_bp.route("/analytics/forecast")
@login_required
@module_access_required("inventory")
def sap_forecast():
    from app.modules.inventory.analytics.forecast import forecast_material

    material_no = request.args.get("material_no")
    plant = request.args.get("plant")
    if not material_no or not plant:
        return _json_error("material_no and plant are required")

    method = request.args.get("method", "weighted_moving_avg")
    try:
        periods_ahead = int(request.args.get("periods_ahead", 3))
    except (TypeError, ValueError):
        return _json_error("periods_ahead must be an integer")

    return jsonify(forecast_material(material_no, plant, method=method, periods_ahead=periods_ahead))


# ── GET /inventory/analytics/xyz ───────────────────────────────────────────────

@inventory_bp.route("/analytics/xyz")
@login_required
@module_access_required("inventory")
def sap_xyz():
    from app.modules.inventory.analytics.forecast import get_xyz_classification

    raw_fy = request.args.get("fiscal_year")
    try:
        fiscal_year = _validate_fiscal_year(raw_fy)
    except ValueError as exc:
        return _json_error(str(exc))
    if not fiscal_year:
        return _json_error("fiscal_year is required")

    plant = request.args.get("plant")
    return jsonify(get_xyz_classification(fiscal_year, plant=plant))


# ── GET /inventory/load-history ────────────────────────────────────────────────

@inventory_bp.route("/load-history")
@login_required
@module_access_required("inventory")
def sap_load_history():
    """Return the last 50 data_load_log entries."""
    from app.modules.inventory.analytics.queries import _fetch_all

    rows = _fetch_all(
        "SELECT * FROM data_load_log ORDER BY loaded_at DESC LIMIT 50", ()
    )
    return jsonify(rows)
