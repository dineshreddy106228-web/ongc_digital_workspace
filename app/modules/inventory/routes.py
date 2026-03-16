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
        target_fy=request.args.get("target_fy", "").strip(),
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
            _set_pending_token(token)
            return _render_seed_files_page(
                audit_report=audit_report,
                upload_error=(
                    "Sanity Check Failed. Please scroll down to review and clean the flagged rows."
                    if audit_report["has_failures"]
                    else None
                ),
                pending_token=token,
            )
        except ValueError as exc:
            return _render_seed_files_page(upload_error=str(exc))
        except Exception:
            logger.exception("Failed to upload inventory seed workbooks")
            return _render_seed_files_page(upload_error="Could not update the seed files. Check the files and try again.")

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
    )


@inventory_bp.route("/master-data/import", methods=["POST"])
@login_required
@superuser_required
def master_data_import():
    """Upload a Master_data.xlsx workbook and upsert all rows."""
    from flask_login import current_user
    from app.core.services.master_data import import_master_data_xlsx

    file = request.files.get("master_data_file")
    if not file or not file.filename:
        flash("Please select a Master Data Excel file before uploading.", "danger")
        return redirect(url_for("inventory.master_data_page"))

    if not file.filename.lower().endswith((".xlsx", ".xls")):
        flash("Only .xlsx / .xls files are supported.", "danger")
        return redirect(url_for("inventory.master_data_page"))

    try:
        result = import_master_data_xlsx(
            file_bytes=file.read(),
            user_id=current_user.id,
        )
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("inventory.master_data_page"))
    except Exception:
        logger.exception("Unexpected error importing master data")
        flash("Could not import the file. Check the format and try again.", "danger")
        return redirect(url_for("inventory.master_data_page"))

    parts = [f"{result['inserted']} inserted, {result['updated']} updated"]
    if result["skipped"]:
        parts.append(f"{result['skipped']} skipped (no material code)")
    if result["extra_columns"]:
        parts.append(
            f"Extra columns stored: {', '.join(result['extra_columns'])}"
        )
    if result["errors"]:
        flash(
            f"Import completed with {len(result['errors'])} error(s): "
            + "; ".join(result["errors"][:3]),
            "warning",
        )
    flash(f"Master data imported — {'; '.join(parts)}.", "success")
    return redirect(url_for("inventory.master_data_page"))


# ── API: Delete a single master data row (superuser only) ────────────────────

@inventory_bp.route("/api/master-data/<path:material>/delete", methods=["POST"])
@login_required
@superuser_required
def api_master_data_delete(material: str):
    """Permanently delete a MaterialMaster row. Returns {ok: true} on success."""
    from app.extensions import db
    from app.models.inventory.material_master import MaterialMaster

    record = MaterialMaster.query.get(material)
    if not record:
        return jsonify({"error": f"Material {material!r} not found."}), 404

    try:
        db.session.delete(record)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        logger.exception("Failed to delete master data record %s", material)
        return jsonify({"error": str(exc)}), 500

    return jsonify({"ok": True, "deleted": material})


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
    from flask_login import current_user
    from app.extensions import db
    from app.models.inventory.material_master import MaterialMaster, CORE_FIELDS

    record = MaterialMaster.query.get(material)
    if not record:
        return jsonify({"error": f"Material {material!r} not found."}), 404

    data = request.get_json(silent=True) or {}

    # Update all core fields (CORE_FIELDS = all model fields except the PK)
    for field in CORE_FIELDS:
        if field in data:
            setattr(record, field, data[field] or None)

    # Update extra_data fields (JSON bucket for any non-schema columns)
    if "extra_data" in data and isinstance(data["extra_data"], dict):
        merged = dict(record.extra_data or {})
        merged.update(data["extra_data"])
        record.extra_data = merged

    from datetime import datetime, timezone
    record.updated_at = datetime.now(timezone.utc)
    record.updated_by = current_user.id

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        logger.exception("Failed to update master data record %s", material)
        return jsonify({"error": str(exc)}), 500

    return jsonify({"ok": True, "record": record.to_dict()})


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

    # Collect all plants that appear in the consumption data
    store = _get_inventory_store()
    try:
        payload = store.get_dashboard_payload()
        all_plants_set = set()
        for mat in payload.get("materials", []):
            for p in mat.get("plants", []):
                all_plants_set.add(p)
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
