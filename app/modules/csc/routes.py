"""Routes for Corporate Specifications Management."""

from __future__ import annotations

import io
import logging
from datetime import datetime

from flask import (
    abort, flash, redirect, render_template, request, send_file, url_for,
)
from flask_login import current_user, login_required

from app.core.utils.decorators import module_access_required, module_admin_required
from app.extensions import db
from app.modules.csc import csc_bp

logger = logging.getLogger(__name__)


@csc_bp.context_processor
def inject_specification_navigation():
    """Expose the admin flag to every template in this module."""
    return {"csc_is_module_admin": current_user.is_authenticated and current_user.is_module_admin("csc")}


def _current_username() -> str:
    return (getattr(current_user, "full_name", "") or "").strip() or current_user.username


def _entry_or_404(ref: str) -> dict:
    from app.core.services.corporate_specifications import specification_data

    data = specification_data(ref)
    if data is None:
        abort(404)
    return data


# ── Catalogue ────────────────────────────────────────────────────────────────


@csc_bp.route("/")
@login_required
@module_access_required("csc")
def index():
    from app.core.services.corporate_specifications import landing_data

    return render_template("csc/landing.html", **landing_data())


@csc_bp.route("/landing")
@login_required
@module_access_required("csc")
def landing():
    """Alias kept so older links and bookmarks still resolve."""
    return redirect(url_for("csc.index"))


@csc_bp.route("/category/<key>")
@login_required
@module_access_required("csc")
def category(key: str):
    from app.core.services.corporate_specifications import category_data

    data = category_data(key, request.args.get("q", ""))
    if not data:
        abort(404)
    return render_template("csc/category.html", **data)


@csc_bp.route("/specification/<ref>")
@login_required
@module_access_required("csc")
def specification(ref: str):
    return render_template("csc/specification.html", **_entry_or_404(ref))


@csc_bp.route("/specification/<ref>/dossier.docx")
@login_required
@module_access_required("csc")
def specification_dossier(ref: str):
    """The full CSC dossier for one specification, as the workflow used to produce it."""
    from app.core.services.corporate_specifications import dossier_context
    from app.core.services.csc_export import build_flask_review_document

    data = _entry_or_404(ref)
    if data["record"] is None:
        flash("This chemical has no specification record to build a dossier from.", "info")
        return redirect(url_for("csc.specification", ref=ref))
    try:
        document = build_flask_review_document(dossier_context(data))
    except Exception:
        logger.exception("Dossier export failed for ref=%s", ref)
        flash("The dossier could not be generated.", "danger")
        return redirect(url_for("csc.specification", ref=ref))
    name = (data["record"].spec_number or "specification").replace("/", "-").replace(" ", "")
    return send_file(
        io.BytesIO(document),
        as_attachment=True,
        download_name=f"{name}_v{data['entry']['version']}_dossier.docx",
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        max_age=0,
    )


# ── Admin editing ────────────────────────────────────────────────────────────


@csc_bp.route("/specification/<ref>/open-record", methods=["POST"])
@login_required
@module_access_required("csc")
@module_admin_required("csc")
def open_specification_record(ref: str):
    """Start a specification record for a register chemical that has none yet."""
    from app.core.services.corporate_specifications import create_record

    data = _entry_or_404(ref)
    if data["record"] is not None:
        return redirect(url_for("csc.edit_specification", ref=ref))
    try:
        create_record(data["entry"], _current_username())
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("Could not open a specification record for ref=%s", ref)
        flash("The specification record could not be opened.", "danger")
        return redirect(url_for("csc.specification", ref=ref))
    flash("Specification record opened. Enter the parameters and save to publish version 1.", "success")
    return redirect(url_for("csc.edit_specification", ref=ref))


@csc_bp.route("/specification/<ref>/edit", methods=["GET", "POST"])
@login_required
@module_access_required("csc")
@module_admin_required("csc")
def edit_specification(ref: str):
    from app.core.services.corporate_specifications import (
        IMPACT_FLAG_VALUES, NARRATIVE_SECTIONS, PARAMETER_TYPES, SpecificationError,
        parameter_payload, save_specification, supporting_payload,
    )

    data = _entry_or_404(ref)
    record = data["record"]
    if record is None:
        flash("Open a specification record for this chemical before editing it.", "info")
        return redirect(url_for("csc.specification", ref=ref))

    if request.method == "POST":
        parameters = parameter_payload(request.form)
        narratives = {key: request.form.get(f"section_{key}", "") for key, _label in NARRATIVE_SECTIONS}
        supporting = supporting_payload(request.form)
        try:
            created, version = save_specification(
                record,
                chemical_name=request.form.get("chemical_name", ""),
                spec_number=request.form.get("spec_number", ""),
                material_code=request.form.get("material_code", ""),
                test_procedure=request.form.get("test_procedure", ""),
                narratives=narratives,
                parameters=parameters,
                supporting=supporting,
                reason=request.form.get("reason", ""),
                username=_current_username(),
                user_id=current_user.id,
            )
            db.session.commit()
        except SpecificationError as exc:
            db.session.rollback()
            flash(str(exc), "warning")
            return render_template(
                "csc/specification_edit.html",
                parameter_types=PARAMETER_TYPES,
                narrative_sections=NARRATIVE_SECTIONS,
                impact_values=IMPACT_FLAG_VALUES,
                submitted={
                    "chemical_name": request.form.get("chemical_name", ""),
                    "spec_number": request.form.get("spec_number", ""),
                    "material_code": request.form.get("material_code", ""),
                    "test_procedure": request.form.get("test_procedure", ""),
                    "reason": request.form.get("reason", ""),
                    "narratives": narratives,
                    "parameters": parameters,
                    "supporting": supporting,
                },
                **data,
            )
        except Exception:
            db.session.rollback()
            logger.exception("Could not save specification ref=%s", ref)
            flash("The specification could not be saved. Please try again.", "danger")
            return redirect(url_for("csc.edit_specification", ref=ref))

        if created:
            flash(f"Specification revised and published as version {version}.", "success")
        else:
            flash("Saved. Nothing substantive changed, so the version is unchanged.", "info")
        return redirect(url_for("csc.specification", ref=ref))

    return render_template(
        "csc/specification_edit.html",
        parameter_types=PARAMETER_TYPES,
        narrative_sections=NARRATIVE_SECTIONS,
        impact_values=IMPACT_FLAG_VALUES,
        submitted=None,
        **data,
    )


# ── Master export ────────────────────────────────────────────────────────────


@csc_bp.route("/master-export")
@login_required
@module_access_required("csc")
def master_export():
    from app.core.services.corporate_specifications import export_categories, landing_data

    data = landing_data()
    categories = export_categories()
    return render_template(
        "csc/master_export.html",
        categories=categories,
        chemical_total=data["chemical_total"],
        specification_total=data["specification_total"],
        document_total=sum(option["count"] for option in categories),
        parameter_total=data["parameter_total"],
    )


@csc_bp.route("/master-export/register.xlsx")
@login_required
@module_access_required("csc")
def export_register_workbook():
    from app.core.services.corporate_specifications import build_register_workbook

    try:
        stream, filename = build_register_workbook()
    except Exception:
        logger.exception("Corporate specification register export failed")
        flash("The register workbook could not be generated.", "danger")
        return redirect(url_for("csc.master_export"))
    return send_file(
        stream,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        max_age=0,
    )


@csc_bp.route("/master-export/specifications.docx")
@login_required
@module_access_required("csc")
def export_master_document():
    from app.core.services.corporate_specifications import export_bundles
    from app.core.services.csc_export import build_master_spec_document

    selected = (request.args.get("category") or "").strip().upper()
    try:
        bundles = export_bundles(selected or None)
        if not bundles:
            flash("No specifications with recorded parameters matched that category.", "warning")
            return redirect(url_for("csc.master_export"))
        document = build_master_spec_document(
            bundles,
            include_draft_note=request.args.get("dossier") == "1",
            include_metadata=request.args.get("metadata") == "1",
        )
    except Exception:
        logger.exception("Corporate specification master document export failed")
        flash("The master specification document could not be generated.", "danger")
        return redirect(url_for("csc.master_export"))
    filename = (
        f"ONGC_Corporate_Specifications_{selected or 'ALL'}_{datetime.now().strftime('%Y%m%d_%H%M')}.docx"
    )
    return send_file(
        io.BytesIO(document),
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        max_age=0,
    )


# ── MSDS Center ──────────────────────────────────────────────────────────────


def _row_value(row, key: str) -> str:
    """Master rows arrive as dicts today and as models in older call sites."""
    value = row.get(key) if isinstance(row, dict) else getattr(row, key, "")
    return str(value or "").strip()


def _msds_selector_options(rows) -> list[dict[str, str]]:
    """Material picker ordered by specification, then any remaining master material."""
    from app.core.services.corporate_specifications import catalogue

    descriptions = {
        _row_value(row, "material"): _row_value(row, "short_text")
        for row in rows
        if _row_value(row, "material")
    }
    options: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in catalogue():
        code = entry["material_code"]
        if not code or code in seen:
            continue
        seen.add(code)
        options.append(
            {
                "value": code,
                "spec_number": entry["spec_number"] if entry["spec_number"] != "—" else "",
                "material_code": code,
                "description": descriptions.get(code) or entry["chemical_name"],
            }
        )
    for code in sorted(code for code in descriptions if code not in seen):
        options.append(
            {"value": code, "spec_number": "", "material_code": code, "description": descriptions[code] or "—"}
        )
    return options


@csc_bp.route("/msds")
@login_required
@module_access_required("csc")
def msds_page():
    """MSDS Center within Corporate Specifications Management."""
    from app.core.services.master_data import get_all_master_data
    from app.core.services.msds_service import MSDSError, get_msds_material_index, get_msds_slot_options

    rows = []
    try:
        rows = get_all_master_data()
    except Exception:
        logger.exception("Failed to load material master rows for the MSDS Center")
        flash("Material master rows could not be loaded right now.", "warning")
    try:
        msds_by_material = get_msds_material_index(
            [_row_value(row, "material") for row in rows if _row_value(row, "material")]
        )
    except MSDSError as exc:
        flash(str(exc), "warning")
        msds_by_material = {}
    return render_template(
        "csc/msds.html",
        rows=rows,
        material_selector_options=_msds_selector_options(rows),
        msds_slot_options=get_msds_slot_options(),
        total=len(rows),
        msds_by_material=msds_by_material,
        msds_count=sum(len(files) for files in msds_by_material.values()),
        msds_material_total=len(msds_by_material),
        prefill_material_code=(request.args.get("material_code") or "").strip(),
        prefill_slot_code=(request.args.get("slot_code") or "").strip().lower() or "standard",
    )


@csc_bp.route("/msds/upload", methods=["POST"])
@login_required
@module_access_required("csc")
def upload_msds():
    from app.core.services.msds_service import MSDSError, store_msds_document

    material_code = request.form.get("material_code", "").strip()
    slot_code = request.form.get("slot_code", "").strip().lower()
    try:
        msds_file = store_msds_document(
            material_code=material_code,
            file_obj=request.files.get("msds_file"),
            slot_code=slot_code,
        )
        db.session.commit()
    except MSDSError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
        return redirect(url_for("csc.msds_page", material_code=material_code, slot_code=slot_code))
    except Exception:
        db.session.rollback()
        logger.exception("Failed to upload MSDS PDF for material=%s", material_code)
        flash("Could not upload the MSDS PDF.", "danger")
        return redirect(url_for("csc.msds_page", material_code=material_code, slot_code=slot_code))

    action = "replaced" if getattr(msds_file, "storage_action", "") == "replaced" else "stored"
    flash(f"{msds_file.slot_label} {action} for material '{material_code}'.", "success")
    return redirect(url_for("csc.msds_page", material_code=material_code, slot_code=slot_code))


@csc_bp.route("/msds/<int:file_id>")
@login_required
@module_access_required("csc")
def open_msds(file_id: int):
    from app.core.services.msds_service import MSDSError, MSDSNotFoundError, get_msds_file

    download = (request.args.get("download") or "").strip().lower() in {"1", "true", "yes"}
    try:
        document = get_msds_file(file_id, include_data=True)
    except MSDSNotFoundError as exc:
        abort(404, description=str(exc))
    except MSDSError as exc:
        abort(500, description=str(exc))

    return send_file(
        io.BytesIO(document.data),
        mimetype=document.content_type or "application/pdf",
        as_attachment=download,
        download_name=document.filename,
        max_age=0,
    )


@csc_bp.route("/msds/<int:file_id>/delete", methods=["POST"])
@login_required
@module_access_required("csc")
@module_admin_required("csc")
def delete_msds(file_id: int):
    from app.core.services.msds_service import MSDSError, delete_msds_file, get_msds_file

    try:
        msds_file = get_msds_file(file_id)
        if not delete_msds_file(file_id):
            flash("The selected MSDS file no longer exists.", "info")
            return redirect(url_for("csc.msds_page"))
        db.session.commit()
    except MSDSError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
        return redirect(url_for("csc.msds_page"))
    except Exception:
        db.session.rollback()
        logger.exception("Failed to delete MSDS PDF for file_id=%s", file_id)
        flash("Could not delete the MSDS PDF.", "danger")
        return redirect(url_for("csc.msds_page"))

    flash(
        f"{msds_file.slot_label} '{msds_file.filename}' deleted for material '{msds_file.material_code}'.",
        "success",
    )
    return redirect(url_for("csc.msds_page", material_code=msds_file.material_code))
