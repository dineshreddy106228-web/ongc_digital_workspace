"""MSDS-specific inventory routes."""

from __future__ import annotations

import logging
from io import BytesIO

from flask import abort, flash, redirect, render_template, request, send_file, url_for
from flask_login import login_required

from app.core.utils.decorators import module_access_required, superuser_required
from app.extensions import db
from app.modules.inventory import inventory_bp

logger = logging.getLogger(__name__)


def _row_material_code(row) -> str:
    if isinstance(row, dict):
        return str(row.get("material") or "").strip()
    return str(getattr(row, "material", "") or "").strip()


@inventory_bp.route("/msds")
@login_required
@superuser_required
def msds_page():
    """Dedicated MSDS upload and management page for superusers."""
    from app.core.services.master_data import get_all_master_data
    from app.core.services.msds_service import MSDSError, get_msds_material_index

    rows = []
    try:
        rows = get_all_master_data()
    except Exception:
        logger.exception("Failed to load material master rows for Inventory MSDS Center")
        flash("Material master rows could not be loaded right now.", "warning")

    msds_by_material = {}
    try:
        msds_by_material = get_msds_material_index(
            [_row_material_code(row) for row in rows if _row_material_code(row)]
        )
    except MSDSError as exc:
        flash(str(exc), "warning")

    return render_template(
        "inventory/msds.html",
        rows=rows,
        total=len(rows),
        msds_by_material=msds_by_material,
        msds_count=sum(len(files) for files in msds_by_material.values()),
        msds_material_total=len(msds_by_material),
        prefill_material_code=(request.args.get("material_code") or "").strip(),
    )


@inventory_bp.route("/msds/upload", methods=["POST"])
@inventory_bp.route("/master-data/msds/upload", methods=["POST"])
@login_required
@superuser_required
def upload_msds():
    from app.core.services.msds_service import MSDSError, store_msds_document

    material_code = request.form.get("material_code", "").strip()
    file_obj = request.files.get("msds_file")

    try:
        store_msds_document(material_code=material_code, file_obj=file_obj)
        db.session.commit()
    except MSDSError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
        return redirect(url_for("inventory.msds_page", material_code=material_code))
    except Exception:
        db.session.rollback()
        logger.exception("Failed to upload MSDS PDF for material=%s", material_code)
        flash("Could not upload the MSDS PDF.", "danger")
        return redirect(url_for("inventory.msds_page", material_code=material_code))

    flash(f"MSDS PDF stored for material '{material_code}'.", "success")
    return redirect(url_for("inventory.msds_page", material_code=material_code))


@inventory_bp.route("/msds/<int:file_id>")
@login_required
@module_access_required("inventory")
def get_msds_file(file_id: int):
    from app.core.services.msds_service import (
        MSDSError,
        MSDSNotFoundError,
        get_msds_file as fetch_msds_file,
    )

    download = (request.args.get("download") or "").strip().lower() in {"1", "true", "yes"}
    try:
        msds_file = fetch_msds_file(file_id, include_data=True)
    except MSDSNotFoundError as exc:
        abort(404, description=str(exc))
    except MSDSError as exc:
        abort(500, description=str(exc))

    return send_file(
        BytesIO(msds_file.data),
        mimetype=msds_file.content_type or "application/pdf",
        as_attachment=download,
        download_name=msds_file.filename,
        max_age=0,
    )


@inventory_bp.route("/master-data/msds/<path:material_code>")
@login_required
@module_access_required("inventory")
def open_latest_msds(material_code: str):
    from app.core.services.msds_service import MSDSError, get_latest_msds_file

    try:
        msds_file = get_latest_msds_file(material_code)
    except MSDSError as exc:
        flash(str(exc), "warning")
        return redirect(request.referrer or url_for("inventory.materials_page"))

    if msds_file is None:
        flash(f"No MSDS PDF is stored for material '{material_code}'.", "warning")
        return redirect(request.referrer or url_for("inventory.materials_page"))

    route_kwargs = {"file_id": msds_file.id}
    if request.args.get("download") is not None:
        route_kwargs["download"] = request.args.get("download")
    return redirect(url_for("inventory.get_msds_file", **route_kwargs))


@inventory_bp.route("/msds/<int:file_id>/delete", methods=["POST"])
@login_required
@superuser_required
def delete_msds(file_id: int):
    from app.core.services.msds_service import (
        MSDSError,
        delete_msds_file,
        get_msds_file as fetch_msds_file,
    )

    try:
        msds_file = fetch_msds_file(file_id)
        deleted = delete_msds_file(file_id)
        if not deleted:
            flash("The selected MSDS file no longer exists.", "info")
            return redirect(url_for("inventory.msds_page"))
        db.session.commit()
    except MSDSError as exc:
        db.session.rollback()
        flash(str(exc), "danger")
        return redirect(url_for("inventory.msds_page"))
    except Exception:
        db.session.rollback()
        logger.exception("Failed to delete MSDS PDF for file_id=%s", file_id)
        flash("Could not delete the MSDS PDF.", "danger")
        return redirect(url_for("inventory.msds_page"))

    flash(
        f"MSDS PDF '{msds_file.filename}' deleted for material '{msds_file.material_code}'.",
        "success",
    )
    return redirect(url_for("inventory.msds_page", material_code=msds_file.material_code))
