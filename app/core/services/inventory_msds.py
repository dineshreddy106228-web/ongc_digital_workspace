"""Filesystem-backed MSDS storage with DB metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import os
from pathlib import Path
import re

from flask import current_app
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename

from app.extensions import db
from app.models.inventory.material_master import MaterialMaster
from app.models.inventory.material_msds_document import MaterialMSDSDocument


class MSDSError(RuntimeError):
    """Raised when an MSDS file is invalid or storage fails."""


@dataclass(frozen=True)
class StoredMSDSFile:
    material_code: str
    original_filename: str
    stored_filename: str
    relative_path: str
    file_size_bytes: int
    sha256_hex: str
    mime_type: str = "application/pdf"


def _repo_root() -> Path:
    return Path(current_app.root_path).resolve().parent


def get_msds_storage_root() -> Path:
    configured = (current_app.config.get("MSDS_STORAGE_DIR") or "").strip()
    root = Path(configured) if configured else (_repo_root() / "storage" / "msds")
    if not root.is_absolute():
        root = (_repo_root() / root).resolve()
    return root


def ensure_msds_storage_root() -> Path:
    root = get_msds_storage_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _material_slug(material_code: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "-", (material_code or "").strip())
    clean = clean.strip("-._")
    return clean or "material"


def _validate_material_exists(material_code: str) -> MaterialMaster:
    material = MaterialMaster.query.filter_by(material=material_code).first()
    if material is None:
        raise MSDSError(f"Material '{material_code}' was not found in material master.")
    return material


def _validate_pdf_bytes(file_bytes: bytes, original_filename: str) -> None:
    if not file_bytes:
        raise MSDSError("Uploaded PDF is empty.")
    max_bytes = int(current_app.config.get("MSDS_MAX_UPLOAD_BYTES", 20 * 1024 * 1024))
    if len(file_bytes) > max_bytes:
        raise MSDSError(
            f"MSDS file exceeds the {max_bytes // (1024 * 1024)} MB upload limit."
        )
    if not (original_filename or "").lower().endswith(".pdf"):
        raise MSDSError("MSDS upload must be a PDF file.")
    if not file_bytes.startswith(b"%PDF"):
        raise MSDSError("Uploaded file does not appear to be a valid PDF.")


def _relative_storage_path(material_code: str, original_filename: str) -> tuple[str, str]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    safe_name = secure_filename(original_filename or "msds.pdf") or "msds.pdf"
    stored_filename = f"{timestamp}_{safe_name}"
    relative_path = Path(_material_slug(material_code)) / stored_filename
    return stored_filename, str(relative_path)


def _absolute_storage_path(relative_path: str) -> Path:
    return ensure_msds_storage_root() / relative_path


def _delete_file_if_present(path_value: str | None) -> None:
    if not path_value:
        return
    try:
        _absolute_storage_path(path_value).unlink(missing_ok=True)
    except OSError as exc:
        raise MSDSError(f"Could not remove stored MSDS file: {exc}") from exc


def _msds_metadata_unavailable_error(exc: Exception) -> MSDSError:
    current_app.logger.warning("MSDS metadata query failed", exc_info=True)
    message = str(exc).lower()
    if (
        "inventory_msds_documents" in message
        and ("doesn't exist" in message or "does not exist" in message or "1146" in message)
    ):
        return MSDSError(
            "MSDS metadata is not available in this environment yet. "
            "Apply the MSDS database migration on Railway and try again."
        )
    return MSDSError(
        "MSDS metadata could not be loaded right now. "
        "Check the Railway application logs for the exact database error."
    )


def list_msds_documents() -> list[MaterialMSDSDocument]:
    try:
        return MaterialMSDSDocument.query.order_by(
            MaterialMSDSDocument.material_code.asc()
        ).all()
    except SQLAlchemyError as exc:
        raise _msds_metadata_unavailable_error(exc) from exc


def get_msds_document(material_code: str) -> MaterialMSDSDocument | None:
    try:
        return MaterialMSDSDocument.query.filter_by(material_code=material_code).first()
    except SQLAlchemyError as exc:
        raise _msds_metadata_unavailable_error(exc) from exc


def store_msds_document(
    material_code: str,
    file_obj: FileStorage,
    uploaded_by: int | None = None,
) -> MaterialMSDSDocument:
    clean_material = (material_code or "").strip()
    if not clean_material:
        raise MSDSError("Material code is required.")
    if file_obj is None or not file_obj.filename:
        raise MSDSError("Select a PDF file before uploading.")

    _validate_material_exists(clean_material)
    file_bytes = file_obj.read()
    _validate_pdf_bytes(file_bytes, file_obj.filename)

    sha256_hex = hashlib.sha256(file_bytes).hexdigest()
    stored_filename, relative_path = _relative_storage_path(clean_material, file_obj.filename)
    absolute_path = _absolute_storage_path(relative_path)
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(file_bytes)

    document = get_msds_document(clean_material)
    previous_path = document.storage_path if document else None
    if document is None:
        document = MaterialMSDSDocument(material_code=clean_material)
        db.session.add(document)

    document.original_filename = file_obj.filename or "msds.pdf"
    document.stored_filename = stored_filename
    document.storage_path = relative_path
    document.mime_type = "application/pdf"
    document.file_size_bytes = len(file_bytes)
    document.sha256_hex = sha256_hex
    document.uploaded_by = uploaded_by
    document.uploaded_at = datetime.now(timezone.utc)
    db.session.flush()

    if previous_path and previous_path != relative_path:
        _delete_file_if_present(previous_path)

    return document


def open_msds_file(material_code: str) -> tuple[MaterialMSDSDocument, BytesIO]:
    document = get_msds_document(material_code)
    if document is None:
        raise MSDSError(f"No MSDS PDF is stored for material '{material_code}'.")

    absolute_path = _absolute_storage_path(document.storage_path)
    if not absolute_path.exists() or not absolute_path.is_file():
        raise MSDSError(
            f"Stored MSDS file is missing for material '{material_code}'."
        )
    return document, BytesIO(absolute_path.read_bytes())


def delete_msds_document(material_code: str) -> bool:
    document = get_msds_document(material_code)
    if document is None:
        return False

    storage_path = document.storage_path
    db.session.delete(document)
    db.session.flush()
    _delete_file_if_present(storage_path)
    return True
