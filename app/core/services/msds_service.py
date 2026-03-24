"""Database-backed MSDS storage and retrieval helpers."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path

from flask import current_app
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import load_only, undefer
from werkzeug.datastructures import FileStorage

from app.extensions import db
from app.models.inventory.material_master import MaterialMaster
from app.models.inventory.msds_file import (
    MSDSFile,
    MSDS_SLOT_LABELS,
    MSDS_SLOT_ORDER,
    MSDS_SLOT_STANDARD,
)


class MSDSError(RuntimeError):
    """Raised when MSDS storage or validation fails."""


class MSDSNotFoundError(MSDSError):
    """Raised when the requested MSDS file does not exist."""


MYSQL_BLOB_TYPE_LIMITS = {
    "tinyblob": 255,
    "blob": 65_535,
    "mediumblob": 16_777_215,
    "longblob": 4_294_967_295,
}


def _metadata_options():
    return (
        load_only(
            MSDSFile.id,
            MSDSFile.material_code,
            MSDSFile.slot_code,
            MSDSFile.filename,
            MSDSFile.content_type,
            MSDSFile.file_size,
            MSDSFile.uploaded_at,
        ),
    )


def _metadata_query():
    return MSDSFile.query.options(*_metadata_options()).order_by(
        MSDSFile.material_code.asc(),
        MSDSFile.uploaded_at.desc(),
        MSDSFile.id.desc(),
    )


def _normalize_material_code(material_code: str | None) -> str:
    return (material_code or "").strip()


def _normalize_filename(filename: str | None) -> str:
    clean_name = (filename or "").strip().replace("\\", "/").split("/")[-1]
    return clean_name or "msds.pdf"


def _normalize_slot_code(slot_code: str | None) -> str:
    normalized = (slot_code or "").strip().lower()
    return normalized or MSDS_SLOT_STANDARD


def _raw_slot_code(slot_code: str | None) -> str:
    return (slot_code or "").strip().lower()


def _validate_slot_code(slot_code: str) -> str:
    normalized = _normalize_slot_code(slot_code)
    if normalized not in MSDS_SLOT_ORDER:
        raise MSDSError("Select a valid MSDS slot.")
    return normalized


def _slot_sort_key(slot_code: str | None) -> tuple[int, str]:
    normalized = _normalize_slot_code(slot_code)
    if normalized in MSDS_SLOT_ORDER:
        return (MSDS_SLOT_ORDER.index(normalized), normalized)
    return (len(MSDS_SLOT_ORDER), normalized)


def get_msds_slot_options() -> list[dict[str, str]]:
    return [
        {"value": slot_code, "label": MSDS_SLOT_LABELS[slot_code]}
        for slot_code in MSDS_SLOT_ORDER
    ]


def _next_available_slot_code(material_code: str) -> str:
    existing_slot_codes = {
        _raw_slot_code(slot_code)
        for (slot_code,) in db.session.query(MSDSFile.slot_code)
        .filter_by(material_code=material_code)
        .all()
    }
    for slot_code in MSDS_SLOT_ORDER:
        if slot_code not in existing_slot_codes:
            return slot_code

    legacy_index = 1
    while True:
        legacy_slot = f"legacy_{legacy_index}"
        if legacy_slot not in existing_slot_codes:
            return legacy_slot
        legacy_index += 1


def _validate_material_exists(material_code: str) -> MaterialMaster:
    material = db.session.get(MaterialMaster, material_code)
    if material is None:
        raise MSDSError(f"Material '{material_code}' was not found in material master.")
    return material


def _validate_pdf_bytes(file_bytes: bytes, filename: str) -> None:
    if not file_bytes:
        raise MSDSError("Uploaded PDF is empty.")

    max_bytes = int(current_app.config.get("MSDS_MAX_UPLOAD_BYTES", 10 * 1024 * 1024))
    if len(file_bytes) > max_bytes:
        raise MSDSError(
            f"MSDS file exceeds the {max_bytes // (1024 * 1024)} MB upload limit."
        )
    if not filename.lower().endswith(".pdf"):
        raise MSDSError("MSDS upload must be a PDF file.")
    if not file_bytes.startswith(b"%PDF"):
        raise MSDSError("Uploaded file does not appear to be a valid PDF.")


def _infer_mysql_blob_capacity(column_type: object) -> tuple[str | None, int | None]:
    column_label = str(column_type or "").strip()
    normalized = column_label.lower().replace(" ", "")
    for blob_type, limit in (
        ("tinyblob", MYSQL_BLOB_TYPE_LIMITS["tinyblob"]),
        ("mediumblob", MYSQL_BLOB_TYPE_LIMITS["mediumblob"]),
        ("longblob", MYSQL_BLOB_TYPE_LIMITS["longblob"]),
        ("blob", MYSQL_BLOB_TYPE_LIMITS["blob"]),
    ):
        if blob_type in normalized:
            return blob_type.upper(), limit
    return (column_label or None, None)


def _format_capacity_label(size_bytes: int) -> str:
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.0f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.0f} KB"
    return f"{size_bytes} bytes"


def get_msds_storage_diagnostics() -> dict[str, object]:
    inspector = inspect(db.engine)
    payload: dict[str, object] = {
        "dialect_name": inspector.bind.dialect.name,
        "table_present": False,
        "data_column_type": None,
        "data_column_capacity_bytes": None,
    }
    if "msds_files" not in inspector.get_table_names():
        return payload

    payload["table_present"] = True
    for column in inspector.get_columns("msds_files"):
        if column.get("name") != "data":
            continue
        column_type = column.get("type")
        payload["data_column_type"] = str(column_type)
        column_type_label, limit = _infer_mysql_blob_capacity(column_type)
        if column_type_label and not payload["data_column_type"]:
            payload["data_column_type"] = column_type_label
        payload["data_column_capacity_bytes"] = limit
        break
    return payload


def _raise_for_storage_capacity(file_size: int) -> None:
    try:
        storage = get_msds_storage_diagnostics()
    except SQLAlchemyError as exc:
        raise _msds_query_error(exc) from exc

    if storage.get("dialect_name") != "mysql":
        return
    capacity = storage.get("data_column_capacity_bytes")
    if not isinstance(capacity, int) or file_size <= capacity:
        return
    column_type = storage.get("data_column_type") or "BLOB"
    raise MSDSError(
        "MSDS storage is still using "
        f"{column_type} with a limit of {_format_capacity_label(capacity)}. "
        "Apply the MSDS blob-widening migration and retry the upload."
    )


def _msds_query_error(exc: Exception) -> MSDSError:
    current_app.logger.warning("MSDS query failed", exc_info=True)
    message = str(exc).lower()
    if "msds_files" in message and (
        "doesn't exist" in message
        or "does not exist" in message
        or "undefined table" in message
        or "relation" in message
    ):
        return MSDSError(
            "MSDS storage is not available in this environment yet. "
            "Apply the database migration and try again."
        )
    return MSDSError("MSDS data could not be loaded right now.")


def _msds_write_error(exc: Exception) -> MSDSError:
    current_app.logger.warning("MSDS write failed", exc_info=True)
    message = str(exc).lower()
    if "data too long" in message or "data truncated" in message:
        try:
            storage = get_msds_storage_diagnostics()
        except Exception:
            storage = {}
        capacity = storage.get("data_column_capacity_bytes")
        column_type = storage.get("data_column_type") or "database binary column"
        if isinstance(capacity, int):
            return MSDSError(
                "MSDS upload is larger than the current "
                f"{column_type} limit of {_format_capacity_label(capacity)}. "
                "Apply the MSDS blob-widening migration and retry."
            )
        return MSDSError(
            "MSDS upload could not be stored because the database binary column is too small. "
            "Apply the latest MSDS migration and retry."
        )
    if "max_allowed_packet" in message or "packet for query is too large" in message:
        return MSDSError(
            "MSDS upload exceeds the current MySQL packet limit. "
            "Increase max_allowed_packet and retry."
        )
    return MSDSError("MSDS PDF could not be stored right now.")


def list_msds_files(
    material_code: str | None = None,
    material_codes: Sequence[str] | None = None,
) -> list[MSDSFile]:
    try:
        query = _metadata_query()
        if material_code:
            query = query.filter_by(material_code=_normalize_material_code(material_code))
        elif material_codes:
            clean_codes = [code for code in {_normalize_material_code(code) for code in material_codes} if code]
            if not clean_codes:
                return []
            query = query.filter(MSDSFile.material_code.in_(clean_codes))
        return query.all()
    except SQLAlchemyError as exc:
        raise _msds_query_error(exc) from exc


def get_msds_material_index(
    material_codes: Sequence[str] | None = None,
) -> dict[str, list[MSDSFile]]:
    grouped: dict[str, list[MSDSFile]] = {}
    for msds_file in list_msds_files(material_codes=material_codes):
        grouped.setdefault(msds_file.material_code, []).append(msds_file)
    for files in grouped.values():
        files.sort(
            key=lambda item: (
                _slot_sort_key(item.slot_code),
                -(item.uploaded_at.timestamp() if item.uploaded_at else 0),
                -int(item.id or 0),
            )
        )
    return grouped


def get_latest_msds_file(material_code: str) -> MSDSFile | None:
    files = list_msds_files(material_code=material_code)
    if not files:
        return None
    files.sort(
        key=lambda item: (
            _slot_sort_key(item.slot_code),
            -(item.uploaded_at.timestamp() if item.uploaded_at else 0),
            -int(item.id or 0),
        )
    )
    return files[0]


def get_msds_file(file_id: int, include_data: bool = False) -> MSDSFile:
    try:
        query = MSDSFile.query
        if not include_data:
            query = query.options(*_metadata_options())
        else:
            query = query.options(undefer(MSDSFile.data))
        msds_file = query.filter_by(id=file_id).first()
    except SQLAlchemyError as exc:
        raise _msds_query_error(exc) from exc

    if msds_file is None:
        raise MSDSNotFoundError(f"MSDS file '{file_id}' was not found.")
    return msds_file


def store_msds_bytes(
    material_code: str,
    filename: str,
    file_bytes: bytes,
    slot_code: str | None = None,
    content_type: str | None = None,
    uploaded_at: datetime | None = None,
) -> MSDSFile:
    clean_material = _normalize_material_code(material_code)
    if not clean_material:
        raise MSDSError("Material code is required.")

    safe_filename = _normalize_filename(filename)
    _validate_material_exists(clean_material)
    _validate_pdf_bytes(file_bytes, safe_filename)
    _raise_for_storage_capacity(len(file_bytes))

    raw_slot = _raw_slot_code(slot_code)
    explicit_slot = bool(raw_slot)
    clean_slot = _validate_slot_code(raw_slot) if explicit_slot else _next_available_slot_code(clean_material)

    timestamp = uploaded_at or datetime.now(timezone.utc)
    existing_files = []
    if explicit_slot:
        existing_files = (
            MSDSFile.query.filter_by(material_code=clean_material, slot_code=clean_slot)
            .order_by(MSDSFile.uploaded_at.desc(), MSDSFile.id.desc())
            .all()
        )
    msds_file = existing_files[0] if existing_files else None

    if msds_file is None:
        msds_file = MSDSFile(
            material_code=clean_material,
            slot_code=clean_slot,
            filename=safe_filename,
            content_type=(content_type or "application/pdf").strip() or "application/pdf",
            file_size=len(file_bytes),
            uploaded_at=timestamp,
            data=file_bytes,
        )
        db.session.add(msds_file)
        msds_file.storage_action = "created"
    else:
        msds_file.slot_code = clean_slot
        msds_file.filename = safe_filename
        msds_file.content_type = (content_type or "application/pdf").strip() or "application/pdf"
        msds_file.file_size = len(file_bytes)
        msds_file.uploaded_at = timestamp
        msds_file.data = file_bytes
        msds_file.storage_action = "replaced"

        for duplicate in existing_files[1:]:
            db.session.delete(duplicate)

    try:
        db.session.flush()
    except SQLAlchemyError as exc:
        raise _msds_write_error(exc) from exc
    return msds_file


def store_msds_document(
    material_code: str,
    file_obj: FileStorage,
    slot_code: str = MSDS_SLOT_STANDARD,
) -> MSDSFile:
    if file_obj is None or not file_obj.filename:
        raise MSDSError("Select a PDF file before uploading.")

    return store_msds_bytes(
        material_code=material_code,
        filename=file_obj.filename,
        file_bytes=file_obj.read(),
        slot_code=slot_code,
        content_type=file_obj.mimetype,
    )


def delete_msds_file(file_id: int) -> bool:
    try:
        msds_file = MSDSFile.query.options(*_metadata_options()).filter_by(id=file_id).first()
    except SQLAlchemyError as exc:
        raise _msds_query_error(exc) from exc

    if msds_file is None:
        return False

    db.session.delete(msds_file)
    db.session.flush()
    return True


def msds_file_exists(
    material_code: str,
    filename: str,
    file_size: int,
    uploaded_at: datetime | None = None,
) -> bool:
    try:
        query = MSDSFile.query.options(load_only(MSDSFile.id)).filter_by(
            material_code=_normalize_material_code(material_code),
            filename=_normalize_filename(filename),
            file_size=file_size,
        )
        if uploaded_at is not None:
            query = query.filter(MSDSFile.uploaded_at == uploaded_at)
        return query.first() is not None
    except SQLAlchemyError as exc:
        raise _msds_query_error(exc) from exc


def get_legacy_msds_storage_root() -> Path:
    configured = (current_app.config.get("MSDS_STORAGE_DIR") or "").strip()
    base_dir = Path(current_app.root_path).resolve().parent
    root = Path(configured) if configured else (base_dir / "storage" / "msds")
    if not root.is_absolute():
        root = (base_dir / root).resolve()
    return root


def has_legacy_msds_table() -> bool:
    inspector = inspect(db.engine)
    return "inventory_msds_documents" in inspector.get_table_names()


def iter_legacy_msds_records() -> Iterable[dict]:
    if not has_legacy_msds_table():
        raise MSDSError("Legacy MSDS metadata table 'inventory_msds_documents' was not found.")

    result = db.session.execute(
        text(
            """
            SELECT
                material_code,
                original_filename,
                mime_type,
                file_size_bytes,
                uploaded_at,
                storage_path
            FROM inventory_msds_documents
            ORDER BY material_code ASC, uploaded_at DESC
            """
        )
    )
    for row in result.mappings():
        yield dict(row)
