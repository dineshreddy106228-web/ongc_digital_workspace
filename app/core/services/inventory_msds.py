"""Compatibility wrapper for MSDS service imports."""

from app.core.services.msds_service import (  # noqa: F401
    MSDSError,
    MSDSNotFoundError,
    delete_msds_file,
    get_latest_msds_file,
    get_legacy_msds_storage_root,
    get_msds_file,
    get_msds_material_index,
    has_legacy_msds_table,
    iter_legacy_msds_records,
    list_msds_files,
    msds_file_exists,
    store_msds_bytes,
    store_msds_document,
)
