from __future__ import annotations

from pathlib import Path
import sys

import pytest
from sqlalchemy.dialects import mysql

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services import msds_service
from app.core.services.msds_service import MSDSError


def test_infer_mysql_blob_capacity_maps_known_blob_types() -> None:
    assert msds_service._infer_mysql_blob_capacity(mysql.BLOB()) == ("BLOB", 65_535)
    assert msds_service._infer_mysql_blob_capacity(mysql.LONGBLOB()) == (
        "LONGBLOB",
        4_294_967_295,
    )


def test_raise_for_storage_capacity_rejects_oversized_mysql_blob(monkeypatch) -> None:
    monkeypatch.setattr(
        msds_service,
        "get_msds_storage_diagnostics",
        lambda: {
            "dialect_name": "mysql",
            "table_present": True,
            "data_column_type": "BLOB",
            "data_column_capacity_bytes": 65_535,
        },
    )

    with pytest.raises(MSDSError, match="64 KB"):
        msds_service._raise_for_storage_capacity(92 * 1024)


def test_raise_for_storage_capacity_ignores_non_mysql_backends(monkeypatch) -> None:
    monkeypatch.setattr(
        msds_service,
        "get_msds_storage_diagnostics",
        lambda: {
            "dialect_name": "sqlite",
            "table_present": True,
            "data_column_type": "BLOB",
            "data_column_capacity_bytes": 65_535,
        },
    )

    msds_service._raise_for_storage_capacity(300 * 1024)
