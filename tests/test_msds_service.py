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


@pytest.fixture()
def msds_app(tmp_path):
    from sqlalchemy import Integer

    from app import create_app
    from app.extensions import db
    from config import Config

    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'msds.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        from app.models.inventory.msds_file import MSDSFile

        # SQLite only autoincrements INTEGER primary keys; MySQL keeps BIGINT.
        MSDSFile.__table__.c.id.type = Integer()
        db.create_all()
        yield app
        db.session.remove()


def test_store_msds_bytes_ignores_the_uploaded_content_type(msds_app) -> None:
    """The viewer serves MSDS files inline, so an upload must not pick the media type."""
    from app.extensions import db
    from app.models.inventory.material_master import MaterialMaster
    from app.models.inventory.msds_file import MSDSFile

    db.session.add(MaterialMaster(material="090001043"))
    db.session.flush()

    stored = msds_service.store_msds_bytes(
        material_code="090001043",
        filename="datasheet.pdf",
        file_bytes=b"%PDF-1.4\n<script>alert(1)</script>",
        slot_code="standard",
        content_type="text/html",
    )

    assert stored.content_type == "application/pdf"

    replaced = msds_service.store_msds_bytes(
        material_code="090001043",
        filename="datasheet.pdf",
        file_bytes=b"%PDF-1.4\nrevised",
        slot_code="standard",
        content_type="image/svg+xml",
    )

    assert replaced.storage_action == "replaced"
    assert replaced.content_type == "application/pdf"
    assert db.session.get(MSDSFile, stored.id).content_type == "application/pdf"
