"""Coverage for workbook retention.

Inventory and the weekly QC workbook keep their source for a 15-day rollback
window. SAP daily exports retain only the source pairs behind active laboratory
snapshots, whatever their age.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import sys

import pytest
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


@pytest.fixture()
def retention_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'workbook_retention.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        TESTING = True
        AUDIT_WORKBOOK_RETENTION_DAYS = 15
        AUDIT_WORKBOOK_RETENTION_ENABLED = False

    app = create_app(_Config)
    with app.app_context():
        # SQLite only autoincrements an INTEGER primary key; production uses
        # BIGINT for these audit tables.
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


def test_expired_payloads_are_cleared_but_audit_rows_and_recent_rollbacks_remain(retention_app):
    from app.core.services.audit_workbook_retention import purge_expired_audit_workbook_payloads
    from app.models.inventory.monitoring import InventoryMonitoringUploadBatch
    from app.models.quality_control.qc_sap_monitoring import QCSAPSourceDocument, QCSAPUploadBatch
    from app.models.quality_control.qc_upload_batch import QCUploadBatch

    reference = datetime(2026, 8, 28, 12, 0, 0)
    cutoff = reference - timedelta(days=15)
    old_time = cutoff - timedelta(seconds=1)
    current_time = cutoff

    old_inventory = InventoryMonitoringUploadBatch(
        source_group="09", reporting_date=date(2026, 8, 1),
        source_filename="inventory-old.xlsx", source_content_type="application/vnd.ms-excel",
        source_checksum="inventory-checksum", source_file_size=101, source_data=b"inventory-old",
        uploaded_at=old_time,
    )
    current_inventory = InventoryMonitoringUploadBatch(
        source_group="10", reporting_date=date(2026, 8, 2),
        source_filename="inventory-current.xlsx", source_content_type="application/vnd.ms-excel",
        source_checksum="inventory-current-checksum", source_file_size=102, source_data=b"inventory-current",
        uploaded_at=current_time,
    )
    old_weekly = QCUploadBatch(
        lab_code="rgl_panvel", lab_name="RGL Panvel", report_label="Week 31",
        week_start=date(2026, 8, 3), week_end=date(2026, 8, 7),
        source_filename="weekly-old.xlsx", source_content_type="application/vnd.ms-excel",
        source_file_size=201, source_data=b"weekly-old", uploaded_at=old_time,
    )
    current_weekly = QCUploadBatch(
        lab_code="rgl_panvel", lab_name="RGL Panvel", report_label="Week 32",
        week_start=date(2026, 8, 10), week_end=date(2026, 8, 14),
        source_filename="weekly-current.xlsx", source_content_type="application/vnd.ms-excel",
        source_file_size=202, source_data=b"weekly-current", uploaded_at=current_time,
    )
    # Both SAP pairs sit inside the rollback window. The older one is still
    # cleared because a newer snapshot for the same laboratory references the
    # current pair.
    old_sap = QCSAPSourceDocument(
        inspection_filename="inspection-old.xlsx", inspection_content_type="application/vnd.ms-excel",
        inspection_file_size=301, inspection_source_data=b"inspection-old",
        notification_filename="notifications-old.xlsx", notification_content_type="application/vnd.ms-excel",
        notification_file_size=302, notification_source_data=b"notifications-old", uploaded_at=old_time,
    )
    current_sap = QCSAPSourceDocument(
        inspection_filename="inspection-current.xlsx", inspection_content_type="application/vnd.ms-excel",
        inspection_file_size=303, inspection_source_data=b"inspection-current",
        notification_filename="notifications-current.xlsx", notification_content_type="application/vnd.ms-excel",
        notification_file_size=304, notification_source_data=b"notifications-current", uploaded_at=current_time,
    )
    db.session.add_all([
        old_inventory, current_inventory, old_weekly, current_weekly, old_sap, current_sap,
    ])
    db.session.flush()
    old_sap_batch = QCSAPUploadBatch(
        lab_code="rgl_panvel", plant_code="10R2", as_of_date=date(2026, 8, 27),
        source_document_id=old_sap.id,
        inspection_filename=old_sap.inspection_filename,
        inspection_content_type=old_sap.inspection_content_type,
        inspection_file_size=old_sap.inspection_file_size,
        notification_filename=old_sap.notification_filename,
        notification_content_type=old_sap.notification_content_type,
        notification_file_size=old_sap.notification_file_size,
    )
    current_sap_batch = QCSAPUploadBatch(
        lab_code="rgl_panvel", plant_code="10R2", as_of_date=date(2026, 8, 28),
        source_document_id=current_sap.id,
        inspection_filename=current_sap.inspection_filename,
        inspection_content_type=current_sap.inspection_content_type,
        inspection_file_size=current_sap.inspection_file_size,
        notification_filename=current_sap.notification_filename,
        notification_content_type=current_sap.notification_content_type,
        notification_file_size=current_sap.notification_file_size,
    )
    db.session.add_all([old_sap_batch, current_sap_batch])
    db.session.commit()

    counts = purge_expired_audit_workbook_payloads(now=reference)
    db.session.commit()
    db.session.expire_all()

    assert counts == {"inventory": 1, "qc_weekly": 1, "qc_sap": 1}
    assert old_inventory.source_data == b""
    assert old_inventory.source_purged_at == reference
    assert old_inventory.source_filename == "inventory-old.xlsx"
    assert old_inventory.source_file_size == 101
    assert old_weekly.source_data == b""
    assert old_weekly.source_purged_at == reference
    assert old_weekly.report_label == "Week 31"
    assert old_sap.inspection_source_data == b""
    assert old_sap.notification_source_data == b""
    assert old_sap.purged_at == reference
    assert old_sap.inspection_filename == "inspection-old.xlsx"
    assert current_inventory.source_data == b"inventory-current"
    assert current_inventory.source_purged_at is None
    assert current_weekly.source_data == b"weekly-current"
    assert current_weekly.source_purged_at is None
    assert current_sap.inspection_source_data == b"inspection-current"
    assert current_sap.notification_source_data == b"notifications-current"
    assert current_sap.purged_at is None

    # The sweep is safe to run repeatedly: a purged audit row is never touched
    # again and no fresh source is prematurely removed.
    assert purge_expired_audit_workbook_payloads(now=reference) == {
        "inventory": 0, "qc_weekly": 0, "qc_sap": 0,
    }
