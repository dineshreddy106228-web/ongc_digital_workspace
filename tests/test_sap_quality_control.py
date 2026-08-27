"""Tests for the Corporate Chemistry SAP QC control-tower workflow."""
from __future__ import annotations

from datetime import date
from io import BytesIO
from pathlib import Path
import sys

import pandas as pd
import pytest
from sqlalchemy import Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


def _xlsx(frame: pd.DataFrame, *, startrow: int = 0, title: str | None = None) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        if title:
            pd.DataFrame([[title]]).to_excel(writer, index=False, header=False)
        frame.to_excel(writer, index=False, startrow=startrow)
    return buffer.getvalue()


def _inspection_export(*, plant: str = "10R2") -> bytes:
    return _xlsx(pd.DataFrame([
        ["5001", "00001234", plant, "01.08.2026", "", "REL CALC", ""],
        ["5002", "00005678", plant, "02.08.2026", "25.08.2026", "UD ICCO STUP", "A"],
        ["5003", "00009999", plant, "03.08.2026", "", "REL CALC", ""],
    ], columns=[
        "Inspection Lot", "Material", "Plant", "Start of Inspection", "End of Inspection",
        "System Status", "Usage Decision Code",
    ]))


def _notification_export(*, plant: str = "10R2", first_work_center: str = "MUDLAB") -> bytes:
    return _xlsx(pd.DataFrame([
        ["7001", "OPEN", "45000001", "10", "00001234", "Barytes", first_work_center, plant, "5001", "REL CALC", "01.08.2026", "20.08.2026", "", "6"],
        ["7002", "COMP", "45000002", "10", "00005678", "Calcium carbonate", "OILLAB", plant, "5002", "UD ICCO", "02.08.2026", "24.08.2026", "25.08.2026", "1"],
        ["7003", "OPEN", "45000003", "10", "00007777", "Unlinked item", "WATERLAB", plant, "0", "", "04.08.2026", "21.08.2026", "", "5"],
    ], columns=[
        "Notification No", "Notification Status", "Purchasing Document", "Item", "Material Number",
        "Material Description", "Work Center", "Plant", "Inspection Lot Number", "Status of Inspection Lot",
        "Start Date", "Planned End Date", "Completion Date", "Delay Days",
    ]), startrow=4, title="Date : 26.08.2026")


def _central_inspection_export() -> bytes:
    return _xlsx(pd.DataFrame([
        ["5101", "00001234", "10R2", "26.08.2026", "", "REL CALC", ""],
        ["5201", "00005678", "42R2", "26.08.2026", "", "REL CALC", ""],
    ], columns=[
        "Inspection Lot", "Material", "Plant", "Start of Inspection", "End of Inspection",
        "System Status", "Usage Decision Code",
    ]))


def _central_notification_export() -> bytes:
    return _xlsx(pd.DataFrame([
        ["7101", "OPEN", "45000001", "10", "00001234", "Barytes", "MUDLAB", "10R2", "5101", "REL CALC", "26.08.2026", "30.08.2026", "", "0"],
        ["7201", "OPEN", "45000002", "10", "00005678", "Xylene", "QUALILAB", "42R2", "5201", "REL CALC", "26.08.2026", "30.08.2026", "", "0"],
        ["7301", "OPEN", "45000003", "10", "00009999", "Glycol", "MUDLAB", "51R2", "5301", "REL CALC", "26.08.2026", "30.08.2026", "", "0"],
    ], columns=[
        "Notification No", "Notification Status", "Purchasing Document", "Item", "Material Number",
        "Material Description", "Work Center", "Plant", "Inspection Lot Number", "Status of Inspection Lot",
        "Start Date", "Planned End Date", "Completion Date", "Delay Days",
    ]), startrow=4, title="Date : 27.08.2026")


@pytest.fixture()
def sap_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'sap_qc.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        from app.models.quality_control.qc_sap_monitoring import (
            QCNonSAPSample, QCNonSAPSampleUpdate, QCSAPLabUpdate, QCSAPMonitoringDisposition, QCSAPRecord,
            QCSAPUploadBatch,
        )

        # SQLite only auto-generates primary keys for an INTEGER column; the
        # production schema intentionally retains BIGINT primary keys for MySQL.
        for model in (
            QCSAPUploadBatch, QCSAPRecord, QCSAPLabUpdate, QCSAPMonitoringDisposition,
            QCNonSAPSample, QCNonSAPSampleUpdate,
        ):
            model.__table__.c.id.type = Integer()
        db.create_all()
        yield app
        db.session.remove()


def test_native_sap_exports_are_parsed_and_unmatched_rows_are_retained():
    from app.core.services.sap_quality_control import (
        merge_sap_exports, parse_sap_inspection_workbook, parse_sap_notification_workbook,
    )

    inspections = parse_sap_inspection_workbook(_inspection_export(), "SAP_INSPECTION_20260826.xlsx")
    notifications = parse_sap_notification_workbook(_notification_export(), "SAP_NOTIFICATIONS_20260826.xlsx")
    rows, reconciliation = merge_sap_exports(inspections.rows, notifications.rows)

    assert inspections.as_of_date == date(2026, 8, 26)
    assert notifications.as_of_date == date(2026, 8, 26)
    assert len(rows) == 4
    assert reconciliation == {"unmatched_inspection_count": 1, "unmatched_notification_count": 1}
    by_lot = {row["inspection_lot_number"]: row for row in rows if row["inspection_lot_number"]}
    assert by_lot["5001"]["official_status"] == "open"
    assert by_lot["5002"]["official_status"] == "completed"
    assert by_lot["5003"]["source_completeness"] == "inspection_lot_only"
    assert next(row for row in rows if row["notification_no"] == "7003")["source_completeness"] == "notification_only"


def test_sap_import_remains_authoritative_when_laboratory_logs_completion(sap_app):
    from app.core.services.sap_quality_control import (
        create_sap_lab_update, import_sap_panvel_exports, sap_panvel_dashboard_data,
    )
    from app.models.quality_control.qc_sap_monitoring import QCSAPRecord

    batch = import_sap_panvel_exports(
        _inspection_export(), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(), "SAP_NOTIFICATIONS_20260826.xlsx", None,
    )
    db.session.commit()
    assert batch.record_count == 4
    record = QCSAPRecord.query.filter_by(inspection_lot_number="5001").one()
    assert record.official_status == "open"

    create_sap_lab_update(record.id, {
        "activity_status": "action_completed",
        "sampling_date": "2026-07-30",
        "actual_start_date": "2026-08-02",
        "expected_completion_date": "2026-08-27",
        "action_owner": "MUDLAB",
        "delay_reason": "Report sent for SAP posting",
        "update_note": "Awaiting QM update",
    }, None)
    db.session.commit()
    db.session.refresh(record)

    assert record.official_status == "open"
    data = sap_panvel_dashboard_data()
    assert data["kpis"]["open"] == 3
    assert data["kpis"]["accepted"] == 1
    assert data["kpis"]["rejected"] == 0
    assert data["kpis"]["awaiting_sap_confirmation"] == 1
    entry = next(item for item in data["records"] if item["record"].id == record.id)
    assert entry["reconciliation_key"] == "awaiting_sap_confirmation"
    assert entry["lab_update"].action_owner == "MUDLAB"
    assert entry["lab_update"].sampling_date == date(2026, 7, 30)
    assert entry["lab_update"].actual_start_date == date(2026, 8, 2)
    assert entry["sampling_to_sap_receipt_days"] == 2


def test_panvel_import_rejects_a_non_panvel_plant():
    from app.core.services.sap_quality_control import parse_sap_notification_workbook

    with pytest.raises(ValueError, match="plant 10R2 only"):
        parse_sap_notification_workbook(_notification_export(plant="11A1"), "SAP_NOTIFICATIONS_20260826.xlsx")


def test_central_sap_import_splits_rows_by_the_approved_plant_map(sap_app):
    from app.core.services.sap_quality_control import (
        SAP_PLANT_LAB_CODES,
        import_central_sap_exports,
    )
    from app.models.quality_control.qc_sap_monitoring import QCSAPRecord

    batches = import_central_sap_exports(
        _central_inspection_export(), "SAP_INSP_20260827.xlsx",
        _central_notification_export(), "SAP_ZLABIMS_20260827.xlsx", None,
    )
    db.session.commit()

    assert SAP_PLANT_LAB_CODES == {
        "10R2": "rgl_panvel", "23R2": "rgl_vadodara", "42R2": "rgl_chennai",
        "41B1": "rgl_rajahmundry", "50R2": "rgl_jorhat_sivasagar", "51R2": "rgl_jorhat",
        "70T1": "idwe_dehradun",
    }
    assert {(batch.plant_code, batch.lab_code) for batch in batches} == {
        ("10R2", "rgl_panvel"), ("42R2", "rgl_chennai"), ("51R2", "rgl_jorhat"),
    }
    jorhat_record = QCSAPRecord.query.filter_by(lab_code="rgl_jorhat").one()
    assert jorhat_record.source_completeness == "notification_only"
    assert jorhat_record.plant_code == "51R2"


def test_central_sap_import_rejects_any_unmapped_plant_before_persisting(sap_app):
    from app.core.services.sap_quality_control import import_central_sap_exports

    with pytest.raises(ValueError, match=r"unmapped plant row\(s\): 50R1 \(3\)"):
        import_central_sap_exports(
            _inspection_export(plant="50R1"), "SAP_INSP_20260827.xlsx",
            _notification_export(plant="50R1"), "SAP_ZLABIMS_20260827.xlsx", None,
        )


def test_import_explains_when_a_notification_export_is_selected_as_inspection_lots():
    from app.core.services.sap_quality_control import parse_sap_inspection_workbook

    with pytest.raises(ValueError, match="Notifications / ZLABIMS export"):
        parse_sap_inspection_workbook(_notification_export(plant="42R2"), "RGL_Chennai_Inspection_Lots.xlsx", expected_plant=None)


def test_other_sap_reporting_lab_accepts_its_single_plant_snapshot(sap_app):
    from app.core.services.sap_quality_control import import_sap_lab_exports, sap_lab_dashboard_data

    batch = import_sap_lab_exports(
        "rgl_vadodara", _inspection_export(plant="23R2"), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(plant="23R2"), "SAP_NOTIFICATIONS_20260826.xlsx", None,
    )
    db.session.commit()

    assert batch.lab_code == "rgl_vadodara"
    assert batch.plant_code == "23R2"
    data = sap_lab_dashboard_data("rgl_vadodara")
    assert data["laboratory"]["name"] == "RGL Vadodara"
    assert data["kpis"]["open"] == 3


def test_non_sap_register_is_separate_from_the_sap_position(sap_app):
    from app.core.services.sap_quality_control import (
        create_non_sap_sample, import_sap_panvel_exports, sap_panvel_dashboard_data,
        update_non_sap_sample,
    )

    import_sap_panvel_exports(
        _inspection_export(), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(), "SAP_NOTIFICATIONS_20260826.xlsx", None,
    )
    sample = create_non_sap_sample({
        "lab_code": "rgl_panvel", "sample_reference": "PANVEL-LOCAL-08", "chemical_name": "Field control sample",
        "current_status": "under_testing", "expected_completion_date": "2026-08-28", "action_owner": "QC Panvel",
        "delay_reason": "Awaiting calibration", "update_note": "Declared separately from SAP",
    }, None)
    db.session.commit()
    assert sap_panvel_dashboard_data()["kpis"]["open"] == 3
    assert sap_panvel_dashboard_data()["kpis"]["non_sap_pending"] == 1

    update_non_sap_sample(sample.id, {
        "current_status": "closed_pass", "expected_completion_date": "2026-08-28", "action_owner": "QC Panvel",
        "delay_reason": "", "update_note": "Reported accepted", "reported_outcome": "pass",
    }, None)
    db.session.commit()
    data = sap_panvel_dashboard_data()
    assert data["kpis"]["open"] == 3
    assert data["kpis"]["non_sap_pending"] == 0


def test_qc_admin_exclusion_is_auditable_and_reopens_for_sap_assignment_change(sap_app):
    from app.core.services.sap_quality_control import (
        exclude_sap_record_from_monitoring, import_sap_panvel_exports,
        reinstate_sap_record_for_monitoring, sap_panvel_dashboard_data,
    )
    from app.models.quality_control.qc_sap_monitoring import QCSAPRecord

    import_sap_panvel_exports(
        _inspection_export(), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(), "SAP_NOTIFICATIONS_20260826.xlsx", None,
    )
    db.session.commit()
    record = QCSAPRecord.query.filter_by(notification_no="7001").one()
    exclusion = exclude_sap_record_from_monitoring(record.id, {
        "exclusion_reason": "junk_notification",
        "exclusion_note": "Created in error; no laboratory work was requested.",
    }, None)
    db.session.commit()

    data = sap_panvel_dashboard_data()
    assert exclusion.decision == "exclude_non_actionable"
    assert data["kpis"]["open"] == 2
    assert data["kpis"]["excluded_from_monitoring"] == 1
    assert all(item["record"].id != record.id for item in data["open_records"])
    assert data["excluded_entries"][0]["exclusion_reason_label"] == "Junk / test notification"

    import_sap_panvel_exports(
        _inspection_export(), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(first_work_center="NEWLAB"), "SAP_NOTIFICATIONS_20260827.xlsx", None,
    )
    db.session.commit()
    changed = sap_panvel_dashboard_data()
    assert changed["kpis"]["excluded_from_monitoring"] == 0
    assert changed["kpis"]["exclusion_review"] == 1
    assert changed["exclusion_review_entries"][0]["record"].id == record.id

    reinstate_sap_record_for_monitoring(record.id, None)
    db.session.commit()
    reinstated = sap_panvel_dashboard_data()
    assert reinstated["kpis"]["open"] == 3
    assert reinstated["kpis"]["exclusion_review"] == 0


def test_management_data_uses_latest_sap_records_and_excludes_non_sap_rows(sap_app):
    from app.core.services.sap_quality_control import (
        create_non_sap_sample, exclude_sap_record_from_monitoring,
        import_sap_panvel_exports, sap_management_data,
    )
    from app.models.quality_control.qc_sap_monitoring import QCSAPRecord

    import_sap_panvel_exports(
        _inspection_export(), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(), "SAP_NOTIFICATIONS_20260826.xlsx", None,
    )
    create_non_sap_sample({
        "lab_code": "rgl_panvel", "sample_reference": "LOCAL-01",
        "chemical_name": "Not in SAP", "current_status": "under_testing",
    }, None)
    db.session.commit()

    data = sap_management_data()
    assert data["reporting_labs"] == 1
    assert data["kpis"]["total"] == 4
    assert data["kpis"]["actionable_open"] == 3
    assert data["kpis"]["accepted"] == 1
    assert len(data["action_entries"]) == 3
    assert all(item["record"].material_description != "Not in SAP" for item in data["action_entries"])

    record = QCSAPRecord.query.filter_by(notification_no="7001").one()
    exclude_sap_record_from_monitoring(record.id, {
        "exclusion_reason": "junk_notification", "exclusion_note": "Test entry",
    }, None)
    db.session.commit()
    excluded = sap_management_data()
    assert excluded["kpis"]["total"] == 4
    assert excluded["kpis"]["actionable_open"] == 2
    assert excluded["kpis"]["excluded"] == 1


def test_sap_management_presentation_uses_the_current_snapshot(sap_app):
    from app.core.services.qc_presentation import (
        build_sap_panvel_presentation, build_sap_portfolio_management_presentation,
    )
    from app.core.services.sap_quality_control import import_sap_panvel_exports

    import_sap_panvel_exports(
        _inspection_export(), "SAP_INSPECTION_20260826.xlsx",
        _notification_export(), "SAP_NOTIFICATIONS_20260826.xlsx", None,
    )
    db.session.commit()
    output, filename = build_sap_panvel_presentation(sap_app.static_folder)

    assert filename == "RGL Panvel SAP QC Action Pack 26 Aug 2026.pptx"
    assert output.read(2) == b"PK"

    portfolio_output, portfolio_filename = build_sap_portfolio_management_presentation(sap_app.static_folder)
    assert portfolio_filename == "QC SAP Management Review 26 Aug 2026.pptx"
    assert portfolio_output.read(2) == b"PK"


def test_open_register_paginates_every_item_for_the_management_presentation():
    from app.core.services.qc_presentation import _paginated_rows

    pages = _paginated_rows(list(range(37)), 11)

    assert [len(page) for page in pages] == [11, 11, 11, 4]
    assert [item for page in pages for item in page] == list(range(37))
