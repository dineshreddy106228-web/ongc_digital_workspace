"""An accepted mapping decision must outlive later mapping-workbook imports."""
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


def _mapping_workbook(work_centre: str, codes: list[str]) -> bytes:
    frame = pd.DataFrame(
        [["", "Western Onshore", work_centre, "DFS", *codes]],
        columns=["Sl", "Zone", "Work centre", "Type", *[f"Material {index + 1}" for index in range(len(codes))]],
    )
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False)
    return buffer.getvalue()


def _inventory_workbook(rows: list[list], sheet: str = "09 Oil well cement - Inventory") -> bytes:
    frame = pd.DataFrame(rows, columns=["Material Code", "Material Description", "Work Centre", "Stock Qty", "UOM", "Inventory Value INR", "Stock Months"])
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([[""]]).to_excel(writer, sheet_name=sheet, index=False, header=False)
        frame.to_excel(writer, sheet_name=sheet, index=False, startrow=1)
    return buffer.getvalue()


@pytest.fixture()
def inventory_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'monitoring.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        from app.models.inventory import monitoring
        from app.models.quality_control.qc_testing_standard import QCTestingStandard

        # SQLite only autoincrements INTEGER primary keys; MySQL keeps BIGINT.
        models = [getattr(monitoring, name) for name in dir(monitoring)] + [QCTestingStandard]
        for model in models:
            if hasattr(model, "__table__") and "id" in getattr(model, "__table__").c:
                model.__table__.c.id.type = Integer()
        db.create_all()
        yield app
        db.session.remove()


def test_accepted_mapping_survives_later_workbooks(inventory_app):
    from app.core.services.inventory_monitoring import (
        _current_mapping_pairs, import_workbook, review_mapping_exception,
    )
    from app.models.inventory.monitoring import (
        InventoryMonitoringException, InventoryMonitoringMaterial, InventoryMonitoringWorkCenter,
    )

    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map_v1.xlsx", "mapping", None, 1)
    import_workbook(
        _inventory_workbook([
            ["090001043", "OIL WELL CEMENT CLASS G", "Ankleshwar DFS", 120, "MT", 30000000, 4],
            ["090002222", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 400, "MT", 18000000, 9],
        ]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    db.session.commit()

    queued = InventoryMonitoringException.query.filter_by(exception_type="held_not_mapped").all()
    assert [item.material.material_code for item in queued] == ["090002222"]

    review_mapping_exception(queued[0].id, "add_mapping", 1)
    db.session.commit()

    centre = InventoryMonitoringWorkCenter.query.one()
    accepted = InventoryMonitoringMaterial.query.filter_by(material_code="090002222").one()
    assert (centre.id, accepted.id) in _current_mapping_pairs()

    # A later mapping workbook that still omits the material must not undo the decision.
    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map_v2.xlsx", "mapping", None, 1)
    db.session.commit()
    assert (centre.id, accepted.id) in _current_mapping_pairs()

    # Nor may a later inventory import queue it again.
    import_workbook(
        _inventory_workbook([
            ["090001043", "OIL WELL CEMENT CLASS G", "Ankleshwar DFS", 100, "MT", 26000000, 3],
            ["090002222", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 380, "MT", 17000000, 8],
        ]),
        "09_All_Tables_20260831_110000.xlsx", "09", date(2026, 8, 31), 1,
    )
    db.session.commit()
    assert InventoryMonitoringException.query.filter_by(exception_type="held_not_mapped").count() == 0


def test_mapping_review_filters_narrow_the_queue(inventory_app):
    from app.core.services.inventory_monitoring import import_workbook, mapping_review_query, mapping_review_work_centres
    from decimal import Decimal

    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map_v1.xlsx", "mapping", None, 1)
    import_workbook(
        _inventory_workbook([
            ["090001043", "OIL WELL CEMENT CLASS G", "Ankleshwar DFS", 120, "MT", 30000000, 4],
            ["090002222", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 400, "MT", 18000000, 9],
            ["090003333", "CEMENT RETARDER", "Ankleshwar DFS", 40, "MT", 900000, 2],
        ]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    db.session.commit()

    assert mapping_review_query().count() == 2
    assert mapping_review_query(term="barytes").count() == 1
    assert mapping_review_query(term="090003333").count() == 1
    assert mapping_review_query(min_value=Decimal("10000000")).count() == 1
    assert mapping_review_query(work_center_id=mapping_review_work_centres()[0].id).count() == 2
    assert mapping_review_query(status="dismissed").count() == 0


def test_specification_category_reads_the_specification_number():
    from app.core.services.inventory_monitoring import _specification_category

    assert _specification_category("ONGC / DFC / 01 / 2026") == "DFC"
    assert _specification_category("ONGC / WIC / 12 / 2026") == "WIC"
    assert _specification_category("-") is None
    assert _specification_category(None) is None


def _standard(index, name, specification_no, code, days=2):
    from app.models.quality_control.qc_testing_standard import QCTestingStandard

    return QCTestingStandard(
        id=index, chemical_name=name, normalized_name=name.casefold(),
        specification_no=specification_no, material_code=code, standard_days=days,
    )


def test_specification_index_matches_inventory_material_codes(inventory_app):
    from app.core.services.inventory_monitoring import specification_index

    db.session.add_all([
        _standard(1, "Barytes", "ONGC / DFC / 03 / 2026", "90001043"),          # eight digits in the master
        _standard(2, "Chromium Acetate", "ONGC / PC / 01 / 2026", "100203400"),
        _standard(3, "Calcium Bromide", "ONGC / WCF / 01 / 2026", "Code not mapped*"),
    ])
    db.session.commit()

    index = specification_index()
    assert set(index) == {"090001043", "100203400"}          # zero-padded, unmatchable row skipped
    assert index["090001043"]["category"] == "DFC"
    assert index["100203400"]["category_label"] == "Production Chemicals"


def test_monitored_materials_group_by_specification_category(inventory_app):
    from app.core.services.inventory_monitoring import import_workbook, monitored_material_categories

    db.session.add_all([
        _standard(1, "Barytes", "ONGC / DFC / 03 / 2026", "90001043"),
        _standard(2, "Chromium Acetate", "ONGC / PC / 01 / 2026", "100203400"),
    ])
    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map.xlsx", "mapping", None, 1)
    import_workbook(
        _inventory_workbook([["090001043", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 400, "MT", 30000000, 4]]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    import_workbook(
        _inventory_workbook([
            ["100203400", "CHROMIUM ACETATE", "Ankleshwar DFS", 20, "MT", 8000000, 5],
            ["100999999", "UNLISTED CHEMICAL", "Ankleshwar DFS", 10, "MT", 2000000, 7],
        ], sheet="10 Chemi incl mud chemi - Inven"),
        "10_All_Tables_20260731_110000.xlsx", "10", date(2026, 7, 31), 1,
    )
    db.session.commit()

    tiles = {tile["key"]: tile for tile in monitored_material_categories()["tiles"]}
    assert [tile["key"] for tile in monitored_material_categories()["tiles"]][:7] == [
        "DFC", "PC", "WS", "WIC", "WM", "UTL", "LPG",
    ]
    assert tiles["DFC"]["materials"] == 1 and tiles["DFC"]["value"] == 30000000
    assert tiles["PC"]["materials"] == 1
    assert tiles["WS"]["materials"] == 0
    assert tiles["unspecified"]["materials"] == 1
    assert tiles["unspecified"]["value"] == 2000000


def test_unspecified_material_register_orders_by_latest_inventory_value(inventory_app):
    from app.core.services.inventory_monitoring import import_workbook, material_mapping_register_data

    import_workbook(
        _mapping_workbook("Ankleshwar DFS", ["090001043", "100999999", "100888888"]),
        "map.xlsx", "mapping", None, 1,
    )
    import_workbook(
        _inventory_workbook([[
            "090001043", "UNLISTED CEMENT", "Ankleshwar DFS", 100, "MT", 1000000, 4,
        ]]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    import_workbook(
        _inventory_workbook([
            ["100999999", "UNLISTED HIGH VALUE", "Ankleshwar DFS", 10, "MT", 5000000, 7],
            ["100888888", "UNLISTED MID VALUE", "Ankleshwar DFS", 20, "MT", 2000000, 3],
        ], sheet="10 Chemi incl mud chemi - Inven"),
        "10_All_Tables_20260731_110000.xlsx", "10", date(2026, 7, 31), 1,
    )
    db.session.commit()

    data = material_mapping_register_data(category="unspecified")
    rows = data["material_groups"][0]["rows"]

    assert [item.material_code for item in rows] == ["100999999", "100888888", "090001043"]
    assert data["inventory_values_by_material"][rows[0].id] == 5000000
    assert data["inventory_values_by_material"][rows[1].id] == 2000000


def test_selected_candidates_are_mapped_together(inventory_app):
    from app.core.services.inventory_monitoring import (
        _current_mapping_pairs, import_workbook, review_selected_mapping_exceptions,
    )
    from app.models.inventory.monitoring import InventoryMonitoringException

    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map.xlsx", "mapping", None, 1)
    import_workbook(
        _inventory_workbook([
            ["090001043", "OIL WELL CEMENT CLASS G", "Ankleshwar DFS", 120, "MT", 30000000, 4],
            ["090002222", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 400, "MT", 18000000, 9],
            ["090003333", "CEMENT RETARDER", "Ankleshwar DFS", 40, "MT", 900000, 2],
            ["090004444", "DEFOAMER", "Ankleshwar DFS", 15, "MT", 400000, 5],
        ]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    db.session.commit()

    queued = InventoryMonitoringException.query.filter_by(exception_type="held_not_mapped", review_status="pending").all()
    assert len(queued) == 3
    chosen = [item.id for item in queued[:2]]

    assert review_selected_mapping_exceptions(chosen, "add_mapping", 1) == 2
    db.session.commit()

    pairs = _current_mapping_pairs()
    assert len(pairs) == 3  # the workbook pair plus the two accepted together
    statuses = {item.id: item.review_status for item in InventoryMonitoringException.query.filter_by(exception_type="held_not_mapped").all()}
    assert all(statuses[item_id] == "added_to_mapping" for item_id in chosen)
    assert statuses[queued[2].id] == "pending"

    # Every accepted pair rides on one auditable manual batch.
    from app.models.inventory.monitoring import InventoryMonitoringUploadBatch
    manual = InventoryMonitoringUploadBatch.query.filter_by(source_group="mapping_manual").all()
    assert len(manual) == 1 and manual[0].accepted_count == 2


def test_selected_review_rejects_an_empty_or_settled_selection(inventory_app):
    from app.core.services.inventory_monitoring import review_selected_mapping_exceptions

    with pytest.raises(ValueError):
        review_selected_mapping_exceptions([], "add_mapping", 1)
    with pytest.raises(ValueError):
        review_selected_mapping_exceptions([9999], "add_mapping", 1)
