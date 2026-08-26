"""Work-centre mapping is derived from the imported inventory, not declared ahead of it."""
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


def test_every_imported_stock_line_maps_its_work_centre(inventory_app):
    from app.core.services.inventory_monitoring import _current_mapping_pairs, import_workbook
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

    centre = InventoryMonitoringWorkCenter.query.one()
    codes = {item.material_code: item.id for item in InventoryMonitoringMaterial.query.all()}
    assert _current_mapping_pairs() == {(centre.id, codes["090001043"]), (centre.id, codes["090002222"])}
    # Nothing is held back for a super-user to clear.
    assert InventoryMonitoringException.query.filter(
        InventoryMonitoringException.exception_type.in_(("held_not_mapped", "mapped_not_held", "unknown_mapping"))
    ).count() == 0

    # A later mapping workbook is the work-centre directory only; it cannot unmap held stock.
    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map_v2.xlsx", "mapping", None, 1)
    db.session.commit()
    assert _current_mapping_pairs() == {(centre.id, codes["090001043"]), (centre.id, codes["090002222"])}


def test_every_positive_quantity_or_value_holding_is_monitored(inventory_app):
    from app.core.services.inventory_monitoring import _current_mapping_pairs, import_workbook
    from app.models.inventory.monitoring import InventoryMonitoringRecord

    batch = import_workbook(
        _inventory_workbook([
            ["090001043", "OIL WELL CEMENT CLASS G", "Ankleshwar DFS", 120, "MT", 30000000, 4],
            ["090002222", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 0, "MT", 0, 9],
            ["090003333", "CEMENT RETARDER", "Ankleshwar DFS", 40, "MT", None, 2],
            ["090004444", "VALUE-ONLY HOLDING", "Ankleshwar DFS", None, "MT", 500000, 2],
        ]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    db.session.commit()

    assert batch.accepted_count == 3 and batch.rejected_count == 1
    assert {item.material_code for item in InventoryMonitoringRecord.query.all()} == {
        "090001043", "090003333", "090004444",
    }
    assert len(_current_mapping_pairs()) == 3


def test_material_register_maps_from_the_uploaded_inventory(inventory_app):
    from app.core.services.inventory_monitoring import import_workbook, material_mapping_register_data

    import_workbook(_mapping_workbook("Ankleshwar DFS", ["090001043"]), "map.xlsx", "mapping", None, 1)
    import_workbook(
        _inventory_workbook([["090002222", "MUD CHEMICAL BARYTES", "Mehsana ST", 400, "MT", 18000000, 9]]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    import_workbook(
        _inventory_workbook([["100203400", "CHROMIUM ACETATE", "Mehsana ST", 20, "MT", 8000000, 5]],
                           sheet="10 Chemi incl mud chemi - Inven"),
        "10_All_Tables_20260731_110000.xlsx", "10", date(2026, 7, 31), 1,
    )
    db.session.commit()

    data = material_mapping_register_data()
    centres = {
        item.material_code: [centre.name for centre in data["centres_by_material"].get(item.id, [])]
        for item in data["materials"]
    }
    assert centres["090002222"] == ["Mehsana ST"]
    assert centres["100203400"] == ["Mehsana ST"]


def test_material_register_does_not_hide_rows_after_the_first_five_hundred(inventory_app):
    from app.core.services.inventory_monitoring import material_mapping_register_data
    from app.models.inventory.monitoring import InventoryMonitoringMaterial

    db.session.add_all([
        InventoryMonitoringMaterial(material_code=f"1{index:08d}", description=f"Material {index}")
        for index in range(501)
    ])
    db.session.commit()

    data = material_mapping_register_data()
    assert data["total_count"] == 501
    assert len(data["materials"]) == 501
    assert data["materials"][-1].material_code == "100000500"


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
    assert [tile["key"] for tile in monitored_material_categories()["tiles"]][:10] == [
        "DFC", "CCA", "WCF", "WS", "PC", "WIC", "WM", "UTL", "LPG", "API",
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
