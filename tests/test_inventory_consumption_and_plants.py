"""Consumption by phase, SAP plant codes on a merged asset, and scoped decks."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
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

SHEETS = {
    "09": ("09 Oil well cement - Inventory", "09 Oil well cement - Chemical S", "09 Oil well cement - Chemical P"),
    "10": ("10 Chemi incl mud chemi - Inven", "10 Chemi incl mud chemi - Chemi", "10 Chemi incl mud chemi - Che 1"),
}


def _workbook(inventory_rows: list[list], quantity_rows: list[list] | None = None, value_rows: list[list] | None = None, group: str = "09") -> bytes:
    """A workbook shaped like the real export: stock per work centre, consumption per material."""
    inventory_sheet, quantity_sheet, value_sheet = SHEETS[group]
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet, columns, rows in (
            (inventory_sheet, ["Material Code", "Material Description", "Work Centre", "Stock Qty", "UOM", "Inventory Value INR", "Stock Months"], inventory_rows),
            (quantity_sheet, ["Material Code", "Material Description", "Stock Qty", "UOM", "Consumption Qty (12M)", "Stock Months"], quantity_rows or []),
            (value_sheet, ["Material Code", "Material Description", "Inventory Value (Cr)", "Consumption Value (Cr)", "Stock Months"], value_rows or []),
        ):
            # The exports carry a title row above the header, which the readers skip.
            pd.DataFrame([[""]]).to_excel(writer, sheet_name=sheet, index=False, header=False)
            pd.DataFrame(rows, columns=columns).to_excel(writer, sheet_name=sheet, index=False, startrow=1)
    return buffer.getvalue()


def _publish(inventory_rows: list[list], quantity_rows=None, value_rows=None, as_on: date = date(2026, 7, 31), tail: str = "20260731") -> None:
    """Import both material groups for one date, which is what publishes it."""
    from app.core.services.inventory_monitoring import import_workbook

    import_workbook(_workbook(inventory_rows, quantity_rows, value_rows, "09"), f"09_All_Tables_{tail}_110000.xlsx", "09", as_on, 1)
    import_workbook(_workbook([], group="10"), f"10_All_Tables_{tail}_110000.xlsx", "10", as_on, 1)
    db.session.commit()


@pytest.fixture()
def inventory_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'consumption.db'}"
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


def test_units_are_read_as_volume_or_weight_and_never_averaged_together():
    from app.core.services.inventory_monitoring import normalized_quantity, phase_of

    assert phase_of("KL") == "liquid" and phase_of("M3") == "liquid" and phase_of("l") == "liquid"
    assert phase_of("MT") == "solid" and phase_of("KG") == "solid"
    # A piece count is neither, so it is never ranked against either.
    assert phase_of("NO") == "other" and phase_of(None) == "other"
    assert normalized_quantity(2000, "L") == Decimal("2.000")
    assert normalized_quantity(500, "KG") == Decimal("0.500")
    assert normalized_quantity(12, "NO") is None


def test_consumption_value_is_read_from_the_workbook(inventory_app):
    from app.core.services.inventory_monitoring import consumption_leaders

    crore = 10000000
    _publish(
        [["090001043", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 400, "MT", 18 * crore, 9]],
        quantity_rows=[
            ["090001043", "MUD CHEMICAL BARYTES", 400, "MT", 9000, 9],
            ["090002222", "BRINE", 250, "KL", 4000, 3],
            ["090003333", "DRUMMED ADDITIVE", 40, "NO", 90, 5],
            ["090004444", "MINOR SOLID", 5, "KG", 12, 4],
        ],
        value_rows=[
            ["090001043", "MUD CHEMICAL BARYTES", 18, 89, 9],
            ["090002222", "BRINE", 6, 24, 3],
            ["090003333", "DRUMMED ADDITIVE", 1, 11, 5],
            ["090004444", "MINOR SOLID", 0.2, 0.4, 4],
        ],
    )

    leaders = consumption_leaders(date(2026, 7, 31))
    assert leaders["materials"] == 4
    # Value has one scale, so the table is everything above the ₹ 10 Cr floor.
    assert [item["code"] for item in leaders["by_value"]] == ["090001043", "090002222", "090003333"]
    assert leaders["by_value"][0]["consumption_value"] == Decimal("890000000.00")
    # The phase still travels with each line, for the movers that rank by it.
    assert {item["code"]: item["phase"] for item in leaders["by_value"]} == {
        "090001043": "solid", "090002222": "liquid", "090003333": "other",
    }
    assert leaders["phase_counts"]["other"] == 1


def test_a_material_takes_the_unit_the_workbook_states_against_its_code(inventory_app):
    """The summary sheet is the only sheet carrying a unit, so it is the source."""
    from app.core.services.inventory_monitoring import material_uom_map
    from app.models.inventory.monitoring import InventoryMonitoringRecord

    _publish(
        [["090002222", "BRINE", "Ankleshwar DFS", 250, None, 60000000, 3]],
        quantity_rows=[["090002222", "BRINE", 250, "KL", 4000, 3]],
    )

    assert material_uom_map()["090002222"] == "KL"
    # The stock line carries a copy, so a quantity can be read where it is shown.
    assert InventoryMonitoringRecord.query.filter_by(material_code="090002222").one().uom == "KL"


def test_movement_is_ranked_within_liquids_and_within_solids():
    from app.core.services.inventory_monitoring import _phase_movers

    current = {"A": Decimal("120"), "B": Decimal("40"), "C": Decimal("10")}
    previous = {"A": Decimal("100"), "B": Decimal("90"), "C": Decimal("10")}
    movers = _phase_movers(current, previous, {"A": ("BRINE", "10"), "B": ("CEMENT", "09"), "C": ("DRUMS", "10")}, {"A": "KL", "B": "MT", "C": "NO"})

    assert [item["code"] for item in movers["liquid"]["up"]] == ["A"]
    assert [item["code"] for item in movers["solid"]["down"]] == ["B"]
    # A material that did not move is in neither list, whatever its phase.
    assert movers["other"]["up"] == [] and movers["other"]["down"] == []
    # Each phase is also handed over as one table, biggest movement first.
    assert [item["code"] for item in movers["solid"]["ranked"]] == ["B"]
    assert movers["liquid"]["ranked"][0]["delta"] == Decimal("20")


def test_an_unrecognised_plant_is_raised_for_the_module_admin(inventory_app):
    from app.core.services.inventory_monitoring import import_workbook
    from app.models.inventory.monitoring import InventoryMonitoringPlantAlert, InventoryMonitoringWorkCenter

    import_workbook(
        _workbook([["090001043", "MUD CHEMICAL BARYTES", "Karaikal ST", 400, "MT", 180000000, 9]]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    db.session.commit()

    alert = InventoryMonitoringPlantAlert.query.one()
    assert alert.work_center_name == "Karaikal ST" and alert.status == "open" and alert.line_count == 1
    # The stock is still imported: an unclaimed plant is a decision, not a rejection.
    assert InventoryMonitoringWorkCenter.query.filter_by(name="Karaikal ST").count() == 1

    # An asset the directory declares — it carries a zone — raises nothing.
    centre = InventoryMonitoringWorkCenter.query.filter_by(name="Karaikal ST").one()
    centre.zone = "Southern"
    db.session.commit()
    import_workbook(
        _workbook([["090001043", "MUD CHEMICAL BARYTES", "Karaikal ST", 500, "MT", 190000000, 9]]),
        "09_All_Tables_20260831_110000.xlsx", "09", date(2026, 8, 31), 1,
    )
    db.session.commit()
    assert InventoryMonitoringPlantAlert.query.count() == 1


def test_a_merged_asset_carries_both_plant_codes_and_counts_once(inventory_app):
    """N&H and B&S merged into NH-BS while SAP kept 12A1 and 13A1 apart."""
    from app.core.services.inventory_monitoring import import_workbook, plant_code_index, resolve_plant_alert
    from app.models.inventory.monitoring import (
        InventoryMonitoringPlantAlert, InventoryMonitoringRecord, InventoryMonitoringWorkCenter,
    )

    import_workbook(
        _workbook([
            ["090001043", "MUD CHEMICAL BARYTES", "NH-BS Asset", 400, "MT", 180000000, 9],
            ["090002222", "CEMENT RETARDER", "B&S Asset, Mumbai", 100, "MT", 40000000, 5],
        ]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    db.session.commit()

    surviving = InventoryMonitoringWorkCenter.query.filter_by(name="NH-BS Asset").one()
    surviving.sap_plant_codes = "12A1"
    db.session.commit()

    alert = InventoryMonitoringPlantAlert.query.filter_by(work_center_name="B&S Asset, Mumbai").one()
    alert.plant_code = "13A1"
    resolve_plant_alert(alert.id, "attach", {"work_center_id": str(surviving.id)}, 1)
    db.session.commit()

    assert surviving.plant_codes == ["12A1", "13A1"]
    assert plant_code_index()["13A1"].id == surviving.id
    absorbed = InventoryMonitoringWorkCenter.query.filter_by(name="B&S Asset, Mumbai").one()
    assert absorbed.merged_into_id == surviving.id

    # The next workbook still reports the old name, and it lands on the one asset.
    import_workbook(
        _workbook([["090002222", "CEMENT RETARDER", "B&S Asset, Mumbai", 120, "MT", 44000000, 5]]),
        "09_All_Tables_20260831_110000.xlsx", "09", date(2026, 8, 31), 1,
    )
    db.session.commit()
    later = InventoryMonitoringRecord.query.filter(InventoryMonitoringRecord.material_code == "090002222").order_by(
        InventoryMonitoringRecord.id.desc()
    ).first()
    assert later.work_center_id == surviving.id
    # The line still says which name reported it.
    assert later.work_center_name == "B&S Asset, Mumbai"


def test_a_review_can_be_narrowed_to_chosen_assets(inventory_app):
    from app.core.services.inventory_monitoring import import_workbook, management_review_data
    from app.models.inventory.monitoring import InventoryMonitoringWorkCenter

    import_workbook(
        _workbook([
            ["090001043", "MUD CHEMICAL BARYTES", "Ankleshwar DFS", 400, "MT", 180000000, 9],
            ["090002222", "CEMENT RETARDER", "Cauvery Asset", 100, "MT", 40000000, 5],
        ]),
        "09_All_Tables_20260731_110000.xlsx", "09", date(2026, 7, 31), 1,
    )
    import_workbook(
        _workbook([["100001111", "XC POLYMER", "Ankleshwar DFS", 20, "MT", 10000000, 4]], group="10"),
        "10_All_Tables_20260731_110000.xlsx", "10", date(2026, 7, 31), 1,
    )
    db.session.commit()

    everything = management_review_data(date(2026, 7, 31))
    assert everything["kpis"]["total_value"] == Decimal("230000000.00")

    ankleshwar = InventoryMonitoringWorkCenter.query.filter_by(name="Ankleshwar DFS").one()
    scoped = management_review_data(date(2026, 7, 31), None, {ankleshwar.id})
    assert scoped["kpis"]["total_value"] == Decimal("190000000.00")
    assert [item["name"] for item in scoped["scope_centres"]] == ["Ankleshwar DFS"]
    assert all(row["centre"] == "Ankleshwar DFS" for register in scoped["coverage_registers"] for row in register["rows"])
