"""The merged inventory management review: one page, three denominators.

Management Review and Inventory Health used to be two screens reading the same
period out of the same snapshot.  These tests hold them together: one route
serves both, the register answers for the period the reader selected, and no
chart series adds up figures that were never a total.
"""
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
CRORE = 10000000


def _workbook(inventory_rows: list[list], group: str = "09") -> bytes:
    inventory_sheet, quantity_sheet, value_sheet = SHEETS[group]
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet, columns, rows in (
            (inventory_sheet, ["Material Code", "Material Description", "Work Centre", "Stock Qty", "UOM", "Inventory Value INR", "Stock Months"], inventory_rows),
            (quantity_sheet, ["Material Code", "Material Description", "Stock Qty", "UOM", "Consumption Qty (12M)", "Stock Months"], []),
            (value_sheet, ["Material Code", "Material Description", "Inventory Value (Cr)", "Consumption Value (Cr)", "Stock Months"], []),
        ):
            # The exports carry a title row above the header, which the readers skip.
            pd.DataFrame([[""]]).to_excel(writer, sheet_name=sheet, index=False, header=False)
            pd.DataFrame(rows, columns=columns).to_excel(writer, sheet_name=sheet, index=False, startrow=1)
    return buffer.getvalue()


def _publish(inventory_rows: list[list], as_on: date, tail: str) -> None:
    """Import both material groups for one date, which is what publishes it."""
    from app.core.services.inventory_monitoring import import_workbook

    import_workbook(_workbook(inventory_rows, "09"), f"09_All_Tables_{tail}_110000.xlsx", "09", as_on, 1)
    import_workbook(_workbook([], "10"), f"10_All_Tables_{tail}_110000.xlsx", "10", as_on, 1)
    db.session.commit()


@pytest.fixture()
def inventory_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'review.db'}"
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


def _two_periods() -> None:
    """July and August, with a line in each coverage band and one asset moving."""
    july = [
        ["090001043", "OIL WELL CEMENT CLASS G", "Ankleshwar DFS", 400, "MT", 18 * CRORE, 0.4],
        ["090002222", "CEMENT RETARDER", "Ankleshwar DFS", 250, "MT", 9 * CRORE, 2.5],
        ["090003333", "MUD CHEMICAL BARYTES", "Mehsana DFS", 90, "MT", 4 * CRORE, 4.5],
        ["090004444", "SCALE INHIBITOR", "Mehsana DFS", 60, "MT", 6 * CRORE, 8],
        ["090005555", "DEFOAMER", "Ankleshwar DFS", 20, "MT", 12 * CRORE, 30],
    ]
    august = [[*row] for row in july]
    august[0][5] = 30 * CRORE  # Ankleshwar gains against July
    _publish(july, date(2026, 7, 31), "20260731")
    _publish(august, date(2026, 8, 31), "20260831")


def test_one_route_serves_the_position_the_movement_and_the_register(inventory_app):
    """The review and the health register are one page for one period."""
    from flask import render_template

    from app.core.services.inventory_management_charts import management_chart_series
    from app.core.services.inventory_monitoring import inventory_health_data, portfolio_data, scope_directory

    _two_periods()
    data = portfolio_data()
    health = inventory_health_data(data["reporting_date"])
    with inventory_app.test_request_context("/inventory/portfolio"):
        page = render_template(
            "inventory/portfolio.html",
            scope_directory=scope_directory(data["reporting_date"]),
            health=health, charts=management_chart_series(data, health), **data,
        )

    assert "Total monitored inventory" in page          # position
    assert "Largest movers" in page                     # movement
    assert "Critical low stock" in page                 # register
    assert "Non-moving, aged, surplus and transit exposure" in page
    # The line items behind the position are on the same page, not another one.
    assert "OIL WELL CEMENT CLASS G" in page
    assert 'data-mr-panel="position"' in page and 'data-mr-panel="movement"' in page
    assert 'data-mr-panel="register"' in page


def test_the_register_answers_for_the_period_the_reader_selected(inventory_app):
    """Health used to read the latest import whatever period was on screen."""
    from app.core.services.inventory_monitoring import inventory_health_data, portfolio_data

    _two_periods()
    july = portfolio_data(date(2026, 7, 31))
    assert july["reporting_date"] == date(2026, 7, 31)
    register = inventory_health_data(july["reporting_date"])

    assert register["reporting_date"] == date(2026, 7, 31)
    critical = register["groups"]["critical_low_stock"]
    assert [row.inventory_value_inr for row in critical] == [Decimal("180000000.00")]
    # Left to itself it still reads the latest live snapshot.
    assert inventory_health_data()["reporting_date"] == date(2026, 8, 31)


def test_inventory_health_is_no_longer_a_second_page(inventory_app):
    """One management screen, and nothing still routing to the old one."""
    endpoints = {rule.endpoint for rule in inventory_app.url_map.iter_rules()}

    assert "inventory.portfolio" in endpoints
    assert "inventory.health" not in endpoints
    nav = (Path(__file__).resolve().parents[1] / "app/templates/inventory/_monitoring_nav.html").read_text()
    assert nav.count("Management Review") == 1
    assert "Inventory Health" not in nav


def test_coverage_bands_are_offered_by_value_or_by_count_but_never_summed(inventory_app):
    """Value and line count are two readings of the same stock, not two halves."""
    from app.core.services.inventory_management_charts import management_chart_series
    from app.core.services.inventory_monitoring import inventory_health_data, portfolio_data

    _two_periods()
    data = portfolio_data()
    charts = management_chart_series(data, inventory_health_data(data["reporting_date"]))
    mix = charts["position"]["mix"]

    assert len(mix["labels"]) == len(mix["value"]) == len(mix["count"])
    # Each reading partitions the whole exactly, which is why they are a toggle.
    assert round(sum(mix["value"]), 2) == round(float(data["kpis"]["total_value"]) / CRORE, 2)
    assert sum(mix["count"]) == data["kpis"]["record_count"]
    # A band holding nothing is left out rather than drawn at zero.
    assert all(count > 0 for count in mix["count"])


def test_a_stacked_bar_is_only_ever_a_true_partition(inventory_app):
    """The exposure stack is exact; the workbook registers are never a total."""
    from app.core.services.inventory_management_charts import management_chart_series
    from app.core.services.inventory_monitoring import inventory_health_data, portfolio_data

    _two_periods()
    data = portfolio_data()
    health = inventory_health_data(data["reporting_date"])
    charts = management_chart_series(data, health)
    exposure = charts["register"]["exposure"]

    # The four bands do not overlap, so the stack totals the exposed line count.
    stacked = sum(sum(series["data"]) for series in exposure["series"])
    assert stacked == sum(
        len(health["groups"][key])
        for key in ("critical_low_stock", "low_stock", "slow_moving_stock", "excess_stock")
    )
    # One bar is one asset, and every asset holding an exposed line is on it.
    assert set(exposure["labels"]) == {"Ankleshwar DFS", "Mehsana DFS"}
    assert len(exposure["series"]) == 4


def test_movement_charts_only_the_assets_reporting_in_both_periods(inventory_app):
    """An asset that appears or disappears has changed the scope, not moved."""
    from app.core.services.inventory_management_charts import management_chart_series
    from app.core.services.inventory_monitoring import inventory_health_data, portfolio_data

    _two_periods()
    data = portfolio_data()
    charts = management_chart_series(data, inventory_health_data(data["reporting_date"]))
    movers = charts["movement"]["movers"]

    assert charts["movement"]["has_comparison"] is True
    assert movers["labels"] == ["Ankleshwar DFS"]
    assert movers["delta"] == [12.0]
    assert movers["previous"] == [39.0] and movers["value"] == [51.0]


def test_no_published_period_offers_no_charts_at_all(inventory_app):
    """An empty portfolio has nothing to plot, and says so instead of drawing zeroes."""
    from app.core.services.inventory_management_charts import management_chart_series
    from app.core.services.inventory_monitoring import inventory_health_data, portfolio_data

    data = portfolio_data()
    assert data["kpis"] is None
    assert management_chart_series(data, inventory_health_data(data["reporting_date"])) == {}


def test_workbook_registers_are_stacked_by_band_but_never_added_together():
    """One drum can be named in three of the workbook's own registers at once.

    Each bar is stacked by our coverage band, which is exact within a register.
    The bars themselves are not a total, so the reshape never produces one.
    """
    from app.core.services.inventory_management_charts import _source_registers

    charts = _source_registers({
        "source_bands": {
            "non_moving": [
                {"key": "excess_stock", "label": "Excess", "count": 30, "anchor": "excess"},
                {"key": "slow_moving_stock", "label": "Slow-moving", "count": 4, "anchor": "slow"},
            ],
            "aged_stock_over_one_year": [
                {"key": "excess_stock", "label": "Excess", "count": 30, "anchor": "excess"},
            ],
            "surplus": [
                {"key": "unmatched", "label": "Not held at this snapshot", "count": 5, "anchor": None},
            ],
        },
    })

    # One bar per register the workbook actually carried, in the register order.
    assert charts["labels"] == [
        "Non-moving materials", "Stock lying over one year", "Items lying in surplus",
    ]
    # Bands are the stack, in coverage order, and a register missing a band is a
    # zero in that band rather than a shorter bar.
    assert [series["label"] for series in charts["series"]] == [
        "Slow-moving", "Excess", "Not held at this snapshot",
    ]
    assert [series["data"] for series in charts["series"]] == [[4, 0, 0], [30, 30, 0], [0, 0, 5]]
    # The same thirty lines are in two bars; nothing here sums them to sixty.
    assert "total" not in charts
