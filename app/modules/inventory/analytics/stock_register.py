"""Build the ``stock_register_monthly`` computed table from MC.9 data.

The stock register provides month-by-month opening / receipts / issues /
closing quantities and values for each material-plant combination.
"""

from __future__ import annotations

import logging
from typing import Any

from app.extensions import db
from app.modules.inventory.ingestion.base_loader import (
    fiscal_period_from_period,
    fiscal_year_from_period,
)

logger = logging.getLogger(__name__)


def build_stock_register(
    fiscal_year: int,
    plant: str | None = None,
    recalculate_all: bool = False,
) -> dict[str, Any]:
    """Compute stock_register_monthly rows from mc9_stock_analysis.

    Parameters
    ----------
    fiscal_year : int
        The fiscal year to compute (April-March convention).
    plant : str, optional
        Restrict to a single plant code.
    recalculate_all : bool
        If True, truncate and rebuild for ALL available fiscal years.

    Returns
    -------
    dict with keys: periods_processed, materials_processed, warnings
    """
    warnings: list[str] = []
    connection = db.engine.raw_connection()
    try:
        cursor = connection.cursor()

        if recalculate_all:
            if plant:
                cursor.execute(
                    "DELETE FROM stock_register_monthly WHERE plant = %s", (plant,)
                )
            else:
                cursor.execute("TRUNCATE TABLE stock_register_monthly")
            connection.commit()

            # Get all distinct fiscal years in mc9
            fy_sql = "SELECT DISTINCT fiscal_year FROM mc9_stock_analysis ORDER BY fiscal_year"
            cursor.execute(fy_sql)
            fiscal_years = [row[0] for row in cursor.fetchall()]
        else:
            fiscal_years = [fiscal_year]

        total_periods = 0
        all_materials: set[str] = set()

        for fy in fiscal_years:
            periods, materials, fy_warnings = _build_for_fy(cursor, fy, plant)
            total_periods += periods
            all_materials.update(materials)
            warnings.extend(fy_warnings)

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

    return {
        "periods_processed": total_periods,
        "materials_processed": len(all_materials),
        "warnings": warnings,
    }


def _build_for_fy(
    cursor, fiscal_year: int, plant: str | None
) -> tuple[int, set[str], list[str]]:
    """Process a single fiscal year and return (periods, materials_set, warnings)."""
    warnings: list[str] = []
    materials_seen: set[str] = set()
    periods_processed = 0

    # Fetch MC9 data ordered by material, plant, period
    sql = (
        "SELECT material_no, material_desc, material_group, plant, "
        "       analysis_period, valuated_stock_qty, valuated_stock_value, "
        "       receipts_qty, receipts_value, issues_qty, issues_value, "
        "       moving_avg_price "
        "FROM mc9_stock_analysis "
        "WHERE fiscal_year = %s "
    )
    params: list[Any] = [fiscal_year]
    if plant:
        sql += "AND plant = %s "
        params.append(plant)
    sql += "ORDER BY material_no, plant, analysis_period"

    cursor.execute(sql, params)
    rows = cursor.fetchall()
    columns = [
        "material_no", "material_desc", "material_group", "plant",
        "analysis_period", "valuated_stock_qty", "valuated_stock_value",
        "receipts_qty", "receipts_value", "issues_qty", "issues_value",
        "moving_avg_price",
    ]
    data = [dict(zip(columns, row)) for row in rows]

    if not data:
        warnings.append(f"No MC9 data found for FY{fiscal_year}")
        return 0, set(), warnings

    # Group by (material_no, plant)
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in data:
        key = (row["material_no"], row["plant"])
        grouped.setdefault(key, []).append(row)

    upsert_sql = (
        "INSERT INTO stock_register_monthly "
        "(material_no, material_desc, material_group, plant, period_month, "
        " fiscal_year, fiscal_period, "
        " opening_qty, opening_value, receipts_qty, receipts_value, "
        " issues_qty, issues_value, closing_qty, closing_value, map_at_close) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        " material_desc = VALUES(material_desc), "
        " material_group = VALUES(material_group), "
        " fiscal_year = VALUES(fiscal_year), "
        " fiscal_period = VALUES(fiscal_period), "
        " opening_qty = VALUES(opening_qty), "
        " opening_value = VALUES(opening_value), "
        " receipts_qty = VALUES(receipts_qty), "
        " receipts_value = VALUES(receipts_value), "
        " issues_qty = VALUES(issues_qty), "
        " issues_value = VALUES(issues_value), "
        " closing_qty = VALUES(closing_qty), "
        " closing_value = VALUES(closing_value), "
        " map_at_close = VALUES(map_at_close)"
    )

    for (mat, plt), periods in grouped.items():
        materials_seen.add(mat)
        # Sort chronologically
        periods.sort(key=lambda r: r["analysis_period"])

        prev_closing_qty = None
        prev_map = None

        for p in periods:
            period_month = p["analysis_period"]
            fy = fiscal_year_from_period(period_month)
            fp = fiscal_period_from_period(period_month)

            receipts_qty = _dec(p.get("receipts_qty"), 0)
            receipts_value = _dec(p.get("receipts_value"), 0)
            issues_qty = _dec(p.get("issues_qty"), 0)
            issues_value = _dec(p.get("issues_value"), 0)
            stock_qty = _dec(p.get("valuated_stock_qty"), 0)
            stock_value = _dec(p.get("valuated_stock_value"), 0)
            map_price = p.get("moving_avg_price")

            if prev_closing_qty is not None:
                opening_qty = prev_closing_qty
            else:
                # First period: back-calculate opening
                opening_qty = stock_qty - receipts_qty + issues_qty

            closing_qty = opening_qty + receipts_qty - issues_qty

            # Values
            opening_value = opening_qty * (prev_map or 0) if prev_map else None
            closing_value = closing_qty * map_price if map_price else stock_value

            cursor.execute(upsert_sql, (
                mat, p.get("material_desc"), p.get("material_group"), plt,
                period_month, fy, fp,
                opening_qty, opening_value,
                receipts_qty, receipts_value,
                issues_qty, issues_value,
                closing_qty, closing_value, map_price,
            ))

            prev_closing_qty = closing_qty
            prev_map = map_price
            periods_processed += 1

    return periods_processed, materials_seen, warnings


def _dec(val, default=0):
    """Safely convert to float, returning *default* on None/NaN."""
    if val is None:
        return default
    try:
        f = float(val)
        return f if f == f else default  # NaN check
    except (ValueError, TypeError):
        return default
