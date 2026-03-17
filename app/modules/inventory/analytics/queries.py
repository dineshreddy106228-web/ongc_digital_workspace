"""Phase-1 analytics queries for the Inventory Intelligence module.

All functions use parameterised queries and return lists of dicts
(JSON-serialisable).  Optional ``plant`` filter is supported on every query.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from app.extensions import db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_all(sql: str, params: tuple | list) -> list[dict]:
    """Execute a parameterised SELECT and return rows as list of dicts."""
    connection = db.engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    return [_serialise_row(dict(zip(columns, row))) for row in rows]


def _serialise_row(row: dict) -> dict:
    """Ensure all values are JSON-safe (dates → strings, Decimals → floats)."""
    from decimal import Decimal

    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, date):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _plant_clause(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f" AND {prefix}plant = %s "


# ---------------------------------------------------------------------------
# 1. Stock Register
# ---------------------------------------------------------------------------

def get_stock_register(
    fiscal_year: int,
    plant: str | None = None,
    material_no: str | None = None,
) -> list[dict]:
    """Return stock_register_monthly rows for *fiscal_year*."""
    sql = "SELECT * FROM stock_register_monthly WHERE fiscal_year = %s"
    params: list[Any] = [fiscal_year]
    if plant:
        sql += " AND plant = %s"
        params.append(plant)
    if material_no:
        sql += " AND material_no = %s"
        params.append(material_no)
    sql += " ORDER BY material_no, period_month"
    return _fetch_all(sql, params)


# ---------------------------------------------------------------------------
# 2. Year-over-Year Spend
# ---------------------------------------------------------------------------

def get_yoy_spend(
    fiscal_years: list[int],
    plant: str | None = None,
    material_group: str | None = None,
) -> list[dict]:
    """Return total spend per fiscal_year per material_group with YoY variance."""
    if not fiscal_years:
        return []
    placeholders = ", ".join(["%s"] * len(fiscal_years))
    sql = (
        "SELECT fiscal_year, material_group, SUM(net_order_value) AS total_spend "
        f"FROM me2m_purchase_orders WHERE fiscal_year IN ({placeholders}) "
    )
    params: list[Any] = list(fiscal_years)
    if plant:
        sql += " AND plant = %s"
        params.append(plant)
    if material_group:
        sql += " AND material_group = %s"
        params.append(material_group)
    sql += " GROUP BY fiscal_year, material_group ORDER BY material_group, fiscal_year"

    rows = _fetch_all(sql, params)

    # Compute YoY variance %
    prev_map: dict[str, float] = {}
    for row in rows:
        key = row.get("material_group", "")
        current = row.get("total_spend", 0) or 0
        prev = prev_map.get(key)
        if prev is not None and prev != 0:
            row["yoy_variance_pct"] = round((current - prev) / prev * 100, 2)
        else:
            row["yoy_variance_pct"] = None
        prev_map[key] = current

    return rows


# ---------------------------------------------------------------------------
# 3. Vendor Scorecard
# ---------------------------------------------------------------------------

def get_vendor_scorecard(
    fiscal_year: int,
    grade: str | None = None,
) -> list[dict]:
    """Return vendor_scorecard rows ordered by composite_score descending."""
    sql = "SELECT * FROM vendor_scorecard WHERE fiscal_year = %s"
    params: list[Any] = [fiscal_year]
    if grade:
        sql += " AND grade = %s"
        params.append(grade.upper())
    sql += " ORDER BY composite_score DESC"
    return _fetch_all(sql, params)


# ---------------------------------------------------------------------------
# 4. ABC Analysis
# ---------------------------------------------------------------------------

def get_abc_analysis(
    fiscal_year: int,
    plant: str | None = None,
) -> list[dict]:
    """Compute ABC classification from goods-issue (GI) movements."""
    sql = (
        "SELECT material_no, MAX(material_desc) AS material_desc, "
        "       SUM(ABS(amount_lc)) AS total_consumption_value "
        "FROM mb51_movements "
        "WHERE fiscal_year = %s AND mvt_category = 'GI' "
    )
    params: list[Any] = [fiscal_year]
    if plant:
        sql += " AND plant = %s"
        params.append(plant)
    sql += " GROUP BY material_no ORDER BY total_consumption_value DESC"

    rows = _fetch_all(sql, params)
    if not rows:
        return []

    grand_total = sum(r["total_consumption_value"] or 0 for r in rows)
    if grand_total == 0:
        for r in rows:
            r["abc_class"] = "C"
            r["cumulative_pct"] = 0
        return rows

    cumulative = 0.0
    for r in rows:
        val = r["total_consumption_value"] or 0
        cumulative += val
        pct = cumulative / grand_total * 100
        r["cumulative_pct"] = round(pct, 2)
        if pct <= 70:
            r["abc_class"] = "A"
        elif pct <= 90:
            r["abc_class"] = "B"
        else:
            r["abc_class"] = "C"

    return rows


# ---------------------------------------------------------------------------
# 5. Material Consumption Trend
# ---------------------------------------------------------------------------

def get_material_consumption_trend(
    material_no: str,
    plant: str,
    months: int = 24,
) -> list[dict]:
    """Return monthly GI quantities and values for a material."""
    cutoff = (date.today().replace(day=1) - timedelta(days=months * 31)).strftime("%Y-%m-%d")
    sql = (
        "SELECT DATE_FORMAT(posting_date, '%%Y-%%m') AS period_month, "
        "       SUM(ABS(quantity)) AS gi_qty, "
        "       SUM(ABS(amount_lc)) AS gi_value "
        "FROM mb51_movements "
        "WHERE material_no = %s AND plant = %s "
        "  AND mvt_category = 'GI' AND posting_date >= %s "
        "GROUP BY period_month ORDER BY period_month"
    )
    return _fetch_all(sql, (material_no, plant, cutoff))


# ---------------------------------------------------------------------------
# 6. Dead Stock
# ---------------------------------------------------------------------------

def get_dead_stock(
    plant: str | None = None,
    no_movement_months: int = 6,
) -> list[dict]:
    """Return materials with closing stock but no GI in the last N months."""
    cutoff = (date.today().replace(day=1) - timedelta(days=no_movement_months * 31)).strftime("%Y-%m-%d")
    latest_period = date.today().strftime("%Y-%m")

    sql = (
        "SELECT s.material_no, s.material_desc, s.plant, "
        "       s.closing_qty, s.closing_value, s.period_month "
        "FROM stock_register_monthly s "
        "WHERE s.closing_qty > 0 "
        "  AND s.period_month = ("
        "      SELECT MAX(s2.period_month) FROM stock_register_monthly s2 "
        "      WHERE s2.material_no = s.material_no AND s2.plant = s.plant"
        "  ) "
        "  AND NOT EXISTS ("
        "      SELECT 1 FROM mb51_movements m "
        "      WHERE m.material_no = s.material_no "
        "        AND m.plant = s.plant "
        "        AND m.mvt_category = 'GI' "
        "        AND m.posting_date >= %s"
        "  ) "
    )
    params: list[Any] = [cutoff]
    if plant:
        sql += " AND s.plant = %s"
        params.append(plant)
    sql += " ORDER BY s.closing_value DESC"

    return _fetch_all(sql, params)


# ---------------------------------------------------------------------------
# 7. Price Evolution (MAP trend)
# ---------------------------------------------------------------------------

def get_price_evolution(
    material_no: str,
    plant: str,
) -> list[dict]:
    """Return moving_avg_price per analysis_period from mc9_stock_analysis."""
    sql = (
        "SELECT analysis_period, moving_avg_price "
        "FROM mc9_stock_analysis "
        "WHERE material_no = %s AND plant = %s "
        "ORDER BY analysis_period"
    )
    return _fetch_all(sql, (material_no, plant))


# ---------------------------------------------------------------------------
# 8. Open PO Aging
# ---------------------------------------------------------------------------

def get_open_po_aging(
    plant: str | None = None,
) -> list[dict]:
    """Return open/partial POs grouped by aging bucket."""
    today = date.today().isoformat()
    sql = (
        "SELECT po_number, po_item, po_date, vendor_no, vendor_name, "
        "       material_no, material_desc, plant, "
        "       order_qty, gr_qty, open_qty, net_order_value, "
        "       delivery_date, po_status, "
        "       DATEDIFF(%s, delivery_date) AS days_overdue "
        "FROM me2m_purchase_orders "
        "WHERE po_status IN ('open', 'partial') "
        "  AND delivery_date < %s "
    )
    params: list[Any] = [today, today]
    if plant:
        sql += " AND plant = %s"
        params.append(plant)
    sql += " ORDER BY days_overdue DESC"

    rows = _fetch_all(sql, params)

    # Tag each row with an aging bucket
    for r in rows:
        days = r.get("days_overdue", 0) or 0
        if days <= 30:
            r["aging_bucket"] = "0-30 days"
        elif days <= 60:
            r["aging_bucket"] = "31-60 days"
        elif days <= 90:
            r["aging_bucket"] = "61-90 days"
        else:
            r["aging_bucket"] = "90+ days"

    return rows
