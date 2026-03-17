"""Demand forecasting and XYZ classification for the Inventory module.

Provides four forecasting methods and a coefficient-of-variation-based
XYZ classification.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Any

import numpy as np

from app.extensions import db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_all(sql: str, params: tuple | list) -> list[dict]:
    connection = db.engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    from decimal import Decimal
    result = []
    for row in rows:
        d: dict[str, Any] = {}
        for k, v in zip(columns, row):
            if isinstance(v, Decimal):
                d[k] = float(v)
            elif isinstance(v, date):
                d[k] = v.isoformat()
            else:
                d[k] = v
        result.append(d)
    return result


def _month_add(period: str, n: int) -> str:
    """Add *n* months to a YYYY-MM string."""
    y, m = int(period[:4]), int(period[5:7])
    m += n
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def _get_gi_history(material_no: str, plant: str, months: int) -> list[dict]:
    """Retrieve monthly GI quantities from mb51_movements."""
    cutoff = (date.today().replace(day=1) - timedelta(days=months * 31)).strftime("%Y-%m-%d")
    sql = (
        "SELECT DATE_FORMAT(posting_date, '%%Y-%%m') AS period_month, "
        "       SUM(ABS(quantity)) AS actual_qty "
        "FROM mb51_movements "
        "WHERE material_no = %s AND plant = %s "
        "  AND mvt_category = 'GI' AND posting_date >= %s "
        "GROUP BY period_month ORDER BY period_month"
    )
    return _fetch_all(sql, (material_no, plant, cutoff))


# ---------------------------------------------------------------------------
# Forecast methods
# ---------------------------------------------------------------------------

def _simple_moving_avg(
    history: list[dict], periods_ahead: int
) -> tuple[list[dict], list[dict]]:
    """Average of last 12 months projected forward as flat line."""
    recent = history[-12:] if len(history) >= 12 else history
    if not recent:
        return [], []

    avg = sum(h["actual_qty"] for h in recent) / len(recent)
    std = float(np.std([h["actual_qty"] for h in recent])) if len(recent) > 1 else 0

    smoothed = [
        {"period_month": h["period_month"], "actual_qty": h["actual_qty"], "smoothed_qty": avg}
        for h in history
    ]

    last_period = history[-1]["period_month"]
    forecast = []
    for i in range(1, periods_ahead + 1):
        pm = _month_add(last_period, i)
        forecast.append({
            "period_month": pm,
            "forecast_qty": round(avg, 3),
            "lower_bound": round(max(avg - 1.96 * std, 0), 3),
            "forecast_value": None,
        })

    return smoothed, forecast


def _weighted_moving_avg(
    history: list[dict], periods_ahead: int
) -> tuple[list[dict], list[dict]]:
    """Linearly-weighted average of last 12 months."""
    recent = history[-12:] if len(history) >= 12 else history
    if not recent:
        return [], []

    n = len(recent)
    weights = list(range(1, n + 1))
    w_sum = sum(weights)
    wma = sum(h["actual_qty"] * w for h, w in zip(recent, weights)) / w_sum
    std = float(np.std([h["actual_qty"] for h in recent])) if n > 1 else 0

    smoothed = [
        {"period_month": h["period_month"], "actual_qty": h["actual_qty"], "smoothed_qty": wma}
        for h in history
    ]

    last_period = history[-1]["period_month"]
    forecast = []
    for i in range(1, periods_ahead + 1):
        pm = _month_add(last_period, i)
        forecast.append({
            "period_month": pm,
            "forecast_qty": round(wma, 3),
            "lower_bound": round(max(wma - 1.96 * std, 0), 3),
            "forecast_value": None,
        })

    return smoothed, forecast


def _exponential_smoothing(
    history: list[dict], periods_ahead: int, alpha: float = 0.3
) -> tuple[list[dict], list[dict]]:
    """Single exponential smoothing with alpha=0.3."""
    if not history:
        return [], []

    smoothed_vals = []
    s = history[0]["actual_qty"]
    smoothed_vals.append(s)
    for h in history[1:]:
        s = alpha * h["actual_qty"] + (1 - alpha) * s
        smoothed_vals.append(s)

    smoothed = [
        {
            "period_month": h["period_month"],
            "actual_qty": h["actual_qty"],
            "smoothed_qty": round(sv, 3),
        }
        for h, sv in zip(history, smoothed_vals)
    ]

    # Forecast = last smoothed value
    last_smooth = smoothed_vals[-1]
    residuals = [h["actual_qty"] - sv for h, sv in zip(history, smoothed_vals)]
    std = float(np.std(residuals)) if len(residuals) > 1 else 0

    last_period = history[-1]["period_month"]
    forecast = []
    for i in range(1, periods_ahead + 1):
        pm = _month_add(last_period, i)
        forecast.append({
            "period_month": pm,
            "forecast_qty": round(last_smooth, 3),
            "lower_bound": round(max(last_smooth - 1.96 * std, 0), 3),
            "forecast_value": None,
        })

    return smoothed, forecast


def _seasonal_index(
    history: list[dict], periods_ahead: int
) -> tuple[list[dict], list[dict]]:
    """Seasonal decomposition requiring minimum 3 years of data."""
    if len(history) < 36:
        # Fall back to weighted moving average
        return _weighted_moving_avg(history, periods_ahead)

    # Build per-fiscal-period indices (period 1 = April, period 12 = March)
    from app.modules.inventory.ingestion.base_loader import fiscal_period_from_period

    period_sums: dict[int, list[float]] = {p: [] for p in range(1, 13)}
    for h in history:
        fp = fiscal_period_from_period(h["period_month"])
        period_sums[fp].append(h["actual_qty"])

    overall_avg = sum(h["actual_qty"] for h in history) / len(history)
    if overall_avg == 0:
        return _weighted_moving_avg(history, periods_ahead)

    seasonal_idx: dict[int, float] = {}
    for fp, vals in period_sums.items():
        if vals:
            seasonal_idx[fp] = (sum(vals) / len(vals)) / overall_avg
        else:
            seasonal_idx[fp] = 1.0

    # Deseasonalised history
    smoothed = []
    for h in history:
        fp = fiscal_period_from_period(h["period_month"])
        idx = seasonal_idx.get(fp, 1.0)
        deseason = h["actual_qty"] / idx if idx != 0 else h["actual_qty"]
        smoothed.append({
            "period_month": h["period_month"],
            "actual_qty": h["actual_qty"],
            "smoothed_qty": round(deseason, 3),
        })

    # Trailing 12 month average (deseasonalised)
    recent = history[-12:]
    base_avg = sum(h["actual_qty"] for h in recent) / len(recent)
    std = float(np.std([h["actual_qty"] for h in recent])) if len(recent) > 1 else 0

    last_period = history[-1]["period_month"]
    forecast = []
    for i in range(1, periods_ahead + 1):
        pm = _month_add(last_period, i)
        fp = fiscal_period_from_period(pm)
        idx = seasonal_idx.get(fp, 1.0)
        fc = base_avg * idx
        forecast.append({
            "period_month": pm,
            "forecast_qty": round(fc, 3),
            "lower_bound": round(max(fc - 1.96 * std * idx, 0), 3),
            "forecast_value": None,
        })

    return smoothed, forecast


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_METHODS = {
    "simple_moving_avg": _simple_moving_avg,
    "weighted_moving_avg": _weighted_moving_avg,
    "exponential_smoothing": _exponential_smoothing,
    "seasonal_index": _seasonal_index,
}

_HISTORY_MONTHS = {
    "simple_moving_avg": 12,
    "weighted_moving_avg": 12,
    "exponential_smoothing": 24,
    "seasonal_index": 48,
}


def forecast_material(
    material_no: str,
    plant: str,
    method: str = "weighted_moving_avg",
    periods_ahead: int = 3,
) -> dict[str, Any]:
    """Generate a demand forecast for a single material-plant.

    Returns
    -------
    dict with keys: material_no, plant, method, historical, forecast, data_quality
    """
    if method not in _METHODS:
        method = "weighted_moving_avg"

    look_back = _HISTORY_MONTHS.get(method, 24)
    history = _get_gi_history(material_no, plant, look_back)

    months_available = len(history)
    method_eligible = True
    warnings: list[str] = []

    if method == "seasonal_index" and months_available < 36:
        warnings.append(
            f"seasonal_index requires >=36 months of data; only {months_available} available. "
            "Falling back to weighted_moving_avg."
        )
        method_eligible = False

    fn = _METHODS[method]
    smoothed, forecast = fn(history, periods_ahead)

    return {
        "material_no": material_no,
        "plant": plant,
        "method": method,
        "historical": smoothed,
        "forecast": forecast,
        "data_quality": {
            "months_available": months_available,
            "method_eligible": method_eligible,
            "warnings": warnings,
        },
    }


# ---------------------------------------------------------------------------
# XYZ Classification
# ---------------------------------------------------------------------------

def get_xyz_classification(
    fiscal_year: int,
    plant: str | None = None,
) -> list[dict]:
    """Classify materials by demand variability (coefficient of variation).

    X = CV < 0.5  (stable)
    Y = CV 0.5–1.0 (variable)
    Z = CV > 1.0  (erratic)
    """
    # Retrieve monthly GI quantities per material for the fiscal year
    sql = (
        "SELECT material_no, "
        "       DATE_FORMAT(posting_date, '%%Y-%%m') AS period_month, "
        "       SUM(ABS(quantity)) AS monthly_qty "
        "FROM mb51_movements "
        "WHERE fiscal_year = %s AND mvt_category = 'GI' "
    )
    params: list[Any] = [fiscal_year]
    if plant:
        sql += " AND plant = %s"
        params.append(plant)
    sql += " GROUP BY material_no, period_month ORDER BY material_no, period_month"

    rows = _fetch_all(sql, params)

    # Group by material
    by_mat: dict[str, list[float]] = {}
    for r in rows:
        by_mat.setdefault(r["material_no"], []).append(r["monthly_qty"] or 0)

    results: list[dict] = []
    for mat, quantities in by_mat.items():
        arr = np.array(quantities, dtype=float)
        mean_q = float(np.mean(arr))
        std_q = float(np.std(arr))
        cv = std_q / mean_q if mean_q > 0 else float("inf")

        if cv < 0.5:
            xyz = "X"
        elif cv <= 1.0:
            xyz = "Y"
        else:
            xyz = "Z"

        results.append({
            "material_no": mat,
            "mean_monthly_qty": round(mean_q, 3),
            "cv": round(cv, 4),
            "xyz_class": xyz,
        })

    results.sort(key=lambda r: r["cv"])
    return results
