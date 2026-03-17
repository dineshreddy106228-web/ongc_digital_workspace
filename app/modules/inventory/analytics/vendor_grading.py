"""Build the ``vendor_scorecard`` computed table from ME2M purchase orders.

Computes OTD%, quantity accuracy, lead-time statistics, a weighted composite
score, and an A/B/C/D grade for each vendor per fiscal year.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from app.extensions import db

logger = logging.getLogger(__name__)


def build_vendor_scorecard(fiscal_year: int) -> dict[str, Any]:
    """Compute vendor scorecards for *fiscal_year* and upsert results.

    Returns
    -------
    dict with keys: vendors_graded, grade_distribution
    """
    connection = db.engine.raw_connection()
    try:
        cursor = connection.cursor()

        # Pull aggregated vendor metrics from me2m_purchase_orders
        sql = """
            SELECT
                vendor_no,
                MAX(vendor_name)                                    AS vendor_name,
                COUNT(DISTINCT po_number)                           AS total_pos,
                SUM(net_order_value)                                AS total_spend,
                -- OTD: count of lines delivered on or before promised date
                SUM(CASE WHEN actual_gr_date IS NOT NULL
                              AND delivery_variance_days <= 0
                         THEN 1 ELSE 0 END)                        AS otd_count,
                SUM(CASE WHEN actual_gr_date IS NOT NULL
                         THEN 1 ELSE 0 END)                        AS gr_count,
                -- Quantity accuracy: avg(gr_qty / order_qty) capped at 1
                AVG(CASE WHEN order_qty > 0
                         THEN LEAST(COALESCE(gr_qty, 0) / order_qty, 1.0)
                         ELSE NULL END)                             AS qty_accuracy_ratio,
                AVG(CASE WHEN lead_time_days IS NOT NULL
                         THEN lead_time_days ELSE NULL END)         AS avg_lt,
                STDDEV(CASE WHEN lead_time_days IS NOT NULL
                            THEN lead_time_days ELSE NULL END)      AS stddev_lt
            FROM me2m_purchase_orders
            WHERE fiscal_year = %s
            GROUP BY vendor_no
        """
        cursor.execute(sql, (fiscal_year,))
        rows = cursor.fetchall()
        col_names = [
            "vendor_no", "vendor_name", "total_pos", "total_spend",
            "otd_count", "gr_count", "qty_accuracy_ratio",
            "avg_lt", "stddev_lt",
        ]
        vendors = [dict(zip(col_names, r)) for r in rows]

        grade_dist: dict[str, int] = {"A": 0, "B": 0, "C": 0, "D": 0}

        upsert_sql = """
            INSERT INTO vendor_scorecard
                (vendor_no, vendor_name, fiscal_year,
                 total_pos, total_spend, otd_pct, qty_accuracy_pct,
                 avg_lead_time_days, lead_time_stddev,
                 composite_score, grade)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                vendor_name        = VALUES(vendor_name),
                total_pos          = VALUES(total_pos),
                total_spend        = VALUES(total_spend),
                otd_pct            = VALUES(otd_pct),
                qty_accuracy_pct   = VALUES(qty_accuracy_pct),
                avg_lead_time_days = VALUES(avg_lead_time_days),
                lead_time_stddev   = VALUES(lead_time_stddev),
                composite_score    = VALUES(composite_score),
                grade              = VALUES(grade)
        """

        for v in vendors:
            otd_pct = _safe_pct(v["otd_count"], v["gr_count"])
            qty_acc = _safe_mul(v["qty_accuracy_ratio"], 100, cap=100)
            avg_lt = v["avg_lt"]
            stddev_lt = v["stddev_lt"]

            # Normalised lead-time variability (0 = perfect, 100 = worst)
            if stddev_lt is not None and not (isinstance(stddev_lt, float) and math.isnan(stddev_lt)):
                norm_stddev = min(float(stddev_lt) / 30 * 100, 100)
            else:
                norm_stddev = 50  # neutral default when no data

            consistency_score = 100 - norm_stddev

            composite = (
                (otd_pct or 0) * 0.40
                + (qty_acc or 0) * 0.35
                + consistency_score * 0.25
            )
            composite = round(composite, 2)

            if composite >= 85:
                grade = "A"
            elif composite >= 70:
                grade = "B"
            elif composite >= 50:
                grade = "C"
            else:
                grade = "D"

            grade_dist[grade] += 1

            cursor.execute(upsert_sql, (
                v["vendor_no"], v["vendor_name"], fiscal_year,
                v["total_pos"], v["total_spend"],
                otd_pct, qty_acc,
                _round(avg_lt, 1), _round(stddev_lt, 2),
                composite, grade,
            ))

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

    return {
        "vendors_graded": len(vendors),
        "grade_distribution": grade_dist,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_pct(numerator, denominator) -> float | None:
    if not numerator or not denominator:
        return None
    try:
        return round(float(numerator) / float(denominator) * 100, 2)
    except (ZeroDivisionError, TypeError):
        return None


def _safe_mul(val, factor, cap=None) -> float | None:
    if val is None:
        return None
    try:
        result = float(val) * factor
        if cap is not None:
            result = min(result, cap)
        return round(result, 2)
    except (TypeError, ValueError):
        return None


def _round(val, digits) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f):
            return None
        return round(f, digits)
    except (TypeError, ValueError):
        return None
