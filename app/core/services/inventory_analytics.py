"""Inventory analytics helpers.

All queries operate against InventoryRecord and return plain Python dicts /
lists that are safe to pass directly to Jinja2 templates.

Query pattern: SQLAlchemy Core-style queries via db.session to avoid ORM
overhead on large result sets.
"""

from __future__ import annotations

import calendar
import logging
from decimal import Decimal

from sqlalchemy import func, select, distinct

from app.extensions import db
from app.models.inventory.inventory_record import InventoryRecord
from app.models.inventory.inventory_upload import InventoryUpload

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _period_label(year: int | None, month: int | None) -> str:
    if year and month:
        return f"{calendar.month_abbr[month]} {year}"
    return "Unknown"


def _safe_float(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# ── KPI summary ────────────────────────────────────────────────────────────────

def get_inventory_kpis() -> dict:
    """
    Returns headline KPI figures for the inventory dashboard.
    {
        total_records, unique_materials, unique_plants,
        total_value, currency, earliest_period, latest_period,
        upload_count
    }
    """
    record_count = db.session.execute(
        select(func.count(InventoryRecord.id))
    ).scalar() or 0

    unique_materials = db.session.execute(
        select(func.count(distinct(InventoryRecord.material)))
    ).scalar() or 0

    unique_plants = db.session.execute(
        select(func.count(distinct(InventoryRecord.plant)))
    ).scalar() or 0

    total_value_row = db.session.execute(
        select(
            func.sum(InventoryRecord.total_usage_value),
            InventoryRecord.currency,
        ).group_by(InventoryRecord.currency)
        .order_by(func.sum(InventoryRecord.total_usage_value).desc())
        .limit(1)
    ).first()

    total_value = _safe_float(total_value_row[0]) if total_value_row else 0.0
    currency = total_value_row[1] if total_value_row else "—"

    # Earliest / latest periods
    bounds = db.session.execute(
        select(
            func.min(InventoryRecord.period_year * 100 + InventoryRecord.period_month),
            func.max(InventoryRecord.period_year * 100 + InventoryRecord.period_month),
        )
    ).first()

    def _decode_period(encoded):
        if not encoded:
            return "—"
        return _period_label(encoded // 100, encoded % 100)

    earliest = _decode_period(bounds[0]) if bounds else "—"
    latest = _decode_period(bounds[1]) if bounds else "—"

    upload_count = db.session.execute(
        select(func.count(InventoryUpload.id))
        .where(InventoryUpload.status == "success")
    ).scalar() or 0

    return {
        "total_records": record_count,
        "unique_materials": unique_materials,
        "unique_plants": unique_plants,
        "total_value": total_value,
        "currency": currency,
        "earliest_period": earliest,
        "latest_period": latest,
        "upload_count": upload_count,
    }


# ── Monthly trend ──────────────────────────────────────────────────────────────

def get_monthly_trend(plant: str | None = None, limit_months: int = 24) -> dict:
    """
    Aggregate total usage value and quantity by period.

    Returns:
        {
            labels: ["Jan 2024", "Feb 2024", ...],
            values: [123456.78, ...],   # total_usage_value per month
            quantities: [45.0, ...],    # total_usage per month
        }
    """
    q = (
        select(
            InventoryRecord.period_year,
            InventoryRecord.period_month,
            func.sum(InventoryRecord.total_usage_value).label("total_val"),
            func.sum(InventoryRecord.total_usage).label("total_qty"),
        )
        .where(
            InventoryRecord.period_year.isnot(None),
            InventoryRecord.period_month.isnot(None),
        )
        .group_by(InventoryRecord.period_year, InventoryRecord.period_month)
        .order_by(InventoryRecord.period_year, InventoryRecord.period_month)
    )

    if plant:
        q = q.where(InventoryRecord.plant == plant)

    rows = db.session.execute(q).all()

    # Limit to most recent N months
    if len(rows) > limit_months:
        rows = rows[-limit_months:]

    labels = [_period_label(r.period_year, r.period_month) for r in rows]
    values = [round(_safe_float(r.total_val), 2) for r in rows]
    quantities = [round(_safe_float(r.total_qty), 3) for r in rows]

    return {"labels": labels, "values": values, "quantities": quantities}


# ── Top consumers ──────────────────────────────────────────────────────────────

def get_top_consumers(
    top_n: int = 20,
    plant: str | None = None,
    material_group: str | None = None,
    period_year: int | None = None,
) -> list[dict]:
    """
    Returns the top N materials by total usage value.
    Each dict: {rank, material, plant, material_group, material_type,
                total_value, total_usage, uom, currency}
    """
    q = (
        select(
            InventoryRecord.material,
            InventoryRecord.material_group,
            InventoryRecord.material_type,
            InventoryRecord.plant,
            InventoryRecord.usage_uom,
            InventoryRecord.currency,
            func.sum(InventoryRecord.total_usage_value).label("total_val"),
            func.sum(InventoryRecord.total_usage).label("total_qty"),
        )
        .group_by(
            InventoryRecord.material,
            InventoryRecord.material_group,
            InventoryRecord.material_type,
            InventoryRecord.plant,
            InventoryRecord.usage_uom,
            InventoryRecord.currency,
        )
        .order_by(func.sum(InventoryRecord.total_usage_value).desc())
        .limit(top_n)
    )

    if plant:
        q = q.where(InventoryRecord.plant == plant)
    if material_group:
        q = q.where(InventoryRecord.material_group == material_group)
    if period_year:
        q = q.where(InventoryRecord.period_year == period_year)

    rows = db.session.execute(q).all()
    return [
        {
            "rank": idx + 1,
            "material": r.material or "—",
            "material_group": r.material_group or "—",
            "material_type": r.material_type or "—",
            "plant": r.plant or "—",
            "total_value": round(_safe_float(r.total_val), 2),
            "total_usage": round(_safe_float(r.total_qty), 3),
            "uom": r.usage_uom or "—",
            "currency": r.currency or "—",
        }
        for idx, r in enumerate(rows)
    ]


# ── Material search / list ─────────────────────────────────────────────────────

def search_materials(
    query: str = "",
    plant: str | None = None,
    material_group: str | None = None,
    page: int = 1,
    per_page: int = 50,
) -> dict:
    """
    Paginated list of distinct materials with aggregated usage totals.

    Returns:
        {items: [...], total: int, page: int, per_page: int, pages: int}
    """
    base_q = (
        select(
            InventoryRecord.material,
            InventoryRecord.material_group,
            InventoryRecord.material_type,
            InventoryRecord.plant,
            InventoryRecord.currency,
            InventoryRecord.usage_uom,
            func.sum(InventoryRecord.total_usage_value).label("total_val"),
            func.sum(InventoryRecord.total_usage).label("total_qty"),
            func.count(InventoryRecord.id).label("record_count"),
        )
        .group_by(
            InventoryRecord.material,
            InventoryRecord.material_group,
            InventoryRecord.material_type,
            InventoryRecord.plant,
            InventoryRecord.currency,
            InventoryRecord.usage_uom,
        )
        .order_by(func.sum(InventoryRecord.total_usage_value).desc())
    )

    if query:
        like = f"%{query}%"
        base_q = base_q.where(InventoryRecord.material.ilike(like))
    if plant:
        base_q = base_q.where(InventoryRecord.plant == plant)
    if material_group:
        base_q = base_q.where(InventoryRecord.material_group == material_group)

    # Count subquery
    count_q = select(func.count()).select_from(base_q.subquery())
    total = db.session.execute(count_q).scalar() or 0

    offset = (page - 1) * per_page
    rows = db.session.execute(base_q.offset(offset).limit(per_page)).all()

    items = [
        {
            "material": r.material or "—",
            "material_group": r.material_group or "—",
            "material_type": r.material_type or "—",
            "plant": r.plant or "—",
            "total_value": round(_safe_float(r.total_val), 2),
            "total_usage": round(_safe_float(r.total_qty), 3),
            "uom": r.usage_uom or "—",
            "currency": r.currency or "—",
            "record_count": r.record_count,
        }
        for r in rows
    ]

    pages = max(1, (total + per_page - 1) // per_page)
    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": pages,
    }


# ── Material drilldown ─────────────────────────────────────────────────────────

def get_material_detail(material: str, plant: str | None = None) -> dict | None:
    """
    Full history for a specific material (optionally filtered by plant).

    Returns:
        {
            material, plant, material_group, material_type, division,
            business_area, currency, uom,
            monthly_trend: {labels, values, quantities},
            summary: {total_value, total_usage, months_active},
        }
    """
    q = (
        select(
            InventoryRecord.period_year,
            InventoryRecord.period_month,
            InventoryRecord.material_group,
            InventoryRecord.material_type,
            InventoryRecord.business_area,
            InventoryRecord.division,
            InventoryRecord.currency,
            InventoryRecord.usage_uom,
            func.sum(InventoryRecord.total_usage_value).label("total_val"),
            func.sum(InventoryRecord.total_usage).label("total_qty"),
        )
        .where(InventoryRecord.material == material)
        .group_by(
            InventoryRecord.period_year,
            InventoryRecord.period_month,
            InventoryRecord.material_group,
            InventoryRecord.material_type,
            InventoryRecord.business_area,
            InventoryRecord.division,
            InventoryRecord.currency,
            InventoryRecord.usage_uom,
        )
        .order_by(InventoryRecord.period_year, InventoryRecord.period_month)
    )

    if plant:
        q = q.where(InventoryRecord.plant == plant)

    rows = db.session.execute(q).all()
    if not rows:
        return None

    first = rows[0]
    labels = [_period_label(r.period_year, r.period_month) for r in rows]
    values = [round(_safe_float(r.total_val), 2) for r in rows]
    quantities = [round(_safe_float(r.total_qty), 3) for r in rows]
    total_value = sum(values)
    total_usage = sum(quantities)

    return {
        "material": material,
        "plant": plant or "All Plants",
        "material_group": first.material_group or "—",
        "material_type": first.material_type or "—",
        "business_area": first.business_area or "—",
        "division": first.division or "—",
        "currency": first.currency or "—",
        "uom": first.usage_uom or "—",
        "monthly_trend": {
            "labels": labels,
            "values": values,
            "quantities": quantities,
        },
        "summary": {
            "total_value": round(total_value, 2),
            "total_usage": round(total_usage, 3),
            "months_active": len([v for v in values if v > 0]),
        },
    }


# ── Filter option lists ────────────────────────────────────────────────────────

def get_filter_options() -> dict:
    """Returns distinct values for UI filter dropdowns."""
    plants = [
        r[0] for r in db.session.execute(
            select(distinct(InventoryRecord.plant))
            .where(InventoryRecord.plant.isnot(None))
            .order_by(InventoryRecord.plant)
        ).all()
        if r[0]
    ]
    material_groups = [
        r[0] for r in db.session.execute(
            select(distinct(InventoryRecord.material_group))
            .where(InventoryRecord.material_group.isnot(None))
            .order_by(InventoryRecord.material_group)
        ).all()
        if r[0]
    ]
    years = [
        r[0] for r in db.session.execute(
            select(distinct(InventoryRecord.period_year))
            .where(InventoryRecord.period_year.isnot(None))
            .order_by(InventoryRecord.period_year.desc())
        ).all()
        if r[0]
    ]
    return {
        "plants": plants,
        "material_groups": material_groups,
        "years": years,
    }
