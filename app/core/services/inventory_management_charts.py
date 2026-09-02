"""Chart series for the inventory management review.

Every figure here is already computed by ``inventory_monitoring`` for the tables
and registers on the same page.  This module only reshapes those dicts into JSON
the browser can plot, so the charts cost no extra database work — the route
hands over what it has already fetched.

Two rules shape what is offered as a stacked series.  A stack must be a true
partition of its total, or the chart quietly double-counts: the workbook's own
registers overlap each other — one drum of a non-moving chemical can be named in
the non-moving, the aged and the surplus sheets at once — so those bars are
never summed into a portfolio figure.  And value and line count are separate
readings of the same stock, so the coverage chart toggles between them rather
than plotting one against the other.

Money is plotted in crore, because a rupee axis on portfolio value is unreadable.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.services.inventory_monitoring import SUPPORTING_REGISTERS

# Bars stop being readable long before a table does, so the charts show the
# leading rows and the table underneath carries the rest.
CHART_SERIES_LIMIT = 12

CRORE = Decimal("10000000")

# Only the bands that sit outside healthy coverage are an exposure; healthy and
# unclassified stock is not something to act on, so it is not stacked in.
EXPOSURE_BANDS = (
    ("critical_low_stock", "Critical low", "bad"),
    ("low_stock", "Low stock", "hot"),
    ("slow_moving_stock", "Slow-moving", "warn"),
    ("excess_stock", "Excess", "alt"),
)

BAND_TONES = {
    "critical_low_stock": "bad",
    "low_stock": "hot",
    "healthy_stock": "accent",
    "slow_moving_stock": "warn",
    "excess_stock": "alt",
    "unclassified": "muted",
    "unmatched": "muted",
}

SEVERITY_TONES = {"critical": "bad", "high": "hot", "medium": "warn", "low": "info"}


def _crore(value: Any) -> float:
    """A rupee figure as crore, rounded the way the page prints it."""
    return round(float(Decimal(str(value or 0)) / CRORE), 2)


def _coverage_mix(data: dict[str, Any]) -> dict[str, Any]:
    """Coverage bands, read once by value and once by line count.

    Both readings partition the same stock exactly, which is why they are a
    toggle: a band holding a quarter of the value can hold a fortieth of the
    lines, and plotting them side by side invites the two to be added up.
    """
    mix = data.get("health_mix", [])
    return {
        "labels": [item["label"] for item in mix],
        "tones": [BAND_TONES.get(item["key"], "muted") for item in mix],
        "value": [_crore(item["value"]) for item in mix],
        "count": [item["count"] for item in mix],
        "share": [item["share"] for item in mix],
    }


def _zones(data: dict[str, Any]) -> dict[str, Any]:
    """Value by zone, with the comparison period where there is one."""
    zones = data.get("zones", [])[:CHART_SERIES_LIMIT]
    return {
        "labels": [item["zone"] for item in zones],
        "value": [_crore(item["value"]) for item in zones],
        "previous": [None if item["prev"] is None else _crore(item["prev"]) for item in zones],
        "share": [item["share"] for item in zones],
        "has_previous": any(item["prev"] is not None for item in zones),
    }


def _centres(data: dict[str, Any]) -> dict[str, Any]:
    """The largest holdings by asset, each bar linking to that asset's page."""
    centres = data.get("centres", [])[:CHART_SERIES_LIMIT]
    return {
        "labels": [item["name"] for item in centres],
        "ids": [item["id"] for item in centres],
        "value": [_crore(item["value"]) for item in centres],
        "share": [item["share"] for item in centres],
        "exceptions": [item["exceptions"] for item in centres],
    }


def _exception_severities(data: dict[str, Any]) -> dict[str, Any]:
    """Open exceptions by severity — a true partition of the exception total."""
    counts = data.get("exception_severities", {})
    ordered = [key for key in ("critical", "high", "medium", "low") if counts.get(key)]
    ordered += sorted(key for key in counts if key not in SEVERITY_TONES and counts[key])
    return {
        "labels": [key.capitalize() for key in ordered],
        "values": [counts[key] for key in ordered],
        "tones": [SEVERITY_TONES.get(key, "muted") for key in ordered],
    }


def _movers(data: dict[str, Any]) -> dict[str, Any]:
    """Assets whose held value moved most, gains and falls on one signed axis.

    Only assets reporting in both periods are here — an asset that appears or
    disappears has not moved, it has changed the scope, and the page says so
    separately rather than drawing it as a swing.
    """
    movers = data.get("movers", {})
    rows = sorted(
        [*movers.get("up", []), *movers.get("down", [])],
        key=lambda item: abs(item["delta"]),
        reverse=True,
    )[:CHART_SERIES_LIMIT]
    rows.sort(key=lambda item: item["delta"], reverse=True)
    return {
        "labels": [item["name"] for item in rows],
        "delta": [_crore(item["delta"]) for item in rows],
        "value": [_crore(item["value"]) for item in rows],
        "previous": [_crore(item["prev"]) for item in rows],
    }


def _consumption(data: dict[str, Any]) -> dict[str, Any]:
    """Twelve-month consumption against stock held, per material.

    Consumption is SAP's own all-ONGC figure from the workbook's material
    summary sheet; the held value is this period's snapshot.  They are two
    series on one chart because the comparison is the point — a material
    consuming little and held in quantity is the whole finding — but they are
    never stacked, because one is a year of flow and the other is a position.
    """
    rows = data.get("consumption", {}).get("by_value", [])[:CHART_SERIES_LIMIT]
    return {
        "labels": [item["code"] for item in rows],
        "descriptions": [item["description"] or "" for item in rows],
        "consumption": [_crore(item["consumption_value"]) for item in rows],
        "inventory": [_crore(item["inventory_value"]) for item in rows],
        "months": [None if item["stock_months"] is None else float(item["stock_months"]) for item in rows],
    }


def _exposure_by_centre(health: dict[str, Any]) -> dict[str, Any]:
    """Lines outside healthy coverage, by asset, split across the four bands.

    The four bands are disjoint, so this stack is a true partition of one
    asset's exposed line count.  Assets are ranked by the shortage bands first,
    because a stock-out stops work and an overstock does not.
    """
    groups = health.get("groups", {})
    counts: dict[str, dict[str, int]] = {}
    for key, _label, _tone in EXPOSURE_BANDS:
        for record in groups.get(key, []):
            counts.setdefault(record.work_center_name, dict.fromkeys(
                (band for band, _l, _t in EXPOSURE_BANDS), 0
            ))[key] += 1
    ranked = sorted(
        counts.items(),
        key=lambda item: (
            -(item[1]["critical_low_stock"] + item[1]["low_stock"]),
            -sum(item[1].values()),
            item[0].casefold(),
        ),
    )[:CHART_SERIES_LIMIT]
    return {
        "labels": [name for name, _ in ranked],
        "series": [
            {"label": label, "tone": tone, "data": [row[key] for _name, row in ranked]}
            for key, label, tone in EXPOSURE_BANDS
        ],
    }


def _source_registers(health: dict[str, Any]) -> dict[str, Any]:
    """Where each workbook register's lines land in our own coverage bands.

    One bar is one register, stacked by band — that stack is exact.  The bars
    are not comparable as a total: a chemical the workbook calls non-moving is
    usually also in its aged and surplus registers, so summing the bars would
    count the same drum three times.
    """
    bands = health.get("source_bands", {})
    present = [(kind, label) for kind, label, _description in SUPPORTING_REGISTERS if bands.get(kind)]
    counts = {kind: {entry["key"]: entry["count"] for entry in bands[kind]} for kind, _label in present}
    band_labels = {entry["key"]: entry["label"] for entries in bands.values() for entry in entries}
    band_order = list(BAND_TONES)
    band_keys = sorted(
        {key for row in counts.values() for key in row},
        key=lambda key: band_order.index(key) if key in band_order else len(band_order),
    )
    return {
        "labels": [label for _kind, label in present],
        "series": [
            {
                "label": band_labels.get(key, key.replace("_", " ").capitalize()),
                "tone": BAND_TONES.get(key, "muted"),
                "data": [counts[kind].get(key, 0) for kind, _label in present],
            }
            for key in band_keys
        ],
    }


def management_chart_series(data: dict[str, Any], health: dict[str, Any]) -> dict[str, Any]:
    """Reshape the management review's own data into plottable series.

    ``data`` is one published period's portfolio and ``health`` is the register
    of line items for that same period.  They are kept in separate branches of
    the result for the same reason the page keeps them on separate tabs: the
    position is money held on a date, movement is money against an earlier date
    over the assets common to both, and the register counts lines rather than
    rupees.  A chart that blended them would say something untrue.
    """
    if not data.get("kpis"):
        return {}
    return {
        "position": {
            "mix": _coverage_mix(data),
            "zones": _zones(data),
            "centres": _centres(data),
            "exceptions": _exception_severities(data),
        },
        "movement": {
            "has_comparison": data.get("comparison") is not None,
            "movers": _movers(data),
            "consumption": _consumption(data),
        },
        "register": {
            "exposure": _exposure_by_centre(health),
            "source_registers": _source_registers(health),
        },
    }
