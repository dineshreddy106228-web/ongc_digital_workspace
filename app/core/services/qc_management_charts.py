"""Chart series for the QC management review.

Every figure here is already computed by ``sap_quality_control`` for the tables
on the same page.  This module only reshapes those dicts into JSON the browser
can plot, so adding the charts costs no extra database work — the route hands
over what it has already fetched.

Two rules shape what is offered as a stacked series.  A stack must be a true
partition of its total, or the chart quietly double-counts: ``stt_overdue``
overlaps ``awaiting_lab``, so those two never share a stack.  And a rate is
carried alongside the count it was measured over, so a bar drawn from one
decision is not read as confidently as a bar drawn from fifty.
"""

from __future__ import annotations

from typing import Any

# Bars stop being readable long before a table does, so the charts show the
# leading rows and the table underneath carries the rest.
CHART_SERIES_LIMIT = 12


def _reporting_reviews(management: dict[str, Any]) -> list[dict[str, Any]]:
    """Laboratories that actually have a snapshot, worst position first."""
    reviews = [
        review for review in management.get("laboratory_reviews", [])
        if review.get("batch") is not None
    ]
    return sorted(
        reviews,
        key=lambda review: (
            -review["kpis"]["stt_overdue"],
            -review["kpis"]["actionable_open"],
            review["laboratory"]["name"].casefold(),
        ),
    )


def _workload_by_laboratory(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    """Open workload per laboratory, offered under two honest segmentations.

    Both add up to ``actionable_open``.  "Response" partitions on the
    reconciliation state — an actionable record is awaiting the laboratory, has
    a returned update, or is waiting for SAP to confirm a completion the
    laboratory has declared.  "Timing" partitions on the Corporate
    Specification testing time.  They are alternative readings of the same
    bar, which is why the chart toggles rather than stacking all five.
    """
    labels, awaiting_lab, lab_updated, awaiting_sap, past_stt, within_stt = [], [], [], [], [], []
    for review in reviews:
        kpis = review["kpis"]
        open_count = kpis["actionable_open"]
        labels.append(review["laboratory"]["name"])
        awaiting_lab.append(kpis["awaiting_lab"])
        awaiting_sap.append(kpis["awaiting_sap_confirmation"])
        # The remainder of the partition: an update came back but the work is
        # not yet declared complete.
        lab_updated.append(open_count - kpis["awaiting_lab"] - kpis["awaiting_sap_confirmation"])
        past_stt.append(kpis["stt_overdue"])
        within_stt.append(open_count - kpis["stt_overdue"])
    return {
        "labels": labels,
        "response": [
            {"label": "Lab update requested", "tone": "warn", "data": awaiting_lab},
            {"label": "Lab update received", "tone": "accent", "data": lab_updated},
            {"label": "Lab complete — SAP pending", "tone": "info", "data": awaiting_sap},
        ],
        "timing": [
            {"label": "Past STT", "tone": "bad", "data": past_stt},
            {"label": "Within STT", "tone": "ok", "data": within_stt},
        ],
    }


def _movement(management: dict[str, Any]) -> dict[str, Any]:
    """Previous against current SAP-open, for laboratories with both."""
    trend = [item for item in management.get("trend", []) if item["previous_open"] is not None]
    trend.sort(key=lambda item: (-(item["open_change"] or 0), item["laboratory"]["name"].casefold()))
    return {
        "labels": [item["laboratory"]["name"] for item in trend],
        "previous": [item["previous_open"] for item in trend],
        "current": [item["current_open"] for item in trend],
    }


def _usage_decisions(management: dict[str, Any]) -> dict[str, Any]:
    decisions = management.get("usage_decisions", [])
    return {
        "labels": [item["label"] for item in decisions],
        "values": [item["count"] for item in decisions],
        "tones": [{"success": "ok", "danger": "bad"}.get(item["tone"], "muted") for item in decisions],
    }


def _laboratory_performance(portfolio: dict[str, Any]) -> dict[str, Any]:
    """On-time rate against the Corporate Specification testing time.

    A laboratory with nothing measurable is left out rather than plotted at
    zero — no measurement is not a failed measurement.
    """
    rows = [
        item for item in portfolio.get("laboratories", [])
        if item["stt_on_time_rate"] is not None
    ]
    return {
        "labels": [item["laboratory"]["name"] for item in rows],
        "values": [item["stt_on_time_rate"] for item in rows],
        "measured": [item["stt_measured"] for item in rows],
        "median_turnaround": [item["median_turnaround_days"] for item in rows],
    }


def _materials_by_load(portfolio: dict[str, Any]) -> dict[str, Any]:
    rows = portfolio.get("materials_by_load", [])[:CHART_SERIES_LIMIT]
    return {
        "labels": [item["material_description"] for item in rows],
        "completed": [item["completed"] for item in rows],
        "open": [item["open"] for item in rows],
    }


def _materials_by_failure(portfolio: dict[str, Any]) -> dict[str, Any]:
    rows = portfolio.get("materials_by_failure", [])[:CHART_SERIES_LIMIT]
    return {
        "labels": [item["material_description"] for item in rows],
        "values": [item["rejection_rate"] for item in rows],
        "decided": [item["decided"] for item in rows],
        "rejected": [item["rejected"] for item in rows],
    }


def _subgroups(portfolio: dict[str, Any]) -> dict[str, Any]:
    rows = portfolio.get("subgroups", [])[:CHART_SERIES_LIMIT]
    return {
        "labels": [item["label"] for item in rows],
        "values": [item["total"] for item in rows],
        "rejection_rate": [item["rejection_rate"] for item in rows],
    }


def _non_sap_by_laboratory(non_sap: dict[str, Any]) -> dict[str, Any]:
    """Pending, closed pass and closed fail partition the register exactly.

    ``overdue`` is a subset of ``pending``, so it travels as a tooltip figure
    rather than as a fourth segment.
    """
    rows = non_sap.get("non_sap_by_laboratory", [])
    return {
        "labels": [item["laboratory"]["name"] for item in rows],
        "pending": [item["pending"] for item in rows],
        "closed_pass": [item["closed_pass"] for item in rows],
        "closed_fail": [item["closed_fail"] for item in rows],
        "overdue": [item["overdue"] for item in rows],
    }


def _non_sap_by_status(non_sap: dict[str, Any]) -> dict[str, Any]:
    rows = non_sap.get("non_sap_by_status", [])
    return {
        "labels": [item["label"] for item in rows],
        "values": [item["count"] for item in rows],
        "tones": [
            {"closed_pass": "ok", "closed_fail": "bad"}.get(item["key"], "accent")
            for item in rows
        ],
    }


def management_chart_series(
    management: dict[str, Any],
    portfolio: dict[str, Any] | None,
    non_sap: dict[str, Any],
) -> dict[str, Any]:
    """Reshape the management page's own data into plottable series.

    ``portfolio`` is the whole recorded load and ``management`` is one day's
    snapshot.  They are kept in separate branches of the result for the same
    reason the page keeps them on separate tabs: their denominators are not the
    same, and a chart that blends them would say something untrue.
    """
    reviews = _reporting_reviews(management)
    portfolio = portfolio or {}
    return {
        "position": {
            "workload": _workload_by_laboratory(reviews),
            "movement": _movement(management),
            "usage_decisions": _usage_decisions(management),
        },
        "performance": {
            "has_data": bool(portfolio.get("has_data")),
            "laboratories": _laboratory_performance(portfolio),
            "materials_by_load": _materials_by_load(portfolio),
            "materials_by_failure": _materials_by_failure(portfolio),
            "subgroups": _subgroups(portfolio),
        },
        "non_sap": {
            "has_data": bool(non_sap.get("non_sap_kpis", {}).get("total")),
            "by_laboratory": _non_sap_by_laboratory(non_sap),
            "by_status": _non_sap_by_status(non_sap),
        },
    }
