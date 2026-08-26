from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

from app.core.services.quality_control import calculate_management_metrics


AS_OF = date(2026, 8, 14)
PERIOD_START = date(2026, 8, 10)
PERIOD_END = date(2026, 8, 16)


def _sample(
    chemical_name: str,
    *,
    status: str = "under_testing",
    received: date | None = None,
    reported: date | None = None,
):
    return SimpleNamespace(
        chemical_name=chemical_name,
        result_status=status,
        sample_receipt_date=received,
        report_issue_date=reported,
    )


def _metrics(samples, standards):
    return calculate_management_metrics(
        samples,
        standards,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        as_of=AS_OF,
    )


def _distribution(metrics, section):
    return {item["key"]: item["count"] for item in metrics[section]["distribution"]}


def test_pending_management_status_boundaries_and_delay_metrics():
    standards = {name.replace(" ", "").lower(): 4 for name in ("below", "at standard", "one beyond", "three beyond", "critical")}
    samples = [
        _sample("below", received=AS_OF - timedelta(days=2)),
        _sample("at standard", received=AS_OF - timedelta(days=4)),
        _sample("one beyond", received=AS_OF - timedelta(days=5)),
        _sample("three beyond", received=AS_OF - timedelta(days=7)),
        _sample("critical", received=AS_OF - timedelta(days=8)),
    ]

    metrics = _metrics(samples, standards)

    assert _distribution(metrics, "pending") == {
        "within_standard": 1,
        "approaching_standard": 1,
        "delayed": 2,
        "critical": 1,
    }
    assert metrics["pending"]["average_delay"] == 1.6
    assert metrics["pending"]["oldest_delay"] == 4
    assert metrics["queue_health"] == {
        "key": "critical",
        "label": "Critical",
        "overdue_count": 3,
        "overdue_percentage": 60.0,
    }


def test_completed_management_status_boundaries_use_receipt_and_report_dates():
    standards = {name.replace(" ", "").lower(): 4 for name in ("within", "one beyond", "three beyond", "critical")}
    samples = [
        _sample("within", status="pass", received=date(2026, 8, 10), reported=date(2026, 8, 14)),
        _sample("one beyond", status="fail", received=date(2026, 8, 9), reported=date(2026, 8, 14)),
        _sample("three beyond", status="report_issued", received=date(2026, 8, 7), reported=date(2026, 8, 14)),
        _sample("critical", status="pass", received=date(2026, 8, 6), reported=date(2026, 8, 14)),
    ]

    metrics = _metrics(samples, standards)

    assert _distribution(metrics, "completed") == {
        "within_standard": 1,
        "delayed": 2,
        "critical": 1,
    }
    assert metrics["completed"]["standard_compliance"] == 25.0
    assert metrics["completed"]["average_delay"] == 2.0


def test_zero_pending_samples_are_healthy_with_zero_overdue():
    metrics = _metrics([], {})

    assert metrics["pending"]["total"] == 0
    assert metrics["queue_health"] == {
        "key": "healthy",
        "label": "Healthy",
        "overdue_count": 0,
        "overdue_percentage": 0.0,
    }


def test_missing_standard_testing_time_is_neutral_and_exposed():
    metrics = _metrics([_sample("unregistered", received=AS_OF - timedelta(days=12))], {})

    assert metrics["pending"]["unresolved"] == 1
    assert metrics["pending"]["missing_standard"] == 1
    assert metrics["pending"]["average_delay"] is None
    assert metrics["queue_health"]["key"] == "insufficient_data"
    assert metrics["queue_health"]["overdue_percentage"] is None


def test_clearance_ratio_handles_zero_received_and_backlog_clearance_above_100_percent():
    standards = {"completed": 2}
    completed = [
        _sample("completed", status="pass", received=date(2026, 8, 8), reported=date(2026, 8, 12)),
        _sample("completed", status="fail", received=date(2026, 8, 7), reported=date(2026, 8, 13)),
    ]
    no_received_metrics = _metrics(completed, standards)
    assert no_received_metrics["clearance"] == {"received": 0, "completed": 2, "ratio": None}

    received_and_completed = completed + [
        _sample("completed", received=date(2026, 8, 11)),
    ]
    metrics = _metrics(received_and_completed, standards)
    assert metrics["clearance"] == {"received": 1, "completed": 2, "ratio": 200.0}
