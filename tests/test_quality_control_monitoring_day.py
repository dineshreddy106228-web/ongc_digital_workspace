"""Working-day rules for the SAP-first QC monitoring cycle."""

from datetime import date, timedelta

from app.core.services.quality_control import current_monitoring_day


def test_monitoring_day_uses_the_current_working_day():
    monday = date(2026, 8, 31)

    monitoring_day = current_monitoring_day(monday)

    assert monitoring_day["date"] == monday
    assert monitoring_day["is_carried_forward"] is False
    assert monitoring_day["label"] == "Monday, 31 Aug 2026"


def test_monitoring_day_carries_friday_forward_on_a_weekend():
    monday = date(2026, 8, 31)
    saturday = monday - timedelta(days=2)
    sunday = monday - timedelta(days=1)

    saturday_monitoring_day = current_monitoring_day(saturday)
    sunday_monitoring_day = current_monitoring_day(sunday)

    assert saturday_monitoring_day["date"] == monday - timedelta(days=3)
    assert sunday_monitoring_day["date"] == monday - timedelta(days=3)
    assert saturday_monitoring_day["is_carried_forward"] is True
    assert sunday_monitoring_day["is_carried_forward"] is True
