from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")


def format_datetime_ist(value: datetime | None, fmt: str = "%d %b %Y, %H:%M") -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(IST).strftime(fmt)
