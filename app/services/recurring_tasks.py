"""Recurring task generation helpers and CLI-facing utilities."""

from __future__ import annotations

from calendar import monthrange
from datetime import date as date_type, datetime, timezone, timedelta

from app.extensions import db
from app.models.recurring_task_collaborator import RecurringTaskCollaborator
from app.models.recurring_task_template import (
    RECURRENCE_TYPES,
    RECURRENCE_WEEKDAYS,
    RecurringTaskTemplate,
)
from app.models.task import Task
from app.models.task_collaborator import TaskCollaborator


WEEKDAY_TO_INDEX = {code: index for index, (code, _) in enumerate(RECURRENCE_WEEKDAYS)}


def normalize_weekday_codes(raw_codes) -> list[str]:
    allowed_codes = set(WEEKDAY_TO_INDEX)
    seen_codes = set()
    normalized = []
    for raw_code in raw_codes or []:
        clean_code = (raw_code or "").strip().upper()
        if clean_code in allowed_codes and clean_code not in seen_codes:
            normalized.append(clean_code)
            seen_codes.add(clean_code)
    return normalized


def encode_weekday_codes(codes) -> str | None:
    normalized = normalize_weekday_codes(codes)
    return ",".join(normalized) if normalized else None


def decode_weekday_codes(raw_codes: str | None) -> list[str]:
    return normalize_weekday_codes((raw_codes or "").split(","))


def first_occurrence_date(
    recurrence_type: str,
    start_date: date_type,
    weekly_days=None,
    monthly_day: int | None = None,
) -> date_type:
    normalized_type = (recurrence_type or "").strip().upper()
    if normalized_type == "DAILY":
        return start_date
    if normalized_type == "WEEKLY":
        valid_days = normalize_weekday_codes(weekly_days)
        if not valid_days:
            raise ValueError("Weekly recurrence requires at least one weekday.")
        allowed_indices = {WEEKDAY_TO_INDEX[day] for day in valid_days}
        current_date = start_date
        for _ in range(7):
            if current_date.weekday() in allowed_indices:
                return current_date
            current_date += timedelta(days=1)
        raise ValueError("Unable to compute first weekly occurrence.")
    if normalized_type == "MONTHLY":
        if monthly_day is None or monthly_day < 1 or monthly_day > 28:
            raise ValueError("Monthly recurrence requires a day between 1 and 28.")
        if start_date.day <= monthly_day:
            return start_date.replace(day=monthly_day)
        year = start_date.year + (1 if start_date.month == 12 else 0)
        month = 1 if start_date.month == 12 else start_date.month + 1
        return date_type(year, month, monthly_day)
    raise ValueError(f"Unsupported recurrence type: {recurrence_type}")


def next_occurrence_date(
    occurrence_date: date_type,
    recurrence_type: str,
    weekly_days=None,
    monthly_day: int | None = None,
) -> date_type:
    normalized_type = (recurrence_type or "").strip().upper()
    if normalized_type == "DAILY":
        return occurrence_date + timedelta(days=1)
    if normalized_type == "WEEKLY":
        valid_days = normalize_weekday_codes(weekly_days)
        if not valid_days:
            raise ValueError("Weekly recurrence requires at least one weekday.")
        allowed_indices = {WEEKDAY_TO_INDEX[day] for day in valid_days}
        current_date = occurrence_date + timedelta(days=1)
        for _ in range(7):
            if current_date.weekday() in allowed_indices:
                return current_date
            current_date += timedelta(days=1)
        raise ValueError("Unable to compute next weekly occurrence.")
    if normalized_type == "MONTHLY":
        if monthly_day is None or monthly_day < 1 or monthly_day > 28:
            raise ValueError("Monthly recurrence requires a day between 1 and 28.")
        year = occurrence_date.year + (1 if occurrence_date.month == 12 else 0)
        month = 1 if occurrence_date.month == 12 else occurrence_date.month + 1
        day = min(monthly_day, monthrange(year, month)[1])
        return date_type(year, month, day)
    raise ValueError(f"Unsupported recurrence type: {recurrence_type}")


def recurrence_summary(template: RecurringTaskTemplate | None) -> str:
    if template is None:
        return "One-time"
    if template.recurrence_type == "DAILY":
        return "Daily"
    if template.recurrence_type == "WEEKLY":
        labels = {
            code: label for code, label in RECURRENCE_WEEKDAYS
        }
        weekdays = ", ".join(labels[code][:3] for code in decode_weekday_codes(template.weekly_days))
        return f"Weekly ({weekdays})" if weekdays else "Weekly"
    if template.recurrence_type == "MONTHLY":
        return f"Monthly (day {template.monthly_day})"
    return template.recurrence_type.title()


def _create_task_instance(template: RecurringTaskTemplate, occurrence_date: date_type) -> Task:
    task = Task(
        task_title=template.task_title,
        task_description=template.task_description,
        task_origin=template.task_origin,
        status=template.status,
        priority=template.priority,
        due_date=occurrence_date,
        owner_id=template.owner_id,
        created_by=template.created_by,
        office_id=template.office_id,
        is_active=True,
        task_scope=template.task_scope,
        recurring_template_id=template.id,
        occurrence_date=occurrence_date,
    )
    db.session.add(task)
    db.session.flush()

    for collaborator_link in template.collaborator_links:
        if collaborator_link.user_id:
            db.session.add(
                TaskCollaborator(task_id=task.id, user_id=collaborator_link.user_id)
            )

    db.session.flush()
    return task


def generate_due_recurring_tasks(as_of_date: date_type | None = None) -> dict[str, int]:
    generation_date = as_of_date or date_type.today()
    templates = (
        RecurringTaskTemplate.query
        .filter(
            RecurringTaskTemplate.is_active.is_(True),
            RecurringTaskTemplate.next_generation_date.isnot(None),
            RecurringTaskTemplate.next_generation_date <= generation_date,
        )
        .order_by(
            RecurringTaskTemplate.next_generation_date.asc(),
            RecurringTaskTemplate.id.asc(),
        )
        .all()
    )

    created_tasks = 0
    touched_templates = 0

    for template in templates:
        occurrence_date = template.next_generation_date
        template_created = False

        while occurrence_date and occurrence_date <= generation_date:
            if template.end_date and occurrence_date > template.end_date:
                occurrence_date = None
                template.is_active = False
                break

            existing = Task.query.filter_by(
                recurring_template_id=template.id,
                occurrence_date=occurrence_date,
            ).first()
            if existing is None:
                _create_task_instance(template, occurrence_date)
                created_tasks += 1
                template_created = True

            occurrence_date = next_occurrence_date(
                occurrence_date,
                template.recurrence_type,
                weekly_days=decode_weekday_codes(template.weekly_days),
                monthly_day=template.monthly_day,
            )

        if occurrence_date and template.end_date and occurrence_date > template.end_date:
            occurrence_date = None
            template.is_active = False

        template.next_generation_date = occurrence_date
        if template_created:
            touched_templates += 1
            template.last_generated_at = datetime.now(timezone.utc)

    return {"templates": touched_templates, "tasks": created_tasks}
