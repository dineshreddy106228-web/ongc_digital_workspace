"""Recurring task generation helpers and CLI-facing utilities."""

from __future__ import annotations

from calendar import monthrange
from datetime import date as date_type, datetime, timezone, timedelta

from app.extensions import db
from app.models.tasks.recurring_task_collaborator import RecurringTaskCollaborator
from app.models.tasks.recurring_task_template import (
    RECURRENCE_TYPES,
    RECURRENCE_WEEKDAYS,
    RecurringTaskTemplate,
)
from app.models.tasks.task import Task
from app.models.tasks.task_collaborator import TaskCollaborator

# A routine's standing task is the one still running; a closed period is left
# alone and a fresh task opens for the current occurrence.
CLOSED_RECURRING_STATUSES = ("Completed", "Cancelled")


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
        is_private_self_task=template.is_private_self_task,
        self_task_visible_to_controlling_officer=template.self_task_visible_to_controlling_officer,
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


def create_initial_task_for_template(template: RecurringTaskTemplate) -> Task | None:
    occurrence_date = template.next_generation_date
    if occurrence_date is None:
        return None

    task = _create_task_instance(template, occurrence_date)
    template.last_generated_at = datetime.now(timezone.utc)

    next_date = next_occurrence_date(
        occurrence_date,
        template.recurrence_type,
        weekly_days=decode_weekday_codes(template.weekly_days),
        monthly_day=template.monthly_day,
    )
    if template.end_date and next_date > template.end_date:
        template.next_generation_date = None
        template.is_active = False
    else:
        template.next_generation_date = next_date

    return task


def occurrence_dates_in_window(
    template: RecurringTaskTemplate,
    window_start: date_type,
    window_end: date_type,
    seed_date: date_type | None = None,
) -> list[date_type]:
    if window_end <= window_start:
        return []

    occurrence_date = seed_date or template.next_generation_date
    if occurrence_date is None:
        return []

    dates = []
    while occurrence_date and occurrence_date < window_end:
        if template.end_date and occurrence_date > template.end_date:
            break
        if occurrence_date >= window_start:
            dates.append(occurrence_date)
        occurrence_date = next_occurrence_date(
            occurrence_date,
            template.recurrence_type,
            weekly_days=decode_weekday_codes(template.weekly_days),
            monthly_day=template.monthly_day,
        )

    return dates


def next_scheduled_occurrence_for_template(
    template: RecurringTaskTemplate,
    after_date: date_type | None = None,
) -> date_type | None:
    occurrence_date = first_occurrence_date(
        template.recurrence_type,
        template.start_date,
        weekly_days=decode_weekday_codes(template.weekly_days),
        monthly_day=template.monthly_day,
    )
    if after_date is not None:
        while occurrence_date and occurrence_date <= after_date:
            occurrence_date = next_occurrence_date(
                occurrence_date,
                template.recurrence_type,
                weekly_days=decode_weekday_codes(template.weekly_days),
                monthly_day=template.monthly_day,
            )

    if template.end_date and occurrence_date and occurrence_date > template.end_date:
        return None
    return occurrence_date


def current_occurrence_on_or_before(
    template: RecurringTaskTemplate, as_of: date_type
) -> date_type | None:
    """The latest occurrence of *template* that is due on or before *as_of*.

    Walks forward from the first occurrence rather than guessing, so weekly and
    monthly rules are honoured exactly as they are elsewhere. Returns None when
    the routine has not started yet.
    """
    weekly_days = decode_weekday_codes(template.weekly_days)
    try:
        occurrence = first_occurrence_date(
            template.recurrence_type, template.start_date,
            weekly_days=weekly_days, monthly_day=template.monthly_day,
        )
    except ValueError:
        return None

    if occurrence > as_of:
        return None

    limit = min(as_of, template.end_date) if template.end_date else as_of
    if occurrence > limit:
        return None

    latest = occurrence
    while True:
        nxt = next_occurrence_date(
            latest, template.recurrence_type,
            weekly_days=weekly_days, monthly_day=template.monthly_day,
        )
        if nxt > limit:
            return latest
        latest = nxt


def generate_due_recurring_tasks(as_of_date: date_type | None = None) -> dict[str, int]:
    """Keep one live task per active routine, rolled to its current occurrence.

    A recurring routine is a standing obligation, not a queue of dated rows: a
    daily round is done every day and may or may not be written up each time.
    So each template carries a single open task whose due date rolls forward to
    the occurrence that is current today, and its updates accumulate on it.

    That means a routine is only ever late within its current period, instead of
    ageing into a permanently overdue row — and a template left ungenerated for
    months catches up in one step rather than spawning one task per missed day.

    A task that has been completed or cancelled is left alone and a fresh one is
    opened for the current occurrence, so closing off a period still works.
    A task that has been archived stops the routine instead: lifting it off the
    tracker is a removal, not the end of a period.
    """
    generation_date = as_of_date or date_type.today()
    templates = (
        RecurringTaskTemplate.query
        .filter(RecurringTaskTemplate.is_active.is_(True))
        .order_by(RecurringTaskTemplate.id.asc())
        .all()
    )

    created_tasks = 0
    rolled_tasks = 0
    touched_templates = 0

    for template in templates:
        # A routine past its end date stops; its open task stays as it is.
        if template.end_date and generation_date > template.end_date:
            template.is_active = False
            template.next_generation_date = None
            touched_templates += 1
            continue

        occurrence = current_occurrence_on_or_before(template, generation_date)
        if occurrence is None:
            continue

        # The routine's standing task is the most recent one it produced.
        latest_task = (
            Task.query.filter(Task.recurring_template_id == template.id)
            .order_by(Task.id.desc())
            .first()
        )

        # Archiving that task takes the routine off the tracker: it is a
        # deliberate removal, so nothing is opened in its place. The routine
        # restarts when the template is edited or a task is raised again.
        if latest_task is not None and not latest_task.is_active:
            continue

        open_task = (
            latest_task
            if latest_task is not None and latest_task.status not in CLOSED_RECURRING_STATUSES
            else None
        )

        changed = False
        if open_task is None:
            # The routine may already have produced — and closed — the task for
            # this occurrence. A monthly routine completed on the 1st is in
            # exactly that state for the rest of the month. Opening a second task
            # for the same occurrence collides with the (template, occurrence)
            # uniqueness, and that collision aborted the whole roll-forward on
            # every request, so nothing after it was rolled either. The period is
            # accounted for: the routine waits for the next occurrence.
            already_generated = db.session.query(
                Task.query.filter(
                    Task.recurring_template_id == template.id,
                    Task.occurrence_date == occurrence,
                ).exists()
            ).scalar()
            if not already_generated:
                _create_task_instance(template, occurrence)
                created_tasks += 1
                changed = True
        elif open_task.occurrence_date is None or open_task.occurrence_date < occurrence:
            # Roll the standing task forward; its updates and status ride along.
            open_task.occurrence_date = occurrence
            open_task.due_date = occurrence
            rolled_tasks += 1
            changed = True

        # next_generation_date now reads as "the occurrence after the current
        # one", which is what the register and the series views describe.
        try:
            template.next_generation_date = next_occurrence_date(
                occurrence, template.recurrence_type,
                weekly_days=decode_weekday_codes(template.weekly_days),
                monthly_day=template.monthly_day,
            )
        except ValueError:
            template.next_generation_date = None

        if changed:
            touched_templates += 1
            template.last_generated_at = datetime.now(timezone.utc)

    return {
        "templates": touched_templates,
        "tasks": created_tasks,
        "rolled": rolled_tasks,
    }
