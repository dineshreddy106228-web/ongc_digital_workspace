"""A recurring routine keeps one live task, rolled to its current occurrence.

A daily round is a standing obligation, not a queue of dated rows: it is done
every day and may or may not be written up each time. Generating one task per
occurrence meant a routine left ungenerated for months would either spawn a row
per missed day, or — as actually happened — sit on a single task from the day it
was created and age into a permanently overdue row.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pytest
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


@pytest.fixture()
def task_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'recur.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


def _template(recurrence_type="DAILY", start=date(2026, 3, 24), **kwargs):
    from app.models.tasks.recurring_task_template import RecurringTaskTemplate

    template = RecurringTaskTemplate(
        task_title="Netra sampling round",
        status="In Progress",
        priority="High",
        task_scope="MY",
        recurrence_type=recurrence_type,
        start_date=start,
        next_generation_date=start,
        is_active=True,
        **kwargs,
    )
    db.session.add(template)
    db.session.flush()
    return template


def _tasks_for(template):
    from app.models.tasks.task import Task

    return Task.query.filter_by(recurring_template_id=template.id).order_by(Task.id).all()


def test_a_routine_left_for_months_rolls_forward_instead_of_backfilling(task_app):
    """The bug this replaces: one March task, still open, five months overdue."""
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 3, 24))
    create_initial_task_for_template(template)
    db.session.commit()
    assert len(_tasks_for(template)) == 1

    result = generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    tasks = _tasks_for(template)
    assert len(tasks) == 1, "a daily routine must not spawn one row per missed day"
    assert tasks[0].occurrence_date == date(2026, 8, 24)
    assert tasks[0].due_date == date(2026, 8, 24)
    assert result["rolled"] == 1
    assert result["tasks"] == 0


def test_a_daily_routine_is_not_overdue_on_its_own_day(task_app):
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 3, 24))
    create_initial_task_for_template(template)
    db.session.commit()

    today = date(2026, 8, 24)
    generate_due_recurring_tasks(as_of_date=today)
    db.session.commit()

    task = _tasks_for(template)[0]
    assert task.due_date == today
    assert not (task.due_date < today), "today's round is due, not overdue"


def test_updates_written_against_the_routine_survive_the_roll(task_app):
    """Rolling must move the task, not replace it — its history rides along."""
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )
    from app.models.tasks.task_update import TaskUpdate

    template = _template(start=date(2026, 3, 24))
    task = create_initial_task_for_template(template)
    db.session.add(TaskUpdate(task_id=task.id, update_text="Sampled unit 3"))
    db.session.commit()
    original_id = task.id

    generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    rolled = _tasks_for(template)[0]
    assert rolled.id == original_id
    assert [u.update_text for u in TaskUpdate.query.filter_by(task_id=original_id)] == ["Sampled unit 3"]


def test_closing_a_period_opens_a_fresh_task_for_the_current_one(task_app):
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 8, 20))
    task = create_initial_task_for_template(template)
    task.status = "Completed"
    db.session.commit()

    result = generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    tasks = _tasks_for(template)
    assert len(tasks) == 2, "a closed period stays closed and a new one opens"
    assert result["tasks"] == 1
    assert tasks[0].status == "Completed"
    assert tasks[1].occurrence_date == date(2026, 8, 24)


def test_a_weekly_routine_rolls_to_its_own_weekday(task_app):
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    # Fridays. 24 Aug 2026 is a Monday, so the current occurrence is Fri 21 Aug.
    template = _template(recurrence_type="WEEKLY", start=date(2026, 7, 31), weekly_days="FRI")
    create_initial_task_for_template(template)
    db.session.commit()

    generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    task = _tasks_for(template)[0]
    assert task.occurrence_date == date(2026, 8, 21)
    assert task.due_date < date(2026, 8, 24), "a Friday round not done by Monday is late"


def test_generation_is_idempotent_within_a_day(task_app):
    """Both triggers may fire on the same day; the second must be a no-op."""
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 3, 24))
    create_initial_task_for_template(template)
    db.session.commit()

    generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()
    second = generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    assert len(_tasks_for(template)) == 1
    assert second["tasks"] == 0 and second["rolled"] == 0


def test_a_routine_past_its_end_date_stops(task_app):
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 8, 1), end_date=date(2026, 8, 10))
    create_initial_task_for_template(template)
    db.session.commit()

    generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    db.session.refresh(template)
    assert template.is_active is False
    assert template.next_generation_date is None


def test_a_routine_that_has_not_started_yet_creates_nothing(task_app):
    from app.core.services.recurring_tasks import generate_due_recurring_tasks

    template = _template(start=date(2026, 12, 1))
    db.session.commit()

    result = generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    assert _tasks_for(template) == []
    assert result["tasks"] == 0


def test_archiving_the_standing_task_stops_the_routine(task_app):
    """Lifting a routine off the tracker is a removal, not the end of a period.

    The generator must not quietly re-open it: an active template whose task was
    archived stays off the register until someone puts it back.
    """
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 3, 24))
    task = create_initial_task_for_template(template)
    task.is_active = False
    db.session.commit()

    result = generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    assert len(_tasks_for(template)) == 1, "no replacement is opened"
    assert result["tasks"] == 0
    assert result["rolled"] == 0
    # The archived task itself is untouched.
    assert _tasks_for(template)[0].occurrence_date == date(2026, 3, 24)


def test_a_completed_period_still_opens_the_next_one(task_app):
    """The contrast: completing is closing a period, not removing the routine."""
    from app.core.services.recurring_tasks import (
        create_initial_task_for_template, generate_due_recurring_tasks,
    )

    template = _template(start=date(2026, 3, 24))
    task = create_initial_task_for_template(template)
    task.status = "Completed"
    db.session.commit()

    result = generate_due_recurring_tasks(as_of_date=date(2026, 8, 24))
    db.session.commit()

    assert len(_tasks_for(template)) == 2
    assert result["tasks"] == 1
