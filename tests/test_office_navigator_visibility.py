"""The office navigator reports across the whole workspace, on purpose.

Corporate Chemistry shows how the office works to everyone in it: a reader sees
that another location is carrying late work without being able to open that
work. This is a deliberate exception to the visibility model that governs the
rest of the module, so it is pinned down here — restricting these counts to a
reader's own visible tasks would make every other location report zero, which
is worse than showing nothing at all.

Two limits ride with the exception: a task marked private is never counted, and
a location the reader cannot open is shown but not offered.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pytest
from flask import g
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


@pytest.fixture()
def task_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'nav.db'}"
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


def _seed():
    """One reader in Corporate Chemistry; late work sitting in another office."""
    from app.core.roles import SUPERUSER_ROLE, USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.core.user_module_permission import UserModulePermission
    from app.models.office.office import Office
    from app.models.tasks.task import Task

    su_role = Role(id=1, name=SUPERUSER_ROLE)
    user_role = Role(id=2, name=USER_ROLE)
    home = Office(id=1, office_code="CORP_CHEM", office_name="Corporate Chemistry", is_active=True)
    other = Office(id=2, office_code="ST_RJY", office_name="RJY Surface Chemistry Team", is_active=True)
    superuser = User(id=1, username="su", password_hash="x", role=su_role, office_id=1,
                     is_active=True, must_change_password=False)
    reader = User(id=2, username="reader", full_name="Ravi Reader", password_hash="x",
                  role=user_role, office_id=1, is_active=True, must_change_password=False)
    db.session.add_all([su_role, user_role, home, other, superuser, reader])
    db.session.flush()
    db.session.add(UserModulePermission(user_id=reader.id, module_code="tasks", can_access=True))

    yesterday = date.today() - timedelta(days=1)
    db.session.add_all([
        # Late work in the office the reader is not part of.
        Task(id=10, task_title="RJY late work", status="In Progress", priority="High",
             office_id=2, due_date=yesterday, is_active=True, task_scope="MY"),
        # Someone else's private task, in that same office.
        Task(id=11, task_title="RJY private note", status="In Progress", priority="Low",
             office_id=2, due_date=yesterday, is_active=True, task_scope="MY",
             owner_id=1, is_private_self_task=True),
        # The reader's own office, on time.
        Task(id=12, task_title="Home work", status="In Progress", priority="Medium",
             office_id=1, due_date=date.today() + timedelta(days=7), is_active=True, task_scope="MY"),
    ])
    db.session.commit()
    return superuser, reader


def _navigator_for(app, user):
    from app.modules.office.routes import _offices_with_open_tasks

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    g.pop("_login_user", None)
    # A request context so current_user resolves the way the view sees it.
    with app.test_request_context():
        from flask_login import login_user

        login_user(user)
        return {row["office_code"]: row for row in _offices_with_open_tasks()}


def test_a_reader_sees_every_location_not_only_their_own(task_app):
    _superuser, reader = _seed()

    rows = _navigator_for(task_app, reader)

    assert set(rows) == {"CORP_CHEM", "ST_RJY"}


def test_a_reader_sees_late_work_in_an_office_they_cannot_open(task_app):
    """The exception itself: counts are workspace-wide, not reader-visible."""
    _superuser, reader = _seed()

    rows = _navigator_for(task_app, reader)

    assert rows["ST_RJY"]["overdue_count"] == 1, (
        "restricting these counts to the reader's own tasks would report zero here"
    )


def test_a_private_task_is_never_counted(task_app):
    """A public number must not reveal that a private task exists."""
    _superuser, reader = _seed()

    rows = _navigator_for(task_app, reader)

    # RJY holds one ordinary late task and one private one; only the first counts.
    assert rows["ST_RJY"]["open_count"] == 1
    assert rows["ST_RJY"]["overdue_count"] == 1


def test_the_counts_match_what_a_superuser_sees(task_app):
    """Everyone reads the same workspace picture, whatever they may open."""
    superuser, reader = _seed()

    as_reader = _navigator_for(task_app, reader)
    as_superuser = _navigator_for(task_app, superuser)

    for code in as_superuser:
        assert as_reader[code]["open_count"] == as_superuser[code]["open_count"]
        assert as_reader[code]["overdue_count"] == as_superuser[code]["overdue_count"]


def _dashboard(app, user):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    g.pop("_login_user", None)
    return client.get("/tasks/dashboard").data.decode()


def test_a_reader_is_not_offered_a_register_they_cannot_open(task_app):
    """A shown location must not look like a link to somewhere it will not go."""
    _superuser, reader = _seed()

    body = _dashboard(task_app, reader)

    # Their own office is a link; the other is present but locked.
    assert 'href="/tasks/?office=1"' in body
    assert 'href="/tasks/?office=2"' not in body
    assert "RJY Surface Chemistry Team" in body, "the location is still shown"
    assert "is-locked" in body


def test_a_superuser_may_open_every_location(task_app):
    superuser, _reader = _seed()

    body = _dashboard(task_app, superuser)

    assert 'href="/tasks/?office=1"' in body
    assert 'href="/tasks/?office=2"' in body
    assert "is-locked" not in body


def test_the_navigator_wording_is_the_same_for_everyone(task_app):
    superuser, reader = _seed()
    sentence = "Task status across locations"

    assert sentence in _dashboard(task_app, reader)
    assert sentence in _dashboard(task_app, superuser)
