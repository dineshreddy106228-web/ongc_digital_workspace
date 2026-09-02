"""The add-update page carries the trail it is adding to.

Writing an update used to mean recalling from memory, or leaving the form to
go and read the task page, what was last said. The earlier updates are on the
form now, newest first, so a new note continues the record instead of
repeating it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'tasks.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        # SQLite only autoincrements INTEGER primary keys.
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


def _seed(with_updates=True):
    from app.core.roles import SUPERUSER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.tasks.task import Task
    from app.models.tasks.task_update import TaskUpdate

    role = Role(id=1, name=SUPERUSER_ROLE)
    user = User(id=1, username="hcc", full_name="A Admin", password_hash="x",
                role=role, is_active=True, must_change_password=False)
    task = Task(id=1, task_title="Digitization of Quality Control Testing",
                status="In Progress", priority="High", owner_id=1, created_by=1)
    db.session.add_all([role, user, task])
    if with_updates:
        now = datetime.now(timezone.utc)
        db.session.add_all([
            TaskUpdate(id=1, task_id=1, update_text="Draft circulated to all offices.",
                       updated_by=1, created_at=now - timedelta(hours=30)),
            TaskUpdate(id=2, task_id=1, update_text="Inputs consolidated; moved to review.",
                       old_status="Not Started", new_status="In Progress",
                       updated_by=1, created_at=now - timedelta(hours=2)),
        ])
    db.session.commit()
    return user


def _client_as(app, user):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    return client


def test_add_update_page_shows_the_earlier_updates(task_app):
    user = _seed()

    response = _client_as(task_app, user).get("/tasks/1/add-update")

    assert response.status_code == 200
    page = response.get_data(as_text=True)
    assert "Earlier updates" in page
    # The newest note leads and the rest sit in a drawer, the same way the
    # task page reads it.
    assert "Inputs consolidated; moved to review." in page
    assert "Draft circulated to all offices." in page
    assert "Older updates <b>(1)</b>" in page
    # A recorded status change is shown as the transition it was.
    assert "om-pill-status-in-progress" in page
    # And the form the page exists for is untouched.
    assert 'name="update_text"' in page


def test_add_update_page_says_so_when_there_is_no_trail_yet(task_app):
    user = _seed(with_updates=False)

    page = _client_as(task_app, user).get("/tasks/1/add-update").get_data(as_text=True)

    assert "No updates yet" in page
    assert 'name="update_text"' in page


def test_a_rejected_update_redisplays_the_trail_with_the_typed_text(task_app):
    """A validation failure must not cost the author their draft or the trail."""
    user = _seed()

    response = _client_as(task_app, user).post(
        "/tasks/1/add-update", data={"update_text": "", "new_status": "Completed"},
    )

    assert response.status_code == 200
    page = response.get_data(as_text=True)
    assert "Update text is required." in page
    assert "Draft circulated to all offices." in page
