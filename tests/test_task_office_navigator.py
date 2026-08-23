"""The office navigator counts what the register's office filter selects.

Both sides use the same union rule — the tasks an office owns plus the open
GLOBAL tasks tagged to it — so a marker's number and the rows its link produces
can never describe different sets of tasks.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pytest
from flask import g

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


TODAY = date.today()


@pytest.fixture()
def task_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'tasks.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


def _office(office_id: int, code: str, name: str, location: str = ""):
    from app.models.office.office import Office

    office = Office(id=office_id, office_code=code, office_name=name, location=location)
    db.session.add(office)
    return office


def _task(task_id: int, title: str, office_id: int | None, **kwargs):
    """One task, open and undated unless the caller says otherwise."""
    from app.models.tasks.task import Task

    task = Task(
        id=task_id,
        task_title=title,
        office_id=office_id,
        status=kwargs.pop("status", "In Progress"),
        task_scope=kwargs.pop("task_scope", "MY"),
        is_active=kwargs.pop("is_active", True),
        **kwargs,
    )
    db.session.add(task)
    return task


def _tag(link_id: int, task_id: int, office_id: int):
    from app.models.tasks.task_office import TaskOffice

    link = TaskOffice(id=link_id, task_id=task_id, office_id=office_id)
    db.session.add(link)
    return link


def _super_user(user_id: int = 1, office_id: int | None = None):
    from app.core.roles import SUPERUSER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User

    role = Role(id=1, name=SUPERUSER_ROLE)
    user = User(
        id=user_id,
        username="superuser",
        full_name="Super User",
        password_hash="x",
        role=role,
        office_id=office_id,
        is_active=True,
        must_change_password=False,
    )
    db.session.add_all([role, user])
    return user


def _signed_in_client(task_app, user):
    client = task_app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    return client


def _visit(client, url):
    """Sign-in-aware GET: every visit starts from a clean login context.

    These tests hold one app context open for seeding, and Flask reuses it for
    test requests, so Flask-Login's cached user would otherwise leak from one
    signed-in client into the next.
    """
    g.pop("_login_user", None)
    return client.get(url)


def _user(user_id: int, username: str, office_id: int | None, role_id: int = 2,
          module_access: bool = False):
    from app.core.roles import USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.core.user_module_permission import UserModulePermission

    role = db.session.get(Role, role_id) or Role(id=role_id, name=USER_ROLE)
    user = User(id=user_id, username=username, full_name=username.title(), password_hash="x",
                role=role, office_id=office_id, is_active=True, must_change_password=False)
    db.session.add_all([role, user])
    if module_access:
        # The register and dashboard are behind the Task Management module grant.
        db.session.add(UserModulePermission(id=user_id, user_id=user_id, module_code="tasks",
                                            can_access=True))
    return user


def _navigator_for(task_app, user):
    """The navigator rows and global counts the dashboard would render for this user."""
    from flask import g
    from flask_login import login_user

    with task_app.test_request_context("/tasks/dashboard"):
        g.pop("_login_user", None)
        login_user(user)
        from app.modules.office.routes import _global_task_counts, _offices_with_open_tasks

        offices = (
            _offices_with_open_tasks()
            if user.is_super_user()
            else _offices_with_open_tasks(office_ids=[user.office_id] if user.office_id else [])
        )
        return {row["office_code"]: row for row in offices}, _global_task_counts()


def _navigator_rows():
    """The rows a super user's map would carry — the helper reads current_user."""
    from app.models.core.user import User

    viewer = User.query.filter_by(username="superuser").first()
    if viewer is None:
        viewer = _super_user(user_id=99)
        db.session.commit()
    rows, _globals = _navigator_for(_current_app(), viewer)
    return rows


def _current_app():
    from flask import current_app

    return current_app._get_current_object()


def test_only_offices_carrying_open_tasks_are_listed(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry", "Dehradun")
    _office(2, "QPCL", "Quality and Process Control Laboratory", "Hazira")
    _office(3, "ST_RJY", "RJY Surface Chemistry Team", "Rajahmundry")
    _task(1, "Draft the annual plan", 1)
    _task(2, "Review the SOP", 1, status="Not Started")
    _task(3, "Closed work", 2, status="Completed")
    _task(4, "Cancelled work", 2, status="Cancelled")
    _task(5, "Archived work", 3, is_active=False)
    db.session.commit()

    rows = _navigator_rows()

    assert list(rows) == ["CORP_CHEM"]
    assert rows["CORP_CHEM"]["open_count"] == 2
    assert rows["CORP_CHEM"]["overdue_count"] == 0
    assert rows["CORP_CHEM"]["office_name"] == "Office of Head Corporate Chemistry"
    assert rows["CORP_CHEM"]["location"] == "Dehradun"


def test_a_tagged_global_task_counts_for_every_office_it_reaches_but_once_each(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _office(2, "QPCL", "Quality and Process Control Laboratory")
    # Owned by CORP_CHEM, tagged to both — CORP_CHEM must not count it twice.
    _task(1, "Corporate audit", 1, task_scope="GLOBAL")
    _tag(1, 1, 1)
    _tag(2, 1, 2)
    db.session.commit()

    rows = _navigator_rows()

    assert rows["CORP_CHEM"]["open_count"] == 1
    assert rows["QPCL"]["open_count"] == 1


def test_overdue_counts_only_open_tasks_past_their_due_date(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _task(1, "Overdue report", 1, due_date=TODAY - timedelta(days=3))
    _task(2, "Due today", 1, due_date=TODAY)
    _task(3, "Due next week", 1, due_date=TODAY + timedelta(days=7))
    _task(4, "Undated", 1)
    _task(5, "Overdue but completed", 1, status="Completed", due_date=TODAY - timedelta(days=9))
    db.session.commit()

    rows = _navigator_rows()

    assert rows["CORP_CHEM"]["open_count"] == 4
    assert rows["CORP_CHEM"]["overdue_count"] == 1


def test_offices_are_ordered_by_open_count_then_name(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _office(2, "QPCL", "Quality and Process Control Laboratory")
    _office(3, "ST_RJY", "RJY Surface Chemistry Team")
    _task(1, "One", 1)
    _task(2, "Two", 1)
    _task(3, "Three", 2)
    _task(4, "Four", 3)
    db.session.commit()

    assert list(_navigator_rows()) == [
        "CORP_CHEM",  # 2 open
        "QPCL",  # 1 open each, so the office name breaks the tie
        "ST_RJY",
    ]


def test_the_register_office_filter_returns_exactly_what_the_navigator_counted(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _office(2, "QPCL", "Quality and Process Control Laboratory")
    _task(1, "Owned open task", 1)
    _task(2, "Owned closed task", 1, status="Completed")
    _task(3, "Tagged global task", 2, task_scope="GLOBAL")
    _tag(1, 3, 1)
    _task(4, "Another office task", 2)
    user = _super_user()
    db.session.commit()

    navigator = _navigator_rows()
    assert navigator["CORP_CHEM"]["open_count"] == 2

    client = _signed_in_client(task_app, user)
    page = _visit(client, "/tasks/?office=1")
    body = page.get_data(as_text=True)

    assert page.status_code == 200
    # Everything the navigator counted, plus this office's closed work, and
    # nothing belonging only to the other office.
    assert "Owned open task" in body
    assert "Tagged global task" in body
    assert "Owned closed task" in body
    assert "Another office task" not in body


def test_an_unusable_office_value_is_ignored_rather_than_erroring(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _task(1, "Owned open task", 1)
    user = _super_user()
    db.session.commit()

    client = _signed_in_client(task_app, user)

    for raw_value in ("", "abc", "-1", "9999", "1 OR 1=1"):
        page = _visit(client, f"/tasks/?office={raw_value}")
        assert page.status_code == 200
        assert "Owned open task" in page.get_data(as_text=True)


def test_everyone_gets_the_map_but_only_a_super_user_sees_every_location(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _office(2, "QPCL", "Quality and Process Control Laboratory")
    _task(1, "Corporate work", 1)
    _task(2, "Lab work", 2)
    super_user = _super_user()
    local_user = _user(20, "chemist", office_id=2)
    db.session.commit()

    super_rows, _ = _navigator_for(task_app, super_user)
    assert set(super_rows) == {"CORP_CHEM", "QPCL"}

    # A standard user's map carries their own office and nothing else.
    local_rows, _ = _navigator_for(task_app, local_user)
    assert set(local_rows) == {"QPCL"}
    assert local_rows["QPCL"]["open_count"] == 1


def test_a_standard_user_never_counts_a_task_the_visibility_model_hides(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    owner = _user(21, "owner", office_id=1)
    colleague = _user(22, "colleague", office_id=1)
    _task(1, "Shared office task", 1)
    # A private self-task belongs to its owner alone, office membership aside.
    _task(2, "Private self task", 1, owner_id=owner.id, is_private_self_task=True)
    super_user = _super_user()
    db.session.commit()

    super_rows, _ = _navigator_for(task_app, super_user)
    colleague_rows, _ = _navigator_for(task_app, colleague)
    owner_rows, _ = _navigator_for(task_app, owner)

    assert super_rows["CORP_CHEM"]["open_count"] == 2
    assert colleague_rows["CORP_CHEM"]["open_count"] == 1
    assert owner_rows["CORP_CHEM"]["open_count"] == 2


def test_global_tasks_are_counted_apart_from_the_map(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _task(1, "Local work", 1)
    _task(2, "Workspace rollout", 1, task_scope="GLOBAL", due_date=TODAY - timedelta(days=2))
    _task(3, "Workspace audit", 1, task_scope="GLOBAL")
    _task(4, "Closed global", 1, task_scope="GLOBAL", status="Completed")
    super_user = _super_user()
    local_user = _user(23, "chemist", office_id=1)
    db.session.commit()

    for user in (super_user, local_user):
        _rows, globals_ = _navigator_for(task_app, user)
        # GLOBAL tasks are visible to everybody, so both roles count the same two.
        assert globals_ == {"open_count": 2, "overdue_count": 1}


def test_a_user_with_no_office_gets_no_map_but_still_gets_global_counts(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _task(1, "Local work", 1)
    _task(2, "Workspace rollout", 1, task_scope="GLOBAL")
    unmapped = _user(24, "unmapped", office_id=None)
    db.session.commit()

    rows, globals_ = _navigator_for(task_app, unmapped)

    assert rows == {}
    assert globals_["open_count"] == 1


def test_the_dashboard_renders_the_navigator_for_a_standard_user(task_app):
    _office(1, "CORP_CHEM", "Office of Head Corporate Chemistry")
    _task(1, "Owned open task", 1)
    local_user = _user(25, "chemist", office_id=1, module_access=True)
    db.session.commit()

    body = _visit(_signed_in_client(task_app, local_user), "/tasks/dashboard").get_data(as_text=True)

    assert "Location navigator" in body
    assert "taskOfficeMap" in body
    assert "Global tasks" in body
