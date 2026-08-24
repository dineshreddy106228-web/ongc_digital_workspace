"""A post outlives its holders, and a handover never rewrites what was written.

Reusing one user account for a designation and renaming it on handover
retroactively re-attributes every task update, approval and audit entry that
person authored, because those records resolve the display name live. These
tests cover the alternative: the post is its own record with a succession of
holders.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
def admin_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'posts.db'}"
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


def _seed():
    from app.core.roles import ADMIN_ROLE, USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.office.office import Office

    admin_role = Role(id=1, name=ADMIN_ROLE)
    user_role = Role(id=2, name=USER_ROLE)
    office = Office(id=1, office_code="ST_RJY", office_name="RJY Surface Chemistry Team")
    admin = User(id=1, username="admin", password_hash="x", role=admin_role,
                 office_id=1, is_active=True, must_change_password=False)
    sapna = User(id=10, username="sapna.sethi", full_name="Sapna Sethi", password_hash="x",
                 role=user_role, office_id=1, is_active=True, must_change_password=False)
    successor = User(id=11, username="r.iyer", full_name="R Iyer", password_hash="x",
                     role=user_role, office_id=1, is_active=True, must_change_password=False)
    db.session.add_all([admin_role, user_role, office, admin, sapna, successor])
    db.session.commit()
    return admin, office, sapna, successor


def _client_as(app, user):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    return client


def _post(client, url, data):
    g.pop("_login_user", None)
    return client.post(url, data=data, follow_redirects=False)


def _form(office_id, holder_id=""):
    return {
        "post_code": "HEAD_RGL_RJY",
        "post_title": "Head, RGL Rajahmundry",
        "description": "Laboratory head",
        "office_id": str(office_id),
        "holder_user_id": str(holder_id),
    }


def test_creating_a_post_opens_a_tenure_for_its_first_holder(admin_app):
    admin, office, sapna, _ = _seed()

    response = _post(_client_as(admin_app, admin), "/admin/posts/add", _form(office.id, sapna.id))

    assert response.status_code == 302
    from app.models.office.office_post import OfficePost

    post = OfficePost.query.one()
    assert post.post_code == "HEAD_RGL_RJY"
    assert post.holder_user_id == sapna.id
    assert post.holder_label == "Sapna Sethi"
    assert len(post.assignments) == 1
    assert post.assignments[0].is_current is True
    assert post.assignments[0].ended_at is None


def test_a_handover_closes_the_old_tenure_and_keeps_it_on_record(admin_app):
    admin, office, sapna, successor = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost

    post_id = OfficePost.query.one().id
    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, successor.id))

    post = db.session.get(OfficePost, post_id)
    assert post.holder_user_id == successor.id
    assert len(post.assignments) == 2, "the outgoing tenure must be kept, not replaced"

    current = post.current_assignment
    assert current.user_id == successor.id
    closed = [a for a in post.assignments if not a.is_current]
    assert len(closed) == 1
    assert closed[0].user_id == sapna.id
    assert closed[0].ended_at is not None
    # The point of the whole exercise: the earlier holder is still named.
    assert closed[0].holder_name == "Sapna Sethi"


def test_the_succession_answers_who_held_the_post_on_a_past_date(admin_app):
    admin, office, sapna, successor = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost

    post_id = OfficePost.query.one().id
    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, successor.id))

    post = db.session.get(OfficePost, post_id)
    outgoing = [a for a in post.assignments if not a.is_current][0]
    current = post.current_assignment

    # A moment inside the first tenure resolves to its holder, and one after the
    # handover to the next — which is what makes a past record readable.
    during_first = OfficePost._as_utc(outgoing.started_at)
    after_handover = OfficePost._as_utc(current.started_at) + timedelta(days=30)
    assert post.holder_on(during_first).id == sapna.id
    assert post.holder_on(after_handover).id == successor.id
    # Before the post existed nobody held it.
    assert post.holder_on(during_first - timedelta(days=30)) is None


def test_renaming_the_previous_holder_does_not_rewrite_the_succession(admin_app):
    """The failure mode this feature exists to prevent."""
    admin, office, sapna, successor = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.core.user import User
    from app.models.office.office_post import OfficePost

    post_id = OfficePost.query.one().id
    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, successor.id))

    # Someone renames the old account anyway.
    db.session.get(User, sapna.id).full_name = "Someone Else"
    db.session.commit()

    post = db.session.get(OfficePost, post_id)
    closed = [a for a in post.assignments if not a.is_current][0]
    assert closed.holder_name == "Sapna Sethi", "the snapshot must survive a rename"


def test_a_post_can_be_left_vacant_between_holders(admin_app):
    admin, office, sapna, _ = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost

    post_id = OfficePost.query.one().id
    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, ""))

    post = db.session.get(OfficePost, post_id)
    assert post.holder_user_id is None
    assert post.holder_label == "Vacant"
    assert post.current_assignment is None
    assert post.assignments[0].ended_at is not None


def test_retiring_a_post_closes_its_tenure_without_losing_history(admin_app):
    admin, office, sapna, _ = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost

    post_id = OfficePost.query.one().id
    _post(client, f"/admin/posts/{post_id}/toggle-active", {})

    post = db.session.get(OfficePost, post_id)
    assert post.is_active is False
    assert post.holder_user_id is None
    assert len(post.assignments) == 1, "the tenure is closed, not deleted"
    assert post.assignments[0].ended_at is not None


def test_a_duplicate_post_code_is_rejected(admin_app):
    admin, office, sapna, _ = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost

    assert OfficePost.query.count() == 1


def _task(task_id: int, title: str, post_id: int, owner_id: int, status: str = "In Progress",
          is_active: bool = True):
    from app.models.tasks.task import Task

    task = Task(id=task_id, task_title=title, status=status, priority="High",
                owner_id=owner_id, assigned_post_id=post_id, office_id=1,
                is_active=is_active, task_scope="MY")
    db.session.add(task)
    return task


def test_a_handover_moves_open_post_tasks_to_the_new_holder(admin_app):
    """The point of assigning to a post: open work follows the post."""
    admin, office, sapna, successor = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost
    from app.models.tasks.task import Task

    post_id = OfficePost.query.one().id
    _task(100, "Weekly seal audit", post_id, sapna.id)
    _task(101, "Closed last quarter", post_id, sapna.id, status="Completed")
    _task(102, "Lifted off the tracker", post_id, sapna.id, is_active=False)
    db.session.commit()

    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, successor.id))

    # Open work moves.
    assert db.session.get(Task, 100).owner_id == successor.id
    # Finished work keeps the owner it had when it finished — reassigning it
    # would misstate who did it.
    assert db.session.get(Task, 101).owner_id == sapna.id
    assert db.session.get(Task, 102).owner_id == sapna.id


def test_a_task_assigned_to_a_person_is_untouched_by_a_handover(admin_app):
    admin, office, sapna, successor = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost
    from app.models.tasks.task import Task

    post_id = OfficePost.query.one().id
    _task(200, "Personal follow-up", None, sapna.id)
    db.session.commit()

    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, successor.id))

    assert db.session.get(Task, 200).owner_id == sapna.id


def test_vacating_a_post_leaves_its_open_tasks_unassigned(admin_app):
    admin, office, sapna, _ = _seed()
    client = _client_as(admin_app, admin)
    _post(client, "/admin/posts/add", _form(office.id, sapna.id))

    from app.models.office.office_post import OfficePost
    from app.models.tasks.task import Task

    post_id = OfficePost.query.one().id
    _task(300, "Awaiting the next holder", post_id, sapna.id)
    db.session.commit()

    _post(client, f"/admin/posts/{post_id}/edit", _form(office.id, ""))

    task = db.session.get(Task, 300)
    assert task.owner_id is None
    # Still attached to the post, so the next holder inherits it.
    assert task.assigned_post_id == post_id
    assert task.assignment_label.endswith("Vacant")
