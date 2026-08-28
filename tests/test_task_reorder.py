"""Reordering the task register moves what the reader can actually see.

Task order is one sequence shared by the whole workspace, but the register
never renders that sequence whole: it shows one office, split into Global and
Local tables. A reorder therefore has to be defined against the rendered list,
not against the global neighbours — otherwise a click moves a task past rows
nobody is looking at, and the table it was clicked in does not move at all.
"""
from __future__ import annotations

from datetime import datetime, timezone
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
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'reorder.db'}"
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


def _workspace():
    """One office, one superuser, and Global/Local tasks interleaved in order."""
    from app.core.roles import SUPERUSER_ROLE, USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.office.office import Office
    from app.models.tasks.task import Task

    super_role, user_role = Role(id=1, name=SUPERUSER_ROLE), Role(id=2, name=USER_ROLE)
    office = Office(id=1, office_code="HQ", office_name="HQ", is_active=True)
    superuser = User(
        id=1, username="super", password_hash="x", role=super_role, office_id=1,
        is_active=True, must_change_password=False,
    )
    reader = User(
        id=2, username="reader", password_hash="x", role=user_role, office_id=1,
        is_active=True, must_change_password=False,
    )
    db.session.add_all([super_role, user_role, office, superuser, reader])

    now = datetime.now(timezone.utc)
    plan = [("G1", "GLOBAL"), ("L1", "MY"), ("G2", "GLOBAL"), ("L2", "MY"), ("G3", "GLOBAL")]
    for index, (title, scope) in enumerate(plan, start=1):
        db.session.add(Task(
            id=index, task_title=title, task_scope=scope, office_id=1, status="Pending",
            is_active=True, created_by=1, created_at=now, display_order=index,
        ))
    db.session.commit()
    return superuser, reader


def _tables():
    from app.models.tasks.task import Task
    from app.modules.office.routes import _order_task_collection, _split_tasks_for_list

    ordered = _order_task_collection(Task.query.all())
    global_tasks, my_tasks = _split_tasks_for_list(ordered)
    return (
        [task.task_title for task in ordered],
        [task.task_title for task in global_tasks],
        [task.task_title for task in my_tasks],
    )


def _post(app, user, data, query=""):
    from flask_login import login_user, logout_user
    from app.modules.office.routes import reorder_tasks

    with app.test_request_context(f"/tasks/reorder{query}", method="POST", data=data):
        login_user(user)
        try:
            return reorder_tasks.__wrapped__.__wrapped__()
        finally:
            logout_user()


def _ids(*titles):
    from app.models.tasks.task import Task
    return ",".join(
        str(Task.query.filter_by(task_title=title).one().id) for title in titles
    )


def test_moving_a_task_up_moves_it_in_the_table_it_was_clicked_in(task_app):
    """The move the reader saw is the move that happens.

    L2 sits directly below L1 in the Local table, but its neighbour in the
    shared order is a Global task. Swapping order values with that neighbour
    left the Local table unchanged while quietly reordering the workspace.
    """
    superuser, _reader = _workspace()
    assert _tables() == (
        ["G1", "L1", "G2", "L2", "G3"], ["G1", "G2", "G3"], ["L1", "L2"],
    )

    from app.models.tasks.task import Task
    target = Task.query.filter_by(task_title="L2").one()
    _post(task_app, superuser, {
        "sequence": _ids("L1", "L2"), "task_id": str(target.id), "direction": "up",
    })
    db.session.commit()

    order, global_tasks, my_tasks = _tables()
    assert my_tasks == ["L2", "L1"], "the clicked table moved by exactly one row"
    assert global_tasks == ["G1", "G2", "G3"], "an untouched table never moves"
    # L1 and L2 exchanged the two slots they already held; nothing else shifted.
    assert order == ["G1", "L2", "G2", "L1", "G3"]


def test_a_dragged_sequence_is_applied_whole(task_app):
    superuser, _reader = _workspace()

    _post(task_app, superuser, {"sequence": _ids("G3", "G1", "G2")})
    db.session.commit()

    order, global_tasks, my_tasks = _tables()
    assert global_tasks == ["G3", "G1", "G2"]
    assert my_tasks == ["L1", "L2"], "the other table keeps its own order"
    # The Global tasks held slots 1, 3 and 5 and still do, rearranged.
    assert order == ["G3", "L1", "G1", "L2", "G2"]


def test_a_task_at_the_end_of_its_table_reports_rather_than_moving(task_app):
    from app.models.tasks.task import Task

    superuser, _reader = _workspace()
    top = Task.query.filter_by(task_title="L1").one()

    _post(task_app, superuser, {
        "sequence": _ids("L1", "L2"), "task_id": str(top.id), "direction": "up",
    })
    db.session.commit()

    assert _tables()[2] == ["L1", "L2"]


def test_a_stale_sequence_is_refused_rather_than_applied(task_app):
    """A page rendered before a task closed must not write an order from it."""
    from app.models.tasks.task import Task

    superuser, _reader = _workspace()
    stale = _ids("G1", "G2", "G3")
    Task.query.filter_by(task_title="G2").one().status = "Completed"
    db.session.commit()

    _post(task_app, superuser, {"sequence": stale})
    db.session.commit()

    assert _tables()[1] == ["G1", "G2", "G3"], "nothing was rewritten"


def test_reordering_is_refused_while_the_list_is_narrowed(task_app):
    """A filtered list is a partial view of a shared order, so it is read-only."""
    superuser, _reader = _workspace()

    _post(task_app, superuser, {"sequence": _ids("G3", "G1", "G2")}, query="?status=Pending")
    db.session.commit()

    assert _tables()[1] == ["G1", "G2", "G3"]


def test_only_a_superuser_may_change_the_shared_order(task_app):
    from werkzeug.exceptions import Forbidden

    _superuser, reader = _workspace()

    with pytest.raises(Forbidden):
        _post(task_app, reader, {"sequence": _ids("G3", "G1", "G2")})
    assert _tables()[1] == ["G1", "G2", "G3"]
