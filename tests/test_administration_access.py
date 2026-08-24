"""Administration is readable by everyone with module access, editable by superusers.

Hiding these pages meant the people working under a setting could not see what
it was. They are now visible to anyone with access to the module, while the
ability to change them — and the record of who changed what — stays with
superusers.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest
from flask import g
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config

ADMIN_PAGES = ("/inventory/administration", "/quality-control/testing-standards")


@pytest.fixture()
def admin_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'admin_access.db'}"
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
    from app.core.roles import SUPERUSER_ROLE, USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.office.office import Office

    su_role = Role(id=1, name=SUPERUSER_ROLE)
    user_role = Role(id=2, name=USER_ROLE)
    office = Office(id=1, office_code="CORP_CHEM", office_name="Corporate Chemistry")
    superuser = User(id=1, username="su", full_name="Super User", password_hash="x",
                     role=su_role, office_id=1, is_active=True, must_change_password=False)
    regular = User(id=2, username="reader", full_name="Ravi Reader", password_hash="x",
                   role=user_role, office_id=1, is_active=True, must_change_password=False)
    db.session.add_all([su_role, user_role, office, superuser, regular])
    db.session.flush()

    # A plain user reaches a module only through an explicit permission row;
    # "regular user" here means one who already has access to the module.
    from app.models.core.user_module_permission import UserModulePermission

    for module_code in ("inventory", "quality_control"):
        db.session.add(UserModulePermission(
            user_id=regular.id, module_code=module_code, can_access=True,
        ))
    db.session.commit()
    return superuser, regular


def _client_as(app, user):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    return client


def _get(client, url):
    g.pop("_login_user", None)
    return client.get(url)


def _post(client, url, data=None):
    g.pop("_login_user", None)
    return client.post(url, data=data or {}, follow_redirects=False)


@pytest.mark.parametrize("url", ADMIN_PAGES)
def test_a_regular_user_may_read_an_administration_page(admin_app, url):
    _superuser, regular = _seed()

    response = _get(_client_as(admin_app, regular), url)

    assert response.status_code == 200
    body = response.data.decode()
    assert "Read-only" in body, "a reader must be told they cannot change these"


@pytest.mark.parametrize("url", ADMIN_PAGES)
def test_a_regular_user_cannot_submit_an_administration_change(admin_app, url):
    _superuser, regular = _seed()

    response = _post(_client_as(admin_app, regular), url)

    assert response.status_code == 403, "read access must not imply write access"


@pytest.mark.parametrize("url", ADMIN_PAGES)
def test_a_superuser_sees_the_editing_controls(admin_app, url):
    superuser, _regular = _seed()

    response = _get(_client_as(admin_app, superuser), url)

    assert response.status_code == 200
    assert "Read-only" not in response.data.decode()


def test_a_threshold_change_is_recorded_against_its_author(admin_app):
    superuser, _regular = _seed()

    _post(_client_as(admin_app, superuser), "/inventory/administration", {
        "critical_low_stock_months": "2",
        "low_stock_months": "4",
        "slow_moving_months": "7",
        "excess_stock_months": "13",
    })

    from app.models.core.audit_log import AuditLog

    entry = AuditLog.query.filter_by(action="INVENTORY_ADMIN_UPDATED").one()
    assert entry.user_id == superuser.id
    assert "critical low stock months" in entry.details
    assert "→ 2" in entry.details


def test_the_trail_is_shown_on_the_page_to_readers_too(admin_app):
    superuser, regular = _seed()
    _post(_client_as(admin_app, superuser), "/inventory/administration", {
        "critical_low_stock_months": "2",
    })

    body = _get(_client_as(admin_app, regular), "/inventory/administration").data.decode()

    assert "Administration audit trail" in body
    assert "Super User" in body, "the trail names who made the change"


def test_an_unchanged_submission_records_nothing(admin_app):
    """The trail is a record of changes, not of visits to the save button."""
    superuser, _regular = _seed()
    client = _client_as(admin_app, superuser)
    _post(client, "/inventory/administration", {"critical_low_stock_months": "2"})
    _post(client, "/inventory/administration", {"critical_low_stock_months": "2"})

    from app.models.core.audit_log import AuditLog

    assert AuditLog.query.filter_by(action="INVENTORY_ADMIN_UPDATED").count() == 1
