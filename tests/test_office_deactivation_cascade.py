"""Closing an office closes the accounts mapped to it."""
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


@pytest.fixture()
def admin_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'admin.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        # SQLite only autoincrements INTEGER primary keys; MySQL keeps BIGINT, so
        # the audit rows this flow writes need the narrower type under test.
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


def _seed(office_active: bool = True):
    from app.core.roles import ADMIN_ROLE, USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.office.office import Office

    admin_role = Role(id=1, name=ADMIN_ROLE)
    user_role = Role(id=2, name=USER_ROLE)
    office = Office(id=1, office_code="QPCL", office_name="Quality Lab", is_active=office_active)
    other_office = Office(id=2, office_code="ST_RJY", office_name="RJY Team", is_active=True)
    admin = User(id=1, username="admin", password_hash="x", role=admin_role,
                 office_id=2, is_active=True, must_change_password=False)
    members = [
        User(id=10, username="lab_one", password_hash="x", role=user_role,
             office_id=1, is_active=True, must_change_password=False),
        User(id=11, username="lab_two", password_hash="x", role=user_role,
             office_id=1, is_active=True, must_change_password=False),
        User(id=12, username="lab_dormant", password_hash="x", role=user_role,
             office_id=1, is_active=False, must_change_password=False),
    ]
    outsider = User(id=13, username="rjy_one", password_hash="x", role=user_role,
                    office_id=2, is_active=True, must_change_password=False)
    db.session.add_all([admin_role, user_role, office, other_office, admin, outsider, *members])
    db.session.commit()
    return admin, office


def _client_as(app, user):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user.id)
        session["_fresh"] = True
    return client


def _post(client, url):
    """Each request starts from a clean login context — see the office navigator tests."""
    g.pop("_login_user", None)
    return client.post(url, follow_redirects=False)


def _active(username: str) -> bool:
    from app.models.core.user import User

    return User.query.filter_by(username=username).one().is_active


def test_deactivating_an_office_deactivates_its_mapped_users(admin_app):
    admin, office = _seed()

    response = _post(_client_as(admin_app, admin), f"/admin/offices/{office.id}/toggle")

    assert response.status_code == 302
    from app.models.office.office import Office

    assert db.session.get(Office, 1).is_active is False
    assert _active("lab_one") is False
    assert _active("lab_two") is False
    # Users in other offices are untouched.
    assert _active("rjy_one") is True


def test_the_cascade_is_recorded_for_each_user(admin_app):
    admin, office = _seed()

    _post(_client_as(admin_app, admin), f"/admin/offices/{office.id}/toggle")

    from app.models.core.audit_log import AuditLog

    cascaded = AuditLog.query.filter_by(action="USER_DEACTIVATED").all()
    assert {entry.entity_id for entry in cascaded} == {"10", "11"}
    office_entry = AuditLog.query.filter_by(action="OFFICE_DEACTIVATED").one()
    assert "lab_one, lab_two" in office_entry.details


def test_an_admin_mapped_to_the_office_keeps_their_own_account(admin_app):
    admin, office = _seed()
    admin.office_id = office.id
    db.session.commit()

    _post(_client_as(admin_app, admin), f"/admin/offices/{office.id}/toggle")

    assert _active("admin") is True
    assert _active("lab_one") is False


def test_reactivating_an_office_leaves_accounts_deactivated(admin_app):
    admin, office = _seed(office_active=False)
    from app.models.core.user import User

    User.query.filter_by(office_id=office.id).update({"is_active": False})
    db.session.commit()

    _post(_client_as(admin_app, admin), f"/admin/offices/{office.id}/toggle")

    from app.models.office.office import Office

    assert db.session.get(Office, 1).is_active is True
    # Each account is restored deliberately, not swept back in by the office.
    assert _active("lab_one") is False
    assert _active("lab_two") is False
