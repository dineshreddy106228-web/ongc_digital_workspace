"""User Management offers only the modules this deployment actually runs.

Retired modules (Committee) and feature-off ones (Reports, Forecasting, Manpower
Planning) are not offered for granting, but grants already stored against them
survive an edit — hiding a module must not quietly revoke it.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


@pytest.fixture()
def admin_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'modules.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True
        # The three flags that are off in production today.
        ENABLE_MANPOWER_PLANNING = False
        ENABLE_REPORTS = False
        ENABLE_FORECASTING = False

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


def _plain_user():
    from app.core.roles import USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User

    role = Role(id=1, name=USER_ROLE)
    user = User(id=1, username="chemist", password_hash="x", role=role,
                is_active=True, must_change_password=False)
    db.session.add_all([role, user])
    db.session.commit()
    return user, role


def _grants(user_id: int) -> set[str]:
    from app.models.core.user_module_permission import UserModulePermission

    return {
        permission.module_code
        for permission in UserModulePermission.query.filter_by(
            user_id=user_id, can_access=True
        ).all()
    }


def test_feature_off_modules_are_not_offered_for_granting(admin_app):
    from app.modules.admin.routes import _offered_module_codes

    offered = _offered_module_codes()

    assert {"tasks", "inventory", "csc", "quality_control"} <= offered
    assert not offered & {"manpower", "reports", "forecasting"}
    # Committee was removed from the registry altogether, so it cannot be offered.
    assert "committee" not in offered


def test_the_module_admin_picker_offers_only_live_business_modules(admin_app):
    from app.modules.admin.routes import _manageable_module_options

    codes = {option["code"] for option in _manageable_module_options()}

    assert codes == {"tasks", "inventory", "csc", "quality_control"}


def test_editing_a_user_keeps_grants_the_form_never_showed(admin_app):
    from app.models.core.user_module_permission import UserModulePermission
    from app.modules.admin.routes import _set_module_permissions

    user, role = _plain_user()
    for code in ("tasks", "committee", "reports"):
        db.session.add(UserModulePermission(user_id=user.id, module_code=code, can_access=True))
    db.session.commit()

    # The admin re-saves the user with only what the form offered.
    _set_module_permissions(user, ["tasks", "inventory"], role)
    db.session.commit()

    assert _grants(user.id) == {"tasks", "inventory", "committee", "reports"}


def test_clearing_every_offered_module_still_leaves_the_retired_grant(admin_app):
    from app.models.core.user_module_permission import UserModulePermission
    from app.modules.admin.routes import _set_module_permissions

    user, role = _plain_user()
    for code in ("tasks", "committee"):
        db.session.add(UserModulePermission(user_id=user.id, module_code=code, can_access=True))
    db.session.commit()

    _set_module_permissions(user, [], role)
    db.session.commit()

    assert _grants(user.id) == {"committee"}


def test_a_retired_code_is_never_rendered_on_the_edit_form(admin_app):
    from flask import g
    from app.core.roles import ADMIN_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User
    from app.models.core.user_module_permission import UserModulePermission

    user, _role = _plain_user()
    db.session.add_all([
        UserModulePermission(user_id=user.id, module_code="committee", can_access=True),
        UserModulePermission(user_id=user.id, module_code="tasks", can_access=True),
    ])
    admin_role = Role(id=2, name=ADMIN_ROLE)
    admin = User(id=2, username="admin", password_hash="x", role=admin_role,
                 is_active=True, must_change_password=False)
    db.session.add_all([admin_role, admin])
    db.session.commit()

    client = admin_app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(admin.id)
        session["_fresh"] = True
    g.pop("_login_user", None)
    body = client.get(f"/admin/users/{user.id}/edit").get_data(as_text=True)

    assert "committee" not in body
    assert "forecasting" not in body
    assert 'value="tasks"' in body


def test_the_backup_center_is_its_own_admin_module(admin_app):
    """It shares the admin blueprint but stands alone in the nav and on the home page."""
    from app.core.module_registry import (
        get_module_definition,
        is_module_registered,
        user_can_access_module,
    )
    from app.core.roles import ADMIN_ROLE, SUPERUSER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User

    definition = get_module_definition("admin_backups")
    assert definition is not None
    assert definition.endpoint == "admin.backup_center"
    assert definition.nav_visible and definition.dashboard_visible
    # Registered even though User Management registered the blueprint first.
    assert is_module_registered("admin_backups")
    assert is_module_registered("admin_users")

    admin_role = Role(id=10, name=ADMIN_ROLE)
    admin = User(id=10, username="admin", password_hash="x", role=admin_role,
                 is_active=True, must_change_password=False)
    super_role = Role(id=11, name=SUPERUSER_ROLE)
    super_user = User(id=11, username="super", password_hash="x", role=super_role,
                      is_active=True, must_change_password=False)
    db.session.add_all([admin_role, admin, super_role, super_user])
    db.session.commit()

    assert user_can_access_module("admin_backups", admin) is True
    # A super user runs the business modules; the admin surfaces are not theirs.
    assert user_can_access_module("admin_backups", super_user) is False
    assert user_can_access_module("admin_users", super_user) is False


def test_the_admin_home_showcases_both_administration_modules(admin_app):
    from app.core.roles import ADMIN_ROLE, SUPERUSER_ROLE
    from app.core.services.dashboard import _build_module_showcase
    from app.models.core.role import Role
    from app.models.core.user import User

    admin_role = Role(id=10, name=ADMIN_ROLE)
    admin = User(id=10, username="admin", password_hash="x", role=admin_role,
                 is_active=True, must_change_password=False)
    super_role = Role(id=11, name=SUPERUSER_ROLE)
    super_user = User(id=11, username="super", password_hash="x", role=super_role,
                      is_active=True, must_change_password=False)
    db.session.add_all([admin_role, admin, super_role, super_user])
    db.session.commit()

    # url_for needs a request context to build the card links.
    with admin_app.test_request_context("/dashboard"):
        admin_cards = {card["key"]: card for card in _build_module_showcase(admin)}
        super_keys = {card["key"] for card in _build_module_showcase(super_user)}

    assert set(admin_cards) == {"admin_users", "admin_backups"}
    assert admin_cards["admin_backups"]["clickable"] is True
    assert admin_cards["admin_backups"]["href"] == "/admin/backups"

    assert "admin_backups" not in super_keys
    assert "admin_users" not in super_keys


def test_the_module_admin_picker_never_offers_an_administration_module(admin_app):
    from app.modules.admin.routes import _manageable_module_options

    codes = {option["code"] for option in _manageable_module_options()}

    assert not codes & {"admin_users", "admin_backups", "dashboard"}
