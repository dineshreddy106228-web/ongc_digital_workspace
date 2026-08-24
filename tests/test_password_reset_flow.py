"""A locked-out user asks by CPF; an administrator, not the form, decides.

The request form is unauthenticated, so everything that matters is checked on
the other side of it: the reply never says whether a CPF is real, approval
needs a human confirming identity, and the password it issues stops working.

Each request is made outside an application context on purpose.  Flask-Login
caches the resolved user on ``g``, which is application-context scoped, so
driving several differently-authenticated clients from inside one context
would hand every later request the first request's user.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


PRESETS = ["Password@123", "Ongc@12345"]
ADMIN_PASSWORD = "AdminPass@2026"
# Values that legitimately differ between two identical responses.
PER_REQUEST_TOKENS = (
    re.compile(rb'nonce="[^"]*"'),
    re.compile(rb'name="csrf_token" value="[^"]*"'),
)


def _stable(body: bytes) -> bytes:
    for pattern in PER_REQUEST_TOKENS:
        body = pattern.sub(b"token", body)
    return body


@pytest.fixture()
def app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'reset_flow.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True
        LOGIN_RATE_LIMIT_ENABLED = False
        PASSWORD_RESET_PRESETS = PRESETS
        PASSWORD_RESET_TEMP_TTL_HOURS = 3

    application = create_app(_Config)
    with application.app_context():
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        _seed()

    yield application

    with application.app_context():
        db.session.remove()
        db.drop_all()


def _seed():
    from app.core.roles import ADMIN_ROLE, USER_ROLE
    from app.models.core.role import Role
    from app.models.core.user import User

    admin_role = Role(id=1, name=ADMIN_ROLE)
    user_role = Role(id=2, name=USER_ROLE)
    admin = User(id=1, username="admin_one", full_name="Admin One", role=admin_role,
                 is_active=True, must_change_password=False, employee_code="")
    admin.set_password(ADMIN_PASSWORD)
    locked_out = User(id=2, username="dinesh_106228", full_name="Dinesh Reddy",
                      role=user_role, is_active=True, must_change_password=False,
                      employee_code="106228")
    locked_out.set_password("OldPass@2026")
    db.session.add_all([admin_role, user_role, admin, locked_out])
    db.session.commit()


def _admin_client(app):
    client = app.test_client()
    response = client.post(
        "/login", data={"username": "admin_one", "password": ADMIN_PASSWORD}
    )
    assert response.status_code == 302
    return client


def _signed_in_client(app, user_id):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = str(user_id)
        session["_fresh"] = True
    return client


def _temp_password_client(app, password, username="dinesh_106228"):
    """A client that signed in with the temporary password, as a user would."""
    client = app.test_client()
    response = client.post("/login", data={"username": username, "password": password})
    assert response.status_code == 302, response.status_code
    return client


def _raise_request(app, identifier="106228"):
    return app.test_client().post(
        "/forgot-password", data={"identifier": identifier}
    )


def _pending(app):
    from app.models.core.password_reset_request import (
        STATUS_PENDING, PasswordResetRequest,
    )
    with app.app_context():
        return PasswordResetRequest.query.filter_by(status=STATUS_PENDING).all()


def _user(app, user_id=2):
    from app.models.core.user import User

    with app.app_context():
        return db.session.get(User, user_id)


def _issue_temp_password(app, password, ttl_hours=3, expires_at=None):
    from app.models.core.user import User

    with app.app_context():
        target = db.session.get(User, 2)
        target.set_temporary_password(password, ttl_hours=ttl_hours)
        if expires_at is not None:
            target.temp_password_expires_at = expires_at
        db.session.commit()


def test_reply_is_identical_for_real_and_unknown_values(app):
    known = _raise_request(app, "106228")
    unknown = _raise_request(app, "999999")

    assert known.status_code == unknown.status_code == 200
    assert b"If that CPF number or username is registered" in known.data
    # Byte-identical once per-request tokens are normalised, so the page
    # cannot be read as an account-validity oracle.
    assert _stable(known.data) == _stable(unknown.data)
    assert len(_pending(app)) == 1


def test_an_admin_account_is_reached_by_username(app):
    """Admin accounts hold no CPF, so the username is their only way in."""
    response = _raise_request(app, "admin_one")

    assert response.status_code == 200
    pending = _pending(app)
    assert len(pending) == 1
    assert pending[0].user.username == "admin_one"
    assert pending[0].submitted_identifier == "admin_one"


def test_username_lookup_ignores_case_and_padding(app):
    _raise_request(app, "  Admin_One  ")
    assert len(_pending(app)) == 1


def test_a_cpf_still_wins_over_a_username_shaped_value(app):
    _raise_request(app, "106228")
    assert _pending(app)[0].user.username == "dinesh_106228"


def test_an_admin_request_is_marked_as_privileged_in_the_queue(app):
    _raise_request(app, "admin_one")

    page = _admin_client(app).get("/admin/password-requests")

    assert b"Administrator account" in page.data
    assert b"returns control" in page.data


def test_leading_zeros_still_find_the_account(app):
    _raise_request(app, " 0106228 ")
    assert len(_pending(app)) == 1


def test_repeat_requests_do_not_stack_up(app):
    for _ in range(3):
        _raise_request(app)
    assert len(_pending(app)) == 1


def test_approval_requires_verified_identity(app):
    _raise_request(app)
    entry_id = _pending(app)[0].id

    response = _admin_client(app).post(
        f"/admin/password-requests/{entry_id}/approve",
        data={"password_mode": "generate"},
    )

    assert response.status_code == 400
    assert b"Confirm you verified" in response.data
    assert len(_pending(app)) == 1


def test_generated_password_is_issued_once_and_expires(app):
    _raise_request(app)
    entry_id = _pending(app)[0].id

    response = _admin_client(app).post(
        f"/admin/password-requests/{entry_id}/approve",
        data={"password_mode": "generate", "identity_verified": "1"},
    )

    assert response.status_code == 200
    assert b"Shown once" in response.data
    assert not _pending(app)

    target = _user(app)
    assert target.must_change_password is True
    assert target.temp_password_expires_at is not None
    window = (
        target.temp_password_expires_at.replace(tzinfo=timezone.utc)
        - datetime.now(timezone.utc)
    )
    assert timedelta(hours=2, minutes=50) < window <= timedelta(hours=3)

    # The password shown on screen is the one that now signs the user in.
    issued = response.data.decode().split('<code id="issued-password">')[1].split("</code>")[0]
    assert target.check_password(issued.strip())

    # ...and it is not repeated on any later view of the queue.
    revisit = _admin_client(app).get("/admin/password-requests")
    assert issued.strip().encode() not in revisit.data


def test_only_configured_presets_are_accepted(app):
    _raise_request(app)
    entry_id = _pending(app)[0].id
    client = _admin_client(app)

    smuggled = client.post(
        f"/admin/password-requests/{entry_id}/approve",
        data={"password_mode": "preset", "preset_password": "hunter2",
              "identity_verified": "1"},
    )
    assert smuggled.status_code == 400
    assert _user(app).check_password("hunter2") is False

    allowed = client.post(
        f"/admin/password-requests/{entry_id}/approve",
        data={"password_mode": "preset", "preset_password": PRESETS[0],
              "identity_verified": "1"},
    )
    assert allowed.status_code == 200
    assert _user(app).check_password(PRESETS[0])


def test_a_handled_request_cannot_be_approved_twice(app):
    _raise_request(app)
    entry_id = _pending(app)[0].id
    client = _admin_client(app)
    payload = {"password_mode": "generate", "identity_verified": "1"}

    assert client.post(f"/admin/password-requests/{entry_id}/approve", data=payload).status_code == 200
    repeat = client.post(f"/admin/password-requests/{entry_id}/approve", data=payload)
    assert repeat.status_code == 409


def test_expired_temporary_password_no_longer_signs_in(app):
    _issue_temp_password(
        app, "Temp@12345",
        expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )

    response = app.test_client().post(
        "/login", data={"username": "dinesh_106228", "password": "Temp@12345"}
    )

    assert response.status_code == 403
    assert b"temporary password has expired" in response.data


def test_live_temporary_password_signs_in_and_lands_on_change_password(app):
    _issue_temp_password(app, "Temp@12345")

    response = app.test_client().post(
        "/login", data={"username": "dinesh_106228", "password": "Temp@12345"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"You signed in with a temporary password" in response.data
    # The card asks only for the replacement; the temporary password is not
    # put back into the page.
    assert b'name="current_password"' not in response.data
    assert b"Temp@12345" not in response.data
    assert _user(app).temp_password_used_at is not None


def test_pending_change_blocks_the_rest_of_the_workspace(app):
    _issue_temp_password(app, "Temp@12345")

    response = _signed_in_client(app, 2).get("/tasks/", follow_redirects=False)

    assert response.status_code in (301, 302, 308)
    assert "/change-password" in response.headers["Location"]


def test_setting_a_real_password_clears_the_temporary_state(app):
    _issue_temp_password(app, "Temp@12345")

    response = _temp_password_client(app, "Temp@12345").post("/change-password", data={
        "new_password": "MyOwnPass@2026",
        "confirm_password": "MyOwnPass@2026",
    })

    assert response.status_code == 302
    target = _user(app)
    assert target.check_password("MyOwnPass@2026")
    assert target.must_change_password is False
    assert target.temp_password_expires_at is None
    assert target.temp_password_used_at is None


def test_a_shared_password_cannot_be_kept_as_your_own(app):
    _issue_temp_password(app, "Temp@12345")

    response = _temp_password_client(app, "Temp@12345").post("/change-password", data={
        "new_password": PRESETS[0],
        "confirm_password": PRESETS[0],
    })

    assert response.status_code == 200
    assert b"cannot be kept as your own" in response.data
    assert _user(app).must_change_password is True


def test_the_temporary_password_cannot_be_kept_either(app):
    _issue_temp_password(app, "Temp@12345")

    response = _temp_password_client(app, "Temp@12345").post("/change-password", data={
        "new_password": "Temp@12345",
        "confirm_password": "Temp@12345",
    })

    assert response.status_code == 200
    assert b"must differ from the one you signed in with" in response.data


# ── Break-glass CLI ──────────────────────────────────────────────
# Every other reset needs a second person. This one answers to nobody, so what
# it must not do is happen quietly.


def _run_cli(app, args):
    from app.cli.passwords import issue_temp_password

    return app.test_cli_runner().invoke(issue_temp_password, args)


def _audit_actions(app, action):
    from app.models.core.audit_log import AuditLog

    with app.app_context():
        return AuditLog.query.filter_by(action=action).all()


def test_break_glass_issues_a_password_and_records_who_ran_it(app):
    result = _run_cli(app, ["--username", "admin_one", "--yes"])

    assert result.exit_code == 0, result.output
    issued = result.output.split("Temporary password:")[1].split("\n")[0].strip()

    admin = _user(app, 1)
    assert admin.check_password(issued)
    assert admin.must_change_password is True
    assert admin.temp_password_expires_at is not None

    entries = _audit_actions(app, "PASSWORD_RESET_BREAK_GLASS")
    assert len(entries) == 1
    assert "admin_one" in entries[0].details
    assert "from the server shell by" in entries[0].details
    # The password itself is never written to the record.
    assert issued not in entries[0].details


def test_break_glass_declined_at_the_prompt_changes_nothing(app):
    result = _run_cli(app, ["--username", "admin_one"])  # no --yes; stdin is empty

    assert result.exit_code == 0
    assert "Nothing was changed" in result.output
    assert _user(app, 1).temp_password_expires_at is None
    assert not _audit_actions(app, "PASSWORD_RESET_BREAK_GLASS")


def test_break_glass_rejects_an_unknown_account(app):
    result = _run_cli(app, ["--username", "nobody_here", "--yes"])

    assert result.exit_code != 0
    assert "No user found" in result.output


def test_break_glass_holds_a_supplied_password_to_the_same_rules(app):
    result = _run_cli(app, ["--username", "admin_one", "--password", "short", "--yes"])

    assert result.exit_code != 0
    assert "at least 8 characters" in result.output
    assert _user(app, 1).temp_password_expires_at is None


def test_break_glass_answers_a_request_the_user_already_raised(app):
    from app.models.core.password_reset_request import STATUS_APPROVED

    _raise_request(app, "admin_one")
    assert len(_pending(app)) == 1

    assert _run_cli(app, ["--username", "admin_one", "--yes"]).exit_code == 0

    assert not _pending(app)
    from app.models.core.password_reset_request import PasswordResetRequest

    with app.app_context():
        entry = PasswordResetRequest.query.one()
        assert entry.status == STATUS_APPROVED
        assert "server shell" in entry.handled_note


# ── A session that predates the reset ────────────────────────────
# The reset replaces the password on the account, so a session opened before
# it holds a credential that no longer exists. It must not be able to set a
# new one without proving it holds the current password.


def test_a_session_opened_before_the_reset_is_still_asked_for_the_password(app):
    # Sign in normally, the way the user was already signed in...
    client = app.test_client()
    signed_in = client.post(
        "/login", data={"username": "dinesh_106228", "password": "OldPass@2026"}
    )
    assert signed_in.status_code == 302

    # ...then an administrator issues a temporary password behind their back.
    _issue_temp_password(app, "Temp@12345")

    # The open session is now good for one page only.
    blocked = client.get("/tasks/", follow_redirects=False)
    assert blocked.status_code in (301, 302, 308)
    assert "/change-password" in blocked.headers["Location"]

    # And that page asks for the current password, which this session cannot
    # supply: the old one is gone, and it never saw the temporary one.
    card = client.get("/change-password")
    assert b'name="current_password"' in card.data

    stale = client.post("/change-password", data={
        "current_password": "OldPass@2026",
        "new_password": "SneakyPass@2026",
        "confirm_password": "SneakyPass@2026",
    })
    assert stale.status_code == 200
    assert b"Current password is incorrect" in stale.data
    assert _user(app).check_password("SneakyPass@2026") is False
    assert _user(app).must_change_password is True


def test_signing_in_with_the_temporary_password_clears_the_older_claim(app):
    """A fresh temp-password sign-in is what earns the shorter form."""
    _issue_temp_password(app, "Temp@12345")

    client = app.test_client()
    client.post("/login", data={"username": "dinesh_106228", "password": "Temp@12345"})

    card = client.get("/change-password")
    assert b'name="current_password"' not in card.data

    done = client.post("/change-password", data={
        "new_password": "MyOwnPass@2026",
        "confirm_password": "MyOwnPass@2026",
    })
    assert done.status_code == 302

    # The claim does not outlive the change: a later visit asks again.
    assert b'name="current_password"' in client.get("/change-password").data
