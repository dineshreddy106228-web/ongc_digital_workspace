"""Authentication routes – login / logout / change-password."""

from collections import defaultdict, deque
from datetime import datetime, timezone
from flask import (
    render_template, redirect, url_for, flash, request, current_app,
)
from flask_login import login_user, logout_user, login_required, current_user
from app.auth import auth_bp
from app.extensions import db
from app.models.user import User
from app.models.audit_log import AuditLog
from app.utils.request_meta import get_client_ip, get_user_agent
from app.utils.activity import log_activity


_FAILED_LOGIN_ATTEMPTS = defaultdict(deque)
_LOGIN_LOCK_UNTIL = {}


def _client_ip():
    """Best-effort client IP (respects X-Forwarded-For behind a proxy)."""
    return get_client_ip()


def _prune_rate_limit_state(now_ts: float, window: int) -> None:
    """
    Drop stale rate-limit state to avoid unbounded process memory growth.

    Keys are removed when:
    - lock has expired and
    - there are no attempts in the active window
    """
    stale_attempt_cutoff = now_ts - window

    for key in list(_LOGIN_LOCK_UNTIL.keys()):
        if _LOGIN_LOCK_UNTIL[key] <= now_ts:
            _LOGIN_LOCK_UNTIL.pop(key, None)

    for key, dq in list(_FAILED_LOGIN_ATTEMPTS.items()):
        while dq and dq[0] < stale_attempt_cutoff:
            dq.popleft()
        if not dq and key not in _LOGIN_LOCK_UNTIL:
            _FAILED_LOGIN_ATTEMPTS.pop(key, None)


def _rate_key(username: str) -> str:
    # Rate-limit key combines caller IP and attempted username.
    return f"{_client_ip()}::{(username or '').strip().lower()}"


def _check_login_rate_limit(username: str):
    if not current_app.config.get("LOGIN_RATE_LIMIT_ENABLED", True):
        return False, 0

    key = _rate_key(username)
    now_ts = datetime.now(timezone.utc).timestamp()
    window = max(int(current_app.config.get("LOGIN_RATE_LIMIT_WINDOW_SECONDS", 300)), 1)
    _prune_rate_limit_state(now_ts, window)
    locked_until = _LOGIN_LOCK_UNTIL.get(key, 0)
    if now_ts < locked_until:
        return True, int(locked_until - now_ts)
    return False, 0


def _record_login_failure(username: str):
    if not current_app.config.get("LOGIN_RATE_LIMIT_ENABLED", True):
        return

    key = _rate_key(username)
    now_ts = datetime.now(timezone.utc).timestamp()
    window = max(int(current_app.config.get("LOGIN_RATE_LIMIT_WINDOW_SECONDS", 300)), 1)
    limit = max(int(current_app.config.get("LOGIN_RATE_LIMIT_MAX_ATTEMPTS", 8)), 1)
    lock_seconds = max(int(current_app.config.get("LOGIN_RATE_LIMIT_LOCK_SECONDS", 300)), 1)
    _prune_rate_limit_state(now_ts, window)

    dq = _FAILED_LOGIN_ATTEMPTS[key]
    dq.append(now_ts)
    while dq and now_ts - dq[0] > window:
        dq.popleft()

    if len(dq) >= limit:
        _LOGIN_LOCK_UNTIL[key] = now_ts + lock_seconds
        dq.clear()


def _clear_login_failures(username: str):
    key = _rate_key(username)
    _FAILED_LOGIN_ATTEMPTS.pop(key, None)
    _LOGIN_LOCK_UNTIL.pop(key, None)


# ── LOGIN ────────────────────────────────────────────────────────
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("main.dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        is_limited, retry_after = _check_login_rate_limit(username)
        if is_limited:
            flash(
                f"Too many failed login attempts. Try again in about {retry_after} seconds.",
                "danger",
            )
            return render_template("auth/login.html"), 429

        user = User.query.filter_by(username=username).first()

        # Failed – user not found or bad password
        if user is None or not user.check_password(password):
            _record_login_failure(username)
            AuditLog.log(
                action="login_failed",
                details=f"Attempt for username='{username}'",
                ip_address=_client_ip(),
                user_agent=get_user_agent(),
            )
            flash("Invalid username or password.", "danger")
            return render_template("auth/login.html"), 401

        # Account deactivated
        if not user.is_active:
            _record_login_failure(username)
            AuditLog.log(
                action="login_blocked_inactive",
                user_id=user.id,
                ip_address=_client_ip(),
                user_agent=get_user_agent(),
            )
            flash("Your account is deactivated. Contact an administrator.", "danger")
            return render_template("auth/login.html"), 403

        # Success
        login_user(user, remember=False)
        _clear_login_failures(username)
        user.last_login_at = datetime.now(timezone.utc)
        db.session.commit()

        AuditLog.log(
            action="login_success",
            user_id=user.id,
            entity_type="User",
            entity_id=str(user.id),
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )
        log_activity(user.username, "login", "user", user.full_name or user.username)
        db.session.commit()

        if user.must_change_password:
            flash("Please change your password before continuing.", "warning")
            return redirect(url_for("auth.change_password"))

        return redirect(url_for("main.dashboard"))

    return render_template("auth/login.html")


# ── LOGOUT ───────────────────────────────────────────────────────
@auth_bp.route("/logout")
@login_required
def logout():
    uid = current_user.id
    uname = current_user.username
    logout_user()
    AuditLog.log(
        action="logout",
        user_id=uid,
        ip_address=_client_ip(),
        user_agent=get_user_agent(),
    )
    log_activity(uname, "logout", "user", uname)
    db.session.commit()
    flash("You have been logged out.", "info")
    return redirect(url_for("auth.login"))


# ── CHANGE PASSWORD ──────────────────────────────────────────────
@auth_bp.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    if request.method == "POST":
        current_pw = request.form.get("current_password", "")
        new_pw = request.form.get("new_password", "")
        confirm_pw = request.form.get("confirm_password", "")

        if not current_user.check_password(current_pw):
            flash("Current password is incorrect.", "danger")
            return render_template("auth/change_password.html")

        if len(new_pw) < 8:
            flash("New password must be at least 8 characters.", "danger")
            return render_template("auth/change_password.html")

        if new_pw != confirm_pw:
            flash("New passwords do not match.", "danger")
            return render_template("auth/change_password.html")

        if current_pw == new_pw:
            flash("New password must differ from the current one.", "danger")
            return render_template("auth/change_password.html")

        current_user.set_password(new_pw)
        current_user.must_change_password = False
        db.session.commit()

        AuditLog.log(
            action="password_changed",
            user_id=current_user.id,
            entity_type="User",
            entity_id=str(current_user.id),
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )
        log_activity(current_user.username, "password_changed", "user",
                      current_user.full_name or current_user.username)
        db.session.commit()

        flash("Password changed successfully.", "success")
        return redirect(url_for("main.dashboard"))

    return render_template("auth/change_password.html")
