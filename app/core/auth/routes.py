"""Authentication routes – login / logout / change-password."""

from collections import defaultdict, deque
from datetime import datetime, timezone
from flask import (
    render_template, redirect, url_for, flash, request, current_app, session, jsonify,
)
from flask_login import login_user, logout_user, login_required, current_user
from app.core.auth import auth_bp
from app.extensions import db
from app.models.core.user import User
from app.models.core.audit_log import AuditLog
from app.models.core.password_reset_request import (
    STATUS_PENDING,
    PasswordResetRequest,
)
from app.core.utils.request_meta import get_client_ip, get_user_agent, safe_referrer_target
from app.core.utils.activity import log_activity
from app.core.services.password_reset import (
    expire_stale_requests,
    find_user_by_identifier,
    is_preset_password,
    password_min_length,
    validate_chosen_password,
)


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

        # Temporary password past its window – the credential is spent even
        # though it still matches the stored hash.
        if user.temporary_password_expired():
            AuditLog.log(
                action="login_blocked_temp_expired",
                user_id=user.id,
                entity_type="User",
                entity_id=str(user.id),
                details="Sign-in refused: the issued temporary password had expired.",
                ip_address=_client_ip(),
                user_agent=get_user_agent(),
            )
            flash(
                "Your temporary password has expired. Please raise a new "
                "password reset request.",
                "danger",
            )
            return render_template("auth/login.html"), 403

        # Success
        login_user(user, remember=False)
        session["show_login_welcome"] = True
        _clear_login_failures(username)
        user.last_login_at = datetime.now(timezone.utc)
        if user.has_temporary_password() and user.temp_password_used_at is None:
            # Recorded for the administration queue: an issued password that
            # was never used looks different from one that was.
            user.temp_password_used_at = user.last_login_at
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
            # Only a session that just authenticated with the issued password
            # may skip the current-password check when replacing it.  Sessions
            # opened before the reset must still prove they hold the password
            # on the account — which, after a reset, they no longer do.
            session["temp_password_login"] = True
            flash("Please change your password before continuing.", "warning")
            return redirect(url_for("auth.change_password"))

        session.pop("temp_password_login", None)

        return redirect(url_for("main.dashboard"))

    return render_template("auth/login.html")


# ── LOGOUT ───────────────────────────────────────────────────────
@auth_bp.route("/logout")
@login_required
def logout():
    uid = current_user.id
    uname = current_user.username
    session.pop("show_login_welcome", None)
    logout_user()
    AuditLog.log(
        action="logout",
        user_id=uid,
        ip_address=_client_ip(),
        user_agent=get_user_agent(),
    )
    log_activity(uname, "logout", "user", uname)
    db.session.commit()
    return redirect(url_for("auth.login"))


@auth_bp.route("/welcome-acknowledge", methods=["POST"])
@login_required
def welcome_acknowledge():
    session.pop("show_login_welcome", None)
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True})
    return redirect(safe_referrer_target(url_for("main.dashboard")))


# ── FORGOT PASSWORD ──────────────────────────────────────────────
# The reply is deliberately identical whether or not the value matches an
# account.  Usernames in this workspace carry the CPF number, so a form that
# confirmed which numbers exist would hand out a roster.
_RESET_ACK = (
    "If that CPF number or username is registered, a password reset request "
    "has been raised. Your administrator will contact you with a temporary "
    "password."
)


@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for("auth.change_password"))

    if request.method != "POST":
        return render_template("auth/forgot_password.html")

    submitted = request.form.get("identifier", "").strip()[:80]

    if not submitted:
        flash("Enter your CPF number or username.", "danger")
        return render_template("auth/forgot_password.html"), 400

    # Reuse the login limiter so the form cannot be walked through the CPF
    # range, and so a flood of requests cannot fill the admin queue.
    rate_subject = f"reset::{submitted}"
    is_limited, retry_after = _check_login_rate_limit(rate_subject)
    if is_limited:
        flash(
            f"Too many reset requests. Try again in about {retry_after} seconds.",
            "danger",
        )
        return render_template("auth/forgot_password.html"), 429
    _record_login_failure(rate_subject)

    expire_stale_requests()
    user = find_user_by_identifier(submitted)

    if user is None or not user.is_active:
        AuditLog.log(
            action="password_reset_requested_unmatched",
            details=f"No active account for submitted identifier '{submitted}'.",
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )
        flash(_RESET_ACK, "success")
        return render_template("auth/forgot_password.html")

    existing = PasswordResetRequest.query.filter_by(
        user_id=user.id, status=STATUS_PENDING
    ).first()

    if existing is None:
        db.session.add(
            PasswordResetRequest(
                user_id=user.id,
                submitted_identifier=submitted,
                status=STATUS_PENDING,
                request_ip=_client_ip()[:45],
                request_user_agent=(get_user_agent() or "")[:500],
            )
        )
        db.session.commit()
        AuditLog.log(
            action="password_reset_requested",
            user_id=user.id,
            entity_type="User",
            entity_id=str(user.id),
            details=f"Reset request raised via identifier '{submitted}'.",
            ip_address=_client_ip(),
            user_agent=get_user_agent(),
        )

    flash(_RESET_ACK, "success")
    return render_template("auth/forgot_password.html")


# ── CHANGE PASSWORD ──────────────────────────────────────────────
@auth_bp.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    # A user who signed in with an administrator-issued password proved on
    # that sign-in that they hold it; asking again proves nothing, and putting
    # a live credential back into the page only spreads it around.  The claim
    # is scoped to that one session: a session opened before the reset never
    # saw the new password, so it is still asked for the current one — which
    # the reset has already invalidated.
    requires_current = not session.get("temp_password_login", False)
    minimum = password_min_length()

    def _render(status=200):
        return render_template(
            "auth/change_password.html",
            requires_current=requires_current,
            min_length=minimum,
            temp_password_expires_at=current_user.temp_password_expires_at,
        ), status

    if request.method != "POST":
        return _render()[0]

    current_pw = request.form.get("current_password", "")
    new_pw = request.form.get("new_password", "")
    confirm_pw = request.form.get("confirm_password", "")

    if requires_current and not current_user.check_password(current_pw):
        flash("Current password is incorrect.", "danger")
        return _render()

    error = validate_chosen_password(new_pw)
    if error:
        flash(error, "danger")
        return _render()

    if new_pw != confirm_pw:
        flash("New passwords do not match.", "danger")
        return _render()

    if current_user.check_password(new_pw):
        flash(
            "New password must differ from the one you signed in with.",
            "danger",
        )
        return _render()

    if is_preset_password(new_pw):
        # These values circulate by design; nobody may keep one.
        flash(
            "That is a shared temporary password and cannot be kept as your "
            "own. Choose a different password.",
            "danger",
        )
        return _render()

    current_user.set_password(new_pw)
    current_user.must_change_password = False
    current_user.clear_temporary_password_state()
    session.pop("temp_password_login", None)
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
