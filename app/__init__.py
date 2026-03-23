"""Application factory for ONGC Digital Workspace."""

import logging
import secrets
from importlib import import_module
from urllib.parse import urlparse
from flask import Flask, g, jsonify, render_template_string, request, session
from flask_login import current_user
from config import Config

logger = logging.getLogger(__name__)
from app.core.services.rich_text import render_rich_text
from app.core.utils.datetime import format_datetime_ist
from app.extensions import cache, csrf, db, login_manager, migrate
from app.features import (
    get_nav_modules,
    is_module_enabled,
    register_feature_blueprints,
)


def create_app(config_class=Config):
    """Create, configure, and return the Flask application."""

    app = Flask(__name__)
    app.config.from_object(config_class)

    # Fail fast in non-development environments when default insecure key is used.
    if (
        app.config.get("FLASK_ENV") != "development"
        and app.config.get("SECRET_KEY") == "fallback-insecure-key-change-me"
    ):
        raise RuntimeError(
            "SECRET_KEY is not configured. Set a strong SECRET_KEY in your environment."
        )

    # ── Initialise extensions ────────────────────────────────────
    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)
    csrf.init_app(app)
    cache.init_app(app)

    # Import all models so relationship resolution and Alembic autogenerate
    # see the complete metadata set.
    import_module("app.models")

    # ── Flask-Login user loader ──────────────────────────────────
    from app.models.core.user import User

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # ── Register blueprints ──────────────────────────────────────
    from app.core.auth import auth_bp
    from app.notifications import notifications_bp
    app.register_blueprint(auth_bp)
    app.register_blueprint(notifications_bp, url_prefix="/notifications")

    # Registry-managed modules are registered dynamically so production can expose
    # only the approved surfaces for the current environment.
    # NOTE: committee_bp is registered via the module registry (module_registry.py)
    # — no separate registration needed here.
    register_feature_blueprints(app)

    # ── Register CLI commands ────────────────────────────────────
    from app.cli import register_cli
    register_cli(app)

    app.jinja_env.filters["datetime_ist"] = format_datetime_ist
    app.jinja_env.filters["richtext"] = render_rich_text

    @cache.memoize(timeout=30)
    def _get_committee_open_count(user_id: int, role_name: str, office_id) -> int:
        """Return committee open-task count for nav badge (short-lived cache)."""
        try:
            from app.models.committee.committee_task import CommitteeTask
            from app.models.committee.committee_task_member import CommitteeTaskMember
            from sqlalchemy import or_
            if role_name in ("superuser", "admin"):
                return CommitteeTask.query.filter(
                    CommitteeTask.status != "done"
                ).count()
            # role == "user": scope to office + directly assigned tasks
            return CommitteeTask.query.filter(
                CommitteeTask.status != "done",
                or_(
                    CommitteeTask.office_id == office_id,
                    CommitteeTask.members.any(CommitteeTaskMember.user_id == user_id),
                ),
            ).count()
        except Exception:
            logger.exception("Failed to load committee open count for user_id=%s", user_id)
            return 0

    # ── Inject common template context ───────────────────────────
    @app.context_processor
    def inject_globals():
        from app.core.services.notifications import (
            get_unread_notification_count,
            get_unread_notifications,
        )
        from app.core.services.announcements import get_latest_login_announcement_for_user

        def _get_broadcast_popup(user_id):
            """Return the latest unread broadcast unless dismissed this session."""
            try:
                recipient = get_latest_login_announcement_for_user(user_id)
            except Exception:
                logger.exception("Failed to load login announcement for user_id=%s", user_id)
                return None
            if recipient is None:
                return None
            dismissed = session.get("dismissed_broadcast")
            if dismissed is not None and dismissed == recipient.announcement_id:
                return None
            return recipient

        # Committee open-task count for nav badge
        committee_open = 0
        if current_user.is_authenticated:
            role_name = (
                current_user.role.name if current_user.role else ""
            ).lower()
            if role_name in ("superuser", "admin") or (
                role_name == "user" and current_user.has_module_access("committee")
            ):
                committee_open = _get_committee_open_count(
                    current_user.id, role_name, current_user.office_id
                )

        return dict(
            app_name=app.config["APP_NAME"],
            csp_nonce=lambda: getattr(g, "csp_nonce", ""),
            is_module_enabled=lambda module_code: is_module_enabled(module_code, app),
            nav_modules=get_nav_modules(current_user, app)
            if current_user.is_authenticated
            else [],
            unread_notification_count=get_unread_notification_count(current_user.id)
            if current_user.is_authenticated
            else 0,
            unread_notifications=get_unread_notifications(current_user.id, limit=5)
            if current_user.is_authenticated
            else [],
            login_announcement=(
                _get_broadcast_popup(current_user.id)
                if current_user.is_authenticated
                else None
            ),
            committee_open_count=committee_open,
        )

    # ── Per-request nonce for CSP-compatible inline scripts ──────
    @app.before_request
    def set_csp_nonce():
        g.csp_nonce = secrets.token_urlsafe(16)

    # ── Security response headers ────────────────────────────────
    @app.after_request
    def set_security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"

        if app.config.get("CSP_ENABLED", True):
            nonce = getattr(g, "csp_nonce", "")
            csp_parts = [
                "default-src 'self'",
                "base-uri 'self'",
                "form-action 'self'",
                "frame-ancestors 'self'",
                "object-src 'none'",
                f"script-src 'self' 'nonce-{nonce}' https://cdnjs.cloudflare.com",
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
                "font-src 'self' https://fonts.gstatic.com data:",
                "img-src 'self' data:",
                "connect-src 'self'",
            ]
            report_uri = app.config.get("CSP_REPORT_URI")
            if report_uri:
                csp_parts.append(f"report-uri {report_uri}")

            csp_header_name = (
                "Content-Security-Policy-Report-Only"
                if app.config.get("CSP_REPORT_ONLY", True)
                else "Content-Security-Policy"
            )
            response.headers[csp_header_name] = "; ".join(csp_parts)

        # Tell browsers to use HTTPS only for the next year (production only)
        if app.config.get("FLASK_ENV") != "development":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
        return response

    # ── Error handlers ───────────────────────────────────────────
    @app.errorhandler(403)
    def forbidden(e):
        from flask import render_template_string
        return render_template_string("""
        {% extends "base.html" %}
        {% block title %}403 Forbidden{% endblock %}
        {% block content %}
        <div class="auth-wrapper">
            <div class="auth-card" style="text-align:center">
                <h1 style="font-size:2.5rem;color:var(--color-danger)">403</h1>
                <p>You do not have permission to access this page.</p>
                <a href="{{ url_for('main.dashboard') }}" class="btn btn-primary" style="margin-top:1rem">Back to Dashboard</a>
            </div>
        </div>
        {% endblock %}
        """), 403

    @app.errorhandler(404)
    def not_found(e):
        return render_template_string("""
        {% extends "base.html" %}
        {% block title %}404 Not Found{% endblock %}
        {% block content %}
        <div class="auth-wrapper">
            <div class="auth-card" style="text-align:center">
                <h1 style="font-size:2.5rem;color:var(--color-text-muted)">404</h1>
                <p>The page you requested was not found.</p>
                <a href="{{ url_for('main.dashboard') }}" class="btn btn-primary" style="margin-top:1rem">Back to Dashboard</a>
            </div>
        </div>
        {% endblock %}
        """), 404

    @app.errorhandler(413)
    def request_entity_too_large(e):
        limit_bytes = int(app.config.get("MAX_CONTENT_LENGTH") or 0)
        limit_mb = max(limit_bytes / (1024 * 1024), 0)
        referrer = request.referrer or "/"
        parsed = urlparse(referrer)
        back_url = referrer if parsed.scheme in ("http", "https", "") else "/"
        return render_template_string("""
        {% extends "base.html" %}
        {% block title %}Upload Too Large{% endblock %}
        {% block content %}
        <div class="auth-wrapper">
            <div class="auth-card" style="max-width: 720px;">
                <h1 style="font-size:2rem;color:var(--color-danger)">Upload Too Large</h1>
                <p>The selected files exceed the current request limit of {{ limit_mb|round(0)|int }} MB.</p>
                <p>Reduce the upload size or raise <code>MAX_CONTENT_LENGTH</code> in the app configuration if larger files are expected.</p>
                <a href="{{ back_url }}" class="btn btn-primary" style="margin-top:1rem">Go Back</a>
            </div>
        </div>
        {% endblock %}
        """, limit_mb=limit_mb, back_url=back_url), 413

    # ── Health check endpoint (no auth, no redirect) ─────────────
    @app.route("/health")
    def health_check():
        return jsonify({"status": "ok"}), 200

    return app
