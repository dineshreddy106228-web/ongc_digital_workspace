"""Application factory for ONGC Digital Workspace."""

import logging
import os
import secrets
from importlib import import_module
from flask import Flask, flash, g, jsonify, redirect, render_template_string, request, url_for
from flask_login import current_user
from flask_wtf.csrf import CSRFError
from config import Config

logger = logging.getLogger(__name__)
from app.core.services.rich_text import render_rich_text
from app.core.utils.datetime import format_datetime_ist
from app.core.utils.request_meta import safe_referrer_target
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
    app.register_blueprint(auth_bp)

    # Registry-managed modules are registered dynamically so production can expose
    # only the approved surfaces for the current environment.
    register_feature_blueprints(app)

    # ── Register CLI commands ────────────────────────────────────
    # Gunicorn never invokes application-specific Flask commands.  Deferring
    # their registration keeps CLI-only dependencies (notably the Inventory
    # pandas/numpy audit stack) out of every long-lived web worker.
    if os.environ.get("FLASK_RUN_FROM_CLI") == "true":
        from app.cli import register_cli

        register_cli(app)

    # A platform cron can run ``flask create-daily-backup`` explicitly.  The
    # web-process scheduler is a safety net for normal single-worker operation
    # so the day is still backed up when no external scheduler is configured.
    # Never start it in CLI commands, tests, or Flask's debug reloader parent.
    is_reloader_parent = app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true"
    if (
        app.config.get("AUTO_BACKUP_ENABLED", True)
        and not app.config.get("TESTING", False)
        and os.environ.get("FLASK_RUN_FROM_CLI") != "true"
        and not is_reloader_parent
    ):
        from app.core.services.backups import start_retained_backup_scheduler

        start_retained_backup_scheduler(app)

    app.jinja_env.filters["datetime_ist"] = format_datetime_ist
    app.jinja_env.filters["richtext"] = render_rich_text

    # ── Inject common template context ───────────────────────────
    @app.context_processor
    def inject_globals():
        return dict(
            app_name=app.config["APP_NAME"],
            csp_nonce=lambda: getattr(g, "csp_nonce", ""),
            is_module_enabled=lambda module_code: is_module_enabled(module_code, app),
            nav_modules=get_nav_modules(current_user, app)
            if current_user.is_authenticated
            else [],
        )

    # ── Per-request nonce for CSP-compatible inline scripts ──────
    @app.before_request
    def set_csp_nonce():
        g.csp_nonce = secrets.token_urlsafe(16)

    # ── A pending password change blocks everything else ─────────
    # An admin-issued password is meant to survive exactly one sign-in.  Without
    # this gate a user could reach the change-password prompt and simply
    # navigate away, leaving a shared or dictated credential live on the
    # account for the rest of its window.
    _PASSWORD_CHANGE_EXEMPT = {
        "auth.change_password",
        "auth.logout",
        "auth.login",
        "auth.forgot_password",
        "static",
    }

    @app.before_request
    def force_pending_password_change():
        endpoint = request.endpoint
        if endpoint in (None, "static") or endpoint in _PASSWORD_CHANGE_EXEMPT:
            return None
        if not current_user.is_authenticated:
            return None
        if not getattr(current_user, "must_change_password", False):
            return None
        flash("Please change your password before continuing.", "warning")
        return redirect(url_for("auth.change_password"))

    # ── Recurring routines catch up on first use each day ────────
    # A cron running `flask generate-recurring-tasks` is the intended trigger;
    # this is the safety net for the day it does not run, so a routine never
    # silently stops rolling. Runs at most once per process per day, and the
    # generator is idempotent, so a second run is a no-op.
    app._recurring_generated_on = None

    @app.before_request
    def roll_recurring_tasks():
        from datetime import date as _date

        if request.endpoint in (None, "static"):
            return
        today = _date.today()
        if app._recurring_generated_on == today:
            return
        # Claim the day before doing the work: a failure must not put every
        # later request into a retry loop.
        app._recurring_generated_on = today
        try:
            from app.core.services.recurring_tasks import generate_due_recurring_tasks
            from app.core.services.dashboard import invalidate_dashboard_summary_metrics

            result = generate_due_recurring_tasks()
            if result["tasks"] or result["rolled"]:
                invalidate_dashboard_summary_metrics()
                db.session.commit()
                app.logger.info(
                    "Recurring routines rolled: %s created, %s rolled forward.",
                    result["tasks"], result["rolled"],
                )
            else:
                db.session.rollback()
        except Exception:
            db.session.rollback()
            app.logger.exception("Recurring task roll-forward failed")

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
                f"script-src 'self' 'nonce-{nonce}' https://cdnjs.cloudflare.com https://cdn.jsdelivr.net",
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net",
                "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net data:",
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
        back_url = safe_referrer_target("/")
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

    @app.errorhandler(CSRFError)
    def handle_csrf_error(e):
        back_url = safe_referrer_target("/")
        flash("Your form session expired. Reload the page and submit the upload again.", "warning")
        return redirect(back_url)

    # ── Health check endpoint (no auth, no redirect) ─────────────
    @app.route("/health")
    def health_check():
        return jsonify({"status": "ok"}), 200

    return app
