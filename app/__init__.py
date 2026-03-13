"""Application factory for ONGC Digital Workspace."""

import secrets
from flask import Flask, g
from config import Config
from app.extensions import db, migrate, login_manager, csrf
from app.features import is_module_enabled, register_feature_blueprints


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

    # ── Flask-Login user loader ──────────────────────────────────
    from app.models.user import User

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    # ── Register blueprints ──────────────────────────────────────
    from app.auth import auth_bp
    from app.main import main_bp
    from app.admin import admin_bp
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")

    # Business modules are feature-flagged so production can expose only approved areas.
    register_feature_blueprints(app)

    # ── Register CLI commands ────────────────────────────────────
    from app.cli import register_cli
    register_cli(app)

    # ── Inject common template context ───────────────────────────
    @app.context_processor
    def inject_globals():
        return dict(
            app_name=app.config["APP_NAME"],
            csp_nonce=lambda: getattr(g, "csp_nonce", ""),
            is_module_enabled=lambda module_code: is_module_enabled(module_code, app),
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
        from flask import render_template_string
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

    return app
