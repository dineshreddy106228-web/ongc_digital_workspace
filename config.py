"""Application configuration loaded from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Config:
    """Central configuration – values come from .env, never hard-coded."""

    # ── Flask core ───────────────────────────────────────────────
    SECRET_KEY = os.environ.get("SECRET_KEY", "fallback-insecure-key-change-me")
    FLASK_ENV = os.environ.get("FLASK_ENV", "production")
    DEBUG = _as_bool(os.environ.get("FLASK_DEBUG"), default=FLASK_ENV == "development")
    TESTING = _as_bool(os.environ.get("TESTING"), default=False)

    # ── Database (MySQL via PyMySQL) ─────────────────────────────
    DB_HOST = os.environ.get("DB_HOST", "localhost")
    DB_PORT = os.environ.get("DB_PORT", "3306")
    DB_NAME = os.environ.get("DB_NAME", "ongc_workspace")
    DB_USER = os.environ.get("DB_USER", "root")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

    SQLALCHEMY_DATABASE_URI = (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": 280,
        "pool_timeout": int(os.environ.get("DB_POOL_TIMEOUT", "30")),
    }

    # ── Security headers / session hardening ─────────────────────
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_DURATION = 3600  # 1 hour
    # Set SESSION_COOKIE_SECURE = True when serving over HTTPS
    SESSION_COOKIE_SECURE = _as_bool(
        os.environ.get("SESSION_COOKIE_SECURE"),
        default=FLASK_ENV != "development",
    )
    REMEMBER_COOKIE_SECURE = SESSION_COOKIE_SECURE
    MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", str(2 * 1024 * 1024)))

    # ── Login hardening ──────────────────────────────────────────
    LOGIN_RATE_LIMIT_ENABLED = _as_bool(os.environ.get("LOGIN_RATE_LIMIT_ENABLED"), default=True)
    LOGIN_RATE_LIMIT_MAX_ATTEMPTS = int(os.environ.get("LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "8"))
    LOGIN_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("LOGIN_RATE_LIMIT_WINDOW_SECONDS", "300"))
    LOGIN_RATE_LIMIT_LOCK_SECONDS = int(os.environ.get("LOGIN_RATE_LIMIT_LOCK_SECONDS", "300"))

    # ── CSP rollout (report-only by default to avoid behavior changes) ─────
    CSP_ENABLED = _as_bool(os.environ.get("CSP_ENABLED"), default=True)
    CSP_REPORT_ONLY = _as_bool(os.environ.get("CSP_REPORT_ONLY"), default=True)
    CSP_REPORT_URI = os.environ.get("CSP_REPORT_URI", "").strip() or None

    # ── WTF CSRF ─────────────────────────────────────────────────
    WTF_CSRF_ENABLED = True
    WTF_CSRF_TIME_LIMIT = 3600

    # ── Bootstrap admin (used only by seed command) ──────────────
    BOOTSTRAP_ADMIN_USERNAME = os.environ.get("BOOTSTRAP_ADMIN_USERNAME", "superadmin")
    BOOTSTRAP_ADMIN_EMAIL = os.environ.get("BOOTSTRAP_ADMIN_EMAIL", "admin@ongc.example.com")
    BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("BOOTSTRAP_ADMIN_PASSWORD", "ChangeMe@First1")

    # ── App metadata ─────────────────────────────────────────────
    APP_NAME = os.environ.get("APP_NAME", "ONGC Digital Workspace")
    PILOT_OFFICE_CODE = os.environ.get("PILOT_OFFICE_CODE", "CORP_CHEM")
