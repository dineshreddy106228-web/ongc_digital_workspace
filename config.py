from __future__ import annotations

"""Application configuration loaded from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()


def _as_bool(value: str | None, default: bool = False) -> bool:
    """Parse common truthy environment variable strings into booleans."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_database_url(value: str | None) -> str | None:
    """Normalize Railway/Postgres URLs while keeping local MySQL DB_* support."""
    if value is None:
        return None
    clean_value = value.strip()
    if not clean_value:
        return None
    if clean_value.startswith("postgres://"):
        return "postgresql+psycopg://" + clean_value[len("postgres://") :]
    if clean_value.startswith("postgresql://") and "+psycopg" not in clean_value:
        return "postgresql+psycopg://" + clean_value[len("postgresql://") :]
    return clean_value


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
    DB_SSL_MODE = os.environ.get("DB_SSL_MODE") or os.environ.get("MYSQL_SSL_MODE")
    DB_SSL_CA = os.environ.get("DB_SSL_CA") or os.environ.get("MYSQL_SSL_CA")

    DATABASE_URL = _normalize_database_url(
        os.environ.get("SQLALCHEMY_DATABASE_URI") or os.environ.get("DATABASE_URL")
    )
    SQLALCHEMY_DATABASE_URI = DATABASE_URL or (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": 280,
        "pool_timeout": int(os.environ.get("DB_POOL_TIMEOUT", "30")),
        # A single low-traffic sync web worker needs at most one active
        # connection; retain one spare without allocating a large pool per
        # process.  Environment overrides remain available for future scale.
        "pool_size": int(os.environ.get("DB_POOL_SIZE", "2")),
        "max_overflow": int(os.environ.get("DB_MAX_OVERFLOW", "1")),
    }

    # ── Cache ────────────────────────────────────────────────────
    CACHE_TYPE = os.environ.get("CACHE_TYPE", "SimpleCache")
    CACHE_DEFAULT_TIMEOUT = int(os.environ.get("CACHE_DEFAULT_TIMEOUT", "300"))
    # SimpleCache resides in each worker.  Keep a firm cap on cached objects
    # so per-user memoized values cannot accumulate indefinitely.
    CACHE_THRESHOLD = int(os.environ.get("CACHE_THRESHOLD", "100"))

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
    REMEMBER_COOKIE_SAMESITE = "Lax"
    # The CSC UI permits documents up to 100 MB, so 128 MB leaves operational
    # headroom while avoiding accidental 256 MB request bodies in memory.
    MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", str(128 * 1024 * 1024)))
    MSDS_STORAGE_DIR = os.environ.get("MSDS_STORAGE_DIR", "storage/msds")
    MSDS_MAX_UPLOAD_BYTES = int(os.environ.get("MSDS_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024)))
    DB_COMMAND_TIMEOUT_SECONDS = int(os.environ.get("DB_COMMAND_TIMEOUT_SECONDS", "600"))
    MYSQL_BIN = os.environ.get("MYSQL_BIN", "mysql")
    MYSQLDUMP_BIN = os.environ.get("MYSQLDUMP_BIN", "mysqldump")

    # ── Retained daily backups ───────────────────────────────────
    # Backups remain in application storage, so deploy this directory on a
    # persistent volume when the hosting platform has ephemeral local disks.
    AUTO_BACKUP_ENABLED = _as_bool(os.environ.get("AUTO_BACKUP_ENABLED"), default=True)
    AUTO_BACKUP_DIR = os.environ.get("AUTO_BACKUP_DIR", "storage/backups")
    AUTO_BACKUP_RETENTION_DAYS = int(os.environ.get("AUTO_BACKUP_RETENTION_DAYS", "15"))
    AUTO_BACKUP_TIMEZONE = os.environ.get("AUTO_BACKUP_TIMEZONE", "Asia/Kolkata")
    AUTO_BACKUP_CHECK_INTERVAL_SECONDS = int(
        os.environ.get("AUTO_BACKUP_CHECK_INTERVAL_SECONDS", "3600")
    )

    # ── Imported workbook rollback window ────────────────────────
    # Parsed operational records and audit metadata remain in the database.
    # The source workbook bytes are retained only long enough for controlled
    # rollback/download of a recent import.
    AUDIT_WORKBOOK_RETENTION_DAYS = int(
        os.environ.get("AUDIT_WORKBOOK_RETENTION_DAYS", "15")
    )
    AUDIT_WORKBOOK_RETENTION_ENABLED = _as_bool(
        os.environ.get("AUDIT_WORKBOOK_RETENTION_ENABLED"), default=True
    )
    AUDIT_WORKBOOK_RETENTION_CHECK_INTERVAL_SECONDS = int(
        os.environ.get("AUDIT_WORKBOOK_RETENTION_CHECK_INTERVAL_SECONDS", "3600")
    )

    # ── Login hardening ──────────────────────────────────────────
    LOGIN_RATE_LIMIT_ENABLED = _as_bool(os.environ.get("LOGIN_RATE_LIMIT_ENABLED"), default=True)
    LOGIN_RATE_LIMIT_MAX_ATTEMPTS = int(os.environ.get("LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "8"))
    LOGIN_RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("LOGIN_RATE_LIMIT_WINDOW_SECONDS", "300"))
    LOGIN_RATE_LIMIT_LOCK_SECONDS = int(os.environ.get("LOGIN_RATE_LIMIT_LOCK_SECONDS", "300"))

    # ── Password reset requests ──────────────────────────────────
    # A temporary password is a stop-gap the user is expected to replace on
    # the next sign-in, so its life is measured in hours, not days.
    PASSWORD_RESET_TEMP_TTL_HOURS = int(
        os.environ.get("PASSWORD_RESET_TEMP_TTL_HOURS", "3")
    )
    # An unhandled request goes stale; expiring it keeps the admin queue
    # showing live work and lets the user raise a fresh one.
    PASSWORD_RESET_REQUEST_TTL_HOURS = int(
        os.environ.get("PASSWORD_RESET_REQUEST_TTL_HOURS", "24")
    )
    PASSWORD_MIN_LENGTH = int(os.environ.get("PASSWORD_MIN_LENGTH", "8"))
    # Shortlist an admin can pick from when dictating a password over the
    # phone.  These values are public knowledge by design — they are only ever
    # safe because approval requires verified identity and expires in hours,
    # and because no user may keep one as their own password.
    PASSWORD_RESET_PRESETS = [
        entry.strip()
        for entry in os.environ.get(
            "PASSWORD_RESET_PRESETS",
            "Password@123,Ongc@12345,Welcome@123",
        ).split(",")
        if entry.strip()
    ]

    # ── CSP rollout (report-only by default to avoid behavior changes) ─────
    CSP_ENABLED = _as_bool(os.environ.get("CSP_ENABLED"), default=True)
    CSP_REPORT_ONLY = _as_bool(os.environ.get("CSP_REPORT_ONLY"), default=False)
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
    APP_ENVIRONMENT_NAME = (
        os.environ.get("APP_ENVIRONMENT_NAME")
        or os.environ.get("RAILWAY_ENVIRONMENT_NAME")
        or os.environ.get("RAILWAY_ENVIRONMENT")
        or FLASK_ENV
    )

    # ── Feature flags ────────────────────────────────────────────
    # These flags control which business modules are registered and exposed.
    ENABLE_OFFICE_MANAGEMENT = _as_bool(
        os.environ.get("ENABLE_OFFICE_MANAGEMENT"),
        default=True,
    )
    ENABLE_INVENTORY = _as_bool(
        os.environ.get("ENABLE_INVENTORY"),
        default=True,
    )
    ENABLE_CSC = _as_bool(
        os.environ.get("ENABLE_CSC"),
        default=True,
    )
    ENABLE_QUALITY_CONTROL = _as_bool(
        os.environ.get("ENABLE_QUALITY_CONTROL"),
        default=True,
    )
