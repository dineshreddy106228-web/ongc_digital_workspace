"""Shared utilities for OCC HCC tracker hardening."""

from __future__ import annotations

import base64
import binascii
import hashlib
import html
import hmac
import json
import os
import re
import secrets
import time
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, MutableMapping

import pandas as pd

PASSWORD_MIN_LENGTH = 10
PBKDF2_ITERATIONS = 240000
_SESSION_TOKEN_ALGO = "sha256"
_MAX_TEXT_DEFAULT = 2000


class ValidationError(ValueError):
    """Raised when input validation fails."""



def get_utc_now() -> datetime:
    """Return timezone-aware current UTC time."""
    return datetime.now(timezone.utc)



def utc_timestamp_naive() -> pd.Timestamp:
    """Return current UTC time as naive pandas Timestamp for DataFrame compatibility."""
    return pd.Timestamp(get_utc_now()).tz_localize(None)



def to_iso_datetime(value: Any) -> str | None:
    """Normalize datetime-like values to ISO8601 string."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        else:
            value = value.tz_convert("UTC")
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime().isoformat()



def parse_datetime_series_utc(series: pd.Series) -> pd.Series:
    """Parse a Series as UTC datetimes and convert to naive UTC for app calculations."""
    # Pandas can fail vectorized parsing when ISO strings mix fractional seconds and non-fractional seconds.
    # Use format="mixed" to parse element-wise while still remaining fast enough for our dataset sizes.
    parsed = pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    return parsed.dt.tz_convert("UTC").dt.tz_localize(None)



def _normalize_whitespace(value: str) -> str:
    return " ".join(value.strip().split())



def sanitize_text(value: Any, max_length: int = _MAX_TEXT_DEFAULT) -> str:
    """Sanitize text by removing control chars and bounding length."""
    if value is None:
        text = ""
    else:
        try:
            text = "" if pd.isna(value) else str(value)
        except Exception:
            text = str(value)
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    text = _normalize_whitespace(text)
    if len(text) > max_length:
        raise ValidationError(f"Input exceeds max length {max_length}.")
    return text



def sanitize_multiline_text(value: Any, max_length: int = 8000) -> str:
    """Sanitize multi-line text while preserving line breaks."""
    if value is None:
        text = ""
    else:
        try:
            text = "" if pd.isna(value) else str(value)
        except Exception:
            text = str(value)
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    text = text.strip()
    if len(text) > max_length:
        raise ValidationError(f"Input exceeds max length {max_length}.")
    return text



def escape_html_text(value: Any, max_length: int = _MAX_TEXT_DEFAULT) -> str:
    """Sanitize and escape text for HTML contexts."""
    return html.escape(sanitize_text(value, max_length=max_length), quote=True)



def validate_base64_image(data: str, max_bytes: int = 2_000_000) -> bool:
    """Validate base64 payload for image rendering."""
    if not data or not isinstance(data, str):
        return False
    try:
        raw = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError):
        return False
    return 0 < len(raw) <= max_bytes



def decode_base64_image(data: str) -> bytes:
    """Decode a validated base64 image payload."""
    if not validate_base64_image(data):
        raise ValidationError("Invalid base64 image payload.")
    return base64.b64decode(data)



def validate_target_date(
    target_date: date,
    *,
    allow_past_dates: bool = False,
    today: date | None = None,
) -> tuple[bool, str]:
    """Validate target date policy."""
    ref_today = today or get_utc_now().date()
    if not allow_past_dates and target_date < ref_today:
        return False, "Target date cannot be in the past unless override is enabled."
    return True, ""



def validate_export_format(fmt: str, allowed: Iterable[str]) -> bool:
    """Validate export format against allowed values."""
    normalized = str(fmt or "").strip().lower()
    return normalized in {x.strip().lower() for x in allowed}



def hash_password(password: str, iterations: int = PBKDF2_ITERATIONS) -> str:
    """Hash password using PBKDF2-HMAC-SHA256."""
    if len(password or "") < PASSWORD_MIN_LENGTH:
        raise ValidationError(f"Password must be at least {PASSWORD_MIN_LENGTH} characters.")
    salt = secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    digest = derived.hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"



def verify_password(password: str, password_hash: str) -> bool:
    """Verify raw password against stored PBKDF2 hash."""
    if not password or not password_hash:
        return False
    try:
        algo, iters, salt, digest = password_hash.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iters)
    except (ValueError, TypeError):
        return False
    test_digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations).hex()
    return hmac.compare_digest(test_digest, digest)



def generate_session_token() -> str:
    """Generate random session token."""
    return secrets.token_urlsafe(48)



def hash_session_token(token: str) -> str:
    """Hash session token before persistence."""
    return hashlib.new(_SESSION_TOKEN_ALGO, token.encode("utf-8")).hexdigest()



def default_users_from_env() -> list[dict[str, str]]:
    """Load bootstrap users from OCC_DEFAULT_USERS_JSON or OCC_DEFAULT_ADMIN_* vars.

    JSON format:
    [
      {"username": "admin", "password": "StrongPassword123", "role": "Admin"}
    ]
    """
    raw_json = os.getenv("OCC_DEFAULT_USERS_JSON", "").strip()
    if raw_json:
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ValidationError("OCC_DEFAULT_USERS_JSON is not valid JSON.") from exc
        users: list[dict[str, str]] = []
        for item in payload:
            username = sanitize_text(item.get("username", ""), max_length=80)
            role = sanitize_text(item.get("role", "Viewer"), max_length=20)
            password = str(item.get("password", ""))
            if username and role and password:
                users.append({"username": username, "role": role, "password": password})
        return users

    admin_name = sanitize_text(os.getenv("OCC_DEFAULT_ADMIN_USERNAME", "admin"), max_length=80)
    admin_role = sanitize_text(os.getenv("OCC_DEFAULT_ADMIN_ROLE", "Admin"), max_length=20)
    admin_password = os.getenv("OCC_DEFAULT_ADMIN_PASSWORD", "ChangeMeNow123!")
    return [{"username": admin_name, "role": admin_role, "password": admin_password}]



def preferred_users_from_env() -> list[str]:
    """Load preferred users from env var OCC_PREFERRED_USERS (comma-separated)."""
    raw = os.getenv("OCC_PREFERRED_USERS", "")
    if not raw.strip():
        return []
    users = []
    for value in raw.split(","):
        clean = sanitize_text(value, max_length=80)
        if clean:
            users.append(clean)
    return users



def is_rate_limited(
    state: MutableMapping[str, Any],
    key: str,
    *,
    max_calls: int,
    window_seconds: int,
    now_epoch: float | None = None,
) -> bool:
    """Simple in-memory rate limiter for Streamlit session actions."""
    now_ts = now_epoch if now_epoch is not None else time.time()
    bucket_key = f"rate_limit::{key}"
    history = state.get(bucket_key, [])
    history = [ts for ts in history if (now_ts - ts) <= window_seconds]
    limited = len(history) >= max_calls
    if not limited:
        history.append(now_ts)
    state[bucket_key] = history
    return limited



def truncate_for_log(value: Any, max_length: int = 512) -> str:
    """Bound text length for logs and audit messages."""
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."



def require_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    """Validate required dataframe columns."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValidationError(f"Missing required columns: {', '.join(missing)}")



def safe_mapping_get(mapping: Mapping[str, Any], key: str, default: Any = None) -> Any:
    """Typed helper for mapping access."""
    if key not in mapping:
        return default
    return mapping[key]
