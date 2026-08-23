"""Helpers for safe request metadata extraction."""

from __future__ import annotations

import ipaddress
from urllib.parse import urlparse, urlunparse
from flask import request

MAX_IP_LEN = 45
MAX_USER_AGENT_LEN = 500


def safe_referrer_target(default: str) -> str:
    """
    Return the referring page only when it points back at this host.

    Handlers that bounce the user "back" run on requests an attacker can trigger —
    a cross-site POST fails CSRF and lands in the error handler carrying the
    attacker's Referer — so an unfiltered referrer turns those handlers into an
    open redirect.  Same-host referrers are reduced to a path so the redirect can
    never leave the application.
    """
    referrer = request.referrer or ""
    if not referrer:
        return default

    parsed = urlparse(referrer)
    if parsed.scheme and parsed.scheme not in ("http", "https"):
        return default
    if parsed.netloc and parsed.netloc != request.host:
        return default

    return urlunparse(("", "", parsed.path or "/", parsed.params, parsed.query, parsed.fragment))


def get_client_ip() -> str:
    """
    Return a normalized client IP.

    - Prefers the first value from X-Forwarded-For when present.
    - Falls back to request.remote_addr.
    - Validates with ipaddress and caps to MAX_IP_LEN.
    """
    if not request:
        return ""

    raw = request.headers.get("X-Forwarded-For", "") or ""
    if raw:
        # Railway edge proxy appends the real client IP as the last entry.
        # Using the rightmost value prevents spoofing via a forged first entry.
        candidate = raw.split(",")[-1].strip()
    else:
        candidate = (request.remote_addr or "").strip()
    if not candidate:
        return ""

    try:
        return str(ipaddress.ip_address(candidate))[:MAX_IP_LEN]
    except ValueError:
        # Keep behavior resilient even if proxies send malformed values.
        return candidate[:MAX_IP_LEN]


def get_user_agent() -> str:
    """Return request user-agent string with a defensive length cap."""
    if not request:
        return ""
    return (request.user_agent.string or "")[:MAX_USER_AGENT_LEN]
