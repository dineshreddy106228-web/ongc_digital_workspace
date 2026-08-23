"""Referrer handling for the handlers that bounce a user back to the previous page."""
from __future__ import annotations

from pathlib import Path
import sys

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.utils.request_meta import safe_referrer_target


def _target(referrer: str | None, default: str = "/fallback") -> str:
    app = Flask(__name__)
    headers = {"Referer": referrer} if referrer is not None else {}
    with app.test_request_context("/upload", base_url="https://workspace.example", headers=headers):
        return safe_referrer_target(default)


def test_same_host_referrer_is_reduced_to_a_path() -> None:
    assert _target("https://workspace.example/tasks?status=OPEN") == "/tasks?status=OPEN"


def test_relative_referrer_is_preserved() -> None:
    assert _target("/csc/msds") == "/csc/msds"


def test_cross_host_referrer_falls_back_to_the_default() -> None:
    # A cross-site POST fails CSRF and reaches the error handler carrying the
    # attacker's Referer, so honouring it would be an open redirect.
    assert _target("https://attacker.example/landing") == "/fallback"


def test_protocol_relative_referrer_falls_back_to_the_default() -> None:
    assert _target("//attacker.example/landing") == "/fallback"


def test_non_http_scheme_falls_back_to_the_default() -> None:
    assert _target("javascript:alert(1)") == "/fallback"


def test_missing_referrer_falls_back_to_the_default() -> None:
    assert _target(None) == "/fallback"
