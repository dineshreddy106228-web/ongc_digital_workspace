"""
Application entry point.
Usage: python run.py
"""

import os
from app import create_app

app = create_app()


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5000"))
    debug = _as_bool(os.environ.get("FLASK_DEBUG"), default=False)
    app.run(host=host, port=port, debug=debug, use_reloader=debug)
