"""
Application entry point.
Usage: python run.py
"""

import os
import sys


MIN_PYTHON = (3, 9)
MAX_PYTHON_EXCLUSIVE = (3, 13)


def _assert_supported_python() -> None:
    version = sys.version_info[:3]
    if MIN_PYTHON <= version < MAX_PYTHON_EXCLUSIVE:
        return

    min_label = ".".join(str(part) for part in MIN_PYTHON)
    max_label = ".".join(str(part) for part in MAX_PYTHON_EXCLUSIVE)
    current_label = ".".join(str(part) for part in version)
    raise RuntimeError(
        "Unsupported Python version for ONGC Digital Workspace: "
        f"{current_label}. Use Python >= {min_label} and < {max_label}. "
        "If your venv was created with Python 3.13+ or 3.14, rebuild it with "
        "a supported interpreter such as Python 3.11."
    )


_assert_supported_python()

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
