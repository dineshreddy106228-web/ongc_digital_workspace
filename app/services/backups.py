"""Database backup and restore helpers for ONGC Digital Workspace."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import logging
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
from urllib.parse import parse_qs, unquote, urlparse

from flask import current_app


SUPPORTED_BACKUP_EXTENSIONS = (".sql", ".sql.gz")
SQL_PREVIEW_LINE_LIMIT = 5
logger = logging.getLogger(__name__)


class BackupError(RuntimeError):
    """Raised when backup creation, validation, or restore fails."""


@dataclass(frozen=True)
class DatabaseConnectionSettings:
    """Resolved database credentials for command-line MySQL utilities."""

    host: str
    port: int
    database: str
    username: str
    password: str
    ssl_mode: str | None = None
    ssl_ca: str | None = None


@dataclass(frozen=True)
class BackupArtifact:
    """Temporary backup file returned to the admin download response."""

    temp_path: Path
    download_name: str

    @property
    def size_bytes(self) -> int:
        return self.temp_path.stat().st_size

    def cleanup(self) -> None:
        try:
            self.temp_path.unlink(missing_ok=True)
        except OSError:
            logger.warning(
                "Failed to delete temporary backup file: %s",
                self.temp_path,
                exc_info=True,
            )


def _clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _first_query_value(query: dict[str, list[str]], *keys: str) -> str | None:
    for key in keys:
        values = query.get(key)
        if not values:
            continue
        candidate = _clean_value(values[0])
        if candidate is not None:
            return candidate
    return None


def _parse_database_url(raw_url: str | None) -> DatabaseConnectionSettings | None:
    url = _clean_value(raw_url)
    if url is None:
        return None

    parsed = urlparse(url)
    if "mysql" not in (parsed.scheme or ""):
        return None
    if not parsed.hostname or not parsed.path:
        return None

    database = parsed.path.lstrip("/")
    if not database:
        return None

    query = parse_qs(parsed.query)
    ssl_mode = _first_query_value(query, "ssl-mode", "ssl_mode")
    ssl_ca = _first_query_value(query, "ssl-ca", "ssl_ca")

    return DatabaseConnectionSettings(
        host=parsed.hostname,
        port=parsed.port or 3306,
        database=database,
        username=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        ssl_mode=ssl_mode,
        ssl_ca=ssl_ca,
    )


def resolve_database_connection_settings() -> DatabaseConnectionSettings:
    """Resolve DB credentials from explicit env vars, Railway vars, or app config."""

    env_discrete = {
        "host": _clean_value(os.environ.get("DB_HOST")),
        "port": _clean_value(os.environ.get("DB_PORT")),
        "database": _clean_value(os.environ.get("DB_NAME")),
        "username": _clean_value(os.environ.get("DB_USER")),
        "password": os.environ.get("DB_PASSWORD", ""),
        "ssl_mode": _clean_value(os.environ.get("DB_SSL_MODE")),
        "ssl_ca": _clean_value(os.environ.get("DB_SSL_CA")),
    }
    if env_discrete["host"] and env_discrete["database"] and env_discrete["username"]:
        return DatabaseConnectionSettings(
            host=env_discrete["host"],
            port=int(env_discrete["port"] or 3306),
            database=env_discrete["database"],
            username=env_discrete["username"],
            password=env_discrete["password"],
            ssl_mode=env_discrete["ssl_mode"],
            ssl_ca=env_discrete["ssl_ca"],
        )

    railway_discrete = {
        "host": _clean_value(os.environ.get("MYSQLHOST")),
        "port": _clean_value(os.environ.get("MYSQLPORT")),
        "database": _clean_value(os.environ.get("MYSQLDATABASE")),
        "username": _clean_value(os.environ.get("MYSQLUSER")),
        "password": os.environ.get("MYSQLPASSWORD", ""),
        "ssl_mode": _clean_value(os.environ.get("MYSQL_SSL_MODE")),
        "ssl_ca": _clean_value(os.environ.get("MYSQL_SSL_CA")),
    }
    if railway_discrete["host"] and railway_discrete["database"] and railway_discrete["username"]:
        return DatabaseConnectionSettings(
            host=railway_discrete["host"],
            port=int(railway_discrete["port"] or 3306),
            database=railway_discrete["database"],
            username=railway_discrete["username"],
            password=railway_discrete["password"],
            ssl_mode=railway_discrete["ssl_mode"],
            ssl_ca=railway_discrete["ssl_ca"],
        )

    for url_value in (
        os.environ.get("MYSQL_URL"),
        os.environ.get("DATABASE_URL"),
        current_app.config.get("SQLALCHEMY_DATABASE_URI"),
    ):
        parsed = _parse_database_url(url_value)
        if parsed is not None and parsed.username:
            return parsed

    config_values = current_app.config
    host = _clean_value(config_values.get("DB_HOST"))
    database = _clean_value(config_values.get("DB_NAME"))
    username = _clean_value(config_values.get("DB_USER"))
    if host and database and username:
        return DatabaseConnectionSettings(
            host=host,
            port=int(config_values.get("DB_PORT") or 3306),
            database=database,
            username=username,
            password=str(config_values.get("DB_PASSWORD") or ""),
            ssl_mode=_clean_value(config_values.get("DB_SSL_MODE")),
            ssl_ca=_clean_value(config_values.get("DB_SSL_CA")),
        )

    raise BackupError(
        "Database credentials could not be resolved from environment or app configuration."
    )


def get_runtime_environment_name() -> str:
    return (
        _clean_value(current_app.config.get("APP_ENVIRONMENT_NAME"))
        or _clean_value(os.environ.get("APP_ENVIRONMENT_NAME"))
        or _clean_value(os.environ.get("RAILWAY_ENVIRONMENT_NAME"))
        or _clean_value(os.environ.get("RAILWAY_ENVIRONMENT"))
        or _clean_value(current_app.config.get("FLASK_ENV"))
        or "production"
    )


def build_backup_filename() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
    return f"ongc_workspace_backup_{timestamp}.sql.gz"


def _create_temp_path(suffix: str) -> Path:
    fd, temp_path = tempfile.mkstemp(prefix="ongc-backup-", suffix=suffix)
    os.close(fd)
    return Path(temp_path)


def _write_client_defaults_file(settings: DatabaseConnectionSettings) -> Path:
    fd, temp_path = tempfile.mkstemp(prefix="ongc-mysql-client-", suffix=".cnf")
    os.fchmod(fd, stat.S_IRUSR | stat.S_IWUSR)

    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write("[client]\n")
        handle.write(f"user={settings.username}\n")
        handle.write(f"password={settings.password.replace(chr(10), '')}\n")
        handle.write(f"host={settings.host}\n")
        handle.write(f"port={settings.port}\n")
        if settings.ssl_mode:
            handle.write(f"ssl-mode={settings.ssl_mode}\n")
        if settings.ssl_ca:
            handle.write(f"ssl-ca={settings.ssl_ca}\n")

    return Path(temp_path)


def _command_timeout_seconds() -> int | None:
    timeout = int(current_app.config.get("DB_COMMAND_TIMEOUT_SECONDS", 600))
    return timeout if timeout > 0 else None


def _format_command_failure(stderr: bytes | str | None) -> str:
    if stderr is None:
        return "No stderr output was captured."
    if isinstance(stderr, bytes):
        message = stderr.decode("utf-8", errors="replace").strip()
    else:
        message = stderr.strip()
    return message or "No stderr output was captured."


def create_database_backup() -> BackupArtifact:
    """Run mysqldump into a temporary gzip file and return the artifact."""

    settings = resolve_database_connection_settings()
    defaults_file = _write_client_defaults_file(settings)
    raw_dump_path = _create_temp_path(".sql")
    compressed_dump_path = _create_temp_path(".sql.gz")
    dump_command = [
        current_app.config.get("MYSQLDUMP_BIN", "mysqldump"),
        f"--defaults-extra-file={defaults_file}",
        "--protocol=TCP",
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
        "--no-tablespaces",
        "--routines",
        "--events",
        "--default-character-set=utf8mb4",
        settings.database,
    ]
    backup_created = False

    try:
        with raw_dump_path.open("wb") as dump_handle:
            result = subprocess.run(
                dump_command,
                stdout=dump_handle,
                stderr=subprocess.PIPE,
                timeout=_command_timeout_seconds(),
                check=False,
            )
        if result.returncode != 0:
            raise BackupError(
                "mysqldump failed. "
                f"{_format_command_failure(result.stderr)}"
            )

        if raw_dump_path.stat().st_size == 0:
            raise BackupError("mysqldump completed but produced an empty backup file.")

        with raw_dump_path.open("rb") as source, gzip.open(compressed_dump_path, "wb") as target:
            shutil.copyfileobj(source, target)
        backup_created = True
    except FileNotFoundError as exc:
        raise BackupError(
            "mysqldump is not installed or not available on PATH. "
            "Install the MySQL client package in the deployment image."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BackupError(
            "Backup export timed out before mysqldump completed."
        ) from exc
    finally:
        defaults_file.unlink(missing_ok=True)
        raw_dump_path.unlink(missing_ok=True)
        if not backup_created:
            compressed_dump_path.unlink(missing_ok=True)

    return BackupArtifact(
        temp_path=compressed_dump_path,
        download_name=build_backup_filename(),
    )


def _is_supported_backup_file(file_path: Path) -> bool:
    return file_path.name.endswith(".sql") or file_path.name.endswith(".sql.gz")


def validate_backup_file(file_path: str | Path) -> dict:
    """Verify a backup file exists, can be read, and looks like SQL."""

    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise BackupError(f"Backup file does not exist: {path}")
    if not path.is_file():
        raise BackupError(f"Backup path is not a file: {path}")
    if not _is_supported_backup_file(path):
        raise BackupError("Backup file must end with .sql or .sql.gz.")

    open_fn = gzip.open if path.name.endswith(".gz") else open
    preview_lines: list[str] = []
    try:
        with open_fn(path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                preview_lines.append(stripped)
                if len(preview_lines) >= SQL_PREVIEW_LINE_LIMIT:
                    break
    except OSError as exc:
        raise BackupError(f"Backup file could not be read: {exc}") from exc

    if not preview_lines:
        raise BackupError("Backup file is empty or does not contain readable SQL text.")

    sql_markers = (
        "-- MySQL dump",
        "CREATE TABLE",
        "INSERT INTO",
        "DROP TABLE",
        "LOCK TABLES",
        "UNLOCK TABLES",
        "/*!",
    )
    sql_like = any(marker in line for line in preview_lines for marker in sql_markers)
    if not sql_like:
        raise BackupError(
            "Backup file was readable, but the preview did not look like a MySQL dump."
        )

    return {
        "path": str(path),
        "compressed": path.name.endswith(".gz"),
        "size_bytes": path.stat().st_size,
        "preview_lines": preview_lines,
    }


def restore_database_backup(file_path: str | Path) -> dict:
    """Restore a .sql or .sql.gz file into the configured MySQL database."""

    validation = validate_backup_file(file_path)
    settings = resolve_database_connection_settings()
    defaults_file = _write_client_defaults_file(settings)
    restore_command = [
        current_app.config.get("MYSQL_BIN", "mysql"),
        f"--defaults-extra-file={defaults_file}",
        "--protocol=TCP",
        "--default-character-set=utf8mb4",
        settings.database,
    ]

    try:
        open_fn = gzip.open if validation["compressed"] else open
        with open_fn(validation["path"], "rb") as source:
            result = subprocess.run(
                restore_command,
                stdin=source,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=_command_timeout_seconds(),
                check=False,
            )
        if result.returncode != 0:
            raise BackupError(
                "mysql restore failed. "
                f"{_format_command_failure(result.stderr)}"
            )
    except FileNotFoundError as exc:
        raise BackupError(
            "mysql client is not installed or not available on PATH. "
            "Install the MySQL client package in the deployment image or admin shell."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BackupError(
            "Restore timed out before the mysql client completed."
        ) from exc
    finally:
        defaults_file.unlink(missing_ok=True)

    return {
        "database": settings.database,
        "host": settings.host,
        "path": validation["path"],
        "compressed": validation["compressed"],
        "size_bytes": validation["size_bytes"],
    }
