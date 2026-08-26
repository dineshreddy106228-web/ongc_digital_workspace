"""Database backup and restore helpers for ONGC Digital Workspace."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import gzip
import io
import json
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from urllib.parse import parse_qs, unquote, urlparse

from flask import current_app


SUPPORTED_BACKUP_EXTENSIONS = (".sql", ".sql.gz", ".tar.gz")
SQL_PREVIEW_LINE_LIMIT = 5
BUNDLE_MANIFEST_FILENAME = "manifest.json"
BUNDLE_EXCLUDED_FILES_MEMBER = "excluded_files.json"
# Tables whose rows are deliberately left out of the dump. Stored PDFs dominate the
# backup size and can be re-uploaded, so the schema is kept, the rows are not, and
# the manifest carries an inventory of what to re-upload after a restore.
DEFAULT_EXCLUDED_TABLE_DATA = ("msds_files",)
# How to inventory each excluded table: the columns worth keeping, and the byte-size
# column so the manifest can report how much was left out.
EXCLUDED_TABLE_INVENTORY = {
    "msds_files": {
        "columns": ("id", "material_code", "filename", "slot_code", "content_type", "file_size", "uploaded_at"),
        "size_column": "file_size",
        "order_by": "material_code, slot_code",
        "describes": "MSDS PDF documents",
    },
}
BUNDLE_DATABASE_MEMBER = "database.sql.gz"
BUNDLE_COMMITTEE_UPLOADS_DIR = "committee_uploads"
RETAINED_BACKUP_FILENAME_PREFIX = "ongc_workspace_daily_backup_"
RETAINED_BACKUP_FILENAME_PATTERN = re.compile(
    rf"^{RETAINED_BACKUP_FILENAME_PREFIX}(\d{{4}}-\d{{2}}-\d{{2}})\.tar\.gz$"
)
# These labels make the bundle manifest easier to audit.  The SQL export itself
# contains every application table except rows from BACKUP_EXCLUDE_TABLE_DATA.
DATABASE_BACKED_MODULES = (
    "task_management",
    "inventory_monitoring",
    "quality_control",
    "corporate_specifications_management",
)
BUNDLE_FORMAT_VERSION = 1
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


@dataclass(frozen=True)
class RetainedBackup:
    """A completed daily backup retained on application storage."""

    filename: str
    path: Path
    backup_date: date
    size_bytes: int
    created_at: datetime


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
    return f"ongc_workspace_backup_{timestamp}.tar.gz"


def build_database_backup_filename() -> str:
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


def _is_sql_backup_file(file_path: Path) -> bool:
    return file_path.name.endswith(".sql") or file_path.name.endswith(".sql.gz")


def _is_bundle_backup_file(file_path: Path) -> bool:
    return file_path.name.endswith(".tar.gz")


def _committee_upload_dir() -> Path:
    configured = current_app.config.get("COMMITTEE_UPLOAD_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return (Path(current_app.instance_path) / BUNDLE_COMMITTEE_UPLOADS_DIR).resolve()


def _count_files_in_directory(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for path in directory.rglob("*") if path.is_file())


def _read_sql_preview_from_binary_stream(binary_stream, *, compressed: bool) -> list[str]:
    preview_lines: list[str] = []
    wrapper = None
    archive_reader = None
    try:
        readable = binary_stream
        if compressed:
            archive_reader = gzip.GzipFile(fileobj=binary_stream, mode="rb")
            readable = archive_reader
        wrapper = io.TextIOWrapper(readable, encoding="utf-8", errors="replace")
        for line in wrapper:
            stripped = line.strip()
            if not stripped:
                continue
            preview_lines.append(stripped)
            if len(preview_lines) >= SQL_PREVIEW_LINE_LIMIT:
                break
    except OSError as exc:
        raise BackupError(f"Backup file could not be read: {exc}") from exc
    finally:
        if wrapper is not None:
            wrapper.detach()
        if archive_reader is not None:
            archive_reader.close()
    return preview_lines


def _validate_sql_preview_lines(preview_lines: list[str]) -> None:
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


def _safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    destination = destination.resolve()
    for member in archive.getmembers():
        # A bundle only ever holds the SQL payload, the manifests, and the committee
        # upload tree.  Anything else — symlinks, hard links, devices, FIFOs — is
        # rejected outright: the path check below cannot see through a link that the
        # archive itself creates during extraction, so a `link -> /` member followed
        # by `link/anything` would otherwise write outside the destination.
        if not (member.isfile() or member.isdir()):
            raise BackupError("Backup bundle contained an unsupported archive entry.")
        member_path = (destination / member.name).resolve()
        if destination not in member_path.parents and member_path != destination:
            raise BackupError("Backup bundle contained an unsafe archive path.")
    try:
        archive.extractall(destination, filter="data")
    except TypeError:
        # Python releases without the extraction filter (pre-3.11.4) fall back to the
        # member checks above, which already cover the cases `data` would reject.
        archive.extractall(destination)


def _replace_directory_contents(source: Path | None, destination: Path) -> None:
    destination_parent = destination.parent
    destination_parent.mkdir(parents=True, exist_ok=True)
    backup_path = destination_parent / f".{destination.name}_restore_backup"
    if backup_path.exists():
        if backup_path.is_dir():
            shutil.rmtree(backup_path, ignore_errors=True)
        else:
            backup_path.unlink(missing_ok=True)

    existing_destination = destination.exists()
    if existing_destination:
        destination.rename(backup_path)

    try:
        if source and source.exists():
            shutil.move(str(source), str(destination))
        else:
            destination.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        if destination.exists():
            if destination.is_dir():
                shutil.rmtree(destination, ignore_errors=True)
            else:
                destination.unlink(missing_ok=True)
        if backup_path.exists():
            backup_path.rename(destination)
        raise BackupError(f"Attachment restore failed: {exc}") from exc
    else:
        if backup_path.exists():
            if backup_path.is_dir():
                shutil.rmtree(backup_path, ignore_errors=True)
            else:
                backup_path.unlink(missing_ok=True)


def _resolve_mysql_client_binary(
    configured_name: str | None,
    *fallback_names: str,
) -> str:
    """Return the first available MySQL/MariaDB client binary on PATH."""
    candidates: list[str] = []
    primary = (configured_name or "").strip()
    if primary:
        candidates.append(primary)
    candidates.extend(name for name in fallback_names if name and name not in candidates)

    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved

    raise FileNotFoundError(", ".join(candidates) or "mysql client")


def get_excluded_table_data() -> tuple[str, ...]:
    """Tables dumped as schema only. Configurable, defaulting to the stored-PDF table."""
    configured = current_app.config.get("BACKUP_EXCLUDE_TABLE_DATA", DEFAULT_EXCLUDED_TABLE_DATA)
    if isinstance(configured, str):
        configured = [part.strip() for part in configured.split(",")]
    return tuple(name for name in (configured or ()) if str(name).strip())


def collect_excluded_table_inventory(table_names: tuple[str, ...]) -> dict:
    """List the rows each excluded table holds, so a restore knows what to re-upload.

    Never fatal: a table that is missing or unreadable is reported and skipped rather
    than failing the backup.
    """
    from sqlalchemy import text

    from app.extensions import db

    inventory: dict = {}
    for table_name in table_names:
        spec = EXCLUDED_TABLE_INVENTORY.get(table_name)
        if spec is None:
            inventory[table_name] = {"rows": [], "row_count": 0, "note": "No inventory columns configured."}
            continue
        columns = ", ".join(f"`{column}`" for column in spec["columns"])
        try:
            result = db.session.execute(
                text(f"SELECT {columns} FROM `{table_name}` ORDER BY {spec['order_by']}")
            )
            rows = [
                {key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in row.items()}
                for row in result.mappings()
            ]
        except Exception as exc:  # noqa: BLE001 - inventory must never break a backup
            db.session.rollback()
            logger.warning("Could not inventory excluded table %s: %s", table_name, exc)
            inventory[table_name] = {"rows": [], "row_count": 0, "note": f"Inventory unavailable: {exc}"}
            continue
        size_column = spec.get("size_column")
        inventory[table_name] = {
            "describes": spec["describes"],
            "row_count": len(rows),
            "omitted_bytes": sum(int(row.get(size_column) or 0) for row in rows) if size_column else None,
            "rows": rows,
        }
    return inventory


def create_database_backup() -> BackupArtifact:
    """Run mysqldump into a temporary gzip file and return the artifact.

    Tables listed in ``BACKUP_EXCLUDE_TABLE_DATA`` are dumped as schema only, in a
    second pass, so their rows stay out of the file while the table still exists
    after a restore.
    """

    settings = resolve_database_connection_settings()
    defaults_file = _write_client_defaults_file(settings)
    raw_dump_path = _create_temp_path(".sql")
    compressed_dump_path = _create_temp_path(".sql.gz")
    excluded_tables = get_excluded_table_data()
    dump_binary = _resolve_mysql_client_binary(
        current_app.config.get("MYSQLDUMP_BIN", "mysqldump"),
        "mysqldump",
        "mariadb-dump",
    )
    base_command = [
        dump_binary,
        f"--defaults-extra-file={defaults_file}",
        "--protocol=TCP",
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
        "--no-tablespaces",
        "--default-character-set=utf8mb4",
    ]
    # On a GTID-enabled server mysqldump emits SET @@GLOBAL.GTID_PURGED, which makes the
    # dump unrestorable into any server that already has GTIDs executed. MariaDB's client
    # has no such option, so only pass it to mysqldump proper.
    if "mariadb" not in Path(dump_binary).name.lower():
        base_command.append("--set-gtid-purged=OFF")
    # Pass 1: everything except the excluded tables, including routines and events.
    dump_command = [
        *base_command,
        "--routines",
        "--events",
        *[f"--ignore-table={settings.database}.{table}" for table in excluded_tables],
        settings.database,
    ]
    # Pass 2: the excluded tables' schema, so a restore recreates them empty.
    schema_command = (
        [*base_command, "--no-data", settings.database, *excluded_tables]
        if excluded_tables
        else None
    )
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
            if schema_command is not None:
                dump_handle.write(
                    f"\n--\n-- Schema only for excluded tables: {', '.join(excluded_tables)}\n"
                    "-- Row data was deliberately omitted; see the bundle manifest.\n--\n\n".encode()
                )
                dump_handle.flush()
                schema_result = subprocess.run(
                    schema_command,
                    stdout=dump_handle,
                    stderr=subprocess.PIPE,
                    timeout=_command_timeout_seconds(),
                    check=False,
                )
                if schema_result.returncode != 0:
                    raise BackupError(
                        "mysqldump failed while writing the excluded-table schema. "
                        f"{_format_command_failure(schema_result.stderr)}"
                    )

        if raw_dump_path.stat().st_size == 0:
            raise BackupError("mysqldump completed but produced an empty backup file.")

        with raw_dump_path.open("rb") as source, gzip.open(compressed_dump_path, "wb") as target:
            shutil.copyfileobj(source, target)
        backup_created = True
    except FileNotFoundError as exc:
        raise BackupError(
            "No MySQL dump client was found on PATH. "
            "Install a MySQL/MariaDB client package in the deployment image."
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
        download_name=build_database_backup_filename(),
    )


def _is_supported_backup_file(file_path: Path) -> bool:
    return _is_sql_backup_file(file_path) or _is_bundle_backup_file(file_path)


def _add_bytes_member(archive: tarfile.TarFile, name: str, payload: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(payload)
    info.mtime = int(datetime.now(timezone.utc).timestamp())
    archive.addfile(info, io.BytesIO(payload))


def create_full_backup_bundle() -> BackupArtifact:
    """Create a tar.gz bundle containing the SQL backup and filesystem uploads."""

    excluded_tables = get_excluded_table_data()
    inventory = collect_excluded_table_inventory(excluded_tables)
    database_artifact = create_database_backup()
    bundle_path = _create_temp_path(".tar.gz")
    upload_dir = _committee_upload_dir()
    upload_dir_exists = upload_dir.exists()
    upload_file_count = _count_files_in_directory(upload_dir)
    manifest = {
        "format": "ongc_workspace_full_backup",
        "bundle_version": BUNDLE_FORMAT_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": get_runtime_environment_name(),
        "database_backup": {
            "filename": BUNDLE_DATABASE_MEMBER,
            "source_filename": database_artifact.download_name,
            "size_bytes": database_artifact.size_bytes,
            "included_modules": list(DATABASE_BACKED_MODULES),
        },
        "excluded_table_data": {
            "tables": list(excluded_tables),
            "reason": (
                "Stored PDF payloads are omitted to keep the backup small. The tables are "
                "recreated empty by a restore and the documents must be re-uploaded."
            ),
            "inventory_file": BUNDLE_EXCLUDED_FILES_MEMBER if excluded_tables else None,
            "row_counts": {name: data.get("row_count", 0) for name, data in inventory.items()},
            "omitted_bytes": sum(
                int(data.get("omitted_bytes") or 0) for data in inventory.values()
            ),
        },
        "artifacts": {
            "committee_uploads": {
                "path": BUNDLE_COMMITTEE_UPLOADS_DIR,
                "present": upload_dir_exists,
                "file_count": upload_file_count,
            }
        },
    }

    try:
        manifest_bytes = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
        with tarfile.open(bundle_path, "w:gz") as archive:
            archive.add(database_artifact.temp_path, arcname=BUNDLE_DATABASE_MEMBER)
            if upload_dir_exists:
                archive.add(upload_dir, arcname=BUNDLE_COMMITTEE_UPLOADS_DIR)
            if excluded_tables:
                _add_bytes_member(
                    archive,
                    BUNDLE_EXCLUDED_FILES_MEMBER,
                    json.dumps(inventory, indent=2, sort_keys=True, default=str).encode("utf-8"),
                )
            _add_bytes_member(archive, BUNDLE_MANIFEST_FILENAME, manifest_bytes)
    except Exception:
        bundle_path.unlink(missing_ok=True)
        raise
    finally:
        database_artifact.cleanup()

    return BackupArtifact(
        temp_path=bundle_path,
        download_name=build_backup_filename(),
    )


def get_retained_backup_directory() -> Path:
    """Return the configured application-storage directory for daily bundles.

    Relative paths are deliberately rooted beside the Flask application, rather
    than in the process working directory, so a Gunicorn or CLI launch cannot
    silently split the retained-backup store.
    """

    configured = str(current_app.config.get("AUTO_BACKUP_DIR") or "").strip()
    base_dir = Path(current_app.root_path).resolve().parent
    directory = Path(configured) if configured else (base_dir / "storage" / "backups")
    if not directory.is_absolute():
        directory = base_dir / directory
    return directory.expanduser().resolve()


def _retained_backup_filename(backup_date: date) -> str:
    return f"{RETAINED_BACKUP_FILENAME_PREFIX}{backup_date.isoformat()}.tar.gz"


def _retained_backup_date(filename: str) -> date | None:
    match = RETAINED_BACKUP_FILENAME_PATTERN.fullmatch(filename)
    if match is None:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _retained_backup_from_path(path: Path) -> RetainedBackup | None:
    backup_date = _retained_backup_date(path.name)
    if backup_date is None or path.is_symlink() or not path.is_file():
        return None
    try:
        stats = path.stat()
    except OSError:
        return None
    if stats.st_size <= 0:
        return None
    return RetainedBackup(
        filename=path.name,
        path=path,
        backup_date=backup_date,
        size_bytes=stats.st_size,
        created_at=datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc),
    )


def list_retained_backups() -> list[RetainedBackup]:
    """List readable daily backups, newest day first.

    This is intentionally filesystem-backed rather than a database BLOB: a
    database backup must not make the database itself grow by another complete
    copy every day.
    """

    directory = get_retained_backup_directory()
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise BackupError("The configured daily-backup storage path is not a directory.")
    try:
        backups = [
            backup
            for path in directory.iterdir()
            if (backup := _retained_backup_from_path(path)) is not None
        ]
    except OSError as exc:
        raise BackupError(f"Daily-backup storage could not be read: {exc}") from exc
    return sorted(backups, key=lambda backup: backup.backup_date, reverse=True)


def get_retained_backup(filename: str) -> RetainedBackup:
    """Resolve one retained backup without allowing path traversal or symlinks."""

    if Path(filename).name != filename or _retained_backup_date(filename) is None:
        raise BackupError("The requested daily backup was not found.")
    backup = _retained_backup_from_path(get_retained_backup_directory() / filename)
    if backup is None:
        raise BackupError("The requested daily backup was not found.")
    return backup


def _backup_local_date(now: datetime | None = None) -> date:
    """Return the configured calendar day for the once-per-day backup cadence."""

    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    timezone_name = str(current_app.config.get("AUTO_BACKUP_TIMEZONE") or "Asia/Kolkata")
    try:
        from zoneinfo import ZoneInfo

        return current_time.astimezone(ZoneInfo(timezone_name)).date()
    except Exception:  # noqa: BLE001 - an invalid optional timezone must not stop backups
        logger.warning(
            "Invalid AUTO_BACKUP_TIMEZONE %r; using UTC for the daily backup schedule.",
            timezone_name,
        )
        return current_time.astimezone(timezone.utc).date()


def _retention_days() -> int:
    try:
        value = int(current_app.config.get("AUTO_BACKUP_RETENTION_DAYS", 15))
    except (TypeError, ValueError):
        value = 15
    return max(value, 1)


def prune_retained_backups(*, reference_date: date | None = None) -> list[str]:
    """Delete daily bundles outside the rolling retention window.

    A date cutoff handles gaps in scheduled runs, and the count guard keeps no
    more than the configured number of retained daily files even if an operator
    has copied duplicate-day files into the directory.
    """

    reference = reference_date or _backup_local_date()
    retention_days = _retention_days()
    cutoff = reference - timedelta(days=retention_days - 1)
    retained = list_retained_backups()
    expired = [
        backup
        for index, backup in enumerate(retained)
        if backup.backup_date < cutoff or index >= retention_days
    ]
    removed: list[str] = []
    for backup in expired:
        try:
            backup.path.unlink()
        except OSError as exc:
            logger.warning("Could not prune retained backup %s: %s", backup.path, exc)
        else:
            removed.append(backup.filename)
    return removed


def create_retained_daily_backup(*, backup_date: date | None = None) -> tuple[RetainedBackup, bool]:
    """Create one full bundle for a calendar day and retain it on local storage.

    The returned flag is ``True`` only if a new bundle was written.  Existing
    completed files make this operation idempotent, which lets an external cron,
    the in-process safety-net scheduler, and a manual CLI run coexist safely.
    """

    scheduled_date = backup_date or _backup_local_date()
    directory = get_retained_backup_directory()
    try:
        directory.mkdir(parents=True, exist_ok=True, mode=stat.S_IRWXU)
        os.chmod(directory, stat.S_IRWXU)
    except OSError as exc:
        raise BackupError(f"Daily-backup storage could not be created: {exc}") from exc
    if not directory.is_dir():
        raise BackupError("The configured daily-backup storage path is not a directory.")

    filename = _retained_backup_filename(scheduled_date)
    destination = directory / filename
    existing = _retained_backup_from_path(destination)
    if existing is not None:
        prune_retained_backups(reference_date=scheduled_date)
        return existing, False

    artifact = create_full_backup_bundle()
    staged_path: Path | None = None
    try:
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{filename}.", suffix=".tmp", dir=directory
        )
        staged_path = Path(temporary_name)
        with os.fdopen(file_descriptor, "wb") as staged_file, artifact.temp_path.open("rb") as source:
            shutil.copyfileobj(source, staged_file)
            staged_file.flush()
            os.fsync(staged_file.fileno())
        os.chmod(staged_path, stat.S_IRUSR | stat.S_IWUSR)
        os.replace(staged_path, destination)
        staged_path = None
    except OSError as exc:
        raise BackupError(f"Daily backup could not be saved to application storage: {exc}") from exc
    finally:
        artifact.cleanup()
        if staged_path is not None:
            staged_path.unlink(missing_ok=True)

    stored = _retained_backup_from_path(destination)
    if stored is None:
        raise BackupError("Daily backup was saved but could not be verified.")
    prune_retained_backups(reference_date=scheduled_date)
    return stored, True


def run_retained_daily_backup() -> tuple[RetainedBackup, bool]:
    """Run today's retained backup and record the successful automatic run."""

    backup, created = create_retained_daily_backup()
    if not created:
        return backup, False

    # The file is already safely stored.  An audit logging failure must not
    # discard it or turn a successful daily backup into a failed one.
    try:
        from app.extensions import db
        from app.models.core.audit_log import AuditLog

        db.session.add(
            AuditLog(
                action="DATABASE_DAILY_BACKUP_CREATED",
                entity_type="RetainedBackup",
                entity_id=backup.filename,
                details=(
                    "Automatic daily full backup stored in application backup storage; "
                    f"rolling retention is {_retention_days()} days."
                ),
            )
        )
        db.session.commit()
    except Exception:  # noqa: BLE001 - preserve the backup if audit persistence is unavailable
        try:
            from app.extensions import db

            db.session.rollback()
        except Exception:  # noqa: BLE001
            pass
        logger.exception("Daily backup %s was stored but could not be added to the audit trail", backup.filename)
    return backup, True


def start_retained_backup_scheduler(app) -> None:
    """Start the web-process safety-net that ensures one backup per day.

    A scheduled ``flask create-daily-backup`` command remains suitable for a
    platform cron.  This loop means a normally running single-worker deployment
    still creates the day's bundle if no separate scheduler is configured.
    """

    if getattr(app, "_retained_backup_scheduler_started", False):
        return
    app._retained_backup_scheduler_started = True

    try:
        interval_seconds = int(app.config.get("AUTO_BACKUP_CHECK_INTERVAL_SECONDS", 3600))
    except (TypeError, ValueError):
        interval_seconds = 3600
    interval_seconds = max(interval_seconds, 60)

    def _run() -> None:
        import threading

        stop_event = getattr(app, "_retained_backup_scheduler_stop", None)
        if stop_event is None:
            stop_event = threading.Event()
            app._retained_backup_scheduler_stop = stop_event
        while not stop_event.is_set():
            with app.app_context():
                try:
                    backup, created = run_retained_daily_backup()
                    if created:
                        app.logger.info("Retained daily backup created: %s", backup.filename)
                except Exception:  # noqa: BLE001 - retry on the next scheduled check
                    app.logger.exception("Automatic daily backup failed")
            stop_event.wait(interval_seconds)

    import threading

    thread = threading.Thread(
        target=_run,
        name="ongc-retained-backup-scheduler",
        daemon=True,
    )
    app._retained_backup_scheduler_thread = thread
    thread.start()


def _validate_sql_backup_file(path: Path) -> dict:
    open_fn = gzip.open if path.name.endswith(".gz") else open
    try:
        with open_fn(path, "rb") as handle:
            preview_lines = _read_sql_preview_from_binary_stream(
                handle,
                compressed=False,
            )
    except OSError as exc:
        raise BackupError(f"Backup file could not be read: {exc}") from exc

    _validate_sql_preview_lines(preview_lines)

    return {
        "path": str(path),
        "format": "sql",
        "compressed": path.name.endswith(".gz"),
        "size_bytes": path.stat().st_size,
        "preview_lines": preview_lines,
    }


def _validate_bundle_backup_file(path: Path) -> dict:
    try:
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getnames()
            if BUNDLE_DATABASE_MEMBER not in members:
                raise BackupError(
                    "Backup bundle is missing the embedded database.sql.gz file."
                )

            manifest = {}
            if BUNDLE_MANIFEST_FILENAME in members:
                manifest_member = archive.extractfile(BUNDLE_MANIFEST_FILENAME)
                if manifest_member is None:
                    raise BackupError("Backup bundle manifest could not be read.")
                manifest = json.load(manifest_member)

            database_member = archive.extractfile(BUNDLE_DATABASE_MEMBER)
            if database_member is None:
                raise BackupError("Backup bundle database payload could not be read.")
            preview_lines = _read_sql_preview_from_binary_stream(
                database_member,
                compressed=True,
            )
    except tarfile.TarError as exc:
        raise BackupError(f"Backup bundle could not be read: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise BackupError(f"Backup bundle manifest is invalid JSON: {exc}") from exc

    _validate_sql_preview_lines(preview_lines)

    excluded = manifest.get("excluded_table_data") or {}
    return {
        "path": str(path),
        "format": "bundle",
        "compressed": True,
        "size_bytes": path.stat().st_size,
        "preview_lines": preview_lines,
        "manifest": manifest,
        "database_backup_member": BUNDLE_DATABASE_MEMBER,
        "includes_committee_uploads": any(
            member == BUNDLE_COMMITTEE_UPLOADS_DIR
            or member.startswith(f"{BUNDLE_COMMITTEE_UPLOADS_DIR}/")
            for member in members
        ),
        "excluded_tables": list(excluded.get("tables") or []),
        "excluded_row_counts": dict(excluded.get("row_counts") or {}),
        "excluded_bytes": int(excluded.get("omitted_bytes") or 0),
        "includes_excluded_inventory": BUNDLE_EXCLUDED_FILES_MEMBER in members,
    }


def validate_backup_file(file_path: str | Path) -> dict:
    """Verify a backup file exists, can be read, and looks like a supported backup."""

    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise BackupError(f"Backup file does not exist: {path}")
    if not path.is_file():
        raise BackupError(f"Backup path is not a file: {path}")
    if not _is_supported_backup_file(path):
        raise BackupError("Backup file must end with .sql, .sql.gz, or .tar.gz.")

    if _is_bundle_backup_file(path):
        return _validate_bundle_backup_file(path)
    return _validate_sql_backup_file(path)


def _restore_sql_backup_from_validation(validation: dict) -> dict:
    settings = resolve_database_connection_settings()
    defaults_file = _write_client_defaults_file(settings)
    restore_command = [
        _resolve_mysql_client_binary(
            current_app.config.get("MYSQL_BIN", "mysql"),
            "mysql",
            "mariadb",
        ),
        f"--defaults-extra-file={defaults_file}",
        "--protocol=TCP",
        "--default-character-set=utf8mb4",
        settings.database,
    ]

    # A gzip file object cannot be handed to a subprocess as stdin: its fileno() is the
    # descriptor of the *compressed* file, so the client would receive raw gzip bytes.
    # Decompress to a plain file first and restore from that.
    plain_sql_path: Path | None = None
    try:
        if validation["compressed"]:
            plain_sql_path = _create_temp_path(".sql")
            with gzip.open(validation["path"], "rb") as source, plain_sql_path.open("wb") as target:
                shutil.copyfileobj(source, target)
            restore_source_path = plain_sql_path
        else:
            restore_source_path = Path(validation["path"])

        with restore_source_path.open("rb") as source:
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
        # Raised when the mysql client is absent; must precede the OSError clause.
        raise BackupError(
            "No MySQL client was found on PATH. "
            "Install a MySQL/MariaDB client package in the deployment image or admin shell."
        ) from exc
    except OSError as exc:
        raise BackupError(f"Backup file could not be read for restore: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise BackupError(
            "Restore timed out before the mysql client completed."
        ) from exc
    finally:
        defaults_file.unlink(missing_ok=True)
        if plain_sql_path is not None:
            plain_sql_path.unlink(missing_ok=True)

    return {
        "database": settings.database,
        "host": settings.host,
        "path": validation["path"],
        "format": "sql",
        "compressed": validation["compressed"],
        "size_bytes": validation["size_bytes"],
        "attachments_restored": False,
    }


def _restore_backup_bundle_from_validation(validation: dict) -> dict:
    extract_dir = Path(tempfile.mkdtemp(prefix="ongc-backup-bundle-"))
    try:
        with tarfile.open(validation["path"], "r:gz") as archive:
            _safe_extract_tar(archive, extract_dir)

        embedded_database_path = extract_dir / validation["database_backup_member"]
        if not embedded_database_path.exists():
            raise BackupError("Backup bundle did not extract a database payload.")

        restore_result = _restore_sql_backup_from_validation(
            _validate_sql_backup_file(embedded_database_path)
        )
        uploads_source = extract_dir / BUNDLE_COMMITTEE_UPLOADS_DIR
        uploads_destination = _committee_upload_dir()
        _replace_directory_contents(
            uploads_source if uploads_source.exists() else None,
            uploads_destination,
        )
        restore_result.update(
            {
                "path": validation["path"],
                "format": "bundle",
                "size_bytes": validation["size_bytes"],
                "attachments_restored": True,
                "attachments_path": str(uploads_destination),
                "bundle_manifest": validation.get("manifest") or {},
                "excluded_tables": validation.get("excluded_tables") or [],
                "excluded_row_counts": validation.get("excluded_row_counts") or {},
            }
        )
        return restore_result
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)


def restore_database_backup(file_path: str | Path) -> dict:
    """Restore a supported SQL or full-backup bundle into the configured database."""

    validation = validate_backup_file(file_path)
    if validation["format"] == "bundle":
        return _restore_backup_bundle_from_validation(validation)
    return _restore_sql_backup_from_validation(validation)
