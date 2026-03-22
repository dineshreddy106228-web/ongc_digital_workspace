from __future__ import annotations

import gzip
import json
import sys
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import patch

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services import backups


def _make_sql_backup_file(directory: Path, filename: str = "sample.sql.gz") -> Path:
    target = directory / filename
    with gzip.open(target, "wt", encoding="utf-8") as handle:
        handle.write("-- MySQL dump\n")
        handle.write("CREATE TABLE sample (id INT);\n")
    return target


def _build_test_app(instance_path: Path, upload_dir: Path) -> Flask:
    app = Flask(__name__, instance_path=str(instance_path))
    app.config.update(
        SECRET_KEY="test",
        COMMITTEE_UPLOAD_DIR=str(upload_dir),
        DB_COMMAND_TIMEOUT_SECONDS=30,
        APP_ENVIRONMENT_NAME="test",
    )
    return app


def test_create_full_backup_bundle_includes_manifest_and_uploads() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        instance_path = root / "instance"
        upload_dir = instance_path / "committee_uploads"
        upload_dir.mkdir(parents=True)
        (upload_dir / "note.txt").write_text("attachment", encoding="utf-8")
        sql_path = _make_sql_backup_file(root, "database.sql.gz")
        app = _build_test_app(instance_path, upload_dir)

        fake_artifact = backups.BackupArtifact(
            temp_path=sql_path,
            download_name="database.sql.gz",
        )

        with app.app_context():
            with patch.object(backups, "create_database_backup", return_value=fake_artifact):
                artifact = backups.create_full_backup_bundle()
                try:
                    assert artifact.download_name.endswith(".tar.gz")
                    with tarfile.open(artifact.temp_path, "r:gz") as archive:
                        names = archive.getnames()
                        assert backups.BUNDLE_DATABASE_MEMBER in names
                        assert backups.BUNDLE_MANIFEST_FILENAME in names
                        assert f"{backups.BUNDLE_COMMITTEE_UPLOADS_DIR}/note.txt" in names

                        manifest_member = archive.extractfile(backups.BUNDLE_MANIFEST_FILENAME)
                        assert manifest_member is not None
                        manifest = json.load(manifest_member)
                        assert manifest["artifacts"]["committee_uploads"]["present"] is True
                        assert manifest["artifacts"]["committee_uploads"]["file_count"] == 1
                finally:
                    artifact.cleanup()


def test_validate_backup_file_detects_full_bundle() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        instance_path = root / "instance"
        upload_dir = instance_path / "committee_uploads"
        upload_dir.mkdir(parents=True)
        (upload_dir / "evidence.txt").write_text("ok", encoding="utf-8")
        sql_path = _make_sql_backup_file(root, "database.sql.gz")
        bundle_path = root / "bundle.tar.gz"
        manifest = {
            "format": "ongc_workspace_full_backup",
            "bundle_version": backups.BUNDLE_FORMAT_VERSION,
            "artifacts": {"committee_uploads": {"present": True, "file_count": 1}},
        }
        with tarfile.open(bundle_path, "w:gz") as archive:
            archive.add(sql_path, arcname=backups.BUNDLE_DATABASE_MEMBER)
            archive.add(upload_dir, arcname=backups.BUNDLE_COMMITTEE_UPLOADS_DIR)
            manifest_bytes = json.dumps(manifest).encode("utf-8")
            info = tarfile.TarInfo(backups.BUNDLE_MANIFEST_FILENAME)
            info.size = len(manifest_bytes)
            archive.addfile(info, fileobj=backups.io.BytesIO(manifest_bytes))

        app = _build_test_app(instance_path, upload_dir)
        with app.app_context():
            validation = backups.validate_backup_file(bundle_path)

        assert validation["format"] == "bundle"
        assert validation["includes_committee_uploads"] is True
        assert validation["manifest"]["bundle_version"] == backups.BUNDLE_FORMAT_VERSION


def test_restore_database_backup_restores_committee_uploads_from_bundle() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        instance_path = root / "instance"
        upload_dir = instance_path / "committee_uploads"
        upload_dir.mkdir(parents=True)
        (upload_dir / "old.txt").write_text("old", encoding="utf-8")

        sql_path = _make_sql_backup_file(root, "database.sql.gz")
        bundle_uploads = root / "bundle_uploads"
        bundle_uploads.mkdir()
        (bundle_uploads / "new.txt").write_text("new", encoding="utf-8")

        bundle_path = root / "restore_bundle.tar.gz"
        manifest = {
            "format": "ongc_workspace_full_backup",
            "bundle_version": backups.BUNDLE_FORMAT_VERSION,
            "artifacts": {"committee_uploads": {"present": True, "file_count": 1}},
        }
        with tarfile.open(bundle_path, "w:gz") as archive:
            archive.add(sql_path, arcname=backups.BUNDLE_DATABASE_MEMBER)
            archive.add(bundle_uploads, arcname=backups.BUNDLE_COMMITTEE_UPLOADS_DIR)
            manifest_bytes = json.dumps(manifest).encode("utf-8")
            info = tarfile.TarInfo(backups.BUNDLE_MANIFEST_FILENAME)
            info.size = len(manifest_bytes)
            archive.addfile(info, fileobj=backups.io.BytesIO(manifest_bytes))

        app = _build_test_app(instance_path, upload_dir)
        fake_restore = {
            "database": "test_db",
            "host": "localhost",
            "path": str(sql_path),
            "format": "sql",
            "compressed": True,
            "size_bytes": sql_path.stat().st_size,
            "attachments_restored": False,
        }

        with app.app_context():
            with patch.object(backups, "_restore_sql_backup_from_validation", return_value=fake_restore):
                result = backups.restore_database_backup(bundle_path)

        assert result["format"] == "bundle"
        assert result["attachments_restored"] is True
        assert (upload_dir / "new.txt").exists()
        assert not (upload_dir / "old.txt").exists()


def _run_direct() -> None:
    tests = [
        test_create_full_backup_bundle_includes_manifest_and_uploads,
        test_validate_backup_file_detects_full_bundle,
        test_restore_database_backup_restores_committee_uploads_from_bundle,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    _run_direct()
