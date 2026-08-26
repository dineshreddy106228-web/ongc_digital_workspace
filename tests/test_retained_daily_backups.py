"""Retained daily-backup storage and admin download coverage."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
from unittest.mock import patch

import pytest
from flask import Flask
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.core.services import backups
from app.core.roles import ADMIN_ROLE
from app.extensions import db
from app.models.core.role import Role
from app.models.core.user import User
from config import Config


def _artifact(directory: Path, index: int) -> backups.BackupArtifact:
    source = directory / f"source-{index}.tar.gz"
    source.write_bytes(f"daily backup {index}".encode("utf-8"))
    return backups.BackupArtifact(temp_path=source, download_name=source.name)


@pytest.fixture()
def backup_storage_app(tmp_path):
    app = Flask(__name__)
    app.config.update(
        AUTO_BACKUP_DIR=str(tmp_path / "retained"),
        AUTO_BACKUP_RETENTION_DAYS=15,
        AUTO_BACKUP_TIMEZONE="Asia/Kolkata",
    )
    with app.app_context():
        yield app


def test_daily_backup_is_retained_once_and_reused_for_the_same_day(backup_storage_app, tmp_path):
    with backup_storage_app.app_context():
        with patch.object(backups, "create_full_backup_bundle", return_value=_artifact(tmp_path, 1)) as create:
            stored, created = backups.create_retained_daily_backup(backup_date=date(2026, 8, 25))
            repeated, repeated_created = backups.create_retained_daily_backup(backup_date=date(2026, 8, 25))

    assert created is True
    assert repeated_created is False
    assert repeated == stored
    assert stored.filename == "ongc_workspace_daily_backup_2026-08-25.tar.gz"
    assert stored.path.read_bytes() == b"daily backup 1"
    assert not (tmp_path / "source-1.tar.gz").exists()
    assert create.call_count == 1


def test_daily_backup_prunes_to_the_latest_fifteen_calendar_days(backup_storage_app, tmp_path):
    artifacts = iter(_artifact(tmp_path, index) for index in range(16))

    with backup_storage_app.app_context():
        with patch.object(backups, "create_full_backup_bundle", side_effect=lambda: next(artifacts)):
            for day in range(1, 17):
                backups.create_retained_daily_backup(backup_date=date(2026, 8, day))
        retained = backups.list_retained_backups()

    assert len(retained) == 15
    assert retained[0].filename == "ongc_workspace_daily_backup_2026-08-16.tar.gz"
    assert retained[-1].filename == "ongc_workspace_daily_backup_2026-08-02.tar.gz"
    assert not (tmp_path / "retained" / "ongc_workspace_daily_backup_2026-08-01.tar.gz").exists()


def test_retained_backup_lookup_rejects_a_path_outside_backup_storage(backup_storage_app):
    with backup_storage_app.app_context():
        with pytest.raises(backups.BackupError):
            backups.get_retained_backup("../outside.tar.gz")


@pytest.fixture()
def admin_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'backup_admin.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True
        AUTO_BACKUP_ENABLED = False
        AUTO_BACKUP_DIR = str(tmp_path / "retained")

    app = create_app(_Config)
    with app.app_context():
        # SQLite only autoincrements INTEGER primary keys.
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        role = Role(id=1, name=ADMIN_ROLE)
        admin = User(
            id=1,
            username="admin",
            password_hash="x",
            role=role,
            is_active=True,
            must_change_password=False,
        )
        db.session.add_all([role, admin])
        db.session.commit()
        yield app
        db.session.remove()
        db.drop_all()


def _admin_client(app):
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = "1"
        session["_fresh"] = True
    return client


def test_admin_can_download_a_retained_daily_backup(admin_app):
    with admin_app.app_context():
        directory = backups.get_retained_backup_directory()
        directory.mkdir(parents=True)
        filename = "ongc_workspace_daily_backup_2026-08-25.tar.gz"
        (directory / filename).write_bytes(b"retained bundle")

    response = _admin_client(admin_app).get(f"/admin/backups/retained/{filename}")

    assert response.status_code == 200
    assert response.data == b"retained bundle"
    assert "attachment" in response.headers["Content-Disposition"]


def test_backup_center_lists_retained_daily_backups_for_an_admin(admin_app):
    with admin_app.app_context():
        directory = backups.get_retained_backup_directory()
        directory.mkdir(parents=True)
        filename = "ongc_workspace_daily_backup_2026-08-25.tar.gz"
        (directory / filename).write_bytes(b"retained bundle")

    response = _admin_client(admin_app).get("/admin/backups")

    assert response.status_code == 200
    assert b"Automatic Daily Backups" in response.data
    assert filename.encode("utf-8") in response.data
