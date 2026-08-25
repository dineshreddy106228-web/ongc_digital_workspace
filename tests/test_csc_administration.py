"""Administration changes only persist when their values genuinely differ."""
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import BigInteger, Integer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import create_app
from app.extensions import db
from config import Config


@pytest.fixture()
def csc_admin_app(tmp_path):
    class _Config(Config):
        SQLALCHEMY_DATABASE_URI = f"sqlite:///{tmp_path / 'csc_admin.db'}"
        SQLALCHEMY_ENGINE_OPTIONS: dict = {}
        WTF_CSRF_ENABLED = False
        TESTING = True

    app = create_app(_Config)
    with app.app_context():
        for mapper in db.Model.registry.mappers:
            for column in mapper.local_table.primary_key:
                if isinstance(column.type, BigInteger):
                    column.type = Integer()
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


def test_save_entry_administration_ignores_an_unchanged_row_and_summarises_changes(
    csc_admin_app, monkeypatch
):
    from app.core.services import csc_administration as administration
    from app.models.csc.authorized_lab import CSCAuthorizedLab

    entry = {"ref": "r-1", "chemical_name": "Alpha", "spec_number": "ONGC/DFC/01/2026"}
    labs = [
        {"code": "lab-a", "name": "Alpha Lab", "location": "Mumbai", "description": ""},
        {"code": "lab-b", "name": "Beta Lab", "location": "Chennai", "description": ""},
    ]
    register_row = SimpleNamespace(standard_days=3, remarks="Approved", updated_by=None)
    monkeypatch.setattr(administration, "catalogue", lambda: [entry])
    monkeypatch.setattr(administration, "laboratory_options", lambda: labs)
    monkeypatch.setattr(administration, "_register_row", lambda _entry: register_row)

    db.session.add(CSCAuthorizedLab(entry_ref="r-1", lab_code="lab-a", updated_by=1))
    db.session.commit()

    unchanged = administration.save_entry_administration(
        "r-1", ["lab-a"], "3", "  Approved  ", user_id=7
    )
    assert unchanged == ""
    assert CSCAuthorizedLab.query.filter_by(entry_ref="r-1").count() == 1

    summary = administration.save_entry_administration(
        "r-1", ["lab-b"], "4", "Revised note", user_id=7
    )
    db.session.flush()

    assert "authorised laboratories set to Beta Lab" in summary
    assert "standard testing time 3 days → 4 days" in summary
    assert "remarks updated" in summary
    assert [row.lab_code for row in CSCAuthorizedLab.query.filter_by(entry_ref="r-1").all()] == ["lab-b"]
    assert (register_row.standard_days, register_row.remarks, register_row.updated_by) == (4, "Revised note", 7)
