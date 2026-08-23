"""The MSDS archive names every sheet by its corporate specification reference."""
from __future__ import annotations

from pathlib import Path
import sys
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from types import SimpleNamespace

from app.core.services.msds_service import msds_archive_filenames


def _file(file_id: int, slot_code: str = "standard"):
    return SimpleNamespace(id=file_id, slot_code=slot_code)


def test_a_sheet_is_named_by_its_corporate_specification_reference():
    names = msds_archive_filenames(
        {"090001043": [_file(1)]},
        {"090001043": "ONGC / DFC / 03 / 2026"},
    )

    assert names == {1: "ONGC-DFC-03-2026.pdf"}


def test_a_chemical_with_no_specification_falls_back_to_its_material_code():
    names = msds_archive_filenames({"100101102": [_file(7)]}, {})

    assert names == {7: "100101102.pdf"}


def test_several_sheets_for_one_chemical_carry_their_slot():
    names = msds_archive_filenames(
        {"090001043": [_file(1, "standard"), _file(2, "vendor_1"), _file(3, "vendor_2")]},
        {"090001043": "ONGC / DFC / 03 / 2026"},
    )

    assert names == {
        1: "ONGC-DFC-03-2026_STANDARD.pdf",
        2: "ONGC-DFC-03-2026_VENDOR-1.pdf",
        3: "ONGC-DFC-03-2026_VENDOR-2.pdf",
    }


def test_one_specification_covering_two_materials_keeps_both_files():
    names = msds_archive_filenames(
        {"090001043": [_file(1)], "090001044": [_file(2)]},
        {"090001043": "ONGC / DFC / 03 / 2026", "090001044": "ONGC / DFC / 03 / 2026"},
    )

    assert sorted(names.values()) == [
        "ONGC-DFC-03-2026_090001043.pdf",
        "ONGC-DFC-03-2026_090001044.pdf",
    ]
    assert len(set(names.values())) == 2


def test_every_name_is_unique_and_zip_safe():
    names = msds_archive_filenames(
        {
            "090001043": [_file(1), _file(2, "vendor_1")],
            "100101102": [_file(3)],
            "100101003": [_file(4)],
        },
        {"090001043": "ONGC / DFC / 03 / 2026", "100101003": "ONGC/DFC/04/2026"},
    )

    assert len(set(names.values())) == len(names)
    for name in names.values():
        assert name.endswith(".pdf")
        assert not set(name) & set("/\\:*?\"<>|")
        # A name a zip reader can write straight to disk.
        assert Path(name).name == name


def test_the_archive_writes_one_pdf_per_stored_sheet(tmp_path):
    # The naming contract the route relies on, exercised through a real zip.
    names = msds_archive_filenames(
        {"090001043": [_file(1)], "100101102": [_file(2)]},
        {"090001043": "ONGC / DFC / 03 / 2026"},
    )
    archive_path = tmp_path / "msds.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        for file_id, name in names.items():
            archive.writestr(name, b"%PDF-1.4 test")

    with zipfile.ZipFile(archive_path) as archive:
        assert sorted(archive.namelist()) == ["100101102.pdf", "ONGC-DFC-03-2026.pdf"]
