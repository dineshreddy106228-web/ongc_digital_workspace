"""Document-designated laboratory seed data and the shared QC/CSC roster stay aligned."""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from app.core.services.quality_control import (
    CSC_DESIGNATION_ONLY_LABORATORIES, LABORATORIES,
)


def _seed_module():
    path = Path(__file__).resolve().parents[1] / "migrations/versions/d4e5f6a7b8c9_seed_csc_designated_laboratories.py"
    spec = spec_from_file_location("csc_designated_labs_seed", path)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_document_designations_decode_to_every_assignable_source_row():
    assignments = _seed_module().document_assignments()

    assert len(assignments) == 284
    assert next(item for item in assignments if item[0] == "ONGC/WS/24/2026")[2] == (
        "rgl_panvel", "rgl_vadodara", "rgl_jorhat", "rgl_chennai",
        "rgl_rajahmundry", "wss_ahmedabad", "ankleshwar_asset_lab", "uran_plant_lab",
    )
    assert next(item for item in assignments if item[1] == "Baryte API Grade")[2] == (
        "rgl_vadodara", "rgl_chennai",
    )


def test_document_only_laboratories_are_available_to_the_qc_and_csc_roster():
    expected = {
        "wss_ahmedabad", "ahmedabad_asset_lab", "ankleshwar_asset_lab",
        "mehsana_asset_lab", "hazira_plant_lab", "uran_plant_lab",
    }
    assert expected <= set(LABORATORIES)
    assert all(LABORATORIES[code]["is_additional_designated"] for code in expected)
    assert CSC_DESIGNATION_ONLY_LABORATORIES["idwe_dehradun"]["name"] == "IDWE Dehradun"


def test_missing_document_chemicals_keep_their_lab_assignments_without_an_invented_stt():
    path = Path(__file__).resolve().parents[1] / "migrations/versions/e5f6a7b8c9d0_add_missing_designated_chemicals.py"
    spec = spec_from_file_location("missing_designated_chemicals_seed", path)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)

    assert {item[1] for item in module._MISSING_DOCUMENT_STANDARDS} == {
        "Corrosion Inhibitor (Kalol/Nawagam)", "THPS",
        "CMC (HVT) API Grade", "CMC (LVT) API Grade",
        "Strong Base Anion Exchange Resin",
    }
    assert all(not item[0] or item[0].startswith("ONGC/") for item in module._MISSING_DOCUMENT_STANDARDS)
