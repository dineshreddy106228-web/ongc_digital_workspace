from __future__ import annotations

import sys
import zipfile
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.services.csc_export import build_flask_review_document
from app.modules.csc.routes import (
    _build_comparison_value_rows,
    _subset_code_is_within_scope,
    _should_blank_legacy_supporting_baseline,
    _should_hide_legacy_supporting_sections,
    _workflow_subset_code_for_draft,
)


def test_build_comparison_value_rows_marks_published_delta() -> None:
    rows = _build_comparison_value_rows(
        [{"label": "Material Code", "value": "NEW-001"}],
        [{"label": "Material Code", "value": "OLD-001"}],
    )

    assert rows == [
        {
            "label": "Material Code",
            "value": "NEW-001",
            "source_value": "OLD-001",
            "change_status": "Revised",
        }
    ]


def test_build_flask_review_document_uses_comparison_layout_without_update_fields() -> None:
    review_context = {
        "draft": {
            "spec_number": "ONGC/DFC/100",
            "chemical_name": "Test Chemical",
            "material_code": "MAT-01",
            "status": "Drafting",
            "test_procedure": "TP-100",
        },
        "summary_rows": [
            {"label": "Specification", "value": "ONGC/DFC/100"},
            {"label": "Chemical", "value": "Test Chemical"},
        ],
        "section_rows": [
            {
                "label": "Background Context",
                "source_text": "Published background",
                "text": "Draft background",
                "change_status": "Revised",
            }
        ],
        "parameter_rows": [
            {
                "parameter_name": "Viscosity",
                "source_parameter_type": "Mandatory",
                "parameter_type": "Optional",
                "source_unit_of_measure": "cP",
                "unit_of_measure": "mPa.s",
                "required_value": "10-20",
                "final_requirement": "12-18",
                "source_conditions": "25 C",
                "conditions": "30 C",
                "source_test_procedure_type": "ASTM",
                "test_procedure_type": "ISO",
                "source_test_method": "Old method",
                "procedure_text": "New method",
                "change_status": "Revised",
            }
        ],
        "master_rows": [
            {
                "label": "Material Code",
                "source_value": "MAT-OLD",
                "value": "MAT-01",
                "change_status": "Revised",
            }
        ],
        "material_property_rows": [],
        "storage_rows": [],
        "impact_rows": [],
        "latest_review_notes": "",
    }

    doc_bytes = build_flask_review_document(review_context)

    with zipfile.ZipFile(BytesIO(doc_bytes)) as archive:
        document_xml = archive.read("word/document.xml").decode("utf-8")
        settings_xml = archive.read("word/settings.xml").decode("utf-8")

    assert "Published Baseline" in document_xml
    assert "Draft Snapshot" in document_xml
    assert "Change Status" in document_xml
    assert "Published background" in document_xml
    assert "Draft background" in document_xml
    assert "MAT-OLD" in document_xml
    assert "MAT-01" in document_xml
    assert "w:updateFields" not in settings_xml


def test_legacy_v0_supporting_rules() -> None:
    class DraftStub:
        def __init__(self, spec_version: int, parent_draft_id=None):
            self.spec_version = spec_version
            self.parent_draft_id = parent_draft_id

    assert _should_blank_legacy_supporting_baseline(DraftStub(0)) is True
    assert _should_blank_legacy_supporting_baseline(DraftStub(1)) is False
    assert _should_hide_legacy_supporting_sections(DraftStub(0, None)) is True
    assert _should_hide_legacy_supporting_sections(DraftStub(1, None)) is False
    assert _should_hide_legacy_supporting_sections(DraftStub(0, 99)) is False


def test_workflow_subset_code_prefers_published_parent_subset() -> None:
    class DraftStub:
        def __init__(self, subset: str | None):
            self.subset = subset
            self.parent_draft_id = None

    class RevisionStub:
        def __init__(self, parent_draft):
            self.parent_draft = parent_draft

    parent = DraftStub("DFC")
    child = DraftStub("WTC")

    assert _workflow_subset_code_for_draft(child, RevisionStub(parent)) == "DFC"


def test_subset_scope_helper_respects_configured_subset_codes() -> None:
    assert _subset_code_is_within_scope(None, []) is True
    assert _subset_code_is_within_scope("DFC", []) is True
    assert _subset_code_is_within_scope(None, ["DFC"]) is False
    assert _subset_code_is_within_scope("DFC", ["DFC"]) is True
    assert _subset_code_is_within_scope("WTC", ["DFC"]) is False


def _run_direct() -> None:
    tests = [
        test_build_comparison_value_rows_marks_published_delta,
        test_build_flask_review_document_uses_comparison_layout_without_update_fields,
        test_legacy_v0_supporting_rules,
        test_workflow_subset_code_prefers_published_parent_subset,
        test_subset_scope_helper_respects_configured_subset_codes,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    _run_direct()
