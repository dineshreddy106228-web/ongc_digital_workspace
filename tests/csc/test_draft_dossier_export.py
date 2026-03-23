from __future__ import annotations

import sys
import zipfile
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.services.csc_export import build_flask_review_document
from app.core.services.csc_master_data import (
    ADMIN_MASTER_FIELD_CONFIGS,
    get_material_properties_fields,
)
from app.modules.csc.routes import (
    _build_comparison_value_rows,
    _build_workbook_created_notification_message,
    _editor_grid_snapshot,
    _master_data_editable_for_stream,
    _normalize_editor_proposed_summary_text,
    _revision_can_be_force_deleted_by_secretary,
    _subset_code_is_within_scope,
    _should_blank_legacy_supporting_baseline,
    _should_hide_legacy_supporting_sections,
    _workflow_subset_code_for_draft,
)
from app.core.services.csc_utils import (
    WORKFLOW_DRAFTING_HEAD_APPROVED,
    WORKFLOW_DRAFTING_OPEN,
    WORKFLOW_DRAFTING_REJECTED,
    WORKFLOW_DRAFTING_STAGING,
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


def test_normalize_editor_proposed_summary_text_strips_generated_wrapper() -> None:
    wrapped = (
        "Sections updated:\n"
        "- Material Properties\n"
        "- Storage and Handling\n\n"
        "Change summary:\n"
        "Updated packing controls."
    )
    assert _normalize_editor_proposed_summary_text(wrapped) == "Updated packing controls."
    assert _normalize_editor_proposed_summary_text("Change types: Material\n\nManual summary") == "Manual summary"


def test_editor_grid_snapshot_can_keep_legacy_v0_baseline_blank() -> None:
    config = [
        {
            "field_name": "storage_conditions_general",
            "default_value": "Store in a cool and dry place",
        },
        {
            "field_name": "container_type",
            "show_if": {"field_name": "storage_conditions_general", "equals": "Store in a cool and dry place"},
            "default_value": "HDPE Drum",
        },
    ]

    assert _editor_grid_snapshot(config, {}, include_defaults=True) == (
        '{"storage_conditions_general": "Store in a cool and dry place", "container_type": "HDPE Drum"}'
    )
    assert _editor_grid_snapshot(config, {}, include_defaults=False) == (
        '{"storage_conditions_general": "", "container_type": ""}'
    )


def test_secretary_force_delete_helper_matches_active_workflow_statuses() -> None:
    class RevisionStub:
        def __init__(self, status: str | None):
            self.status = status

    assert _revision_can_be_force_deleted_by_secretary(RevisionStub(WORKFLOW_DRAFTING_STAGING)) is True
    assert _revision_can_be_force_deleted_by_secretary(RevisionStub(WORKFLOW_DRAFTING_OPEN)) is True
    assert _revision_can_be_force_deleted_by_secretary(RevisionStub(WORKFLOW_DRAFTING_HEAD_APPROVED)) is True
    assert _revision_can_be_force_deleted_by_secretary(RevisionStub(WORKFLOW_DRAFTING_REJECTED)) is False
    assert _revision_can_be_force_deleted_by_secretary(None) is False


def test_workbook_created_notification_message_uses_numbered_list() -> None:
    message = _build_workbook_created_notification_message(
        "Type Classification",
        ["DFC"],
        ["ONGC/DFC/01/2026 — ALUMINIUM STEARATE", "ONGC/DFC/02/2026 — BENTONITE"],
    )

    assert "under your subset coverage (DFC)" in message
    assert "1. ONGC/DFC/01/2026 — ALUMINIUM STEARATE" in message
    assert "2. ONGC/DFC/02/2026 — BENTONITE" in message
    assert "\nReview them in your committee workspace." in message


def test_admin_master_field_configs_define_group_text_and_selects_for_type_and_centralization() -> None:
    by_name = {field["field_name"]: field for field in ADMIN_MASTER_FIELD_CONFIGS}

    assert by_name["group"]["input_type"] == "suggest"
    assert by_name["group"]["label"] == "Group"
    assert "Flow Improver" in by_name["group"]["options"]
    assert "Other" in by_name["group"]["options"]
    assert "Other" in by_name["group"]["hint"]
    assert by_name["material_type"]["label"] == "Performance / Other"
    assert by_name["material_type"]["options"] == ["Performance", "Other"]
    assert by_name["centralization"]["label"] == "Centralization for Procurement (Yes / No)"
    assert by_name["centralization"]["options"] == ["Yes", "No"]


def test_master_data_editable_only_for_material_handling_stream() -> None:
    assert _master_data_editable_for_stream("material_handling") is True
    assert _master_data_editable_for_stream("type_classification") is False
    assert _master_data_editable_for_stream(None) is False


def test_material_properties_fields_use_renamed_storage_labels() -> None:
    by_name = {field["field_name"]: field for field in get_material_properties_fields()}

    assert by_name["volatility_ambient_temperature"]["label"] == "Volatility at Ambient Temperature"
    assert by_name["sunlight_sensitivity_up_to_50c"]["label"] == "Sunlight Sensitivity (up to 50degC atmospheric temperature)"
    assert by_name["refrigeration_required"]["label"] == "Refrigeration required (Yes / No)"
    assert by_name["refrigeration_required"]["options"] == ["Yes", "No"]


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
        test_normalize_editor_proposed_summary_text_strips_generated_wrapper,
        test_editor_grid_snapshot_can_keep_legacy_v0_baseline_blank,
        test_secretary_force_delete_helper_matches_active_workflow_statuses,
        test_workbook_created_notification_message_uses_numbered_list,
        test_admin_master_field_configs_define_group_text_and_selects_for_type_and_centralization,
        test_master_data_editable_only_for_material_handling_stream,
        test_material_properties_fields_use_renamed_storage_labels,
        test_workflow_subset_code_prefers_published_parent_subset,
        test_subset_scope_helper_respects_configured_subset_codes,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    _run_direct()
