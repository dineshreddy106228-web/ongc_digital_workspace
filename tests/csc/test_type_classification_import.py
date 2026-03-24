from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
import sys
from unittest.mock import patch

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.services.csc_type_classification_import import (
    IMPORTED_JUSTIFICATION_END,
    IMPORTED_JUSTIFICATION_START,
    MATERIAL_HANDLING_JUSTIFICATION_END,
    MATERIAL_HANDLING_JUSTIFICATION_START,
    apply_material_handling_core_updates,
    build_parent_lookup_by_material_code,
    import_material_handling_workbook,
    import_type_classification_workbook,
    merge_material_handling_section_block,
    parse_material_handling_workbook,
    parse_type_classification_workbook,
)
from app.modules.csc import routes as csc_routes
from app.modules.csc.routes import (
    _default_proposed_changes_section_text,
    _default_recommendation_section_text,
    _default_section_rows,
    _discard_staged_material_handling_workbook_payload,
    _ensure_default_draft_sections,
    _load_staged_material_handling_workbook_payload,
    _material_handling_preview_comparisons,
    _seed_child_draft_master_snapshot,
    _set_draft_section_text,
    _resolve_material_handling_row_for_preview,
    _stage_material_handling_workbook_payload,
)
from app.models.csc.section import CSCSection


@dataclass
class FakeParameter:
    sort_order: int
    parameter_name: str
    parameter_type: str = "Desirable"
    test_procedure_type: str = ""
    test_method: str = ""
    test_procedure_text: str = ""
    justification: str = ""


@dataclass
class FakeDraft:
    id: int
    spec_number: str
    material_code: str
    chemical_name: str
    parent_draft_id: int | None = None
    parameters: list[FakeParameter] = field(default_factory=list)


@dataclass
class FakeParentDraft:
    id: int
    spec_number: str
    material_code: str
    chemical_name: str


class _FakeOrderedSections:
    def __init__(self, sections: list[CSCSection]):
        self._sections = sections

    def order_by(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._sections)


class _FakeSession:
    def __init__(self):
        self.added: list[object] = []
        self.deleted: list[object] = []

    def add(self, value):
        self.added.append(value)

    def delete(self, value):
        self.deleted.append(value)


class _FakeSectionQuery:
    def __init__(self, sections: list[CSCSection]):
        self._sections = sections
        self._filters: dict[str, object] = {}

    def filter_by(self, **kwargs):
        clone = _FakeSectionQuery(self._sections)
        clone._filters = kwargs
        return clone

    def first(self):
        for section in self._sections:
            if all(getattr(section, key) == value for key, value in self._filters.items()):
                return section
        return None


def _make_workbook(sheet_rows: dict[str, list[list[object]]]) -> BytesIO:
    workbook = Workbook()
    first_sheet = True
    for sheet_name, rows in sheet_rows.items():
        if first_sheet:
            worksheet = workbook.active
            worksheet.title = sheet_name
            first_sheet = False
        else:
            worksheet = workbook.create_sheet(title=sheet_name)
        for row in rows:
            worksheet.append(row)
    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return buffer


def _header(include_changes_column: bool = False) -> list[str]:
    columns = [
        "Spec Number",
        "Chemical Name",
        "Material Code",
        "Draft ID",
        "Parameter ID",
        "S. No.",
        "Parameter Name",
        "Requirement",
        "Current Type",
        "Type",
        "Vital Reason 1",
        "Vital Reason 2",
        "Vital Reason 3",
        "Vital Reason 4",
        "Any more changes proposed ?",
        "Test Procedure Type",
        "Test Procedure Text",
    ]
    if include_changes_column:
        columns.append("Proposed Modifications, if any, to Parameters / Requirement")
    return columns


def _material_header(
    *,
    include_chemical_name: bool = True,
    include_source_spec_number: bool = True,
    include_officers_responsible: bool = True,
) -> list[str]:
    columns = ["S. No."]
    if include_chemical_name:
        columns.append("Chemical Name")
    columns.append("Material Code")
    if include_source_spec_number:
        columns.append("Specification No.")
    columns.extend(
        [
            "Proposed New Specification No.",
            "Unit",
            "Physical State",
            "Volatility",
            "Sunlight Sensitivity",
            "Moisture Sensitivity",
            "Temperature Sensitivity",
            "Reactivity",
            "Flammable",
            "Toxic",
            "Corrosive",
            "Storage Conditions – General",
            "Storage Conditions – Special",
            "Container Type (Packing)",
            "Container Capacity (Packing)",
            "Container Description",
            "Primary Storage Classification",
            "Remarks (If Any)",
        ]
    )
    if include_officers_responsible:
        columns.insert(columns.index("Unit") + 1, "Officers Responsible")
    return columns


def test_ensure_default_draft_sections_normalizes_legacy_changes_placeholder(monkeypatch):
    legacy_changes = CSCSection(
        id=11,
        draft_id=7,
        section_name="changes",
        section_text="Testing",
        sort_order=0,
    )
    draft = type(
        "Draft",
        (),
        {
            "id": 7,
            "sections": _FakeOrderedSections([legacy_changes]),
        },
    )()
    fake_session = _FakeSession()

    monkeypatch.setattr(csc_routes.db, "session", fake_session)

    _ensure_default_draft_sections(draft)

    assert legacy_changes.section_name == "proposed_changes"
    assert legacy_changes.section_text == _default_proposed_changes_section_text()
    assert legacy_changes.sort_order == 30
    assert any(
        isinstance(section, CSCSection) and section.section_name == "background"
        for section in fake_session.added
    )


def test_ensure_default_draft_sections_replaces_parent_recommendation_for_material_handling(monkeypatch):
    recommendation = CSCSection(
        id=12,
        draft_id=8,
        section_name="recommendation",
        section_text=_default_recommendation_section_text(),
        sort_order=70,
    )
    draft = type(
        "Draft",
        (),
        {
            "id": 8,
            "sections": _FakeOrderedSections([recommendation]),
        },
    )()
    fake_session = _FakeSession()

    monkeypatch.setattr(csc_routes.db, "session", fake_session)

    _ensure_default_draft_sections(draft, "material_handling")

    assert recommendation.section_text == _default_recommendation_section_text("material_handling")
    assert recommendation.sort_order == 70


def test_seed_child_draft_master_snapshot_backfills_missing_impact_classification(monkeypatch):
    parent = FakeParentDraft(
        id=7,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    child = FakeDraft(
        id=8,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parent_draft_id=parent.id,
    )
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        csc_routes,
        "get_master_form_values",
        lambda draft: {
            "material_code": getattr(draft, "material_code", ""),
            "physical_state": "Liquid",
            "impact_classification": "High",
            "impact_confidence": "High confidence",
        } if draft is parent else {},
    )
    monkeypatch.setattr(csc_routes, "_load_staged_master_payload", lambda _draft: {"physical_state": "Solid"})
    monkeypatch.setattr(
        csc_routes,
        "_write_staged_master_payload",
        lambda _draft, payload: captured.update(payload),
    )

    _seed_child_draft_master_snapshot(parent, child)

    assert captured["material_code"] == "100101102"
    assert captured["physical_state"] == "Solid"
    assert captured["impact_classification"] == "High"
    assert captured["impact_confidence"] == "High confidence"


def test_seed_child_draft_master_snapshot_skips_non_child_drafts(monkeypatch):
    parent = FakeParentDraft(
        id=9,
        spec_number="ONGC/DFC/02/2026",
        material_code="100101103",
        chemical_name="BARITE",
    )
    draft = FakeDraft(
        id=10,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
    )

    monkeypatch.setattr(csc_routes, "get_master_form_values", lambda _draft: {"impact_classification": "Medium"})
    monkeypatch.setattr(csc_routes, "_load_staged_master_payload", lambda _draft: {})
    monkeypatch.setattr(
        csc_routes,
        "_write_staged_master_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not write snapshot")),
    )

    _seed_child_draft_master_snapshot(parent, draft)


def test_set_draft_section_text_renames_legacy_changes_row(monkeypatch):
    legacy_changes = CSCSection(
        id=21,
        draft_id=9,
        section_name="changes",
        section_text="Testing",
        sort_order=5,
    )
    draft = type("Draft", (), {"id": 9})()
    fake_session = _FakeSession()

    fake_section_model = type("FakeSectionModel", (), {"query": _FakeSectionQuery([legacy_changes])})

    monkeypatch.setattr(csc_routes, "CSCSection", fake_section_model)
    monkeypatch.setattr(csc_routes.db, "session", fake_session)
    monkeypatch.setattr(csc_routes, "_infer_workflow_stream_name", lambda _draft: None)

    _set_draft_section_text(draft, "proposed_changes", "Updated proposed changes text")

    assert legacy_changes.section_name == "proposed_changes"
    assert legacy_changes.section_text == "Updated proposed changes text"
    assert legacy_changes.sort_order == 30


def test_parse_type_classification_workbook_across_multiple_sheets():
    workbook_buffer = _make_workbook(
        {
            "ONGC-DFC-03-2026": [
                _header(include_changes_column=True),
                [
                    "ONGC/DFC/03/2026",
                    "BARYTES",
                    "090001043",
                    "draft-1",
                    "param-1",
                    1,
                    "Density",
                    "4.10 (Minimum)",
                    "Desirable",
                    "Vital",
                    "Field Performance",
                    "",
                    "",
                    "",
                    "No",
                    "API",
                    "13A",
                    "None",
                ],
            ],
            "ONGC-CCA-03-2026": [
                _header(),
                [
                    "ONGC/CCA/03/2026",
                    "CEMENT",
                    "100200300",
                    "draft-2",
                    "param-2",
                    "2",
                    "Setting time",
                    "60 min",
                    "Desirable",
                    "Desirable",
                    "",
                    "",
                    "",
                    "",
                    "No",
                    "",
                    "",
                ],
            ],
        }
    )

    parsed = parse_type_classification_workbook(
        workbook_buffer,
        "Type_Classification_Exercise_2026_DFC&CCA.xlsx",
    )

    assert parsed.workbook_name == "Type_Classification_Exercise_2026_DFC&CCA.xlsx"
    assert [sheet.sheet_name for sheet in parsed.sheets] == [
        "ONGC-DFC-03-2026",
        "ONGC-CCA-03-2026",
    ]
    assert parsed.subset_codes == ["DFC", "CCA"]
    assert parsed.sheets[0].rows[0].spec_number == "ONGC/DFC/03/2026"
    assert parsed.sheets[0].rows[0].material_code == "090001043"
    assert parsed.sheets[0].rows[0].parameter_serial == 1
    assert parsed.sheets[0].rows[0].test_procedure_type == "API"
    assert parsed.sheets[1].rows[0].parameter_serial == 2


def test_import_creates_draft_and_normalizes_essential_to_desirable():
    workbook_buffer = _make_workbook(
        {
            "ONGC-DFC-03-2026": [
                _header(),
                [
                    "ONGC/DFC/03/2026",
                    "BARYTES",
                    "090001043",
                    "draft-1",
                    "param-1",
                    1,
                    "Density 4.1",
                    "4.10 (Minimum)",
                    "Desirable",
                    "Essential",
                    "",
                    "",
                    "",
                    "",
                    "No",
                    "",
                    "",
                ],
                [
                    "ONGC/DFC/03/2026",
                    "BARYTES",
                    "090001043",
                    "draft-1",
                    "param-2",
                    2,
                    "Density 4.2",
                    "4.20 (Minimum)",
                    "Desirable",
                    "Vital",
                    "Changed from Essential to Vital for field use",
                    "",
                    "",
                    "",
                    "No",
                    "astm",
                    "D-123",
                ],
            ]
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "bulk.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/DFC/03/2026",
        material_code="090001043",
        chemical_name="BARYTES",
    )
    draft = FakeDraft(
        id=20,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[
            FakeParameter(sort_order=1, parameter_name="Density 4.1", parameter_type="Vital"),
            FakeParameter(sort_order=2, parameter_name="Density 4.2", parameter_type="Desirable"),
        ],
    )
    sections = {"justification": ""}

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, True, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert summary.specs_matched == 1
    assert summary.drafts_created == 1
    assert summary.drafts_reused == 0
    assert summary.parameters_matched == 2
    assert summary.parameters_updated == 2
    assert summary.justification_entries_created == 1
    assert draft.parameters[0].parameter_type == "Desirable"
    assert draft.parameters[1].parameter_type == "Vital"
    assert draft.parameters[1].test_procedure_type == "ASTIM"
    assert draft.parameters[1].test_method == "ASTIM"
    assert draft.parameters[1].test_procedure_text == "D-123"
    assert "Desirable to Vital" in sections["justification"]
    assert IMPORTED_JUSTIFICATION_START in sections["justification"]
    assert IMPORTED_JUSTIFICATION_END in sections["justification"]


def test_import_reuses_existing_draft_and_replaces_only_imported_block():
    workbook_buffer = _make_workbook(
        {
            "ONGC-DFC-03-2026": [
                _header(),
                [
                    "ONGC/DFC/03/2026",
                    "BARYTES",
                    "090001043",
                    "draft-1",
                    "param-2",
                    2,
                    "Density 4.2",
                    "4.20 (Minimum)",
                    "Desirable",
                    "Vital",
                    "Operationally critical",
                    "",
                    "",
                    "",
                    "No",
                    "API",
                    "13A",
                ],
            ]
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "replacement.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/DFC/03/2026",
        material_code="090001043",
        chemical_name="BARYTES",
    )
    draft = FakeDraft(
        id=21,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[FakeParameter(sort_order=2, parameter_name="Density 4.2")],
    )
    sections = {
        "justification": (
            "Manual committee note.\n\n"
            f"{IMPORTED_JUSTIFICATION_START}\nWorkbook: old.xlsx\n"
            "Old import content\n"
            f"{IMPORTED_JUSTIFICATION_END}"
        )
    }

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, False, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert summary.drafts_created == 0
    assert summary.drafts_reused == 1
    assert sections["justification"].startswith("Manual committee note.")
    assert "replacement.xlsx" in sections["justification"]
    assert sections["justification"].count(IMPORTED_JUSTIFICATION_START) == 1
    assert "old.xlsx" not in sections["justification"]


def test_import_reports_unmatched_sheets_and_parameter_rows():
    workbook_buffer = _make_workbook(
        {
            "ONGC-DFC-03-2026": [
                _header(),
                [
                    "ONGC/DFC/03/2026",
                    "BARYTES",
                    "090001043",
                    "draft-1",
                    "param-99",
                    99,
                    "Unknown parameter",
                    "n/a",
                    "Desirable",
                    "Vital",
                    "Needed in field",
                    "",
                    "",
                    "",
                    "No",
                    "API",
                    "13A",
                ],
            ],
            "ONGC-DFC-04-2026": [
                _header(),
                [
                    "ONGC/DFC/04/2026",
                    "BENTONITE",
                    "100101003",
                    "draft-2",
                    "param-1",
                    1,
                    "Physical state",
                    "Powder",
                    "Desirable",
                    "Vital",
                    "Needed in field",
                    "",
                    "",
                    "",
                    "No",
                    "",
                    "",
                ],
            ],
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "report.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/DFC/03/2026",
        material_code="090001043",
        chemical_name="BARYTES",
    )
    draft = FakeDraft(
        id=22,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[FakeParameter(sort_order=1, parameter_name="Known parameter")],
    )
    sections = {"justification": ""}

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, False, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert len(summary.unmatched_sheets) == 1
    assert summary.unmatched_sheets[0].sheet_name == "ONGC-DFC-04-2026"
    assert len(summary.unmatched_parameter_rows) == 1
    assert summary.unmatched_parameter_rows[0].parameter_serial == 99
    assert "No draft parameter matched" in summary.unmatched_parameter_rows[0].reason


def test_import_matches_parent_by_material_code_even_when_workbook_spec_number_differs():
    workbook_buffer = _make_workbook(
        {
            "ONGC-CCA-03-2026": [
                _header(),
                [
                    "ONGC/CCA/99/2026",
                    "CEMENT CHANGED",
                    "100200300",
                    "draft-1",
                    "param-1",
                    1,
                    "Setting time",
                    "60 min",
                    "Desirable",
                    "Vital",
                    "Operational need",
                    "",
                    "",
                    "",
                    "No",
                    "",
                    "",
                ],
            ]
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "fallback.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/CCA/03/2026",
        material_code="100200300",
        chemical_name="CEMENT",
    )
    draft = FakeDraft(
        id=20,
        spec_number=parent.spec_number,
        material_code="100200300",
        chemical_name=parent.chemical_name,
        parameters=[FakeParameter(sort_order=1, parameter_name="Setting time")],
    )
    sections = {"justification": ""}

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, True, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert summary.specs_matched == 1
    assert summary.unmatched_sheets == []
    assert draft.parameters[0].parameter_type == "Vital"


def test_import_does_not_match_when_material_code_differs_even_if_spec_number_matches():
    workbook_buffer = _make_workbook(
        {
            "ONGC-DFC-03-2026": [
                _header(),
                [
                    "ONGC/DFC/03/2026",
                    "BARYTES",
                    "090001044",
                    "draft-1",
                    "param-1",
                    1,
                    "Density",
                    "4.10 (Minimum)",
                    "Desirable",
                    "Vital",
                    "Field Performance",
                    "",
                    "",
                    "",
                    "No",
                    "",
                    "",
                ],
            ]
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "numeric-mismatch.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/DFC/03/2026",
        material_code="090001043",
        chemical_name="BARYTES",
    )
    draft = FakeDraft(
        id=20,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[FakeParameter(sort_order=1, parameter_name="Density")],
    )
    sections = {"justification": ""}

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, True, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert summary.specs_matched == 0
    assert len(summary.unmatched_sheets) == 1
    assert summary.unmatched_sheets[0].reason == "No published parent specification matched this sheet's Material Code."


def test_import_cleaned_workbook_shape_uses_parameter_serial_for_reason_entries():
    workbook_buffer = _make_workbook(
        {
            "1. ONGC-DFC-01-2026": [
                [
                    "Spec Number",
                    "Chemical Name",
                    "Material Code",
                    "Draft ID",
                    "Parameter ID",
                    "S. No.",
                    "Current Type",
                    "Type",
                    "Vital Reason 1",
                    "Vital Reason 2",
                    "Vital Reason 3",
                    "Vital Reason 4",
                ],
                [
                    "ONGC/DFC/01/2026",
                    "ALUMINIUM STEARATE",
                    "100101102",
                    "draft-1",
                    "param-1",
                    1,
                    "Desirable",
                    "Vital",
                    "Process Efficiency",
                    "Field Performance",
                    "",
                    "",
                ],
            ]
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "cleaned-first-18.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    draft = FakeDraft(
        id=20,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[FakeParameter(sort_order=1, parameter_name="Physical state")],
    )
    sections = {"justification": ""}

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, True, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert summary.specs_matched == 1
    assert summary.parameters_matched == 1
    assert "Parameter 1\nType classification: Vital" in sections["justification"]
    assert "Parameter 1 - Parameter 1" not in sections["justification"]


def test_import_matches_cleaned_workbook_serials_against_zero_based_parameter_order():
    workbook_buffer = _make_workbook(
        {
            "1. ONGC-DFC-01-2026": [
                [
                    "Spec Number",
                    "Chemical Name",
                    "Material Code",
                    "Draft ID",
                    "Parameter ID",
                    "S. No.",
                    "Current Type",
                    "Type",
                    "Vital Reason 1",
                ],
                [
                    "ONGC/DFC/01/2026",
                    "ALUMINIUM STEARATE",
                    "100101102",
                    "draft-1",
                    "param-1",
                    1,
                    "Desirable",
                    "Vital",
                    "Process Efficiency",
                ],
                [
                    "ONGC/DFC/01/2026",
                    "ALUMINIUM STEARATE",
                    "100101102",
                    "draft-1",
                    "param-2",
                    2,
                    "Desirable",
                    "Desirable",
                    "",
                ],
            ]
        }
    )
    workbook = parse_type_classification_workbook(workbook_buffer, "zero-based-order.xlsx")

    parent = FakeParentDraft(
        id=10,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    draft = FakeDraft(
        id=20,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[
            FakeParameter(sort_order=0, parameter_name="Physical state", parameter_type="Desirable"),
            FakeParameter(sort_order=1, parameter_name="Moisture content", parameter_type="Desirable"),
        ],
    )
    sections = {"justification": ""}

    summary = import_type_classification_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, True, None),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
    )

    assert summary.parameters_matched == 2
    assert draft.parameters[0].parameter_type == "Vital"
    assert draft.parameters[1].parameter_type == "Desirable"


def test_parse_material_handling_workbook_and_normalize_spec_numbers():
    workbook_buffer = _make_workbook(
        {
            "Sheet1": [
                _material_header(),
                [
                    1,
                    "Aluminum Stearate",
                    100101102,
                    "ONGC / MC / 01 / 2015",
                    "ONGC / DFC / 01 / 2026",
                    "KG",
                    "Officer A",
                    "Solid",
                    "No",
                    "No",
                    "Yes",
                    "Ambient",
                    "Stable",
                    "No",
                    "Low",
                    "No",
                    "Store in dry place",
                    "",
                    "HDPE Bags",
                    "25 Kg",
                    "New HDPE bag",
                    "Zone-1: General Stable",
                    "No major issue",
                ],
            ]
        }
    )

    parsed = parse_material_handling_workbook(workbook_buffer, "Chemical Properties Register.xlsx")

    assert parsed.workbook_name == "Chemical Properties Register.xlsx"
    assert len(parsed.rows) == 1
    assert parsed.rows[0].source_spec_number == "ONGC/MC/01/2015"
    assert parsed.rows[0].proposed_spec_number == "ONGC/DFC/01/2026"
    assert parsed.rows[0].material_code == "100101102"
    assert parsed.subset_codes == ["DFC"]


def test_parse_material_handling_workbook_accepts_uploaded_shape_without_optional_columns():
    workbook_buffer = _make_workbook(
        {
            "Sheet1": [
                _material_header(
                    include_chemical_name=False,
                    include_source_spec_number=False,
                    include_officers_responsible=False,
                ),
                [
                    1,
                    100101102,
                    "ONGC / DFC / 01 / 2026",
                    "KG",
                    "Solid",
                    "No",
                    "No",
                    "Yes",
                    "Ambient",
                    "Stable",
                    "No",
                    "Low",
                    "No",
                    "Store in dry place",
                    "",
                    "HDPE Bags",
                    "25 Kg",
                    "New HDPE bag",
                    "Zone-1: General Stable",
                    "No major issue",
                ],
            ]
        }
    )

    parsed = parse_material_handling_workbook(workbook_buffer, "uploaded-shape.xlsx")

    assert len(parsed.rows) == 1
    assert parsed.invalid_rows == []
    assert parsed.rows[0].chemical_name == ""
    assert parsed.rows[0].source_spec_number == ""
    assert parsed.rows[0].proposed_spec_number == "ONGC/DFC/01/2026"
    assert parsed.rows[0].material_code == "100101102"


def test_import_material_handling_reuses_separate_draft_stream_and_stages_only_draft_values():
    workbook_buffer = _make_workbook(
        {
            "Sheet1": [
                _material_header(),
                [
                    1,
                    "Aluminum Stearate",
                    100101102,
                    "ONGC / MC / 01 / 2015",
                    "ONGC / DFC / 01 / 2026",
                    "KG",
                    "Officer A",
                    "Solid",
                    "No",
                    "No",
                    "Yes",
                    "Ambient",
                    "Stable",
                    "No",
                    "Low",
                    "No",
                    "Store in dry place",
                    "",
                    "HDPE Bags",
                    "25 Kg",
                    "New HDPE bag",
                    "Zone-1: General Stable",
                    "Imported remarks",
                ],
            ]
        }
    )
    workbook = parse_material_handling_workbook(workbook_buffer, "mh.xlsx")

    parent = FakeParentDraft(
        id=30,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    draft = FakeDraft(
        id=40,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
    )
    sections = {"background": "", "justification": ""}
    staged_payloads: list[dict[str, str]] = []

    summary = import_material_handling_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, False, None),
        get_background_text=lambda _draft: sections["background"],
        set_background_text=lambda _draft, text: sections.__setitem__("background", text),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
        stage_master_values=lambda _draft, values: staged_payloads.append(dict(values)) or len(values),
    )

    assert summary.drafts_created == 0
    assert summary.drafts_reused == 1
    assert summary.rows_updated == 1
    assert draft.chemical_name == "Aluminum Stearate"
    assert draft.spec_number == "ONGC/DFC/01/2026"
    assert parent.chemical_name == "ALUMINIUM STEARATE"
    assert parent.spec_number == "ONGC/DFC/01/2026"
    assert staged_payloads[0]["physical_state"] == "Solid"
    assert staged_payloads[0]["container_type"] == "HDPE Bags"
    assert "Material Code: 100101102" in sections["background"]
    assert "[" not in sections["background"]
    assert "Proposed specification number - ONGC/DFC/01/2026" in sections["justification"]
    assert "[" not in sections["justification"]


def test_import_material_handling_backfills_parent_values_when_workbook_omits_them():
    workbook_buffer = _make_workbook(
        {
            "Sheet1": [
                _material_header(
                    include_chemical_name=False,
                    include_source_spec_number=False,
                    include_officers_responsible=False,
                ),
                [
                    1,
                    100101102,
                    "ONGC / DFC / 05 / 2026",
                    "KG",
                    "Solid",
                    "No",
                    "No",
                    "Yes",
                    "Ambient",
                    "Stable",
                    "No",
                    "Low",
                    "No",
                    "Store in dry place",
                    "",
                    "HDPE Bags",
                    "25 Kg",
                    "New HDPE bag",
                    "Zone-1: General Stable",
                    "Imported remarks",
                ],
            ]
        }
    )
    workbook = parse_material_handling_workbook(workbook_buffer, "mh-uploaded.xlsx")

    parent = FakeParentDraft(
        id=31,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    draft = FakeDraft(
        id=41,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
    )
    sections = {"background": "", "justification": ""}

    summary = import_material_handling_workbook(
        workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (draft, True, None),
        get_background_text=lambda _draft: sections["background"],
        set_background_text=lambda _draft, text: sections.__setitem__("background", text),
        get_justification_text=lambda _draft: sections["justification"],
        set_justification_text=lambda _draft, text: sections.__setitem__("justification", text),
        stage_master_values=lambda _draft, values: len(values),
    )

    assert summary.specs_matched == 1
    assert summary.details[0].source_spec_number == "ONGC/DFC/01/2026"
    assert summary.details[0].chemical_name == "ALUMINIUM STEARATE"
    assert draft.spec_number == "ONGC/DFC/05/2026"
    assert draft.chemical_name == "ALUMINIUM STEARATE"
    assert "Current Specification Number: ONGC/DFC/01/2026" in sections["background"]
    assert "Chemical Name: ALUMINIUM STEARATE" in sections["background"]


def test_import_material_handling_reports_unmatched_rows():
    workbook_buffer = _make_workbook(
        {
            "Sheet1": [
                _material_header(),
                [
                    1,
                    "Unknown Chemical",
                    999999999,
                    "ONGC / MC / 99 / 2015",
                    "ONGC / DFC / 99 / 2026",
                    "KG",
                    "Officer A",
                    "Solid",
                    "No",
                    "No",
                    "Yes",
                    "Ambient",
                    "Stable",
                    "No",
                    "Low",
                    "No",
                    "Store in dry place",
                    "",
                    "HDPE Bags",
                    "25 Kg",
                    "New HDPE bag",
                    "Zone-1: General Stable",
                    "",
                ],
            ]
        }
    )
    workbook = parse_material_handling_workbook(workbook_buffer, "mh-unmatched.xlsx")

    summary = import_material_handling_workbook(
        workbook,
        build_parent_lookup_by_material_code([]),
        open_draft_for_parent=lambda _parent: (None, False, None),
        get_background_text=lambda _draft: "",
        set_background_text=lambda _draft, text: None,
        get_justification_text=lambda _draft: "",
        set_justification_text=lambda _draft, text: None,
        stage_master_values=lambda _draft, values: 0,
    )

    assert summary.specs_matched == 0
    assert len(summary.unmatched_rows) == 1
    assert summary.unmatched_rows[0].material_code == "999999999"


def test_material_handling_core_updates_apply_on_publish():
    parent = FakeParentDraft(
        id=50,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    approved_draft = FakeDraft(
        id=51,
        spec_number="ONGC/DFC/05/2026",
        material_code="100101102",
        chemical_name="Aluminum Stearate Updated",
    )

    changed = apply_material_handling_core_updates(parent, approved_draft)

    assert changed is True
    assert parent.spec_number == "ONGC/DFC/05/2026"
    assert parent.chemical_name == "Aluminum Stearate Updated"


def test_type_and_material_handling_imports_can_target_separate_drafts_for_same_parent():
    type_workbook = parse_type_classification_workbook(
        _make_workbook(
            {
                "ONGC-DFC-03-2026": [
                    _header(),
                    [
                        "ONGC/DFC/03/2026",
                        "BARYTES",
                        "090001043",
                        "draft-1",
                        "param-1",
                        1,
                        "Density",
                        "4.10 (Minimum)",
                        "Desirable",
                        "Vital",
                        "Field Performance",
                        "",
                        "",
                        "",
                        "No",
                        "API",
                        "13A",
                    ],
                ],
            }
        ),
        "type.xlsx",
    )
    material_workbook = parse_material_handling_workbook(
        _make_workbook(
            {
                "Sheet1": [
                    _material_header(),
                    [
                        1,
                        "Barytes",
                        90001043,
                        "ONGC / MC / 03 / 2015",
                        "ONGC / DFC / 03 / 2026",
                        "MT",
                        "Officer A",
                        "Solid",
                        "No",
                        "No",
                        "Yes",
                        "Ambient",
                        "Stable",
                        "No",
                        "Low",
                        "No",
                        "Store in dry place",
                        "",
                        "HDPE Bags",
                        "50 Kg",
                        "New HDPE bag",
                        "Zone-1: General Stable",
                        "",
                    ],
                ],
            }
        ),
        "mh.xlsx",
    )

    parent = FakeParentDraft(
        id=60,
        spec_number="ONGC/DFC/03/2026",
        material_code="090001043",
        chemical_name="BARYTES",
    )
    type_draft = FakeDraft(
        id=61,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
        parameters=[FakeParameter(sort_order=1, parameter_name="Density")],
    )
    material_draft = FakeDraft(
        id=62,
        spec_number=parent.spec_number,
        material_code=parent.material_code,
        chemical_name=parent.chemical_name,
    )

    type_summary = import_type_classification_workbook(
        type_workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (type_draft, True, None),
        get_justification_text=lambda _draft: "",
        set_justification_text=lambda _draft, text: None,
    )
    material_summary = import_material_handling_workbook(
        material_workbook,
        build_parent_lookup_by_material_code([parent]),
        open_draft_for_parent=lambda _parent: (material_draft, True, None),
        get_background_text=lambda _draft: "",
        set_background_text=lambda _draft, text: None,
        get_justification_text=lambda _draft: "",
        set_justification_text=lambda _draft, text: None,
        stage_master_values=lambda _draft, values: len(values),
    )

    assert type_summary.drafts_created == 1
    assert material_summary.drafts_created == 1
    assert type_summary.details[0].draft_id != material_summary.details[0].draft_id


def test_material_handling_section_defaults_cover_background_changes_and_reason():
    defaults = {
        section_name: text
        for section_name, text, _sort_order in _default_section_rows("material_handling")
    }

    assert defaults["background"].startswith("Background /")
    assert defaults["proposed_changes"] == _default_proposed_changes_section_text("material_handling")
    assert "material handling" in defaults["proposed_changes"].lower()
    assert defaults["recommendation"] == _default_recommendation_section_text("material_handling")
    assert defaults["recommendation"] == "Migration to 2026 version with definition of material properties, handling and storage conditions"


def test_merge_material_handling_section_block_drops_testing_placeholder():
    merged = merge_material_handling_section_block(
        "Testing",
        "Imported material handling text",
        start_marker=MATERIAL_HANDLING_JUSTIFICATION_START,
        end_marker=MATERIAL_HANDLING_JUSTIFICATION_END,
    )

    assert merged == "Imported material handling text"


def test_material_handling_staged_payload_round_trip():
    workbook = parse_material_handling_workbook(
        _make_workbook(
            {
                "Sheet1": [
                    _material_header(
                        include_chemical_name=False,
                        include_source_spec_number=False,
                        include_officers_responsible=False,
                    ),
                    [
                        1,
                        100101102,
                        "ONGC / DFC / 01 / 2026",
                        "KG",
                        "Solid",
                        "No",
                        "No",
                        "Yes",
                        "Ambient",
                        "Stable",
                        "No",
                        "Low",
                        "No",
                        "Store in dry place",
                        "",
                        "HDPE Bags",
                        "25 Kg",
                        "New HDPE bag",
                        "Zone-1: General Stable",
                        "No major issue",
                    ],
                ]
            }
        ),
        "preview.xlsx",
    )

    token = _stage_material_handling_workbook_payload(workbook)
    try:
        loaded = _load_staged_material_handling_workbook_payload(token)
        assert loaded.workbook_name == "preview.xlsx"
        assert len(loaded.rows) == 1
        assert loaded.rows[0].material_code == "100101102"
        assert loaded.rows[0].proposed_spec_number == "ONGC/DFC/01/2026"
    finally:
        _discard_staged_material_handling_workbook_payload(token)


def test_material_handling_preview_comparisons_show_current_workbook_and_staged_values():
    workbook = parse_material_handling_workbook(
        _make_workbook(
            {
                "Sheet1": [
                    _material_header(
                        include_chemical_name=False,
                        include_source_spec_number=False,
                        include_officers_responsible=False,
                    ),
                    [
                        1,
                        100101102,
                        "ONGC / DFC / 05 / 2026",
                        "KG",
                        "Solid",
                        "No",
                        "No",
                        "Yes",
                        "Ambient",
                        "Stable",
                        "No",
                        "Low",
                        "No",
                        "Store in dry place",
                        "",
                        "HDPE Bags",
                        "25 Kg",
                        "New HDPE bag",
                        "Zone-1: General Stable",
                        "Imported remarks",
                    ],
                ]
            }
        ),
        "preview-compare.xlsx",
    )
    parent = FakeParentDraft(
        id=70,
        spec_number="ONGC/DFC/01/2026",
        material_code="100101102",
        chemical_name="ALUMINIUM STEARATE",
    )
    resolved = _resolve_material_handling_row_for_preview(workbook.rows[0], parent)

    with patch(
        "app.modules.csc.routes.get_master_form_values",
        return_value={
            "physical_state": "Liquid",
            "container_type": "Drum",
            "storage_conditions_general": "Legacy storage guidance",
        },
    ):
        comparisons = _material_handling_preview_comparisons(parent, workbook.rows[0], resolved)

    by_label = {item["label"]: item for item in comparisons}
    assert by_label["Specification No."]["current_value"] == "ONGC/DFC/01/2026"
    assert by_label["Specification No."]["workbook_value"] == "ONGC/DFC/05/2026"
    assert by_label["Specification No."]["staged_value"] == "ONGC/DFC/05/2026"
    assert by_label["Specification No."]["changed"] is True
    assert by_label["Chemical Name"]["current_value"] == "ALUMINIUM STEARATE"
    assert by_label["Chemical Name"]["workbook_value"] == ""
    assert by_label["Chemical Name"]["staged_value"] == "ALUMINIUM STEARATE"
    assert by_label["Physical State"]["current_value"] == "Liquid"
    assert by_label["Physical State"]["staged_value"] == "Solid"
