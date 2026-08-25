"""The management lab panel and PDF read the same controlled assignments."""

from app.core.services import csc_management


def _entry(ref, name, subgroup="DFC", spec="ONGC/DFC/01/2026", material=""):
    return {
        "ref": ref,
        "chemical_name": name,
        "spec_number": spec,
        "material_code": material,
        "category": subgroup,
        "category_label": f"{subgroup} chemicals",
        "standard_days": 4,
    }


def _assignment_source(monkeypatch):
    laboratories = [
        {"code": "lab-a", "name": "Alpha Laboratory", "location": "Mumbai", "description": "Primary"},
        {"code": "lab-b", "name": "Beta Laboratory", "location": "Chennai", "description": "Primary"},
        {"code": "lab-c", "name": "Gamma Laboratory", "location": "Delhi", "description": "Reserve"},
    ]
    monkeypatch.setattr(csc_management, "laboratory_options", lambda: laboratories)
    monkeypatch.setattr(
        csc_management,
        "authorized_labs_index",
        lambda: {"r-1": ["lab-a", "lab-b"], "r-2": ["lab-b"]},
    )
    return [
        _entry("r-1", "Alpha Chemical", material="100000001"),
        _entry("r-2", "Beta Chemical", subgroup="WIC", spec="ONGC/WIC/01/2026"),
        _entry("r-3", "Gamma Chemical", subgroup="WS", spec="ONGC/WS/01/2026"),
    ]


def test_assignment_analytics_answers_both_laboratory_and_chemical_questions(monkeypatch):
    data = csc_management.laboratory_assignment_analytics(_assignment_source(monkeypatch))

    assert data["summary"] == {
        "chemicals": 3,
        "assigned_chemicals": 2,
        "unassigned_chemicals": 1,
        "laboratories": 3,
        "active_laboratories": 2,
        "assignments": 3,
        "multi_lab_chemicals": 1,
    }
    assert [(row["code"], row["chemical_total"]) for row in data["laboratories"]] == [
        ("lab-b", 2), ("lab-a", 1), ("lab-c", 0),
    ]
    assert [lab["name"] for lab in data["chemicals"][0]["laboratories"]] == [
        "Alpha Laboratory", "Beta Laboratory",
    ]
    assert not data["chemicals"][2]["is_authorised"]


def test_authorised_laboratory_pdf_is_a_readable_chemical_directory(monkeypatch):
    document = csc_management.build_authorised_laboratory_list_pdf(_assignment_source(monkeypatch))

    assert document.startswith(b"%PDF")
    # Several rows plus the Corporate Chemistry header and logo should produce
    # a substantive one-page directory, not an empty response.
    assert len(document) > 8_000
