from __future__ import annotations

"""Committee directory data for Material Master Management landing page."""

from pathlib import Path


ROOT_COMMITTEE = {
    "slug": "corporate-specification-committee",
    "title": "Corporate Specification Committee (Oil Field Chemicals)",
    "kind": "Apex Committee",
    "tone": "indigo",
    "summary": "Apex governance committee for oil field chemical material masters, specification approval, review alignment, and constitution of subcommittees.",
    "subsets": [
        {"code": "DFC", "label": "Drilling Fluid Chemicals"},
        {"code": "CCA", "label": "Cement & Cement Additive"},
        {"code": "WCF", "label": "Well Completion Fluid"},
        {"code": "WS", "label": "Well Stimulation Chemicals"},
        {"code": "PC", "label": "Production Chemicals"},
        {"code": "WIC", "label": "Water Injection Chemicals"},
        {"code": "WM", "label": "Well Maker Chemicals"},
        {"code": "UTL", "label": "Utility Chemicals"},
        {"code": "LPG", "label": "Plant Chemicals (LPG)"},
    ],
    "members": [],
    "members_note": "Apex committee members are defined through the governing office order.",
    "office_orders": ["review-committees-corporate-specification"],
}


CHILD_COMMITTEES = [
    {
        "slug": "coordination",
        "title": "Coordination Committee",
        "kind": "Governance",
        "tone": "indigo",
        "summary": "Central coordination across CSC and all material subset committees.",
        "subsets": [],
        "members": [],
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["review-committees-corporate-specification"],
    },
    {
        "slug": "committee-1",
        "title": "Material Subset Management Committee 1",
        "kind": "Subset Committee",
        "tone": "blue",
        "summary": "Oversight for drilling fluid and cement-related master materials.",
        "subsets": [
            {"code": "DFC", "label": "Drilling Fluid Chemicals"},
            {"code": "CCA", "label": "Cement & Cement Additive"},
        ],
        "members": [],
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["review-committees-corporate-specification"],
    },
    {
        "slug": "committee-2",
        "title": "Material Subset Management Committee 2",
        "kind": "Subset Committee",
        "tone": "green",
        "summary": "Oversight for well stimulation materials.",
        "subsets": [
            {"code": "WS", "label": "Well Stimulation Chemicals"},
        ],
        "members": [],
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["review-committees-corporate-specification"],
    },
    {
        "slug": "committee-3",
        "title": "Material Subset Management Committee 3",
        "kind": "Subset Committee",
        "tone": "teal",
        "summary": "Oversight for production and water injection chemicals.",
        "subsets": [
            {"code": "PC", "label": "Production Chemicals"},
            {"code": "WIC", "label": "Water Injection Chemicals"},
        ],
        "members": [],
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["review-committees-corporate-specification"],
    },
    {
        "slug": "committee-4",
        "title": "Material Subset Management Committee 4",
        "kind": "Subset Committee",
        "tone": "amber",
        "summary": "Oversight for well maker, utility, and LPG plant chemicals.",
        "subsets": [
            {"code": "WM", "label": "Well Maker Chemicals"},
            {"code": "UTL", "label": "Utility Chemicals"},
            {"code": "LPG", "label": "Plant Chemicals (LPG)"},
        ],
        "members": [],
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["review-committees-corporate-specification"],
    },
    {
        "slug": "material-handling",
        "title": "Material Handling Committee",
        "kind": "Support Committee",
        "tone": "rose",
        "summary": "Focus on storage conditions, material handling, and preservation controls.",
        "subsets": [],
        "members": [],
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["storage-conditions-material-handling"],
    },
]


OFFICE_ORDERS = [
    {
        "slug": "review-committees-corporate-specification",
        "title": "Review Committees of Corporate Specification",
        "issued_label": "Office Order issued on 06.03.2026",
        "summary": "Formation order for review committees governing the corporate specification structure.",
        "path": Path("/Users/dineshreddy/Downloads/Review committees of Corporate Specification 06.03.2026.pdf"),
    },
    {
        "slug": "storage-conditions-material-handling",
        "title": "Review Committee for Storage Conditions & Material Handling",
        "issued_label": "Office Order",
        "summary": "Formation order for the storage conditions and material handling committee.",
        "path": Path("/Users/dineshreddy/Downloads/Office Order_Review Committee_Storage Conditions_Material Handling.pdf"),
    },
]


def get_committee_directory() -> list[dict]:
    """Return the landing-page committee directory structure."""
    return [ROOT_COMMITTEE, *CHILD_COMMITTEES]


def get_committee_tree() -> dict[str, object]:
    """Return committees grouped by visual tree level."""
    return {
        "root": ROOT_COMMITTEE,
        "level_one": [committee for committee in CHILD_COMMITTEES if committee["slug"] == "coordination"],
        "level_two": [committee for committee in CHILD_COMMITTEES if committee["slug"] != "coordination"],
    }


def get_office_orders() -> list[dict]:
    """Return office-order metadata with file availability flags."""
    orders = []
    for order in OFFICE_ORDERS:
        orders.append(
            {
                "slug": order["slug"],
                "title": order["title"],
                "issued_label": order["issued_label"],
                "summary": order["summary"],
                "available": order["path"].exists(),
                "filename": order["path"].name,
            }
        )
    return orders
