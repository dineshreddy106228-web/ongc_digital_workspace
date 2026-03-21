from __future__ import annotations

"""Committee directory data for Material Master Management landing page."""

import json
from builtins import Exception
from app.core.services.csc_utils import SPEC_SUBSET_ORDER
from app.extensions import db


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
    "office_orders": ["csc-governing-office-order"],
    "committee_user": "",
    "committee_users": [],
    "committee_head": "",
}


CHILD_COMMITTEES = [
    {
        "slug": "coordination",
        "title": "Governance Committee",
        "kind": "Governance",
        "tone": "indigo",
        "summary": "Central coordination across CSC and all material subset committees.",
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
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["review-committees-corporate-specification"],
        "committee_user": "",
        "committee_users": [],
        "committee_head": "",
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
        "committee_user": "",
        "committee_users": [],
        "committee_head": "",
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
        "committee_user": "",
        "committee_users": [],
        "committee_head": "",
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
        "committee_user": "",
        "committee_users": [],
        "committee_head": "",
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
        "committee_user": "",
        "committee_users": [],
        "committee_head": "",
    },
    {
        "slug": "material-handling",
        "title": "Material Handling Committee",
        "kind": "Support Committee",
        "tone": "rose",
        "summary": "Focus on storage conditions, material handling, and preservation controls.",
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
        "members_note": "Members are notified through the applicable office order.",
        "office_orders": ["storage-conditions-material-handling"],
        "committee_user": "",
        "committee_users": [],
        "committee_head": "",
    },
]


OFFICE_ORDERS = [
    {
        "slug": "csc-governing-office-order",
        "title": "CSC Governing Office Order",
        "issued_label": "Office Order",
        "summary": "Primary governing office order for the Corporate Specification Committee (Oil Field Chemicals).",
        "filename": "CSC_Governing_Office_Order.pdf",
    },
    {
        "slug": "review-committees-corporate-specification",
        "title": "Review Committees of Corporate Specification",
        "issued_label": "Office Order issued on 06.03.2026",
        "summary": "Formation order for review committees governing the corporate specification structure.",
        "filename": "Review committees of Corporate Specification 06.03.2026.pdf",
    },
    {
        "slug": "storage-conditions-material-handling",
        "title": "Review Committee for Storage Conditions & Material Handling",
        "issued_label": "Office Order",
        "summary": "Formation order for the storage conditions and material handling committee.",
        "filename": "Office Order_Review Committee_Storage Conditions_Material Handling.pdf",
    },
]


DEFAULT_ROOT_COMMITTEE = json.loads(json.dumps(ROOT_COMMITTEE))
DEFAULT_CHILD_COMMITTEES = json.loads(json.dumps(CHILD_COMMITTEES))
DEFAULT_OFFICE_ORDERS = json.loads(json.dumps(OFFICE_ORDERS))
DEFAULT_COMMITTEE_BY_SLUG = {
    committee["slug"]: json.loads(json.dumps(committee))
    for committee in [DEFAULT_ROOT_COMMITTEE, *DEFAULT_CHILD_COMMITTEES]
}


def _clone_json(value):
    return json.loads(json.dumps(value))


def _normalize_committee_entry(committee: dict | None, fallback: dict | None) -> dict:
    base = _clone_json(fallback or {})
    incoming = committee if isinstance(committee, dict) else {}
    for key, value in incoming.items():
        if value is not None:
            base[key] = value

    if base.get("slug") == "coordination":
        base["title"] = "Governance Committee"

    base["members"] = [
        str(member).strip()
        for member in (base.get("members") or [])
        if str(member).strip()
    ]
    base["office_orders"] = [
        str(order_slug).strip()
        for order_slug in (base.get("office_orders") or [])
        if str(order_slug).strip()
    ]
    base["committee_head"] = str(base.get("committee_head") or "").strip()

    normalized_subsets = []
    for subset in base.get("subsets") or []:
        if isinstance(subset, dict):
            code = str(subset.get("code") or "").strip().upper()
            label = str(subset.get("label") or "").strip()
        else:
            code = str(subset or "").strip().upper()
            label = ""
        if code:
            normalized_subsets.append({"code": code, "label": label or code})
    base["subsets"] = normalized_subsets

    all_subset_codes = [subset["code"] for subset in normalized_subsets]
    incoming_assignments = base.get("committee_users") or []
    if not incoming_assignments and str(base.get("committee_user") or "").strip():
        incoming_assignments = [{"username": str(base.get("committee_user") or "").strip()}]

    normalized_assignments = []
    seen_usernames = set()
    for entry in incoming_assignments:
        if isinstance(entry, dict):
            username = str(entry.get("username") or "").strip()
            raw_subset_codes = entry.get("subset_codes") or []
        else:
            username = str(entry or "").strip()
            raw_subset_codes = []
        username_key = username.lower()
        if not username or username_key in seen_usernames:
            continue
        seen_usernames.add(username_key)

        subset_codes = []
        for code in raw_subset_codes:
            normalized_code = str(code or "").strip().upper()
            if normalized_code and normalized_code in all_subset_codes and normalized_code not in subset_codes:
                subset_codes.append(normalized_code)

        normalized_assignments.append(
            {
                "username": username,
                "subset_codes": subset_codes,
            }
        )
        if len(normalized_assignments) >= 4:
            break

    base["committee_users"] = normalized_assignments
    base["committee_user"] = (
        normalized_assignments[0]["username"] if normalized_assignments else str(base.get("committee_user") or "").strip()
    )
    return base


def normalize_committee_config_payload(payload: dict[str, object] | None) -> dict[str, object]:
    """Normalize a posted governance payload before persisting it."""
    data = payload if isinstance(payload, dict) else {}
    root_committee = _normalize_committee_entry(
        data.get("ROOT_COMMITTEE"),
        DEFAULT_ROOT_COMMITTEE,
    )

    child_committees = []
    seen_slugs = set()
    for committee in data.get("CHILD_COMMITTEES") or []:
        if not isinstance(committee, dict):
            continue
        slug = str(committee.get("slug") or "").strip()
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        child_committees.append(
            _normalize_committee_entry(
                committee,
                DEFAULT_COMMITTEE_BY_SLUG.get(slug, {"slug": slug}),
            )
        )

    for committee in DEFAULT_CHILD_COMMITTEES:
        slug = committee["slug"]
        if slug in seen_slugs:
            continue
        child_committees.append(_clone_json(committee))

    office_orders = [
        _clone_json(order)
        for order in (data.get("OFFICE_ORDERS") or DEFAULT_OFFICE_ORDERS)
        if isinstance(order, dict)
    ] or [_clone_json(order) for order in DEFAULT_OFFICE_ORDERS]

    return {
        "ROOT_COMMITTEE": root_committee,
        "CHILD_COMMITTEES": child_committees,
        "OFFICE_ORDERS": office_orders,
    }


def get_committee_config_payload() -> dict[str, object]:
    """Return the normalized committee configuration payload."""
    payload = {
        "ROOT_COMMITTEE": _clone_json(DEFAULT_ROOT_COMMITTEE),
        "CHILD_COMMITTEES": [_clone_json(committee) for committee in DEFAULT_CHILD_COMMITTEES],
        "OFFICE_ORDERS": [_clone_json(order) for order in DEFAULT_OFFICE_ORDERS],
    }

    try:
        from app.models.csc.governance import CSCConfig

        config = db.session.query(CSCConfig).first()
        if config and config.directory_json:
            raw = json.loads(config.directory_json)
            payload = normalize_committee_config_payload(raw)
    except Exception:
        pass

    return payload


def get_committee_access_for_username(username: str | None) -> dict[str, object]:
    """Return committee assignments and covered subset codes for a username."""
    username_key = str(username or "").strip().lower()
    if not username_key:
        return {"committee_slugs": [], "committee_titles": [], "subset_codes": []}

    payload = get_committee_config_payload()
    subset_codes = set()
    committee_slugs = []
    committee_titles = []

    for committee in [payload["ROOT_COMMITTEE"], *payload["CHILD_COMMITTEES"]]:
        matched_committee_user = None
        for assignment in committee.get("committee_users") or []:
            assignment_username = str((assignment or {}).get("username") or "").strip().lower()
            if assignment_username == username_key:
                matched_committee_user = assignment or {}
                break
        committee_head = str(committee.get("committee_head") or "").strip().lower()
        if matched_committee_user is None and username_key != committee_head:
            continue

        committee_slugs.append(committee.get("slug") or "")
        committee_titles.append(committee.get("title") or "")
        if matched_committee_user is not None and committee.get("slug") == "material-handling":
            effective_subsets = [
                {"code": code}
                for code in ((matched_committee_user or {}).get("subset_codes") or [])
            ]
        else:
            effective_subsets = committee.get("subsets") or []

        for subset in effective_subsets:
            code = str((subset or {}).get("code") or "").strip().upper()
            if code:
                subset_codes.add(code)

    ordered_codes = [code for code in SPEC_SUBSET_ORDER if code in subset_codes]
    ordered_codes.extend(sorted(code for code in subset_codes if code not in SPEC_SUBSET_ORDER))
    return {
        "committee_slugs": committee_slugs,
        "committee_titles": committee_titles,
        "subset_codes": ordered_codes,
    }


def get_committee_directory() -> list[dict]:
    """Return the landing-page committee directory structure."""
    payload = get_committee_config_payload()
    return [payload["ROOT_COMMITTEE"], *payload["CHILD_COMMITTEES"]]


def get_committee_tree() -> dict[str, object]:
    """Return committees grouped by visual tree level."""
    payload = get_committee_config_payload()
    children = payload["CHILD_COMMITTEES"]
    return {
        "root": payload["ROOT_COMMITTEE"],
        "level_one": [committee for committee in children if committee.get("slug") == "coordination"],
        "level_two": [committee for committee in children if committee.get("slug") != "coordination"],
    }


def get_office_orders() -> list[dict]:
    """Return office-order metadata with DB-backed availability flags."""
    order_defs = get_committee_config_payload().get("OFFICE_ORDERS", DEFAULT_OFFICE_ORDERS)

    orders = []
    try:
        from app.models.csc.governance import CSCOfficeOrderFile

        existing_slugs = {row.slug for row in db.session.query(CSCOfficeOrderFile.slug).all()}
    except Exception:
        existing_slugs = set()

    for order in order_defs:
        if order["slug"] in existing_slugs:
            orders.append(
                {
                    "slug": order["slug"],
                    "title": order["title"],
                    "issued_label": order["issued_label"],
                    "summary": order["summary"],
                    "available": True,
                    "filename": order.get("filename", f"{order['slug']}.pdf"),
                }
            )
            continue

        orders.append(
            {
                "slug": order["slug"],
                "title": order["title"],
                "issued_label": order["issued_label"],
                "summary": order["summary"],
                "available": False,
                "filename": order.get("filename", f"{order['slug']}.pdf"),
            }
        )
    return orders
