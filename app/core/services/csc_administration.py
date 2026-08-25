"""Administration data held *against* a corporate specification, not inside it.

A specification sheet states what the chemical must be. Two things sit beside it
and belong to no single version of it:

* **Standard Testing Time** — how long a test against this specification is
  expected to take. It already lives on the corporate register that QC
  Laboratory Monitoring maintains (``qc_testing_standards``), so it is read and
  written there rather than copied.
* **Authorised testing laboratories** — which ONGC laboratories may issue a
  report against this specification. Nothing recorded that before, so it is kept
  in ``csc_authorized_labs``, keyed by the catalogue reference the module
  already addresses a chemical by.
"""

from __future__ import annotations

from typing import Any

from app.core.services.corporate_specifications import (
    UNCATEGORISED_KEY, catalogue, category_label,
)
from app.core.services.csc_utils import SPEC_SUBSET_ORDER
from app.extensions import db
from app.models.csc.authorized_lab import CSCAuthorizedLab


def laboratory_options() -> list[dict[str, str]]:
    """Every ONGC laboratory a specification can be authorised against.

    The list is the QC Laboratory Monitoring roster — the same laboratories that
    report the tests — so an authorisation always names a laboratory that can
    actually be measured against it.
    """
    from app.core.services.quality_control import (
        CSC_DESIGNATION_ONLY_LABORATORIES, LABORATORIES,
    )

    return [
        {
            "code": lab["code"],
            "name": lab["name"],
            "location": lab["location"],
            "description": lab["description"],
        }
        for lab in sorted(
            [*LABORATORIES.values(), *CSC_DESIGNATION_ONLY_LABORATORIES.values()],
            key=lambda lab: (lab["location"].casefold(), lab["name"].casefold()),
        )
    ]


def _valid_lab_codes() -> set[str]:
    return {option["code"] for option in laboratory_options()}


def authorized_labs_index() -> dict[str, list[str]]:
    """Authorised laboratory codes for every catalogue entry that has any."""
    index: dict[str, list[str]] = {}
    for row in CSCAuthorizedLab.query.all():
        index.setdefault(row.entry_ref, []).append(row.lab_code)
    return index


def _register_row(entry: dict[str, Any]):
    """The corporate register row behind a catalogue entry, when it has one."""
    from app.models.quality_control.qc_testing_standard import QCTestingStandard

    ref = entry["ref"]
    if not ref.startswith("r-"):
        return None
    try:
        row_id = int(ref[2:])
    except ValueError:
        return None
    return QCTestingStandard.query.get(row_id)


def administration_rows(
    category: str = "", query: str = ""
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Every catalogue entry with the administration data recorded against it."""
    labs_by_ref = authorized_labs_index()
    options = {option["code"]: option for option in laboratory_options()}

    rows: list[dict[str, Any]] = []
    for entry in catalogue():
        codes = [code for code in labs_by_ref.get(entry["ref"], []) if code in options]
        codes.sort(key=lambda code: options[code]["name"].casefold())
        rows.append(
            {
                "ref": entry["ref"],
                "chemical_name": entry["chemical_name"],
                "spec_number": entry["spec_number"],
                "material_code": entry["material_code"],
                "category": entry["category"],
                "category_label": entry["category_label"],
                "on_register": entry["on_register"],
                "has_parameters": entry["has_parameters"],
                "version": entry["version"],
                "standard_days": entry["standard_days"],
                "remarks": entry["remarks"],
                "lab_codes": codes,
                "laboratories": [options[code] for code in codes],
            }
        )

    summary = {
        "total": len(rows),
        "with_time": sum(1 for row in rows if row["standard_days"] is not None),
        "with_labs": sum(1 for row in rows if row["lab_codes"]),
        "laboratories": len(options),
        "categories": _category_options(rows),
    }

    selected = (category or "").strip()
    if selected:
        rows = [row for row in rows if row["category"] == selected]
    needle = (query or "").strip().casefold()
    if needle:
        rows = [
            row
            for row in rows
            if needle in row["chemical_name"].casefold()
            or needle in row["spec_number"].casefold()
            or needle in row["material_code"].casefold()
        ]
    summary["shown"] = len(rows)
    return rows, summary


def _category_options(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["category"]] = counts.get(row["category"], 0) + 1
    ordered = SPEC_SUBSET_ORDER + sorted(set(counts) - set(SPEC_SUBSET_ORDER))
    return [
        {
            "code": code,
            "label": category_label(None if code == UNCATEGORISED_KEY else code),
            "count": counts[code],
        }
        for code in ordered
        if counts.get(code)
    ]


class AdministrationError(Exception):
    """Raised when an administration change cannot be applied."""


def save_entry_administration(
    ref: str, lab_codes: list[str], standard_days: str, remarks: str, user_id: int | None
) -> str:
    """Apply one row's administration change and return a summary for the trail.

    The caller commits. Nothing is written when nothing changed, so an
    accidental save does not fill the audit trail with empty entries.
    """
    entry = next((item for item in catalogue() if item["ref"] == ref), None)
    if entry is None:
        raise AdministrationError("That chemical is no longer on the register.")

    valid = _valid_lab_codes()
    wanted = {code for code in lab_codes if code in valid}
    existing_rows = CSCAuthorizedLab.query.filter_by(entry_ref=ref).all()
    held = {row.lab_code for row in existing_rows}

    changes: list[str] = []
    for row in existing_rows:
        if row.lab_code not in wanted:
            db.session.delete(row)
    for code in sorted(wanted - held):
        db.session.add(CSCAuthorizedLab(entry_ref=ref, lab_code=code, updated_by=user_id))
    if wanted != held:
        names = {option["code"]: option["name"] for option in laboratory_options()}
        changes.append(
            "authorised laboratories set to "
            + (", ".join(sorted(names[code] for code in wanted)) or "none")
        )

    register_row = _register_row(entry)
    if register_row is not None:
        days = _parse_days(standard_days)
        if days != register_row.standard_days:
            changes.append(
                f"standard testing time {_days_label(register_row.standard_days)}"
                f" → {_days_label(days)}"
            )
            register_row.standard_days = days
        note = (remarks or "").strip()
        if note != (register_row.remarks or "").strip():
            changes.append("remarks updated")
            register_row.remarks = note or None
        if changes:
            register_row.updated_by = user_id
    elif (standard_days or "").strip():
        raise AdministrationError(
            "This specification is not on the corporate register, so a standard "
            "testing time cannot be recorded against it."
        )

    if not changes:
        return ""
    return f"{entry['chemical_name']} ({entry['spec_number']}): " + "; ".join(changes)


def _parse_days(value: str) -> int | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        days = int(float(text))
    except ValueError:
        raise AdministrationError("The standard testing time must be a whole number of days.")
    if days < 0 or days > 365:
        raise AdministrationError("The standard testing time must be between 0 and 365 days.")
    return days


def _days_label(value: int | None) -> str:
    if value is None:
        return "not defined"
    return "1 day" if value == 1 else f"{value} days"
