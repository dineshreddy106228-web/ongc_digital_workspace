"""Corporate Chemistry's SAP-first quality-monitoring control tower.

SAP is the system of record for RGL and IDWE work.  Corporate Chemistry can
record a returned action update, expected completion, or delay reason, but
that activity never edits the official SAP status.  A separate, explicitly
labelled register holds the few samples that are not represented in SAP.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field as dataclass_field
from datetime import date, datetime, timedelta
from io import BytesIO
import json
import re
from typing import Any

import pandas as pd

from sqlalchemy import event, func

from app.extensions import db
from app.models.quality_control.qc_sap_monitoring import (
    QCNonSAPSample,
    QCNonSAPSampleUpdate,
    QCSAPLabUpdate,
    QCSAPMonitoringDisposition,
    QCSAPRecord,
    QCSAPUploadBatch,
)


PANVEL_LAB_CODE = "rgl_panvel"
PANVEL_PLANT_CODE = "10R2"
SAP_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
# Corporate Chemistry's approved SAP QM routing.  A central SAP export may
# contain many plants, but rows are never assigned to a laboratory by a name,
# work centre, or user choice: the plant code below is the sole authority.
SAP_PLANT_LAB_CODES = {
    "10R2": "rgl_panvel",
    "23R2": "rgl_vadodara",
    "42R2": "rgl_chennai",
    "41B1": "rgl_rajahmundry",
    "50R2": "rgl_jorhat_sivasagar",
    "51R2": "rgl_jorhat",
    "70T1": "idwe_dehradun",
}
SAP_REPORTING_LAB_CODES = tuple(dict.fromkeys(SAP_PLANT_LAB_CODES.values()))
SAP_CENTRAL_UPLOAD_CODE = "central_sap_upload"
# IDWE's historic workstream screens remain readable, but their weekly uploads
# are no longer an operational input now that plant 70T1 is centrally sourced.
SAP_REPLACED_WEEKLY_LAB_CODES = frozenset({
    "rgl_panvel", "rgl_vadodara", "rgl_chennai", "rgl_rajahmundry", "rgl_jorhat",
    "idwe_cementing", "idwe_df_cf",
})

# These reporting units are controlled through SAP only.  They intentionally
# stay out of the weekly-workbook laboratory catalogue, which keeps the
# existing IDWE weekly workstream screens unchanged.
SAP_REPORTING_LABORATORY_OVERRIDES = {
    "rgl_jorhat_sivasagar": {
        "code": "rgl_jorhat_sivasagar",
        "name": "RGL Jorhat in Sivasagar",
        "location": "Sivasagar",
        "description": "Regional Geoscience Laboratory · Sivasagar",
    },
    "idwe_dehradun": {
        "code": "idwe_dehradun",
        "name": "IDWE Dehradun",
        "location": "Dehradun",
        "description": "Institute of Drilling and Well Engineering",
    },
}

LAB_UPDATE_STATUSES = (
    ("awaiting_sample", "Awaiting sample / inputs"),
    ("under_testing", "Under testing"),
    ("external_testing", "External testing / support"),
    ("report_in_process", "Report in process"),
    ("action_completed", "Action completed — awaiting SAP confirmation"),
)
LAB_UPDATE_STATUS_LABELS = dict(LAB_UPDATE_STATUSES)
NON_SAP_STATUSES = LAB_UPDATE_STATUSES + (
    ("closed_pass", "Closed — accepted / pass"),
    ("closed_fail", "Closed — rejected / fail"),
)
NON_SAP_STATUS_LABELS = dict(NON_SAP_STATUSES)
NON_SAP_CLOSED_STATUSES = {"closed_pass", "closed_fail"}
USAGE_DECISION_OUTCOMES = {"A": "accepted", "R": "rejected"}
SAP_EXCLUSION_REASONS = (
    ("junk_notification", "Junk / test notification"),
    ("duplicate_notification", "Duplicate SAP notification"),
    ("wrongly_raised", "Wrongly raised / wrong scope"),
    ("no_lab_work_required", "No laboratory work required"),
    ("other", "Other non-actionable notification"),
)
SAP_EXCLUSION_REASON_LABELS = dict(SAP_EXCLUSION_REASONS)
SAP_REGISTER_STATUS_FILTERS = (
    ("open", "Open in SAP"),
    ("completed", "Complete in SAP"),
    ("accepted", "SAP usage decision: accepted"),
    ("rejected", "SAP usage decision: rejected"),
    ("excluded", "Excluded from active monitoring"),
    ("exclusion_review", "QC-admin exclusion review"),
)
CORPORATE_SPECIFICATION_UNMATCHED_KEY = "not_in_corporate_specification"
CORPORATE_SPECIFICATION_UNMATCHED_LABEL = "Not in Corporate Specification"


@dataclass(frozen=True)
class SAPExportPayload:
    rows: list[dict[str, Any]]
    as_of_date: date | None
    # Rows a scope rule removed, by reason, so an import can account for the
    # difference between the workbook SAP produced and the rows it monitors.
    excluded_rows: dict[str, int] = dataclass_field(default_factory=dict)


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    value = str(value).strip()
    return "" if value.casefold() in {"", "nan", "none", "nat", "-"} else value


def _header_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).casefold())


def _date(value: Any) -> date | None:
    text = _text(value)
    if not text:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            pass
    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _identifier(value: Any, *, zero_is_blank: bool = False) -> str | None:
    text = _text(value)
    if not text:
        return None
    # Excel commonly represents SAP numeric identifiers as 8900123.0.
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    text = re.sub(r"\s+", "", text)
    if zero_is_blank and re.fullmatch(r"0+", text):
        return None
    return text or None


def _material_key(value: Any) -> str:
    """Compare SAP material numbers without presentation zero-padding."""
    identifier = _identifier(value) or ""
    return identifier.lstrip("0") or "0"


def _corporate_specifications_by_material_code() -> dict[str, dict[str, Any]]:
    """Index the Corporate Specifications catalogue using SAP-compatible codes.

    SAP commonly pads material numbers with zeroes while the Corporate
    Specifications register may not.  Matching the canonical numeric value
    makes the connection explicit without changing either source record.
    """
    from app.core.services.corporate_specifications import catalogue

    specifications: dict[str, dict[str, Any]] = {}
    for entry in catalogue():
        material_code = _identifier(entry.get("material_code"))
        if not material_code:
            continue
        key = _material_key(material_code)
        existing = specifications.get(key)
        # Where legacy records happen to share a material code, keep the
        # active corporate-register entry in preference to an off-register
        # specification record.
        if existing is not None and existing["on_register"]:
            continue
        specifications[key] = {
            "subgroup_key": entry["category"],
            "subgroup_label": entry["category_label"],
            "specification_no": entry["spec_number"],
            "chemical_name": entry["chemical_name"],
            "stt_days": _integer(entry.get("standard_days")),
            "on_register": bool(entry["on_register"]),
        }
    return specifications


def _corporate_specification_fields(
    material_code: str | None,
    specifications_by_material_code: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Return display-ready Corporate Specifications classification fields."""
    specification = (
        specifications_by_material_code.get(_material_key(material_code))
        if _identifier(material_code)
        else None
    )
    if specification is None:
        return {
            "specification_match": False,
            "subgroup_key": CORPORATE_SPECIFICATION_UNMATCHED_KEY,
            "subgroup_label": CORPORATE_SPECIFICATION_UNMATCHED_LABEL,
            "specification_no": None,
            "specification_chemical_name": None,
            "stt_days": None,
        }
    return {
        "specification_match": True,
        "subgroup_key": specification["subgroup_key"],
        "subgroup_label": specification["subgroup_label"],
        "specification_no": specification["specification_no"],
        "specification_chemical_name": specification["chemical_name"],
        "stt_days": specification["stt_days"],
    }


def _integer(value: Any) -> int | None:
    text = _text(value)
    match = re.search(r"-?\d+", text)
    return int(match.group()) if match else None


# SAP raises a laboratory inspection lot in the 8900 series.  The same QA33
# selection also returns goods-receipt lots from every plant in the company;
# those are not laboratory work and never enter quality monitoring.
SAP_LAB_LOT_PREFIX = "8900"


def financial_year_label(value: date) -> str:
    """Name the Indian financial year (1 April - 31 March) a date falls in."""
    start_year = value.year if value.month >= 4 else value.year - 1
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def financial_year_start(value: date) -> date:
    """The 1 April that opens the financial year the given date falls in."""
    return date(value.year if value.month >= 4 else value.year - 1, 4, 1)


def _find_as_of_date(raw: pd.DataFrame, filename: str | None = None) -> date | None:
    for value in raw.iloc[:12].to_numpy().flat:
        candidate = _text(value)
        match = re.search(r"\bdate\s*[:\-]\s*(\d{1,2}[./-]\d{1,2}[./-]\d{4})", candidate, re.I)
        if match:
            parsed = _date(match.group(1))
            if parsed:
                return parsed
    match = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", filename or "")
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None
    return None


_INSPECTION_COLUMNS = {
    "inspection_lot_number": ("inspectionlot",),
    "material_code": ("material",),
    "plant_code": ("plant",),
    "inspection_quantity": ("inspectionlotquantity",),
    "base_uom": ("baseunitofmeasure", "baseuom"),
    "start_inspection_date": ("startofinspection", "startinspection"),
    "end_inspection_date": ("endofinspection", "endinspection"),
    "sap_system_status": ("systemstatus",),
    "usage_decision_code": ("usagedecisioncode", "usagedecision"),
}
_NOTIFICATION_COLUMNS = {
    "notification_no": ("notificationno", "notificationnumber"),
    "sap_notification_status": ("notificationstatus", "statusofnotification"),
    "po_number": ("purchasingdocument", "purchasingdoc", "ponumber", "po"),
    "po_item": ("item", "poitem"),
    "material_code": ("materialnumber",),
    "material_description": ("materialdescription", "materialdesc"),
    "work_center": ("workcenter", "workcentre"),
    "plant_code": ("plant",),
    "inspection_lot_number": ("inspectionlotnumber", "inspectionlotno"),
    "sap_lot_status": ("statusofinspectionlot", "inspectionlotstatus"),
    "notification_start_date": ("startdate",),
    "planned_end_date": ("plannedenddate", "plannedend"),
    "completion_date": ("completiondate", "actualcompletiondate"),
    "sap_delay_days": ("delaydays",),
}


def _column_positions(header: list[Any], aliases: dict[str, tuple[str, ...]]) -> dict[str, int]:
    positions: dict[str, int] = {}
    normalized = [_header_key(value) for value in header]
    for field, options in aliases.items():
        for index, label in enumerate(normalized):
            if label in options:
                positions[field] = index
                break
    return positions


def _matching_sap_export_kind(
    worksheets: dict[str, pd.DataFrame], *, excluded_kind: str,
) -> str | None:
    """Identify a correctly structured SAP export that was placed in the wrong upload field."""
    export_types = (
        ("inspection-lot", _INSPECTION_COLUMNS, {"inspection_lot_number", "material_code", "plant_code"}),
        ("notification", _NOTIFICATION_COLUMNS, {"notification_no", "material_code", "plant_code"}),
    )
    for kind, aliases, required in export_types:
        if kind == excluded_kind:
            continue
        for raw in worksheets.values():
            for index in range(len(raw)):
                if required.issubset(_column_positions(raw.iloc[index].tolist(), aliases)):
                    return kind
    return None


def _read_sap_export(
    source: bytes,
    filename: str,
    *,
    kind: str,
    aliases: dict[str, tuple[str, ...]],
    required: set[str],
) -> SAPExportPayload:
    """Read a native SAP Excel layout, even where SAP writes title rows first."""
    try:
        worksheets = pd.read_excel(BytesIO(source), sheet_name=None, header=None, dtype=object)
    except Exception as exc:  # pandas reports engine-specific errors otherwise
        raise ValueError(f"Could not read the {kind} SAP workbook as Excel.") from exc

    selected: tuple[pd.DataFrame, int, dict[str, int]] | None = None
    as_of: date | None = None
    for raw in worksheets.values():
        as_of = as_of or _find_as_of_date(raw, filename)
        for index in range(len(raw)):
            positions = _column_positions(raw.iloc[index].tolist(), aliases)
            if required.issubset(positions):
                selected = (raw, index, positions)
                break
        if selected:
            break
    if selected is None:
        other_kind = _matching_sap_export_kind(worksheets, excluded_kind=kind)
        if kind == "inspection-lot" and other_kind == "notification":
            raise ValueError(
                "The selected Inspection Lots file is a SAP Notifications / ZLABIMS export. "
                "Upload the native SAP Inspection Lots export in the Inspection Lots field."
            )
        if kind == "notification" and other_kind == "inspection-lot":
            raise ValueError(
                "The selected Notifications file is a SAP Inspection Lots export. "
                "Upload the native SAP Notifications / ZLABIMS export in the Notifications field."
            )
        required_labels = ", ".join(sorted(required)).replace("_", " ")
        raise ValueError(f"Could not find SAP {kind} headings. Required fields: {required_labels}.")

    raw, header_row, positions = selected
    rows: list[dict[str, Any]] = []
    for _, raw_row in raw.iloc[header_row + 1:].iterrows():
        row = {field: raw_row.iloc[index] for field, index in positions.items()}
        if not any(_text(value) for value in row.values()):
            continue
        rows.append(row)
    if not rows:
        raise ValueError(f"The SAP {kind} workbook contains no data rows.")
    return SAPExportPayload(rows=rows, as_of_date=as_of)


def parse_sap_inspection_workbook(
    source: bytes,
    filename: str,
    *,
    expected_plant: str | None = PANVEL_PLANT_CODE,
    allow_multiple_plants: bool = False,
    lab_lots_only: bool = True,
) -> SAPExportPayload:
    payload = _read_sap_export(
        source, filename, kind="inspection-lot", aliases=_INSPECTION_COLUMNS,
        required={"inspection_lot_number", "material_code", "plant_code"},
    )
    rows = []
    non_lab_lots = 0
    for raw in payload.rows:
        lot = _identifier(raw.get("inspection_lot_number"), zero_is_blank=True)
        if not lot:
            continue
        if lab_lots_only and not lot.startswith(SAP_LAB_LOT_PREFIX):
            # Dropped before the plant routing check, so a goods-receipt lot
            # raised at an unmapped plant cannot fail the whole upload.
            non_lab_lots += 1
            continue
        rows.append({
            "inspection_lot_number": lot,
            "material_code": _identifier(raw.get("material_code")),
            "plant_code": (_text(raw.get("plant_code")) or "").upper(),
            "start_inspection_date": _date(raw.get("start_inspection_date")),
            "end_inspection_date": _date(raw.get("end_inspection_date")),
            "sap_system_status": _text(raw.get("sap_system_status")) or None,
            "usage_decision_code": _text(raw.get("usage_decision_code")) or None,
        })
    if not rows:
        if non_lab_lots:
            raise ValueError(
                f"The SAP inspection-lot workbook holds no {SAP_LAB_LOT_PREFIX}-series laboratory "
                f"inspection lots; all {non_lab_lots} lot(s) are goods-receipt lots."
            )
        raise ValueError("The SAP inspection-lot workbook contains no usable inspection-lot numbers.")
    if not allow_multiple_plants:
        _validate_sap_plants(rows, "inspection-lot", expected_plant)
    return SAPExportPayload(
        rows=rows, as_of_date=payload.as_of_date,
        excluded_rows={"non_laboratory_lots": non_lab_lots} if non_lab_lots else {},
    )


def parse_sap_notification_workbook(
    source: bytes,
    filename: str,
    *,
    expected_plant: str | None = PANVEL_PLANT_CODE,
    allow_multiple_plants: bool = False,
    from_date: date | None = None,
) -> SAPExportPayload:
    """Read a SAP notification export, scoped to one financial year.

    The full SAP history reaches back to 2009.  Monitoring is kept to the
    financial year of the export, so the same workbook can seed a year's base
    data and, on any later day, carry only what is current and newly raised.
    ``from_date`` overrides that boundary where a year must be re-seeded from
    an export taken after it closed.
    """
    payload = _read_sap_export(
        source, filename, kind="notification", aliases=_NOTIFICATION_COLUMNS,
        required={"notification_no", "material_code", "plant_code"},
    )
    if from_date is None and payload.as_of_date is not None:
        from_date = financial_year_start(payload.as_of_date)
    rows = []
    before_year = 0
    undated = 0
    for raw in payload.rows:
        notification_no = _identifier(raw.get("notification_no"), zero_is_blank=True)
        if not notification_no:
            continue
        start_date = _date(raw.get("notification_start_date"))
        if from_date is not None:
            if start_date is None:
                # A notification SAP has not dated belongs to no year.
                undated += 1
                continue
            if start_date < from_date:
                before_year += 1
                continue
        rows.append({
            "notification_no": notification_no,
            "sap_notification_status": _text(raw.get("sap_notification_status")) or None,
            "po_number": _identifier(raw.get("po_number")),
            "po_item": _identifier(raw.get("po_item")),
            "material_code": _identifier(raw.get("material_code")),
            "material_description": _text(raw.get("material_description")) or None,
            "work_center": _text(raw.get("work_center")) or None,
            "plant_code": (_text(raw.get("plant_code")) or "").upper(),
            "inspection_lot_number": _identifier(raw.get("inspection_lot_number"), zero_is_blank=True),
            "sap_lot_status": _text(raw.get("sap_lot_status")) or None,
            "notification_start_date": start_date,
            "planned_end_date": _date(raw.get("planned_end_date")),
            "completion_date": _date(raw.get("completion_date")),
            "sap_delay_days": _integer(raw.get("sap_delay_days")),
        })
    if not rows:
        if before_year or undated:
            raise ValueError(
                "The SAP notification workbook holds no notifications raised on or after "
                f"{from_date:%d.%m.%Y}. Export the notifications for the current financial year."
            )
        raise ValueError("The SAP notification workbook contains no usable notification numbers.")
    if not allow_multiple_plants:
        _validate_sap_plants(rows, "notification", expected_plant)
    excluded = {}
    if before_year:
        excluded["before_financial_year"] = before_year
    if undated:
        excluded["no_start_date"] = undated
    return SAPExportPayload(rows=rows, as_of_date=payload.as_of_date, excluded_rows=excluded)


def _validate_sap_plants(
    rows: list[dict[str, Any]], source_name: str, expected_plant: str | None,
) -> str:
    """Require a complete, single-plant export before it becomes a snapshot.

    A Corporate Chemistry uploader chooses the reporting laboratory.  A
    multi-plant report could therefore misstate that laboratory's workload, so
    it must be split before import.  Panvel retains its known plant check for
    backwards compatibility with the original pilot.
    """
    missing = sum(1 for row in rows if not row.get("plant_code"))
    plants = {row["plant_code"] for row in rows if row.get("plant_code")}
    if missing:
        raise ValueError(f"{missing} SAP {source_name} row(s) have no plant code.")
    if len(plants) != 1:
        names = ", ".join(sorted(plants)) or "none"
        raise ValueError(
            f"The SAP {source_name} workbook contains more than one plant ({names}). "
            "Upload one plant per daily snapshot."
        )
    plant = next(iter(plants))
    if expected_plant and plant != expected_plant:
        raise ValueError(
            f"This SAP import accepts plant {expected_plant} only; the workbook reports {plant}."
        )
    return plant


def _official_status(inspection: dict[str, Any] | None, notification: dict[str, Any] | None) -> str:
    usage = _text((inspection or {}).get("usage_decision_code"))
    system_status = _text((inspection or {}).get("sap_system_status")).upper()
    lot_status = _text((notification or {}).get("sap_lot_status")).upper()
    if usage or re.search(r"\bUD\b", system_status) or re.search(r"\bUD\b", lot_status):
        return "completed"
    if (notification or {}).get("completion_date"):
        return "completed"
    return "open"


def _row_financial_year(row: dict[str, Any], as_of_date: date | None) -> str | None:
    """Place a merged row in the year its SAP work was raised in.

    The notification date is the authority.  An inspection lot carrying no
    notification is placed by its own receipt date, and only a row SAP dates
    in neither way falls back to the date of the export itself.
    """
    anchor = row.get("notification_start_date") or row.get("start_inspection_date") or as_of_date
    return financial_year_label(anchor) if anchor else None


def merge_sap_exports(
    inspections: list[dict[str, Any]], notifications: list[dict[str, Any]], *, lab_code: str = PANVEL_LAB_CODE,
    as_of_date: date | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Join notification detail to inspection lots without discarding unmatched SAP rows."""
    inspection_by_lot = {row["inspection_lot_number"]: row for row in inspections}
    rows: list[dict[str, Any]] = []
    used_lots: set[str] = set()
    seen_keys: set[str] = set()

    def add(inspection: dict[str, Any] | None, notification: dict[str, Any] | None, completeness: str) -> None:
        lot = (notification or {}).get("inspection_lot_number") or (inspection or {}).get("inspection_lot_number")
        notification_no = (notification or {}).get("notification_no")
        key_tail = f"notification:{notification_no}" if notification_no else f"lot:{lot}"
        source_key = f"{lab_code}:{key_tail}"
        if source_key in seen_keys:
            return
        seen_keys.add(source_key)
        material_code = (notification or {}).get("material_code") or (inspection or {}).get("material_code")
        rows.append({
            "source_key": source_key,
            "source_completeness": completeness,
            "inspection_lot_number": lot,
            "notification_no": notification_no,
            "plant_code": (notification or inspection or {}).get("plant_code"),
            "material_code": material_code,
            "material_description": (notification or {}).get("material_description"),
            "po_number": (notification or {}).get("po_number"),
            "po_item": (notification or {}).get("po_item"),
            "work_center": (notification or {}).get("work_center"),
            "sap_system_status": (inspection or {}).get("sap_system_status"),
            "sap_lot_status": (notification or {}).get("sap_lot_status"),
            "sap_notification_status": (notification or {}).get("sap_notification_status"),
            "usage_decision_code": (inspection or {}).get("usage_decision_code"),
            "official_status": _official_status(inspection, notification),
            "start_inspection_date": (inspection or {}).get("start_inspection_date"),
            "end_inspection_date": (inspection or {}).get("end_inspection_date"),
            "notification_start_date": (notification or {}).get("notification_start_date"),
            "planned_end_date": (notification or {}).get("planned_end_date"),
            "completion_date": (notification or {}).get("completion_date"),
            "sap_delay_days": (notification or {}).get("sap_delay_days"),
        })

    for notification in notifications:
        lot = notification.get("inspection_lot_number")
        inspection = inspection_by_lot.get(lot) if lot else None
        if lot and inspection:
            used_lots.add(lot)
        add(inspection, notification, "matched" if inspection else "notification_only")
    for lot, inspection in inspection_by_lot.items():
        if lot not in used_lots:
            add(inspection, None, "inspection_lot_only")

    for row in rows:
        row["financial_year"] = _row_financial_year(row, as_of_date)
    unmatched_inspection = sum(1 for row in rows if row["source_completeness"] == "inspection_lot_only")
    unmatched_notification = sum(1 for row in rows if row["source_completeness"] == "notification_only")
    return rows, {
        "unmatched_inspection_count": unmatched_inspection,
        "unmatched_notification_count": unmatched_notification,
    }


def _standard_testing_time_fields(
    record: QCSAPRecord, stt_days: Any, as_of_date: date,
) -> dict[str, Any]:
    """Assess an SAP record against its Corporate Specification STT.

    The SAP planned-end date is intentionally not used for monitoring.  The
    elapsed testing window begins with SAP's receipt date; for a notification
    without a matched lot, it begins with the notification date instead.
    """
    days = _integer(stt_days)
    if days is not None and days < 0:
        days = None
    start_date = record.start_inspection_date or record.notification_start_date
    due_date = start_date + timedelta(days=days) if start_date and days is not None else None
    variance_days = (as_of_date - due_date).days if due_date else None
    return {
        "stt_days": days,
        "stt_start_date": start_date,
        "stt_due_date": due_date,
        "stt_variance_days": variance_days,
        "stt_overdue": bool(record.official_status == "open" and variance_days is not None and variance_days > 0),
    }


def _summary(rows: list[dict[str, Any]], as_of_date: date) -> dict[str, Any]:
    open_rows = [row for row in rows if row["official_status"] == "open"]
    return {
        "as_of_date": as_of_date.isoformat(),
        "total_records": len(rows),
        "completed_records": len(rows) - len(open_rows),
        "open_records": len(open_rows),
        "work_centers": dict(Counter(row.get("work_center") or "Not assigned in SAP" for row in open_rows)),
        "usage_decisions": dict(Counter(row.get("usage_decision_code") or "Not recorded" for row in rows)),
        "accepted_records": sum(1 for row in rows if usage_decision_outcome(row.get("usage_decision_code")) == "accepted"),
        "rejected_records": sum(1 for row in rows if usage_decision_outcome(row.get("usage_decision_code")) == "rejected"),
    }


def _laboratories_by_code() -> dict[str, dict[str, Any]]:
    # Imported lazily to avoid a circular dependency during Flask model setup.
    from app.core.services.quality_control import CSC_DESIGNATION_ONLY_LABORATORIES, LABORATORIES
    return {**LABORATORIES, **CSC_DESIGNATION_ONLY_LABORATORIES, **SAP_REPORTING_LABORATORY_OVERRIDES}


def sap_reporting_laboratories() -> list[dict[str, Any]]:
    laboratories = _laboratories_by_code()
    return [laboratories[code] for code in SAP_REPORTING_LAB_CODES if code in laboratories]


def sap_plant_mappings() -> list[dict[str, Any]]:
    """Return the approved, visible plant-to-laboratory routing table."""
    laboratories = _laboratories_by_code()
    return [
        {"plant_code": plant_code, "laboratory": laboratories[lab_code]}
        for plant_code, lab_code in SAP_PLANT_LAB_CODES.items()
    ]


def get_sap_reporting_laboratory(lab_code: str) -> dict[str, Any]:
    laboratory = _laboratories_by_code().get(lab_code)
    if lab_code not in SAP_REPORTING_LAB_CODES or laboratory is None:
        raise ValueError("Choose an RGL or IDWE laboratory configured for SAP daily monitoring.")
    return laboratory


def usage_decision_outcome(value: Any) -> str | None:
    """Return the business outcome encoded by SAP usage decision A or R."""
    text = _text(value).upper()
    match = re.search(r"(?:^|\b)(?:UD\s*)?([AR])(?:$|\b)", text)
    return USAGE_DECISION_OUTCOMES.get(match.group(1)) if match else None


def sap_turnaround_days(record: QCSAPRecord) -> int | None:
    """Days from SAP receipt to SAP completion, or ``None`` if unmeasurable.

    The window starts at SAP's receipt date — the notification date where no
    inspection lot was matched — which is what the Corporate Specification
    testing time is written against.  A completion recorded before the start
    is a source-data fault, not a zero-day turnaround, so it stays unmeasured.
    """
    start_date = record.start_inspection_date or record.notification_start_date
    if not start_date or not record.completion_date:
        return None
    elapsed = (record.completion_date - start_date).days
    return elapsed if elapsed >= 0 else None


def apply_derived_sap_readings(record: QCSAPRecord) -> None:
    """Materialise the readings the portfolio analytics groups on in SQL."""
    record.usage_outcome = usage_decision_outcome(record.usage_decision_code)
    record.turnaround_days = sap_turnaround_days(record)


@event.listens_for(QCSAPRecord, "before_insert")
@event.listens_for(QCSAPRecord, "before_update")
def _refresh_derived_sap_readings(_mapper, _connection, record: QCSAPRecord) -> None:
    """Re-derive the materialised readings on every write to a record.

    These columns exist so the analytics can group in SQL, which only works if
    they can never disagree with the SAP values they are taken from.  Deriving
    them here rather than at the importer covers a backfill or a correction
    made anywhere else too.
    """
    apply_derived_sap_readings(record)


def _paired_sap_as_of_date(inspections: SAPExportPayload, notifications: SAPExportPayload) -> date:
    as_of_date = notifications.as_of_date or inspections.as_of_date
    if as_of_date is None:
        raise ValueError("Could not determine the SAP as-of date. Include it in the export or filename as YYYYMMDD.")
    if inspections.as_of_date and notifications.as_of_date and inspections.as_of_date != notifications.as_of_date:
        raise ValueError("The two SAP exports have different as-of dates. Upload reports from the same daily run.")
    return as_of_date


def _rows_by_plant(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        plant_code = row.get("plant_code") or ""
        grouped.setdefault(plant_code, []).append(row)
    return grouped


def _select_approved_plant_rows(
    rows: list[dict[str, Any]], source_name: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Keep the rows an RGL or IDWE laboratory is accountable for.

    A central QA33 or ZLABIMS selection covers the whole company.  SAP raises
    laboratory lots at plants that have no laboratory of their own, so those
    rows are ordinary content rather than a broken export, and refusing the
    whole upload over them only blocks the work.  They are set aside and
    counted, so the import still reports exactly what it did not take.

    An export holding nothing for any approved plant is a different matter:
    that is the wrong report, and it still stops the upload.
    """
    kept: list[dict[str, Any]] = []
    set_aside: Counter = Counter()
    for row in rows:
        plant_code = row.get("plant_code") or ""
        if plant_code in SAP_PLANT_LAB_CODES:
            kept.append(row)
        else:
            set_aside[plant_code or "no plant code"] += 1
    if not kept:
        listed = ", ".join(f"{plant} ({count})" for plant, count in sorted(set_aside.items()))
        raise ValueError(
            f"The SAP {source_name} export holds no rows for an RGL or IDWE plant "
            f"({listed or 'none'}). Check the selection used to run the export."
        )
    return kept, dict(set_aside)


def _plants_set_aside_summary(
    inspection_set_aside: dict[str, int], notification_set_aside: dict[str, int],
) -> dict[str, Any]:
    """Flat counts for the batch summary, plus the plants behind them."""
    summary: dict[str, Any] = {}
    if inspection_set_aside:
        summary["lots_outside_rgl_idwe"] = sum(inspection_set_aside.values())
    if notification_set_aside:
        summary["notifications_outside_rgl_idwe"] = sum(notification_set_aside.values())
    return summary


# How many identifiers each change category keeps in the batch summary.  The
# counts are always exact; the samples exist so a reviewer can recognise what
# moved without the summary growing with the size of the upload.
_CHANGE_SAMPLE_LIMIT = 40
CHANGE_CATEGORIES = (
    ("new", "Newly raised in SAP"),
    ("closed", "Closed in SAP since the last upload"),
    ("reopened", "Reopened in SAP since the last upload"),
    ("usage_decided", "Usage decision recorded since the last upload"),
)


def _record_label(row: dict[str, Any]) -> str:
    return row.get("notification_no") or row.get("inspection_lot_number") or "Unidentified row"


def _blank_change_log() -> dict[str, list[str]]:
    return {key: [] for key, _ in CHANGE_CATEGORIES}


def _note_record_change(
    log: dict[str, list[str]],
    row: dict[str, Any],
    before: tuple[str, str | None] | None,
) -> None:
    """Record what this upload moved, judged against the row it replaces.

    ``before`` is ``None`` for a source key the database has not seen.  The
    comparison is deliberately limited to the two things a reviewer acts on:
    whether SAP still calls the work open, and whether it has decided it.
    """
    if before is None:
        log["new"].append(_record_label(row))
        return
    was_status, was_usage = before
    now_status = row.get("official_status")
    if was_status != now_status:
        log["closed" if now_status == "completed" else "reopened"].append(_record_label(row))
    if not _text(was_usage) and _text(row.get("usage_decision_code")):
        log["usage_decided"].append(_record_label(row))


def _change_summary(log: dict[str, list[str]]) -> dict[str, Any]:
    return {
        key: {"count": len(log[key]), "sample": sorted(log[key])[:_CHANGE_SAMPLE_LIMIT]}
        for key, _ in CHANGE_CATEGORIES
    }


def _persist_sap_lab_snapshot(
    *,
    lab_code: str,
    plant_code: str,
    inspection_rows: list[dict[str, Any]],
    notification_rows: list[dict[str, Any]],
    as_of_date: date,
    inspection_source: bytes,
    inspection_filename: str,
    notification_source: bytes,
    notification_filename: str,
    uploaded_by: int | None,
    excluded_rows: dict[str, int] | None = None,
) -> QCSAPUploadBatch:
    """Store one laboratory's mapped portion of a paired SAP export."""
    rows, reconciliation = merge_sap_exports(
        inspection_rows, notification_rows, lab_code=lab_code, as_of_date=as_of_date,
    )
    if not rows:
        raise ValueError(f"The SAP exports did not yield any monitoring records for plant {plant_code}.")

    summary = _summary(rows, as_of_date)
    summary["financial_year"] = financial_year_label(as_of_date)
    summary["excluded_rows"] = dict(excluded_rows or {})
    batch = QCSAPUploadBatch(
        lab_code=lab_code,
        plant_code=plant_code,
        as_of_date=as_of_date,
        inspection_filename=inspection_filename,
        inspection_content_type=SAP_XLSX_MIME,
        inspection_file_size=len(inspection_source),
        inspection_source_data=inspection_source,
        notification_filename=notification_filename,
        notification_content_type=SAP_XLSX_MIME,
        notification_file_size=len(notification_source),
        notification_source_data=notification_source,
        inspection_lot_count=len(inspection_rows),
        notification_count=len(notification_rows),
        record_count=len(rows),
        unmatched_inspection_count=reconciliation["unmatched_inspection_count"],
        unmatched_notification_count=reconciliation["unmatched_notification_count"],
        summary_json=json.dumps(summary),
        uploaded_by=uploaded_by,
    )
    db.session.add(batch)
    db.session.flush()

    existing = {
        item.source_key: item
        for item in QCSAPRecord.query.filter_by(lab_code=lab_code).all()
    }
    change_log = _blank_change_log()
    for row in rows:
        record = existing.get(row["source_key"])
        before = None if record is None else (record.official_status, record.usage_decision_code)
        if record is None:
            record = QCSAPRecord(
                source_key=row["source_key"], lab_code=lab_code, first_seen_batch_id=batch.id,
            )
            db.session.add(record)
        _note_record_change(change_log, row, before)
        for column, value in row.items():
            setattr(record, column, value)
        record.last_seen_batch_id = batch.id

    summary["changes"] = _change_summary(change_log)
    batch.summary_json = json.dumps(summary)
    return batch


def import_sap_lab_exports(
    lab_code: str,
    inspection_source: bytes,
    inspection_filename: str,
    notification_source: bytes,
    notification_filename: str,
    uploaded_by: int | None,
) -> QCSAPUploadBatch:
    """Persist a laboratory's paired SAP exports and refresh its current view."""
    get_sap_reporting_laboratory(lab_code)
    expected_plants = [
        plant_code for plant_code, mapped_lab_code in SAP_PLANT_LAB_CODES.items()
        if mapped_lab_code == lab_code
    ]
    if len(expected_plants) != 1:
        raise ValueError(f"No single approved SAP plant is configured for {lab_code}.")
    expected_plant = expected_plants[0]
    inspections = parse_sap_inspection_workbook(
        inspection_source, inspection_filename, expected_plant=expected_plant,
    )
    notifications = parse_sap_notification_workbook(
        notification_source, notification_filename, expected_plant=expected_plant,
    )
    inspection_plant = _validate_sap_plants(inspections.rows, "inspection-lot", expected_plant)
    notification_plant = _validate_sap_plants(notifications.rows, "notification", expected_plant)
    if inspection_plant != notification_plant:
        raise ValueError(
            "The two SAP exports report different plants. Upload paired reports from the same laboratory run."
        )
    return _persist_sap_lab_snapshot(
        lab_code=lab_code,
        plant_code=inspection_plant,
        inspection_rows=inspections.rows,
        notification_rows=notifications.rows,
        as_of_date=_paired_sap_as_of_date(inspections, notifications),
        inspection_source=inspection_source,
        inspection_filename=inspection_filename,
        notification_source=notification_source,
        notification_filename=notification_filename,
        uploaded_by=uploaded_by,
        excluded_rows={**inspections.excluded_rows, **notifications.excluded_rows},
    )


def import_central_sap_exports(
    inspection_source: bytes,
    inspection_filename: str,
    notification_source: bytes,
    notification_filename: str,
    uploaded_by: int | None,
) -> list[QCSAPUploadBatch]:
    """Split Corporate Chemistry's all-laboratory SAP pair by the approved plant map."""
    inspections = parse_sap_inspection_workbook(
        inspection_source, inspection_filename, expected_plant=None, allow_multiple_plants=True,
    )
    notifications = parse_sap_notification_workbook(
        notification_source, notification_filename, expected_plant=None, allow_multiple_plants=True,
    )
    inspection_rows, inspection_set_aside = _select_approved_plant_rows(
        inspections.rows, "Inspection Lots",
    )
    notification_rows, notification_set_aside = _select_approved_plant_rows(
        notifications.rows, "Notifications",
    )
    excluded_rows = {
        **inspections.excluded_rows, **notifications.excluded_rows,
        **_plants_set_aside_summary(inspection_set_aside, notification_set_aside),
    }
    as_of_date = _paired_sap_as_of_date(inspections, notifications)
    inspections_by_plant = _rows_by_plant(inspection_rows)
    notifications_by_plant = _rows_by_plant(notification_rows)

    batches = []
    for plant_code in sorted(set(inspections_by_plant) | set(notifications_by_plant)):
        lab_code = SAP_PLANT_LAB_CODES[plant_code]
        get_sap_reporting_laboratory(lab_code)
        batches.append(_persist_sap_lab_snapshot(
            lab_code=lab_code,
            plant_code=plant_code,
            inspection_rows=inspections_by_plant.get(plant_code, []),
            notification_rows=notifications_by_plant.get(plant_code, []),
            as_of_date=as_of_date,
            inspection_source=inspection_source,
            inspection_filename=inspection_filename,
            notification_source=notification_source,
            notification_filename=notification_filename,
            uploaded_by=uploaded_by,
            excluded_rows=excluded_rows,
        ))
    return batches


def _reconcile_financial_year(
    lab_codes: set[str], financial_year: str, keep_source_keys: set[str],
) -> dict[str, Any]:
    """Retire year records the rebuilt workbook no longer accounts for.

    A record is only ever removed when nothing human is attached to it.  A
    laboratory's returned follow-up and a QC-admin exclusion are decisions SAP
    does not hold and cannot reproduce, so a record carrying either is kept and
    reported instead of deleted -- the reviewer decides what to do with it.
    """
    stale = QCSAPRecord.query.filter(
        QCSAPRecord.lab_code.in_(lab_codes),
        QCSAPRecord.financial_year == financial_year,
        QCSAPRecord.source_key.notin_(keep_source_keys) if keep_source_keys else True,
    ).all()
    removed: list[str] = []
    retained: list[dict[str, Any]] = []
    for record in stale:
        if record.lab_updates or record.monitoring_dispositions:
            retained.append({
                "lab_code": record.lab_code,
                "notification_no": record.notification_no,
                "inspection_lot_number": record.inspection_lot_number,
                "reason": (
                    "laboratory follow-up recorded" if record.lab_updates
                    else "QC-admin monitoring decision recorded"
                ),
            })
            continue
        removed.append(record.notification_no or record.inspection_lot_number or str(record.id))
        db.session.delete(record)
    return {
        "removed_count": len(removed),
        "removed_sample": sorted(removed)[:_CHANGE_SAMPLE_LIMIT],
        "retained_count": len(retained),
        "retained": retained[:_CHANGE_SAMPLE_LIMIT],
    }


def rebuild_sap_financial_year(
    inspection_source: bytes,
    inspection_filename: str,
    notification_source: bytes,
    notification_filename: str,
    uploaded_by: int | None,
    *,
    as_of_date: date | None = None,
    reconcile: bool = True,
) -> dict[str, Any]:
    """Reload a whole financial year from a full pair of SAP exports.

    This is not the daily upload and is deliberately a separate act.  A year is
    opened from the full notification history and the full inspection-lot
    register, which are often pulled on different days, so the paired-date rule
    the daily upload enforces is relaxed in favour of an as-of date the
    operator states.  Nothing is wiped: SAP-sourced fields are replaced in
    place, and only records the workbook no longer accounts for -- and that
    carry no laboratory or QC-admin record of their own -- are retired.
    """
    inspections = parse_sap_inspection_workbook(
        inspection_source, inspection_filename, expected_plant=None, allow_multiple_plants=True,
    )
    notifications = parse_sap_notification_workbook(
        notification_source, notification_filename, expected_plant=None,
        allow_multiple_plants=True,
        from_date=financial_year_start(as_of_date) if as_of_date else None,
    )
    if as_of_date is None:
        as_of_date = _paired_sap_as_of_date(inspections, notifications)
    inspection_rows, inspection_set_aside = _select_approved_plant_rows(
        inspections.rows, "Inspection Lots",
    )
    notification_rows, notification_set_aside = _select_approved_plant_rows(
        notifications.rows, "Notifications",
    )
    excluded_rows = {
        **inspections.excluded_rows, **notifications.excluded_rows,
        **_plants_set_aside_summary(inspection_set_aside, notification_set_aside),
    }

    financial_year = financial_year_label(as_of_date)
    inspections_by_plant = _rows_by_plant(inspection_rows)
    notifications_by_plant = _rows_by_plant(notification_rows)

    batches: list[QCSAPUploadBatch] = []
    lab_codes: set[str] = set()
    for plant_code in sorted(set(inspections_by_plant) | set(notifications_by_plant)):
        lab_code = SAP_PLANT_LAB_CODES[plant_code]
        get_sap_reporting_laboratory(lab_code)
        lab_codes.add(lab_code)
        batches.append(_persist_sap_lab_snapshot(
            lab_code=lab_code,
            plant_code=plant_code,
            inspection_rows=inspections_by_plant.get(plant_code, []),
            notification_rows=notifications_by_plant.get(plant_code, []),
            as_of_date=as_of_date,
            inspection_source=inspection_source,
            inspection_filename=inspection_filename,
            notification_source=notification_source,
            notification_filename=notification_filename,
            uploaded_by=uploaded_by,
            excluded_rows=excluded_rows,
        ))

    db.session.flush()
    reconciliation = None
    if reconcile and lab_codes:
        keep = {
            record.source_key
            for record in QCSAPRecord.query.filter(
                QCSAPRecord.lab_code.in_(lab_codes),
                QCSAPRecord.last_seen_batch_id.in_([batch.id for batch in batches]),
            ).all()
        }
        reconciliation = _reconcile_financial_year(lab_codes, financial_year, keep)

    return {
        "batches": batches,
        "as_of_date": as_of_date,
        "financial_year": financial_year,
        "reconciliation": reconciliation,
        "record_count": sum(batch.record_count for batch in batches),
        "excluded_rows": excluded_rows,
    }


def import_sap_panvel_exports(
    inspection_source: bytes,
    inspection_filename: str,
    notification_source: bytes,
    notification_filename: str,
    uploaded_by: int | None,
) -> QCSAPUploadBatch:
    """Compatibility wrapper for the original Panvel pilot endpoint."""
    return import_sap_lab_exports(
        PANVEL_LAB_CODE, inspection_source, inspection_filename,
        notification_source, notification_filename, uploaded_by,
    )


def financial_year_scope(as_of_date: date) -> dict[str, Any]:
    """How a screen states the span of SAP data it is showing.

    The monitoring base is a financial year, not the latest export, so every
    count on a page is a year-to-date figure.  Saying so where the counts are
    read stops a daily snapshot being inferred from them.
    """
    start_date = financial_year_start(as_of_date)
    return {
        "label": financial_year_label(as_of_date),
        "start_date": start_date,
        "note": f"All SAP notifications created on or after {start_date:%d.%m.%Y}",
    }


def financial_year_records(lab_code: str, batch: QCSAPUploadBatch):
    """A laboratory's records for the financial year the batch belongs to.

    A daily SAP export carries only the notifications that are still current
    plus those raised that morning, so the batch it creates holds a fraction of
    the year.  Every screen reads the year the batch falls in, which keeps the
    base data loaded at the start of that year in view.
    """
    return QCSAPRecord.query.filter_by(
        lab_code=lab_code, financial_year=financial_year_label(batch.as_of_date),
    )


def latest_sap_batch(lab_code: str) -> QCSAPUploadBatch | None:
    get_sap_reporting_laboratory(lab_code)
    return QCSAPUploadBatch.query.filter_by(lab_code=lab_code).order_by(
        QCSAPUploadBatch.as_of_date.desc(), QCSAPUploadBatch.id.desc(),
    ).first()


def latest_sap_panvel_batch() -> QCSAPUploadBatch | None:
    return latest_sap_batch(PANVEL_LAB_CODE)


def _latest_lab_updates(records: list[QCSAPRecord]) -> dict[int, QCSAPLabUpdate]:
    if not records:
        return {}
    updates = QCSAPLabUpdate.query.filter(
        QCSAPLabUpdate.record_id.in_([record.id for record in records])
    ).order_by(QCSAPLabUpdate.record_id, QCSAPLabUpdate.created_at.desc(), QCSAPLabUpdate.id.desc()).all()
    latest: dict[int, QCSAPLabUpdate] = {}
    for update in updates:
        latest.setdefault(update.record_id, update)
    return latest


def _latest_monitoring_dispositions(records: list[QCSAPRecord]) -> dict[int, QCSAPMonitoringDisposition]:
    """Return the latest immutable QC-admin decision for each SAP record."""
    if not records:
        return {}
    decisions = QCSAPMonitoringDisposition.query.filter(
        QCSAPMonitoringDisposition.record_id.in_([record.id for record in records])
    ).order_by(
        QCSAPMonitoringDisposition.record_id,
        QCSAPMonitoringDisposition.created_at.desc(),
        QCSAPMonitoringDisposition.id.desc(),
    ).all()
    latest: dict[int, QCSAPMonitoringDisposition] = {}
    for decision in decisions:
        latest.setdefault(decision.record_id, decision)
    return latest


def _work_center_snapshot(value: str | None) -> str | None:
    return _text(value) or None


def _monitoring_disposition_state(
    record: QCSAPRecord,
    disposition: QCSAPMonitoringDisposition | None,
) -> dict[str, bool]:
    """Determine whether an active exclusion still matches the SAP position.

    The decision survives a daily upload only while SAP's official status and
    work-centre assignment are unchanged.  A change returns it to the
    Corporate Chemistry review queue rather than silently hiding it.
    """
    active_exclusion = bool(disposition and disposition.decision == "exclude_non_actionable")
    if not active_exclusion or record.official_status != "open":
        return {"is_excluded": False, "requires_review": False}
    changed_since_decision = (
        record.official_status != disposition.official_status_at_decision
        or _work_center_snapshot(record.work_center) != disposition.work_center_at_decision
    )
    return {
        "is_excluded": not changed_since_decision,
        "requires_review": changed_since_decision,
    }


def _reconciliation_state(record: QCSAPRecord, update: QCSAPLabUpdate | None) -> tuple[str, str]:
    if record.official_status == "completed":
        return "sap_confirmed", "SAP complete"
    if update is None:
        return "awaiting_lab", "Lab update requested"
    if update.activity_status == "action_completed":
        return "awaiting_sap_confirmation", "Lab says complete — SAP confirmation pending"
    return "lab_updated", "Lab update received"


def source_completeness_label(record: QCSAPRecord) -> str:
    """Say what the pairing actually means, not which export the row came from.

    "Notification Only" was accurate about provenance and misleading on the
    page: it sat directly under the inspection-lot number the notification
    states, so it read as contradicting the number above it.  What it means is
    that the stated lot has no row in the paired QA33 export — which is the
    fact a reader needs, because it is why no usage decision could be joined.

    A notification that states no lot at all is a different case and says so:
    there is nothing missing from QA33 to go looking for.
    """
    if record.source_completeness == "notification_only":
        return (
            "Notification only · stated lot not in QA33 export"
            if record.inspection_lot_number
            else "Notification only · no inspection lot stated"
        )
    if record.source_completeness == "inspection_lot_only":
        return "Inspection lot only · no notification in SAP"
    if record.source_completeness == "matched":
        return "Matched · lot and notification paired"
    return (record.source_completeness or "Not recorded").replace("_", " ").title()


def _completed_notification_without_ud_details(record: QCSAPRecord) -> bool:
    """Identify a notification closure for which the paired QA33 row is absent.

    The notification workbook can state a lot number and a completion date even
    when the same lot is not present in the paired inspection-lot export.  Such
    a closure must remain distinct from an SAP usage decision.
    """
    return bool(
        record.official_status == "completed"
        and record.source_completeness == "notification_only"
        and record.completion_date is not None
        and not record.usage_decision_code
        and not record.sap_system_status
    )


def _sample_timing_fields(
    record: QCSAPRecord, update: QCSAPLabUpdate | None,
) -> dict[str, int | None]:
    """Return the sample-movement intervals using the SAP notification date.

    Courier time runs from laboratory sampling to the notification date.  Time
    in queue runs from notification to the laboratory testing-start date.  A
    missing source date intentionally produces no duration rather than an
    inferred value from the inspection-lot receipt.
    """
    sampling_date = update.sampling_date if update else None
    testing_start_date = update.actual_start_date if update else None
    notification_date = record.notification_start_date
    return {
        "courier_days": (
            (notification_date - sampling_date).days
            if notification_date and sampling_date else None
        ),
        "time_in_queue_days": (
            (testing_start_date - notification_date).days
            if notification_date and testing_start_date else None
        ),
    }


def _latest_non_sap_updates(samples: list[QCNonSAPSample]) -> dict[int, QCNonSAPSampleUpdate]:
    if not samples:
        return {}
    updates = QCNonSAPSampleUpdate.query.filter(
        QCNonSAPSampleUpdate.sample_id.in_([sample.id for sample in samples])
    ).order_by(
        QCNonSAPSampleUpdate.sample_id,
        QCNonSAPSampleUpdate.created_at.desc(),
        QCNonSAPSampleUpdate.id.desc(),
    ).all()
    latest: dict[int, QCNonSAPSampleUpdate] = {}
    for update in updates:
        latest.setdefault(update.sample_id, update)
    return latest


def _non_sap_entries(lab_code: str, *, include_closed: bool = False) -> list[dict[str, Any]]:
    query = QCNonSAPSample.query.filter_by(lab_code=lab_code)
    if not include_closed:
        query = query.filter(~QCNonSAPSample.current_status.in_(NON_SAP_CLOSED_STATUSES))
    samples = query.order_by(
        QCNonSAPSample.sample_receipt_date.asc(), QCNonSAPSample.id.asc(),
    ).all()
    updates = _latest_non_sap_updates(samples)
    return [{"sample": sample, "latest_update": updates.get(sample.id)} for sample in samples]


def sap_lab_dashboard_data(lab_code: str) -> dict[str, Any]:
    """One laboratory's SAP-authoritative workload plus separate exceptions."""
    laboratory = get_sap_reporting_laboratory(lab_code)
    batch = latest_sap_batch(lab_code)
    non_sap_entries = _non_sap_entries(lab_code)
    if batch is None:
        return {
            "laboratory": laboratory,
            "lab_code": lab_code,
            "batch": None,
            "records": [],
            "open_records": [],
            "kpis": {},
            "work_centers": [],
            "usage_decisions": [],
            "recent_batches": [],
            "lab_update_statuses": LAB_UPDATE_STATUSES,
            "non_sap_entries": non_sap_entries,
            "non_sap_statuses": NON_SAP_STATUSES,
            "non_sap_status_labels": NON_SAP_STATUS_LABELS,
            "financial_year_scope": None,
        }

    records = financial_year_records(lab_code, batch).order_by(
        QCSAPRecord.official_status.asc(), QCSAPRecord.id.asc(),
    ).all()
    updates = _latest_lab_updates(records)
    dispositions = _latest_monitoring_dispositions(records)
    specifications_by_material_code = _corporate_specifications_by_material_code() if records else {}
    entries = []
    states = Counter()
    for record in records:
        update = updates.get(record.id)
        disposition = dispositions.get(record.id)
        disposition_state = _monitoring_disposition_state(record, disposition)
        reconciliation_key, reconciliation_label = _reconciliation_state(record, update)
        if disposition_state["requires_review"]:
            reconciliation_key, reconciliation_label = "exclusion_review", "Exclusion review required"
        specification_fields = _corporate_specification_fields(
            record.material_code, specifications_by_material_code,
        )
        stt_fields = _standard_testing_time_fields(
            record, specification_fields["stt_days"], batch.as_of_date,
        )
        if record.official_status == "open" and not disposition_state["is_excluded"] and not disposition_state["requires_review"]:
            states[reconciliation_key] += 1
        entries.append({
            "record": record,
            "lab_update": update,
            **specification_fields,
            **stt_fields,
            "disposition": disposition,
            "is_excluded": disposition_state["is_excluded"],
            "exclusion_requires_review": disposition_state["requires_review"],
            "exclusion_reason_label": SAP_EXCLUSION_REASON_LABELS.get(
                disposition.reason_code if disposition else "", "Excluded from monitoring",
            ),
            "reconciliation_key": reconciliation_key,
            "reconciliation_label": reconciliation_label,
            **_sample_timing_fields(record, update),
        })

    open_entries = [
        entry for entry in entries
        if entry["record"].official_status == "open"
        and not entry["is_excluded"]
        and not entry["exclusion_requires_review"]
    ]
    excluded_entries = [
        entry for entry in entries
        if entry["record"].official_status == "open" and entry["is_excluded"]
    ]
    exclusion_review_entries = [
        entry for entry in entries
        if entry["record"].official_status == "open" and entry["exclusion_requires_review"]
    ]
    work_centers: dict[str, dict[str, Any]] = {}
    for entry in open_entries:
        name = entry["record"].work_center or "Not assigned in SAP"
        item = work_centers.setdefault(name, {
            "name": name, "open": 0, "stt_overdue": 0, "awaiting_lab": 0,
            "is_non_sap": False,
        })
        item["open"] += 1
        item["stt_overdue"] += int(entry["stt_overdue"])
        item["awaiting_lab"] += int(entry["reconciliation_key"] == "awaiting_lab")
    if non_sap_entries:
        work_centers["__non_sap__"] = {
            "name": "Non-SAP samples",
            "open": len(non_sap_entries),
            "stt_overdue": 0,
            "awaiting_lab": sum(
                item["sample"].current_status == "awaiting_sample"
                for item in non_sap_entries
            ),
            "is_non_sap": True,
        }

    kpis = {
        "total": len(records),
        "completed": sum(1 for record in records if record.official_status == "completed"),
        "open": len(open_entries),
        "accepted": sum(1 for record in records if usage_decision_outcome(record.usage_decision_code) == "accepted"),
        "rejected": sum(1 for record in records if usage_decision_outcome(record.usage_decision_code) == "rejected"),
        "stt_overdue": sum(1 for entry in open_entries if entry["stt_overdue"]),
        "awaiting_lab": states["awaiting_lab"],
        "awaiting_sap_confirmation": states["awaiting_sap_confirmation"],
        "excluded_from_monitoring": len(excluded_entries),
        "exclusion_review": len(exclusion_review_entries),
        "material_standard_coverage": sum(1 for entry in entries if entry["stt_days"] is not None),
        "unmatched_inspection": batch.unmatched_inspection_count,
        "unmatched_notification": batch.unmatched_notification_count,
        "non_sap_pending": len(non_sap_entries),
        "combined_pending": len(open_entries) + len(non_sap_entries),
    }
    usage_decisions = Counter(record.usage_decision_code or "Not recorded" for record in records)
    return {
        "laboratory": laboratory,
        "lab_code": lab_code,
        "batch": batch,
        "financial_year_scope": financial_year_scope(batch.as_of_date),
        "records": entries,
        "open_records": sorted(
            [
                entry for entry in open_entries
                if entry["stt_overdue"] or entry["reconciliation_key"] != "sap_confirmed"
            ],
            key=lambda entry: (
                entry["subgroup_key"] == CORPORATE_SPECIFICATION_UNMATCHED_KEY,
                entry["subgroup_label"].casefold(),
                not entry["stt_overdue"],
                entry["stt_due_date"] or date.max,
                entry["record"].notification_no or "",
                entry["record"].id,
            ),
        ),
        "excluded_entries": excluded_entries,
        "exclusion_review_entries": exclusion_review_entries,
        "sap_exclusion_reasons": SAP_EXCLUSION_REASONS,
        "sap_exclusion_reason_labels": SAP_EXCLUSION_REASON_LABELS,
        "kpis": kpis,
        "work_centers": sorted(
            work_centers.values(),
            key=lambda item: (item["is_non_sap"], -item["stt_overdue"], -item["open"], item["name"]),
        ),
        "usage_decisions": sorted(({"label": label, "count": count} for label, count in usage_decisions.items()), key=lambda item: (-item["count"], item["label"])),
        "recent_batches": QCSAPUploadBatch.query.filter_by(lab_code=lab_code).order_by(
            QCSAPUploadBatch.as_of_date.desc(), QCSAPUploadBatch.id.desc(),
        ).limit(14).all(),
        "lab_update_statuses": LAB_UPDATE_STATUSES,
        "non_sap_entries": non_sap_entries,
        "non_sap_statuses": NON_SAP_STATUSES,
        "non_sap_status_labels": NON_SAP_STATUS_LABELS,
    }


def sap_panvel_dashboard_data() -> dict[str, Any]:
    """Compatibility wrapper for existing Panvel links and tests."""
    return sap_lab_dashboard_data(PANVEL_LAB_CODE)


def _register_subgroup_groups(
    entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Section the register rows under their Corporate Specification sub-group.

    The register is read one category at a time, so the grouping belongs here
    rather than in the template.  Sub-groups follow the Corporate
    Specifications order used by the review deck, and material codes with no
    register match are collected in a final section instead of being spread
    through the table.
    """
    from app.core.services.csc_utils import SPEC_SUBSET_ORDER

    subgroup_rank = {key: index for index, key in enumerate(SPEC_SUBSET_ORDER)}
    grouped: dict[str, list[dict[str, Any]]] = {}
    labels: dict[str, str] = {}
    for entry in entries:
        key = entry["subgroup_key"] or CORPORATE_SPECIFICATION_UNMATCHED_KEY
        grouped.setdefault(key, []).append(entry)
        labels[key] = entry["subgroup_label"]
    return [
        {
            "key": key,
            "label": labels[key],
            "is_unmatched": key == CORPORATE_SPECIFICATION_UNMATCHED_KEY,
            "entries": grouped[key],
        }
        for key in sorted(
            grouped,
            key=lambda value: (
                99 if value == CORPORATE_SPECIFICATION_UNMATCHED_KEY
                else subgroup_rank.get(value, 90),
                labels[value].casefold(),
            ),
        )
    ]


# The register renders every matching row at once, so it is capped. The cap is
# named here because the page has to say what it is holding back — a caption
# counted after truncation reads "500 matching" however many actually matched.
SAP_REGISTER_VISIBLE_LIMIT = 500

# A default argument is bound once at import, which would freeze the cap into
# the signature and make the constant above unpatchable. The sentinel defers
# the lookup to call time, so the cap stays a single source of truth.
_REGISTER_DEFAULT_LIMIT = object()


def sap_sample_register_data(
    lab_code: str = "", search: str = "", status: str = "", subgroup: str = "",
    *, limit: int | None = _REGISTER_DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Return the current, SAP-authoritative register across reporting labs.

    A record appears when it belongs to that laboratory's current financial
    year, whichever daily export last reported it.  The legacy weekly-workbook
    ``QCSample`` table is never consulted here: its historical rows have
    different source authority and remain available only through the
    local-workbook laboratory screens.
    """
    laboratories = sap_reporting_laboratories()
    laboratories_by_code = {laboratory["code"]: laboratory for laboratory in laboratories}
    if lab_code and lab_code not in laboratories_by_code:
        raise ValueError("Choose a laboratory configured for SAP daily monitoring.")

    selected_laboratories = (
        [laboratories_by_code[lab_code]] if lab_code else laboratories
    )
    current: list[tuple[dict[str, Any], QCSAPUploadBatch, QCSAPRecord]] = []
    for laboratory in selected_laboratories:
        batch = latest_sap_batch(laboratory["code"])
        if batch is None:
            continue
        records = financial_year_records(laboratory["code"], batch).all()
        current.extend((laboratory, batch, record) for record in records)

    records = [record for _, _, record in current]
    updates = _latest_lab_updates(records)
    dispositions = _latest_monitoring_dispositions(records)
    specifications_by_material_code = _corporate_specifications_by_material_code() if records else {}
    search_term = _text(search).casefold()
    entries: list[dict[str, Any]] = []
    subgroup_filters: dict[str, str] = {}
    for laboratory, batch, record in current:
        update = updates.get(record.id)
        disposition_state = _monitoring_disposition_state(
            record, dispositions.get(record.id),
        )
        reconciliation_key, reconciliation_label = _reconciliation_state(record, update)
        outcome = usage_decision_outcome(record.usage_decision_code)
        specification_fields = _corporate_specification_fields(
            record.material_code, specifications_by_material_code,
        )
        stt_fields = _standard_testing_time_fields(
            record, specification_fields["stt_days"], batch.as_of_date,
        )
        subgroup_key = specification_fields["subgroup_key"]
        subgroup_label = specification_fields["subgroup_label"]
        subgroup_filters[subgroup_key] = subgroup_label
        entry = {
            "laboratory": laboratory,
            "batch": batch,
            "record": record,
            "lab_update": update,
            **specification_fields,
            **stt_fields,
            "outcome": outcome,
            "reconciliation_key": reconciliation_key,
            "reconciliation_label": reconciliation_label,
            "is_excluded": disposition_state["is_excluded"],
            "exclusion_requires_review": disposition_state["requires_review"],
            **_sample_timing_fields(record, update),
        }
        if search_term:
            searchable = " ".join(filter(None, (
                record.inspection_lot_number,
                record.notification_no,
                record.po_number,
                record.material_code,
                record.material_description,
                record.work_center,
            ))).casefold()
            if search_term not in searchable:
                continue
        if status == "open" and record.official_status != "open":
            continue
        if status == "completed" and record.official_status != "completed":
            continue
        if status in {"accepted", "rejected"} and outcome != status:
            continue
        if status == "excluded" and not entry["is_excluded"]:
            continue
        if status == "exclusion_review" and not entry["exclusion_requires_review"]:
            continue
        if subgroup and subgroup_key != subgroup:
            continue
        entries.append(entry)

    entries.sort(
        key=lambda entry: (
            entry["batch"].as_of_date,
            entry["record"].start_inspection_date
            or entry["record"].notification_start_date
            or date.min,
            entry["record"].id,
        ),
        reverse=True,
    )
    # The page caps what it renders; the workbook export passes limit=None so
    # a download is never a truncated answer to the question the filters asked.
    if limit is _REGISTER_DEFAULT_LIMIT:
        limit = SAP_REGISTER_VISIBLE_LIMIT
    visible = entries if limit is None else entries[:limit]
    # Each reporting laboratory is read at its own latest snapshot, but they
    # share one financial year; only name it where they genuinely agree.
    scope_dates = {batch.as_of_date for _, batch, _ in current}
    scope_labels = {financial_year_label(value) for value in scope_dates}
    return {
        "financial_year_scope": (
            financial_year_scope(max(scope_dates)) if len(scope_labels) == 1 else None
        ),
        "entries": visible,
        # The true match count, before the cap, so the caption can be honest
        # about a filter that narrowed nothing.
        "total_matching": len(entries),
        "visible_limit": limit,
        "is_truncated": limit is not None and len(entries) > limit,
        "groups": _register_subgroup_groups(visible),
        "laboratories": laboratories,
        "status_filters": SAP_REGISTER_STATUS_FILTERS,
        "subgroup_filters": [
            {"key": key, "label": label}
            for key, label in sorted(
                subgroup_filters.items(),
                key=lambda item: (item[0] == CORPORATE_SPECIFICATION_UNMATCHED_KEY, item[1]),
            )
        ],
    }


def _sap_monitoring_counts(
    lab_code: str, batch: QCSAPUploadBatch | None,
) -> dict[str, int]:
    """Split a laboratory's SAP-open records into the three monitoring states."""
    counts = {"sap_open": 0, "excluded_from_monitoring": 0, "exclusion_review": 0}
    if batch is None:
        return counts
    current_records = financial_year_records(lab_code, batch).filter_by(
        official_status="open",
    ).all()
    latest_dispositions = _latest_monitoring_dispositions(current_records)
    for record in current_records:
        state = _monitoring_disposition_state(record, latest_dispositions.get(record.id))
        counts["excluded_from_monitoring"] += int(state["is_excluded"])
        counts["exclusion_review"] += int(state["requires_review"])
        counts["sap_open"] += int(not state["is_excluded"] and not state["requires_review"])
    return counts


def sap_open_counts_by_lab() -> dict[str, int]:
    """Actionable SAP-open items per reporting laboratory.

    This is the one number the lab navigator shows for a laboratory the reader
    does not belong to, so it must mean what the laboratory dashboard means:
    excluded and review-pending records are not actionable and are left out.
    """
    return {
        laboratory["code"]: _sap_monitoring_counts(
            laboratory["code"], latest_sap_batch(laboratory["code"]),
        )["sap_open"]
        for laboratory in sap_reporting_laboratories()
    }


def sap_control_data() -> dict[str, Any]:
    """Corporate Chemistry's consolidated source and exception control view."""
    laboratories = _laboratories_by_code()
    cards: list[dict[str, Any]] = []
    for laboratory in sap_reporting_laboratories():
        lab_code = laboratory["code"]
        batch = latest_sap_batch(lab_code)
        counts = _sap_monitoring_counts(lab_code, batch)
        sap_open = counts["sap_open"]
        non_sap_pending = QCNonSAPSample.query.filter_by(lab_code=lab_code).filter(
            ~QCNonSAPSample.current_status.in_(NON_SAP_CLOSED_STATUSES)
        ).count()
        cards.append({
            "laboratory": laboratory,
            "batch": batch,
            # What the newest upload moved, so the reader can see the day's
            # work rather than only the standing position.
            "changes": _batch_summary(batch).get("changes") or {},
            "sap_open": sap_open,
            "excluded_from_monitoring": counts["excluded_from_monitoring"],
            "exclusion_review": counts["exclusion_review"],
            "non_sap_pending": non_sap_pending,
            "combined_pending": sap_open + non_sap_pending,
        })
    non_sap_entries = []
    for laboratory in sorted(laboratories.values(), key=lambda item: item["name"]):
        for entry in _non_sap_entries(laboratory["code"]):
            non_sap_entries.append({**entry, "laboratory": laboratory})
    fallback_laboratories = [
        laboratory for code, laboratory in laboratories.items()
        if code not in SAP_REPLACED_WEEKLY_LAB_CODES
    ]
    return {
        "sap_laboratories": sap_reporting_laboratories(),
        "sap_plant_mappings": sap_plant_mappings(),
        "all_laboratories": sorted(laboratories.values(), key=lambda item: item["name"]),
        "control_cards": cards,
        "change_categories": CHANGE_CATEGORIES,
        "non_sap_entries": non_sap_entries,
        "non_sap_statuses": NON_SAP_STATUSES,
        "non_sap_status_labels": NON_SAP_STATUS_LABELS,
        "workbook_fallback_laboratories": fallback_laboratories,
    }


def _batch_summary(batch: QCSAPUploadBatch | None) -> dict[str, Any]:
    """Read a retained SAP snapshot summary without treating it as local data."""
    if batch is None:
        return {}
    try:
        value = json.loads(batch.summary_json or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def non_sap_register_data(lab_codes: set[str] | None = None) -> dict[str, Any]:
    """The controlled non-SAP register, summarised for management reading.

    These samples are deliberately not SAP records and are never merged into
    the SAP position — that separation is the point of the register.  What was
    missing was any management view of it at all: the work was visible only
    from a single laboratory's dashboard, so nobody could see how much of it
    the portfolio was carrying.

    An outcome here is the laboratory's declared result, not an SAP usage
    decision, so it is counted and labelled separately throughout.
    """
    laboratories = [
        laboratory for laboratory in sap_reporting_laboratories()
        if lab_codes is None or laboratory["code"] in lab_codes
    ]
    today = date.today()
    entries: list[dict[str, Any]] = []
    by_laboratory: list[dict[str, Any]] = []
    status_counts: Counter = Counter()
    chemicals: dict[str, dict[str, Any]] = {}

    for laboratory in laboratories:
        lab_entries = []
        for entry in _non_sap_entries(laboratory["code"], include_closed=True):
            sample = entry["sample"]
            is_closed = sample.current_status in NON_SAP_CLOSED_STATUSES
            # Only work still running can be late; a closed sample is history.
            is_overdue = bool(
                not is_closed
                and sample.expected_completion_date
                and sample.expected_completion_date < today
            )
            item = {
                **entry,
                "laboratory": laboratory,
                "status_label": NON_SAP_STATUS_LABELS.get(
                    sample.current_status, sample.current_status,
                ),
                "is_closed": is_closed,
                "is_overdue": is_overdue,
                "outcome": sample.reported_outcome,
                "age_days": (
                    (today - sample.sample_receipt_date).days
                    if sample.sample_receipt_date else None
                ),
            }
            lab_entries.append(item)
            status_counts[sample.current_status] += 1
            name = (sample.chemical_name or "Not stated").strip() or "Not stated"
            chemical = chemicals.setdefault(name, {
                "chemical_name": name, "total": 0, "pending": 0,
                "closed": 0, "failed": 0, "laboratories": set(),
            })
            chemical["total"] += 1
            chemical["pending"] += 0 if is_closed else 1
            chemical["closed"] += 1 if is_closed else 0
            chemical["failed"] += 1 if sample.current_status == "closed_fail" else 0
            chemical["laboratories"].add(laboratory["name"])
        entries.extend(lab_entries)
        if lab_entries:
            by_laboratory.append({
                "laboratory": laboratory,
                "total": len(lab_entries),
                "pending": sum(1 for item in lab_entries if not item["is_closed"]),
                "overdue": sum(1 for item in lab_entries if item["is_overdue"]),
                "closed_pass": sum(
                    1 for item in lab_entries
                    if item["sample"].current_status == "closed_pass"
                ),
                "closed_fail": sum(
                    1 for item in lab_entries
                    if item["sample"].current_status == "closed_fail"
                ),
            })

    closed_pass = status_counts["closed_pass"]
    closed_fail = status_counts["closed_fail"]
    decided = closed_pass + closed_fail
    pending_entries = [item for item in entries if not item["is_closed"]]
    kpis = {
        "total": len(entries),
        "pending": len(pending_entries),
        "closed": decided,
        "closed_pass": closed_pass,
        "closed_fail": closed_fail,
        "overdue": sum(1 for item in entries if item["is_overdue"]),
        "no_expected_date": sum(
            1 for item in pending_entries if not item["sample"].expected_completion_date
        ),
        # Declared results only; pending work is not a pass.
        "fail_rate": round(closed_fail / decided * 100, 1) if decided else None,
    }
    return {
        "non_sap_kpis": kpis,
        "non_sap_entries": sorted(
            entries,
            key=lambda item: (
                item["is_closed"],
                not item["is_overdue"],
                item["sample"].expected_completion_date or date.max,
                item["laboratory"]["name"],
                item["sample"].sample_reference,
            ),
        ),
        "non_sap_by_laboratory": sorted(
            by_laboratory, key=lambda item: (-item["overdue"], -item["pending"], item["laboratory"]["name"]),
        ),
        "non_sap_by_status": [
            {"key": key, "label": label, "count": status_counts[key]}
            for key, label in NON_SAP_STATUSES if status_counts[key]
        ],
        "non_sap_chemicals": sorted(
            ({**item, "laboratories": sorted(item["laboratories"])} for item in chemicals.values()),
            key=lambda item: (-item["total"], item["chemical_name"].casefold()),
        )[:ANALYTICS_TABLE_LIMIT],
        "non_sap_chemical_total": len(chemicals),
        "non_sap_status_labels": NON_SAP_STATUS_LABELS,
    }


def sap_management_data(lab_codes: set[str] | None = None) -> dict[str, Any]:
    """Build the management view exclusively from each laboratory's latest SAP snapshot.

    A row enters this view only when it was present in the most recent SAP
    upload for its plant.  Historical local-workbook samples and declared
    non-SAP rows are intentionally excluded: they must never influence the
    official SAP position.
    """
    laboratories = [
        laboratory for laboratory in sap_reporting_laboratories()
        if lab_codes is None or laboratory["code"] in lab_codes
    ]
    specifications_by_material_code = _corporate_specifications_by_material_code()
    laboratory_reviews: list[dict[str, Any]] = []
    action_entries: list[dict[str, Any]] = []
    completed_without_ud_entries: list[dict[str, Any]] = []
    all_records: list[QCSAPRecord] = []

    for laboratory in laboratories:
        batch = latest_sap_batch(laboratory["code"])
        if batch is None:
            laboratory_reviews.append({
                "laboratory": laboratory,
                "batch": None,
                "records": [],
                "summary": {},
                "previous_summary": None,
                "previous_batch": None,
                "kpis": {
                    "total": 0, "official_open": 0, "actionable_open": 0,
                    "completed": 0, "accepted": 0, "rejected": 0,
                    "completed_without_ud_details": 0,
                    "stt_overdue": 0, "awaiting_lab": 0,
                    "awaiting_sap_confirmation": 0, "excluded": 0,
                    "exclusion_review": 0,
                },
            })
            continue

        records = financial_year_records(laboratory["code"], batch).order_by(
            QCSAPRecord.id.asc(),
        ).all()
        updates = _latest_lab_updates(records)
        dispositions = _latest_monitoring_dispositions(records)
        kpis = {
            "total": len(records), "official_open": 0, "actionable_open": 0,
            "completed": 0, "accepted": 0, "rejected": 0,
            "completed_without_ud_details": 0,
            "stt_overdue": 0, "awaiting_lab": 0,
            "awaiting_sap_confirmation": 0, "excluded": 0,
            "exclusion_review": 0,
        }
        entries: list[dict[str, Any]] = []
        for record in records:
            update = updates.get(record.id)
            disposition_state = _monitoring_disposition_state(
                record, dispositions.get(record.id),
            )
            reconciliation_key, reconciliation_label = _reconciliation_state(record, update)
            is_open = record.official_status == "open"
            is_actionable = is_open and not disposition_state["is_excluded"] and not disposition_state["requires_review"]
            specification_fields = _corporate_specification_fields(
                record.material_code, specifications_by_material_code,
            )
            stt_fields = _standard_testing_time_fields(
                record, specification_fields["stt_days"], batch.as_of_date,
            )
            stt_overdue = bool(is_actionable and stt_fields["stt_overdue"])
            outcome = usage_decision_outcome(record.usage_decision_code)
            completed_without_ud_details = _completed_notification_without_ud_details(record)
            kpis["official_open"] += int(is_open)
            kpis["actionable_open"] += int(is_actionable)
            kpis["completed"] += int(record.official_status == "completed")
            kpis["accepted"] += int(outcome == "accepted")
            kpis["rejected"] += int(outcome == "rejected")
            kpis["completed_without_ud_details"] += int(completed_without_ud_details)
            kpis["stt_overdue"] += int(stt_overdue)
            kpis["excluded"] += int(disposition_state["is_excluded"])
            kpis["exclusion_review"] += int(disposition_state["requires_review"])
            if is_actionable:
                kpis["awaiting_lab"] += int(reconciliation_key == "awaiting_lab")
                kpis["awaiting_sap_confirmation"] += int(reconciliation_key == "awaiting_sap_confirmation")
            entry = {
                "laboratory": laboratory,
                "batch": batch,
                "record": record,
                "lab_update": update,
                **specification_fields,
                **stt_fields,
                "is_actionable": is_actionable,
                "stt_overdue": stt_overdue,
                "reconciliation_key": reconciliation_key,
                "reconciliation_label": reconciliation_label,
                "is_excluded": disposition_state["is_excluded"],
                "exclusion_requires_review": disposition_state["requires_review"],
                "completed_without_ud_details": completed_without_ud_details,
            }
            entries.append(entry)
            if is_actionable:
                action_entries.append(entry)
            if completed_without_ud_details:
                completed_without_ud_entries.append(entry)
        previous_batch = QCSAPUploadBatch.query.filter_by(lab_code=laboratory["code"]).order_by(
            QCSAPUploadBatch.as_of_date.desc(), QCSAPUploadBatch.id.desc(),
        ).offset(1).first()
        laboratory_reviews.append({
            "laboratory": laboratory,
            "batch": batch,
            "records": entries,
            "summary": _batch_summary(batch),
            "previous_summary": _batch_summary(previous_batch) if previous_batch else None,
            "previous_batch": previous_batch,
            "kpis": kpis,
        })
        all_records.extend(records)

    action_entries.sort(key=lambda item: (
        not item["stt_overdue"],
        item["stt_due_date"] or date.max,
        not bool(item["record"].work_center),
        item["laboratory"]["name"].casefold(),
        item["record"].notification_no or "",
        item["record"].id,
    ))
    completed_without_ud_entries.sort(key=lambda item: (
        item["record"].completion_date or date.min,
        item["laboratory"]["name"].casefold(),
        item["record"].notification_no or "",
        item["record"].id,
    ), reverse=True)
    work_centers: dict[str, dict[str, Any]] = {}
    materials: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in action_entries:
        record = entry["record"]
        centre_name = record.work_center or "Not assigned in SAP"
        centre = work_centers.setdefault(centre_name, {
            "name": centre_name, "open": 0, "stt_overdue": 0,
            "awaiting_lab": 0, "laboratories": set(),
        })
        centre["open"] += 1
        centre["stt_overdue"] += int(entry["stt_overdue"])
        centre["awaiting_lab"] += int(entry["reconciliation_key"] == "awaiting_lab")
        centre["laboratories"].add(entry["laboratory"]["name"])
        material_name = record.material_description or "Material not stated in SAP"
        key = ((record.material_code or "—"), material_name)
        material = materials.setdefault(key, {
            "material_code": record.material_code or "—",
            "material_description": material_name,
            "specification_match": entry["specification_match"],
            "subgroup_label": entry["subgroup_label"],
            "specification_no": entry["specification_no"],
            "open": 0, "stt_overdue": 0, "awaiting_lab": 0,
            "laboratories": set(), "work_centers": set(),
        })
        material["open"] += 1
        material["stt_overdue"] += int(entry["stt_overdue"])
        material["awaiting_lab"] += int(entry["reconciliation_key"] == "awaiting_lab")
        material["laboratories"].add(entry["laboratory"]["name"])
        material["work_centers"].add(centre_name)

    kpis = {
        key: sum(review["kpis"][key] for review in laboratory_reviews)
        for key in (
            "total", "official_open", "actionable_open", "completed", "accepted", "rejected",
            "completed_without_ud_details",
            "stt_overdue", "awaiting_lab", "awaiting_sap_confirmation", "excluded", "exclusion_review",
        )
    }
    kpis["usage_not_recorded"] = kpis["total"] - kpis["accepted"] - kpis["rejected"]
    snapshot_dates = sorted({review["batch"].as_of_date for review in laboratory_reviews if review["batch"]})
    source_as_of_label = (
        f"SAP position as at {snapshot_dates[0]:%d %B %Y}"
        if len(snapshot_dates) == 1 else
        "Latest SAP snapshot by laboratory"
    )
    trend = []
    for review in laboratory_reviews:
        batch, previous = review["batch"], review["previous_batch"]
        if batch is None:
            continue
        current_open = int(review["summary"].get("open_records", 0) or 0)
        previous_open = int(review["previous_summary"].get("open_records", 0) or 0) if review["previous_summary"] else None
        trend.append({
            "laboratory": review["laboratory"], "batch": batch, "previous_batch": previous,
            "current_open": current_open, "previous_open": previous_open,
            "open_change": current_open - previous_open if previous_open is not None else None,
            "current_total": int(review["summary"].get("total_records", review["kpis"]["total"]) or 0),
        })

    review_dates = {
        review["batch"].as_of_date for review in laboratory_reviews if review["batch"] is not None
    }
    review_labels = {financial_year_label(value) for value in review_dates}
    return {
        "financial_year_scope": (
            financial_year_scope(max(review_dates)) if len(review_labels) == 1 else None
        ),
        "laboratory_reviews": laboratory_reviews,
        "reporting_labs": sum(review["batch"] is not None for review in laboratory_reviews),
        "configured_labs": len(laboratories),
        "missing_snapshots": [review for review in laboratory_reviews if review["batch"] is None],
        "kpis": kpis,
        "action_entries": action_entries,
        "completed_without_ud_entries": completed_without_ud_entries,
        "work_centers": sorted(({
            **item, "laboratories": sorted(item["laboratories"]),
        } for item in work_centers.values()), key=lambda item: (-item["stt_overdue"], -item["open"], item["name"])),
        "materials": sorted(({
            **item,
            "laboratories": sorted(item["laboratories"]),
            "work_centers": sorted(item["work_centers"]),
        } for item in materials.values()), key=lambda item: (-item["stt_overdue"], -item["open"], item["material_description"].casefold())),
        "usage_decisions": [
            {"label": "UD A · Accepted", "count": kpis["accepted"], "tone": "success"},
            {"label": "UD R · Rejected", "count": kpis["rejected"], "tone": "danger"},
            {"label": "Usage decision not recorded", "count": kpis["usage_not_recorded"], "tone": "muted"},
        ],
        "trend": trend,
        "source_as_of_label": source_as_of_label,
        "source_dates": snapshot_dates,
        "scope_laboratories": laboratories,
    }


def create_sap_lab_update(
    record_id: int, form: dict[str, Any], updated_by: int | None, *, lab_code: str = PANVEL_LAB_CODE,
) -> QCSAPLabUpdate:
    get_sap_reporting_laboratory(lab_code)
    record = QCSAPRecord.query.filter_by(id=record_id, lab_code=lab_code).first()
    if record is None:
        raise ValueError("The requested SAP monitoring record is no longer available.")
    activity_status = _text(form.get("activity_status"))
    if activity_status not in LAB_UPDATE_STATUS_LABELS:
        raise ValueError("Choose a valid laboratory activity status.")
    sampling_date = _date(form.get("sampling_date"))
    actual_start_date = _date(form.get("actual_start_date"))
    expected = _date(form.get("expected_completion_date"))
    if sampling_date and actual_start_date and actual_start_date < sampling_date:
        raise ValueError("The laboratory actual start date cannot be earlier than the sampling date.")
    if sampling_date and record.start_inspection_date and sampling_date > record.start_inspection_date:
        raise ValueError("The sampling date cannot be later than the SAP receipt date.")
    update = QCSAPLabUpdate(
        record_id=record.id,
        activity_status=activity_status,
        sampling_date=sampling_date,
        actual_start_date=actual_start_date,
        expected_completion_date=expected,
        action_owner=_text(form.get("action_owner")) or None,
        delay_reason=_text(form.get("delay_reason")) or None,
        update_note=_text(form.get("update_note")) or None,
        updated_by=updated_by,
    )
    db.session.add(update)
    return update


def exclude_sap_record_from_monitoring(
    record_id: int, form: dict[str, Any], recorded_by: int | None, *, lab_code: str = PANVEL_LAB_CODE,
) -> QCSAPMonitoringDisposition:
    """Exclude a non-actionable SAP notification without removing SAP evidence."""
    get_sap_reporting_laboratory(lab_code)
    record = QCSAPRecord.query.filter_by(id=record_id, lab_code=lab_code).first()
    if record is None:
        raise ValueError("The requested SAP monitoring record is no longer available.")
    if record.official_status != "open":
        raise ValueError("Only SAP-open notifications can be excluded from current monitoring.")
    if not record.notification_no:
        raise ValueError("Only notification-backed SAP rows can be excluded. Inspection-lot-only rows remain monitored.")
    reason_code = _text(form.get("exclusion_reason"))
    if reason_code not in SAP_EXCLUSION_REASON_LABELS:
        raise ValueError("Choose why this SAP notification is non-actionable.")
    note = _text(form.get("exclusion_note"))
    if reason_code == "other" and not note:
        raise ValueError("Explain why this notification is non-actionable when choosing Other.")
    disposition = QCSAPMonitoringDisposition(
        record_id=record.id,
        decision="exclude_non_actionable",
        reason_code=reason_code,
        note=note or None,
        official_status_at_decision=record.official_status,
        work_center_at_decision=_work_center_snapshot(record.work_center),
        recorded_by=recorded_by,
    )
    db.session.add(disposition)
    return disposition


def reinstate_sap_record_for_monitoring(
    record_id: int, recorded_by: int | None, *, lab_code: str = PANVEL_LAB_CODE,
) -> QCSAPMonitoringDisposition:
    """Record a QC-admin reinstatement; the SAP row returns to the action list."""
    get_sap_reporting_laboratory(lab_code)
    record = QCSAPRecord.query.filter_by(id=record_id, lab_code=lab_code).first()
    if record is None:
        raise ValueError("The requested SAP monitoring record is no longer available.")
    latest = _latest_monitoring_dispositions([record]).get(record.id)
    if latest is None or latest.decision != "exclude_non_actionable":
        raise ValueError("This SAP notification is not currently excluded from monitoring.")
    disposition = QCSAPMonitoringDisposition(
        record_id=record.id,
        decision="reinstate",
        official_status_at_decision=record.official_status,
        work_center_at_decision=_work_center_snapshot(record.work_center),
        recorded_by=recorded_by,
    )
    db.session.add(disposition)
    return disposition


def _non_sap_outcome(status: str, value: Any) -> str | None:
    if status == "closed_pass":
        return "pass"
    if status == "closed_fail":
        return "fail"
    outcome = _text(value).casefold()
    if outcome not in {"", "pass", "fail"}:
        raise ValueError("Reported outcome must be pass, fail, or blank until a result is available.")
    return outcome or None


def _non_sap_values(form: dict[str, Any], *, creating: bool) -> dict[str, Any]:
    lab_code = _text(form.get("lab_code"))
    if lab_code not in _laboratories_by_code():
        raise ValueError("Choose a valid laboratory for the non-SAP sample.")
    status = _text(form.get("current_status"))
    if status not in NON_SAP_STATUS_LABELS:
        raise ValueError("Choose a valid non-SAP current status.")
    values: dict[str, Any] = {
        "lab_code": lab_code,
        "current_status": status,
        "expected_completion_date": _date(form.get("expected_completion_date")),
        "action_owner": _text(form.get("action_owner")) or None,
        "delay_reason": _text(form.get("delay_reason")) or None,
        "update_note": _text(form.get("update_note")) or None,
        "reported_outcome": _non_sap_outcome(status, form.get("reported_outcome")),
    }
    if creating:
        sample_reference = _text(form.get("sample_reference"))
        chemical_name = _text(form.get("chemical_name"))
        if not sample_reference or not chemical_name:
            raise ValueError("Enter both the local sample reference and material / sample name.")
        values.update({
            "sample_reference": sample_reference,
            "chemical_name": chemical_name,
            "material_code": _text(form.get("material_code")) or None,
            "sample_receipt_date": _date(form.get("sample_receipt_date")),
        })
    return values


def _append_non_sap_update(sample: QCNonSAPSample, updated_by: int | None) -> QCNonSAPSampleUpdate:
    update = QCNonSAPSampleUpdate(
        sample_id=sample.id,
        current_status=sample.current_status,
        expected_completion_date=sample.expected_completion_date,
        action_owner=sample.action_owner,
        delay_reason=sample.delay_reason,
        update_note=sample.update_note,
        reported_outcome=sample.reported_outcome,
        updated_by=updated_by,
    )
    db.session.add(update)
    return update


def create_non_sap_sample(form: dict[str, Any], created_by: int | None) -> QCNonSAPSample:
    """Add a controlled exception without creating or changing an SAP record."""
    values = _non_sap_values(form, creating=True)
    exists = QCNonSAPSample.query.filter_by(
        lab_code=values["lab_code"], sample_reference=values["sample_reference"],
    ).first()
    if exists:
        raise ValueError("That local sample reference is already in the non-SAP register for this laboratory.")
    sample = QCNonSAPSample(**values, created_by=created_by, updated_by=created_by)
    db.session.add(sample)
    db.session.flush()
    _append_non_sap_update(sample, created_by)
    return sample


def update_non_sap_sample(
    sample_id: int, form: dict[str, Any], updated_by: int | None, *, lab_code: str | None = None,
) -> QCNonSAPSample:
    sample = db.session.get(QCNonSAPSample, sample_id)
    if sample is None:
        raise ValueError("The requested non-SAP sample is no longer available.")
    if lab_code is not None and sample.lab_code != lab_code:
        raise ValueError("The requested non-SAP sample does not belong to this laboratory.")
    values = _non_sap_values({**form, "lab_code": sample.lab_code}, creating=False)
    for field, value in values.items():
        if field != "lab_code":
            setattr(sample, field, value)
    sample.updated_by = updated_by
    _append_non_sap_update(sample, updated_by)
    return sample


# ── Whole-portfolio analytics ────────────────────────────────────
# Below this many recorded usage decisions a percentage is noise rather than a
# signal: one rejected sample out of one is not a 100% failure rate worth
# ranking a chemical by.
MIN_DECISIONS_FOR_RATE = 5

# How many rows each ranked table carries. The totals beside them report the
# full population, so a truncated table never reads as the whole picture.
ANALYTICS_TABLE_LIMIT = 25


def _blank_load_counters() -> dict[str, Any]:
    return {
        "total": 0, "completed": 0, "open": 0, "accepted": 0, "rejected": 0,
        "turnaround": Counter(), "within_stt": 0, "stt_measured": 0,
        "laboratories": set(), "materials": set(),
    }


def _value_at_rank(histogram: Counter, values: list[int], index: int) -> int:
    seen = 0
    for value in values:
        seen += histogram[value]
        if index < seen:
            return value
    return values[-1]


def _median_from_histogram(histogram: Counter) -> float | None:
    """Exact median over a value/count histogram rather than a list of rows.

    The analytics aggregate in SQL, so turnaround arrives as counts per day
    rather than one row per sample.  This returns what statistics.median would
    have returned for the expanded list, including the two-value average on an
    even population.
    """
    total = sum(histogram.values())
    if not total:
        return None
    values = sorted(histogram)
    if total % 2:
        return float(_value_at_rank(histogram, values, total // 2))
    lower = _value_at_rank(histogram, values, total // 2 - 1)
    upper = _value_at_rank(histogram, values, total // 2)
    return (lower + upper) / 2


def _finalise_load(counters: dict[str, Any], **identity: Any) -> dict[str, Any]:
    """Turn raw counters into the rates the analytics tables report.

    A rate is ``None`` rather than zero where nothing was measured, so the
    template can say "not yet measured" instead of showing a confident 0%.
    """
    decided = counters["accepted"] + counters["rejected"]
    turnaround = counters["turnaround"]
    stt_measured = counters["stt_measured"]
    median_days = _median_from_histogram(turnaround)
    return {
        **identity,
        "total": counters["total"],
        "completed": counters["completed"],
        "open": counters["open"],
        "accepted": counters["accepted"],
        "rejected": counters["rejected"],
        "decided": decided,
        # Open samples are not passes, so they stay out of the denominator.
        "rejection_rate": round(counters["rejected"] / decided * 100, 1) if decided else None,
        "median_turnaround_days": round(median_days, 1) if median_days is not None else None,
        "turnaround_measured": sum(turnaround.values()),
        "stt_measured": stt_measured,
        "within_stt": counters["within_stt"],
        "stt_on_time_rate": (
            round(counters["within_stt"] / stt_measured * 100, 1) if stt_measured else None
        ),
        "laboratory_count": len(counters["laboratories"]),
        "material_count": len(counters["materials"]),
    }


def _add_to_load(
    counters: dict[str, Any], group: dict[str, Any],
    *, laboratory_name: str, material_key: str,
) -> None:
    """Fold one aggregated group — a count of identical samples — into a total."""
    count = group["count"]
    counters["total"] += count
    counters["completed"] += count if group["is_completed"] else 0
    counters["open"] += 0 if group["is_completed"] else count
    if group["outcome"] == "accepted":
        counters["accepted"] += count
    elif group["outcome"] == "rejected":
        counters["rejected"] += count
    if group["turnaround_days"] is not None:
        counters["turnaround"][group["turnaround_days"]] += count
        if group["within_stt"] is not None:
            counters["stt_measured"] += count
            counters["within_stt"] += count if group["within_stt"] else 0
    counters["laboratories"].add(laboratory_name)
    counters["materials"].add(material_key)


def _merge_load(target: dict[str, Any], cell: dict[str, Any]) -> None:
    """Roll one laboratory-and-material cell up into a wider total.

    Every wider figure — a laboratory, a sub-group, the portfolio — is a sum of
    these cells, so each aggregated row is folded once and the roll-up then
    runs over cells rather than over rows again.
    """
    for key in ("total", "completed", "open", "accepted", "rejected", "within_stt", "stt_measured"):
        target[key] += cell[key]
    target["turnaround"].update(cell["turnaround"])
    target["laboratories"] |= cell["laboratories"]
    target["materials"] |= cell["materials"]


def _portfolio_load_groups(lab_codes: list[str]) -> list[Any]:
    """Collapse the record table into one row per distinct sample shape.

    The database does the counting.  Grouping on the materialised outcome and
    turnaround columns means the whole recorded load reduces to at most a few
    thousand rows — laboratory by material by status by outcome by day — which
    is what makes this affordable to read on every page load.
    """
    return db.session.query(
        QCSAPRecord.lab_code,
        QCSAPRecord.material_code,
        QCSAPRecord.material_description,
        QCSAPRecord.official_status,
        QCSAPRecord.usage_outcome,
        QCSAPRecord.turnaround_days,
        func.count(QCSAPRecord.id).label("sample_count"),
    ).filter(
        QCSAPRecord.lab_code.in_(lab_codes)
    ).group_by(
        QCSAPRecord.lab_code,
        QCSAPRecord.material_code,
        QCSAPRecord.material_description,
        QCSAPRecord.official_status,
        QCSAPRecord.usage_outcome,
        QCSAPRecord.turnaround_days,
    ).all()


def _empty_portfolio_analytics() -> dict[str, Any]:
    """The same shape with nothing in it, so the template needs no special case."""
    return {
        "totals": _finalise_load(_blank_load_counters()),
        "laboratories": [], "materials_by_load": [], "materials_by_failure": [],
        "material_total": 0, "ranked_failure_total": 0, "subgroups": [],
        "min_decisions": MIN_DECISIONS_FOR_RATE, "table_limit": ANALYTICS_TABLE_LIMIT,
        "has_data": False,
    }


def sap_portfolio_analytics(lab_codes: set[str] | None = None) -> dict[str, Any]:
    """Analyse the whole recorded SAP sample load, not one day's snapshot.

    Every ``QCSAPRecord`` ever imported for a laboratory is counted here.  That
    is what makes "which chemical fails most" answerable at all: the daily
    snapshot holds only what SAP is currently reporting, so a completed sample
    leaves it and its outcome would never be counted.

    The counting is done by the database.  One grouped query collapses the
    record table into distinct sample shapes; only the Corporate Specification
    mapping, which is assembled in Python from the register, is applied here.

    Two cautions are built into the shape of this data rather than left to the
    reader.  A rejection rate is taken only over samples SAP has actually
    decided.  And a rejection rate describes the material and its supplier, not
    the laboratory that tested it — laboratory performance is measured here by
    turnaround against the Corporate Specification testing time, which is the
    part a laboratory controls.
    """
    laboratories = [
        laboratory for laboratory in sap_reporting_laboratories()
        if lab_codes is None or laboratory["code"] in lab_codes
    ]
    laboratory_names = {laboratory["code"]: laboratory for laboratory in laboratories}
    if not laboratory_names:
        return _empty_portfolio_analytics()

    specifications_by_material_code = _corporate_specifications_by_material_code()
    portfolio = _blank_load_counters()
    by_laboratory: dict[str, dict[str, Any]] = {}
    by_material: dict[str, dict[str, Any]] = {}
    material_identity: dict[str, dict[str, Any]] = {}
    by_subgroup: dict[str, dict[str, Any]] = {}
    subgroup_labels: dict[str, str] = {}

    cells: dict[tuple[str, str], dict[str, Any]] = {}
    material_subgroups: dict[str, str] = {}
    specification_cache: dict[Any, dict[str, Any]] = {}
    for row in _portfolio_load_groups(list(laboratory_names)):
        laboratory = laboratory_names[row.lab_code]
        specification = specification_cache.get(row.material_code)
        if specification is None:
            specification = _corporate_specification_fields(
                row.material_code, specifications_by_material_code,
            )
            specification_cache[row.material_code] = specification
        stt_days = specification["stt_days"]
        material_key = _identifier(row.material_code) or (
            _text(row.material_description) or "Not recorded"
        )
        material_identity.setdefault(material_key, {
            "material_code": row.material_code or "No material code",
            "material_description": (
                row.material_description or row.material_code or "Not recorded"
            ),
            "subgroup_label": specification["subgroup_label"],
            "specification_no": specification["specification_no"],
            "specification_match": specification["specification_match"],
        })
        material_subgroups[material_key] = specification["subgroup_key"]
        subgroup_labels[specification["subgroup_key"]] = specification["subgroup_label"]
        _add_to_load(
            cells.setdefault((row.lab_code, material_key), _blank_load_counters()),
            {
                "count": row.sample_count,
                "is_completed": row.official_status == "completed",
                "outcome": row.usage_outcome,
                "turnaround_days": row.turnaround_days,
                "within_stt": (
                    row.turnaround_days <= stt_days
                    if row.turnaround_days is not None and stt_days is not None else None
                ),
            },
            laboratory_name=laboratory["name"], material_key=material_key,
        )

    for (lab_code, material_key), cell in cells.items():
        _merge_load(portfolio, cell)
        _merge_load(by_laboratory.setdefault(lab_code, _blank_load_counters()), cell)
        _merge_load(by_material.setdefault(material_key, _blank_load_counters()), cell)
        _merge_load(
            by_subgroup.setdefault(material_subgroups[material_key], _blank_load_counters()), cell,
        )

    laboratory_rows = [
        _finalise_load(counters, laboratory=laboratory_names[code])
        for code, counters in by_laboratory.items()
    ]
    materials = [
        _finalise_load(counters, **material_identity[key])
        for key, counters in by_material.items()
    ]
    subgroups = [
        _finalise_load(counters, key=key, label=subgroup_labels[key])
        for key, counters in by_subgroup.items()
    ]
    by_load = sorted(
        materials,
        key=lambda item: (-item["total"], item["material_description"].casefold()),
    )
    # Ranked on rate, but only where enough decisions exist to mean anything.
    by_failure = sorted(
        (item for item in materials if item["decided"] >= MIN_DECISIONS_FOR_RATE),
        key=lambda item: (-(item["rejection_rate"] or 0), -item["decided"]),
    )
    return {
        "totals": _finalise_load(portfolio),
        "laboratories": sorted(
            laboratory_rows,
            key=lambda item: (
                # Never measured sorts last rather than ranking as perfect.
                item["stt_on_time_rate"] is None,
                -(item["stt_on_time_rate"] or 0),
                item["median_turnaround_days"] if item["median_turnaround_days"] is not None else 10**6,
            ),
        ),
        "materials_by_load": by_load[:ANALYTICS_TABLE_LIMIT],
        "materials_by_failure": by_failure[:ANALYTICS_TABLE_LIMIT],
        "material_total": len(materials),
        "ranked_failure_total": len(by_failure),
        "subgroups": sorted(subgroups, key=lambda item: (-item["total"], item["label"].casefold())),
        "min_decisions": MIN_DECISIONS_FOR_RATE,
        "table_limit": ANALYTICS_TABLE_LIMIT,
        "has_data": portfolio["total"] > 0,
    }
