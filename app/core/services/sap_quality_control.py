"""Corporate Chemistry's SAP-first quality-monitoring control tower.

SAP is the system of record for RGL and IDWE work.  Corporate Chemistry can
record a returned action update, expected completion, or delay reason, but
that activity never edits the official SAP status.  A separate, explicitly
labelled register holds the few samples that are not represented in SAP.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
import json
import re
from typing import Any

import pandas as pd

from app.extensions import db
from app.models.quality_control.qc_sap_monitoring import (
    QCNonSAPSample,
    QCNonSAPSampleUpdate,
    QCSAPLabUpdate,
    QCSAPRecord,
    QCSAPUploadBatch,
)
from app.models.quality_control.qc_testing_standard import QCTestingStandard


PANVEL_LAB_CODE = "rgl_panvel"
PANVEL_PLANT_CODE = "10R2"
SAP_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
SAP_REPORTING_LAB_CODES = (
    "rgl_panvel", "rgl_vadodara", "rgl_jorhat", "rgl_rajahmundry", "rgl_chennai",
    "idwe_cementing", "idwe_df_cf",
)

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


@dataclass(frozen=True)
class SAPExportPayload:
    rows: list[dict[str, Any]]
    as_of_date: date | None


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


def _integer(value: Any) -> int | None:
    text = _text(value)
    match = re.search(r"-?\d+", text)
    return int(match.group()) if match else None


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
    source: bytes, filename: str, *, expected_plant: str | None = PANVEL_PLANT_CODE,
) -> SAPExportPayload:
    payload = _read_sap_export(
        source, filename, kind="inspection-lot", aliases=_INSPECTION_COLUMNS,
        required={"inspection_lot_number", "material_code", "plant_code"},
    )
    rows = []
    for raw in payload.rows:
        lot = _identifier(raw.get("inspection_lot_number"), zero_is_blank=True)
        if not lot:
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
        raise ValueError("The SAP inspection-lot workbook contains no usable inspection-lot numbers.")
    _validate_sap_plants(rows, "inspection-lot", expected_plant)
    return SAPExportPayload(rows=rows, as_of_date=payload.as_of_date)


def parse_sap_notification_workbook(
    source: bytes, filename: str, *, expected_plant: str | None = PANVEL_PLANT_CODE,
) -> SAPExportPayload:
    payload = _read_sap_export(
        source, filename, kind="notification", aliases=_NOTIFICATION_COLUMNS,
        required={"notification_no", "material_code", "plant_code"},
    )
    rows = []
    for raw in payload.rows:
        notification_no = _identifier(raw.get("notification_no"), zero_is_blank=True)
        if not notification_no:
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
            "notification_start_date": _date(raw.get("notification_start_date")),
            "planned_end_date": _date(raw.get("planned_end_date")),
            "completion_date": _date(raw.get("completion_date")),
            "sap_delay_days": _integer(raw.get("sap_delay_days")),
        })
    if not rows:
        raise ValueError("The SAP notification workbook contains no usable notification numbers.")
    _validate_sap_plants(rows, "notification", expected_plant)
    return SAPExportPayload(rows=rows, as_of_date=payload.as_of_date)


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
            f"This RGL Panvel view accepts plant {expected_plant} only; the workbook reports {plant}."
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


def merge_sap_exports(
    inspections: list[dict[str, Any]], notifications: list[dict[str, Any]], *, lab_code: str = PANVEL_LAB_CODE,
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

    unmatched_inspection = sum(1 for row in rows if row["source_completeness"] == "inspection_lot_only")
    unmatched_notification = sum(1 for row in rows if row["source_completeness"] == "notification_only")
    return rows, {
        "unmatched_inspection_count": unmatched_inspection,
        "unmatched_notification_count": unmatched_notification,
    }


def _summary(rows: list[dict[str, Any]], as_of_date: date) -> dict[str, Any]:
    open_rows = [row for row in rows if row["official_status"] == "open"]
    planned_overdue = [
        row for row in open_rows
        if row.get("planned_end_date") is not None and row["planned_end_date"] < as_of_date
    ]
    return {
        "as_of_date": as_of_date.isoformat(),
        "total_records": len(rows),
        "completed_records": len(rows) - len(open_rows),
        "open_records": len(open_rows),
        "planned_overdue_records": len(planned_overdue),
        "work_centers": dict(Counter(row.get("work_center") or "Not assigned in SAP" for row in open_rows)),
        "usage_decisions": dict(Counter(row.get("usage_decision_code") or "Not recorded" for row in rows)),
        "accepted_records": sum(1 for row in rows if usage_decision_outcome(row.get("usage_decision_code")) == "accepted"),
        "rejected_records": sum(1 for row in rows if usage_decision_outcome(row.get("usage_decision_code")) == "rejected"),
    }


def _laboratories_by_code() -> dict[str, dict[str, Any]]:
    # Imported lazily to avoid a circular dependency during Flask model setup.
    from app.core.services.quality_control import LABORATORIES
    return LABORATORIES


def sap_reporting_laboratories() -> list[dict[str, Any]]:
    laboratories = _laboratories_by_code()
    return [laboratories[code] for code in SAP_REPORTING_LAB_CODES if code in laboratories]


def get_sap_reporting_laboratory(lab_code: str) -> dict[str, Any]:
    laboratory = _laboratories_by_code().get(lab_code)
    if lab_code not in SAP_REPORTING_LAB_CODES or laboratory is None:
        raise ValueError("Choose an RGL or IDWE laboratory configured for SAP daily monitoring.")
    return laboratory


def usage_decision_outcome(value: Any) -> str | None:
    """Return the business outcome encoded by SAP usage decision A or R."""
    text = _text(value).upper()
    match = re.search(r"(?:^|\\b)(?:UD\\s*)?([AR])(?:$|\\b)", text)
    return USAGE_DECISION_OUTCOMES.get(match.group(1)) if match else None


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
    expected_plant = PANVEL_PLANT_CODE if lab_code == PANVEL_LAB_CODE else None
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
    as_of_date = notifications.as_of_date or inspections.as_of_date
    if as_of_date is None:
        raise ValueError("Could not determine the SAP as-of date. Include it in the export or filename as YYYYMMDD.")
    if inspections.as_of_date and notifications.as_of_date and inspections.as_of_date != notifications.as_of_date:
        raise ValueError("The two SAP exports have different as-of dates. Upload reports from the same daily run.")
    rows, reconciliation = merge_sap_exports(inspections.rows, notifications.rows, lab_code=lab_code)
    if not rows:
        raise ValueError("The SAP exports did not yield any monitoring records.")

    summary = _summary(rows, as_of_date)
    batch = QCSAPUploadBatch(
        lab_code=lab_code,
        plant_code=inspection_plant,
        as_of_date=as_of_date,
        inspection_filename=inspection_filename,
        inspection_content_type=SAP_XLSX_MIME,
        inspection_file_size=len(inspection_source),
        inspection_source_data=inspection_source,
        notification_filename=notification_filename,
        notification_content_type=SAP_XLSX_MIME,
        notification_file_size=len(notification_source),
        notification_source_data=notification_source,
        inspection_lot_count=len(inspections.rows),
        notification_count=len(notifications.rows),
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
    for row in rows:
        record = existing.get(row["source_key"])
        if record is None:
            record = QCSAPRecord(
                source_key=row["source_key"], lab_code=lab_code, first_seen_batch_id=batch.id,
            )
            db.session.add(record)
        for field, value in row.items():
            setattr(record, field, value)
        record.last_seen_batch_id = batch.id
    return batch


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


def _reconciliation_state(record: QCSAPRecord, update: QCSAPLabUpdate | None) -> tuple[str, str]:
    if record.official_status == "completed":
        return "sap_confirmed", "SAP complete"
    if update is None:
        return "awaiting_lab", "Lab update requested"
    if update.activity_status == "action_completed":
        return "awaiting_sap_confirmation", "Lab says complete — SAP confirmation pending"
    return "lab_updated", "Lab update received"


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
        }

    records = QCSAPRecord.query.filter_by(last_seen_batch_id=batch.id).order_by(
        QCSAPRecord.official_status.asc(), QCSAPRecord.planned_end_date.asc(), QCSAPRecord.id.asc(),
    ).all()
    updates = _latest_lab_updates(records)
    standards = {
        _material_key(item.material_code): item.standard_days
        for item in QCTestingStandard.query.filter(QCTestingStandard.material_code.isnot(None)).all()
        if _material_key(item.material_code)
    }
    entries = []
    states = Counter()
    for record in records:
        update = updates.get(record.id)
        reconciliation_key, reconciliation_label = _reconciliation_state(record, update)
        is_planned_overdue = bool(
            record.official_status == "open" and record.planned_end_date and record.planned_end_date < batch.as_of_date
        )
        if record.official_status == "open":
            states[reconciliation_key] += 1
        entries.append({
            "record": record,
            "lab_update": update,
            "reconciliation_key": reconciliation_key,
            "reconciliation_label": reconciliation_label,
            "planned_overdue": is_planned_overdue,
            "standard_days": standards.get(_material_key(record.material_code)),
        })

    open_entries = [entry for entry in entries if entry["record"].official_status == "open"]
    work_centers: dict[str, dict[str, Any]] = {}
    for entry in open_entries:
        name = entry["record"].work_center or "Not assigned in SAP"
        item = work_centers.setdefault(name, {"name": name, "open": 0, "planned_overdue": 0, "awaiting_lab": 0})
        item["open"] += 1
        item["planned_overdue"] += int(entry["planned_overdue"])
        item["awaiting_lab"] += int(entry["reconciliation_key"] == "awaiting_lab")

    kpis = {
        "total": len(records),
        "completed": sum(1 for record in records if record.official_status == "completed"),
        "open": len(open_entries),
        "accepted": sum(1 for record in records if usage_decision_outcome(record.usage_decision_code) == "accepted"),
        "rejected": sum(1 for record in records if usage_decision_outcome(record.usage_decision_code) == "rejected"),
        "planned_overdue": sum(1 for entry in open_entries if entry["planned_overdue"]),
        "awaiting_lab": states["awaiting_lab"],
        "awaiting_sap_confirmation": states["awaiting_sap_confirmation"],
        "material_standard_coverage": sum(1 for entry in entries if entry["standard_days"] is not None),
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
        "records": entries,
        "open_records": [entry for entry in open_entries if entry["planned_overdue"] or entry["reconciliation_key"] != "sap_confirmed"],
        "kpis": kpis,
        "work_centers": sorted(work_centers.values(), key=lambda item: (-item["planned_overdue"], -item["open"], item["name"])),
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


def sap_control_data() -> dict[str, Any]:
    """Corporate Chemistry's consolidated source and exception control view."""
    laboratories = _laboratories_by_code()
    cards: list[dict[str, Any]] = []
    for laboratory in sap_reporting_laboratories():
        lab_code = laboratory["code"]
        batch = latest_sap_batch(lab_code)
        sap_open = 0
        if batch:
            sap_open = QCSAPRecord.query.filter_by(
                lab_code=lab_code, last_seen_batch_id=batch.id, official_status="open",
            ).count()
        non_sap_pending = QCNonSAPSample.query.filter_by(lab_code=lab_code).filter(
            ~QCNonSAPSample.current_status.in_(NON_SAP_CLOSED_STATUSES)
        ).count()
        cards.append({
            "laboratory": laboratory,
            "batch": batch,
            "sap_open": sap_open,
            "non_sap_pending": non_sap_pending,
            "combined_pending": sap_open + non_sap_pending,
        })
    non_sap_entries = []
    for laboratory in sorted(laboratories.values(), key=lambda item: item["name"]):
        for entry in _non_sap_entries(laboratory["code"]):
            non_sap_entries.append({**entry, "laboratory": laboratory})
    fallback_laboratories = [
        laboratory for code, laboratory in laboratories.items()
        if code not in SAP_REPORTING_LAB_CODES
    ]
    return {
        "sap_laboratories": sap_reporting_laboratories(),
        "all_laboratories": sorted(laboratories.values(), key=lambda item: item["name"]),
        "control_cards": cards,
        "non_sap_entries": non_sap_entries,
        "non_sap_statuses": NON_SAP_STATUSES,
        "non_sap_status_labels": NON_SAP_STATUS_LABELS,
        "workbook_fallback_laboratories": fallback_laboratories,
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
    expected = _date(form.get("expected_completion_date"))
    update = QCSAPLabUpdate(
        record_id=record.id,
        activity_status=activity_status,
        expected_completion_date=expected,
        action_owner=_text(form.get("action_owner")) or None,
        delay_reason=_text(form.get("delay_reason")) or None,
        update_note=_text(form.get("update_note")) or None,
        updated_by=updated_by,
    )
    db.session.add(update)
    return update


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
    sample_id: int, form: dict[str, Any], updated_by: int | None,
) -> QCNonSAPSample:
    sample = db.session.get(QCNonSAPSample, sample_id)
    if sample is None:
        raise ValueError("The requested non-SAP sample is no longer available.")
    values = _non_sap_values({**form, "lab_code": sample.lab_code}, creating=False)
    for field, value in values.items():
        if field != "lab_code":
            setattr(sample, field, value)
    sample.updated_by = updated_by
    _append_non_sap_update(sample, updated_by)
    return sample
