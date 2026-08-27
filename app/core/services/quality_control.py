"""Import, persistence, and management metrics for QC local reporting workbooks."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from flask import url_for
from sqlalchemy import func, or_
from sqlalchemy.orm import undefer

from app.extensions import db
from app.models.quality_control.qc_sample import QCSample
from app.models.quality_control.qc_testing_standard import QCTestingStandard
from app.models.quality_control.qc_upload_batch import QCUploadBatch


XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_WEEK_PATTERN = re.compile(r"(\d{2}[./-]\d{2}[./-]\d{4}).*?(\d{2}[./-]\d{2}[./-]\d{4})")
IMPORT_STAGING_DIRECTORY = Path(tempfile.gettempdir()) / "ongc_qc_import_staging"
CLOSED_SAMPLE_REVIEW_STT_DAYS = 9
COMPLETED_STATUSES = {"pass", "fail", "report_issued"}

# Queue-health thresholds are deliberately centralised here so the management
# policy can be adjusted without changing either the calculation or template.
QUEUE_HEALTH_THRESHOLDS = (
    ("healthy", 10),
    ("watch", 25),
    ("stressed", 40),
)
PENDING_MANAGEMENT_STATUSES = (
    "within_standard",
    "approaching_standard",
    "delayed",
    "critical",
)
COMPLETED_MANAGEMENT_STATUSES = ("within_standard", "delayed", "critical")
LABORATORIES = {
    "rgl_panvel": {"code": "rgl_panvel", "name": "RGL Panvel", "location": "Panvel", "description": "Regional Geoscience Laboratory"},
    "rgl_vadodara": {"code": "rgl_vadodara", "name": "RGL Vadodara", "location": "Vadodara", "description": "Regional Geoscience Laboratory"},
    "rgl_jorhat": {"code": "rgl_jorhat", "name": "RGL Jorhat", "location": "Jorhat", "description": "Regional Geoscience Laboratory"},
    "rgl_rajahmundry": {"code": "rgl_rajahmundry", "name": "RGL Rajahmundry", "location": "Rajahmundry", "description": "Regional Geoscience Laboratory"},
    "rgl_chennai": {"code": "rgl_chennai", "name": "RGL Chennai", "location": "Chennai", "description": "Regional Geoscience Laboratory"},
    "idwe_cementing": {"code": "idwe_cementing", "name": "IDWE Cementing Laboratory", "location": "Dehradun", "description": "Institute of Drilling and Well Engineering · Cementing"},
    "idwe_df_cf": {"code": "idwe_df_cf", "name": "IDWE DF–CF Laboratory", "location": "Dehradun", "description": "Institute of Drilling and Well Engineering · DF–CF"},
    "wss_ahmedabad": {"code": "wss_ahmedabad", "name": "WSS Ahmedabad", "location": "Ahmedabad", "description": "Well Services Station", "is_additional_designated": True},
    "ahmedabad_asset_lab": {"code": "ahmedabad_asset_lab", "name": "Ahmedabad Asset Lab", "location": "Ahmedabad", "description": "Ahmedabad Asset testing laboratory", "is_additional_designated": True},
    "ankleshwar_asset_lab": {"code": "ankleshwar_asset_lab", "name": "Ankleshwar Asset Lab", "location": "Ankleshwar", "description": "Ankleshwar Asset testing laboratory", "is_additional_designated": True},
    "mehsana_asset_lab": {"code": "mehsana_asset_lab", "name": "Mehsana Asset Lab", "location": "Mehsana", "description": "Mehsana Asset testing laboratory", "is_additional_designated": True},
    "hazira_plant_lab": {"code": "hazira_plant_lab", "name": "Hazira Plant Lab", "location": "Hazira", "description": "Hazira Plant testing laboratory", "is_additional_designated": True},
    "uran_plant_lab": {"code": "uran_plant_lab", "name": "Uran Plant Lab", "location": "Uran", "description": "Uran Plant testing laboratory", "is_additional_designated": True},
}

IDWE_WORKSTREAM_CODES = ("idwe_cementing", "idwe_df_cf")

# The controlled designation list calls this simply "IDWE Dehradun". It is
# deliberately separate from the two weekly-reporting workstreams above: the
# source does not say which one is designated, so the CSC register must not
# infer one. It is offered only when authorising specifications.
CSC_DESIGNATION_ONLY_LABORATORIES = {
    "idwe_dehradun": {
        "code": "idwe_dehradun",
        "name": "IDWE Dehradun",
        "location": "Dehradun",
        "description": "Institute of Drilling and Well Engineering · document designation",
    },
}


@dataclass(frozen=True)
class QCWorkbookPayload:
    report_label: str
    week_start: date
    week_end: date
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class ManagementSampleAssessment:
    """A date-only comparison of one sample with its approved testing time."""

    classification: str
    duration_days: int
    standard_days: int
    excess_days: int


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "-"} else text


def _normalized_chemical(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).casefold())


def _date(value: Any) -> date | None:
    if value is None or pd.isna(value) or _text(value) == "":
        return None
    text = _text(value)
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})", text)
    if iso_match:
        try:
            return date.fromisoformat(iso_match.group(1))
        except ValueError:
            pass
    parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _turnaround(value: Any, received: date | None, issued: date | None) -> int | None:
    found = re.search(r"\d+", _text(value))
    if found:
        return int(found.group())
    if received and issued:
        return max((issued - received).days, 0)
    return None


def _status(value: Any, report_issue_date: date | None = None) -> str:
    normal = _text(value).lower().replace(" ", "_")
    if normal in {"pass", "passed", "approve", "approved"}:
        return "pass"
    if normal in {"fail", "failed", "rejected"}:
        return "fail"
    return "report_issued" if report_issue_date else "under_testing"


def _header_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).lower())


def _column_positions(header: list[Any]) -> dict[str, int]:
    aliases = {
        "serial_number": ["srno", "slno", "sino", "sno"],
        "chemical_name": ["nameofchemical", "nameofsample", "nameofthesample", "nameofthechemical", "chemicalname"],
        "specification_no": ["corporatespecificationno", "tentativespecification"],
        "supply_type": ["bulkpayment"], "po_number": ["pono"], "lot_stack": ["lotstack"],
        "notification_no": ["notificationno", "incaseoflabims"], "result_status": ["passfail", "remarks"],
        "sample_receipt_date": ["dateofsamplereceipt", "dateofreceipt"],
        "report_issue_date": ["dateofissueofreport", "dateofissueoftestreport"],
        "turnaround_days": ["timetakenfortesting", "timetakenforissueorreport"],
        "delay_reason": [
            "delayreasonifmorethan09daysremarks",
            "delayreasonifmorethan09days",
            "reasonfordelay",
        ],
    }
    found: dict[str, int] = {}
    normalized = [_header_key(item) for item in header]
    for field, options in aliases.items():
        for index, label in enumerate(normalized):
            if any(option in label for option in options):
                found[field] = index
                break
    return found


def _find_header_rows(raw: pd.DataFrame) -> list[tuple[int, dict[str, int]]]:
    """Locate each table in a weekly workbook (completed and in-progress sections)."""
    headers: list[tuple[int, dict[str, int]]] = []
    for row_index in range(len(raw)):
        positions = _column_positions(raw.iloc[row_index].tolist())
        if "serial_number" in positions and "chemical_name" in positions and "sample_receipt_date" in positions:
            headers.append((row_index, positions))
    if not headers:
        raise ValueError(
            "Could not find QC sample headings. Include Serial No., Chemical/Sample Name, and Date of Receipt columns."
        )
    return headers


def _rows_from_sheet(raw: pd.DataFrame) -> list[dict[str, Any]]:
    """Normalize every recognized sample table on one worksheet."""
    rows: list[dict[str, Any]] = []
    header_rows = _find_header_rows(raw)
    for header_index, (header_row, positions) in enumerate(header_rows):
        next_header = header_rows[header_index + 1][0] if header_index + 1 < len(header_rows) else len(raw)
        for _, raw_row in raw.iloc[header_row + 1 : next_header].iterrows():
            chemical = _text(raw_row.iloc[positions["chemical_name"]])
            serial = _text(raw_row.iloc[positions["serial_number"]])
            # Panvel uses document identifiers such as ``212/QC/2026`` in
            # the Sl. No. column. The leading number is the serial value;
            # accept that format as well as a conventional numeric serial.
            serial_match = re.match(r"\s*(\d+)(?:\.0)?(?:\D.*)?$", serial)
            if not chemical or serial_match is None:
                continue
            data = {field: _text(raw_row.iloc[index]) for field, index in positions.items()}
            receipt = _date(data.get("sample_receipt_date"))
            issued = _date(data.get("report_issue_date"))
            rows.append({
                "serial_number": int(serial_match.group(1)), "chemical_name": chemical,
                "specification_no": data.get("specification_no") or None,
                "supply_type": data.get("supply_type") or None, "po_number": data.get("po_number") or None,
                "lot_stack": data.get("lot_stack") or None, "notification_no": data.get("notification_no") or None,
                "result_status": _status(data.get("result_status"), issued), "sample_receipt_date": receipt,
                "report_issue_date": issued, "turnaround_days": _turnaround(data.get("turnaround_days"), receipt, issued),
                "delay_reason": data.get("delay_reason") or None,
            })
    return rows


def parse_weekly_qc_workbook(source: bytes, filename: str | None = None) -> QCWorkbookPayload:
    """Convert all recognized sample worksheets in the supplied weekly workbook."""
    worksheets = pd.read_excel(BytesIO(source), sheet_name=None, header=None)
    title = "Weekly QC Data"
    match = None
    for raw in worksheets.values():
        for value in raw.iloc[:3].to_numpy().flat:
            candidate = _text(value)
            candidate_match = _WEEK_PATTERN.search(candidate)
            if candidate_match:
                title, match = candidate, candidate_match
                break
        if match:
            break
    if not match and filename:
        filename_title = Path(filename).stem.replace("_", " ")
        filename_match = _WEEK_PATTERN.search(filename_title)
        if filename_match:
            title, match = filename_title, filename_match
    if not match:
        raise ValueError(
            "Include a reporting period in the workbook title or filename, for example 06.08.2026 to 12.08.2026."
        )
    week_start, week_end = _date(match.group(1)), _date(match.group(2))
    if not week_start or not week_end or week_end < week_start:
        raise ValueError("The reporting dates in the workbook title are invalid.")
    rows: list[dict[str, Any]] = []
    recognized_sheets = 0
    for raw in worksheets.values():
        try:
            rows.extend(_rows_from_sheet(raw))
            recognized_sheets += 1
        except ValueError:
            continue
    if not recognized_sheets:
        raise ValueError("Could not find QC sample headings in any worksheet of this workbook.")
    if not rows:
        raise ValueError("No sample rows were found in the workbook.")
    return QCWorkbookPayload(title, week_start, week_end, rows)


def get_laboratory(lab_code: str) -> dict[str, str]:
    laboratory = LABORATORIES.get(lab_code)
    if laboratory is None:
        raise ValueError("The selected QC laboratory is not configured.")
    return laboratory


def _source_key(row: dict[str, Any], lab_code: str) -> str:
    parts = [lab_code, *[row.get(field) or "" for field in ("chemical_name", "po_number", "lot_stack", "notification_no", "sample_receipt_date")]]
    return hashlib.sha256("|".join(str(part).strip().lower() for part in parts).encode()).hexdigest()


_NON_UNIQUE_NOTIFICATION_VALUES = {"", "na", "n/a", "notapplicable", "nil", "none", "-"}


def _notification_identity(value: Any) -> str | None:
    """Return a canonical notification identity only when it is meaningful."""
    normalized = re.sub(r"\s+", "", _text(value)).casefold()
    if normalized in _NON_UNIQUE_NOTIFICATION_VALUES:
        return None
    return normalized


def _notification_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Count usable notifications so repeated values fall back to safe matching."""
    counts: dict[str, int] = {}
    for row in rows:
        notification = _notification_identity(row.get("notification_no"))
        if notification:
            counts[notification] = counts.get(notification, 0) + 1
    return counts


def _find_existing_sample(
    row: dict[str, Any],
    lab_code: str,
    notification_counts: dict[str, int],
) -> QCSample | None:
    """Match a sample by a unique notification number, then safely fall back."""
    notification = _notification_identity(row.get("notification_no"))
    if notification and notification_counts.get(notification) == 1:
        matches = (
            QCSample.query.filter(
                QCSample.lab_code == lab_code,
                func.lower(func.replace(func.trim(QCSample.notification_no), " ", "")) == notification,
            )
            .order_by(QCSample.id.asc())
            .limit(2)
            .all()
        )
        if len(matches) == 1:
            return matches[0]

    # Legacy records and genuinely repeated/non-meaningful notifications use
    # the original composite key, preserving safe historical behaviour.
    return QCSample.query.filter_by(source_key=_source_key(row, lab_code)).first()


def build_summary(rows: list[QCSample] | list[dict[str, Any]], as_of: date | None = None) -> dict[str, Any]:
    as_of = as_of or date.today()
    def value(row, name): return row.get(name) if isinstance(row, dict) else getattr(row, name)
    total = len(rows)
    under_testing = [row for row in rows if value(row, "result_status") == "under_testing"]
    issued = [row for row in rows if value(row, "result_status") in {"pass", "fail", "report_issued"}]
    delayed_open = [row for row in under_testing if value(row, "sample_receipt_date") and (as_of - value(row, "sample_receipt_date")).days > 9]
    turnaround = [value(row, "turnaround_days") for row in issued if value(row, "turnaround_days") is not None]
    return {
        "total": total, "under_testing": len(under_testing), "issued": len(issued),
        "passed": sum(value(row, "result_status") == "pass" for row in rows),
        "failed": sum(value(row, "result_status") == "fail" for row in rows),
        "delayed_open": len(delayed_open),
        "average_turnaround": round(sum(turnaround) / len(turnaround), 1) if turnaround else None,
    }


def _sample_value(sample: QCSample | dict[str, Any], name: str) -> Any:
    return sample.get(name) if isinstance(sample, dict) else getattr(sample, name, None)


def _standard_days_for(sample: QCSample | dict[str, Any], standards: dict[str, Any]) -> int | None:
    """Resolve the existing Standard Testing Time for a sample, if usable.

    The standards table is the sole source of the value. Unlike older detailed
    views, the management card never substitutes the legacy 9-day review value
    when a material has no registered Standard Testing Time.
    """
    standard = standards.get(_normalized_chemical(_sample_value(sample, "chemical_name")))
    standard_days = getattr(standard, "standard_days", standard)
    return standard_days if isinstance(standard_days, int) and standard_days >= 0 else None


def assess_pending_sample(
    sample: QCSample | dict[str, Any], standard_days: int | None, as_of: date,
) -> ManagementSampleAssessment | None:
    """Classify an open sample using receipt date and Standard Testing Time."""
    receipt_date = _sample_value(sample, "sample_receipt_date")
    if standard_days is None or receipt_date is None or receipt_date > as_of:
        return None
    current_age = (as_of - receipt_date).days
    excess_age = current_age - standard_days
    if current_age < standard_days - 1:
        classification = "within_standard"
    elif current_age <= standard_days:
        # The bands are intentionally disjoint: exactly at Standard Testing
        # Time is approaching, and one day beyond is delayed.
        classification = "approaching_standard"
    elif excess_age <= 3:
        classification = "delayed"
    else:
        classification = "critical"
    return ManagementSampleAssessment(
        classification=classification,
        duration_days=current_age,
        standard_days=standard_days,
        excess_days=excess_age,
    )


def assess_completed_sample(
    sample: QCSample | dict[str, Any], standard_days: int | None,
) -> ManagementSampleAssessment | None:
    """Classify a closed sample from the two available business dates only."""
    receipt_date = _sample_value(sample, "sample_receipt_date")
    report_issue_date = _sample_value(sample, "report_issue_date")
    if standard_days is None or receipt_date is None or report_issue_date is None or report_issue_date < receipt_date:
        return None
    actual_tat = (report_issue_date - receipt_date).days
    excess_tat = actual_tat - standard_days
    if actual_tat <= standard_days:
        classification = "within_standard"
    elif excess_tat <= 3:
        classification = "delayed"
    else:
        classification = "critical"
    return ManagementSampleAssessment(
        classification=classification,
        duration_days=actual_tat,
        standard_days=standard_days,
        excess_days=excess_tat,
    )


def _management_distribution(statuses: tuple[str, ...], assessments: list[ManagementSampleAssessment], total: int) -> list[dict[str, Any]]:
    labels = {
        "within_standard": "Within Standard",
        "approaching_standard": "Approaching Standard",
        "delayed": "Delayed (1–3 days)",
        "critical": "Critical (>3 days)",
    }
    return [
        {
            "key": status,
            "label": labels[status],
            "count": sum(assessment.classification == status for assessment in assessments),
            "percentage": round(sum(assessment.classification == status for assessment in assessments) / total * 100)
            if total
            else 0,
        }
        for status in statuses
    ]


def _queue_health(overdue_percentage: float) -> tuple[str, str]:
    for key, limit in QUEUE_HEALTH_THRESHOLDS:
        if overdue_percentage <= limit:
            return key, key.title()
    return "critical", "Critical"


def calculate_management_metrics(
    samples: list[QCSample] | list[dict[str, Any]],
    standards: dict[str, Any],
    period_start: date,
    period_end: date,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Calculate the laboratory management card from canonical sample dates.

    ``period_start`` and ``period_end`` select closed and received samples for
    the weekly performance metrics. Pending workload always represents the
    current canonical open queue as of ``as_of`` (today by default).
    """
    as_of = as_of or date.today()
    pending_samples = [sample for sample in samples if _sample_value(sample, "result_status") == "under_testing"]
    pending_assessments: list[ManagementSampleAssessment] = []
    pending_missing_standard = 0
    for sample in pending_samples:
        standard_days = _standard_days_for(sample, standards)
        if standard_days is None:
            pending_missing_standard += 1
        assessment = assess_pending_sample(sample, standard_days, as_of)
        if assessment is not None:
            pending_assessments.append(assessment)

    pending_total = len(pending_samples)
    pending_unresolved = pending_total - len(pending_assessments)
    pending_distribution = _management_distribution(
        PENDING_MANAGEMENT_STATUSES, pending_assessments, pending_total,
    )
    pending_counts = {item["key"]: item["count"] for item in pending_distribution}
    pending_overdue = pending_counts["delayed"] + pending_counts["critical"]
    if not pending_total:
        queue_health_key, queue_health_label, overdue_percentage = "healthy", "Healthy", 0.0
    elif pending_unresolved:
        queue_health_key, queue_health_label, overdue_percentage = "insufficient_data", "Insufficient Data", None
    else:
        overdue_percentage = round(pending_overdue / pending_total * 100, 1)
        queue_health_key, queue_health_label = _queue_health(overdue_percentage)

    completed_samples = [
        sample
        for sample in samples
        if _sample_value(sample, "result_status") in COMPLETED_STATUSES
        and (report_issue_date := _sample_value(sample, "report_issue_date")) is not None
        and period_start <= report_issue_date <= period_end
    ]
    completed_assessments: list[ManagementSampleAssessment] = []
    completed_missing_standard = 0
    for sample in completed_samples:
        standard_days = _standard_days_for(sample, standards)
        if standard_days is None:
            completed_missing_standard += 1
        assessment = assess_completed_sample(sample, standard_days)
        if assessment is not None:
            completed_assessments.append(assessment)

    completed_total = len(completed_samples)
    completed_unresolved = completed_total - len(completed_assessments)
    completed_distribution = _management_distribution(
        COMPLETED_MANAGEMENT_STATUSES, completed_assessments, completed_total,
    )
    completed_counts = {item["key"]: item["count"] for item in completed_distribution}
    received_total = sum(
        _sample_value(sample, "sample_receipt_date") is not None
        and period_start <= _sample_value(sample, "sample_receipt_date") <= period_end
        for sample in samples
    )

    return {
        "period_start": period_start,
        "period_end": period_end,
        "pending": {
            "total": pending_total,
            "distribution": pending_distribution,
            "unresolved": pending_unresolved,
            "missing_standard": pending_missing_standard,
            "average_delay": (
                round(sum(max(assessment.excess_days, 0) for assessment in pending_assessments) / pending_total, 1)
                if pending_total and not pending_unresolved
                else None
            ),
            "oldest_delay": (
                max(max(assessment.excess_days, 0) for assessment in pending_assessments)
                if pending_total and not pending_unresolved
                else None
            ),
        },
        "completed": {
            "total": completed_total,
            "distribution": completed_distribution,
            "unresolved": completed_unresolved,
            "missing_standard": completed_missing_standard,
            "within_standard": completed_counts["within_standard"],
            "standard_compliance": (
                round(completed_counts["within_standard"] / completed_total * 100, 1)
                if completed_total and not completed_unresolved
                else None
            ),
            "average_delay": (
                round(sum(max(assessment.excess_days, 0) for assessment in completed_assessments) / completed_total, 1)
                if completed_total and not completed_unresolved
                else None
            ),
        },
        "clearance": {
            "received": received_total,
            "completed": completed_total,
            "ratio": round(completed_total / received_total * 100, 1) if received_total else None,
        },
        "queue_health": {
            "key": queue_health_key,
            "label": queue_health_label,
            "overdue_count": pending_overdue,
            "overdue_percentage": overdue_percentage,
        },
    }


def sanity_check_weekly_qc_workbook(
    source: bytes, lab_code: str, filename: str | None = None,
) -> tuple[QCWorkbookPayload, dict[str, Any]]:
    """Validate a workbook before it is allowed to create or update QC sample records."""
    laboratory = get_laboratory(lab_code)
    payload = parse_weekly_qc_workbook(source, filename)
    errors: list[str] = []
    warnings: list[str] = []
    source_keys: set[str] = set()
    missing_receipt = 0
    missing_issue_date = 0

    for row in payload.rows:
        identity = _source_key(row, lab_code)
        if identity in source_keys:
            errors.append(f"Duplicate sample detected: {row['chemical_name']} (serial {row['serial_number']}).")
        source_keys.add(identity)
        receipt, issued = row["sample_receipt_date"], row["report_issue_date"]
        if receipt is None:
            missing_receipt += 1
        if issued and receipt and issued < receipt:
            errors.append(f"Report date precedes receipt date for {row['chemical_name']} (serial {row['serial_number']}).")
        if row["result_status"] in {"pass", "fail", "report_issued"} and issued is None:
            missing_issue_date += 1

    if missing_receipt:
        warnings.append(f"{missing_receipt} sample(s) have no receipt date; ageing and turnaround cannot be calculated for them.")
    if missing_issue_date:
        warnings.append(f"{missing_issue_date} issued sample(s) have no test-report issue date.")
    if not any(row["result_status"] == "under_testing" for row in payload.rows):
        warnings.append("No samples are marked under testing in this reporting period.")

    return payload, {
        "laboratory": laboratory,
        "summary": build_summary(payload.rows, payload.week_end),
        "errors": errors,
        "warnings": warnings,
        "ready": not errors,
    }


def stage_validated_workbook(source: bytes) -> str:
    """Hold a validated upload briefly while the user confirms the import."""
    IMPORT_STAGING_DIRECTORY.mkdir(parents=True, exist_ok=True)
    cutoff = time.time() - 3600
    for staged_file in IMPORT_STAGING_DIRECTORY.glob("*.xlsx"):
        if staged_file.stat().st_mtime < cutoff:
            staged_file.unlink(missing_ok=True)
    token = secrets.token_urlsafe(24)
    (IMPORT_STAGING_DIRECTORY / f"{token}.xlsx").write_bytes(source)
    return token


def load_staged_workbook(token: str) -> bytes:
    staged_file = IMPORT_STAGING_DIRECTORY / f"{token}.xlsx"
    if not staged_file.is_file() or staged_file.stat().st_mtime < time.time() - 3600:
        staged_file.unlink(missing_ok=True)
        raise ValueError("This import review has expired. Upload the workbook again to run a new sanity check.")
    return staged_file.read_bytes()


def discard_staged_workbook(token: str) -> None:
    (IMPORT_STAGING_DIRECTORY / f"{token}.xlsx").unlink(missing_ok=True)


def import_weekly_qc_workbook(source: bytes, filename: str, uploaded_by: int | None, lab_code: str) -> tuple[QCUploadBatch, str]:
    payload = parse_weekly_qc_workbook(source, filename)
    laboratory = get_laboratory(lab_code)
    batch = QCUploadBatch.query.filter_by(lab_code=lab_code, week_start=payload.week_start, week_end=payload.week_end).first()
    action = "updated" if batch else "created"
    if batch is None:
        batch = QCUploadBatch(lab_code=lab_code, lab_name=laboratory["name"], week_start=payload.week_start, week_end=payload.week_end)
        db.session.add(batch)
    batch.report_label, batch.source_filename, batch.source_content_type = payload.report_label, filename, XLSX_MIME
    batch.source_file_size, batch.source_data, batch.row_count, batch.uploaded_by = len(source), source, len(payload.rows), uploaded_by
    db.session.flush()
    created = updated = 0
    notification_counts = _notification_counts(payload.rows)
    for row in payload.rows:
        key = _source_key(row, lab_code)
        sample = _find_existing_sample(row, lab_code, notification_counts)
        if sample is None:
            sample = QCSample(source_key=key, lab_code=lab_code, first_seen_batch_id=batch.id)
            db.session.add(sample)
            created += 1
        else:
            updated += 1
        for field, value in row.items():
            setattr(sample, field, value)
        sample.last_seen_batch_id = batch.id
    batch.imported_count, batch.updated_count = created, updated
    batch.summary_json = json.dumps(build_summary(payload.rows, payload.week_end))
    return batch, action


def current_monitoring_day(reference_date: date | None = None) -> dict[str, Any]:
    """Return the Monday–Friday operating day for the daily QC control cycle.

    Saturdays and Sundays do not create an expected SAP upload.  On a weekend,
    the workspace carries Friday's monitoring day forward until the next
    working day.  Public-holiday rules can be added later when Corporate
    Chemistry provides the approved calendar.
    """
    calendar_date = reference_date or date.today()
    monitoring_date = calendar_date
    while monitoring_date.weekday() >= 5:
        monitoring_date -= timedelta(days=1)
    return {
        "date": monitoring_date,
        "label": monitoring_date.strftime("%A, %d %b %Y"),
        "is_carried_forward": monitoring_date != calendar_date,
        "calendar_date": calendar_date,
    }


def laboratory_landing_data() -> list[dict[str, Any]]:
    """Build the navigator from daily SAP snapshots, not historic workbook weeks."""
    from app.core.services.sap_quality_control import SAP_REPORTING_LAB_CODES
    from app.models.quality_control.qc_sap_monitoring import QCSAPUploadBatch

    laboratories = []
    for lab in LABORATORIES.values():
        if lab["code"] in IDWE_WORKSTREAM_CODES:
            continue
        local_batch = QCUploadBatch.query.filter_by(lab_code=lab["code"]).order_by(QCUploadBatch.week_end.desc()).first()
        is_sap_monitoring = lab["code"] in SAP_REPORTING_LAB_CODES
        sap_batch = (
            QCSAPUploadBatch.query.filter_by(lab_code=lab["code"]).order_by(
                QCSAPUploadBatch.as_of_date.desc(), QCSAPUploadBatch.id.desc(),
            ).first()
            if is_sap_monitoring else None
        )
        laboratories.append({
            **lab,
            "latest_batch": local_batch,
            "latest_sap_batch": sap_batch,
            "is_sap_monitoring": is_sap_monitoring,
            "monitoring_date": sap_batch.as_of_date if sap_batch else (local_batch.week_end if local_batch else None),
            "monitoring_source": "SAP" if is_sap_monitoring else "Local workbook",
        })

    idwe_sap_batch = QCSAPUploadBatch.query.filter_by(lab_code="idwe_dehradun").order_by(
        QCSAPUploadBatch.as_of_date.desc(), QCSAPUploadBatch.id.desc(),
    ).first()
    laboratories.append({
        "code": "idwe_dehradun",
        "name": "IDWE Dehradun",
        "location": "Dehradun",
        "description": "Institute of Drilling and Well Engineering",
        "latest_batch": None,
        "latest_sap_batch": idwe_sap_batch,
        "is_sap_monitoring": True,
        "monitoring_date": idwe_sap_batch.as_of_date if idwe_sap_batch else None,
        "monitoring_source": "SAP",
    })
    laboratories.sort(key=lambda lab: lab["location"].casefold())
    return laboratories


def _format_week_range(start: date, end: date) -> str:
    """Render a week as one readable range, dropping what the two dates share."""
    if start is None or end is None:
        return ""
    if start == end:
        return start.strftime("%d %b %Y")
    if (start.year, start.month) == (end.year, end.month):
        return f"{start.strftime('%d')}\u2013{end.strftime('%d %b %Y')}"
    if start.year == end.year:
        return f"{start.strftime('%d %b')}\u2013{end.strftime('%d %b %Y')}"
    return f"{start.strftime('%d %b %Y')}\u2013{end.strftime('%d %b %Y')}"


def laboratory_import_targets() -> list[dict[str, Any]]:
    """Laboratories still using the separate local-workbook fallback route."""
    return [lab for lab in laboratory_landing_data() if not lab.get("is_sap_monitoring")]


def laboratory_navigator_data(
    laboratories: list[dict[str, Any]], monitoring_day: date | None = None,
) -> list[dict[str, Any]]:
    """Lab navigator markers: one entry per laboratory.

    SAP laboratories are marked current only when their latest snapshot is for
    the active working day.  Local-workbook fallback laboratories retain their
    most recent reported status without being represented as a daily SAP feed.
    """
    active_day = monitoring_day or current_monitoring_day()["date"]
    entries = []
    for lab in laboratories:
        batch = lab.get("latest_batch")
        latest_sap_batch = lab.get("latest_sap_batch")
        monitoring_date = lab.get("monitoring_date")
        if lab.get("is_group"):
            choices = [{
                "label": "SAP Control Tower",
                "hint": "Upload the daily SAP snapshot and manage RGL / IDWE follow-up",
                "href": url_for("quality_control.data_import"),
                "combined": True,
            }] + [{
                "label": workstream["name"],
                "hint": "Local reporting dashboard"
                        + ("" if workstream["latest_batch"] else " · awaiting first import"),
                "href": url_for("quality_control.laboratory_dashboard", lab_code=workstream["code"]),
            } for workstream in lab["workstreams"]]
        else:
            # A single laboratory opens its dashboard on click, so it needs no
            # chooser; the brief and sample register are reached from there.
            choices = []
        entries.append({
            "code": lab["code"],
            "name": lab["name"],
            "location": lab["location"],
            "description": lab["description"],
            "is_group": bool(lab.get("is_group")),
            "is_additional_designated": bool(lab.get("is_additional_designated")),
            "workstream_count": len(lab.get("workstreams", [])),
            "is_sap_monitoring": bool(lab.get("is_sap_monitoring")),
            "is_reporting": (
                monitoring_date == active_day if lab.get("is_sap_monitoring")
                else batch is not None
            ),
            "monitoring_date": monitoring_date.strftime("%d %b %Y") if monitoring_date else None,
            "monitoring_source": lab.get("monitoring_source"),
            "sap_record_count": latest_sap_batch.record_count if latest_sap_batch else None,
            # Where a click lands. A grouped entry has no single dashboard of its
            # own, so it keeps the chooser its workstreams need.
            "dashboard_href": (
                None if lab.get("is_group")
                else url_for("quality_control.laboratory_dashboard", lab_code=lab["code"])
            ),
            "choices": choices,
        })
    return entries


def latest_dashboard_data(lab_code: str) -> dict[str, Any]:
    laboratory = get_laboratory(lab_code)
    batch = QCUploadBatch.query.filter_by(lab_code=lab_code).order_by(QCUploadBatch.week_end.desc()).first()
    if batch is None:
        return {"laboratory": laboratory, "batch": None, "summary": build_summary([]), "samples": [], "materials": [], "history": []}
    samples = QCSample.query.filter_by(last_seen_batch_id=batch.id).order_by(QCSample.sample_receipt_date.asc(), QCSample.chemical_name.asc()).all()
    summary = build_summary(samples, date.today())
    standards = {item.normalized_name: item for item in QCTestingStandard.query.all()}
    canonical_samples = QCSample.query.filter_by(lab_code=lab_code).order_by(QCSample.sample_receipt_date.asc(), QCSample.id.asc()).all()
    management_metrics = calculate_management_metrics(
        canonical_samples,
        standards,
        period_start=batch.week_start,
        period_end=batch.week_end,
    )

    def stt_performance(rows: list[QCSample]) -> dict[str, Any]:
        closed = [sample for sample in rows if sample.result_status in {"pass", "fail", "report_issued"}]
        assessed = []
        for sample in closed:
            if sample.turnaround_days is None:
                continue
            standard = standards.get(_normalized_chemical(sample.chemical_name))
            standard_days = standard.standard_days if standard and standard.standard_days is not None else CLOSED_SAMPLE_REVIEW_STT_DAYS
            assessed.append((sample, standard_days, standard is not None and standard.standard_days is not None))
        within = [item for item in assessed if item[0].turnaround_days <= item[1]]
        late = [item for item in assessed if item[0].turnaround_days > item[1]]
        times = [sample.turnaround_days for sample, _, _ in assessed]
        return {
            "closed": len(closed),
            "assessed": len(assessed),
            "comparable": len(assessed),
            "within_standard": len(within),
            "late": len(late),
            "material_standard_count": sum(1 for _, _, has_material_standard in assessed if has_material_standard),
            "fallback_stt_count": sum(1 for _, _, has_material_standard in assessed if not has_material_standard),
            "compliance_rate": round(len(within) / len(assessed) * 100, 1) if assessed else None,
            "average_turnaround": round(sum(times) / len(times), 1) if times else None,
        }

    closed_stt_exceptions = []
    for sample in samples:
        if sample.result_status not in {"pass", "fail", "report_issued"} or sample.turnaround_days is None:
            continue
        standard = standards.get(_normalized_chemical(sample.chemical_name))
        stt_days = standard.standard_days if standard and standard.standard_days is not None else CLOSED_SAMPLE_REVIEW_STT_DAYS
        if sample.turnaround_days > stt_days:
            closed_stt_exceptions.append({
                "sample": sample,
                "standard_days": stt_days,
                "stt_source": "Approved testing standard" if standard and standard.standard_days is not None else "9-day review STT (standard not defined)",
                "variance_days": sample.turnaround_days - stt_days,
            })
    closed_stt_exceptions.sort(key=lambda item: (-item["variance_days"], item["sample"].chemical_name.casefold()))

    month_start = batch.week_end.replace(day=1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    month_intake = QCSample.query.filter(
        QCSample.lab_code == lab_code,
        QCSample.sample_receipt_date >= month_start,
        QCSample.sample_receipt_date < next_month,
    ).count()
    month_closed_samples = QCSample.query.filter(
        QCSample.lab_code == lab_code,
        QCSample.report_issue_date >= month_start,
        QCSample.report_issue_date < next_month,
    ).all()
    month_stt = stt_performance(month_closed_samples)
    overdue_samples = sorted(
        [sample for sample in samples if sample.result_status == "under_testing" and sample.days_open is not None and sample.days_open > 9],
        key=lambda sample: sample.sample_receipt_date or date.max,
    )
    materials: dict[str, int] = {}
    for sample in samples:
        materials[sample.chemical_name] = materials.get(sample.chemical_name, 0) + 1
    history = QCUploadBatch.query.filter_by(lab_code=lab_code).order_by(QCUploadBatch.week_end.desc()).limit(12).all()
    return {
        "laboratory": laboratory,
        "batch": batch,
        "summary": summary,
        "samples": samples,
        "overdue_samples": overdue_samples,
        "closed_stt_exceptions": closed_stt_exceptions,
        "week_stt": stt_performance(samples),
        "month_stt": month_stt,
        "month_intake": month_intake,
        "month_label": batch.week_end.strftime("%B %Y"),
        "materials": sorted(materials.items(), key=lambda item: (-item[1], item[0]))[:8],
        "history": history,
        "management_metrics": management_metrics,
    }


def _laboratory_scope_groups(laboratory_reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Laboratories grouped by the establishment they belong to, for the deck chooser.

    The RGL network and the IDWE workstreams are separate establishments, so a
    reader can take a whole establishment in one tick.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    for review in laboratory_reviews:
        laboratory = review["laboratory"]
        establishment = str(laboratory.get("description") or "Laboratories").split("·")[0].strip()
        groups.setdefault(establishment, []).append({
            "code": laboratory["code"],
            "name": laboratory["name"],
            "location": laboratory.get("location") or "",
            "submitted": review["batch"] is not None,
            "samples": review["summary"].get("total", 0) if review["batch"] else 0,
        })
    return [
        {"establishment": establishment, "laboratories": laboratories,
         "submitted": sum(item["submitted"] for item in laboratories)}
        for establishment, laboratories in groups.items()
    ]


def portfolio_management_data(reporting_week_end: date | None = None, lab_codes: set[str] | None = None) -> dict[str, Any]:
    """Build a period-controlled management view without mixing reporting weeks.

    ``lab_codes`` narrows every figure to a chosen set of laboratories, which is
    how a single-laboratory deck is exported; ``None`` is the whole network.
    """
    available_week_ends = [
        item[0]
        for item in QCUploadBatch.query.with_entities(QCUploadBatch.week_end)
        .distinct()
        .order_by(QCUploadBatch.week_end.desc())
        .all()
        if item[0] is not None
    ]
    selected_week_end = reporting_week_end if reporting_week_end in available_week_ends else (available_week_ends[0] if available_week_ends else None)
    submission_counts = {
        week_end: QCUploadBatch.query.filter_by(week_end=week_end).count()
        for week_end in available_week_ends
    }
    laboratory_reviews: list[dict[str, Any]] = []
    portfolio_samples: list[QCSample] = []
    in_scope = [
        laboratory for laboratory in LABORATORIES.values()
        if lab_codes is None or laboratory["code"] in lab_codes
    ]
    for laboratory in in_scope:
        latest_available_batch = (
            QCUploadBatch.query.filter_by(lab_code=laboratory["code"])
            .order_by(QCUploadBatch.week_end.desc())
            .first()
        )
        batch = (
            QCUploadBatch.query.filter_by(lab_code=laboratory["code"], week_end=selected_week_end)
            .first()
            if selected_week_end
            else None
        )
        samples = (
            QCSample.query.filter_by(last_seen_batch_id=batch.id)
            .order_by(QCSample.sample_receipt_date.asc(), QCSample.chemical_name.asc())
            .all()
            if batch
            else []
        )
        portfolio_samples.extend(samples)
        previous_batch = (
            QCUploadBatch.query.filter(
                QCUploadBatch.lab_code == laboratory["code"],
                QCUploadBatch.week_end < selected_week_end,
            )
            .order_by(QCUploadBatch.week_end.desc())
            .first()
            if batch and selected_week_end
            else None
        )
        try:
            previous_summary = json.loads(previous_batch.summary_json or "{}") if previous_batch else None
        except (TypeError, json.JSONDecodeError):
            previous_summary = None
        laboratory_reviews.append({
            "laboratory": laboratory,
            "batch": batch,
            "summary": build_summary(samples),
            "samples": samples,
            "previous_batch": previous_batch,
            "previous_summary": previous_summary,
            "latest_available_batch": latest_available_batch,
            "missing_current_submission": batch is None,
        })
    overdue_samples = sorted(
        [sample for sample in portfolio_samples if sample.result_status == "under_testing" and sample.days_open is not None and sample.days_open > 9],
        key=lambda sample: sample.sample_receipt_date or date.max,
    )
    standards = {item.normalized_name: item for item in QCTestingStandard.query.all()}
    completed_testing = []
    for sample in portfolio_samples:
        if sample.result_status in {"pass", "fail", "report_issued"}:
            standard = standards.get(_normalized_chemical(sample.chemical_name))
            standard_days = standard.standard_days if standard else None
            completed_testing.append({"sample": sample, "standard_days": standard_days, "standard_found": standard is not None, "variance_days": sample.turnaround_days - standard_days if sample.turnaround_days is not None and standard_days is not None else None})
    completed_testing.sort(key=lambda item: (item["sample"].report_issue_date or date.min, item["sample"].chemical_name), reverse=True)
    weekly_chemicals: dict[str, dict[str, Any]] = {}
    for sample in portfolio_samples:
        key = _normalized_chemical(sample.chemical_name)
        metric = weekly_chemicals.setdefault(key, {"chemical_name": sample.chemical_name, "laboratories": set(), "total": 0, "passed": 0, "failed": 0, "under_testing": 0, "actual_times": []})
        metric["laboratories"].add(LABORATORIES.get(sample.lab_code, {"name": sample.lab_code})["name"])
        metric["total"] += 1
        if sample.result_status == "pass": metric["passed"] += 1
        elif sample.result_status == "fail": metric["failed"] += 1
        elif sample.result_status == "under_testing": metric["under_testing"] += 1
        if sample.result_status != "under_testing" and sample.turnaround_days is not None:
            metric["actual_times"].append(sample.turnaround_days)
    weekly_chemical_metrics = []
    for key, metric in weekly_chemicals.items():
        standard = standards.get(key)
        actual = round(sum(metric["actual_times"]) / len(metric["actual_times"]), 1) if metric["actual_times"] else None
        weekly_chemical_metrics.append({"chemical_name": metric["chemical_name"], "laboratories": sorted(metric["laboratories"]), "total": metric["total"], "passed": metric["passed"], "failed": metric["failed"], "under_testing": metric["under_testing"], "average_actual": actual, "standard_days": standard.standard_days if standard else None, "variance_days": round(actual - standard.standard_days, 1) if actual is not None and standard and standard.standard_days is not None else None})
    weekly_chemical_metrics.sort(key=lambda item: (-item["total"], item["chemical_name"].casefold()))
    completed_with_standard = [item for item in completed_testing if item["standard_days"] is not None]
    within_standard = sum(item["variance_days"] <= 0 for item in completed_with_standard if item["variance_days"] is not None)
    reporting_reviews = [review for review in laboratory_reviews if review["batch"]]
    missing_submissions = [review for review in laboratory_reviews if review["missing_current_submission"]]
    comparable_reviews = [review for review in reporting_reviews if review["previous_batch"] and review["previous_summary"]]
    reporting_period = None
    if reporting_reviews:
        reporting_period = {
            "start": min(review["batch"].week_start for review in reporting_reviews),
            "end": selected_week_end,
            "laboratories": len(reporting_reviews),
        }
    week_on_week = {"available": bool(comparable_reviews), "coverage": len(comparable_reviews), "laboratory_metrics": []}
    if comparable_reviews:
        keys = ("total", "under_testing", "delayed_open", "passed", "failed")
        current = {key: sum(review["summary"].get(key, 0) or 0 for review in comparable_reviews) for key in keys}
        previous = {key: sum(review["previous_summary"].get(key, 0) or 0 for review in comparable_reviews) for key in keys}
        current_tat = [review["summary"].get("average_turnaround") for review in comparable_reviews if review["summary"].get("average_turnaround") is not None]
        previous_tat = [review["previous_summary"].get("average_turnaround") for review in comparable_reviews if review["previous_summary"].get("average_turnaround") is not None]
        current["average_turnaround"] = round(sum(current_tat) / len(current_tat), 1) if current_tat else None
        previous["average_turnaround"] = round(sum(previous_tat) / len(previous_tat), 1) if previous_tat else None
        week_on_week.update({
            "current": current,
            "previous": previous,
            "current_period": {
                "start": min(review["batch"].week_start for review in comparable_reviews),
                "end": max(review["batch"].week_end for review in comparable_reviews),
            },
            "previous_period": {
                "start": min(review["previous_batch"].week_start for review in comparable_reviews),
                "end": max(review["previous_batch"].week_end for review in comparable_reviews),
            },
        })
        for review in comparable_reviews:
            current_summary, previous_summary = review["summary"], review["previous_summary"]
            week_on_week["laboratory_metrics"].append({
                "laboratory": review["laboratory"],
                "current_period": review["batch"],
                "previous_period": review["previous_batch"],
                "sample_change": (current_summary.get("total", 0) or 0) - (previous_summary.get("total", 0) or 0),
                "open_change": (current_summary.get("under_testing", 0) or 0) - (previous_summary.get("under_testing", 0) or 0),
                "pass_change": (current_summary.get("passed", 0) or 0) - (previous_summary.get("passed", 0) or 0),
                "fail_change": (current_summary.get("failed", 0) or 0) - (previous_summary.get("failed", 0) or 0),
                "tat_change": (
                    round(current_summary["average_turnaround"] - previous_summary["average_turnaround"], 1)
                    if current_summary.get("average_turnaround") is not None and previous_summary.get("average_turnaround") is not None
                    else None
                ),
            })
    return {
        "laboratory_reviews": laboratory_reviews,
        "summary": build_summary(portfolio_samples),
        "overdue_samples": overdue_samples,
        "reporting_labs": sum(review["batch"] is not None for review in laboratory_reviews),
        "missing_submissions": missing_submissions,
        "selected_week_end": selected_week_end,
        "reporting_week_options": [
            {"week_end": week_end, "label": week_end.strftime("Period ending %d %b %Y"), "submission_count": submission_counts[week_end]}
            for week_end in available_week_ends
        ],
        "laboratories_by_code": LABORATORIES,
        "scope_laboratories": _laboratory_scope_groups(laboratory_reviews),
        "scope_codes": [laboratory["code"] for laboratory in in_scope],
        "completed_testing": completed_testing,
        "weekly_chemical_metrics": weekly_chemical_metrics,
        "completed_with_standard": len(completed_with_standard),
        "within_standard": within_standard,
        "reporting_period": reporting_period,
        "week_on_week": week_on_week,
    }


def import_testing_standards_workbook(source: bytes, updated_by: int | None) -> tuple[int, int]:
    frame = pd.read_excel(BytesIO(source), sheet_name=0)
    columns = {_header_key(column): column for column in frame.columns}
    chemical = next((column for key, column in columns.items() if "chemicalname" in key), None)
    days = next((column for key, column in columns.items() if "averagedays" in key), None)
    if not chemical or not days:
        raise ValueError("The standards workbook must include Chemical Name and Average (Days) columns.")
    created = updated = 0
    for _, row in frame.iterrows():
        name = _text(row[chemical])
        if not name:
            continue
        key = _normalized_chemical(name)
        item = QCTestingStandard.query.filter_by(normalized_name=key).first()
        if item is None:
            item = QCTestingStandard(normalized_name=key); db.session.add(item); created += 1
        else: updated += 1
        value = pd.to_numeric(row[days], errors="coerce")
        item.chemical_name, item.standard_days, item.updated_by = name, (None if pd.isna(value) else int(value)), updated_by
        item.specification_no = _text(row[columns["specificationno"]]) if "specificationno" in columns else None
        item.material_code = _text(row[columns["materialcode"]]) if "materialcode" in columns else None
        item.remarks = _text(row[columns["remarks"]]) if "remarks" in columns else None
    return created, updated


def management_analytics_data() -> dict[str, Any]:
    """Provide cumulative performance, risk, and standards analytics for management."""
    samples = QCSample.query.order_by(QCSample.sample_receipt_date.asc(), QCSample.id.asc()).all()
    standards = {item.normalized_name: item for item in QCTestingStandard.query.all()}

    def standard_performance(rows: list[QCSample]) -> dict[str, Any]:
        completed = [
            sample
            for sample in rows
            if sample.result_status in {"pass", "fail", "report_issued"}
            and sample.turnaround_days is not None
        ]
        comparable = [
            sample
            for sample in completed
            if (standard := standards.get(_normalized_chemical(sample.chemical_name)))
            and standard.standard_days is not None
        ]
        variances = [
            sample.turnaround_days - standards[_normalized_chemical(sample.chemical_name)].standard_days
            for sample in comparable
        ]
        within_standard = sum(variance <= 0 for variance in variances)
        return {
            "completed": len(completed),
            "comparable": len(comparable),
            "within_standard": within_standard,
            "late": sum(variance > 0 for variance in variances),
            "compliance_rate": round(within_standard / len(comparable) * 100, 1) if comparable else None,
            "average_variance": round(sum(variances) / len(variances), 1) if variances else None,
        }

    lab_metrics: list[dict[str, Any]] = []
    for laboratory in LABORATORIES.values():
        lab_samples = [sample for sample in samples if sample.lab_code == laboratory["code"]]
        summary = build_summary(lab_samples)
        tested = summary["passed"] + summary["failed"]
        lab_metrics.append({
            "laboratory": laboratory,
            "summary": summary,
            "tested": tested,
            "pass_rate": round(summary["passed"] / tested * 100, 1) if tested else None,
            "standard_performance": standard_performance(lab_samples),
        })

    chemicals: dict[str, dict[str, Any]] = {}
    for sample in samples:
        key = re.sub(r"\s+", " ", sample.chemical_name.strip()).casefold()
        metric = chemicals.setdefault(key, {
            "chemical_name": sample.chemical_name.strip(), "laboratories": set(), "total": 0,
            "under_testing": 0, "aged_open": 0, "passed": 0, "failed": 0, "report_issued": 0,
            "turnaround": [], "late": 0, "within_standard": 0, "comparable": 0,
        })
        metric["laboratories"].add(LABORATORIES.get(sample.lab_code, {"name": sample.lab_code})["name"])
        metric["total"] += 1
        if sample.result_status == "under_testing":
            metric["under_testing"] += 1
            if sample.days_open is not None and sample.days_open > 9:
                metric["aged_open"] += 1
        elif sample.result_status == "pass":
            metric["passed"] += 1
        elif sample.result_status == "fail":
            metric["failed"] += 1
        else:
            metric["report_issued"] += 1
        if sample.result_status != "under_testing" and sample.turnaround_days is not None:
            metric["turnaround"].append(sample.turnaround_days)
            standard = standards.get(_normalized_chemical(sample.chemical_name))
            if standard and standard.standard_days is not None:
                metric["comparable"] += 1
                if sample.turnaround_days <= standard.standard_days:
                    metric["within_standard"] += 1
                else:
                    metric["late"] += 1

    chemical_metrics = []
    for metric in chemicals.values():
        tested = metric["passed"] + metric["failed"]
        chemical_metrics.append({
            "chemical_name": metric["chemical_name"],
            "laboratories": sorted(metric["laboratories"]),
            "total": metric["total"],
            "under_testing": metric["under_testing"],
            "passed": metric["passed"],
            "failed": metric["failed"],
            "report_issued": metric["report_issued"],
            "pass_rate": round(metric["passed"] / tested * 100, 1) if tested else None,
            "average_turnaround": round(sum(metric["turnaround"]) / len(metric["turnaround"]), 1) if metric["turnaround"] else None,
            "standard_days": (
                standards[_normalized_chemical(metric["chemical_name"])].standard_days
                if _normalized_chemical(metric["chemical_name"]) in standards
                else None
            ),
            "comparable": metric["comparable"],
            "within_standard": metric["within_standard"],
            "late": metric["late"],
            "compliance_rate": (
                round(metric["within_standard"] / metric["comparable"] * 100, 1)
                if metric["comparable"]
                else None
            ),
            "aged_open": metric["aged_open"],
        })
    chemical_metrics.sort(key=lambda item: (-item["total"], item["chemical_name"].casefold()))

    # Each band carries the view that opens the samples in it, so a count on the
    # dashboard is a way into the register rather than a number to go and re-find.
    age_buckets = [
        {"label": "0-3 days", "count": 0, "tone": "neutral", "view": "age_0_3"},
        {"label": "4-9 days", "count": 0, "tone": "warning", "view": "age_4_9"},
        {"label": "10-14 days", "count": 0, "tone": "risk", "view": "age_10_14"},
        {"label": "15+ days", "count": 0, "tone": "critical", "view": "age_15_plus"},
    ]
    for sample in samples:
        if sample.result_status != "under_testing" or sample.days_open is None:
            continue
        if sample.days_open <= 3:
            age_buckets[0]["count"] += 1
        elif sample.days_open <= 9:
            age_buckets[1]["count"] += 1
        elif sample.days_open <= 14:
            age_buckets[2]["count"] += 1
        else:
            age_buckets[3]["count"] += 1

    monthly: dict[str, dict[str, Any]] = {}

    def monthly_metric(period_date: date) -> dict[str, Any]:
        period = period_date.strftime("%Y-%m")
        return monthly.setdefault(period, {
            "period": period,
            "label": period_date.strftime("%b %Y"),
            "received": 0,
            "completed": 0,
            "failed": 0,
            "turnaround": [],
        })

    for sample in samples:
        if sample.sample_receipt_date:
            monthly_metric(sample.sample_receipt_date)["received"] += 1
        if sample.result_status in {"pass", "fail", "report_issued"} and sample.report_issue_date:
            metric = monthly_metric(sample.report_issue_date)
            metric["completed"] += 1
            if sample.result_status == "fail":
                metric["failed"] += 1
            if sample.turnaround_days is not None:
                metric["turnaround"].append(sample.turnaround_days)
    monthly_trend = [
        {
            **metric,
            "average_turnaround": round(sum(metric["turnaround"]) / len(metric["turnaround"]), 1)
            if metric["turnaround"]
            else None,
        }
        for _, metric in sorted(monthly.items())[-12:]
    ]

    risk_register = [
        {
            **metric,
            "risk_score": metric["failed"] * 3 + metric["late"] * 2 + metric["aged_open"] * 3 + metric["under_testing"],
        }
        for metric in chemical_metrics
    ]
    risk_register = [metric for metric in risk_register if metric["risk_score"] > 0]
    risk_register.sort(key=lambda item: (-item["risk_score"], item["chemical_name"].casefold()))

    summary = build_summary(samples)
    tested = summary["passed"] + summary["failed"]
    standards_summary = standard_performance(samples)

    # The head's own view: the few numbers a decision is taken on, each one the
    # count of a register that can be opened. "Comparable" is the number that
    # needs explaining, so its other half — completed tests with no standard to
    # compare against — is carried beside it rather than left implied.
    open_ages = [sample.days_open for sample in samples if sample.result_status == "under_testing" and sample.days_open is not None]
    completed_samples = [
        sample for sample in samples
        if sample.result_status in COMPLETED_STATUSES and sample.turnaround_days is not None
    ]
    head_dashboard = {
        "samples": summary["total"],
        "laboratories": len(LABORATORIES),
        "laboratories_reporting": len({sample.lab_code for sample in samples}),
        "chemicals": len(chemical_metrics),
        "under_testing": summary["under_testing"],
        "aged_open": summary["delayed_open"],
        "oldest_open": max(open_ages) if open_ages else None,
        "completed": len(completed_samples),
        "passed": summary["passed"],
        "failed": summary["failed"],
        "pass_rate": round(summary["passed"] / tested * 100, 1) if tested else None,
        "average_turnaround": summary["average_turnaround"],
        "comparable": standards_summary["comparable"],
        "no_standard": len(completed_samples) - standards_summary["comparable"],
        "standard_coverage": round(standards_summary["comparable"] / len(completed_samples) * 100, 1) if completed_samples else None,
        "within_standard": standards_summary["within_standard"],
        "late": standards_summary["late"],
        "compliance_rate": standards_summary["compliance_rate"],
        "average_variance": standards_summary["average_variance"],
        "chemicals_at_risk": len(risk_register),
        "top_risk": risk_register[0] if risk_register else None,
    }

    analysis_briefs: list[dict[str, str]] = []

    if not tested:
        quality_inference = "No pass/fail outcomes are available yet, so quality conformance cannot be assessed. Prioritize completing outcome capture in the imported laboratory reports."
        quality_tone = "neutral"
    elif summary["failed"] == 0:
        quality_inference = f"All {tested} recorded pass/fail outcomes are passes. Continue monitoring as coverage expands across laboratories and chemicals."
        quality_tone = "positive"
    else:
        quality_inference = (
            f"{summary['failed']} of {tested} recorded pass/fail outcomes failed "
            f"({round(summary['failed'] / tested * 100, 1)}%). Review the affected chemicals in the risk register for repeat or cross-laboratory patterns."
        )
        quality_tone = "risk"
    analysis_briefs.append({
        "title": "Quality outcome",
        "importance": "Pass/fail performance is the primary signal of material conformity and protects operational reliability.",
        "inference": quality_inference,
        "tone": quality_tone,
    })

    if not standards_summary["comparable"]:
        standards_inference = "No completed test is currently matched to a defined testing standard. Load or complete the standards register before judging laboratory timeliness."
        standards_tone = "neutral"
    elif standards_summary["late"] == 0:
        standards_inference = f"All {standards_summary['comparable']} comparable completed tests met their approved testing time."
        standards_tone = "positive"
    else:
        standards_inference = (
            f"{standards_summary['late']} of {standards_summary['comparable']} comparable tests exceeded their standard time; "
            f"the average variance is {standards_summary['average_variance']:+.1f} days."
        )
        standards_tone = "risk"
    analysis_briefs.append({
        "title": "Testing-standard adherence",
        "importance": "Meeting approved testing time is essential for timely procurement release, drilling support and interruption-free operations.",
        "inference": standards_inference,
        "tone": standards_tone,
    })

    critical_open = age_buckets[3]["count"]
    if not summary["under_testing"]:
        ageing_inference = "There are no samples currently under testing, so no live laboratory backlog is visible."
        ageing_tone = "positive"
    elif not summary["delayed_open"]:
        ageing_inference = f"All {summary['under_testing']} open samples remain within the 9-day management review threshold."
        ageing_tone = "positive"
    else:
        ageing_inference = (
            f"{summary['delayed_open']} open samples have crossed the 9-day review threshold, "
            f"including {critical_open} open for 15 days or more. Escalate ownership and delay reasons."
        )
        ageing_tone = "risk"
    analysis_briefs.append({
        "title": "Open-sample ageing",
        "importance": "Ageing identifies pending laboratory work before it delays operational decisions or masks an emerging capacity constraint.",
        "inference": ageing_inference,
        "tone": ageing_tone,
    })

    populated_labs = [metric for metric in lab_metrics if metric["summary"]["total"]]
    if not populated_labs:
        laboratory_inference = "No laboratory sample history is available yet, so a delivery comparison cannot be made."
        laboratory_tone = "neutral"
    else:
        attention_lab = max(
            populated_labs,
            key=lambda metric: (
                metric["summary"]["delayed_open"] * 3
                + metric["standard_performance"]["late"] * 2
                + metric["summary"]["under_testing"]
            ),
        )
        attention_score = (
            attention_lab["summary"]["delayed_open"] * 3
            + attention_lab["standard_performance"]["late"] * 2
            + attention_lab["summary"]["under_testing"]
        )
        if attention_score == 0:
            laboratory_inference = "No laboratory currently has a late standard-time test or open workload in the available data."
            laboratory_tone = "positive"
        else:
            laboratory_inference = (
                f"{attention_lab['laboratory']['name']} has the highest current attention signal: "
                f"{attention_lab['standard_performance']['late']} late tests, "
                f"{attention_lab['summary']['under_testing']} open samples and "
                f"{attention_lab['summary']['delayed_open']} aged open samples."
            )
            laboratory_tone = "risk"
    analysis_briefs.append({
        "title": "Laboratory delivery comparison",
        "importance": "A comparable laboratory scorecard directs management support to the location where timeliness or backlog risk is greatest. Product failures are intentionally excluded.",
        "inference": laboratory_inference,
        "tone": laboratory_tone,
    })

    if len(monthly_trend) < 2:
        trend_inference = "At least two reporting months are needed before a directional workload or turnaround trend can be inferred."
        trend_tone = "neutral"
    else:
        previous_month, latest_month = monthly_trend[-2:]
        received_change = latest_month["received"] - previous_month["received"]
        completed_change = latest_month["completed"] - previous_month["completed"]
        tat_change = (
            latest_month["average_turnaround"] - previous_month["average_turnaround"]
            if latest_month["average_turnaround"] is not None and previous_month["average_turnaround"] is not None
            else None
        )
        trend_inference = (
            f"In {latest_month['label']}, intake changed by {received_change:+d} and completed reports by {completed_change:+d} "
            f"versus {previous_month['label']}"
            + (f"; average turnaround changed by {tat_change:+.1f} days." if tat_change is not None else ".")
        )
        trend_tone = "risk" if (received_change > completed_change or (tat_change is not None and tat_change > 0)) else "positive"
    analysis_briefs.append({
        "title": "Operating trend",
        "importance": "The monthly trend separates a one-off exception from a sustained change in workload, report completion or turnaround performance.",
        "inference": trend_inference,
        "tone": trend_tone,
    })

    if not risk_register:
        risk_inference = "No chemical currently has a recorded failure, late completed test or open-sample exposure in the available data."
        risk_tone = "positive"
    else:
        top_risk = risk_register[0]
        risk_inference = (
            f"{top_risk['chemical_name']} is the highest-priority chemical (risk score {top_risk['risk_score']}), driven by "
            f"{top_risk['failed']} failures, {top_risk['late']} late tests and {top_risk['aged_open']} aged open samples."
        )
        risk_tone = "risk"
    analysis_briefs.append({
        "title": "Chemical risk concentration",
        "importance": "A ranked chemical risk view helps focus technical review, vendor communication and laboratory follow-up on the material with the greatest operational exposure.",
        "inference": risk_inference,
        "tone": risk_tone,
    })

    return {
        "summary": summary,
        "tested": tested,
        "pass_rate": round(summary["passed"] / tested * 100, 1) if tested else None,
        "lab_metrics": lab_metrics,
        "chemical_metrics": chemical_metrics,
        "laboratories_by_code": LABORATORIES,
        "head_dashboard": head_dashboard,
        "standards_summary": standards_summary,
        "age_buckets": age_buckets,
        "monthly_trend": monthly_trend,
        "risk_register": risk_register[:20],
        "analysis_briefs": analysis_briefs,
    }


# The views a headline number can be opened as. Each is a question the analytics
# page answers with a count, so the count is a link to the samples behind it —
# the register is the evidence, not a separate place to go and search again.
# Ageing and lateness are derived from a sample's own dates, not stored, so these
# read as predicates rather than as SQL.
SAMPLE_VIEWS: dict[str, dict[str, Any]] = {
    "under_testing": {"label": "Samples under testing", "status": "under_testing"},
    "aged": {"label": "Open beyond 9 days", "status": "under_testing", "age": (10, None)},
    "age_0_3": {"label": "Open 0–3 days", "status": "under_testing", "age": (0, 3)},
    "age_4_9": {"label": "Open 4–9 days", "status": "under_testing", "age": (4, 9)},
    "age_10_14": {"label": "Open 10–14 days", "status": "under_testing", "age": (10, 14)},
    "age_15_plus": {"label": "Open 15 days or more", "status": "under_testing", "age": (15, None)},
    "completed": {"label": "Completed tests", "completed": True},
    "late": {"label": "Completed later than the approved standard", "completed": True, "standard": "late"},
    "within_standard": {"label": "Completed within the approved standard", "completed": True, "standard": "within"},
    "no_standard": {"label": "Completed with no approved standard to compare against", "completed": True, "standard": "none"},
    "failed": {"label": "Failed samples", "status": "fail"},
    "management_pending_all": {"label": "Pending Samples", "status": "under_testing"},
    "management_pending_within_standard": {
        "label": "Pending Samples — Within Standard", "status": "under_testing", "management_pending": "within_standard",
    },
    "management_pending_approaching_standard": {
        "label": "Pending Samples — Approaching Standard", "status": "under_testing", "management_pending": "approaching_standard",
    },
    "management_pending_delayed": {
        "label": "Pending Samples — Delayed", "status": "under_testing", "management_pending": "delayed",
    },
    "management_pending_critical": {
        "label": "Pending Samples — Critical", "status": "under_testing", "management_pending": "critical",
    },
    "management_completed_within_standard": {
        "label": "Completed Samples — Within Standard", "management_completed": "within_standard", "reporting_period": True,
    },
    "management_completed_delayed": {
        "label": "Completed Samples — Delayed", "management_completed": "delayed", "reporting_period": True,
    },
    "management_completed_critical": {
        "label": "Completed Samples — Critical", "management_completed": "critical", "reporting_period": True,
    },
}


def _matches_view(
    sample: QCSample,
    view: dict[str, Any],
    standards: dict[str, Any],
    as_of: date | None = None,
) -> bool:
    """Whether one sample belongs in a named view."""
    if view.get("status") and sample.result_status != view["status"]:
        return False
    if view.get("completed") and (sample.result_status not in COMPLETED_STATUSES or sample.turnaround_days is None):
        return False
    if "age" in view:
        low, high = view["age"]
        if sample.days_open is None or sample.days_open < low or (high is not None and sample.days_open > high):
            return False
    if "standard" in view:
        standard = standards.get(_normalized_chemical(sample.chemical_name))
        standard_days = standard.standard_days if standard else None
        if view["standard"] == "none":
            return standard_days is None
        if standard_days is None:
            return False
        late = sample.turnaround_days > standard_days
        return late if view["standard"] == "late" else not late
    if "management_pending" in view:
        assessment = assess_pending_sample(sample, _standard_days_for(sample, standards), as_of or date.today())
        return assessment is not None and assessment.classification == view["management_pending"]
    if "management_completed" in view:
        assessment = assess_completed_sample(sample, _standard_days_for(sample, standards))
        return assessment is not None and assessment.classification == view["management_completed"]
    return True


def search_samples(
    lab_code: str = "", chemical_name: str = "", specification_no: str = "", status: str = "", view: str = "",
    period_start: date | None = None, period_end: date | None = None, as_of: date | None = None,
) -> list[QCSample]:
    statement = QCSample.query
    if lab_code:
        get_laboratory(lab_code)
        statement = statement.filter(QCSample.lab_code == lab_code)
    if chemical_name:
        statement = statement.filter(QCSample.chemical_name == chemical_name)
    if specification_no:
        statement = statement.filter(QCSample.specification_no.ilike(f"%{specification_no.strip()}%"))
    if status in {"pass", "fail", "under_testing", "report_issued"}:
        statement = statement.filter(QCSample.result_status == status)
    selected = SAMPLE_VIEWS.get(view)
    if selected and selected.get("reporting_period") and period_start and period_end:
        statement = statement.filter(
            QCSample.report_issue_date >= period_start,
            QCSample.report_issue_date <= period_end,
        )
    ordered = statement.order_by(QCSample.sample_receipt_date.desc(), QCSample.id.desc())
    if selected is None:
        return ordered.limit(500).all()
    standards = {item.normalized_name: item for item in QCTestingStandard.query.all()}
    return [
        sample for sample in ordered.limit(2000).all()
        if _matches_view(sample, selected, standards, as_of=as_of)
    ][:500]


def history_filter_options(lab_code: str = "") -> dict[str, list]:
    statement = db.session.query(QCSample.chemical_name).distinct()
    if lab_code:
        get_laboratory(lab_code)
        statement = statement.filter(QCSample.lab_code == lab_code)
    chemicals = [row[0] for row in statement.order_by(QCSample.chemical_name.asc()).all() if row[0]]
    return {"laboratories": list(LABORATORIES.values()), "chemicals": chemicals}


def get_upload_batch(batch_id: int, include_source: bool = False) -> QCUploadBatch | None:
    statement = QCUploadBatch.query
    if include_source:
        statement = statement.options(undefer(QCUploadBatch.source_data))
    return statement.filter_by(id=batch_id).first()
