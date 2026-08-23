"""Import, persistence, and management metrics for QC weekly workbooks."""

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
LABORATORIES = {
    "rgl_panvel": {"code": "rgl_panvel", "name": "RGL Panvel", "location": "Panvel", "description": "Regional Geoscience Laboratory"},
    "rgl_vadodara": {"code": "rgl_vadodara", "name": "RGL Vadodara", "location": "Vadodara", "description": "Regional Geoscience Laboratory"},
    "rgl_jorhat": {"code": "rgl_jorhat", "name": "RGL Jorhat", "location": "Jorhat", "description": "Regional Geoscience Laboratory"},
    "rgl_rajahmundry": {"code": "rgl_rajahmundry", "name": "RGL Rajahmundry", "location": "Rajahmundry", "description": "Regional Geoscience Laboratory"},
    "rgl_chennai": {"code": "rgl_chennai", "name": "RGL Chennai", "location": "Chennai", "description": "Regional Geoscience Laboratory"},
    "idwe_cementing": {"code": "idwe_cementing", "name": "IDWE Cementing Laboratory", "location": "Dehradun", "description": "Institute of Drilling and Well Engineering · Cementing"},
    "idwe_df_cf": {"code": "idwe_df_cf", "name": "IDWE DF–CF Laboratory", "location": "Dehradun", "description": "Institute of Drilling and Well Engineering · DF–CF"},
}

IDWE_WORKSTREAM_CODES = ("idwe_cementing", "idwe_df_cf")


@dataclass(frozen=True)
class QCWorkbookPayload:
    report_label: str
    week_start: date
    week_end: date
    rows: list[dict[str, Any]]


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
        warnings.append("No samples are marked under testing in this reporting week.")

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


def laboratory_landing_data() -> list[dict[str, Any]]:
    laboratories = []
    for lab in LABORATORIES.values():
        if lab["code"] in IDWE_WORKSTREAM_CODES:
            continue
        latest = QCUploadBatch.query.filter_by(lab_code=lab["code"]).order_by(QCUploadBatch.week_end.desc()).first()
        laboratories.append({**lab, "latest_batch": latest})
    laboratories.sort(key=lambda lab: lab["location"].casefold())
    workstreams = []
    for code in IDWE_WORKSTREAM_CODES:
        lab = LABORATORIES[code]
        latest = QCUploadBatch.query.filter_by(lab_code=code).order_by(QCUploadBatch.week_end.desc()).first()
        workstreams.append({**lab, "latest_batch": latest})
    latest_batches = [workstream["latest_batch"] for workstream in workstreams if workstream["latest_batch"]]
    laboratories.append({
        "code": "idwe_dehradun",
        "name": "IDWE Dehradun",
        "location": "Dehradun",
        "description": "Institute of Drilling and Well Engineering",
        "is_group": True,
        "workstreams": workstreams,
        "latest_batch": max(latest_batches, key=lambda batch: batch.week_end) if latest_batches else None,
    })
    return laboratories


def current_monitoring_week() -> dict[str, Any] | None:
    """The reporting week the workspace is currently monitoring.

    Taken from the most recent weekly workbook any laboratory has imported —
    never from today's date, so the navigator states what the system actually
    holds rather than what the calendar suggests it should.
    """
    batch = QCUploadBatch.query.order_by(QCUploadBatch.week_end.desc()).first()
    if batch is None:
        return None
    return {
        "week_start": batch.week_start,
        "week_end": batch.week_end,
        "lab_code": batch.lab_code,
        "label": _format_week_range(batch.week_start, batch.week_end),
    }


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
    """Every laboratory that can receive a weekly workbook, flattened.

    The landing data nests the two IDWE workstreams under one grouped entry
    because they share a location; imports are per workstream, so the group is
    unfolded here into the units that actually accept a file.
    """
    targets = []
    for lab in laboratory_landing_data():
        if lab.get("is_group"):
            targets.extend(lab["workstreams"])
        else:
            targets.append(lab)
    return targets


def laboratory_navigator_data(laboratories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Lab navigator markers: one entry per laboratory.

    A laboratory opens its own dashboard on click. IDWE Dehradun is the one
    grouped entry — a single location running two workstreams with separate
    dashboards — so it alone keeps a chooser of destinations.
    """
    entries = []
    for lab in laboratories:
        batch = lab["latest_batch"]
        if lab.get("is_group"):
            choices = [{
                "label": "Data Import",
                "hint": "Import a weekly workbook for either IDWE workstream",
                "href": url_for("quality_control.data_import"),
                "combined": True,
            }] + [{
                "label": workstream["name"],
                "hint": "Weekly testing dashboard"
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
            "workstream_count": len(lab.get("workstreams", [])),
            "is_reporting": batch is not None,
            "reporting_period": batch.week_end.strftime("%d %b %Y") if batch else None,
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
    }


def portfolio_management_data(reporting_week_end: date | None = None) -> dict[str, Any]:
    """Build a period-controlled management view without mixing reporting weeks."""
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
    for laboratory in LABORATORIES.values():
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
            {"week_end": week_end, "label": week_end.strftime("Week ending %d %b %Y"), "submission_count": submission_counts[week_end]}
            for week_end in available_week_ends
        ],
        "laboratories_by_code": LABORATORIES,
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

    age_buckets = [
        {"label": "0-3 days", "count": 0, "tone": "neutral"},
        {"label": "4-9 days", "count": 0, "tone": "warning"},
        {"label": "10-14 days", "count": 0, "tone": "risk"},
        {"label": "15+ days", "count": 0, "tone": "critical"},
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
        "standards_summary": standards_summary,
        "age_buckets": age_buckets,
        "monthly_trend": monthly_trend,
        "risk_register": risk_register[:20],
        "analysis_briefs": analysis_briefs,
    }


def search_samples(
    lab_code: str = "", chemical_name: str = "", specification_no: str = "", status: str = "",
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
    return statement.order_by(QCSample.sample_receipt_date.desc(), QCSample.id.desc()).limit(500).all()


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
