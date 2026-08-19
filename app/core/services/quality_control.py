"""Import, persistence, and management metrics for QC weekly workbooks."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import or_
from sqlalchemy.orm import undefer

from app.extensions import db
from app.models.quality_control.qc_sample import QCSample
from app.models.quality_control.qc_upload_batch import QCUploadBatch


XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_WEEK_PATTERN = re.compile(r"(\d{2}[./-]\d{2}[./-]\d{4}).*?(\d{2}[./-]\d{2}[./-]\d{4})")
IMPORT_STAGING_DIRECTORY = Path(tempfile.gettempdir()) / "ongc_qc_import_staging"
LABORATORIES = {
    "rgl_panvel": {"code": "rgl_panvel", "name": "RGL Panvel", "location": "Panvel", "description": "Regional Geoscience Laboratory"},
    "rgl_vadodara": {"code": "rgl_vadodara", "name": "RGL Vadodara", "location": "Vadodara", "description": "Regional Geoscience Laboratory"},
    "rgl_jorhat": {"code": "rgl_jorhat", "name": "RGL Jorhat", "location": "Jorhat", "description": "Regional Geoscience Laboratory"},
    "rgl_rajahmundry": {"code": "rgl_rajahmundry", "name": "RGL Rajahmundry", "location": "Rajahmundry", "description": "Regional Geoscience Laboratory"},
    "rgl_chennai": {"code": "rgl_chennai", "name": "RGL Chennai", "location": "Chennai", "description": "Regional Geoscience Laboratory"},
    "idwe_dehradun": {"code": "idwe_dehradun", "name": "IDWE Dehradun", "location": "Dehradun", "description": "Institute of Drilling and Well Engineering"},
}


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
        "delay_reason": ["delayreasonifmorethan09daysremarks", "reasonfordelay"],
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


def parse_weekly_qc_workbook(source: bytes) -> QCWorkbookPayload:
    """Convert the supplied weekly QC sheet into normalized source rows."""
    raw = pd.read_excel(BytesIO(source), sheet_name=0, header=None)
    title = next((_text(value) for value in raw.iloc[0].tolist() if _text(value)), "Weekly QC Data")
    match = _WEEK_PATTERN.search(title)
    if not match:
        raise ValueError("The workbook title must include a reporting period, for example 06.08.2026 to 12.08.2026.")
    week_start, week_end = _date(match.group(1)), _date(match.group(2))
    if not week_start or not week_end or week_end < week_start:
        raise ValueError("The reporting dates in the workbook title are invalid.")
    rows: list[dict[str, Any]] = []
    header_rows = _find_header_rows(raw)
    for header_index, (header_row, positions) in enumerate(header_rows):
        next_header = header_rows[header_index + 1][0] if header_index + 1 < len(header_rows) else len(raw)
        for _, raw_row in raw.iloc[header_row + 1 : next_header].iterrows():
            chemical = _text(raw_row.iloc[positions["chemical_name"]])
            serial = _text(raw_row.iloc[positions["serial_number"]])
            if not chemical or not re.fullmatch(r"\d+(?:\.0)?", serial):
                continue
            data = {field: _text(raw_row.iloc[index]) for field, index in positions.items()}
            receipt = _date(data.get("sample_receipt_date"))
            issued = _date(data.get("report_issue_date"))
            rows.append({
                "serial_number": int(float(serial)), "chemical_name": chemical,
                "specification_no": data.get("specification_no") or None,
                "supply_type": data.get("supply_type") or None, "po_number": data.get("po_number") or None,
                "lot_stack": data.get("lot_stack") or None, "notification_no": data.get("notification_no") or None,
                "result_status": _status(data.get("result_status"), issued), "sample_receipt_date": receipt,
                "report_issue_date": issued, "turnaround_days": _turnaround(data.get("turnaround_days"), receipt, issued),
                "delay_reason": data.get("delay_reason") or None,
            })
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


def sanity_check_weekly_qc_workbook(source: bytes, lab_code: str) -> tuple[QCWorkbookPayload, dict[str, Any]]:
    """Validate a workbook before it is allowed to create or update QC sample records."""
    laboratory = get_laboratory(lab_code)
    payload = parse_weekly_qc_workbook(source)
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
    payload = parse_weekly_qc_workbook(source)
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
    for row in payload.rows:
        key = _source_key(row, lab_code)
        sample = QCSample.query.filter_by(source_key=key).first()
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
        latest = QCUploadBatch.query.filter_by(lab_code=lab["code"]).order_by(QCUploadBatch.week_end.desc()).first()
        laboratories.append({**lab, "latest_batch": latest})
    return laboratories


def latest_dashboard_data(lab_code: str) -> dict[str, Any]:
    laboratory = get_laboratory(lab_code)
    batch = QCUploadBatch.query.filter_by(lab_code=lab_code).order_by(QCUploadBatch.week_end.desc()).first()
    if batch is None:
        return {"laboratory": laboratory, "batch": None, "summary": build_summary([]), "samples": [], "materials": [], "history": []}
    samples = QCSample.query.filter_by(last_seen_batch_id=batch.id).order_by(QCSample.sample_receipt_date.asc(), QCSample.chemical_name.asc()).all()
    summary = build_summary(samples, date.today())
    overdue_samples = sorted(
        [sample for sample in samples if sample.result_status == "under_testing" and sample.days_open is not None and sample.days_open > 9],
        key=lambda sample: sample.sample_receipt_date or date.max,
    )
    materials: dict[str, int] = {}
    for sample in samples:
        materials[sample.chemical_name] = materials.get(sample.chemical_name, 0) + 1
    history = QCUploadBatch.query.filter_by(lab_code=lab_code).order_by(QCUploadBatch.week_end.desc()).limit(12).all()
    return {"laboratory": laboratory, "batch": batch, "summary": summary, "samples": samples, "overdue_samples": overdue_samples, "materials": sorted(materials.items(), key=lambda item: (-item[1], item[0]))[:8], "history": history}


def portfolio_management_data() -> dict[str, Any]:
    """Build a management view from the most recent reporting week of every laboratory."""
    laboratory_reviews: list[dict[str, Any]] = []
    portfolio_samples: list[QCSample] = []
    for laboratory in LABORATORIES.values():
        batch = (
            QCUploadBatch.query.filter_by(lab_code=laboratory["code"])
            .order_by(QCUploadBatch.week_end.desc())
            .first()
        )
        samples = (
            QCSample.query.filter_by(last_seen_batch_id=batch.id)
            .order_by(QCSample.sample_receipt_date.asc(), QCSample.chemical_name.asc())
            .all()
            if batch
            else []
        )
        portfolio_samples.extend(samples)
        laboratory_reviews.append({"laboratory": laboratory, "batch": batch, "summary": build_summary(samples), "samples": samples})
    overdue_samples = sorted(
        [sample for sample in portfolio_samples if sample.result_status == "under_testing" and sample.days_open is not None and sample.days_open > 9],
        key=lambda sample: sample.sample_receipt_date or date.max,
    )
    return {
        "laboratory_reviews": laboratory_reviews,
        "summary": build_summary(portfolio_samples),
        "overdue_samples": overdue_samples,
        "reporting_labs": sum(review["batch"] is not None for review in laboratory_reviews),
        "laboratories_by_code": LABORATORIES,
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
