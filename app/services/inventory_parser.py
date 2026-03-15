"""SAP inventory export parser.

Accepts uploaded CSV or Excel (.xlsx / .xls) files and returns a list of
dicts ready to be bulk-inserted as InventoryRecord rows.

Expected SAP column headers (case-insensitive, leading/trailing whitespace
stripped):
    Plant | Material | Storage Location | Material Type | Material Group |
    Business Area | Division | Month | Tot. usage | Tot. usage UoM |
    Tot.us.val | Tot.us.val Currency

Month formats handled: YYYY.MM  /  MM/YYYY  /  MM-YYYY  /  YYYYMM  / plain text
"""

from __future__ import annotations

import csv
import io
import logging
import re
from decimal import Decimal, InvalidOperation
from typing import IO

logger = logging.getLogger(__name__)

# ── Column header aliases ─────────────────────────────────────────────────────
# Maps normalised header strings → internal field names.
HEADER_MAP: dict[str, str] = {
    "plant": "plant",
    "material": "material",
    "storage location": "storage_location",
    "storagelocation": "storage_location",
    "material type": "material_type",
    "materialtype": "material_type",
    "material group": "material_group",
    "materialgroup": "material_group",
    "business area": "business_area",
    "businessarea": "business_area",
    "division": "division",
    "month": "month_raw",
    "tot. usage": "total_usage",
    "tot.usage": "total_usage",
    "total usage": "total_usage",
    "tot. usage uom": "usage_uom",
    "tot.usage uom": "usage_uom",
    "total usage uom": "usage_uom",
    "tot.us.val": "total_usage_value",
    "tot. us. val": "total_usage_value",
    "total usage value": "total_usage_value",
    "tot.us.val currency": "currency",
    "tot. us. val currency": "currency",
    "total usage value currency": "currency",
    "currency": "currency",
}

REQUIRED_FIELDS = {"plant", "material", "month_raw"}

# ── Month parsing ─────────────────────────────────────────────────────────────

_MONTH_PATTERNS = [
    # YYYY.MM  →  2024.01
    (re.compile(r"^(\d{4})\.(\d{1,2})$"), lambda m: (int(m.group(1)), int(m.group(2)))),
    # YYYYMM   →  202401
    (re.compile(r"^(\d{4})(\d{2})$"), lambda m: (int(m.group(1)), int(m.group(2)))),
    # MM/YYYY  →  01/2024
    (re.compile(r"^(\d{1,2})/(\d{4})$"), lambda m: (int(m.group(2)), int(m.group(1)))),
    # MM-YYYY  →  01-2024
    (re.compile(r"^(\d{1,2})-(\d{4})$"), lambda m: (int(m.group(2)), int(m.group(1)))),
    # YYYY-MM  →  2024-01
    (re.compile(r"^(\d{4})-(\d{1,2})$"), lambda m: (int(m.group(1)), int(m.group(2)))),
]


def _parse_period(raw: str) -> tuple[int | None, int | None]:
    """Return (year, month) integers from a raw SAP month string, or (None, None)."""
    if not raw:
        return None, None
    raw = raw.strip()
    for pattern, extractor in _MONTH_PATTERNS:
        m = pattern.match(raw)
        if m:
            try:
                year, month = extractor(m)
                if 1 <= month <= 12 and 2000 <= year <= 2100:
                    return year, month
            except (ValueError, AttributeError):
                pass
    logger.debug("Could not parse month value: %r", raw)
    return None, None


def _parse_decimal(raw: str | None) -> Decimal | None:
    """Parse a numeric string, handling European comma-decimal separators."""
    if raw is None:
        return None
    cleaned = raw.strip().replace(",", "").replace(" ", "")
    # Handle European style: 1.234,56 → 1234.56
    if cleaned.count(".") > 1:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned and "." in cleaned:
        # 1,234.56 → already ok after removing commas above
        pass
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        logger.debug("Could not parse decimal: %r", raw)
        return None


def _normalise_header(raw: str) -> str:
    """Lowercase + strip for header matching."""
    return raw.strip().lower()


def _map_headers(raw_headers: list[str]) -> dict[int, str]:
    """
    Returns {column_index: field_name} for recognised SAP columns.
    Raises ValueError if no headers match the expected schema.
    """
    mapping: dict[int, str] = {}
    for idx, raw in enumerate(raw_headers):
        norm = _normalise_header(raw)
        field = HEADER_MAP.get(norm)
        if field:
            mapping[idx] = field
    if not mapping:
        raise ValueError(
            "No recognised SAP column headers found. "
            "Expected headers like: Plant, Material, Month, Tot. usage, etc."
        )
    return mapping


def _row_to_dict(row: list[str], col_map: dict[int, str]) -> dict:
    """Convert a raw row (list of strings) into a typed record dict."""
    record: dict = {}
    for idx, field in col_map.items():
        raw_val = row[idx].strip() if idx < len(row) else ""
        if field in ("total_usage", "total_usage_value"):
            record[field] = _parse_decimal(raw_val) if raw_val else None
        elif field == "month_raw":
            record["month_raw"] = raw_val
            year, month = _parse_period(raw_val)
            record["period_year"] = year
            record["period_month"] = month
        else:
            record[field] = raw_val if raw_val else None
    return record


# ── Public interface ──────────────────────────────────────────────────────────

class ParseResult:
    """Return value from parse_sap_export()."""

    def __init__(
        self,
        records: list[dict],
        warnings: list[str],
        skipped: int,
    ) -> None:
        self.records = records
        self.warnings = warnings
        self.skipped = skipped

    @property
    def row_count(self) -> int:
        return len(self.records)


def parse_sap_export(file_stream: IO[bytes], filename: str) -> ParseResult:
    """
    Parse an SAP material-usage export file (CSV or Excel).

    Args:
        file_stream: Werkzeug FileStorage or any binary-mode file object.
        filename:    Original filename (used to detect file type).

    Returns:
        ParseResult with .records (list of dicts), .warnings, .skipped.

    Raises:
        ValueError:  Unrecognised file type or no matching headers.
        RuntimeError: Dependency missing (openpyxl).
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext in ("xlsx", "xls"):
        rows = _read_excel(file_stream, filename)
    elif ext == "csv":
        rows = _read_csv(file_stream)
    else:
        raise ValueError(
            f"Unsupported file type '.{ext}'. Please upload a CSV or Excel (.xlsx) file."
        )

    if not rows:
        raise ValueError("The uploaded file is empty.")

    col_map = _map_headers(rows[0])
    records: list[dict] = []
    warnings: list[str] = []
    skipped = 0

    for line_num, row in enumerate(rows[1:], start=2):
        # Skip completely empty rows
        if not any(c.strip() for c in row):
            skipped += 1
            continue

        record = _row_to_dict(row, col_map)

        # Warn on missing key fields but still import
        missing = [f for f in REQUIRED_FIELDS if not record.get(f)]
        if missing:
            warnings.append(
                f"Row {line_num}: missing {', '.join(missing)} — row imported with blanks."
            )

        records.append(record)

    return ParseResult(records=records, warnings=warnings, skipped=skipped)


# ── File readers ──────────────────────────────────────────────────────────────

def _read_csv(file_stream: IO[bytes]) -> list[list[str]]:
    """Read a CSV file and return all rows as list-of-lists of strings."""
    raw_bytes = file_stream.read()
    # Try UTF-8 first, fall back to cp1252 (common for SAP exports on Windows)
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw_bytes.decode(encoding)
            reader = csv.reader(io.StringIO(text))
            return [row for row in reader]
        except (UnicodeDecodeError, csv.Error):
            continue
    raise ValueError("Could not decode CSV file. Ensure the file is UTF-8 or Windows-1252 encoded.")


def _read_excel(file_stream: IO[bytes], filename: str) -> list[list[str]]:
    """Read an Excel file and return all rows from the first sheet as list-of-lists."""
    try:
        import openpyxl
    except ImportError:
        raise RuntimeError(
            "openpyxl is required to read Excel files. "
            "Run: pip install openpyxl"
        )

    raw_bytes = file_stream.read()
    wb = openpyxl.load_workbook(
        filename=io.BytesIO(raw_bytes),
        read_only=True,
        data_only=True,
    )
    ws = wb.active
    rows: list[list[str]] = []
    for row in ws.iter_rows(values_only=True):
        rows.append([str(cell) if cell is not None else "" for cell in row])
    wb.close()
    return rows
