"""Workbook validation, imports and query helpers for Inventory Monitoring."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
import time
import uuid
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import func, or_, text

from app.core.services.csc_utils import (
    SPEC_SUBSET_LABELS,
    SPEC_SUBSET_ORDER,
    is_api_grade_chemical,
)
from app.extensions import db
from app.models.inventory.monitoring import (
    InventoryMonitoringException, InventoryMonitoringMaterial, InventoryMonitoringRecord,
    InventoryMonitoringSnapshot, InventoryMonitoringThreshold, InventoryMonitoringUploadBatch,
    InventoryMonitoringWorkCenter, InventoryMonitoringWorkCenterMaterial,
)

STAGING_DIRECTORY = Path(tempfile.gettempdir()) / "inventory_monitoring_staging"
STAGING_DIRECTORY.mkdir(parents=True, exist_ok=True)
GROUP_SHEETS = {"09": "09 Oil well cement - Inventory", "10": "10 Chemi incl mud chemi - Inven"}
CORE_COLUMNS = {"materialcode", "materialdescription", "workcentre", "stockqty", "inventoryvalueinr", "stockmonths"}
# The Group 09/10 exports have carried no unit column; accept whatever SAP calls it when they do.
UOM_HEADERS = {"uom", "uoe", "unit", "units", "baseunit", "basicunit", "stockunit", "unitofentry", "unitofmeasure", "unitofmeasurement", "baseunitofmeasure", "bun"}
DEFAULT_THRESHOLDS = {
    "critical_low_stock_months": Decimal("1"),
    "low_stock_months": Decimal("3"),
    "slow_moving_months": Decimal("6"),
    "excess_stock_months": Decimal("12"),
}


def normalize_name(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def material_code(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    if text.isdigit() and len(text) < 9:
        text = text.zfill(9)
    return text


def _header(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def _decimal(value: Any) -> Decimal | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except Exception:
        return None


def _find_sheet(book: pd.ExcelFile, prefix: str) -> str:
    expected = _header(prefix)
    match = next((sheet for sheet in book.sheet_names if _header(sheet).startswith(expected)), None)
    if not match:
        raise ValueError(f"Required detailed inventory sheet beginning '{prefix}' was not found.")
    return match


def _detect_reporting_date(filename: str) -> date | None:
    match = re.search(r"(20\d{2})(\d{2})(\d{2})", filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(0), "%Y%m%d").date()
    except ValueError:
        return None


def _read_inventory(source: bytes, source_group: str) -> tuple[str, list[dict[str, Any]], list[str]]:
    book = pd.ExcelFile(BytesIO(source))
    sheet = _find_sheet(book, GROUP_SHEETS[source_group])
    frame = pd.read_excel(book, sheet_name=sheet, header=1, dtype=object)
    fields = {_header(column): column for column in frame.columns}
    missing = CORE_COLUMNS - set(fields)
    if missing:
        raise ValueError("The detailed inventory sheet is missing: " + ", ".join(sorted(missing)))
    aliases = {"workcentre": "workcentre", "workcenter": "workcentre", "inventoryvalueinr": "inventoryvalueinr", "openpo": "openpo", "openpr": "openpr"}
    canonical = {aliases.get(key, key): value for key, value in fields.items()}
    uom_column = next((column for key, column in canonical.items() if key in UOM_HEADERS), None)
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for index, raw in frame.iterrows():
        code = material_code(raw[canonical["materialcode"]])
        centre = str(raw[canonical["workcentre"]] or "").strip()
        if not code or not centre:
            warnings.append(f"Row {index + 3}: missing material code or work centre.")
            continue
        value = _decimal(raw[canonical["inventoryvalueinr"]])
        if value is None or value <= 0:
            # Monitoring covers stock that carries value; a nil-value line is not held stock.
            warnings.append(f"Row {index + 3}: inventory value is nil, so the line is not monitored.")
            continue
        rows.append({
            "material_code": code, "material_description": str(raw[canonical["materialdescription"]] or "").strip() or None,
            "work_center_name": centre, "stock_qty": _decimal(raw[canonical["stockqty"]]),
            "uom": (str(raw[uom_column] or "").strip().upper() or None) if uom_column else None,
            "inventory_value_inr": value,
            "open_po": _decimal(raw[canonical["openpo"]]) if canonical.get("openpo") else None,
            "open_pr": _decimal(raw[canonical["openpr"]]) if canonical.get("openpr") else None,
            "stock_months": _decimal(raw[canonical["stockmonths"]]), "source_row": int(index + 3), "source_sheet": sheet,
        })
    return sheet, rows, warnings


def _read_mapping(source: bytes) -> tuple[list[dict[str, Any]], list[str]]:
    frame = pd.read_excel(BytesIO(source), sheet_name=0, header=0, dtype=object)
    if frame.shape[1] < 5:
        raise ValueError("The mapping workbook must contain work centres and material-code columns.")
    mappings, warnings = [], []
    for index, raw in frame.iterrows():
        zone, centre, centre_type = (str(raw.iloc[i] or "").strip() for i in (1, 2, 3))
        if not centre:
            continue
        codes = {material_code(value) for value in raw.iloc[4:] if material_code(value)}
        if not codes:
            warnings.append(f"Row {index + 2}: {centre} has no mapped materials.")
        mappings.extend({"zone": zone or None, "work_center_name": centre, "work_center_type": centre_type or None, "material_code": code, "source_row": int(index + 2)} for code in codes)
    return mappings, warnings


def _read_mapping_directory(source: bytes) -> list[dict[str, Any]]:
    """Return every configured location row, including those with no material codes."""
    frame = pd.read_excel(BytesIO(source), sheet_name=0, header=0, dtype=object)
    if frame.shape[1] < 4:
        raise ValueError("The mapping workbook must contain zone, work-centre, and type columns.")
    directory = []
    for index, raw in frame.iterrows():
        zone, centre, centre_type = (str(raw.iloc[i] or "").strip() for i in (1, 2, 3))
        if centre:
            directory.append({
                "zone": zone or None,
                "work_center_name": centre,
                "work_center_type": centre_type or None,
                "source_row": int(index + 2),
            })
    return directory


def _read_supporting_exceptions(source: bytes) -> list[dict[str, Any]]:
    """Extract actionable rows from the supporting sheets without treating them as inventory facts."""
    book = pd.ExcelFile(BytesIO(source))
    sheet_rules = {
        "slowmov": "slow_moving", "non moving inventory": "non_moving",
        "inventorylyinginstoc": "aged_stock_over_one_year", "items lying in surplus": "surplus",
        "mit cases open": "material_in_transit_aged",
    }
    findings: list[dict[str, Any]] = []
    for sheet in book.sheet_names:
        sheet_key = _header(sheet)
        exception_type = next((kind for phrase, kind in sheet_rules.items() if _header(phrase) in sheet_key), None)
        if exception_type is None and sheet_key.endswith("slow"):
            exception_type = "slow_moving"
        if exception_type is None:
            continue
        frame = pd.read_excel(book, sheet_name=sheet, header=1, dtype=object)
        columns = {_header(column): column for column in frame.columns}
        code_column = columns.get("materialcode")
        centre_column = columns.get("workcentre") or columns.get("workcenter")
        description_column = columns.get("materialdescription")
        if not code_column or not centre_column:
            continue
        value_column = next((column for key, column in columns.items() if "stockvalue" in key or "slowmovingvalue" in key or "inventoryvalue" in key), None)
        for index, raw in frame.iterrows():
            code, centre = material_code(raw[code_column]), str(raw[centre_column] or "").strip()
            if code and centre:
                value = _decimal(raw[value_column]) if value_column else None
                value_key = _header(value_column) if value_column else ""
                if value is not None and ("crore" in value_key or value_key.endswith("cr")):
                    value *= Decimal("10000000")
                elif value is not None and "lakh" in value_key:
                    value *= Decimal("100000")
                findings.append({"exception_type": exception_type, "material_code": code, "material_description": str(raw[description_column] or "").strip() or None if description_column else None, "work_center_name": centre, "source_sheet": sheet, "source_row": int(index + 3), "value": value})
    return findings


def stage_workbook(source: bytes) -> str:
    token = uuid.uuid4().hex
    (STAGING_DIRECTORY / f"{token}.xlsx").write_bytes(source)
    return token


def load_staged_workbook(token: str) -> bytes:
    path = STAGING_DIRECTORY / f"{token}.xlsx"
    if not re.fullmatch(r"[a-f0-9]{32}", token or "") or not path.is_file() or path.stat().st_mtime < time.time() - 3600:
        path.unlink(missing_ok=True)
        raise ValueError("This import review has expired. Upload the workbook again.")
    return path.read_bytes()


def discard_staged_workbook(token: str) -> None:
    (STAGING_DIRECTORY / f"{token}.xlsx").unlink(missing_ok=True)


def validate_workbook(source: bytes, filename: str, source_group: str) -> dict[str, Any]:
    if source_group not in {"09", "10", "mapping"}:
        raise ValueError("Select Group 09, Group 10, or Work-centre mapping.")
    if source_group == "mapping":
        rows, warnings = _read_mapping(source)
        duplicate_count = len(rows) - len({(normalize_name(row["work_center_name"]), row["material_code"]) for row in rows})
        return {"source_group": source_group, "reporting_date": None, "row_count": len(rows), "accepted_count": len(rows), "rejected_count": 0, "duplicate_count": duplicate_count, "warnings": warnings[:20], "issue_samples": warnings[:10]}
    _, rows, warnings = _read_inventory(source, source_group)
    duplicate_count = len(rows) - len({(row["material_code"], normalize_name(row["work_center_name"])) for row in rows})
    return {"source_group": source_group, "reporting_date": _detect_reporting_date(filename), "row_count": len(rows) + len(warnings), "accepted_count": len(rows), "rejected_count": len(warnings), "duplicate_count": duplicate_count, "warnings": warnings[:20], "issue_samples": warnings[:10]}


def _get_material(code: str, description: str | None, group: str | None) -> InventoryMonitoringMaterial:
    item = InventoryMonitoringMaterial.query.filter_by(material_code=code).first()
    if item is None:
        item = InventoryMonitoringMaterial(material_code=code); db.session.add(item)
    item.description = description or item.description
    item.material_group = group or item.material_group
    return item


def _get_work_center(name: str, zone: str | None = None, centre_type: str | None = None) -> InventoryMonitoringWorkCenter:
    key = normalize_name(name)
    item = InventoryMonitoringWorkCenter.query.filter_by(normalized_name=key).first()
    if item is None:
        item = InventoryMonitoringWorkCenter(name=name, normalized_name=key); db.session.add(item)
    item.zone, item.work_center_type = zone or item.zone, centre_type or item.work_center_type
    return item


def _thresholds() -> dict[str, Decimal]:
    configured = {item.key: item.value for item in InventoryMonitoringThreshold.query.all()}
    return {**DEFAULT_THRESHOLDS, **configured}


def material_uom_index() -> dict[str, str]:
    """Base unit per material code, taken from the retained consumption and usage history."""
    from sqlalchemy import inspect

    inspector = inspect(db.engine)
    index: dict[str, str] = {}
    for table, code_column, uom_column in (
        ("inventory_records", "material", "usage_uom"),
        ("inventory_consumption_seed_rows", "material_code", "uom"),
    ):
        if not inspector.has_table(table):
            continue
        rows = db.session.execute(text(
            f"SELECT {code_column}, {uom_column}, COUNT(*) AS usage_count FROM {table} "
            f"WHERE {uom_column} IS NOT NULL AND {uom_column} <> '' GROUP BY 1, 2 ORDER BY usage_count"
        )).all()
        for code, uom, _count in rows:  # ordered ascending, so the most-used unit wins
            key = material_code(code)
            if key:
                index[key] = str(uom).strip().upper()
    return index


def backfill_missing_uom() -> int:
    """Fill units of measure the inventory workbooks do not carry, from consumption history."""
    index = material_uom_index()
    if not index:
        return 0
    updated = 0
    for record in InventoryMonitoringRecord.query.filter(
        (InventoryMonitoringRecord.uom.is_(None)) | (InventoryMonitoringRecord.uom == "")
    ).all():
        uom = index.get(record.material_code)
        if uom:
            record.uom = uom
            updated += 1
    return updated


def _create_exceptions(snapshot: InventoryMonitoringSnapshot) -> None:
    """Raise the coverage exceptions for one snapshot.

    Mapping is no longer something to police: every imported stock line maps its own
    material to the work centre holding it, so only stock-coverage findings are raised.
    """
    thresholds = _thresholds()
    records = InventoryMonitoringRecord.query.filter_by(snapshot_id=snapshot.id).all()
    for record in records:
        failures: list[tuple[str, str, str]] = []
        if record.stock_months is not None:
            if record.stock_months <= thresholds["critical_low_stock_months"]:
                failures.append(("critical_low_stock", "critical", "Stock coverage is at or below the critical low-stock threshold."))
            elif record.stock_months <= thresholds["low_stock_months"]:
                failures.append(("low_stock", "high", "Stock coverage is below the low-stock threshold."))
            elif record.stock_months >= thresholds["excess_stock_months"]:
                failures.append(("excess_stock", "high", "Stock coverage is at or above the excess-stock threshold."))
            elif record.stock_months >= thresholds["slow_moving_months"]:
                failures.append(("slow_moving_stock", "medium", "Stock coverage is at or above the slow-moving threshold."))
        if record.stock_months is not None and record.stock_months >= thresholds["slow_moving_months"] and ((record.open_po or 0) > 0 or (record.open_pr or 0) > 0):
            failures.append(("open_supply_with_high_stock", "high", "Open PO/PR exists while stock coverage is high."))
        for kind, severity, detail in failures:
            db.session.add(InventoryMonitoringException(snapshot_id=snapshot.id, record_id=record.id, work_center_id=record.work_center_id, material_id=record.material_id, exception_type=kind, severity=severity, details=detail, inventory_value_inr=record.inventory_value_inr, stock_months=record.stock_months, review_status="not_required"))


def _current_mapping_pairs() -> set[tuple[int, int]]:
    return {(item.work_center_id, item.material_id) for item in InventoryMonitoringWorkCenterMaterial.query.filter_by(is_current=True).all()}


# Corporate-specification categories are shared with CSC exports and coverage
# reporting.  Keep the final no-specification group separate from this sequence.
SPECIFICATION_CATEGORIES = tuple(
    (key, SPEC_SUBSET_LABELS[key]) for key in SPEC_SUBSET_ORDER
)
UNSPECIFIED_CATEGORY_KEY = "unspecified"
UNSPECIFIED_CATEGORY_LABEL = "Not in Corporate Specifications"


def _specification_category(specification_no: Any) -> str | None:
    """The category segment of a specification number, e.g. DFC in ONGC / DFC / 01 / 2026."""
    parts = [part.strip().upper() for part in str(specification_no or "").split("/")]
    if len(parts) < 2 or not re.fullmatch(r"[A-Z]{2,6}", parts[1]):
        return None
    return parts[1]


def _specification_rows() -> list[Any]:
    from app.models.quality_control.qc_testing_standard import QCTestingStandard

    return QCTestingStandard.query.all()


def _specification_serial(specification_no: Any) -> int:
    """The serial within a category, e.g. 7 in ONGC / DFC / 07 / 2026; unnumbered rows sort last."""
    parts = [part.strip() for part in str(specification_no or "").split("/")]
    return int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 9999


def _specification_entry(item: Any) -> tuple[str, dict[str, Any]]:
    """One master row as (matchable material code or "", specification detail)."""
    code = material_code(item.material_code)
    category = _specification_category(item.specification_no)
    # The seven legacy API-grade entries do not have a structured specification
    # number, but their register name identifies their corporate category.
    if category is None and is_api_grade_chemical(item.chemical_name):
        category = "API"
    return code if code.isdigit() else "", {
        "serial": _specification_serial(item.specification_no),
        "chemical_name": item.chemical_name,
        "specification_no": item.specification_no,
        "category": category,
        "category_label": dict(SPECIFICATION_CATEGORIES).get(category, f"{category} chemicals") if category else None,
        "standard_days": item.standard_days,
        "remarks": item.remarks,
    }


def specification_index(standards: list[Any] | None = None) -> dict[str, dict[str, Any]]:
    """Material code → its corporate specification, from the QC testing-standards master.

    Rows carrying "Code not mapped*" or "-" have no SAP code and cannot be matched to
    stock, so they are counted in the catalogue but never indexed against a material.
    """
    index: dict[str, dict[str, Any]] = {}
    for item in standards if standards is not None else _specification_rows():
        code, spec = _specification_entry(item)
        if code:
            index[code] = spec
    return index


def specification_groups(items: list[Any], index: dict[str, dict[str, Any]] | None = None, code_of: Any = None, limit: int | None = None) -> list[dict[str, Any]]:
    """Group rows into specification order — DFC / 01, DFC / 02, then PC / 01 — under category headings.

    Material codes with no current corporate specification are kept together at the end.
    """
    index = index if index is not None else specification_index()
    code_of = code_of or (lambda item: getattr(item, "material_code", "") or "")
    rank = {key: order for order, (key, _label) in enumerate(SPECIFICATION_CATEGORIES)}
    labels = dict(SPECIFICATION_CATEGORIES)
    buckets: dict[str, list[tuple[int, str, Any]]] = defaultdict(list)
    for item in items:
        code = material_code(code_of(item))
        spec = index.get(code)
        key = (spec["category"] if spec and spec["category"] else None) or UNSPECIFIED_CATEGORY_KEY
        buckets[key].append((spec["serial"] if spec else 9999, code, item))
    ordered = sorted(buckets, key=lambda key: (99 if key == UNSPECIFIED_CATEGORY_KEY else rank.get(key, 90), key))
    groups = []
    for key in ordered:
        rows = [item for _serial, _code, item in sorted(buckets[key], key=lambda row: (row[0], row[1]))]
        groups.append({
            "key": key,
            "label": UNSPECIFIED_CATEGORY_LABEL if key == UNSPECIFIED_CATEGORY_KEY else labels.get(key, f"{key} chemicals"),
            "rows": rows[:limit] if limit else rows,
            "total": len(rows),
            "omitted": max(len(rows) - limit, 0) if limit else 0,
        })
    return groups


def specification_codes(category: str) -> set[str]:
    """Material codes carrying a specification in one category."""
    return {code for code, spec in specification_index().items() if spec["category"] == category}


def monitored_material_categories(reporting_date: date | None = None) -> dict[str, Any]:
    """Monitored material codes and value grouped by corporate specification category."""
    latest_date = reporting_date or db.session.query(func.max(InventoryMonitoringSnapshot.reporting_date)).filter_by(is_published=True).scalar()
    standards = _specification_rows()
    index = specification_index(standards)
    rows = []
    if latest_date is not None:
        rows = db.session.query(
            InventoryMonitoringRecord.material_code,
            func.max(InventoryMonitoringRecord.material_description),
            func.coalesce(func.sum(InventoryMonitoringRecord.inventory_value_inr), 0),
        ).join(
            InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
        ).filter(
            InventoryMonitoringSnapshot.reporting_date == latest_date,
            InventoryMonitoringSnapshot.is_published.is_(True),
        ).group_by(InventoryMonitoringRecord.material_code).all()

    held: dict[str, dict[str, Any]] = defaultdict(lambda: {"materials": 0, "value": Decimal("0")})
    total = Decimal("0")
    for code, _description, value in rows:
        value = value or Decimal("0")
        total += value
        spec = index.get(code)
        key = (spec["category"] if spec and spec["category"] else None) or UNSPECIFIED_CATEGORY_KEY
        held[key]["materials"] += 1
        held[key]["value"] += value

    catalogue: dict[str, dict[str, int]] = defaultdict(lambda: {"entries": 0, "without_code": 0})
    for item in standards:
        code, spec = _specification_entry(item)
        entry = catalogue[spec["category"] or UNSPECIFIED_CATEGORY_KEY]
        entry["entries"] += 1
        entry["without_code"] += not code

    ordered = [key for key, _label in SPECIFICATION_CATEGORIES]
    ordered += sorted(key for key in set(held) | set(catalogue) if key not in ordered and key != UNSPECIFIED_CATEGORY_KEY)
    labels = dict(SPECIFICATION_CATEGORIES)
    tiles = [
        {
            "key": key, "label": labels.get(key, f"{key} chemicals"),
            "materials": held[key]["materials"], "value": held[key]["value"],
            "share": _share(held[key]["value"], total),
            "catalogue": catalogue[key]["entries"], "without_code": catalogue[key]["without_code"],
            "is_unspecified": False,
        }
        for key in ordered
    ]
    tiles.append({
        "key": UNSPECIFIED_CATEGORY_KEY, "label": UNSPECIFIED_CATEGORY_LABEL,
        "materials": held[UNSPECIFIED_CATEGORY_KEY]["materials"], "value": held[UNSPECIFIED_CATEGORY_KEY]["value"],
        "share": _share(held[UNSPECIFIED_CATEGORY_KEY]["value"], total),
        "catalogue": 0, "without_code": 0, "is_unspecified": True,
    })
    return {
        "reporting_date": latest_date, "tiles": tiles, "total_value": total,
        "monitored_materials": sum(tile["materials"] for tile in tiles),
        "specification_count": len(index),
    }


def material_mapping_register_data(term: str = "", category: str = "") -> dict[str, Any]:
    """Return the current mapping register, grouped by material for super-user review."""
    index = specification_index()
    query = InventoryMonitoringMaterial.query
    category = (category or "").strip()
    latest_date = db.session.query(func.max(InventoryMonitoringSnapshot.reporting_date)).filter_by(is_published=True).scalar()
    inventory_values = db.session.query(
        InventoryMonitoringRecord.material_id.label("material_id"),
        func.coalesce(func.sum(InventoryMonitoringRecord.inventory_value_inr), 0).label("inventory_value"),
    ).join(
        InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
    ).filter(
        InventoryMonitoringSnapshot.reporting_date == latest_date,
        InventoryMonitoringSnapshot.is_published.is_(True),
        InventoryMonitoringRecord.material_id.isnot(None),
    ).group_by(InventoryMonitoringRecord.material_id).subquery() if latest_date else None
    if term:
        query = query.filter(
            (InventoryMonitoringMaterial.material_code.ilike(f"%{term}%"))
            | (InventoryMonitoringMaterial.description.ilike(f"%{term}%"))
        )
    category_label = None
    if category == UNSPECIFIED_CATEGORY_KEY:
        category_label = UNSPECIFIED_CATEGORY_LABEL
        query = query.filter(InventoryMonitoringMaterial.material_code.notin_(index.keys() or {""}))
    elif category:
        category_label = dict(SPECIFICATION_CATEGORIES).get(category, f"{category} chemicals")
        codes = {code for code, spec in index.items() if spec["category"] == category}
        query = query.filter(InventoryMonitoringMaterial.material_code.in_(codes or {""}))
    total_count = query.count()
    if category == UNSPECIFIED_CATEGORY_KEY and inventory_values is not None:
        query = query.outerjoin(inventory_values, inventory_values.c.material_id == InventoryMonitoringMaterial.id).order_by(
            func.coalesce(inventory_values.c.inventory_value, 0).desc(), InventoryMonitoringMaterial.material_code
        )
    else:
        query = query.order_by(InventoryMonitoringMaterial.material_code)
    materials = query.limit(500).all()
    material_ids = [item.id for item in materials]
    inventory_values_by_material: dict[int, Decimal] = {}
    if inventory_values is not None and material_ids:
        for material_id, value in db.session.query(inventory_values.c.material_id, inventory_values.c.inventory_value).filter(
            inventory_values.c.material_id.in_(material_ids)
        ).all():
            inventory_values_by_material[material_id] = value or Decimal("0")
    # The plant / work centre a material is mapped to is read straight from the uploaded
    # inventory: wherever the latest published workbooks report stock, that is the mapping.
    centres_by_material: dict[int, list[InventoryMonitoringWorkCenter]] = defaultdict(list)
    if material_ids and latest_date is not None:
        for material_id, centre in db.session.query(
            InventoryMonitoringRecord.material_id, InventoryMonitoringWorkCenter
        ).join(
            InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
        ).join(
            InventoryMonitoringWorkCenter, InventoryMonitoringRecord.work_center_id == InventoryMonitoringWorkCenter.id
        ).filter(
            InventoryMonitoringSnapshot.reporting_date == latest_date,
            InventoryMonitoringSnapshot.is_published.is_(True),
            InventoryMonitoringRecord.material_id.in_(material_ids),
        ).distinct().order_by(InventoryMonitoringWorkCenter.zone, InventoryMonitoringWorkCenter.name).all():
            centres_by_material[material_id].append(centre)
    material_groups = specification_groups(materials, index)
    if category == UNSPECIFIED_CATEGORY_KEY:
        for group in material_groups:
            group["rows"].sort(
                key=lambda item: (-inventory_values_by_material.get(item.id, Decimal("0")), item.material_code),
            )
    return {
        "materials": materials,
        "material_groups": material_groups,
        "centres_by_material": centres_by_material,
        "inventory_values_by_material": inventory_values_by_material,
        "term": term,
        "category": category,
        "category_label": category_label,
        "total_count": total_count,
        "specifications": {item.material_code: index.get(item.material_code) for item in materials},
        "category_options": [
            {"key": key, "label": label} for key, label in SPECIFICATION_CATEGORIES
        ] + [{"key": UNSPECIFIED_CATEGORY_KEY, "label": UNSPECIFIED_CATEGORY_LABEL}],
    }


def _live_batch() -> Any:
    """A batch is live unless a *different* batch replaced it.

    A batch recorded as its own successor is the mark of a defect, not a genuine
    replacement, and must never hide a whole material group from the reviews.
    """
    return or_(
        InventoryMonitoringUploadBatch.is_superseded.is_(False),
        InventoryMonitoringUploadBatch.superseded_by_id == InventoryMonitoringUploadBatch.id,
        InventoryMonitoringUploadBatch.superseded_by_id.is_(None),
    )


def _repair_self_supersession() -> None:
    """Clear batches that were recorded as superseding themselves, so imports self-heal."""
    db.session.execute(text(
        "UPDATE inventory_monitoring_upload_batches SET is_superseded = 0, superseded_by_id = NULL "
        "WHERE id = superseded_by_id"
    ))


def import_workbook(source: bytes, filename: str, source_group: str, reporting_date: date | None, uploaded_by: int | None) -> InventoryMonitoringUploadBatch:
    review = validate_workbook(source, filename, source_group)
    _repair_self_supersession()
    checksum = hashlib.sha256(source).hexdigest()
    batch = InventoryMonitoringUploadBatch(source_group=source_group, reporting_date=reporting_date or review["reporting_date"], source_filename=filename, source_checksum=checksum, source_file_size=len(source), source_data=source, row_count=review["row_count"], accepted_count=review["accepted_count"], rejected_count=review["rejected_count"], duplicate_count=review["duplicate_count"], warnings_json=json.dumps(review["warnings"]), validation_json=json.dumps(review, default=str), uploaded_by=uploaded_by)
    db.session.add(batch); db.session.flush()
    if source_group == "mapping":
        # The workbook is the work-centre directory and the DFS / ST unit split. It no longer
        # declares which material may be held where: an inventory import maps what is held.
        directory = _read_mapping_directory(source)
        for row in directory:
            _get_work_center(row["work_center_name"], row["zone"], row["work_center_type"])
        db.session.flush()
        workbook_batches = db.session.query(InventoryMonitoringUploadBatch.id).filter(
            InventoryMonitoringUploadBatch.source_group == "mapping"
        ).subquery()
        InventoryMonitoringWorkCenterMaterial.query.filter(
            InventoryMonitoringWorkCenterMaterial.mapping_batch_id.in_(db.session.query(workbook_batches.c.id))
        ).update({"is_current": False}, synchronize_session=False)
        validation = json.loads(batch.validation_json)
        validation["work_centres"] = len(directory)
        batch.validation_json = json.dumps(validation)
        return batch
    if batch.reporting_date is None:
        raise ValueError("Select the as-on reporting date before confirming this inventory import.")
    _, rows, _ = _read_inventory(source, source_group)
    previous = InventoryMonitoringUploadBatch.query.filter(
        InventoryMonitoringUploadBatch.source_group == source_group,
        InventoryMonitoringUploadBatch.reporting_date == batch.reporting_date,
        InventoryMonitoringUploadBatch.status == "imported",
        InventoryMonitoringUploadBatch.is_superseded.is_(False),
        InventoryMonitoringUploadBatch.id != batch.id,
    ).order_by(InventoryMonitoringUploadBatch.id.desc()).first()
    if previous:
        previous.is_superseded, previous.superseded_by_id = True, batch.id
        InventoryMonitoringSnapshot.query.filter_by(batch_id=previous.id).update({"is_published": False})
    snapshot = InventoryMonitoringSnapshot(reporting_date=batch.reporting_date, material_group=source_group, batch_id=batch.id)
    db.session.add(snapshot); db.session.flush()
    seen = set()
    held_pairs: set[tuple[int, int]] = set()
    for row in rows:
        key = (row["material_code"], normalize_name(row["work_center_name"]))
        if key in seen: continue
        seen.add(key)
        material = _get_material(row["material_code"], row["material_description"], source_group)
        centre = _get_work_center(row["work_center_name"])
        db.session.flush()
        held_pairs.add((centre.id, material.id))
        db.session.add(InventoryMonitoringRecord(snapshot_id=snapshot.id, batch_id=batch.id, material_id=material.id, work_center_id=centre.id, material_group=source_group, **row))
    # Every stock line the workbook reports maps its material to the work centre holding it.
    for work_center_id, material_id in sorted(held_pairs - _current_mapping_pairs()):
        db.session.add(InventoryMonitoringWorkCenterMaterial(work_center_id=work_center_id, material_id=material_id, mapping_batch_id=batch.id, is_current=True))
    db.session.flush(); backfill_missing_uom(); _create_exceptions(snapshot)
    for finding in _read_supporting_exceptions(source):
        centre = _get_work_center(finding["work_center_name"])
        material = _get_material(finding["material_code"], finding["material_description"], source_group)
        db.session.flush()
        db.session.add(InventoryMonitoringException(
            snapshot_id=snapshot.id, work_center_id=centre.id, material_id=material.id,
            exception_type=finding["exception_type"], severity="high",
            details=f"Imported from {finding['source_sheet']} (row {finding['source_row']}).",
            inventory_value_inr=finding["value"],
        ))
    other = InventoryMonitoringSnapshot.query.join(InventoryMonitoringUploadBatch).filter(InventoryMonitoringSnapshot.reporting_date == batch.reporting_date, InventoryMonitoringUploadBatch.source_group == ("10" if source_group == "09" else "09"), InventoryMonitoringUploadBatch.is_superseded.is_(False)).first()
    snapshot.is_published = other is not None
    if other: other.is_published = True
    return batch


def landing_data() -> dict[str, Any]:
    latest_date = db.session.query(func.max(InventoryMonitoringSnapshot.reporting_date)).filter_by(is_published=True).scalar()
    records = InventoryMonitoringRecord.query.join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == latest_date, InventoryMonitoringSnapshot.is_published.is_(True)) if latest_date else InventoryMonitoringRecord.query.filter(False)
    total = records.with_entities(func.coalesce(func.sum(InventoryMonitoringRecord.inventory_value_inr), 0)).scalar() if latest_date else 0
    centres = InventoryMonitoringWorkCenter.query.order_by(InventoryMonitoringWorkCenter.zone, InventoryMonitoringWorkCenter.name).all()
    exception_counts = dict(db.session.query(InventoryMonitoringException.work_center_id, func.count(InventoryMonitoringException.id)).join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == latest_date).group_by(InventoryMonitoringException.work_center_id).all()) if latest_date else {}
    latest_mapping_batch = InventoryMonitoringUploadBatch.query.filter_by(source_group="mapping").order_by(
        InventoryMonitoringUploadBatch.id.desc()
    ).first()
    centres_by_name = {centre.normalized_name: centre for centre in centres}
    grouped_directory: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    if latest_mapping_batch is not None:
        for row in _read_mapping_directory(latest_mapping_batch.source_data):
            centre = centres_by_name.get(normalize_name(row["work_center_name"]))
            grouped_directory[row["zone"] or "Unassigned zone"][row["work_center_name"]].append({
                "id": centre.id if centre else None,
                "name": row["work_center_name"],
                "work_center_type": row["work_center_type"] or "Work centre",
                "exception_count": exception_counts.get(centre.id, 0) if centre else 0,
            })
    directory = [
        {
            "zone": zone,
            "assets": [
                {
                    "name": asset,
                    "work_centres": sorted(
                        entries,
                        key=lambda item: ({"DFS": 0, "ST": 1}.get(item["work_center_type"], 2), item["work_center_type"].casefold()),
                    ),
                }
                for asset, entries in sorted(assets.items(), key=lambda item: item[0].casefold())
            ],
        }
        for zone, assets in sorted(grouped_directory.items(), key=lambda item: item[0].casefold())
    ]
    map_assets = [
        {
            "id": next((entry["id"] for entry in asset["work_centres"] if entry["id"]), None),
            "name": asset["name"],
            "zone": zone["zone"],
            "units": [entry["work_center_type"] for entry in asset["work_centres"]],
        }
        for zone in directory
        for asset in zone["assets"]
    ]
    return {
        "reporting_date": latest_date,
        "total_inventory": total or 0,
        "work_centers": centres,
        "material_categories": monitored_material_categories(latest_date),
        "map_assets": map_assets,
        "mapped_work_center_count": sum(len(asset["work_centres"]) for zone in directory for asset in zone["assets"]),
        "exception_counts": exception_counts,
        "latest_batches": InventoryMonitoringUploadBatch.query.order_by(InventoryMonitoringUploadBatch.uploaded_at.desc()).limit(6).all(),
    }


HEALTH_MIX_LABELS = {
    "critical_low_stock": "Critical low",
    "low_stock": "Low stock",
    "healthy_stock": "Healthy",
    "slow_moving_stock": "Slow-moving",
    "excess_stock": "Excess",
    "unclassified": "Unclassified",
}


def _health_category(months: Decimal | None, thresholds: dict[str, Decimal]) -> str:
    """Same banding as inventory_health_data, shared by the portfolio review."""
    if months is None:
        return "unclassified"
    if months <= thresholds["critical_low_stock_months"]:
        return "critical_low_stock"
    if months <= thresholds["low_stock_months"]:
        return "low_stock"
    if months < thresholds["slow_moving_months"]:
        return "healthy_stock"
    if months < thresholds["excess_stock_months"]:
        return "slow_moving_stock"
    return "excess_stock"


def _share(part: Decimal, whole: Decimal) -> float:
    return round(float(part) / float(whole) * 100, 1) if whole else 0.0


def pending_periods() -> list[dict[str, Any]]:
    """Imported as-on dates that cannot be published yet because the other material group is missing."""
    groups_by_date: dict[date, set[str]] = defaultdict(set)
    published: set[date] = set()
    for as_on, group, is_published in db.session.query(
        InventoryMonitoringSnapshot.reporting_date, InventoryMonitoringSnapshot.material_group, InventoryMonitoringSnapshot.is_published
    ).join(InventoryMonitoringUploadBatch, InventoryMonitoringSnapshot.batch_id == InventoryMonitoringUploadBatch.id).filter(
        _live_batch()
    ).all():
        groups_by_date[as_on].add(group)
        if is_published:
            published.add(as_on)
    return [
        {"reporting_date": as_on, "imported": sorted(groups), "awaiting": sorted({"09", "10"} - groups)}
        for as_on, groups in sorted(groups_by_date.items(), reverse=True)
        if as_on not in published and {"09", "10"} - groups
    ]


def portfolio_data(reporting_date: date | None = None, compare_date: date | None = None) -> dict[str, Any]:
    """Executive review dataset for one published reporting period, compared like-for-like with an earlier one."""
    published_dates = [row[0] for row in db.session.query(InventoryMonitoringSnapshot.reporting_date).filter_by(is_published=True).distinct().order_by(InventoryMonitoringSnapshot.reporting_date.desc()).all()]
    selected = reporting_date or (published_dates[0] if published_dates else None)
    earlier_dates = [item for item in published_dates if selected and item < selected]
    previous = compare_date if compare_date in earlier_dates else (earlier_dates[0] if earlier_dates else None)
    empty = {
        "reporting_date": selected, "previous_date": previous, "available_dates": published_dates,
        "comparison_dates": earlier_dates, "comparison": None, "pending_periods": pending_periods(),
        "kpis": None, "health_mix": [], "zones": [], "centres": [], "movers": {"up": [], "down": []},
        "entrants": [], "exits": [],
        "top_materials": [], "exception_severities": {}, "exception_types": [], "exceptions": [],
    }
    if selected is None:
        return empty

    def _rows(as_on: date):
        return db.session.query(
            InventoryMonitoringRecord.work_center_id, InventoryMonitoringWorkCenter.name, InventoryMonitoringWorkCenter.zone,
            InventoryMonitoringRecord.material_group, InventoryMonitoringRecord.material_id, InventoryMonitoringRecord.material_code,
            InventoryMonitoringRecord.material_description, InventoryMonitoringRecord.inventory_value_inr, InventoryMonitoringRecord.stock_months,
        ).join(InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id).outerjoin(
            InventoryMonitoringWorkCenter, InventoryMonitoringRecord.work_center_id == InventoryMonitoringWorkCenter.id
        ).filter(InventoryMonitoringSnapshot.reporting_date == as_on, InventoryMonitoringSnapshot.is_published.is_(True)).all()

    rows = _rows(selected)
    if not rows:
        return empty
    thresholds = _thresholds()

    total = Decimal("0")
    zone_value: dict[str, Decimal] = defaultdict(Decimal)
    centre_value: dict[int, Decimal] = defaultdict(Decimal)
    centre_meta: dict[int, tuple[str, str | None]] = {}
    material_value: dict[str, Decimal] = defaultdict(Decimal)
    material_meta: dict[str, tuple[str | None, str]] = {}
    material_centres: dict[str, set] = defaultdict(set)
    mix_value: dict[str, Decimal] = defaultdict(Decimal)
    mix_count: dict[str, int] = defaultdict(int)
    for wc_id, wc_name, wc_zone, grp, _mat_id, code, desc, value, months in rows:
        value = value or Decimal("0")
        total += value
        zone_value[wc_zone or "Unassigned"] += value
        if wc_id is not None:
            centre_value[wc_id] += value
            centre_meta[wc_id] = (wc_name, wc_zone)
        material_value[code] += value
        material_meta[code] = (desc, grp)
        if wc_id is not None:
            material_centres[code].add(wc_id)
        category = _health_category(months, thresholds)
        mix_value[category] += value
        mix_count[category] += 1

    prev_total = prev_centre = prev_zone = None
    prev_centre_meta: dict[int, tuple[str, str | None]] = {}
    if previous:
        prev_total = Decimal("0")
        prev_centre = defaultdict(Decimal)
        prev_zone = defaultdict(Decimal)
        prev_mix = defaultdict(Decimal)
        for wc_id, wc_name, wc_zone, _g, _m, _c, _d, value, months in _rows(previous):
            value = value or Decimal("0")
            prev_total += value
            prev_zone[wc_zone or "Unassigned"] += value
            if wc_id is not None:
                prev_centre[wc_id] += value
                prev_centre_meta[wc_id] = (wc_name, wc_zone)
            prev_mix[_health_category(months, thresholds)] += value

    at_risk = mix_value["slow_moving_stock"] + mix_value["excess_stock"]
    stockout = mix_value["critical_low_stock"] + mix_value["low_stock"]
    prev_at_risk = (prev_mix["slow_moving_stock"] + prev_mix["excess_stock"]) if previous else None
    prev_stockout = (prev_mix["critical_low_stock"] + prev_mix["low_stock"]) if previous else None

    exception_query = InventoryMonitoringException.query.join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == selected)
    severity_counts = dict(db.session.query(InventoryMonitoringException.severity, func.count()).join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == selected).group_by(InventoryMonitoringException.severity).all())
    type_summary = [
        {"type": kind, "count": count, "value": value or Decimal("0")}
        for kind, count, value in db.session.query(
            InventoryMonitoringException.exception_type, func.count(), func.sum(InventoryMonitoringException.inventory_value_inr)
        ).join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == selected).group_by(InventoryMonitoringException.exception_type).order_by(func.sum(InventoryMonitoringException.inventory_value_inr).desc()).all()
    ]
    exception_total = sum(severity_counts.values())
    prev_exception_total = None
    if previous:
        prev_exception_total = db.session.query(func.count(InventoryMonitoringException.id)).join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == previous).scalar() or 0
    centre_exceptions = dict(db.session.query(InventoryMonitoringException.work_center_id, func.count()).join(InventoryMonitoringSnapshot).filter(InventoryMonitoringSnapshot.reporting_date == selected).group_by(InventoryMonitoringException.work_center_id).all())
    top_exceptions = exception_query.order_by(InventoryMonitoringException.inventory_value_inr.desc()).limit(15).all()

    centres_ranked = sorted(centre_value.items(), key=lambda item: item[1], reverse=True)
    centres = [
        {
            "id": wc_id, "name": centre_meta[wc_id][0], "zone": centre_meta[wc_id][1],
            "value": value, "share": _share(value, total),
            "prev": (prev_centre.get(wc_id) if previous else None),
            "exceptions": centre_exceptions.get(wc_id, 0),
        }
        for wc_id, value in centres_ranked[:12]
    ]
    movers: dict[str, list[dict[str, Any]]] = {"up": [], "down": []}
    entrants: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    comparison = None
    if previous:
        common = set(centre_value) & set(prev_centre)
        deltas = [
            {"name": centre_meta[wc_id][0], "zone": centre_meta[wc_id][1],
             "value": centre_value[wc_id], "prev": prev_centre[wc_id], "delta": centre_value[wc_id] - prev_centre[wc_id]}
            for wc_id in common
        ]
        movers["up"] = sorted((item for item in deltas if item["delta"] > 0), key=lambda item: item["delta"], reverse=True)[:4]
        movers["down"] = sorted((item for item in deltas if item["delta"] < 0), key=lambda item: item["delta"])[:4]
        entrants = sorted(
            ({"name": centre_meta[wc_id][0], "zone": centre_meta[wc_id][1], "value": centre_value[wc_id]} for wc_id in set(centre_value) - common),
            key=lambda item: item["value"], reverse=True,
        )
        exits = sorted(
            ({"name": prev_centre_meta.get(wc_id, ("Unknown work centre", None))[0], "zone": prev_centre_meta.get(wc_id, (None, None))[1], "value": prev_centre[wc_id]} for wc_id in set(prev_centre) - common),
            key=lambda item: item["value"], reverse=True,
        )
        like_for_like = sum((centre_value[wc_id] for wc_id in common), Decimal("0"))
        like_for_like_prev = sum((prev_centre[wc_id] for wc_id in common), Decimal("0"))
        comparison = {
            "previous_date": previous, "gap_days": (selected - previous).days,
            "common_centres": len(common),
            "entrant_count": len(entrants), "entrant_value": sum((item["value"] for item in entrants), Decimal("0")),
            "exit_count": len(exits), "exit_value": sum((item["value"] for item in exits), Decimal("0")),
            "like_for_like": like_for_like, "like_for_like_prev": like_for_like_prev,
            "like_for_like_delta": like_for_like - like_for_like_prev,
            "like_for_like_change": _share(like_for_like - like_for_like_prev, like_for_like_prev) if like_for_like_prev else None,
            "is_default": previous == (earlier_dates[0] if earlier_dates else None),
        }

    return {
        "reporting_date": selected,
        "previous_date": previous,
        "available_dates": published_dates,
        "comparison_dates": earlier_dates,
        "comparison": comparison,
        "pending_periods": pending_periods(),
        "entrants": entrants,
        "exits": exits,
        "kpis": {
            "total_value": total, "prev_total_value": prev_total,
            "value_at_risk": at_risk, "prev_value_at_risk": prev_at_risk, "at_risk_share": _share(at_risk, total),
            "stockout_value": stockout, "prev_stockout_value": prev_stockout, "stockout_share": _share(stockout, total),
            "centre_count": len(centre_value), "material_count": len(material_value), "record_count": len(rows),
            "top5_share": _share(sum((v for _w, v in centres_ranked[:5]), Decimal("0")), total),
            "exception_total": exception_total, "prev_exception_total": prev_exception_total,
            "critical_exceptions": severity_counts.get("critical", 0),
        },
        "health_mix": [
            {"key": key, "label": label, "value": mix_value[key], "count": mix_count[key], "share": _share(mix_value[key], total)}
            for key, label in HEALTH_MIX_LABELS.items() if mix_count[key]
        ],
        "zones": sorted(
            [
                {"zone": zone, "value": value, "share": _share(value, total), "prev": (prev_zone.get(zone) if previous else None)}
                for zone, value in zone_value.items()
            ],
            key=lambda item: item["value"], reverse=True,
        ),
        "centres": centres,
        "movers": movers,
        "top_materials": [
            {"code": code, "description": material_meta[code][0], "group": material_meta[code][1], "value": value, "share": _share(value, total), "centres": len(material_centres[code])}
            for code, value in sorted(material_value.items(), key=lambda item: item[1], reverse=True)[:10]
        ],
        "exception_severities": severity_counts,
        "exception_types": type_summary,
        "exceptions": top_exceptions,
    }


MANAGEMENT_REGISTER_ROW_LIMIT = 250
HEALTH_REGISTER_GROUP_LIMIT = 25  # rows shown per specification category on the health register
MANAGEMENT_REGISTER_GROUP_LIMIT = 40  # rows shown per specification category in a deck register
HIGH_VALUE_MATERIAL_FLOOR = Decimal("10000000")  # a material is reviewed individually above one crore of value
COVERAGE_REGISTERS = (
    ("critical_low_stock", "Critical low stock", "Coverage at or below the critical low-stock threshold; supply action is required."),
    ("low_stock", "Low stock", "Coverage below the low-stock threshold but above the critical band."),
    ("slow_moving_stock", "Slow-moving stock", "Coverage at or above the slow-moving threshold and below the excess threshold."),
    ("excess_stock", "Excess stock", "Coverage at or above the excess-stock threshold; working capital is locked up."),
)
SUPPORTING_REGISTERS = (
    ("non_moving", "Non-moving materials", "Reported as non-moving in the source workbook for this reporting date."),
    ("slow_moving", "Slow-moving materials (source register)", "Reported as slow-moving in the source workbook for this reporting date."),
    ("aged_stock_over_one_year", "Stock lying over one year", "Inventory lying in stock for more than one year."),
    ("surplus", "Items lying in surplus", "Items reported in the surplus register of the source workbook."),
    ("material_in_transit_aged", "Material in transit, open cases", "Open material-in-transit cases awaiting closure."),
)


def _management_register(key: str, label: str, description: str, lines: list[dict[str, Any]], index: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """One review register in specification order — DFC / 01, DFC / 02, then PC / 01 — under category headings.

    Each category is capped so a single deck stays presentable, and every category keeps a
    place on the slide rather than the tail being cut off by a flat row limit.
    """
    groups = specification_groups(lines, index, code_of=lambda row: row.get("code", ""), limit=MANAGEMENT_REGISTER_GROUP_LIMIT)
    rows = [{**row, "section": group["label"]} for group in groups for row in group["rows"]]
    return {
        "key": key, "label": label, "description": description,
        "count": sum(group["total"] for group in groups),
        "value": sum((item["value"] or Decimal("0") for item in lines), Decimal("0")),
        "rows": rows, "groups": groups,
        "omitted": sum(group["omitted"] for group in groups),
    }


def management_review_data(reporting_date: date | None = None, compare_date: date | None = None) -> dict[str, Any]:
    """Portfolio headline plus the complete registers management reviews for one published period."""
    base = portfolio_data(reporting_date, compare_date)
    thresholds = _thresholds()
    payload: dict[str, Any] = {
        **base, "thresholds": thresholds, "high_value_floor": HIGH_VALUE_MATERIAL_FLOOR,
        "row_limit": MANAGEMENT_REGISTER_ROW_LIMIT, "group_limit": MANAGEMENT_REGISTER_GROUP_LIMIT,
        "high_value_materials": [], "high_value_total": Decimal("0"),
        "centres_ranked": [], "coverage_registers": [], "supporting_registers": [],
    }
    selected = base["reporting_date"]
    if selected is None or base["kpis"] is None:
        return payload
    total = base["kpis"]["total_value"] or Decimal("0")

    lines = db.session.query(
        InventoryMonitoringRecord.material_group, InventoryMonitoringRecord.material_code,
        InventoryMonitoringRecord.material_description, InventoryMonitoringWorkCenter.name,
        InventoryMonitoringWorkCenter.zone, InventoryMonitoringRecord.stock_qty, InventoryMonitoringRecord.uom,
        InventoryMonitoringRecord.inventory_value_inr, InventoryMonitoringRecord.open_po,
        InventoryMonitoringRecord.open_pr, InventoryMonitoringRecord.stock_months,
    ).join(InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id).outerjoin(
        InventoryMonitoringWorkCenter, InventoryMonitoringRecord.work_center_id == InventoryMonitoringWorkCenter.id
    ).filter(InventoryMonitoringSnapshot.reporting_date == selected, InventoryMonitoringSnapshot.is_published.is_(True)).all()

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    open_supply: list[dict[str, Any]] = []
    materials: dict[str, dict[str, Any]] = {}
    material_centres: dict[str, set] = defaultdict(set)
    centre_value: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for group, code, description, centre, zone, qty, uom, value, open_po, open_pr, months in lines:
        centre_name, zone_name = centre or "Unmapped work centre", zone or "Unassigned"
        line = {
            "group": group, "code": code, "description": description, "centre": centre_name, "zone": zone_name,
            "qty": qty, "uom": uom, "value": value or Decimal("0"), "open_po": open_po, "open_pr": open_pr,
            "months": months,
        }
        buckets[_health_category(months, thresholds)].append(line)
        centre_value[(centre_name, zone_name)] += line["value"]
        if months is not None and months >= thresholds["slow_moving_months"] and ((open_po or 0) > 0 or (open_pr or 0) > 0):
            open_supply.append(line)
        material = materials.setdefault(code, {"code": code, "description": description, "group": group, "value": Decimal("0"), "months_low": None, "months_high": None})
        material["description"] = material["description"] or description
        material["value"] += line["value"]
        material_centres[code].add(centre_name)
        if months is not None:
            material["months_low"] = months if material["months_low"] is None else min(material["months_low"], months)
            material["months_high"] = months if material["months_high"] is None else max(material["months_high"], months)

    high_value = [item for item in materials.values() if item["value"] >= HIGH_VALUE_MATERIAL_FLOOR]
    payload["high_value_materials"] = [
        {**item, "centres": len(material_centres[item["code"]]), "share": _share(item["value"], total), "section": group["label"]}
        for group in specification_groups(high_value, code_of=lambda row: row["code"])
        for item in group["rows"]
    ]
    payload["high_value_total"] = sum((item["value"] for item in high_value), Decimal("0"))
    payload["centres_ranked"] = [
        {"name": name, "zone": zone, "value": value, "share": _share(value, total)}
        for (name, zone), value in sorted(centre_value.items(), key=lambda item: item[1], reverse=True)
    ]
    spec_index = specification_index()
    payload["coverage_registers"] = [_management_register(key, label, description, buckets[key], spec_index) for key, label, description in COVERAGE_REGISTERS]
    payload["coverage_registers"].append(_management_register(
        "open_supply_with_high_stock", "Open PO / PR against high stock",
        "Open purchase orders or requisitions where coverage is already at or above the slow-moving threshold.",
        open_supply, spec_index,
    ))

    supporting: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for kind, code, description, group, centre, zone, value, months, details in db.session.query(
        InventoryMonitoringException.exception_type, InventoryMonitoringMaterial.material_code,
        InventoryMonitoringMaterial.description, InventoryMonitoringMaterial.material_group,
        InventoryMonitoringWorkCenter.name, InventoryMonitoringWorkCenter.zone,
        InventoryMonitoringException.inventory_value_inr, InventoryMonitoringException.stock_months,
        InventoryMonitoringException.details,
    ).join(InventoryMonitoringSnapshot, InventoryMonitoringException.snapshot_id == InventoryMonitoringSnapshot.id).outerjoin(
        InventoryMonitoringMaterial, InventoryMonitoringException.material_id == InventoryMonitoringMaterial.id
    ).outerjoin(
        InventoryMonitoringWorkCenter, InventoryMonitoringException.work_center_id == InventoryMonitoringWorkCenter.id
    ).filter(
        InventoryMonitoringSnapshot.reporting_date == selected,
        InventoryMonitoringSnapshot.is_published.is_(True),
        InventoryMonitoringException.exception_type.in_([key for key, _label, _description in SUPPORTING_REGISTERS]),
    ).all():
        supporting[kind].append({
            "group": group, "code": code or "—", "description": description, "centre": centre or "Unmapped work centre",
            "zone": zone or "Unassigned", "value": value or Decimal("0"), "months": months, "details": details,
        })
    payload["supporting_registers"] = [_management_register(key, label, description, supporting[key], spec_index) for key, label, description in SUPPORTING_REGISTERS]
    return payload


def inventory_health_data() -> dict[str, Any]:
    """Return a plain-language health register for the latest active snapshot date."""
    thresholds = _thresholds()
    latest_date = db.session.query(func.max(InventoryMonitoringSnapshot.reporting_date)).join(
        InventoryMonitoringUploadBatch
    ).filter(_live_batch()).scalar()
    if latest_date is None:
        return {"reporting_date": None, "thresholds": thresholds, "groups": {}, "source_findings": {}}
    records = InventoryMonitoringRecord.query.join(InventoryMonitoringSnapshot).join(
        InventoryMonitoringUploadBatch, InventoryMonitoringSnapshot.batch_id == InventoryMonitoringUploadBatch.id
    ).filter(
        InventoryMonitoringSnapshot.reporting_date == latest_date,
        _live_batch(),
    ).order_by(InventoryMonitoringRecord.inventory_value_inr.desc()).all()
    groups = {key: [] for key in ("critical_low_stock", "low_stock", "healthy_stock", "slow_moving_stock", "excess_stock", "unclassified")}
    for record in records:
        months = record.stock_months
        if months is None:
            category = "unclassified"
        elif months <= thresholds["critical_low_stock_months"]:
            category = "critical_low_stock"
        elif months <= thresholds["low_stock_months"]:
            category = "low_stock"
        elif months < thresholds["slow_moving_months"]:
            category = "healthy_stock"
        elif months < thresholds["excess_stock_months"]:
            category = "slow_moving_stock"
        else:
            category = "excess_stock"
        groups[category].append(record)
    source_findings: dict[str, list[InventoryMonitoringException]] = {}
    for kind in ("slow_moving", "non_moving", "aged_stock_over_one_year", "surplus", "material_in_transit_aged"):
        source_findings[kind] = InventoryMonitoringException.query.join(InventoryMonitoringSnapshot).filter(
            InventoryMonitoringSnapshot.reporting_date == latest_date,
            InventoryMonitoringException.exception_type == kind,
        ).order_by(InventoryMonitoringException.inventory_value_inr.desc()).all()
    index = specification_index()
    return {
        "reporting_date": latest_date, "thresholds": thresholds, "groups": groups, "source_findings": source_findings,
        "spec_groups": {key: specification_groups(rows, index, limit=HEALTH_REGISTER_GROUP_LIMIT) for key, rows in groups.items()},
        "source_spec_groups": {
            key: specification_groups(rows, index, code_of=lambda item: item.material.material_code if item.material else "")
            for key, rows in source_findings.items()
        },
    }


def _centre_units(centre: InventoryMonitoringWorkCenter, unit: str | None) -> tuple[str | None, list[str], set[str] | None]:
    """Mapped units available for one asset, and the material codes of the selected unit."""
    selected_unit = (unit or "").strip() or None
    available_units: list[str] = []
    selected_unit_codes: set[str] | None = None
    latest_mapping_batch = InventoryMonitoringUploadBatch.query.filter_by(source_group="mapping").order_by(
        InventoryMonitoringUploadBatch.id.desc()
    ).first()
    if latest_mapping_batch is not None:
        # The directory retains configured DFS/ST units even when no material
        # codes are assigned yet.  Keep those units selectable so the page can
        # explain the empty state instead of treating the unit as missing.
        directory_rows = _read_mapping_directory(latest_mapping_batch.source_data)
        directory_asset_rows = [
            row for row in directory_rows
            if normalize_name(row["work_center_name"]) == centre.normalized_name
        ]
        mapping_rows, _ = _read_mapping(latest_mapping_batch.source_data)
        mapped_asset_rows = [
            row for row in mapping_rows
            if normalize_name(row["work_center_name"]) == centre.normalized_name
        ]
        available_units = sorted(
            {row["work_center_type"] for row in directory_asset_rows if row["work_center_type"]}
            or {row["work_center_type"] for row in mapped_asset_rows if row["work_center_type"]},
            key=lambda value: ({"DFS": 0, "ST": 1}.get(value, 2), value.casefold()),
        )
        if selected_unit:
            if selected_unit not in available_units:
                raise ValueError("That mapped unit is not available for this asset.")
            selected_unit_codes = {
                row["material_code"]
                for row in mapped_asset_rows
                if row["work_center_type"] == selected_unit
            }
    return selected_unit, available_units, selected_unit_codes


def _centre_records(centre: InventoryMonitoringWorkCenter, unit_codes: set[str] | None, as_on: date | None = None) -> list[InventoryMonitoringRecord]:
    """Stock lines for one work centre: one reporting date, or the latest snapshot per material group."""
    query = InventoryMonitoringRecord.query.join(
        InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
    ).join(
        InventoryMonitoringUploadBatch, InventoryMonitoringSnapshot.batch_id == InventoryMonitoringUploadBatch.id
    ).filter(
        InventoryMonitoringRecord.work_center_id == centre.id,
        _live_batch(),
    )
    if as_on is not None:
        query = query.filter(InventoryMonitoringSnapshot.reporting_date == as_on)
    # Newest reporting date first, so a back-dated import never displaces a later one.
    records = query.order_by(InventoryMonitoringSnapshot.reporting_date.desc(), InventoryMonitoringRecord.id.desc()).all()
    if as_on is None:
        latest_snapshot_by_group: dict[str, int] = {}
        for record in records:
            latest_snapshot_by_group.setdefault(record.material_group, record.snapshot_id)
        records = [record for record in records if latest_snapshot_by_group.get(record.material_group) == record.snapshot_id]
    return [record for record in records if unit_codes is None or record.material_code in unit_codes]


def _band_records(records: list[InventoryMonitoringRecord], thresholds: dict[str, Decimal]) -> dict[str, list[InventoryMonitoringRecord]]:
    groups: dict[str, list[InventoryMonitoringRecord]] = {key: [] for key in HEALTH_MIX_LABELS}
    for record in records:
        groups[_health_category(record.stock_months, thresholds)].append(record)
    return groups


def _centre_source_findings(centre: InventoryMonitoringWorkCenter, unit_codes: set[str] | None) -> list[InventoryMonitoringException]:
    return [
        item for item in InventoryMonitoringException.query.filter_by(work_center_id=centre.id).filter(
            InventoryMonitoringException.exception_type.in_([key for key, _label, _description in SUPPORTING_REGISTERS])
        ).order_by(InventoryMonitoringException.id.desc()).all()
        if unit_codes is None or (item.material and item.material.material_code in unit_codes)
    ]


def _value_of(records: list[InventoryMonitoringRecord]) -> Decimal:
    return sum((record.inventory_value_inr or Decimal("0") for record in records), Decimal("0"))


def work_center_health_data(work_center_id: int, unit: str | None = None) -> dict[str, Any]:
    """Inventory Health view for one work centre, using its latest available records."""
    centre = db.session.get(InventoryMonitoringWorkCenter, work_center_id)
    if centre is None:
        raise ValueError("Work centre was not found.")
    thresholds = _thresholds()
    selected_unit, available_units, unit_codes = _centre_units(centre, unit)
    records = _centre_records(centre, unit_codes)
    return {
        "centre": centre, "thresholds": thresholds, "groups": _band_records(records, thresholds),
        "source_findings": _centre_source_findings(centre, unit_codes),
        "selected_unit": selected_unit, "available_units": available_units,
    }


def work_center_review_data(work_center_id: int, unit: str | None = None, compare_date: date | None = None) -> dict[str, Any]:
    """Management review for one work centre: headline, coverage mix, registers and like-for-like movement."""
    centre = db.session.get(InventoryMonitoringWorkCenter, work_center_id)
    if centre is None:
        raise ValueError("Work centre was not found.")
    thresholds = _thresholds()
    selected_unit, available_units, unit_codes = _centre_units(centre, unit)
    records = _centre_records(centre, unit_codes)
    groups = _band_records(records, thresholds)
    source_findings = _centre_source_findings(centre, unit_codes)

    snapshot_dates = dict(db.session.query(InventoryMonitoringSnapshot.id, InventoryMonitoringSnapshot.reporting_date).filter(
        InventoryMonitoringSnapshot.id.in_({record.snapshot_id for record in records} or {0})
    ).all())
    as_on_by_group: dict[str, date | None] = {}
    for record in records:
        as_on_by_group.setdefault(record.material_group, snapshot_dates.get(record.snapshot_id))
    reporting_date = max((value for value in as_on_by_group.values() if value), default=None)
    centre_dates = [
        row[0] for row in db.session.query(InventoryMonitoringSnapshot.reporting_date).join(
            InventoryMonitoringRecord, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
        ).join(
            InventoryMonitoringUploadBatch, InventoryMonitoringSnapshot.batch_id == InventoryMonitoringUploadBatch.id
        ).filter(
            InventoryMonitoringRecord.work_center_id == centre.id,
            _live_batch(),
        ).distinct().order_by(InventoryMonitoringSnapshot.reporting_date.desc()).all()
    ]
    earlier_dates = [item for item in centre_dates if reporting_date and item < reporting_date]
    previous = compare_date if compare_date in earlier_dates else (earlier_dates[0] if earlier_dates else None)

    def _by_material(items: list[InventoryMonitoringRecord]):
        values: dict[str, Decimal] = defaultdict(Decimal)
        meta: dict[str, tuple[str | None, str]] = {}
        for record in items:
            values[record.material_code] += record.inventory_value_inr or Decimal("0")
            meta.setdefault(record.material_code, (record.material_description, record.material_group))
        return values, meta

    total = _value_of(records)
    band_value = {key: _value_of(rows) for key, rows in groups.items()}
    material_value, material_meta = _by_material(records)

    portfolio_total = Decimal("0")
    if records:
        portfolio_total = db.session.query(
            func.coalesce(func.sum(InventoryMonitoringRecord.inventory_value_inr), 0)
        ).filter(InventoryMonitoringRecord.snapshot_id.in_({record.snapshot_id for record in records})).scalar() or Decimal("0")

    comparison = None
    prior_band: dict[str, Decimal] = {}
    prior_total = None
    movers: dict[str, list[dict[str, Any]]] = {"up": [], "down": []}
    entrants: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    if previous:
        prior_records = _centre_records(centre, unit_codes, as_on=previous)
        prior_value, prior_meta = _by_material(prior_records)
        prior_total = _value_of(prior_records)
        prior_band = {key: _value_of(rows) for key, rows in _band_records(prior_records, thresholds).items()}
        common = set(material_value) & set(prior_value)
        deltas = [
            {"code": code, "description": material_meta[code][0], "group": material_meta[code][1],
             "value": material_value[code], "prev": prior_value[code], "delta": material_value[code] - prior_value[code]}
            for code in common
        ]
        movers["up"] = sorted((item for item in deltas if item["delta"] > 0), key=lambda item: item["delta"], reverse=True)[:5]
        movers["down"] = sorted((item for item in deltas if item["delta"] < 0), key=lambda item: item["delta"])[:5]
        entrants = sorted(
            ({"code": code, "description": material_meta[code][0], "group": material_meta[code][1], "value": material_value[code]} for code in set(material_value) - common),
            key=lambda item: item["value"], reverse=True,
        )
        exits = sorted(
            ({"code": code, "description": prior_meta[code][0], "group": prior_meta[code][1], "value": prior_value[code]} for code in set(prior_value) - common),
            key=lambda item: item["value"], reverse=True,
        )
        like_for_like = sum((material_value[code] for code in common), Decimal("0"))
        like_for_like_prev = sum((prior_value[code] for code in common), Decimal("0"))
        comparison = {
            "previous_date": previous, "gap_days": (reporting_date - previous).days,
            "common_materials": len(common),
            "entrant_count": len(entrants), "entrant_value": sum((item["value"] for item in entrants), Decimal("0")),
            "exit_count": len(exits), "exit_value": sum((item["value"] for item in exits), Decimal("0")),
            "like_for_like": like_for_like, "like_for_like_prev": like_for_like_prev,
            "like_for_like_delta": like_for_like - like_for_like_prev,
            "like_for_like_change": _share(like_for_like - like_for_like_prev, like_for_like_prev) if like_for_like_prev else None,
        }

    at_risk = band_value["slow_moving_stock"] + band_value["excess_stock"]
    stockout = band_value["critical_low_stock"] + band_value["low_stock"]
    source_summary: dict[str, dict[str, Any]] = {
        key: {"key": key, "label": label, "count": 0, "value": Decimal("0")}
        for key, label, _description in SUPPORTING_REGISTERS
    }
    for item in source_findings:
        entry = source_summary[item.exception_type]
        entry["count"] += 1
        entry["value"] += item.inventory_value_inr or Decimal("0")

    return {
        "centre": centre, "thresholds": thresholds, "groups": groups, "source_findings": source_findings,
        "spec_groups": {key: specification_groups(rows) for key, rows in groups.items()},
        "source_spec_groups": specification_groups(source_findings, code_of=lambda item: item.material.material_code if item.material else ""),
        "selected_unit": selected_unit, "available_units": available_units,
        "reporting_date": reporting_date, "as_on_by_group": as_on_by_group,
        "previous_date": previous, "comparison_dates": earlier_dates, "comparison": comparison,
        "movers": movers, "entrants": entrants, "exits": exits,
        "kpis": {
            "total_value": total, "prev_total_value": prior_total,
            "value_at_risk": at_risk, "at_risk_share": _share(at_risk, total),
            "prev_value_at_risk": (prior_band["slow_moving_stock"] + prior_band["excess_stock"]) if previous else None,
            "stockout_value": stockout, "stockout_share": _share(stockout, total),
            "prev_stockout_value": (prior_band["critical_low_stock"] + prior_band["low_stock"]) if previous else None,
            "line_count": len(records), "material_count": len(material_value),
            "portfolio_value": portfolio_total, "portfolio_share": _share(total, portfolio_total),
            "source_case_count": len(source_findings),
            "source_case_value": sum((item.inventory_value_inr or Decimal("0") for item in source_findings), Decimal("0")),
        },
        "health_mix": [
            {"key": key, "label": label, "value": band_value[key], "count": len(groups[key]), "share": _share(band_value[key], total),
             "prev": prior_band.get(key) if previous else None}
            for key, label in HEALTH_MIX_LABELS.items() if groups[key]
        ],
        "top_materials": [
            {**item, "section": group["label"]}
            for group in specification_groups(
                [{"code": code, "description": material_meta[code][0], "group": material_meta[code][1],
                  "value": value, "share": _share(value, total)} for code, value in material_value.items()],
                code_of=lambda row: row["code"],
            )
            for item in group["rows"]
        ],
        "source_summary": [entry for entry in source_summary.values() if entry["count"]],
    }
