"""Workbook validation, imports and query helpers for Inventory Monitoring."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
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
    InventoryMonitoringException, InventoryMonitoringMaterial, InventoryMonitoringMaterialSummary,
    InventoryMonitoringPlantAlert, InventoryMonitoringRecord,
    InventoryMonitoringSnapshot, InventoryMonitoringThreshold, InventoryMonitoringUploadBatch,
    InventoryMonitoringWorkCenter, InventoryMonitoringWorkCenterMaterial,
)

STAGING_DIRECTORY = Path(tempfile.gettempdir()) / "inventory_monitoring_staging"
STAGING_DIRECTORY.mkdir(parents=True, exist_ok=True)
GROUP_SHEETS = {"09": "09 Oil well cement - Inventory", "10": "10 Chemi incl mud chemi - Inven"}
CORE_COLUMNS = {"materialcode", "materialdescription", "workcentre", "stockqty", "inventoryvalueinr", "stockmonths"}
# The Group 09/10 exports have carried no unit column; accept whatever SAP calls it when they do.
UOM_HEADERS = {"uom", "uoe", "unit", "units", "baseunit", "basicunit", "stockunit", "unitofentry", "unitofmeasure", "unitofmeasurement", "baseunitofmeasure", "bun"}
# The detailed inventory sheet reports a work-centre name today. If the export ever
# carries the SAP plant code as well, it is read from whichever of these it uses.
PLANT_HEADERS = {"plant", "plantcode", "sapplant", "sapplantcode", "werks", "plantcd"}

# A chemical is bought and consumed either by volume or by weight, and the two
# cannot be ranked against each other. Every unit the workbooks use is mapped to
# its phase and to a common unit within that phase, so a litre and a kilolitre
# rank on one scale. Units that count pieces belong to neither and are reported
# as such rather than being forced into one.
LIQUID_UNIT, SOLID_UNIT = "KL", "MT"
QUANTITY_UNITS: dict[str, tuple[str, Decimal]] = {
    # liquids, normalised to kilolitres
    "ML": ("liquid", Decimal("0.000001")), "L": ("liquid", Decimal("0.001")),
    "LT": ("liquid", Decimal("0.001")), "LTR": ("liquid", Decimal("0.001")),
    "LTRS": ("liquid", Decimal("0.001")), "LTS": ("liquid", Decimal("0.001")),
    "KL": ("liquid", Decimal("1")), "KLS": ("liquid", Decimal("1")),
    "M3": ("liquid", Decimal("1")), "CBM": ("liquid", Decimal("1")),
    "GAL": ("liquid", Decimal("0.0037854")), "BBL": ("liquid", Decimal("0.158987")),
    # solids, normalised to metric tonnes
    "G": ("solid", Decimal("0.000001")), "GM": ("solid", Decimal("0.000001")),
    "GMS": ("solid", Decimal("0.000001")), "KG": ("solid", Decimal("0.001")),
    "KGS": ("solid", Decimal("0.001")), "MT": ("solid", Decimal("1")),
    "TO": ("solid", Decimal("1")), "TON": ("solid", Decimal("1")),
    "TONNE": ("solid", Decimal("1")), "QTL": ("solid", Decimal("0.1")),
    "LB": ("solid", Decimal("0.00045359")), "LBS": ("solid", Decimal("0.00045359")),
}
PHASE_LABELS = {"liquid": "Liquids", "solid": "Solids", "other": "Counted items"}
PHASE_UNITS = {"liquid": LIQUID_UNIT, "solid": SOLID_UNIT, "other": ""}
# A material's consumption is reviewed line by line above this much value.
HIGH_CONSUMPTION_VALUE_FLOOR = Decimal("100000000")  # ₹ 10 Cr
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


def _text(value: Any) -> str:
    """A cell as text, with an empty cell reading empty.

    pandas hands back NaN for a blank cell, and NaN is truthy: ``str(value or "")``
    turned every blank description into "nan" and every blank unit into "NAN".
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _decimal(value: Any) -> Decimal | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).replace(",", ""))
    except Exception:
        return None


def phase_of(uom: Any) -> str:
    """Whether a unit measures volume, weight, or neither."""
    key = re.sub(r"[^A-Z0-9]", "", str(uom or "").upper())
    return QUANTITY_UNITS.get(key, ("other", Decimal("0")))[0]


def normalized_quantity(quantity: Any, uom: Any) -> Decimal | None:
    """A quantity restated in its phase's common unit — kilolitres or tonnes.

    ``None`` when the unit counts pieces, because there is nothing to convert to.
    """
    key = re.sub(r"[^A-Z0-9]", "", str(uom or "").upper())
    phase, factor = QUANTITY_UNITS.get(key, ("other", Decimal("0")))
    if phase == "other" or quantity is None:
        return None
    return Decimal(str(quantity)) * factor


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
    plant_column = next((column for key, column in canonical.items() if key in PLANT_HEADERS), None)
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    for index, raw in frame.iterrows():
        code = material_code(raw[canonical["materialcode"]])
        centre = _text(raw[canonical["workcentre"]])
        if not code or not centre:
            warnings.append(f"Row {index + 3}: missing material code or work centre.")
            continue
        stock_qty = _decimal(raw[canonical["stockqty"]])
        value = _decimal(raw[canonical["inventoryvalueinr"]])
        # SAP sometimes carries a physical balance before it has an inventory
        # value against it. A positive quantity is still a holding and must be
        # monitored; conversely, retain a positive-value holding if quantity is
        # absent or zero. Only a line with neither signal is not held stock.
        if not ((stock_qty is not None and stock_qty > 0) or (value is not None and value > 0)):
            warnings.append(
                f"Row {index + 3}: stock quantity and inventory value are both nil or non-positive, "
                "so the line is not monitored."
            )
            continue
        rows.append({
            "material_code": code, "material_description": _text(raw[canonical["materialdescription"]]) or None,
            "work_center_name": centre, "stock_qty": stock_qty,
            "plant_code": (_text(raw[plant_column]).upper() or None) if plant_column else None,
            "uom": (_text(raw[uom_column]).upper() or None) if uom_column else None,
            "inventory_value_inr": value,
            "open_po": _decimal(raw[canonical["openpo"]]) if canonical.get("openpo") else None,
            "open_pr": _decimal(raw[canonical["openpr"]]) if canonical.get("openpr") else None,
            "stock_months": _decimal(raw[canonical["stockmonths"]]), "source_row": int(index + 3), "source_sheet": sheet,
        })
    return sheet, rows, warnings


CRORE = Decimal("10000000")


def _read_material_summaries(source: bytes, source_group: str) -> list[dict[str, Any]]:
    """All-ONGC consumption and stock per material, from the workbook's summary sheets.

    Two sheets carry it and neither is the detailed inventory sheet: one reports
    stock quantity, its unit and the twelve-month consumption quantity; the other
    reports inventory and consumption value in crores. They are found by their
    columns rather than their titles, because each material group names its own
    sheets ("09 Oil well cement - Chemical S", "10 Chemi incl mud chemi - Che 1").
    """
    book = pd.ExcelFile(BytesIO(source))
    merged: dict[str, dict[str, Any]] = {}
    for sheet in book.sheet_names:
        frame = pd.read_excel(book, sheet_name=sheet, header=1, dtype=object)
        columns = {_header(column): column for column in frame.columns}
        code_column = columns.get("materialcode")
        if not code_column or columns.get("workcentre") or columns.get("workcenter"):
            continue  # not a material summary: it has no codes, or it is reported per work centre
        quantity_column = next((column for key, column in columns.items() if key.startswith("consumptionqty")), None)
        value_column = next((column for key, column in columns.items() if key.startswith("consumptionvalue")), None)
        if not quantity_column and not value_column:
            continue
        description_column = columns.get("materialdescription")
        months_column = columns.get("stockmonths")
        stock_column = columns.get("stockqty")
        uom_column = next((column for key, column in columns.items() if key in UOM_HEADERS), None)
        inventory_column = next((column for key, column in columns.items() if key.startswith("inventoryvalue")), None)
        for _index, raw in frame.iterrows():
            code = material_code(raw[code_column])
            if not code:
                continue
            entry = merged.setdefault(code, {"material_code": code, "material_group": source_group})
            if description_column and not entry.get("material_description"):
                entry["material_description"] = _text(raw[description_column]) or None
            if months_column and entry.get("stock_months") is None:
                entry["stock_months"] = _decimal(raw[months_column])
            if stock_column and entry.get("stock_qty") is None:
                entry["stock_qty"] = _decimal(raw[stock_column])
            if uom_column and not entry.get("uom"):
                entry["uom"] = _text(raw[uom_column]).upper() or None
            if quantity_column and entry.get("consumption_qty_12m") is None:
                entry["consumption_qty_12m"] = _decimal(raw[quantity_column])
            # The value sheets are stated in crores; store rupees like every other figure.
            if value_column and entry.get("consumption_value_inr") is None:
                value = _decimal(raw[value_column])
                entry["consumption_value_inr"] = None if value is None else value * CRORE
            if inventory_column and entry.get("inventory_value_inr") is None:
                value = _decimal(raw[inventory_column])
                entry["inventory_value_inr"] = None if value is None else value * CRORE
    return list(merged.values())


def _read_mapping(source: bytes) -> tuple[list[dict[str, Any]], list[str]]:
    frame = pd.read_excel(BytesIO(source), sheet_name=0, header=0, dtype=object)
    if frame.shape[1] < 5:
        raise ValueError("The mapping workbook must contain work centres and material-code columns.")
    mappings, warnings = [], []
    for index, raw in frame.iterrows():
        zone, centre, centre_type = (_text(raw.iloc[i]) for i in (1, 2, 3))
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
        zone, centre, centre_type = (_text(raw.iloc[i]) for i in (1, 2, 3))
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
            code, centre = material_code(raw[code_column]), _text(raw[centre_column])
            if code and centre:
                value = _decimal(raw[value_column]) if value_column else None
                value_key = _header(value_column) if value_column else ""
                if value is not None and ("crore" in value_key or value_key.endswith("cr")):
                    value *= Decimal("10000000")
                elif value is not None and "lakh" in value_key:
                    value *= Decimal("100000")
                findings.append({"exception_type": exception_type, "material_code": code, "material_description": (_text(raw[description_column]) or None) if description_column else None, "work_center_name": centre, "source_sheet": sheet, "source_row": int(index + 3), "value": value})
    return findings


def stage_workbook(source: bytes) -> str:
    # An abandoned review never reaches discard_staged_workbook, so sweep anything
    # past the one-hour window that load_staged_workbook already refuses to read.
    cutoff = time.time() - 3600
    for staged_file in STAGING_DIRECTORY.glob("*.xlsx"):
        if staged_file.stat().st_mtime < cutoff:
            staged_file.unlink(missing_ok=True)
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
    new_plants = unrecognised_plants(rows)
    return {
        "source_group": source_group, "reporting_date": _detect_reporting_date(filename),
        "row_count": len(rows) + len(warnings), "accepted_count": len(rows), "rejected_count": len(warnings),
        "duplicate_count": duplicate_count, "warnings": warnings[:20], "issue_samples": warnings[:10],
        "material_summary_count": len(_read_material_summaries(source, source_group)),
        # Shown before the import is confirmed: an unfamiliar plant is a decision for
        # the module admin, not a silent addition to the registers.
        "new_plants": [{**item, "value": float(item["value"])} for item in new_plants],
    }


def _get_material(code: str, description: str | None, group: str | None, uom: str | None = None) -> InventoryMonitoringMaterial:
    """The material row for one code, kept current with what the workbook states.

    The unit comes from the workbook's material summary sheet, which is the only
    sheet that carries one. Once stored it is what every table prints beside the
    code, and what decides whether the material is read as a liquid or a solid.
    """
    item = InventoryMonitoringMaterial.query.filter_by(material_code=code).first()
    if item is None:
        item = InventoryMonitoringMaterial(material_code=code); db.session.add(item)
    item.description = description or item.description
    item.material_group = group or item.material_group
    item.uom = (uom or "").strip().upper() or item.uom
    return item


def material_uom_map() -> dict[str, str]:
    """Material code → the unit the workbook states against it."""
    return {
        code: uom for code, uom in db.session.query(
            InventoryMonitoringMaterial.material_code, InventoryMonitoringMaterial.uom
        ).filter(InventoryMonitoringMaterial.uom.isnot(None), InventoryMonitoringMaterial.uom != "").all()
    }


def _get_work_center(name: str, zone: str | None = None, centre_type: str | None = None) -> InventoryMonitoringWorkCenter:
    """The asset a reported work-centre name belongs to.

    A merged asset keeps its old row so history still resolves, but the row points
    at its successor: N&H and B&S both land on NH-BS, which carries plant codes
    12A1 and 13A1. Following the pointer here is what keeps a merged asset one
    line in every register instead of two.
    """
    key = normalize_name(name)
    item = InventoryMonitoringWorkCenter.query.filter_by(normalized_name=key).first()
    if item is None:
        item = InventoryMonitoringWorkCenter(name=name, normalized_name=key); db.session.add(item)
    item.zone, item.work_center_type = zone or item.zone, centre_type or item.work_center_type
    return _merge_target(item)


def _merge_target(centre: InventoryMonitoringWorkCenter) -> InventoryMonitoringWorkCenter:
    """Follow a merged asset to its successor, refusing to loop on a broken chain."""
    seen: set[int] = set()
    while centre.merged_into_id and centre.merged_into_id not in seen and centre.merged_into_id != centre.id:
        seen.add(centre.merged_into_id)
        successor = db.session.get(InventoryMonitoringWorkCenter, centre.merged_into_id)
        if successor is None:
            break
        centre = successor
    return centre


def plant_code_index() -> dict[str, InventoryMonitoringWorkCenter]:
    """SAP plant code → the asset reporting under it.

    A merger leaves several codes on one asset, which is the whole point: 12A1 and
    13A1 both answer to NH-BS Asset, and nothing downstream has to know that.
    """
    index: dict[str, InventoryMonitoringWorkCenter] = {}
    for centre in InventoryMonitoringWorkCenter.query.all():
        for code in centre.plant_codes:
            index[code] = _merge_target(centre)
    return index


def _directory_names() -> set[str]:
    """Normalised names of every asset the work-centre directory declares."""
    batch = InventoryMonitoringUploadBatch.query.filter_by(source_group="mapping").order_by(
        InventoryMonitoringUploadBatch.id.desc()
    ).first()
    if batch is None:
        return set()
    return {normalize_name(row["work_center_name"]) for row in _read_mapping_directory(batch.source_data)}


def unrecognised_plants(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reported plants that no asset claims, summarised one line each.

    Work centres are not expected to change, so an unfamiliar one is news for the
    module admin rather than a routine addition. A row is recognised when its plant
    code is registered against an asset, or — while the exports still carry no plant
    column — when its work-centre name is one the directory declares or an admin has
    already placed in a zone.
    """
    known_codes = set(plant_code_index())
    known_names = _directory_names() | {
        centre.normalized_name for centre in InventoryMonitoringWorkCenter.query.filter(
            InventoryMonitoringWorkCenter.zone.isnot(None), InventoryMonitoringWorkCenter.zone != ""
        ).all()
    }
    unknown: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        code = (row.get("plant_code") or "").strip().upper()
        name = row.get("work_center_name") or ""
        if code:
            if code in known_codes:
                continue
        elif normalize_name(name) in known_names:
            continue
        entry = unknown.setdefault((code, normalize_name(name)), {
            "plant_code": code or None, "work_center_name": name, "line_count": 0, "value": Decimal("0"),
        })
        entry["line_count"] += 1
        entry["value"] += row.get("inventory_value_inr") or Decimal("0")
    return sorted(unknown.values(), key=lambda item: item["value"], reverse=True)


def _raise_plant_alerts(findings: list[dict[str, Any]], batch: InventoryMonitoringUploadBatch) -> int:
    """Record one open alert per unrecognised plant, refreshing an alert already raised."""
    raised = 0
    for finding in findings:
        existing = InventoryMonitoringPlantAlert.query.filter_by(
            plant_code=finding["plant_code"], work_center_name=finding["work_center_name"],
        ).first()
        if existing is None:
            db.session.add(InventoryMonitoringPlantAlert(
                plant_code=finding["plant_code"], work_center_name=finding["work_center_name"],
                batch_id=batch.id, line_count=finding["line_count"], inventory_value_inr=finding["value"],
            ))
            raised += 1
        elif existing.status == "open":
            existing.batch_id, existing.line_count, existing.inventory_value_inr = batch.id, finding["line_count"], finding["value"]
    return raised


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
    """Fill the units the detailed inventory sheet does not carry.

    The workbook's own material summary sheet states a unit against each material
    code, so that is used first. Retained consumption history covers whatever the
    workbook is silent about.
    """
    index = {**material_uom_index(), **material_uom_map()}
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


def audit_imported_plants() -> int:
    """Raise alerts for stock already imported under a plant no asset claims.

    Imports raise these as they run. This is the same check applied to what is
    already in the register, so an install that imported before the check existed
    still learns which plants are unclaimed.
    """
    latest_date = db.session.query(func.max(InventoryMonitoringSnapshot.reporting_date)).filter_by(is_published=True).scalar()
    if latest_date is None:
        return 0
    rows = [
        {"work_center_name": name, "plant_code": None, "inventory_value_inr": value or Decimal("0")}
        for name, value in db.session.query(
            InventoryMonitoringRecord.work_center_name, InventoryMonitoringRecord.inventory_value_inr
        ).join(
            InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
        ).filter(
            InventoryMonitoringSnapshot.reporting_date == latest_date,
            InventoryMonitoringSnapshot.is_published.is_(True),
        ).all()
    ]
    latest_batch = InventoryMonitoringUploadBatch.query.filter(
        InventoryMonitoringUploadBatch.source_group.in_(("09", "10"))
    ).order_by(InventoryMonitoringUploadBatch.id.desc()).first()
    if latest_batch is None:
        return 0
    return _raise_plant_alerts(unrecognised_plants(rows), latest_batch)


def backfill_material_summaries() -> int:
    """Read consumption from workbooks that were imported before it was stored.

    Every import retains its workbook, so the material summary sheets are still
    there to be read: an install that imported before consumption was recorded
    does not have to upload anything again.
    """
    added = 0
    snapshots = InventoryMonitoringSnapshot.query.join(
        InventoryMonitoringUploadBatch, InventoryMonitoringSnapshot.batch_id == InventoryMonitoringUploadBatch.id
    ).filter(_live_batch()).all()
    stored = {
        row[0] for row in db.session.query(InventoryMonitoringMaterialSummary.snapshot_id).distinct().all()
    }
    for snapshot in snapshots:
        if snapshot.id in stored or snapshot.material_group not in GROUP_SHEETS:
            continue
        batch = db.session.get(InventoryMonitoringUploadBatch, snapshot.batch_id)
        if batch is None or not batch.source_data:
            continue
        for summary in _read_material_summaries(batch.source_data, snapshot.material_group):
            material = _get_material(summary["material_code"], summary.get("material_description"), snapshot.material_group, summary.get("uom"))
            db.session.flush()
            db.session.add(InventoryMonitoringMaterialSummary(
                snapshot_id=snapshot.id, batch_id=batch.id, material_id=material.id, **summary,
            ))
            added += 1
    return added


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
    # The register is the complete monitoring catalogue. A fixed first-500
    # slice made correctly imported higher-numbered SAP materials invisible
    # unless someone already knew to search for them.
    materials = query.all()
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
        # The plant code is provenance for the alert, not a stock fact: the record
        # already carries the work centre the line was reported under.
        db.session.add(InventoryMonitoringRecord(
            snapshot_id=snapshot.id, batch_id=batch.id, material_id=material.id, work_center_id=centre.id,
            material_group=source_group, **{key: value for key, value in row.items() if key != "plant_code"},
        ))
    # Every stock line the workbook reports maps its material to the work centre holding it.
    for work_center_id, material_id in sorted(held_pairs - _current_mapping_pairs()):
        db.session.add(InventoryMonitoringWorkCenterMaterial(work_center_id=work_center_id, material_id=material_id, mapping_batch_id=batch.id, is_current=True))
    for summary in _read_material_summaries(source, source_group):
        material = _get_material(summary["material_code"], summary.get("material_description"), source_group, summary.get("uom"))
        db.session.flush()
        db.session.add(InventoryMonitoringMaterialSummary(
            snapshot_id=snapshot.id, batch_id=batch.id, material_id=material.id, **summary,
        ))
    alerts = _raise_plant_alerts(unrecognised_plants(rows), batch)
    if alerts:
        validation = json.loads(batch.validation_json)
        validation["new_plant_alerts"] = alerts
        batch.validation_json = json.dumps(validation, default=str)
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
            # A merged asset is one entry on the navigator, listed under its
            # successor: the directory still names both, but the map is the
            # organisation as it stands, not as the workbook last described it.
            asset = _merge_target(centre) if centre else None
            name = asset.name if asset else row["work_center_name"]
            zone = (asset.zone if asset and asset.zone else row["zone"]) or "Unassigned zone"
            grouped_directory[zone][name].append({
                "id": asset.id if asset else None,
                "name": name,
                # The name the directory used, so a merged asset can still be
                # placed on the map by the coordinates held for either of them.
                "reported_name": row["work_center_name"],
                "work_center_type": row["work_center_type"] or "Work centre",
                "exception_count": exception_counts.get(asset.id, 0) if asset else 0,
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
    # SAP keeps a plant code per legacy asset, so a merged asset carries several.
    # The navigator states them, because that is how an asset is recognised in SAP.
    plant_codes_by_centre = {centre.id: centre.plant_codes for centre in centres}
    map_assets = [
        {
            "id": next((entry["id"] for entry in asset["work_centres"] if entry["id"]), None),
            "name": asset["name"],
            "zone": zone["zone"],
            "units": list(dict.fromkeys(entry["work_center_type"] for entry in asset["work_centres"])),
            "aliases": list(dict.fromkeys(
                entry["reported_name"] for entry in asset["work_centres"] if entry["reported_name"] != asset["name"]
            )),
            "plant_codes": sorted({
                code
                for entry in asset["work_centres"]
                for code in plant_codes_by_centre.get(entry["id"], [])
            }),
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


def scope_directory(reporting_date: date | None) -> list[dict[str, Any]]:
    """Zones and the assets reporting stock in one period, for a region-wise chooser.

    Reading it from the period rather than from the directory means the chooser
    only ever offers assets the deck can actually be built for.
    """
    if reporting_date is None:
        return []
    rows = db.session.query(
        InventoryMonitoringWorkCenter.id, InventoryMonitoringWorkCenter.name,
        InventoryMonitoringWorkCenter.zone, InventoryMonitoringWorkCenter.work_center_type,
        func.coalesce(func.sum(InventoryMonitoringRecord.inventory_value_inr), 0),
    ).join(
        InventoryMonitoringRecord, InventoryMonitoringRecord.work_center_id == InventoryMonitoringWorkCenter.id
    ).join(
        InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id
    ).filter(
        InventoryMonitoringSnapshot.reporting_date == reporting_date,
        InventoryMonitoringSnapshot.is_published.is_(True),
    ).group_by(
        InventoryMonitoringWorkCenter.id, InventoryMonitoringWorkCenter.name,
        InventoryMonitoringWorkCenter.zone, InventoryMonitoringWorkCenter.work_center_type,
    ).all()
    zones: dict[str, list[dict[str, Any]]] = defaultdict(list)
    plant_codes = {centre.id: centre.plant_codes for centre in InventoryMonitoringWorkCenter.query.all()}
    for centre_id, name, zone, centre_type, value in rows:
        zones[zone or "Unassigned zone"].append({
            "id": centre_id, "name": name, "unit": centre_type, "value": value or Decimal("0"),
            "plant_codes": plant_codes.get(centre_id, []),
        })
    return [
        {
            "zone": zone,
            "assets": sorted(assets, key=lambda item: item["value"], reverse=True),
            "value": sum((item["value"] for item in assets), Decimal("0")),
        }
        for zone, assets in sorted(zones.items(), key=lambda item: item[0].casefold())
    ]


def _empty_material_movers() -> dict[str, dict[str, list]]:
    return {phase: {"up": [], "down": [], "ranked": []} for phase in ("liquid", "solid", "other")}


def _phase_movers(current: dict[str, Decimal], previous: dict[str, Decimal], meta: dict[str, Any], uoms: dict[str, str | None], limit: int = 4) -> dict[str, dict[str, list]]:
    """Materials that moved most between two periods, kept apart by phase.

    A drum of chemical and a tonne of cement do not belong in one league table, so
    build-ups and draw-downs are ranked within liquids and within solids.
    """
    movers = _empty_material_movers()
    for code in set(current) & set(previous):
        delta = current[code] - previous[code]
        if delta == 0:
            continue
        description, group = meta.get(code, (None, None))
        entry = {
            "code": code, "description": description, "group": group, "uom": uoms.get(code),
            "value": current[code], "prev": previous[code], "delta": delta,
        }
        bucket = movers[phase_of(uoms.get(code))]
        bucket["up" if delta > 0 else "down"].append(entry)
    for bucket in movers.values():
        bucket["up"] = sorted(bucket["up"], key=lambda item: item["delta"], reverse=True)[:limit]
        bucket["down"] = sorted(bucket["down"], key=lambda item: item["delta"])[:limit]
        # One table per phase reads better than two lists, so the movements are
        # also handed over ranked by size, whichever way they went.
        bucket["ranked"] = sorted(bucket["up"] + bucket["down"], key=lambda item: abs(item["delta"]), reverse=True)
    return movers


def material_summary_rows(reporting_date: date | None) -> list[InventoryMonitoringMaterialSummary]:
    """The all-ONGC material summary lines published for one reporting date."""
    if reporting_date is None:
        return []
    return InventoryMonitoringMaterialSummary.query.join(
        InventoryMonitoringSnapshot, InventoryMonitoringMaterialSummary.snapshot_id == InventoryMonitoringSnapshot.id
    ).filter(
        InventoryMonitoringSnapshot.reporting_date == reporting_date,
        InventoryMonitoringSnapshot.is_published.is_(True),
    ).all()


def consumption_leaders(reporting_date: date | None) -> dict[str, Any]:
    """What ONGC actually consumes, ranked by value.

    The figures are the workbook's own: SAP states twelve-month consumption per
    material on the material summary sheet, so this is a full year of consumption
    read out of the file, not something derived from the imported snapshots.

    Value is one scale for every material, so the table lists everything above the
    ₹ 10 Cr floor. Quantity is not one scale — a kilolitre and a tonne do not
    compare — which is why quantity is ranked only where it is being compared
    like with like, in the period movers.
    """
    rows = material_summary_rows(reporting_date)
    total_value = sum((row.consumption_value_inr or Decimal("0") for row in rows), Decimal("0"))

    def entry(row: InventoryMonitoringMaterialSummary) -> dict[str, Any]:
        phase = phase_of(row.uom)
        return {
            "material_id": row.material_id, "code": row.material_code, "description": row.material_description,
            "group": row.material_group, "uom": row.uom, "phase": phase, "phase_label": PHASE_LABELS[phase],
            "consumption_value": row.consumption_value_inr or Decimal("0"),
            "consumption_qty": row.consumption_qty_12m,
            "normalized_qty": normalized_quantity(row.consumption_qty_12m, row.uom),
            "normalized_unit": PHASE_UNITS[phase],
            "inventory_value": row.inventory_value_inr, "stock_months": row.stock_months,
            "share": _share(row.consumption_value_inr or Decimal("0"), total_value),
        }

    entries = [entry(row) for row in rows]
    by_value = sorted(
        (item for item in entries if item["consumption_value"] >= HIGH_CONSUMPTION_VALUE_FLOOR),
        key=lambda item: item["consumption_value"], reverse=True,
    )
    return {
        "reporting_date": reporting_date,
        "materials": len(entries),
        "total_value": total_value,
        "floor": HIGH_CONSUMPTION_VALUE_FLOOR,
        "by_value": by_value,
        "by_value_total": sum((item["consumption_value"] for item in by_value), Decimal("0")),
        "phase_counts": {phase: sum(item["phase"] == phase for item in entries) for phase in ("liquid", "solid", "other")},
    }


def portfolio_data(reporting_date: date | None = None, compare_date: date | None = None, centre_ids: set[int] | None = None) -> dict[str, Any]:
    """Executive review dataset for one published reporting period, compared like-for-like with an earlier one.

    ``centre_ids`` narrows every figure to a chosen set of assets, which is how a
    region- or asset-level deck is exported; ``None`` is all of ONGC.
    """
    published_dates = [row[0] for row in db.session.query(InventoryMonitoringSnapshot.reporting_date).filter_by(is_published=True).distinct().order_by(InventoryMonitoringSnapshot.reporting_date.desc()).all()]
    selected = reporting_date or (published_dates[0] if published_dates else None)
    earlier_dates = [item for item in published_dates if selected and item < selected]
    previous = compare_date if compare_date in earlier_dates else (earlier_dates[0] if earlier_dates else None)
    empty = {
        "reporting_date": selected, "previous_date": previous, "available_dates": published_dates,
        "comparison_dates": earlier_dates, "comparison": None, "pending_periods": pending_periods(),
        "kpis": None, "health_mix": [], "zones": [], "centres": [], "movers": {"up": [], "down": []},
        "entrants": [], "exits": [], "material_movers": _empty_material_movers(),
        "consumption": consumption_leaders(selected),
        "scope_centres": [], "top_materials": [], "exception_severities": {}, "exception_types": [], "exceptions": [],
    }
    if selected is None:
        return empty

    def _rows(as_on: date):
        return db.session.query(
            InventoryMonitoringRecord.work_center_id, InventoryMonitoringWorkCenter.name, InventoryMonitoringWorkCenter.zone,
            InventoryMonitoringRecord.material_group, InventoryMonitoringRecord.material_id, InventoryMonitoringRecord.material_code,
            InventoryMonitoringRecord.material_description, InventoryMonitoringRecord.inventory_value_inr, InventoryMonitoringRecord.stock_months,
            InventoryMonitoringRecord.uom,
        ).join(InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id).outerjoin(
            InventoryMonitoringWorkCenter, InventoryMonitoringRecord.work_center_id == InventoryMonitoringWorkCenter.id
        ).filter(
            InventoryMonitoringSnapshot.reporting_date == as_on, InventoryMonitoringSnapshot.is_published.is_(True),
            *( [InventoryMonitoringRecord.work_center_id.in_(centre_ids or {0})] if centre_ids is not None else [] ),
        ).all()

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
    # The unit belongs to the material, and the workbook states it on the material
    # summary sheet; a stock line's unit is only a fallback for a code the summary
    # sheets have never carried.
    material_uom: dict[str, str | None] = material_uom_map()
    for wc_id, wc_name, wc_zone, grp, _mat_id, code, desc, value, months, uom in rows:
        value = value or Decimal("0")
        total += value
        material_uom.setdefault(code, uom)
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
        prev_material = defaultdict(Decimal)
        for wc_id, wc_name, wc_zone, _g, _m, code, _d, value, months, uom in _rows(previous):
            value = value or Decimal("0")
            prev_total += value
            prev_material[code] += value
            material_uom.setdefault(code, uom)
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
    material_movers = _empty_material_movers()
    entrants: list[dict[str, Any]] = []
    exits: list[dict[str, Any]] = []
    comparison = None
    if previous:
        material_movers = _phase_movers(material_value, prev_material, material_meta, material_uom)
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
        "material_movers": material_movers,
        "consumption": consumption_leaders(selected),
        "scope_centres": sorted(
            ({"id": wc_id, "name": centre_meta[wc_id][0], "zone": centre_meta[wc_id][1]} for wc_id in centre_value),
            key=lambda item: (item["zone"] or "", item["name"]),
        ) if centre_ids is not None else [],
        "top_materials": [
            {"code": code, "description": material_meta[code][0], "group": material_meta[code][1], "uom": material_uom.get(code),
             "value": value, "share": _share(value, total), "centres": len(material_centres[code])}
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


def management_review_data(reporting_date: date | None = None, compare_date: date | None = None, centre_ids: set[int] | None = None) -> dict[str, Any]:
    """Portfolio headline plus the complete registers management reviews for one published period.

    ``centre_ids`` narrows the review to chosen assets, so a region or a single
    asset can be exported without rebuilding the deck for it.
    """
    base = portfolio_data(reporting_date, compare_date, centre_ids)
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
    ).filter(
        InventoryMonitoringSnapshot.reporting_date == selected, InventoryMonitoringSnapshot.is_published.is_(True),
        *([InventoryMonitoringRecord.work_center_id.in_(centre_ids or {0})] if centre_ids is not None else []),
    ).all()

    uom_by_code = material_uom_map()
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    open_supply: list[dict[str, Any]] = []
    materials: dict[str, dict[str, Any]] = {}
    material_centres: dict[str, set] = defaultdict(set)
    centre_value: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for group, code, description, centre, zone, qty, uom, value, open_po, open_pr, months in lines:
        centre_name, zone_name = centre or "Unmapped work centre", zone or "Unassigned"
        line = {
            "group": group, "code": code, "description": description, "centre": centre_name, "zone": zone_name,
            "qty": qty, "uom": uom_by_code.get(code) or uom, "value": value or Decimal("0"), "open_po": open_po, "open_pr": open_pr,
            "months": months,
        }
        buckets[_health_category(months, thresholds)].append(line)
        centre_value[(centre_name, zone_name)] += line["value"]
        if months is not None and months >= thresholds["slow_moving_months"] and ((open_po or 0) > 0 or (open_pr or 0) > 0):
            open_supply.append(line)
        material = materials.setdefault(code, {"code": code, "description": description, "group": group, "uom": uom_by_code.get(code) or uom, "value": Decimal("0"), "months_low": None, "months_high": None})
        material["description"] = material["description"] or description
        material["uom"] = material["uom"] or uom
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
        *([InventoryMonitoringException.work_center_id.in_(centre_ids or {0})] if centre_ids is not None else []),
    ).all():
        supporting[kind].append({
            "group": group, "code": code or "—", "description": description, "uom": uom_by_code.get(code or ""),
            "centre": centre or "Unmapped work centre",
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
        source_findings[kind] = InventoryMonitoringException.query.join(
            InventoryMonitoringSnapshot
        ).join(
            InventoryMonitoringUploadBatch,
            InventoryMonitoringSnapshot.batch_id == InventoryMonitoringUploadBatch.id,
        ).filter(
            InventoryMonitoringSnapshot.reporting_date == latest_date,
            _live_batch(),
            InventoryMonitoringException.exception_type == kind,
        ).order_by(InventoryMonitoringException.inventory_value_inr.desc()).all()
    # A coverage band is our own reading of stock months; the workbook's own
    # registers are the source's reading. Where both point at the same material in
    # the same work centre, the row is confirmed twice over and is shaded to say so.
    confirmed_conditions: dict[tuple[int, int], list[str]] = defaultdict(list)
    for kind, label, _description in SUPPORTING_REGISTERS:
        for item in source_findings.get(kind, []):
            if item.material_id and item.work_center_id:
                confirmed_conditions[(item.material_id, item.work_center_id)].append(label)

    # Where each workbook register's lines actually sit in our bands. A reader who
    # sees "49 non-moving" needs to know which table to look in for them, and the
    # answer is rarely the one the name suggests: a material with no consumption has
    # enormous coverage, so the workbook's non-moving and slow-moving lines land in
    # our excess band, not our slow-moving one.
    band_of: dict[tuple[int, int], str] = {
        (record.material_id, record.work_center_id): band
        for band, rows in groups.items() for record in rows
    }
    source_bands: dict[str, list[dict[str, Any]]] = {}
    for kind, _label, _description in SUPPORTING_REGISTERS:
        counts: dict[str, int] = defaultdict(int)
        for item in source_findings.get(kind, []):
            counts[band_of.get((item.material_id, item.work_center_id), "unmatched")] += 1
        source_bands[kind] = [
            {"key": key, "label": HEALTH_MIX_LABELS.get(key, "Not held at this snapshot"), "count": count,
             "anchor": {"critical_low_stock": "critical", "low_stock": "low", "slow_moving_stock": "slow", "excess_stock": "excess"}.get(key)}
            for key, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        ]
    index = specification_index()
    return {
        "reporting_date": latest_date, "thresholds": thresholds, "groups": groups, "source_findings": source_findings,
        "group_limit": HEALTH_REGISTER_GROUP_LIMIT,
        "confirmed_conditions": {key: sorted(set(value)) for key, value in confirmed_conditions.items()},
        "source_bands": source_bands,
        "spec_groups": {key: specification_groups(rows, index, limit=HEALTH_REGISTER_GROUP_LIMIT) for key, rows in groups.items()},
        "source_spec_groups": {
            key: specification_groups(rows, index, code_of=lambda item: item.material.material_code if item.material else "")
            for key, rows in source_findings.items()
        },
    }


def asset_administration_data() -> dict[str, Any]:
    """Every asset with the SAP plant codes reporting into it, and the plants nothing claims.

    The register is the answer to a merger: an asset that absorbed another carries
    both plant codes on one row, and the absorbed asset is shown against its
    successor rather than as a second line in every report.
    """
    centres = InventoryMonitoringWorkCenter.query.order_by(
        InventoryMonitoringWorkCenter.zone, InventoryMonitoringWorkCenter.name
    ).all()
    by_id = {centre.id: centre for centre in centres}
    latest_date = db.session.query(func.max(InventoryMonitoringSnapshot.reporting_date)).filter_by(is_published=True).scalar()
    values = dict(db.session.query(
        InventoryMonitoringRecord.work_center_id, func.coalesce(func.sum(InventoryMonitoringRecord.inventory_value_inr), 0)
    ).join(InventoryMonitoringSnapshot, InventoryMonitoringRecord.snapshot_id == InventoryMonitoringSnapshot.id).filter(
        InventoryMonitoringSnapshot.reporting_date == latest_date, InventoryMonitoringSnapshot.is_published.is_(True),
    ).group_by(InventoryMonitoringRecord.work_center_id).all()) if latest_date else {}
    assets = [
        {
            "centre": centre, "plant_codes": centre.plant_codes,
            "merged_into": by_id.get(centre.merged_into_id),
            "value": values.get(centre.id, Decimal("0")),
        }
        for centre in centres
    ]
    return {
        "assets": assets,
        "merge_options": [centre for centre in centres if centre.merged_into_id is None],
        "plant_alerts": InventoryMonitoringPlantAlert.query.filter_by(status="open").order_by(
            InventoryMonitoringPlantAlert.inventory_value_inr.desc()
        ).all(),
        "resolved_alerts": InventoryMonitoringPlantAlert.query.filter_by(status="resolved").order_by(
            InventoryMonitoringPlantAlert.resolved_at.desc()
        ).limit(10).all(),
        "reporting_date": latest_date,
    }


def save_asset_plant_codes(form: Any) -> list[str]:
    """Apply the plant-code and merge edits one administration form carries.

    Returns a description of each change, so the audit trail records what moved
    rather than only that something did.
    """
    changes: list[str] = []
    for centre in InventoryMonitoringWorkCenter.query.all():
        codes_field = form.get(f"plant_codes-{centre.id}")
        if codes_field is not None:
            codes = ",".join(dict.fromkeys(
                part.strip().upper() for part in re.split(r"[,\s]+", codes_field) if part.strip()
            ))
            if (centre.sap_plant_codes or "") != codes:
                changes.append(f"{centre.name}: plant codes {centre.sap_plant_codes or 'none'} → {codes or 'none'}")
                centre.sap_plant_codes = codes or None
        merge_field = form.get(f"merged_into-{centre.id}")
        if merge_field is not None:
            target = int(merge_field) if merge_field.strip().isdigit() else None
            if target == centre.id:
                raise ValueError(f"{centre.name} cannot be merged into itself.")
            if target != centre.merged_into_id:
                successor = db.session.get(InventoryMonitoringWorkCenter, target) if target else None
                if target and successor is None:
                    raise ValueError("The asset selected to merge into no longer exists.")
                changes.append(
                    f"{centre.name}: merged into {successor.name}" if successor else f"{centre.name}: merge cleared"
                )
                centre.merged_into_id = target
    return changes


def resolve_plant_alert(alert_id: int, action: str, form: Any, user_id: int | None) -> str:
    """Close one unrecognised-plant alert the way the module admin chose.

    ``attach`` puts the plant code on an existing asset — which is how a merged
    asset ends up carrying two codes. ``create`` opens a new asset for it.
    ``dismiss`` records that the plant is deliberately not monitored.
    """
    alert = db.session.get(InventoryMonitoringPlantAlert, alert_id)
    if alert is None or alert.status != "open":
        raise ValueError("That plant alert is no longer open.")
    if action == "attach":
        centre = db.session.get(InventoryMonitoringWorkCenter, int(form.get("work_center_id") or 0))
        if centre is None:
            raise ValueError("Select the asset this plant reports into.")
        centre = _merge_target(centre)
        if alert.plant_code:
            codes = dict.fromkeys(centre.plant_codes + [alert.plant_code])
            centre.sap_plant_codes = ",".join(codes)
        reported = InventoryMonitoringWorkCenter.query.filter_by(normalized_name=normalize_name(alert.work_center_name)).first()
        if reported is not None and reported.id != centre.id:
            # Stock already imported under the reported name follows the asset it
            # was attached to, instead of standing as a second line beside it.
            reported.merged_into_id = centre.id
        alert.work_center_id = centre.id
        summary = f"{alert.plant_code or alert.work_center_name} attached to {centre.name}"
    elif action == "create":
        name = (form.get("name") or alert.work_center_name).strip()
        if not name:
            raise ValueError("A new asset needs a name.")
        centre = _get_work_center(name, (form.get("zone") or "").strip() or None, (form.get("work_center_type") or "").strip() or None)
        db.session.flush()
        if alert.plant_code:
            centre.sap_plant_codes = ",".join(dict.fromkeys(centre.plant_codes + [alert.plant_code]))
        alert.work_center_id = centre.id
        summary = f"{alert.plant_code or alert.work_center_name} opened as asset {centre.name}"
    elif action == "dismiss":
        summary = f"{alert.plant_code or alert.work_center_name} recorded as not monitored"
    else:
        raise ValueError("Choose what to do with this plant.")
    alert.status, alert.resolution, alert.resolved_by = "resolved", summary, user_id
    alert.resolved_at = datetime.now(timezone.utc)
    return summary


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
