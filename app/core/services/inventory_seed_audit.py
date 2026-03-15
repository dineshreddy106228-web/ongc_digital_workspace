from __future__ import annotations

"""Sanity analysis and cleanup helpers for inventory seed workbooks."""

from dataclasses import asdict, dataclass
from io import BytesIO
import json
import os
import shutil
import tempfile
import uuid

import pandas as pd

from app.core.services.inventory_intelligence import (
    CONS_FILE,
    PROC_FILE,
    _detect_consumption_fields,
    _detect_procurement_fields,
    _looks_like_period,
    _normalize_consumption_frame,
    _normalize_procurement_frame,
    _read_uploaded_seed_dataframe,
    _validate_seed_frame,
    get_data_store,
    save_seed_workbook,
    save_seed_workbooks,
)


LOW_VALUE_THRESHOLD = 100000.0
STAGING_ROOT = os.path.join(tempfile.gettempdir(), "inventory_seed_staging")


@dataclass
class AuditFinding:
    severity: str
    scope: str
    check: str
    count: int
    message: str
    columns: list[str]
    rows: list[dict]
    examples: list[str]
    selectable_rows: list[int]
    primary_action: str


def _read_seed_frame(path: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=0)


def _ensure_staging_root() -> str:
    os.makedirs(STAGING_ROOT, exist_ok=True)
    return STAGING_ROOT


def _staging_paths(token: str) -> dict:
    root = os.path.join(_ensure_staging_root(), token)
    return {
        "root": root,
        "consumption": os.path.join(root, "consumption.xlsx"),
        "procurement": os.path.join(root, "procurement.xlsx"),
        "meta": os.path.join(root, "meta.json"),
    }


def _write_frame(path: str, frame: pd.DataFrame, sheet_name: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name=sheet_name)


def _read_staged_meta(token: str) -> dict:
    paths = _staging_paths(token)
    with open(paths["meta"], "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_staged_meta(token: str, meta: dict) -> None:
    paths = _staging_paths(token)
    os.makedirs(paths["root"], exist_ok=True)
    with open(paths["meta"], "w", encoding="utf-8") as handle:
        json.dump(meta, handle)


def discard_staged_seed_upload(token: str) -> None:
    paths = _staging_paths(token)
    if os.path.isdir(paths["root"]):
        shutil.rmtree(paths["root"], ignore_errors=True)


def _load_staged_frames(token: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    paths = _staging_paths(token)
    if not os.path.exists(paths["consumption"]) or not os.path.exists(paths["procurement"]) or not os.path.exists(paths["meta"]):
        raise ValueError("The staged seed upload is no longer available. Upload the files again.")
    cons_raw = _read_seed_frame(paths["consumption"])
    proc_raw = _read_seed_frame(paths["procurement"])
    meta = _read_staged_meta(token)
    return cons_raw, proc_raw, meta


def _serialize_value(value):
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return value


def _serialize_rows(frame: pd.DataFrame, columns: list[str]) -> list[dict]:
    rows: list[dict] = []
    for index, row in frame.iterrows():
        record = {"Excel Row": int(index) + 2}
        for column in columns:
            record[column] = _serialize_value(row.get(column))
        rows.append(record)
    return rows


def _row_fingerprint_from_series(row: pd.Series, columns: list[str]) -> str:
    payload = {column: _serialize_value(row.get(column)) for column in columns}
    return json.dumps(payload, sort_keys=True, default=str)


def _format_examples(rows: list[dict], columns: list[str], limit: int = 5) -> list[str]:
    examples: list[str] = []
    for row in rows[:limit]:
        examples.append(", ".join(f"{column}={row.get(column) or '∅'}" for column in columns))
    return examples


def _build_consumption_raw_view(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame.columns = [str(column).strip() for column in frame.columns]
    col_map = {}
    for col in frame.columns:
        key = col.lower().strip()
        if key == "plant":
            col_map[col] = "plant"
        elif key == "material":
            col_map[col] = "material"
        elif "storage" in key:
            col_map[col] = "storage_location"
        elif key == "month":
            col_map[col] = "month_raw"
        elif "usage quantity" in key:
            col_map[col] = "usage_qty"
        elif "usage uom" in key:
            col_map[col] = "uom"
        elif "usage value" in key:
            col_map[col] = "usage_value"
        elif "usage currency" in key or key == "currency":
            col_map[col] = "currency"
    frame = frame.rename(columns=col_map)
    for column in ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "currency"]:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame


def _build_procurement_raw_view(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame.columns = [str(column).strip() for column in frame.columns]
    col_map = {}
    for col in frame.columns:
        key = col.lower().strip()
        if key == "plant":
            col_map[col] = "plant"
        elif key == "net price":
            col_map[col] = "net_price"
        elif "purchasing document" in key:
            col_map[col] = "po_number"
        elif "document date" in key:
            col_map[col] = "doc_date"
        elif key == "material":
            col_map[col] = "material_code"
        elif key == "item":
            col_map[col] = "item"
        elif "supplier" in key or "supplying" in key:
            col_map[col] = "vendor"
        elif "short text" in key:
            col_map[col] = "material_desc"
        elif "order quantity" in key:
            col_map[col] = "order_qty"
        elif "order unit" in key:
            col_map[col] = "order_unit"
        elif key == "currency":
            col_map[col] = "currency"
        elif "price unit" in key:
            col_map[col] = "price_unit"
        elif "effective value" in key:
            col_map[col] = "effective_value"
        elif "release" in key:
            col_map[col] = "release_indicator"
    frame = frame.rename(columns=col_map)
    for column in [
        "plant",
        "net_price",
        "po_number",
        "doc_date",
        "material_code",
        "item",
        "vendor",
        "material_desc",
        "order_qty",
        "order_unit",
        "currency",
        "price_unit",
        "effective_value",
    ]:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame["doc_date"] = pd.to_datetime(frame["doc_date"], errors="coerce")
    for column in ["net_price", "order_qty", "price_unit", "effective_value"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _format_consumption_period(cons: pd.DataFrame, which: str) -> str:
    if cons.empty:
        return ""
    view = cons.dropna(subset=["year", "month"])[["year", "month"]].drop_duplicates().sort_values(["year", "month"])
    if view.empty:
        return ""
    row = view.iloc[0] if which == "first" else view.iloc[-1]
    return f"{int(row['month']):02d}.{int(row['year'])}"


def _finding(
    *,
    severity: str,
    scope: str,
    check: str,
    message: str,
    frame: pd.DataFrame | None = None,
    columns: list[str] | None = None,
    examples: list[str] | None = None,
    selectable_rows: list[int] | None = None,
    fingerprint_columns: list[str] | None = None,
    ignored_fingerprints: set[str] | None = None,
    primary_action: str = "delete",
) -> AuditFinding:
    frame = frame if frame is not None else pd.DataFrame()
    columns = columns or []
    fingerprint_columns = fingerprint_columns or columns
    ignored_fingerprints = ignored_fingerprints or set()
    if not frame.empty and fingerprint_columns:
        mask = frame.apply(lambda row: _row_fingerprint_from_series(row, fingerprint_columns) not in ignored_fingerprints, axis=1)
        frame = frame.loc[mask].copy()
    rows = _serialize_rows(frame, columns) if not frame.empty else []
    return AuditFinding(
        severity=severity,
        scope=scope,
        check=check,
        count=len(rows) if rows else len(examples or []),
        message=message,
        columns=(["Excel Row"] + columns) if rows else [],
        rows=rows,
        examples=examples if examples is not None else _format_examples(rows, ["Excel Row"] + columns),
        selectable_rows=selectable_rows if selectable_rows is not None else [int(row["Excel Row"]) for row in rows],
        primary_action=primary_action,
    )


def _collect_consumption_findings(raw_df: pd.DataFrame, cons: pd.DataFrame, ignored_rows: dict[str, set[str]]) -> tuple[list[AuditFinding], dict]:
    findings: list[AuditFinding] = []
    raw_view = _build_consumption_raw_view(raw_df)

    shifted_mask = (
        raw_view["month_raw"].fillna("").astype(str).str.strip().eq("")
        & raw_view["usage_qty"].apply(_looks_like_period)
    )
    shifted_rows = raw_view.loc[shifted_mask, ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "currency"]]
    if not shifted_rows.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="consumption",
                check="shifted_columns",
                message="Rows look column-shifted and are only usable because the loader repairs them.",
                frame=shifted_rows,
                columns=["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "currency"],
                ignored_fingerprints=ignored_rows.get("consumption:shifted_columns", set()),
            )
        )

    invalid_periods = cons.loc[cons["month"].isna() | cons["year"].isna(), ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"]]
    if not invalid_periods.empty:
        findings.append(
            _finding(
                severity="error",
                scope="consumption",
                check="invalid_period",
                message="Rows have unparseable reporting periods.",
                frame=invalid_periods,
                columns=["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"],
                ignored_fingerprints=ignored_rows.get("consumption:invalid_period", set()),
            )
        )

    non_positive_qty = cons.loc[cons["usage_qty"].fillna(0) <= 0, ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"]]
    if not non_positive_qty.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="consumption",
                check="non_positive_quantity",
                message="Rows have zero or negative usage quantity.",
                frame=non_positive_qty,
                columns=["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"],
                ignored_fingerprints=ignored_rows.get("consumption:non_positive_quantity", set()),
            )
        )

    negative_value = cons.loc[cons["usage_value"].fillna(0) < 0, ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"]]
    if not negative_value.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="consumption",
                check="negative_value",
                message="Rows have negative consumption value.",
                frame=negative_value,
                columns=["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"],
                ignored_fingerprints=ignored_rows.get("consumption:negative_value", set()),
            )
        )

    duplicates = cons.loc[
        cons.duplicated(["plant", "material", "storage_location", "month_raw", "usage_qty", "usage_value"], keep=False),
        ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"],
    ]
    if not duplicates.empty:
        duplicate_view = duplicates.sort_index().copy()
        duplicate_view["Selection Status"] = "Delete Candidate"
        frozen_indices: list[int] = []
        group_fields = ["plant", "material", "storage_location", "month_raw", "usage_qty", "usage_value"]
        for _, group in duplicate_view.groupby(group_fields, sort=False):
            keep_index = group.index[0]
            duplicate_view.loc[keep_index, "Selection Status"] = "Keep (Frozen)"
            frozen_indices.append(int(keep_index))
        selectable_rows = [
            int(index) + 2
            for index in duplicate_view.index
            if int(index) not in frozen_indices
        ]
        findings.append(
            _finding(
                severity="warning",
                scope="consumption",
                check="duplicate_rows",
                message="Potential duplicate consumption rows were found on plant/material/location/period/value keys. One row per duplicate set is frozen as the keeper; the remaining rows can be deleted.",
                frame=duplicate_view,
                columns=["Selection Status", "plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"],
                selectable_rows=selectable_rows,
                fingerprint_columns=["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"],
                ignored_fingerprints=ignored_rows.get("consumption:duplicate_rows", set()),
            )
        )

    summary = {
        "rows": int(len(cons)),
        "materials": int(cons["material_desc"].nunique()),
        "plants": int(cons["plant"].nunique()),
        "storage_locations": int(cons["storage_location"].nunique()),
        "period_from": _format_consumption_period(cons, "first"),
        "period_to": _format_consumption_period(cons, "last"),
    }
    return findings, summary


def _collect_procurement_findings(proc_raw: pd.DataFrame, proc: pd.DataFrame, ignored_rows: dict[str, set[str]]) -> tuple[list[AuditFinding], dict]:
    findings: list[AuditFinding] = []
    proc_raw_view = _build_procurement_raw_view(proc_raw)

    non_unit_price = proc_raw_view.loc[
        proc_raw_view["price_unit"].fillna(1).astype(float) != 1,
        ["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "net_price", "price_unit", "effective_value"],
    ].copy()
    unresolved_price_unit_rows = 0
    if not non_unit_price.empty:
        valid_price_unit = non_unit_price["price_unit"].where(non_unit_price["price_unit"].fillna(0) > 0, 1.0)
        non_unit_price["normalized_net_price"] = non_unit_price["net_price"] / valid_price_unit
        price_unit_finding = _finding(
            severity="warning",
            scope="procurement",
            check="price_unit_not_one",
            message="Sanity Check -1: these rows have price unit other than 1. Normalize net price by dividing it by price unit, then rerun the next sanity checks.",
            frame=non_unit_price,
            columns=["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "net_price", "price_unit", "normalized_net_price", "effective_value"],
            ignored_fingerprints=ignored_rows.get("procurement:price_unit_not_one", set()),
            primary_action="normalize",
        )
        unresolved_price_unit_rows = price_unit_finding.count
        if price_unit_finding.count:
            findings.append(price_unit_finding)

    missing_plant = proc.loc[
        proc["plant"].fillna("").astype(str).str.strip().eq(""),
        ["po_number", "doc_date", "material_desc", "vendor", "order_qty", "order_unit", "effective_value"],
    ]
    if not missing_plant.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="procurement",
                check="missing_plant",
                message="Rows are missing plant, which breaks plant-level filtering and overlap checks.",
                frame=missing_plant,
                columns=["po_number", "doc_date", "material_desc", "vendor", "order_qty", "order_unit", "effective_value"],
                ignored_fingerprints=ignored_rows.get("procurement:missing_plant", set()),
            )
        )

    invalid_date = proc.loc[
        proc["doc_date"].isna(),
        ["po_number", "material_desc", "vendor", "order_qty", "order_unit", "effective_value"],
    ]
    if not invalid_date.empty:
        findings.append(
            _finding(
                severity="error",
                scope="procurement",
                check="invalid_doc_date",
                message="Rows have unparseable procurement document dates.",
                frame=invalid_date,
                columns=["po_number", "material_desc", "vendor", "order_qty", "order_unit", "effective_value"],
                ignored_fingerprints=ignored_rows.get("procurement:invalid_doc_date", set()),
            )
        )

    non_positive_qty = proc.loc[
        proc["order_qty"].fillna(0) <= 0,
        ["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "effective_value"],
    ]
    if not non_positive_qty.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="procurement",
                check="non_positive_order_qty",
                message="Rows have zero or negative order quantity.",
                frame=non_positive_qty,
                columns=["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "effective_value"],
                ignored_fingerprints=ignored_rows.get("procurement:non_positive_order_qty", set()),
            )
        )

    non_positive_unit_price = proc.loc[
        proc["unit_price"].fillna(0) <= 0,
        ["po_number", "doc_date", "plant", "material_desc", "vendor", "net_price", "price_unit", "unit_price", "effective_value"],
    ]
    if not non_positive_unit_price.empty:
        findings.append(
            _finding(
                severity="error",
                scope="procurement",
                check="non_positive_unit_price",
                message="Rows have non-positive normalized unit price after net-price/price-unit normalization.",
                frame=non_positive_unit_price,
                columns=["po_number", "doc_date", "plant", "material_desc", "vendor", "net_price", "price_unit", "unit_price", "effective_value"],
                ignored_fingerprints=ignored_rows.get("procurement:non_positive_unit_price", set()),
            )
        )

    if unresolved_price_unit_rows == 0:
        price_view = proc.dropna(subset=["doc_date", "unit_price"]).sort_values(["material_desc", "doc_date", "po_number"]).copy()
        price_view["prev_unit_price"] = price_view.groupby("material_desc")["unit_price"].shift(1)
        price_view["pct_change"] = (
            (price_view["unit_price"] - price_view["prev_unit_price"]) / price_view["prev_unit_price"]
        ) * 100
        extreme_swings = price_view.loc[
            (price_view["prev_unit_price"] > 0) & (price_view["pct_change"].abs() > 100),
            ["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "prev_unit_price", "unit_price", "pct_change", "effective_value"],
        ]
        if not extreme_swings.empty:
            findings.append(
                _finding(
                    severity="warning",
                    scope="procurement",
                    check="extreme_price_swings",
                    message="Sequential normalized price swings above 100% suggest dirty procurement rows, unit mismatch, or exceptional spot buys.",
                    frame=extreme_swings,
                    columns=["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "prev_unit_price", "unit_price", "pct_change", "effective_value"],
                    ignored_fingerprints=ignored_rows.get("procurement:extreme_price_swings", set()),
                )
            )

    summary = {
        "rows": int(len(proc)),
        "materials": int(proc["material_desc"].nunique()),
        "plants": int(proc["plant"].replace("", pd.NA).dropna().nunique()),
        "vendors": int(proc["vendor"].nunique()),
        "period_from": proc["doc_date"].min().strftime("%Y-%m-%d") if not proc.empty else "",
        "period_to": proc["doc_date"].max().strftime("%Y-%m-%d") if not proc.empty else "",
    }
    return findings, summary


def _collect_cross_findings(cons: pd.DataFrame, proc: pd.DataFrame) -> tuple[list[AuditFinding], dict]:
    findings: list[AuditFinding] = []
    cons_materials = set(cons["material_desc"].dropna())
    proc_materials = set(proc["material_desc"].dropna())
    cons_plants = set(cons["plant"].replace("", pd.NA).dropna())
    proc_plants = set(proc["plant"].replace("", pd.NA).dropna())

    material_only_in_cons = sorted(cons_materials - proc_materials)
    material_only_in_proc = sorted(proc_materials - cons_materials)
    if material_only_in_cons or material_only_in_proc:
        findings.append(
            _finding(
                severity="error",
                scope="cross_file",
                check="material_mismatch",
                message="Consumption and procurement files do not cover the same material descriptions.",
                examples=material_only_in_cons + material_only_in_proc,
            )
        )

    plants_only_in_cons = sorted(cons_plants - proc_plants)
    plants_only_in_proc = sorted(proc_plants - cons_plants)
    if plants_only_in_cons or plants_only_in_proc:
        findings.append(
            _finding(
                severity="warning",
                scope="cross_file",
                check="plant_mismatch",
                message="Plant coverage differs between consumption and procurement files. This is advisory and does not block upload by itself.",
                examples=(
                    [f"cons-only:{plant}" for plant in plants_only_in_cons]
                    + [f"proc-only:{plant}" for plant in plants_only_in_proc]
                ),
            )
        )

    summary = {
        "material_overlap": int(len(cons_materials & proc_materials)),
        "materials_in_consumption": int(len(cons_materials)),
        "materials_in_procurement": int(len(proc_materials)),
        "plant_overlap": int(len(cons_plants & proc_plants)),
        "plants_in_consumption": int(len(cons_plants)),
        "plants_in_procurement": int(len(proc_plants)),
    }
    return findings, summary


def build_seed_audit_report(
    cons_raw: pd.DataFrame,
    proc_raw: pd.DataFrame,
    consumption_filename: str,
    procurement_filename: str,
    ignored_rows: dict[str, set[str]] | None = None,
) -> dict:
    _validate_seed_frame(cons_raw, "consumption")
    _validate_seed_frame(proc_raw, "procurement")
    ignored_rows = ignored_rows or {}

    cons = _normalize_consumption_frame(cons_raw)
    proc = _normalize_procurement_frame(proc_raw)

    findings: list[AuditFinding] = []
    cons_findings, cons_summary = _collect_consumption_findings(cons_raw, cons, ignored_rows)
    proc_findings, proc_summary = _collect_procurement_findings(proc_raw, proc, ignored_rows)
    cross_findings, cross_summary = _collect_cross_findings(cons, proc)
    findings.extend(cons_findings)
    findings.extend(proc_findings)
    findings.extend(cross_findings)
    serialized_findings = [asdict(finding) for finding in findings]
    has_blocking_failures = any(
        finding["rows"] or finding["check"] == "material_mismatch"
        for finding in serialized_findings
    )

    return {
        "has_failures": has_blocking_failures,
        "files": {
            "consumption": consumption_filename,
            "procurement": procurement_filename,
        },
        "schema": {
            "consumption_columns_detected": sorted(_detect_consumption_fields([str(column).strip() for column in cons_raw.columns])),
            "procurement_columns_detected": sorted(_detect_procurement_fields([str(column).strip() for column in proc_raw.columns])),
        },
        "summary": {
            "consumption": cons_summary,
            "procurement": proc_summary,
            "cross_file": cross_summary,
        },
        "findings": serialized_findings,
    }


def audit_uploaded_seed_files(
    *,
    consumption_bytes: bytes,
    consumption_filename: str,
    procurement_bytes: bytes,
    procurement_filename: str,
) -> dict:
    cons_raw = _read_uploaded_seed_dataframe(consumption_bytes, consumption_filename)
    proc_raw = _read_uploaded_seed_dataframe(procurement_bytes, procurement_filename)
    return build_seed_audit_report(cons_raw, proc_raw, consumption_filename, procurement_filename)


def audit_current_seed_files() -> dict:
    cons_raw = _read_seed_frame(CONS_FILE)
    proc_raw = _read_seed_frame(PROC_FILE)
    return build_seed_audit_report(cons_raw, proc_raw, CONS_FILE, PROC_FILE)


def create_staged_seed_upload(
    *,
    consumption_bytes: bytes,
    consumption_filename: str,
    procurement_bytes: bytes,
    procurement_filename: str,
) -> tuple[str, dict]:
    cons_raw = _read_uploaded_seed_dataframe(consumption_bytes, consumption_filename)
    proc_raw = _read_uploaded_seed_dataframe(procurement_bytes, procurement_filename)
    report = build_seed_audit_report(cons_raw, proc_raw, consumption_filename, procurement_filename)

    token = uuid.uuid4().hex
    paths = _staging_paths(token)
    _write_frame(paths["consumption"], cons_raw, "Sheet1")
    _write_frame(paths["procurement"], proc_raw, "Data")
    _write_staged_meta(
        token,
        {
            "consumption_filename": consumption_filename,
            "procurement_filename": procurement_filename,
            "ignored_rows": {},
        },
    )
    return token, report


def get_staged_seed_report(token: str) -> dict:
    cons_raw, proc_raw, meta = _load_staged_frames(token)
    return build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows={key: set(values) for key, values in meta.get("ignored_rows", {}).items()},
    )


def delete_rows_from_staged_seed_upload(token: str, seed_type: str, finding_check: str, excel_rows: list[int]) -> dict:
    cons_raw, proc_raw, meta = _load_staged_frames(token)
    if seed_type not in {"consumption", "procurement"}:
        raise ValueError("Unsupported seed type for staged deletion.")
    normalized_rows = sorted({int(row) for row in excel_rows if int(row) >= 2})
    if not normalized_rows:
        raise ValueError("Select at least one row to delete.")
    report = build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows={key: set(values) for key, values in meta.get("ignored_rows", {}).items()},
    )
    finding = next((item for item in report["findings"] if item["check"] == finding_check and item["scope"] == seed_type), None)
    if not finding or not finding["rows"]:
        raise ValueError("The selected sanity-check subgroup is no longer available.")
    allowed_rows = {int(row) for row in finding.get("selectable_rows", [])}
    invalid_rows = [row for row in normalized_rows if row not in allowed_rows]
    if invalid_rows:
        raise ValueError("One or more selected rows are not deletable in this subgroup.")

    indices = [row - 2 for row in normalized_rows]
    if seed_type == "consumption":
        cons_raw = cons_raw.drop(index=[index for index in indices if index in cons_raw.index]).reset_index(drop=True)
    else:
        proc_raw = proc_raw.drop(index=[index for index in indices if index in proc_raw.index]).reset_index(drop=True)

    paths = _staging_paths(token)
    _write_frame(paths["consumption"], cons_raw, "Sheet1")
    _write_frame(paths["procurement"], proc_raw, "Data")
    return build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows={key: set(values) for key, values in meta.get("ignored_rows", {}).items()},
    )


def ignore_rows_from_staged_seed_upload(token: str, seed_type: str, finding_check: str, excel_rows: list[int]) -> dict:
    cons_raw, proc_raw, meta = _load_staged_frames(token)
    normalized_rows = sorted({int(row) for row in excel_rows if int(row) >= 2})
    if not normalized_rows:
        raise ValueError("Select at least one row to keep.")

    ignored_rows = {key: set(values) for key, values in meta.get("ignored_rows", {}).items()}
    report = build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows=ignored_rows,
    )
    finding = next((item for item in report["findings"] if item["check"] == finding_check and item["scope"] == seed_type), None)
    if not finding or not finding["rows"]:
        raise ValueError("The selected sanity-check subgroup is no longer available.")

    selected = [row for row in finding["rows"] if int(row["Excel Row"]) in normalized_rows]
    if not selected:
        raise ValueError("Select at least one valid row to keep.")

    key = f"{seed_type}:{finding_check}"
    current = set(ignored_rows.get(key, set()))
    for row in selected:
        payload = {column: row.get(column, "") for column in finding["columns"] if column != "Excel Row" and column != "Selection Status"}
        current.add(json.dumps(payload, sort_keys=True, default=str))
    ignored_rows[key] = current
    meta["ignored_rows"] = {name: sorted(values) for name, values in ignored_rows.items()}
    _write_staged_meta(token, meta)
    return get_staged_seed_report(token)


def normalize_price_unit_rows_in_staged_seed_upload(token: str, excel_rows: list[int]) -> dict:
    cons_raw, proc_raw, meta = _load_staged_frames(token)
    normalized_rows = sorted({int(row) for row in excel_rows if int(row) >= 2})
    if not normalized_rows:
        raise ValueError("Select at least one row to normalize.")

    ignored_rows = {key: set(values) for key, values in meta.get("ignored_rows", {}).items()}
    report = build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows=ignored_rows,
    )
    finding = next((item for item in report["findings"] if item["check"] == "price_unit_not_one" and item["scope"] == "procurement"), None)
    if not finding or not finding["rows"]:
        raise ValueError("No pending price-unit normalization rows are available.")
    allowed_rows = {int(row) for row in finding.get("selectable_rows", [])}
    invalid_rows = [row for row in normalized_rows if row not in allowed_rows]
    if invalid_rows:
        raise ValueError("One or more selected rows are not valid for price-unit normalization.")

    raw_view = _build_procurement_raw_view(proc_raw)
    target_indices = [row - 2 for row in normalized_rows]
    for index in target_indices:
        if index not in raw_view.index:
            continue
        price_unit = raw_view.at[index, "price_unit"]
        net_price = raw_view.at[index, "net_price"]
        if pd.notna(price_unit) and float(price_unit) not in {0.0, 1.0} and pd.notna(net_price):
            raw_view.at[index, "net_price"] = float(net_price) / float(price_unit)
            raw_view.at[index, "price_unit"] = 1.0

    column_lookup: dict[str, str] = {}
    for column in proc_raw.columns:
        key = str(column).lower().strip()
        if key == "net price":
            column_lookup["net_price"] = column
        elif "price unit" in key:
            column_lookup["price_unit"] = column

    if "net_price" not in column_lookup or "price_unit" not in column_lookup:
        raise ValueError("Could not locate Net Price and Price Unit columns in the staged procurement file.")

    for index in target_indices:
        if index not in raw_view.index:
            continue
        proc_raw.at[index, column_lookup["net_price"]] = raw_view.at[index, "net_price"]
        proc_raw.at[index, column_lookup["price_unit"]] = raw_view.at[index, "price_unit"]

    paths = _staging_paths(token)
    _write_frame(paths["procurement"], proc_raw, "Data")
    return get_staged_seed_report(token)


def apply_staged_seed_upload(token: str) -> dict:
    paths = _staging_paths(token)
    _, _, meta = _load_staged_frames(token)
    with open(paths["consumption"], "rb") as cons_handle:
        consumption_bytes = cons_handle.read()
    with open(paths["procurement"], "rb") as proc_handle:
        procurement_bytes = proc_handle.read()
    save_seed_workbooks(
        consumption_bytes=consumption_bytes,
        consumption_filename=meta["consumption_filename"],
        procurement_bytes=procurement_bytes,
        procurement_filename=meta["procurement_filename"],
    )
    get_data_store().reload()
    discard_staged_seed_upload(token)
    return meta


def get_procurement_rows_below_threshold(threshold: float = LOW_VALUE_THRESHOLD) -> dict:
    raw_proc = _read_seed_frame(PROC_FILE)
    _validate_seed_frame(raw_proc, "procurement")
    proc = _normalize_procurement_frame(raw_proc)
    low_rows = proc.loc[
        proc["effective_value"].fillna(float("inf")) < threshold,
        ["doc_date", "po_number", "plant", "material_code", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "unit_price"],
    ]
    rows = _serialize_rows(
        low_rows.sort_values(["doc_date", "po_number"], ascending=[False, False]),
        ["doc_date", "po_number", "plant", "material_code", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "unit_price"],
    )
    return {
        "threshold": threshold,
        "count": len(rows),
        "columns": list(rows[0].keys()) if rows else ["Excel Row", "doc_date", "po_number", "plant", "material_code", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "unit_price"],
        "rows": rows,
    }


def delete_procurement_rows_below_threshold(threshold: float = LOW_VALUE_THRESHOLD) -> dict:
    raw_proc = _read_seed_frame(PROC_FILE)
    _validate_seed_frame(raw_proc, "procurement")
    proc = _normalize_procurement_frame(raw_proc)
    low_indices = proc.index[proc["effective_value"].fillna(float("inf")) < threshold]
    preview = get_procurement_rows_below_threshold(threshold)
    if not len(low_indices):
        return preview

    cleaned = raw_proc.drop(index=low_indices)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        cleaned.to_excel(writer, index=False, sheet_name="Data")
    save_seed_workbook(buffer.getvalue(), os.path.basename(PROC_FILE), "procurement")
    get_data_store().reload()
    return preview
