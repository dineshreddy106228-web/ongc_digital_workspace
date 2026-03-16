from __future__ import annotations

"""Sanity analysis and cleanup helpers for inventory seed workbooks."""

from dataclasses import asdict, dataclass
from io import BytesIO
import json
import os
import shutil
import tempfile
import uuid

try:
    import pandas as pd
except ImportError:  # pragma: no cover – present only when ENABLE_INVENTORY=False
    pd = None  # type: ignore[assignment]

from app.core.services.inventory_intelligence import (
    CONS_FILE,
    PROC_FILE,
    _MB51_CONSUMPTION_ALIASES,
    _apply_header_aliases,
    _detect_consumption_fields,
    _detect_frame_variant,
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
        excel_row = row.get("source_excel_row")
        if pd.isna(excel_row):
            excel_row = int(index) + 2
        record = {"Excel Row": int(excel_row)}
        for column in columns:
            record[column] = _serialize_value(row.get(column))
        rows.append(record)
    return rows


def _attach_source_excel_row(frame: pd.DataFrame) -> pd.DataFrame:
    if "source_excel_row" in frame.columns:
        return frame
    result = frame.copy()
    result["source_excel_row"] = result.index + 2
    return result


def _row_fingerprint_from_series(row: pd.Series, columns: list[str]) -> str:
    payload = {column: _serialize_value(row.get(column)) for column in columns}
    return json.dumps(payload, sort_keys=True, default=str)


def _format_examples(rows: list[dict], columns: list[str], limit: int = 5) -> list[str]:
    examples: list[str] = []
    for row in rows[:limit]:
        examples.append(", ".join(f"{column}={row.get(column) or '∅'}" for column in columns))
    return examples


def _build_consumption_raw_view(df: pd.DataFrame) -> pd.DataFrame:
    variant = _detect_frame_variant([str(column).strip() for column in df.columns], "consumption")
    if variant == "mb51":
        frame = _apply_header_aliases(df, _MB51_CONSUMPTION_ALIASES)
        for column in [
            "plant",
            "material_code",
            "material_desc",
            "storage_location",
            "movement_type",
            "posting_date",
            "entry_qty",
            "uom",
            "usage_value",
        ]:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame["posting_date"] = pd.to_datetime(frame["posting_date"], errors="coerce")
        frame["entry_qty"] = pd.to_numeric(frame["entry_qty"], errors="coerce")
        frame["usage_value"] = pd.to_numeric(frame["usage_value"], errors="coerce")
        frame = frame.rename(columns={"entry_qty": "usage_qty"})
        return frame

    frame = _normalize_consumption_frame(df).copy()
    for column in ["plant", "reporting_plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "financial_year"]:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame


def _build_procurement_raw_view(df: pd.DataFrame) -> pd.DataFrame:
    frame = _normalize_procurement_frame(df).copy()
    for column in [
        "plant",
        "reporting_plant",
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
        "financial_year",
    ]:
        if column not in frame.columns:
            frame[column] = pd.NA
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

    variant = _detect_frame_variant([str(column).strip() for column in raw_df.columns], "consumption")
    if variant == "legacy":
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

    if variant == "mb51" and cons.empty:
        findings.append(
            _finding(
                severity="error",
                scope="consumption",
                check="no_supported_movements",
                message="No MB51 rows matched the supported consumption movement set (201/221/261 net of 202/222/262).",
                frame=_attach_source_excel_row(
                    raw_view.loc[:, ["plant", "material_code", "material_desc", "movement_type", "posting_date", "usage_qty", "uom", "usage_value"]].head(25)
                ),
                columns=["plant", "material_code", "material_desc", "movement_type", "posting_date", "usage_qty", "uom", "usage_value"],
            )
        )

    if variant == "mb51" and not raw_view.empty:
        raw_materials = set(raw_view["material_desc"].dropna().astype(str).str.strip())
        normalized_materials = set(cons["material_desc"].dropna().astype(str).str.strip())
        excluded_materials = sorted(material for material in raw_materials - normalized_materials if material)
        if excluded_materials:
            exclusion_examples: list[str] = []
            for material in excluded_materials:
                rows = raw_view.loc[raw_view["material_desc"].astype(str).str.strip() == material]
                movement_types = sorted({str(value).strip() for value in rows["movement_type"].dropna().tolist() if str(value).strip()})
                movement_label = "/".join(movement_types) if movement_types else "none"
                exclusion_examples.append(f"{material} (movement types: {movement_label})")
            findings.append(
                _finding(
                    severity="warning",
                    scope="consumption",
                    check="excluded_non_consumption_materials",
                    message=(
                        "Some MB51 materials were excluded from consumption because they only contain "
                        "movement types outside the supported consumption set (201/221/261 net of 202/222/262)."
                    ),
                    examples=exclusion_examples,
                )
            )

    if variant == "mb51":
        invalid_periods = _attach_source_excel_row(cons.loc[
            cons["posting_date"].isna(),
            ["plant", "material_code", "material_desc", "movement_type", "storage_location", "usage_qty", "uom", "usage_value", "source_excel_row"],
        ])
        invalid_check = "invalid_posting_date"
        invalid_message = "Rows have unparseable posting dates."
        invalid_columns = ["plant", "material_code", "material_desc", "movement_type", "storage_location", "usage_qty", "uom", "usage_value"]
    else:
        invalid_periods = _attach_source_excel_row(cons.loc[
            cons["month"].isna() | cons["year"].isna(),
            ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "source_excel_row"],
        ])
        invalid_check = "invalid_period"
        invalid_message = "Rows have unparseable reporting periods."
        invalid_columns = ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"]

    if not invalid_periods.empty:
        findings.append(
            _finding(
                severity="error",
                scope="consumption",
                check=invalid_check,
                message=invalid_message,
                frame=invalid_periods,
                columns=invalid_columns,
                ignored_fingerprints=ignored_rows.get(f"consumption:{invalid_check}", set()),
            )
        )

    if variant == "mb51":
        non_positive_qty = _attach_source_excel_row(cons.loc[
            cons["usage_qty"].abs().fillna(0) <= 0,
            ["plant", "material_code", "material_desc", "movement_type", "storage_location", "posting_date", "usage_qty", "uom", "usage_value", "source_excel_row"],
        ])
        qty_columns = ["plant", "material_code", "material_desc", "movement_type", "storage_location", "posting_date", "usage_qty", "uom", "usage_value"]
        qty_fingerprints = qty_columns
        duplicate_keys = ["plant", "material_code", "movement_type", "posting_date", "material_document", "material_document_item", "usage_qty", "usage_value"]
        duplicate_columns = ["plant", "material_code", "material_desc", "movement_type", "storage_location", "posting_date", "usage_qty", "uom", "usage_value"]
        duplicate_message = "Potential duplicate MB51 rows were found on plant/material/movement/document keys. One row per duplicate set is frozen as the keeper; the remaining rows can be deleted."
        duplicate_frame = _attach_source_excel_row(cons.loc[
            cons.duplicated(duplicate_keys, keep=False),
            duplicate_columns + ["material_document", "material_document_item", "source_excel_row"],
        ])
    else:
        non_positive_qty = _attach_source_excel_row(cons.loc[
            cons["usage_qty"].fillna(0) <= 0,
            ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "source_excel_row"],
        ])
        qty_columns = ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"]
        qty_fingerprints = qty_columns
        duplicate_keys = ["plant", "material", "storage_location", "month_raw", "usage_qty", "usage_value"]
        duplicate_columns = ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value"]
        duplicate_message = "Potential duplicate consumption rows were found on plant/material/location/period/value keys. One row per duplicate set is frozen as the keeper; the remaining rows can be deleted."
        duplicate_frame = _attach_source_excel_row(cons.loc[
            cons.duplicated(duplicate_keys, keep=False),
            duplicate_columns + ["source_excel_row"],
        ])

    if not non_positive_qty.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="consumption",
                check="non_positive_quantity",
                message="Rows have zero or invalid consumption quantity after normalization.",
                frame=non_positive_qty,
                columns=qty_columns,
                fingerprint_columns=qty_fingerprints,
                ignored_fingerprints=ignored_rows.get("consumption:non_positive_quantity", set()),
            )
        )

    if variant == "legacy":
        negative_value = _attach_source_excel_row(
            cons.loc[
                cons["usage_value"].fillna(0) < 0,
                ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "source_excel_row"],
            ]
        )
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

    if not duplicate_frame.empty:
        duplicate_view = duplicate_frame.sort_index().copy()
        duplicate_view["Selection Status"] = "Delete Candidate"
        frozen_indices: list[int] = []
        for _, group in duplicate_view.groupby(duplicate_keys, sort=False):
            keep_index = group.index[0]
            duplicate_view.loc[keep_index, "Selection Status"] = "Keep (Frozen)"
            frozen_indices.append(int(keep_index))
        selectable_rows = [
            int(duplicate_view.loc[index, "source_excel_row"])
            for index in duplicate_view.index
            if int(index) not in frozen_indices
        ]
        findings.append(
            _finding(
                severity="warning",
                scope="consumption",
                check="duplicate_rows",
                message=duplicate_message,
                frame=duplicate_view,
                columns=["Selection Status"] + duplicate_columns,
                selectable_rows=selectable_rows,
                fingerprint_columns=duplicate_columns,
                ignored_fingerprints=ignored_rows.get("consumption:duplicate_rows", set()),
            )
        )

    period_from = cons["posting_date"].min().strftime("%Y-%m-%d") if variant == "mb51" and not cons.empty and pd.notna(cons["posting_date"].min()) else _format_consumption_period(cons, "first")
    period_to = cons["posting_date"].max().strftime("%Y-%m-%d") if variant == "mb51" and not cons.empty and pd.notna(cons["posting_date"].max()) else _format_consumption_period(cons, "last")
    summary = {
        "rows": int(len(cons)),
        "materials": int(cons["material_desc"].nunique()) if "material_desc" in cons.columns else 0,
        "plants": int(cons["reporting_plant"].replace("", pd.NA).dropna().nunique()) if "reporting_plant" in cons.columns else 0,
        "storage_locations": int(cons["storage_location"].replace("", pd.NA).dropna().nunique()) if "storage_location" in cons.columns else 0,
        "period_from": period_from,
        "period_to": period_to,
    }
    return findings, summary


def _collect_procurement_findings(proc_raw: pd.DataFrame, proc: pd.DataFrame, ignored_rows: dict[str, set[str]]) -> tuple[list[AuditFinding], dict]:
    findings: list[AuditFinding] = []
    proc_raw_view = _build_procurement_raw_view(proc_raw)

    missing_plant = _attach_source_excel_row(proc.loc[
        proc["plant"].fillna("").astype(str).str.strip().eq(""),
        ["po_number", "doc_date", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "source_excel_row"],
    ])
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

    invalid_date = _attach_source_excel_row(proc.loc[
        proc["doc_date"].isna(),
        ["po_number", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "source_excel_row"],
    ])
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

    non_positive_qty = _attach_source_excel_row(proc.loc[
        proc["order_qty"].fillna(0) <= 0,
        ["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "source_excel_row"],
    ])
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

    non_positive_unit_price = _attach_source_excel_row(proc.loc[
        proc["unit_price"].fillna(0) <= 0,
        ["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "unit_price", "effective_value", "source_excel_row"],
    ])
    if not non_positive_unit_price.empty:
        findings.append(
            _finding(
                severity="error",
                scope="procurement",
                check="non_positive_unit_price",
                message="Rows have non-positive derived unit price after calculating effective value divided by order quantity.",
                frame=non_positive_unit_price,
                columns=["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "unit_price", "effective_value"],
                ignored_fingerprints=ignored_rows.get("procurement:non_positive_unit_price", set()),
            )
        )

    price_view = proc.dropna(subset=["doc_date", "unit_price"]).sort_values(["material_desc", "doc_date", "po_number"]).copy()
    price_view["prev_unit_price"] = price_view.groupby("material_desc")["unit_price"].shift(1)
    price_view["pct_change"] = (
        (price_view["unit_price"] - price_view["prev_unit_price"]) / price_view["prev_unit_price"]
    ) * 100
    extreme_swings = _attach_source_excel_row(price_view.loc[
        (price_view["prev_unit_price"] > 0)
        & (price_view["pct_change"].abs() > 100)
        & (price_view["effective_value"].fillna(float("inf")) >= LOW_VALUE_THRESHOLD),
        ["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "prev_unit_price", "unit_price", "pct_change", "effective_value", "source_excel_row"],
    ])
    if not extreme_swings.empty:
        findings.append(
            _finding(
                severity="warning",
                scope="procurement",
                check="extreme_price_swings",
                message="Sequential swings above 100% in derived unit price suggest dirty procurement rows, unit mismatch, or exceptional spot buys.",
                frame=extreme_swings,
                columns=["po_number", "doc_date", "plant", "material_desc", "vendor", "order_qty", "order_unit", "prev_unit_price", "unit_price", "pct_change", "effective_value"],
                ignored_fingerprints=ignored_rows.get("procurement:extreme_price_swings", set()),
            )
        )

    summary = {
        "rows": int(len(proc)),
        "materials": int(proc["material_desc"].nunique()),
        "plants": int(proc["reporting_plant"].replace("", pd.NA).dropna().nunique()) if "reporting_plant" in proc.columns else 0,
        "vendors": int(proc["vendor"].nunique()),
        "period_from": proc["doc_date"].min().strftime("%Y-%m-%d") if not proc.empty else "",
        "period_to": proc["doc_date"].max().strftime("%Y-%m-%d") if not proc.empty else "",
    }
    return findings, summary


def _collect_cross_findings(cons: pd.DataFrame, proc: pd.DataFrame) -> tuple[list[AuditFinding], dict]:
    findings: list[AuditFinding] = []
    cons_materials = set(cons["material_desc"].dropna())
    proc_materials = set(proc["material_desc"].dropna())
    cons_plants = set(cons["reporting_plant"].replace("", pd.NA).dropna()) if "reporting_plant" in cons.columns else set()
    proc_plants = set(proc["reporting_plant"].replace("", pd.NA).dropna()) if "reporting_plant" in proc.columns else set()

    material_only_in_cons = sorted(cons_materials - proc_materials)
    material_only_in_proc = sorted(proc_materials - cons_materials)
    if material_only_in_cons or material_only_in_proc:
        cons_only_count = len(material_only_in_cons)
        proc_only_count = len(material_only_in_proc)
        findings.append(
            _finding(
                severity="warning",
                scope="cross_file",
                check="material_mismatch",
                message=(
                    "Consumption and procurement cover different material sets for this FY. "
                    f"Consumption-only materials: {cons_only_count}. Procurement-only materials: {proc_only_count}. "
                    "This is usually a coverage difference, not a bad upload: some materials may have been consumed from "
                    "opening stock, transferred stock, or prior-year purchases, while other materials may have been procured "
                    "in the FY but not yet consumed."
                ),
                examples=(
                    [f"cons-only:{value}" for value in material_only_in_cons]
                    + [f"proc-only:{value}" for value in material_only_in_proc]
                ),
            )
        )

    plants_only_in_cons = sorted(cons_plants - proc_plants)
    plants_only_in_proc = sorted(proc_plants - cons_plants)
    if plants_only_in_cons or plants_only_in_proc:
        cons_only_count = len(plants_only_in_cons)
        proc_only_count = len(plants_only_in_proc)
        findings.append(
            _finding(
                severity="warning",
                scope="cross_file",
                check="plant_mismatch",
                message=(
                    "Consumption and procurement cover different reporting plants for this FY. "
                    f"Consumption-only plants: {cons_only_count}. Procurement-only plants: {proc_only_count}. "
                    "This is advisory and often happens when one file includes operating or well-service units that did not "
                    "raise procurement documents in the same FY, or when procurement happened at a different parent plant."
                ),
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
        finding["severity"] == "error" and (finding["rows"] or finding["examples"])
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


def _resolve_raw_row_indices(frame: pd.DataFrame, seed_type: str, excel_rows: list[int]) -> list[int]:
    # Excel row numbers in the audit report refer to the original uploaded sheet,
    # so the staged raw frame can be addressed directly by row-2. Using the
    # normalized frame indices here is incorrect because normalization can filter
    # and reorder rows before we attempt to delete from the raw staged upload.
    del seed_type  # row resolution is based on the raw staged frame only
    return [
        row - 2
        for row in sorted({int(row) for row in excel_rows if int(row) >= 2})
        if 0 <= (row - 2) < len(frame)
    ]


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

    indices = _resolve_raw_row_indices(cons_raw if seed_type == "consumption" else proc_raw, seed_type, normalized_rows)
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

def bulk_cleanup_staged_seed_upload(
    token: str,
    delete_decisions: dict[str, list[int]],
    keep_decisions: dict[str, list[int]],
) -> dict:
    """Apply all keep/delete decisions across every sanity-check finding in one pass.

    Parameters
    ----------
    token:
        Staged-upload token.
    delete_decisions:
        Mapping of ``"{scope}:{check}"`` → list of excel row numbers to delete.
    keep_decisions:
        Mapping of ``"{scope}:{check}"`` → list of excel row numbers to ignore
        (mark as intentionally kept so they no longer block the upload).

    Returns
    -------
    The updated audit report dict after applying all decisions.
    """
    cons_raw, proc_raw, meta = _load_staged_frames(token)
    ignored_rows: dict[str, set[str]] = {
        key: set(values) for key, values in meta.get("ignored_rows", {}).items()
    }

    # Build the current report once so we can validate selectable rows and
    # derive fingerprints for the "keep/ignore" logic.
    report = build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows=ignored_rows,
    )

    cons_drop: list[int] = []
    proc_drop: list[int] = []

    for finding in report["findings"]:
        scope: str = finding["scope"]
        check: str = finding["check"]
        dict_key = f"{scope}:{check}"

        # ── Deletions ─────────────────────────────────────────────────────
        delete_excel_rows = delete_decisions.get(dict_key, [])
        if delete_excel_rows and finding.get("rows"):
            allowed = {int(r) for r in finding.get("selectable_rows", [])}
            valid_delete = [r for r in delete_excel_rows if r in allowed]
            if valid_delete:
                target_frame = cons_raw if scope == "consumption" else proc_raw
                indices = _resolve_raw_row_indices(target_frame, scope, valid_delete)
                if scope == "consumption":
                    cons_drop.extend(indices)
                else:
                    proc_drop.extend(indices)

        # ── Keeps / ignores ────────────────────────────────────────────────
        keep_excel_rows = keep_decisions.get(dict_key, [])
        if keep_excel_rows and finding.get("rows"):
            keep_set = set(keep_excel_rows)
            selected = [
                row for row in finding["rows"]
                if int(row["Excel Row"]) in keep_set
            ]
            if selected:
                current = set(ignored_rows.get(dict_key, set()))
                for row in selected:
                    payload = {
                        col: row.get(col, "")
                        for col in finding["columns"]
                        if col not in ("Excel Row", "Selection Status")
                    }
                    current.add(json.dumps(payload, sort_keys=True, default=str))
                ignored_rows[dict_key] = current

    # ── Apply frame deletions in a single pass ─────────────────────────────
    if cons_drop:
        unique_cons = list({i for i in cons_drop if i in cons_raw.index})
        cons_raw = cons_raw.drop(index=unique_cons).reset_index(drop=True)
    if proc_drop:
        unique_proc = list({i for i in proc_drop if i in proc_raw.index})
        proc_raw = proc_raw.drop(index=unique_proc).reset_index(drop=True)

    # ── Persist updated staged files and metadata ──────────────────────────
    paths = _staging_paths(token)
    _write_frame(paths["consumption"], cons_raw, "Sheet1")
    _write_frame(paths["procurement"], proc_raw, "Data")
    meta["ignored_rows"] = {name: sorted(values) for name, values in ignored_rows.items()}
    _write_staged_meta(token, meta)

    # ── Return refreshed audit report ──────────────────────────────────────
    return build_seed_audit_report(
        cons_raw,
        proc_raw,
        meta["consumption_filename"],
        meta["procurement_filename"],
        ignored_rows={key: set(values) for key, values in meta.get("ignored_rows", {}).items()},
    )


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


def _load_procurement_preview_frame(token: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    if token:
        _, raw_proc, _ = _load_staged_frames(token)
        return raw_proc, _normalize_procurement_frame(raw_proc), True

    raw_proc = _read_seed_frame(PROC_FILE)
    _validate_seed_frame(raw_proc, "procurement")
    return raw_proc, _normalize_procurement_frame(raw_proc), False


def get_procurement_rows_below_threshold(
    threshold: float = LOW_VALUE_THRESHOLD,
    token: str | None = None,
) -> dict:
    _, proc, is_staged = _load_procurement_preview_frame(token)
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
        "is_staged": is_staged,
        "source_label": "staged procurement file" if is_staged else "current ingested procurement file",
        "count": len(rows),
        "columns": list(rows[0].keys()) if rows else ["Excel Row", "doc_date", "po_number", "plant", "material_code", "material_desc", "vendor", "order_qty", "order_unit", "effective_value", "unit_price"],
        "rows": rows,
    }


def delete_procurement_rows_below_threshold(
    threshold: float = LOW_VALUE_THRESHOLD,
    token: str | None = None,
) -> dict:
    raw_proc, proc, is_staged = _load_procurement_preview_frame(token)
    low_indices = proc.index[proc["effective_value"].fillna(float("inf")) < threshold]
    preview = get_procurement_rows_below_threshold(threshold, token=token)
    if not len(low_indices):
        return preview

    cleaned = raw_proc.drop(index=low_indices)

    if is_staged:
        paths = _staging_paths(token)
        _write_frame(paths["procurement"], cleaned, "Data")
    else:
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            cleaned.to_excel(writer, index=False, sheet_name="Data")
        save_seed_workbook(buffer.getvalue(), os.path.basename(PROC_FILE), "procurement")
        get_data_store().reload()

    return preview
