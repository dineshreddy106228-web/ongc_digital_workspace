"""Monthly inventory upload processing and report generation."""

from __future__ import annotations

import calendar
import json
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from sqlalchemy.orm import undefer

from app.extensions import db
from app.models.inventory.monthly_upload_batch import InventoryMonthlyUploadBatch
from app.core.services.inventory_intelligence import (
    _detect_frame_variant,
    _normalize_consumption_frame,
    _normalize_mc9_frame,
    _normalize_procurement_frame,
    _normalize_text,
    _preferred_uom,
    _quantity_to_mt,
    _read_uploaded_seed_dataframe,
    _safe_float,
    _validate_seed_frame,
)


XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
PDF_MIME = "application/pdf"
HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
SECTION_FILL = PatternFill("solid", fgColor="DCE6F1")
SECTION_FONT = Font(color="203864", bold=True)


@dataclass(frozen=True)
class MonthlyInventoryReportPackage:
    report_year: int
    report_month: int
    report_label: str
    mb51_filename: str
    me2m_filename: str
    mc9_filename: str
    mb51_row_count: int
    me2m_row_count: int
    mc9_row_count: int
    excel_filename: str
    excel_bytes: bytes
    pdf_filename: str
    pdf_bytes: bytes
    summary: dict[str, Any]


def _validate_report_period(report_year: int, report_month: int) -> None:
    if report_year < 2000 or report_year > 2100:
        raise ValueError("Select a valid report year for the monthly update.")
    if report_month < 1 or report_month > 12:
        raise ValueError("Select a valid report month for the monthly update.")


def _month_label(report_year: int, report_month: int) -> str:
    return f"{calendar.month_abbr[int(report_month)]} {int(report_year)}"


def _month_token(report_year: int, report_month: int) -> str:
    return f"{int(report_year):04d}-{int(report_month):02d}"


def _available_month_labels(frame: pd.DataFrame) -> str:
    if frame.empty or "year" not in frame.columns or "month" not in frame.columns:
        return "none"
    months = (
        frame.loc[frame["year"].notna() & frame["month"].notna(), ["year", "month"]]
        .drop_duplicates()
        .sort_values(["year", "month"])
        .itertuples(index=False, name=None)
    )
    labels = [f"{calendar.month_abbr[int(month)]} {int(year)}" for year, month in months]
    return ", ".join(labels) if labels else "none"


def _filter_month(frame: pd.DataFrame, report_year: int, report_month: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    years = pd.to_numeric(frame.get("year"), errors="coerce")
    months = pd.to_numeric(frame.get("month"), errors="coerce")
    mask = years.eq(int(report_year)) & months.eq(int(report_month))
    return frame.loc[mask].copy()


def _previous_month(report_year: int, report_month: int) -> tuple[int, int]:
    if int(report_month) == 1:
        return int(report_year) - 1, 12
    return int(report_year), int(report_month) - 1


def _material_key_columns() -> list[str]:
    return ["material_code", "material_desc"]


def _canonical_material_code(value) -> str:
    text = _normalize_text(value).upper()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text


def _material_display(code: str, desc: str) -> str:
    code_text = _normalize_text(code)
    desc_text = _normalize_text(desc)
    if code_text and desc_text:
        return f"{desc_text} ({code_text})"
    return desc_text or code_text or "Unknown Material"


def _rows_to_dicts(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        item: dict[str, Any] = {}
        for column, value in row.items():
            if isinstance(value, pd.Timestamp):
                item[column] = value.strftime("%Y-%m-%d")
            elif pd.isna(value):
                item[column] = ""
            else:
                item[column] = value
        rows.append(item)
    return rows


def _summarize_consumption(consumption: pd.DataFrame) -> pd.DataFrame:
    if consumption.empty:
        return pd.DataFrame(
            columns=[
                "material_code",
                "material_desc",
                "actual_uom",
                "monthly_usage_qty_actual",
                "monthly_usage_qty_mt",
                "monthly_usage_value",
                "plant_count",
                "movement_rows",
            ]
        )

    frame = consumption.copy()
    frame["material_code"] = frame["material_code"].map(_canonical_material_code)
    frame["actual_usage_qty"] = pd.to_numeric(
        frame.get("actual_usage_qty", frame.get("usage_qty")),
        errors="coerce",
    )
    frame["usage_value"] = pd.to_numeric(frame.get("usage_value"), errors="coerce")
    frame["usage_qty_mt"] = frame.apply(
        lambda row: _quantity_to_mt(row.get("actual_usage_qty"), row.get("actual_uom") or row.get("uom")),
        axis=1,
    )
    grouped = (
        frame.groupby(_material_key_columns(), as_index=False)
        .agg(
            actual_uom=("actual_uom", _preferred_uom),
            monthly_usage_qty_actual=("actual_usage_qty", "sum"),
            monthly_usage_qty_mt=("usage_qty_mt", "sum"),
            monthly_usage_value=("usage_value", "sum"),
            plant_count=("reporting_plant", "nunique"),
            movement_rows=("material_desc", "size"),
        )
        .sort_values(["monthly_usage_value", "monthly_usage_qty_mt", "material_desc"], ascending=[False, False, True])
    )
    return grouped


def _summarize_mc9(mc9: pd.DataFrame) -> pd.DataFrame:
    if mc9.empty:
        return pd.DataFrame(
            columns=[
                "material_code",
                "material_desc",
                "usage_uom",
                "stock_uom",
                "monthly_usage_qty_actual",
                "monthly_usage_qty_mt",
                "monthly_usage_value",
                "closing_stock_qty_actual",
                "closing_stock_qty_mt",
                "closing_stock_value",
                "plant_count",
                "stock_rows",
            ]
        )

    frame = mc9.copy()
    frame["material_code"] = frame["material_code"].map(_canonical_material_code)
    for column in ["usage_qty", "usage_value", "stock_qty", "stock_value"]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame["usage_qty_mt"] = frame.apply(
        lambda row: _quantity_to_mt(row.get("usage_qty"), row.get("uom")),
        axis=1,
    )
    frame["stock_qty_mt"] = frame.apply(
        lambda row: _quantity_to_mt(row.get("stock_qty"), row.get("stock_uom")),
        axis=1,
    )
    grouped = (
        frame.groupby(_material_key_columns(), as_index=False)
        .agg(
            usage_uom=("actual_uom", _preferred_uom),
            stock_uom=("actual_stock_uom", _preferred_uom),
            monthly_usage_qty_actual=("actual_usage_qty", "sum"),
            monthly_usage_qty_mt=("usage_qty_mt", "sum"),
            monthly_usage_value=("usage_value", "sum"),
            closing_stock_qty_actual=("actual_stock_qty", "sum"),
            closing_stock_qty_mt=("stock_qty_mt", "sum"),
            closing_stock_value=("stock_value", "sum"),
            plant_count=("reporting_plant", "nunique"),
            stock_rows=("material_desc", "size"),
        )
        .sort_values(["closing_stock_value", "monthly_usage_value", "material_desc"], ascending=[False, False, True])
    )
    return grouped


def _summarize_opening_stock(mc9_all: pd.DataFrame, report_year: int, report_month: int) -> pd.DataFrame:
    if mc9_all.empty:
        return pd.DataFrame(
            columns=[
                "material_code",
                "material_desc",
                "opening_stock_uom",
                "opening_stock_qty_actual",
                "opening_stock_qty_mt",
                "opening_stock_value",
            ]
        )

    previous_year, previous_month = _previous_month(report_year, report_month)
    previous_frame = _filter_month(mc9_all, previous_year, previous_month)
    if previous_frame.empty:
        return pd.DataFrame(
            columns=[
                "material_code",
                "material_desc",
                "opening_stock_uom",
                "opening_stock_qty_actual",
                "opening_stock_qty_mt",
                "opening_stock_value",
            ]
        )

    frame = previous_frame.copy()
    frame["material_code"] = frame["material_code"].map(_canonical_material_code)
    for column in ["stock_qty", "stock_value"]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame["stock_qty_mt"] = frame.apply(
        lambda row: _quantity_to_mt(row.get("stock_qty"), row.get("stock_uom")),
        axis=1,
    )
    grouped = (
        frame.groupby(_material_key_columns(), as_index=False)
        .agg(
            opening_stock_uom=("actual_stock_uom", _preferred_uom),
            opening_stock_qty_actual=("actual_stock_qty", "sum"),
            opening_stock_qty_mt=("stock_qty_mt", "sum"),
            opening_stock_value=("stock_value", "sum"),
        )
        .sort_values(["opening_stock_value", "material_desc"], ascending=[False, True])
    )
    return grouped


def _summarize_procurement(procurement: pd.DataFrame) -> pd.DataFrame:
    if procurement.empty:
        return pd.DataFrame(
            columns=[
                "material_code",
                "material_desc",
                "order_unit",
                "ordered_qty",
                "procured_qty",
                "open_qty",
                "effective_value",
                "vendor_count",
                "po_count",
            ]
        )

    frame = procurement.copy()
    frame["material_code"] = frame["material_code"].map(_canonical_material_code)
    for column in ["order_qty", "procured_qty", "still_to_be_delivered_qty", "effective_value"]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    grouped = (
        frame.groupby(_material_key_columns(), as_index=False)
        .agg(
            order_unit=("order_unit", _preferred_uom),
            ordered_qty=("order_qty", "sum"),
            procured_qty=("procured_qty", "sum"),
            open_qty=("still_to_be_delivered_qty", "sum"),
            effective_value=("effective_value", "sum"),
            vendor_count=("vendor", "nunique"),
            po_count=("po_number", "nunique"),
        )
        .sort_values(["effective_value", "open_qty", "material_desc"], ascending=[False, False, True])
    )
    return grouped


def _build_material_rollup(
    opening_stock_summary: pd.DataFrame,
    consumption_summary: pd.DataFrame,
    mc9_summary: pd.DataFrame,
    procurement_summary: pd.DataFrame,
) -> pd.DataFrame:
    key_columns = _material_key_columns()
    rollup = opening_stock_summary.merge(
        consumption_summary,
        on=key_columns,
        how="outer",
    ).merge(
        mc9_summary,
        on=key_columns,
        how="outer",
        suffixes=("_mb51", "_mc9"),
    ).merge(
        procurement_summary,
        on=key_columns,
        how="outer",
    )

    if rollup.empty:
        return pd.DataFrame(
            columns=[
                "material_code",
                "material_desc",
                "opening_stock_uom",
                "opening_stock_qty_actual",
                "opening_stock_qty_mt",
                "opening_stock_value",
                "consumption_uom",
                "mb51_usage_qty_actual",
                "mb51_usage_qty_mt",
                "mb51_usage_value",
                "mc9_usage_uom",
                "mc9_usage_qty_actual",
                "mc9_usage_qty_mt",
                "mc9_usage_value",
                "stock_uom",
                "closing_stock_qty_actual",
                "closing_stock_qty_mt",
                "closing_stock_value",
                "procurement_uom",
                "ordered_qty",
                "procured_qty",
                "open_qty",
                "procurement_value",
                "vendor_count",
            ]
        )

    rename_map = {
        "actual_uom": "consumption_uom",
        "monthly_usage_qty_actual_mb51": "mb51_usage_qty_actual",
        "monthly_usage_qty_mt_mb51": "mb51_usage_qty_mt",
        "monthly_usage_value_mb51": "mb51_usage_value",
        "usage_uom": "mc9_usage_uom",
        "monthly_usage_qty_actual_mc9": "mc9_usage_qty_actual",
        "monthly_usage_qty_mt_mc9": "mc9_usage_qty_mt",
        "monthly_usage_value_mc9": "mc9_usage_value",
        "order_unit": "procurement_uom",
        "effective_value": "procurement_value",
    }

    # Align MC9 columns after the merge suffixes.
    for source, target in [
        ("monthly_usage_qty_actual_mc9", "mc9_usage_qty_actual"),
        ("monthly_usage_qty_mt_mc9", "mc9_usage_qty_mt"),
        ("monthly_usage_value_mc9", "mc9_usage_value"),
        ("plant_count_mc9", "mc9_plant_count"),
        ("stock_rows", "mc9_row_count"),
    ]:
        if source in rollup.columns:
            rename_map[source] = target

    if "plant_count_mb51" in rollup.columns:
        rename_map["plant_count_mb51"] = "mb51_plant_count"
    if "movement_rows" in rollup.columns:
        rename_map["movement_rows"] = "mb51_row_count"
    if "usage_uom_mc9" in rollup.columns:
        rename_map["usage_uom_mc9"] = "mc9_usage_uom"
    if "stock_uom_mc9" in rollup.columns:
        rename_map["stock_uom_mc9"] = "stock_uom"

    rollup = rollup.rename(columns=rename_map)

    numeric_columns = [
        "opening_stock_qty_actual",
        "opening_stock_qty_mt",
        "opening_stock_value",
        "mb51_usage_qty_actual",
        "mb51_usage_qty_mt",
        "mb51_usage_value",
        "mc9_usage_qty_actual",
        "mc9_usage_qty_mt",
        "mc9_usage_value",
        "closing_stock_qty_actual",
        "closing_stock_qty_mt",
        "closing_stock_value",
        "ordered_qty",
        "procured_qty",
        "open_qty",
        "procurement_value",
        "vendor_count",
        "mb51_plant_count",
        "mc9_plant_count",
    ]
    for column in numeric_columns:
        if column in rollup.columns:
            rollup[column] = pd.to_numeric(rollup[column], errors="coerce").fillna(0.0)

    for column in ["opening_stock_uom", "consumption_uom", "mc9_usage_uom", "stock_uom", "procurement_uom"]:
        if column not in rollup.columns:
            rollup[column] = ""
        rollup[column] = rollup[column].fillna("").astype(str)

    rollup["material_desc"] = rollup["material_desc"].fillna("")
    rollup["material_code"] = rollup["material_code"].fillna("")
    missing_opening_mask = (
        rollup["opening_stock_uom"].fillna("").astype(str).str.strip().eq("")
        & rollup["opening_stock_qty_actual"].eq(0.0)
        & rollup["opening_stock_qty_mt"].eq(0.0)
        & rollup["opening_stock_value"].eq(0.0)
    )
    if missing_opening_mask.any():
        for index in rollup.index[missing_opening_mask]:
            stock_uom = _normalize_text(rollup.at[index, "stock_uom"]).upper()
            consumption_uom = _normalize_text(rollup.at[index, "consumption_uom"]).upper()
            estimated_mt = (
                _safe_float(rollup.at[index, "closing_stock_qty_mt"])
                - _safe_float(rollup.at[index, "mb51_usage_qty_mt"])
            )
            estimated_value = (
                _safe_float(rollup.at[index, "closing_stock_value"])
                - _safe_float(rollup.at[index, "mb51_usage_value"])
            )
            rollup.at[index, "opening_stock_qty_mt"] = estimated_mt
            rollup.at[index, "opening_stock_value"] = estimated_value

            if stock_uom and (not consumption_uom or stock_uom == consumption_uom):
                rollup.at[index, "opening_stock_uom"] = stock_uom
                rollup.at[index, "opening_stock_qty_actual"] = (
                    _safe_float(rollup.at[index, "closing_stock_qty_actual"])
                    - _safe_float(rollup.at[index, "mb51_usage_qty_actual"])
                )
            elif consumption_uom and not stock_uom:
                rollup.at[index, "opening_stock_uom"] = consumption_uom
                rollup.at[index, "opening_stock_qty_actual"] = (
                    _safe_float(rollup.at[index, "closing_stock_qty_actual"])
                    - _safe_float(rollup.at[index, "mb51_usage_qty_actual"])
                )
            else:
                rollup.at[index, "opening_stock_uom"] = "MT"
                rollup.at[index, "opening_stock_qty_actual"] = estimated_mt

    rollup = rollup.sort_values(
        ["mb51_usage_value", "procurement_value", "closing_stock_value", "material_desc"],
        ascending=[False, False, False, True],
    )
    return rollup


def _build_stock_movement_sheet(material_rollup: pd.DataFrame) -> pd.DataFrame:
    movement = material_rollup.loc[
        :,
        [
            "material_code",
            "material_desc",
            "opening_stock_uom",
            "opening_stock_qty_actual",
            "opening_stock_qty_mt",
            "consumption_uom",
            "mb51_usage_qty_actual",
            "mb51_usage_qty_mt",
            "stock_uom",
            "closing_stock_qty_actual",
            "closing_stock_qty_mt",
        ],
    ].copy()
    movement = movement.rename(
        columns={
            "material_code": "Material",
            "material_desc": "Name of Chemical",
            "opening_stock_uom": "Opening Stock UoM",
            "opening_stock_qty_actual": "Opening Stock Qty (Actual)",
            "opening_stock_qty_mt": "Opening Stock Qty (MT)",
            "consumption_uom": "Consumption UoM",
            "mb51_usage_qty_actual": "Consumption Qty (Actual)",
            "mb51_usage_qty_mt": "Consumption Qty (MT)",
            "stock_uom": "Closing Stock UoM",
            "closing_stock_qty_actual": "Closing Stock Qty (Actual)",
            "closing_stock_qty_mt": "Closing Stock Qty (MT)",
        }
    )
    return movement.sort_values(
        ["Consumption Qty (MT)", "Consumption Qty (Actual)", "Name of Chemical"],
        ascending=[False, False, True],
    )


def _top_rows(frame: pd.DataFrame, value_column: str, limit: int = 10) -> list[dict[str, Any]]:
    if frame.empty or value_column not in frame.columns:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in frame.head(limit).iterrows():
        rows.append(
            {
                "material_code": _normalize_text(row.get("material_code")),
                "material_desc": _normalize_text(row.get("material_desc")),
                "material_label": _material_display(row.get("material_code"), row.get("material_desc")),
                value_column: round(_safe_float(row.get(value_column)), 2),
            }
        )
    return rows


def _build_summary(
    *,
    report_year: int,
    report_month: int,
    mb51_filename: str,
    me2m_filename: str,
    mc9_filename: str,
    consumption_frame: pd.DataFrame,
    procurement_frame: pd.DataFrame,
    mc9_frame: pd.DataFrame,
    consumption_summary: pd.DataFrame,
    procurement_summary: pd.DataFrame,
    mc9_summary: pd.DataFrame,
    material_rollup: pd.DataFrame,
    excluded_mb51_rows: int,
    excluded_mc9_rows: int,
) -> dict[str, Any]:
    report_label = _month_label(report_year, report_month)
    unique_plant_count = len(
        {
            _normalize_text(value)
            for value in pd.concat(
                [
                    consumption_frame.get("reporting_plant", pd.Series(dtype="object")),
                    procurement_frame.get("reporting_plant", pd.Series(dtype="object")),
                    mc9_frame.get("reporting_plant", pd.Series(dtype="object")),
                ],
                axis=0,
            ).tolist()
            if _normalize_text(value)
        }
    )

    top_consumption = _top_rows(consumption_summary, "monthly_usage_value")
    top_procurement = _top_rows(procurement_summary, "effective_value")
    top_stock = _top_rows(mc9_summary, "closing_stock_value")

    observations = []
    if top_consumption:
        item = top_consumption[0]
        observations.append(
            f"Highest MB51 monthly consumption value: {item['material_label']} at Rs {item['monthly_usage_value']:,.2f}."
        )
    if top_procurement:
        item = top_procurement[0]
        observations.append(
            f"Largest procurement snapshot value: {item['material_label']} at Rs {item['effective_value']:,.2f}."
        )
    if top_stock:
        item = top_stock[0]
        observations.append(
            f"Highest MC.9 closing stock value: {item['material_label']} at Rs {item['closing_stock_value']:,.2f}."
        )
    observations.append(
        f"Coverage this month: {len(material_rollup)} materials across {unique_plant_count} reporting plants."
    )

    return {
        "report_year": report_year,
        "report_month": report_month,
        "report_label": report_label,
        "source_files": {
            "mb51": mb51_filename,
            "me2m": me2m_filename,
            "mc9": mc9_filename,
        },
        "row_counts": {
            "mb51_in_month": int(len(consumption_frame)),
            "me2m_snapshot": int(len(procurement_frame)),
            "mc9_in_month": int(len(mc9_frame)),
            "mb51_excluded_other_months": int(excluded_mb51_rows),
            "mc9_excluded_other_months": int(excluded_mc9_rows),
        },
        "kpis": {
            "materials": int(len(material_rollup)),
            "reporting_plants": int(unique_plant_count),
            "vendors": int(procurement_frame.get("vendor", pd.Series(dtype="object")).replace("", pd.NA).dropna().nunique()) if not procurement_frame.empty else 0,
            "total_mb51_consumption_mt": round(_safe_float(consumption_summary.get("monthly_usage_qty_mt", pd.Series(dtype="float")).sum()), 2),
            "total_mb51_consumption_value": round(_safe_float(consumption_summary.get("monthly_usage_value", pd.Series(dtype="float")).sum()), 2),
            "total_mc9_usage_value": round(_safe_float(mc9_summary.get("monthly_usage_value", pd.Series(dtype="float")).sum()), 2),
            "total_closing_stock_value": round(_safe_float(mc9_summary.get("closing_stock_value", pd.Series(dtype="float")).sum()), 2),
            "total_procurement_value": round(_safe_float(procurement_summary.get("effective_value", pd.Series(dtype="float")).sum()), 2),
            "total_open_qty": round(_safe_float(procurement_summary.get("open_qty", pd.Series(dtype="float")).sum()), 2),
        },
        "top_consumption_by_value": top_consumption,
        "top_procurement_by_value": top_procurement,
        "top_stock_by_value": top_stock,
        "observations": observations,
    }


def _write_sheet_headers(sheet, headers: list[str]) -> None:
    sheet.append(headers)
    for cell in sheet[1]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    sheet.freeze_panes = "A2"


def _autosize_sheet(sheet) -> None:
    for column_cells in sheet.columns:
        max_length = 0
        column_letter = column_cells[0].column_letter
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        sheet.column_dimensions[column_letter].width = min(max_length + 2, 32)


def _append_dataframe_sheet(workbook: Workbook, title: str, frame: pd.DataFrame, headers: list[str] | None = None) -> None:
    sheet = workbook.create_sheet(title[:31])
    if frame.empty:
        _write_sheet_headers(sheet, headers or [])
        if headers:
            sheet.append(["" for _ in headers])
            sheet["A2"] = "No rows available."
        _autosize_sheet(sheet)
        return

    data = frame.copy()
    if headers is None:
        headers = [str(column) for column in data.columns]
    _write_sheet_headers(sheet, headers)
    for _, row in data.iterrows():
        values = []
        for column in data.columns:
            value = row.get(column)
            if isinstance(value, pd.Timestamp):
                values.append(value.strftime("%Y-%m-%d"))
            elif pd.isna(value):
                values.append("")
            else:
                values.append(value)
        sheet.append(values)
    _autosize_sheet(sheet)


def _build_excel_report(
    *,
    summary: dict[str, Any],
    material_rollup: pd.DataFrame,
    opening_stock_summary: pd.DataFrame,
    consumption_summary: pd.DataFrame,
    procurement_summary: pd.DataFrame,
    mc9_summary: pd.DataFrame,
    consumption_frame: pd.DataFrame,
    procurement_frame: pd.DataFrame,
    mc9_frame: pd.DataFrame,
) -> bytes:
    workbook = Workbook()
    overview = workbook.active
    overview.title = "Overview"
    overview["A1"] = "Inventory Monthly Update"
    overview["A1"].font = Font(size=16, bold=True, color="1F4E78")
    overview["A3"] = "Reporting Month"
    overview["B3"] = summary["report_label"]
    overview["A4"] = "MB51 File"
    overview["B4"] = summary["source_files"]["mb51"]
    overview["A5"] = "ME2M File"
    overview["B5"] = summary["source_files"]["me2m"]
    overview["A6"] = "MC.9 File"
    overview["B6"] = summary["source_files"]["mc9"]

    overview["A8"] = "Key Metrics"
    overview["A8"].fill = SECTION_FILL
    overview["A8"].font = SECTION_FONT
    metric_rows = [
        ("Materials in rollup", summary["kpis"]["materials"]),
        ("Reporting plants", summary["kpis"]["reporting_plants"]),
        ("Vendors", summary["kpis"]["vendors"]),
        ("MB51 monthly consumption (MT)", summary["kpis"]["total_mb51_consumption_mt"]),
        ("MB51 monthly consumption value", summary["kpis"]["total_mb51_consumption_value"]),
        ("MC.9 monthly usage value", summary["kpis"]["total_mc9_usage_value"]),
        ("MC.9 closing stock value", summary["kpis"]["total_closing_stock_value"]),
        ("ME2M procurement value", summary["kpis"]["total_procurement_value"]),
        ("ME2M open quantity", summary["kpis"]["total_open_qty"]),
    ]
    start_row = 9
    for offset, (label, value) in enumerate(metric_rows):
        overview.cell(row=start_row + offset, column=1, value=label)
        overview.cell(row=start_row + offset, column=2, value=value)

    obs_row = start_row + len(metric_rows) + 2
    overview.cell(row=obs_row, column=1, value="Executive Observations")
    overview.cell(row=obs_row, column=1).fill = SECTION_FILL
    overview.cell(row=obs_row, column=1).font = SECTION_FONT
    for index, text in enumerate(summary["observations"], start=1):
        overview.cell(row=obs_row + index, column=1, value=f"- {text}")
    _autosize_sheet(overview)

    material_rollup_sheet = material_rollup.loc[
        :,
        [
            "material_code",
            "material_desc",
            "opening_stock_uom",
            "opening_stock_qty_actual",
            "opening_stock_qty_mt",
            "opening_stock_value",
            "consumption_uom",
            "mb51_usage_qty_actual",
            "mb51_usage_qty_mt",
            "mb51_usage_value",
            "mc9_usage_uom",
            "mc9_usage_qty_actual",
            "mc9_usage_qty_mt",
            "mc9_usage_value",
            "stock_uom",
            "closing_stock_qty_actual",
            "closing_stock_qty_mt",
            "closing_stock_value",
            "procurement_uom",
            "ordered_qty",
            "procured_qty",
            "open_qty",
            "procurement_value",
            "vendor_count",
        ],
    ].rename(
        columns={
            "material_code": "Material",
            "material_desc": "Name of Chemical",
            "opening_stock_uom": "Opening Stock UoM",
            "opening_stock_qty_actual": "Opening Stock Qty (Actual)",
            "opening_stock_qty_mt": "Opening Stock Qty (MT)",
            "opening_stock_value": "Opening Stock Value",
            "consumption_uom": "MB51 UoM",
            "mb51_usage_qty_actual": "MB51 Qty (Actual)",
            "mb51_usage_qty_mt": "MB51 Qty (MT)",
            "mb51_usage_value": "MB51 Value",
            "mc9_usage_uom": "MC.9 Usage UoM",
            "mc9_usage_qty_actual": "MC.9 Usage Qty (Actual)",
            "mc9_usage_qty_mt": "MC.9 Usage Qty (MT)",
            "mc9_usage_value": "MC.9 Usage Value",
            "stock_uom": "Closing Stock UoM",
            "closing_stock_qty_actual": "Closing Stock Qty (Actual)",
            "closing_stock_qty_mt": "Closing Stock Qty (MT)",
            "closing_stock_value": "Closing Stock Value",
            "procurement_uom": "ME2M UoM",
            "ordered_qty": "Ordered Qty",
            "procured_qty": "Procured Qty",
            "open_qty": "Open Qty",
            "procurement_value": "Procurement Value",
            "vendor_count": "Vendor Count",
        }
    )
    _append_dataframe_sheet(workbook, "Material Rollup", material_rollup_sheet)
    _append_dataframe_sheet(workbook, "Stock Movement", _build_stock_movement_sheet(material_rollup))
    _append_dataframe_sheet(workbook, "MB51 Consumption", consumption_summary.rename(columns={
        "material_code": "Material",
        "material_desc": "Name of Chemical",
        "actual_uom": "Unit of Measurement (actual)",
        "monthly_usage_qty_actual": "Monthly Consumption (Actual)",
        "monthly_usage_qty_mt": "Monthly Consumption (MT)",
        "monthly_usage_value": "Monthly Consumption Value",
        "plant_count": "Reporting Plants",
        "movement_rows": "Movement Rows",
    }))
    _append_dataframe_sheet(workbook, "MC9 Stock", mc9_summary.rename(columns={
        "material_code": "Material",
        "material_desc": "Name of Chemical",
        "usage_uom": "Usage UoM",
        "stock_uom": "Stock UoM",
        "monthly_usage_qty_actual": "Monthly Usage (Actual)",
        "monthly_usage_qty_mt": "Monthly Usage (MT)",
        "monthly_usage_value": "Monthly Usage Value",
        "closing_stock_qty_actual": "Closing Stock Qty (Actual)",
        "closing_stock_qty_mt": "Closing Stock Qty (MT)",
        "closing_stock_value": "Closing Stock Value",
        "plant_count": "Reporting Plants",
        "stock_rows": "Rows",
    }))
    _append_dataframe_sheet(workbook, "Opening Stock", opening_stock_summary.rename(columns={
        "material_code": "Material",
        "material_desc": "Name of Chemical",
        "opening_stock_uom": "Unit of Measurement (actual)",
        "opening_stock_qty_actual": "Opening Stock Qty (Actual)",
        "opening_stock_qty_mt": "Opening Stock Qty (MT)",
        "opening_stock_value": "Opening Stock Value",
    }))
    _append_dataframe_sheet(workbook, "ME2M Snapshot", procurement_summary.rename(columns={
        "material_code": "Material",
        "material_desc": "Name of Chemical",
        "order_unit": "Unit of Measurement (actual)",
        "ordered_qty": "Ordered Quantity",
        "procured_qty": "Procured Quantity",
        "open_qty": "Open Quantity",
        "effective_value": "Procurement Value",
        "vendor_count": "Vendor Count",
        "po_count": "PO Count",
    }))

    _append_dataframe_sheet(
        workbook,
        "MB51 Raw",
        consumption_frame.loc[
            :,
            [
                "reporting_plant",
                "plant",
                "material_code",
                "material_desc",
                "posting_date",
                "movement_type",
                "actual_usage_qty",
                "actual_uom",
                "usage_qty",
                "uom",
                "usage_value",
                "storage_location",
                "po_number",
            ],
        ].rename(
            columns={
                "reporting_plant": "Reporting Plant",
                "plant": "Source Plant",
                "material_code": "Material",
                "material_desc": "Name of Chemical",
                "posting_date": "Posting Date",
                "movement_type": "Movement Type",
                "actual_usage_qty": "Qty (Actual)",
                "actual_uom": "Actual UoM",
                "usage_qty": "Qty (Normalized)",
                "uom": "Normalized UoM",
                "usage_value": "Value",
                "storage_location": "Storage Location",
                "po_number": "PO Number",
            }
        ),
    )
    _append_dataframe_sheet(
        workbook,
        "ME2M Raw",
        procurement_frame.loc[
            :,
            [
                "reporting_plant",
                "plant",
                "material_code",
                "material_desc",
                "doc_date",
                "po_number",
                "vendor",
                "order_qty",
                "procured_qty",
                "still_to_be_delivered_qty",
                "order_unit",
                "effective_value",
                "release_indicator",
            ],
        ].rename(
            columns={
                "reporting_plant": "Reporting Plant",
                "plant": "Source Plant",
                "material_code": "Material",
                "material_desc": "Name of Chemical",
                "doc_date": "Document Date",
                "po_number": "PO Number",
                "vendor": "Vendor",
                "order_qty": "Ordered Qty",
                "procured_qty": "Procured Qty",
                "still_to_be_delivered_qty": "Open Qty",
                "order_unit": "Order Unit",
                "effective_value": "Effective Value",
                "release_indicator": "Release Indicator",
            }
        ),
    )
    _append_dataframe_sheet(
        workbook,
        "MC9 Raw",
        mc9_frame.loc[
            :,
            [
                "reporting_plant",
                "plant",
                "material_code",
                "material_desc",
                "posting_date",
                "usage_qty",
                "uom",
                "usage_value",
                "stock_qty",
                "stock_uom",
                "stock_value",
                "storage_location",
            ],
        ].rename(
            columns={
                "reporting_plant": "Reporting Plant",
                "plant": "Source Plant",
                "material_code": "Material",
                "material_desc": "Name of Chemical",
                "posting_date": "Month",
                "usage_qty": "Usage Qty",
                "uom": "Usage UoM",
                "usage_value": "Usage Value",
                "stock_qty": "Closing Stock Qty",
                "stock_uom": "Closing Stock UoM",
                "stock_value": "Closing Stock Value",
                "storage_location": "Storage Location",
            }
        ),
    )

    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


# ── Enterprise Chairman-Level PDF Report (ReportLab) ─────────────────────────
import os as _os

try:
    from reportlab.lib.colors import HexColor as _RLHex, black as _RL_BLACK, white as _RL_WHITE
    from reportlab.lib.enums import TA_CENTER as _TA_C, TA_LEFT as _TA_L, TA_RIGHT as _TA_R
    from reportlab.lib.pagesizes import A4 as _RL_A4
    from reportlab.lib.styles import ParagraphStyle as _RLParaStyle
    from reportlab.lib.units import mm as _RL_MM
    from reportlab.platypus import (
        PageBreak as _RLPageBreak,
        Paragraph as _RLPara,
        SimpleDocTemplate as _RLDoc,
        Spacer as _RLSpacer,
        Table as _RLTable,
        TableStyle as _RLTableStyle,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - depends on env packaging
    _REPORTLAB_IMPORT_ERROR = exc
else:
    _REPORTLAB_IMPORT_ERROR = None

    # ── Brand colours ─────────────────────────────────────────────────────────
    _PDF_NAVY = _RLHex("#0F3B63")
    _PDF_AMBER = _RLHex("#C55A11")
    _PDF_GOLD = _RLHex("#C9A227")
    _PDF_LIGHT = _RLHex("#DCE6F1")
    _PDF_SLATE = _RLHex("#5B6B7A")
    _PDF_GREY = _RLHex("#F5F6F8")
    _PDF_GLINE = _RLHex("#D0D5DD")

    _PDF_PW, _PDF_PH = _RL_A4
    _PDF_ML = 18 * _RL_MM
    _PDF_MR = 18 * _RL_MM
    _PDF_MT = 20 * _RL_MM
    _PDF_MB = 22 * _RL_MM
    _PDF_CW = _PDF_PW - _PDF_ML - _PDF_MR

    _PDF_LOGO = _os.path.abspath(
        _os.path.join(_os.path.dirname(__file__), "..", "..", "..", "..", "ONGC Logo.jpeg")
    )

    def _pdf_ps(name: str, **kw) -> _RLParaStyle:
        return _RLParaStyle(name, **kw)

    # ── Shared paragraph styles ───────────────────────────────────────────────
    _PS_COVER_TITLE = _pdf_ps(
        "CoverTitle", fontName="Helvetica-Bold", fontSize=24, leading=30, textColor=_RL_WHITE, alignment=_TA_C
    )
    _PS_COVER_SUB = _pdf_ps(
        "CoverSub", fontName="Helvetica", fontSize=13, leading=17, textColor=_PDF_LIGHT, alignment=_TA_C, spaceAfter=4
    )
    _PS_COVER_PERIOD = _pdf_ps(
        "CoverPeriod", fontName="Helvetica-Bold", fontSize=18, leading=24, textColor=_PDF_GOLD, alignment=_TA_C
    )
    _PS_SECTION_H = _pdf_ps(
        "SectionH", fontName="Helvetica-Bold", fontSize=13, leading=16, textColor=_PDF_NAVY, spaceBefore=8, spaceAfter=4
    )
    _PS_BODY = _pdf_ps("Body", fontName="Helvetica", fontSize=9, leading=13, textColor=_RL_BLACK)
    _PS_BODY_RIGHT = _pdf_ps(
        "BodyR", fontName="Helvetica", fontSize=9, leading=13, textColor=_RL_BLACK, alignment=_TA_R
    )
    _PS_SMALL = _pdf_ps("Small", fontName="Helvetica", fontSize=7.5, leading=11, textColor=_PDF_SLATE)
    _PS_OBS = _pdf_ps(
        "Obs", fontName="Helvetica", fontSize=9, leading=14, textColor=_RL_BLACK, leftIndent=6, spaceAfter=3
    )
    _PS_TH = _pdf_ps(
        "TH", fontName="Helvetica-Bold", fontSize=8.5, leading=11, textColor=_RL_WHITE, alignment=_TA_C
    )
    _PS_TD = _pdf_ps("TD", fontName="Helvetica", fontSize=8, leading=11, textColor=_RL_BLACK)
    _PS_TDR = _pdf_ps(
        "TDR", fontName="Helvetica", fontSize=8, leading=11, textColor=_RL_BLACK, alignment=_TA_R
    )


def _pdf_rule(color=None) -> _RLTable:
    """Full-width 1.2 pt colour rule."""
    c = color if color is not None else _PDF_NAVY
    return _RLTable([[""]], colWidths=[_PDF_CW], rowHeights=[1.2],
                    style=[("BACKGROUND", (0, 0), (-1, -1), c)])


def _pdf_fmt_inr(v: float) -> str:
    """Format a Rupee value as Cr / L / plain, with ₹ symbol."""
    if v >= 1_00_00_000:           # ≥ 1 Cr
        return f"\u20b9 {v / 1_00_00_000:,.2f} Cr"
    if v >= 1_00_000:              # ≥ 1 L
        return f"\u20b9 {v / 1_00_000:,.2f} L"
    return f"\u20b9 {v:,.2f}"


def _pdf_header_footer(canvas, doc) -> None:
    """Persistent ONGC header band and confidentiality footer on every page."""
    canvas.saveState()
    # ── Header band (navy) ─────────────────────────────────────────────────
    canvas.setFillColor(_PDF_NAVY)
    canvas.rect(0, _PDF_PH - 14 * _RL_MM, _PDF_PW, 14 * _RL_MM, fill=1, stroke=0)
    if _os.path.isfile(_PDF_LOGO):
        try:
            canvas.drawImage(
                _PDF_LOGO,
                _PDF_ML, _PDF_PH - 13 * _RL_MM,
                width=26 * _RL_MM, height=11 * _RL_MM,
                preserveAspectRatio=True, mask="auto",
            )
        except Exception:
            pass
    canvas.setFillColor(_RL_WHITE)
    canvas.setFont("Helvetica-Bold", 9)
    canvas.drawCentredString(_PDF_PW / 2, _PDF_PH - 8 * _RL_MM,
                             "OIL AND NATURAL GAS CORPORATION LIMITED")
    canvas.setFont("Helvetica", 7.5)
    canvas.drawCentredString(_PDF_PW / 2, _PDF_PH - 12 * _RL_MM,
                             "Inventory Status  |  Monthly Executive Report")
    canvas.drawRightString(_PDF_PW - _PDF_MR, _PDF_PH - 9 * _RL_MM, f"Page {doc.page}")
    # ── Footer band ────────────────────────────────────────────────────────
    canvas.setFillColor(_PDF_NAVY)
    canvas.rect(0, 0, _PDF_PW, 9 * _RL_MM, fill=1, stroke=0)
    canvas.setFillColor(_PDF_GOLD)
    canvas.rect(0, 9 * _RL_MM, _PDF_PW, 0.8 * _RL_MM, fill=1, stroke=0)
    canvas.setFillColor(_RL_WHITE)
    canvas.setFont("Helvetica", 6.5)
    canvas.drawString(_PDF_ML, 3.5 * _RL_MM,
                      "CONFIDENTIAL \u2014 FOR INTERNAL MANAGEMENT USE ONLY")
    canvas.drawRightString(
        _PDF_PW - _PDF_MR, 3.5 * _RL_MM,
        f"Generated: {datetime.now(timezone.utc).strftime('%d %b %Y')}",
    )
    canvas.restoreState()


def _pdf_cover_page(story: list, summary: dict[str, Any]) -> None:
    story.append(_RLSpacer(1, 28 * _RL_MM))
    t = _RLTable([[_RLPara("INVENTORY STATUS", _PS_COVER_TITLE)]],
                 colWidths=[_PDF_CW])
    t.setStyle(_RLTableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _PDF_NAVY),
        ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    story.append(t)
    story.append(_RLSpacer(1, 3 * _RL_MM))
    story.append(_RLPara("Monthly Executive Report", _PS_COVER_SUB))
    story.append(_RLSpacer(1, 5 * _RL_MM))

    period_t = _RLTable([[_RLPara(summary["report_label"], _PS_COVER_PERIOD)]],
                        colWidths=[_PDF_CW])
    period_t.setStyle(_RLTableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _PDF_LIGHT),
        ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    story.append(period_t)
    story.append(_RLSpacer(1, 7 * _RL_MM))

    kpis = summary["kpis"]
    lw = 45 * _RL_MM
    org_rows = [
        ["Organisation",  "Oil and Natural Gas Corporation Limited"],
        ["Division",      "Office of Head Corporate Chemistry"],
        ["Report Type",   "Monthly Inventory Intelligence Update"],
        ["Data Sources",  "MB51 (Consumption)  \u00b7  ME2M (Procurement)  \u00b7  MC.9 (Stock)"],
        ["Coverage",      (f"{kpis['materials']} Materials  |  "
                           f"{kpis['reporting_plants']} Plants  |  "
                           f"{kpis['vendors']} Vendors")],
    ]
    org_tbl = _RLTable(org_rows, colWidths=[lw, _PDF_CW - lw])
    org_tbl.setStyle(_RLTableStyle([
        ("FONTNAME",  (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTNAME",  (1, 0), (1, -1), "Helvetica"),
        ("FONTSIZE",  (0, 0), (-1, -1), 9),
        ("LEADING",   (0, 0), (-1, -1), 14),
        ("TEXTCOLOR", (0, 0), (0, -1), _PDF_NAVY),
        ("TEXTCOLOR", (1, 0), (1, -1), _RL_BLACK),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [_PDF_GREY, _RL_WHITE]),
        ("BOX",       (0, 0), (-1, -1), 0.5, _PDF_GLINE),
        ("INNERGRID", (0, 0), (-1, -1), 0.4, _PDF_GLINE),
    ]))
    story.append(org_tbl)
    story.append(_RLSpacer(1, 8 * _RL_MM))
    story.append(_pdf_rule(_PDF_AMBER))
    story.append(_RLSpacer(1, 4 * _RL_MM))
    story.append(_RLPara(
        "This report is prepared by the Inventory Intelligence system for review. "
        "All values are in Indian Rupees (\u20b9) unless stated.",
        _PS_SMALL,
    ))
    story.append(_RLPageBreak())


def _pdf_kpi_card(label: str, value: str, accent: bool = False) -> _RLTable:
    bg   = _PDF_NAVY if accent else _RL_WHITE
    v_c  = _RL_WHITE if accent else _PDF_NAVY
    l_c  = _PDF_GOLD if accent else _PDF_SLATE
    vps  = _pdf_ps(f"KV_{label[:6]}", fontName="Helvetica-Bold", fontSize=13,
                   leading=17, textColor=v_c, alignment=_TA_C)
    lps  = _pdf_ps(f"KL_{label[:6]}", fontName="Helvetica", fontSize=7.5,
                   leading=10, textColor=l_c, alignment=_TA_C)
    card = _RLTable(
        [[_RLPara(value, vps)], [_RLPara(label, lps)]],
        colWidths=[(_PDF_CW - 6 * _RL_MM) / 4],
    )
    bc = _PDF_NAVY if accent else _PDF_GLINE
    card.setStyle(_RLTableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), bg),
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ("BOX",           (0, 0), (-1, -1), 0.75, bc),
    ]))
    return card


def _pdf_kpi_row(pairs: list) -> _RLTable:
    cells = [_pdf_kpi_card(lbl, val, acc) for lbl, val, acc in pairs]
    outer = _RLTable([cells], colWidths=[(_PDF_CW + 2 * _RL_MM) / 4] * 4)
    outer.setStyle(_RLTableStyle([
        ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return outer


def _pdf_kpi_page(story: list, summary: dict[str, Any]) -> None:
    kpis = summary["kpis"]
    story.append(_RLSpacer(1, 5 * _RL_MM))
    story.append(_RLPara("Key Performance Indicators", _PS_SECTION_H))
    story.append(_pdf_rule(_PDF_NAVY))
    story.append(_RLSpacer(1, 4 * _RL_MM))

    row1 = [
        ("Materials Tracked",        str(kpis["materials"]),                                    True),
        ("Reporting Plants",          str(kpis["reporting_plants"]),                             False),
        ("Active Vendors",            str(kpis["vendors"]),                                      False),
        ("MB51 Consumption (MT)",     f"{kpis['total_mb51_consumption_mt']:,.2f}",                False),
    ]
    row2 = [
        ("MB51 Consumption Value",    _pdf_fmt_inr(kpis["total_mb51_consumption_value"]),         True),
        ("MC.9 Usage Value",          _pdf_fmt_inr(kpis["total_mc9_usage_value"]),                False),
        ("MC.9 Closing Stock Value",  _pdf_fmt_inr(kpis["total_closing_stock_value"]),            False),
        ("ME2M Procurement Value",    _pdf_fmt_inr(kpis["total_procurement_value"]),              False),
    ]
    story.append(_pdf_kpi_row(row1))
    story.append(_RLSpacer(1, 3 * _RL_MM))
    story.append(_pdf_kpi_row(row2))
    story.append(_RLSpacer(1, 6 * _RL_MM))

    story.append(_RLPara("Executive Observations", _PS_SECTION_H))
    story.append(_pdf_rule(_PDF_AMBER))
    story.append(_RLSpacer(1, 3 * _RL_MM))
    for idx, obs in enumerate(summary["observations"], start=1):
        story.append(_RLPara(f"<b>{idx}.</b>&nbsp; {obs}", _PS_OBS))
    story.append(_RLPageBreak())


def _pdf_top_table(title: str, rows: list, value_key: str,
                   value_label: str, story: list) -> None:
    story.append(_RLPara(title, _PS_SECTION_H))
    story.append(_pdf_rule(_PDF_NAVY))
    story.append(_RLSpacer(1, 3 * _RL_MM))
    if not rows:
        story.append(_RLPara("No data available for this section.", _PS_SMALL))
        story.append(_RLSpacer(1, 4 * _RL_MM))
        return

    rank_w = 8  * _RL_MM
    code_w = 22 * _RL_MM
    val_w  = 32 * _RL_MM
    name_w = _PDF_CW - rank_w - code_w - val_w

    hdr = [_RLPara(h, _PS_TH) for h in ["#", "Material Code", "Description", value_label]]
    data: list = [hdr]
    rk_ps = _pdf_ps("RK", fontName="Helvetica-Bold", fontSize=8,
                    alignment=_TA_C, leading=11)
    for i, row in enumerate(rows[:10], start=1):
        data.append([
            _RLPara(str(i),                                         rk_ps),
            _RLPara(str(row.get("material_code", "")),              _PS_TD),
            _RLPara(str(row.get("material_desc", "")),              _PS_TD),
            _RLPara(_pdf_fmt_inr(float(row.get(value_key, 0))),     _PS_TDR),
        ])

    tbl = _RLTable(data, colWidths=[rank_w, code_w, name_w, val_w])
    tbl.setStyle(_RLTableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  _PDF_NAVY),
        ("ALIGN",         (0, 0), (-1, 0),  "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_RL_WHITE, _PDF_GREY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, _PDF_GLINE),
        ("LINEBELOW",     (0, 0), (-1, 0),  1.2, _PDF_AMBER),
        ("BACKGROUND",    (0, 1), (0, -1),  _PDF_LIGHT),
        ("FONTNAME",      (0, 1), (0, -1),  "Helvetica-Bold"),
        ("ALIGN",         (3, 1), (3, -1),  "RIGHT"),
    ]))
    story.append(tbl)
    story.append(_RLSpacer(1, 5 * _RL_MM))


def _pdf_top_materials_page(story: list, summary: dict[str, Any]) -> None:
    story.append(_RLSpacer(1, 4 * _RL_MM))
    _pdf_top_table(
        "Top 10 Materials \u2014 MB51 Monthly Consumption Value",
        summary["top_consumption_by_value"], "monthly_usage_value",
        "Consumption Value", story,
    )
    _pdf_top_table(
        "Top 10 Materials \u2014 ME2M Procurement Value",
        summary["top_procurement_by_value"], "effective_value",
        "Procurement Value", story,
    )
    _pdf_top_table(
        "Top 10 Materials \u2014 MC.9 Closing Stock Value",
        summary["top_stock_by_value"], "closing_stock_value",
        "Closing Stock Value", story,
    )


def _pdf_source_files_page(story: list, summary: dict[str, Any]) -> None:
    story.append(_RLPageBreak())
    story.append(_RLSpacer(1, 5 * _RL_MM))
    story.append(_RLPara("Data Sources & Row Counts", _PS_SECTION_H))
    story.append(_pdf_rule(_PDF_NAVY))
    story.append(_RLSpacer(1, 3 * _RL_MM))

    rc = summary["row_counts"]
    sf = summary["source_files"]
    src = [
        [_RLPara(h, _PS_TH) for h in
         ["Source", "File Name", "Rows (Reporting Month)", "Excluded (Other Months)"]],
        [_RLPara("MB51 \u2014 Consumption", _PS_TD),
         _RLPara(sf.get("mb51", "\u2013"), _PS_TD),
         _RLPara(str(rc.get("mb51_in_month", 0)), _PS_TD),
         _RLPara(str(rc.get("mb51_excluded_other_months", 0)), _PS_TD)],
        [_RLPara("ME2M \u2014 Procurement", _PS_TD),
         _RLPara(sf.get("me2m", "\u2013"), _PS_TD),
         _RLPara(str(rc.get("me2m_snapshot", 0)), _PS_TD),
         _RLPara("Snapshot (all rows)", _PS_TD)],
        [_RLPara("MC.9 \u2014 Stock", _PS_TD),
         _RLPara(sf.get("mc9", "\u2013"), _PS_TD),
         _RLPara(str(rc.get("mc9_in_month", 0)), _PS_TD),
         _RLPara(str(rc.get("mc9_excluded_other_months", 0)), _PS_TD)],
    ]
    src_tbl = _RLTable(src, colWidths=[38 * _RL_MM, _PDF_CW - 38 * _RL_MM - 46 * _RL_MM,
                                       30 * _RL_MM, 16 * _RL_MM])
    src_tbl.setStyle(_RLTableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  _PDF_NAVY),
        ("ALIGN",         (0, 0), (-1, 0),  "CENTER"),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_RL_WHITE, _PDF_GREY]),
        ("GRID",          (0, 0), (-1, -1), 0.4, _PDF_GLINE),
        ("LINEBELOW",     (0, 0), (-1, 0),  1.2, _PDF_AMBER),
    ]))
    story.append(src_tbl)
    story.append(_RLSpacer(1, 7 * _RL_MM))
    story.append(_RLPara(
        "<b>Note:</b> All monetary values are in Indian Rupees (\u20b9). "
        "Consumption data is sourced from SAP MB51 goods movement records. "
        "Procurement data is a point-in-time snapshot from ME2M purchase orders. "
        "Stock data is sourced from MC.9 plant stock reports. "
        "This report is system-generated for internal management review.",
        _PS_SMALL,
    ))


def _build_pdf_report(summary: dict[str, Any]) -> bytes:
    """Render an enterprise Chairman-level PDF using ReportLab Platypus."""
    if _REPORTLAB_IMPORT_ERROR is not None:
        raise ValueError(
            "PDF generation requires the `reportlab` package. Install the project requirements and try again."
        ) from _REPORTLAB_IMPORT_ERROR

    buf = BytesIO()
    doc = _RLDoc(
        buf,
        pagesize=_RL_A4,
        leftMargin=_PDF_ML,
        rightMargin=_PDF_MR,
        topMargin=_PDF_MT + 14 * _RL_MM,   # reserve space for the header band
        bottomMargin=_PDF_MB + 9 * _RL_MM,  # reserve space for the footer band
        title=f"ONGC Inventory Monthly Executive Report \u2014 {summary.get('report_label', '')}",
        author="ONGC Inventory Intelligence System",
        subject="Inventory Monthly Update",
        creator="ONGC Inventory Intelligence",
    )
    story: list = []
    _pdf_cover_page(story, summary)
    _pdf_kpi_page(story, summary)
    _pdf_top_materials_page(story, summary)
    _pdf_source_files_page(story, summary)
    doc.build(story, onFirstPage=_pdf_header_footer, onLaterPages=_pdf_header_footer)
    return buf.getvalue()


def build_monthly_inventory_report_package(
    *,
    report_year: int,
    report_month: int,
    mb51_bytes: bytes,
    mb51_filename: str,
    me2m_bytes: bytes,
    me2m_filename: str,
    mc9_bytes: bytes,
    mc9_filename: str,
) -> MonthlyInventoryReportPackage:
    _validate_report_period(report_year, report_month)

    mb51_raw = _read_uploaded_seed_dataframe(mb51_bytes, mb51_filename)
    _validate_seed_frame(mb51_raw, "consumption")
    if _detect_frame_variant([str(column) for column in mb51_raw.columns], "consumption") != "mb51":
        raise ValueError("Upload the MB51 consumption export for the monthly update module.")
    consumption_all = _normalize_consumption_frame(mb51_raw)
    consumption_frame = _filter_month(consumption_all, report_year, report_month)
    if consumption_frame.empty:
        raise ValueError(
            f"MB51 file does not contain rows for {_month_label(report_year, report_month)}. "
            f"Available months: {_available_month_labels(consumption_all)}."
        )

    me2m_raw = _read_uploaded_seed_dataframe(me2m_bytes, me2m_filename)
    _validate_seed_frame(me2m_raw, "procurement")
    procurement_frame = _normalize_procurement_frame(me2m_raw)
    if procurement_frame.empty:
        raise ValueError("ME2M file does not contain any usable procurement snapshot rows.")

    mc9_raw = _read_uploaded_seed_dataframe(mc9_bytes, mc9_filename)
    _validate_seed_frame(mc9_raw, "mc9")
    if _detect_frame_variant([str(column) for column in mc9_raw.columns], "mc9") != "mc9":
        raise ValueError("Upload the MC.9 stock-and-consumption export for the monthly update module.")
    mc9_all = _normalize_mc9_frame(mc9_raw)
    mc9_frame = _filter_month(mc9_all, report_year, report_month)
    if mc9_frame.empty:
        raise ValueError(
            f"MC.9 file does not contain rows for {_month_label(report_year, report_month)}. "
            f"Available months: {_available_month_labels(mc9_all)}."
        )

    opening_stock_summary = _summarize_opening_stock(mc9_all, report_year, report_month)
    consumption_summary = _summarize_consumption(consumption_frame)
    procurement_summary = _summarize_procurement(procurement_frame)
    mc9_summary = _summarize_mc9(mc9_frame)
    material_rollup = _build_material_rollup(opening_stock_summary, consumption_summary, mc9_summary, procurement_summary)

    summary = _build_summary(
        report_year=report_year,
        report_month=report_month,
        mb51_filename=mb51_filename,
        me2m_filename=me2m_filename,
        mc9_filename=mc9_filename,
        consumption_frame=consumption_frame,
        procurement_frame=procurement_frame,
        mc9_frame=mc9_frame,
        consumption_summary=consumption_summary,
        procurement_summary=procurement_summary,
        mc9_summary=mc9_summary,
        material_rollup=material_rollup,
        excluded_mb51_rows=max(len(consumption_all) - len(consumption_frame), 0),
        excluded_mc9_rows=max(len(mc9_all) - len(mc9_frame), 0),
    )

    token = _month_token(report_year, report_month)
    excel_bytes = _build_excel_report(
        summary=summary,
        material_rollup=material_rollup,
        opening_stock_summary=opening_stock_summary,
        consumption_summary=consumption_summary,
        procurement_summary=procurement_summary,
        mc9_summary=mc9_summary,
        consumption_frame=consumption_frame,
        procurement_frame=procurement_frame,
        mc9_frame=mc9_frame,
    )
    pdf_bytes = _build_pdf_report(summary)

    return MonthlyInventoryReportPackage(
        report_year=report_year,
        report_month=report_month,
        report_label=summary["report_label"],
        mb51_filename=mb51_filename,
        me2m_filename=me2m_filename,
        mc9_filename=mc9_filename,
        mb51_row_count=int(len(consumption_frame)),
        me2m_row_count=int(len(procurement_frame)),
        mc9_row_count=int(len(mc9_frame)),
        excel_filename=f"inventory-monthly-report-{token}.xlsx",
        excel_bytes=excel_bytes,
        pdf_filename=f"inventory-monthly-executive-summary-{token}.pdf",
        pdf_bytes=pdf_bytes,
        summary=summary,
    )


def upsert_monthly_inventory_upload_batch(
    *,
    package: MonthlyInventoryReportPackage,
    uploaded_by: int | None,
) -> tuple[InventoryMonthlyUploadBatch, str]:
    batch = InventoryMonthlyUploadBatch.query.filter_by(
        report_year=package.report_year,
        report_month=package.report_month,
    ).one_or_none()
    action = "created"
    now = datetime.now(timezone.utc)
    if batch is None:
        batch = InventoryMonthlyUploadBatch(
            report_year=package.report_year,
            report_month=package.report_month,
        )
        action = "created"
        db_session_add = True
    else:
        action = "replaced"
        db_session_add = False

    batch.report_label = package.report_label
    batch.status = "success"
    batch.error_message = None
    batch.mb51_filename = package.mb51_filename
    batch.me2m_filename = package.me2m_filename
    batch.mc9_filename = package.mc9_filename
    batch.mb51_row_count = package.mb51_row_count
    batch.me2m_row_count = package.me2m_row_count
    batch.mc9_row_count = package.mc9_row_count
    batch.summary_json = json.dumps(package.summary, ensure_ascii=True)
    batch.excel_filename = package.excel_filename
    batch.excel_content_type = XLSX_MIME
    batch.excel_file_size = len(package.excel_bytes)
    batch.excel_data = package.excel_bytes
    batch.pdf_filename = package.pdf_filename
    batch.pdf_content_type = PDF_MIME
    batch.pdf_file_size = len(package.pdf_bytes)
    batch.pdf_data = package.pdf_bytes
    batch.uploaded_by = uploaded_by
    batch.updated_at = now
    if not batch.uploaded_at:
        batch.uploaded_at = now

    if db_session_add:
        db.session.add(batch)
    return batch, action


def list_monthly_inventory_upload_batches(limit: int = 18) -> list[dict[str, Any]]:
    batches = (
        InventoryMonthlyUploadBatch.query.order_by(
            InventoryMonthlyUploadBatch.report_year.desc(),
            InventoryMonthlyUploadBatch.report_month.desc(),
            InventoryMonthlyUploadBatch.updated_at.desc(),
        )
        .limit(limit)
        .all()
    )
    results: list[dict[str, Any]] = []
    for batch in batches:
        try:
            summary = json.loads(batch.summary_json or "{}")
        except Exception:
            summary = {}
        results.append(
            {
                "id": batch.id,
                "report_year": batch.report_year,
                "report_month": batch.report_month,
                "report_label": batch.report_label,
                "mb51_filename": batch.mb51_filename,
                "me2m_filename": batch.me2m_filename,
                "mc9_filename": batch.mc9_filename,
                "mb51_row_count": batch.mb51_row_count,
                "me2m_row_count": batch.me2m_row_count,
                "mc9_row_count": batch.mc9_row_count,
                "uploaded_at": batch.uploaded_at,
                "updated_at": batch.updated_at,
                "summary": summary,
            }
        )
    return results


def get_monthly_inventory_upload_batch(
    batch_id: int,
    *,
    include_excel: bool = False,
    include_pdf: bool = False,
) -> InventoryMonthlyUploadBatch | None:
    query = InventoryMonthlyUploadBatch.query
    if include_excel:
        query = query.options(undefer(InventoryMonthlyUploadBatch.excel_data))
    if include_pdf:
        query = query.options(undefer(InventoryMonthlyUploadBatch.pdf_data))
    return query.filter_by(id=batch_id).one_or_none()
