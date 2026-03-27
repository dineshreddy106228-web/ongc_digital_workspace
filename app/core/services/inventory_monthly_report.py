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


def _escape_pdf_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_pdf_pages(summary: dict[str, Any]) -> list[list[tuple[str, int]]]:
    pages: list[list[tuple[str, int]]] = [[]]

    def add_line(text: str, size: int = 10) -> None:
        nonlocal pages
        if len(pages[-1]) >= 48:
            pages.append([])
        pages[-1].append((text, size))

    add_line("Inventory Monthly Executive Summary", 16)
    add_line(summary["report_label"], 12)
    add_line("", 10)
    add_line("Key Metrics", 12)
    for label, value in [
        ("Materials in rollup", summary["kpis"]["materials"]),
        ("Reporting plants", summary["kpis"]["reporting_plants"]),
        ("Vendors", summary["kpis"]["vendors"]),
        ("MB51 monthly consumption (MT)", f"{summary['kpis']['total_mb51_consumption_mt']:,.2f}"),
        ("MB51 monthly consumption value", f"Rs {summary['kpis']['total_mb51_consumption_value']:,.2f}"),
        ("MC.9 closing stock value", f"Rs {summary['kpis']['total_closing_stock_value']:,.2f}"),
        ("ME2M procurement value", f"Rs {summary['kpis']['total_procurement_value']:,.2f}"),
        ("ME2M open quantity", f"{summary['kpis']['total_open_qty']:,.2f}"),
    ]:
        add_line(f"- {label}: {value}")

    add_line("", 10)
    add_line("Observations", 12)
    for observation in summary["observations"]:
        for part in textwrap.wrap(f"- {observation}", width=92) or ["-"]:
            add_line(part)

    for title, rows, value_key in [
        ("Top MB51 Consumption by Value", summary["top_consumption_by_value"], "monthly_usage_value"),
        ("Top ME2M Procurement by Value", summary["top_procurement_by_value"], "effective_value"),
        ("Top MC.9 Closing Stock by Value", summary["top_stock_by_value"], "closing_stock_value"),
    ]:
        add_line("", 10)
        add_line(title, 12)
        if not rows:
            add_line("- No rows in this section.")
            continue
        for index, row in enumerate(rows[:10], start=1):
            line = f"{index:>2}. {row['material_label'][:56]:<56} Rs {row[value_key]:>12,.2f}"
            add_line(line)

    return pages


def _render_pdf(pages: list[list[tuple[str, int]]]) -> bytes:
    objects: list[bytes | None] = []

    def add_object(payload: bytes) -> int:
        objects.append(payload)
        return len(objects)

    font_id = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    pages_id = add_object(b"<< >>")
    page_ids: list[int] = []

    for page_lines in pages:
        commands: list[str] = []
        y = 790
        for text, size in page_lines:
            if text:
                commands.append(f"BT /F1 {size} Tf 50 {y} Td ({_escape_pdf_text(text)}) Tj ET")
            y -= 18 if size >= 12 else 14
        stream = "\n".join(commands).encode("latin-1", errors="replace")
        content_id = add_object(b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream")
        page_id = add_object(
            (
                f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 595 842] "
                f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>"
            ).encode("ascii")
        )
        page_ids.append(page_id)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    objects[pages_id - 1] = f"<< /Type /Pages /Count {len(page_ids)} /Kids [{kids}] >>".encode("ascii")
    catalog_id = add_object(f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("ascii"))

    output = BytesIO()
    output.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(output.tell())
        output.write(f"{index} 0 obj\n".encode("ascii"))
        output.write(obj or b"")
        output.write(b"\nendobj\n")
    xref_offset = output.tell()
    output.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    output.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.write(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.write(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF"
        ).encode("ascii")
    )
    return output.getvalue()


def _build_pdf_report(summary: dict[str, Any]) -> bytes:
    return _render_pdf(_build_pdf_pages(summary))


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
