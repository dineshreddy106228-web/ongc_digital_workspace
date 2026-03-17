"""MC.9 (Stock / Consumption Analysis) ingestion loader.

Reads SAP MC.9 exports, normalises columns, coerces analysis_period to
YYYY-MM format, computes MAP when missing, and upserts into
``mc9_stock_analysis``.
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from app.modules.inventory.ingestion.base_loader import (
    BaseLoader,
    fiscal_period_from_period,
    fiscal_year_from_period,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Period parsing helpers
# ---------------------------------------------------------------------------

_MONTH_NAMES = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def normalise_period(raw: str) -> str | None:
    """Coerce various SAP period formats to YYYY-MM.

    Accepted inputs: "2024.03", "03/2024", "Mar 2024", "2024-03",
    "2024/03", "202403".
    """
    raw = str(raw).strip()

    # Already YYYY-MM
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        return raw

    # YYYY.MM
    m = re.fullmatch(r"(\d{4})[./](\d{1,2})", raw)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"

    # MM/YYYY or MM.YYYY
    m = re.fullmatch(r"(\d{1,2})[/.](\d{4})", raw)
    if m:
        return f"{m.group(2)}-{int(m.group(1)):02d}"

    # Mon YYYY or YYYY Mon (e.g. "Mar 2024", "2024 Mar")
    m = re.fullmatch(r"([A-Za-z]{3})\s+(\d{4})", raw)
    if m:
        mon = _MONTH_NAMES.get(m.group(1).lower())
        if mon:
            return f"{m.group(2)}-{mon}"
    m = re.fullmatch(r"(\d{4})\s+([A-Za-z]{3})", raw)
    if m:
        mon = _MONTH_NAMES.get(m.group(2).lower())
        if mon:
            return f"{m.group(1)}-{mon}"

    # YYYYMM (6 digits)
    m = re.fullmatch(r"(\d{4})(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    logger.warning("MC9: could not parse analysis_period '%s'", raw)
    return None


# ---------------------------------------------------------------------------
# MC9 Loader
# ---------------------------------------------------------------------------

class MC9Loader(BaseLoader):
    SOURCE = "MC9"
    TABLE_NAME = "mc9_stock_analysis"

    REQUIRED_COLUMNS = [
        "material_no", "plant", "analysis_period", "valuated_stock_qty",
    ]

    HEADER_ALIASES: dict[str, str] = {
        # material_no
        "material": "material_no",
        "material number": "material_no",
        "material no": "material_no",
        "mat no": "material_no",
        # material_desc
        "material description": "material_desc",
        "material desc": "material_desc",
        "description": "material_desc",
        # plant
        "plant": "plant",
        "plnt": "plant",
        # analysis_period
        "period": "analysis_period",
        "analysis period": "analysis_period",
        "year/month": "analysis_period",
        "year month": "analysis_period",
        "year_month": "analysis_period",
        "per": "analysis_period",
        # valuated_stock_qty
        "valuated stock qty": "valuated_stock_qty",
        "valuated unrestricted use stock": "valuated_stock_qty",
        "val stk qty": "valuated_stock_qty",
        "val stock qty": "valuated_stock_qty",
        "closing stock qty": "valuated_stock_qty",
        "closing stock": "valuated_stock_qty",
        "total stock": "valuated_stock_qty",
        # valuated_stock_value
        "valuated stock value": "valuated_stock_value",
        "val stk val": "valuated_stock_value",
        "val stock value": "valuated_stock_value",
        "stock value": "valuated_stock_value",
        "closing stock value": "valuated_stock_value",
        # receipts_qty
        "receipts qty": "receipts_qty",
        "total receipts": "receipts_qty",
        "total receipt qty": "receipts_qty",
        "receipts": "receipts_qty",
        "receipt qty": "receipts_qty",
        # receipts_value
        "receipts value": "receipts_value",
        "receipt value": "receipts_value",
        "total receipt value": "receipts_value",
        # issues_qty
        "issues qty": "issues_qty",
        "total issues": "issues_qty",
        "total issue qty": "issues_qty",
        "issues": "issues_qty",
        "issue qty": "issues_qty",
        # issues_value
        "issues value": "issues_value",
        "issue value": "issues_value",
        "total issue value": "issues_value",
        # moving_avg_price
        "moving average price": "moving_avg_price",
        "moving avg price": "moving_avg_price",
        "map": "moving_avg_price",
        "price": "moving_avg_price",
        "val price": "moving_avg_price",
        # material_group
        "material group": "material_group",
        "matl group": "material_group",
        "mat grp": "material_group",
        # material_type
        "material type": "material_type",
        "matl type": "material_type",
        "mat type": "material_type",
        # uom
        "base unit of measure": "uom",
        "base uom": "uom",
        "bun": "uom",
        "unit": "uom",
        "uom": "uom",
        # currency
        "currency": "currency",
        "crcy": "currency",
    }

    UPSERT_COLUMNS = [
        "material_no", "material_desc", "plant", "analysis_period",
        "fiscal_year", "fiscal_period",
        "valuated_stock_qty", "valuated_stock_value",
        "receipts_qty", "receipts_value",
        "issues_qty", "issues_value",
        "moving_avg_price",
        "material_group", "material_type", "uom", "currency",
    ]

    UNIQUE_COLUMNS = ["material_no", "plant", "analysis_period"]

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def transform(
        self, df: pd.DataFrame, fiscal_year: int | None = None
    ) -> pd.DataFrame:
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()

        # Normalise analysis_period to YYYY-MM
        df["analysis_period"] = df["analysis_period"].apply(normalise_period)
        invalid = df["analysis_period"].isna()
        if invalid.any():
            logger.warning("MC9: %d rows dropped for un-parseable analysis_period", invalid.sum())
            df = df[~invalid].copy()

        # Coerce numeric columns
        numeric_cols = [
            "valuated_stock_qty", "valuated_stock_value",
            "receipts_qty", "receipts_value",
            "issues_qty", "issues_value",
            "moving_avg_price",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Compute MAP when not directly available
        if "moving_avg_price" not in df.columns or df["moving_avg_price"].isna().all():
            if "valuated_stock_value" in df.columns and "valuated_stock_qty" in df.columns:
                safe_qty = df["valuated_stock_qty"].replace(0, float("nan"))
                df["moving_avg_price"] = df["valuated_stock_value"] / safe_qty
        else:
            # Fill missing MAP from value/qty where possible
            missing_map = df["moving_avg_price"].isna()
            if missing_map.any() and "valuated_stock_value" in df.columns:
                safe_qty = df["valuated_stock_qty"].replace(0, float("nan"))
                df.loc[missing_map, "moving_avg_price"] = (
                    df.loc[missing_map, "valuated_stock_value"] / safe_qty[missing_map]
                )

        # Derive fiscal_year and fiscal_period from analysis_period
        df["fiscal_year"] = df["analysis_period"].apply(fiscal_year_from_period)
        df["fiscal_period"] = df["analysis_period"].apply(fiscal_period_from_period)

        # Ensure optional columns exist
        for col in self.UPSERT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df
