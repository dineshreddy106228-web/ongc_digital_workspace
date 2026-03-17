"""ME2M (Purchase Orders by Material) ingestion loader.

Reads SAP ME2M exports, normalises columns, computes lead time / delivery
variance / PO status, and upserts into ``me2m_purchase_orders``.
"""

from __future__ import annotations

import logging

import pandas as pd

from app.modules.inventory.ingestion.base_loader import (
    BaseLoader,
    fiscal_year_from_date,
)

logger = logging.getLogger(__name__)


class ME2MLoader(BaseLoader):
    SOURCE = "ME2M"
    TABLE_NAME = "me2m_purchase_orders"

    REQUIRED_COLUMNS = [
        "po_number", "po_item", "po_date", "vendor_no",
        "material_no", "plant", "order_qty", "net_order_value",
    ]

    HEADER_ALIASES: dict[str, str] = {
        # po_number
        "purchasing document": "po_number",
        "purchase order": "po_number",
        "po number": "po_number",
        "purch doc": "po_number",
        "po no": "po_number",
        "purchasing doc": "po_number",
        # po_item
        "item": "po_item",
        "po item": "po_item",
        "line item": "po_item",
        "item no": "po_item",
        "purchasing document item": "po_item",
        # po_date
        "document date": "po_date",
        "po date": "po_date",
        "purchasing doc date": "po_date",
        "doc date": "po_date",
        "order date": "po_date",
        "created on": "po_date",
        # vendor_no
        "vendor": "vendor_no",
        "vendor no": "vendor_no",
        "vendor number": "vendor_no",
        "supplier": "vendor_no",
        "vendor account": "vendor_no",
        # vendor_name
        "vendor name": "vendor_name",
        "supplier name": "vendor_name",
        "name 1": "vendor_name",
        # material_no
        "material": "material_no",
        "material number": "material_no",
        "material no": "material_no",
        "mat no": "material_no",
        # material_desc
        "material description": "material_desc",
        "material desc": "material_desc",
        "short text": "material_desc",
        "description": "material_desc",
        # material_group
        "material group": "material_group",
        "matl group": "material_group",
        "mat grp": "material_group",
        # plant
        "plant": "plant",
        "plnt": "plant",
        # order_qty
        "order quantity": "order_qty",
        "order qty": "order_qty",
        "po quantity": "order_qty",
        "po qty": "order_qty",
        "quantity": "order_qty",
        # gr_qty
        "gr quantity": "gr_qty",
        "gr qty": "gr_qty",
        "goods receipt qty": "gr_qty",
        "qty delivered": "gr_qty",
        "delivered qty": "gr_qty",
        # open_qty
        "open quantity": "open_qty",
        "open qty": "open_qty",
        "still to be delivered qty": "open_qty",
        # uom
        "order unit": "uom",
        "uom": "uom",
        "unit": "uom",
        "base unit": "uom",
        "oun": "uom",
        # net_price
        "net price": "net_price",
        "net price in order currency": "net_price",
        "price": "net_price",
        # net_order_value
        "net order value": "net_order_value",
        "net value": "net_order_value",
        "net order value in ordering currency": "net_order_value",
        "po value": "net_order_value",
        "total value": "net_order_value",
        # currency
        "currency": "currency",
        "crcy": "currency",
        "order currency": "currency",
        # delivery_date
        "delivery date": "delivery_date",
        "del date": "delivery_date",
        "deliv date": "delivery_date",
        "stat del date": "delivery_date",
        "stat-rel del date": "delivery_date",
        # actual_gr_date
        "actual gr date": "actual_gr_date",
        "goods receipt date": "actual_gr_date",
        "gr date": "actual_gr_date",
        "last gr date": "actual_gr_date",
        # purchasing_group
        "purchasing group": "purchasing_group",
        "purch group": "purchasing_group",
        "pgrp": "purchasing_group",
        # purchasing_org
        "purchasing organization": "purchasing_org",
        "purchasing org": "purchasing_org",
        "purch org": "purchasing_org",
        "porg": "purchasing_org",
    }

    UPSERT_COLUMNS = [
        "po_number", "po_item", "po_date", "vendor_no", "vendor_name",
        "material_no", "material_desc", "material_group", "plant",
        "order_qty", "gr_qty", "open_qty", "uom",
        "net_price", "net_order_value", "currency",
        "delivery_date", "actual_gr_date",
        "lead_time_days", "delivery_variance_days", "po_status",
        "purchasing_group", "purchasing_org", "fiscal_year",
    ]

    UNIQUE_COLUMNS = ["po_number", "po_item"]

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def transform(
        self, df: pd.DataFrame, fiscal_year: int | None = None
    ) -> pd.DataFrame:
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()

        # Coerce numeric
        for col in ("order_qty", "gr_qty", "open_qty", "net_price", "net_order_value"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Coerce dates
        for col in ("po_date", "delivery_date", "actual_gr_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

        # Compute lead_time_days
        if "actual_gr_date" in df.columns and "po_date" in df.columns:
            mask = df["actual_gr_date"].notna() & df["po_date"].notna()
            df.loc[mask, "lead_time_days"] = (
                (df.loc[mask, "actual_gr_date"] - df.loc[mask, "po_date"]).dt.days
            )
        if "lead_time_days" not in df.columns:
            df["lead_time_days"] = None

        # Compute delivery_variance_days
        if "actual_gr_date" in df.columns and "delivery_date" in df.columns:
            mask = df["actual_gr_date"].notna() & df["delivery_date"].notna()
            df.loc[mask, "delivery_variance_days"] = (
                (df.loc[mask, "actual_gr_date"] - df.loc[mask, "delivery_date"]).dt.days
            )
        if "delivery_variance_days" not in df.columns:
            df["delivery_variance_days"] = None

        # Compute po_status
        def _status(row):
            gr = row.get("gr_qty")
            oq = row.get("order_qty")
            if pd.isna(gr) or gr == 0:
                return "open"
            if pd.notna(oq) and gr < oq:
                return "partial"
            return "closed"

        df["po_status"] = df.apply(_status, axis=1)

        # Fiscal year from po_date
        if fiscal_year:
            df["fiscal_year"] = fiscal_year
        else:
            df["fiscal_year"] = df["po_date"].apply(fiscal_year_from_date)

        # Format dates for MySQL
        for col in ("po_date", "delivery_date", "actual_gr_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

        # Ensure optional columns exist
        for col in self.UPSERT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df
