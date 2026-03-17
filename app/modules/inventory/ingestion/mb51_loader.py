"""MB51 (Material Document List) ingestion loader.

Reads SAP MB51 exports (Excel or CSV), normalises columns, classifies
movement types, and upserts into the ``mb51_movements`` table.
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from app.modules.inventory.ingestion.base_loader import (
    BaseLoader,
    fiscal_period_from_date,
    fiscal_year_from_date,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Movement-type classification
# ---------------------------------------------------------------------------

_GR_TYPES = {"101", "103", "105", "122"}
_GI_TYPES = {"201", "202", "261", "262", "291", "292", "309", "310"}
_TP_TYPES = {
    "301", "303", "305", "311", "313", "315",
    "321", "322", "323", "344", "351", "641", "642",
}


def classify_movement(mvt_type: str) -> str:
    """Return movement category: GR, GI, TP, or RE."""
    mvt = str(mvt_type).strip()
    if mvt in _GR_TYPES:
        return "GR"
    if mvt in _GI_TYPES:
        return "GI"
    if mvt in _TP_TYPES:
        return "TP"
    # Catch-all: anything not matched above is RE (reversal / other)
    return "RE"


# ---------------------------------------------------------------------------
# MB51 Loader
# ---------------------------------------------------------------------------

class MB51Loader(BaseLoader):
    SOURCE = "MB51"
    TABLE_NAME = "mb51_movements"

    REQUIRED_COLUMNS = [
        "mat_doc_no", "mat_doc_year", "mat_doc_item",
        "posting_date", "material_no", "plant",
        "movement_type", "quantity",
    ]

    # SAP label variants → canonical column name
    HEADER_ALIASES: dict[str, str] = {
        # mat_doc_no
        "material document": "mat_doc_no",
        "mat doc": "mat_doc_no",
        "mat_doc": "mat_doc_no",
        "matl doc": "mat_doc_no",
        "material doc": "mat_doc_no",
        "material document number": "mat_doc_no",
        "mat doc no": "mat_doc_no",
        # mat_doc_year
        "doc year": "mat_doc_year",
        "doc. year": "mat_doc_year",
        "mat doc year": "mat_doc_year",
        "material doc year": "mat_doc_year",
        "document year": "mat_doc_year",
        "matl doc yr": "mat_doc_year",
        # mat_doc_item
        "item": "mat_doc_item",
        "doc item": "mat_doc_item",
        "mat doc item": "mat_doc_item",
        "line": "mat_doc_item",
        "line item": "mat_doc_item",
        "matl doc itm": "mat_doc_item",
        # posting_date
        "posting date": "posting_date",
        "pstng date": "posting_date",
        "pstng_date": "posting_date",
        "post date": "posting_date",
        "posting dt": "posting_date",
        # document_date
        "document date": "document_date",
        "doc date": "document_date",
        "doc_date": "document_date",
        "doc. date": "document_date",
        # material_no
        "material": "material_no",
        "material number": "material_no",
        "material no": "material_no",
        "mat no": "material_no",
        "matl no": "material_no",
        # material_desc
        "material description": "material_desc",
        "material desc": "material_desc",
        "mat desc": "material_desc",
        "matl desc": "material_desc",
        "description": "material_desc",
        # plant
        "plant": "plant",
        "plnt": "plant",
        # storage_location
        "storage location": "storage_location",
        "sloc": "storage_location",
        "stor loc": "storage_location",
        "stge loc": "storage_location",
        # movement_type
        "movement type": "movement_type",
        "mvt type": "movement_type",
        "mvmt type": "movement_type",
        "mv type": "movement_type",
        "mvt": "movement_type",
        # movement_indicator
        "movement indicator": "movement_indicator",
        "mvt ind": "movement_indicator",
        # quantity
        "quantity": "quantity",
        "qty": "quantity",
        "qty in un of entry": "quantity",
        "qty in unit of entry": "quantity",
        # uom
        "base unit of measure": "uom",
        "base uom": "uom",
        "bun": "uom",
        "unit": "uom",
        "uom": "uom",
        # amount_lc
        "amount in lc": "amount_lc",
        "amount in local currency": "amount_lc",
        "amount lc": "amount_lc",
        "amt in lc": "amount_lc",
        "val in lc": "amount_lc",
        "value in lc": "amount_lc",
        # currency
        "currency": "currency",
        "crcy": "currency",
        "loc cur": "currency",
        "local currency": "currency",
        # debit_credit_ind
        "debit credit ind": "debit_credit_ind",
        "debit/credit ind": "debit_credit_ind",
        "d/c ind": "debit_credit_ind",
        "debit credit indicator": "debit_credit_ind",
        "d_c_ind": "debit_credit_ind",
        # purchase_order
        "purchase order": "purchase_order",
        "purchasing document": "purchase_order",
        "po number": "purchase_order",
        "purch doc": "purchase_order",
        # po_item
        "po item": "po_item",
        "item no": "po_item",
        "purchasing document item": "po_item",
        # vendor_no
        "vendor": "vendor_no",
        "vendor no": "vendor_no",
        "vendor number": "vendor_no",
        "supplier": "vendor_no",
        # material_group
        "material group": "material_group",
        "matl group": "material_group",
        "mat grp": "material_group",
        # batch
        "batch": "batch",
        # fiscal_year
        "fiscal year": "fiscal_year",
        "fisc year": "fiscal_year",
        "fisc yr": "fiscal_year",
        # fiscal_period
        "fiscal period": "fiscal_period",
        "period": "fiscal_period",
        "fisc period": "fiscal_period",
    }

    UPSERT_COLUMNS = [
        "mat_doc_no", "mat_doc_year", "mat_doc_item",
        "posting_date", "document_date",
        "material_no", "material_desc", "plant", "storage_location",
        "movement_type", "movement_indicator",
        "quantity", "uom", "amount_lc", "currency", "debit_credit_ind",
        "purchase_order", "po_item", "vendor_no", "material_group", "batch",
        "fiscal_year", "fiscal_period", "mvt_category",
    ]

    UNIQUE_COLUMNS = ["mat_doc_no", "mat_doc_year", "mat_doc_item"]

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def transform(
        self, df: pd.DataFrame, fiscal_year: int | None = None
    ) -> pd.DataFrame:
        # Strip whitespace from string columns
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()

        # Reject rows where movement_type is null or not 3-digit numeric
        valid_mvt = df["movement_type"].apply(
            lambda v: bool(re.fullmatch(r"\d{3}", str(v).strip()))
        )
        rejected = df[~valid_mvt]
        if not rejected.empty:
            logger.warning(
                "MB51: %d rows rejected for invalid movement_type", len(rejected)
            )
        df = df[valid_mvt].copy()

        # Coerce types
        df["mat_doc_year"] = pd.to_numeric(df["mat_doc_year"], errors="coerce").astype("Int64")
        df["mat_doc_item"] = pd.to_numeric(df["mat_doc_item"], errors="coerce").astype("Int64")
        df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce", dayfirst=True)
        df["document_date"] = pd.to_datetime(
            df.get("document_date"), errors="coerce", dayfirst=True
        ) if "document_date" in df.columns else pd.NaT
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["amount_lc"] = pd.to_numeric(df.get("amount_lc"), errors="coerce") if "amount_lc" in df.columns else None

        # Signed quantity: if D/C indicator is 'H' (credit), make negative
        if "debit_credit_ind" in df.columns:
            mask_h = df["debit_credit_ind"].str.upper() == "H"
            df.loc[mask_h, "quantity"] = df.loc[mask_h, "quantity"].abs() * -1

        # Compute fiscal year / period from posting_date when not present
        if "fiscal_year" not in df.columns or df["fiscal_year"].isna().all():
            df["fiscal_year"] = df["posting_date"].apply(fiscal_year_from_date)
        else:
            df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")

        if "fiscal_period" not in df.columns or df["fiscal_period"].isna().all():
            df["fiscal_period"] = df["posting_date"].apply(fiscal_period_from_date)
        else:
            df["fiscal_period"] = pd.to_numeric(df["fiscal_period"], errors="coerce").astype("Int64")

        # Classify movement type
        df["mvt_category"] = df["movement_type"].apply(classify_movement)

        # Format dates for MySQL
        df["posting_date"] = df["posting_date"].dt.strftime("%Y-%m-%d")
        if "document_date" in df.columns:
            df["document_date"] = pd.to_datetime(
                df["document_date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        # Ensure optional columns exist
        for col in self.UPSERT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        return df
