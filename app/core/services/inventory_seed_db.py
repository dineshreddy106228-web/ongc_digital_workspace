"""Persist normalized inventory seed data into database tables for fast reads."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import uuid

import pandas as pd
from sqlalchemy import inspect

from app.extensions import db
from app.models.inventory.inventory_consumption_seed import InventoryConsumptionSeed
from app.models.inventory.inventory_procurement_seed import InventoryProcurementSeed

logger = logging.getLogger(__name__)


_CONSUMPTION_COLUMNS = [
    "material_code",
    "material_desc",
    "plant",
    "reporting_plant",
    "storage_location",
    "movement_type",
    "posting_date",
    "year",
    "month",
    "month_raw",
    "financial_year",
    "usage_qty",
    "usage_value",
    "uom",
    "currency",
    "po_number",
    "material_document",
    "material_document_item",
]

_PROCUREMENT_COLUMNS = [
    "material_code",
    "material_desc",
    "plant",
    "reporting_plant",
    "vendor",
    "po_number",
    "item",
    "doc_date",
    "year",
    "month",
    "financial_year",
    "order_qty",
    "still_to_be_delivered_qty",
    "procured_qty",
    "order_unit",
    "unit_price",
    "price_unit",
    "effective_value",
    "currency",
    "release_indicator",
]


def _prepare_datetime_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.Series(series.dt.to_pydatetime(), index=series.index, dtype=object)
    return series.astype(object)


def _records_from_frame(
    frame: pd.DataFrame,
    *,
    ordered_columns: list[str],
    import_batch: str,
    source_filename: str | None,
    user_id: int | None,
    imported_at: datetime,
) -> list[dict]:
    if frame.empty:
        return []

    prepared = frame.reindex(columns=ordered_columns).copy()
    for column in ("posting_date", "doc_date"):
        if column in prepared.columns:
            prepared[column] = _prepare_datetime_series(prepared[column])

    prepared = prepared.astype(object)
    prepared = prepared.where(pd.notna(prepared), None)
    prepared.insert(0, "import_batch", import_batch)
    prepared.insert(1, "source_filename", source_filename)
    prepared.insert(2, "imported_at", imported_at)
    prepared.insert(3, "imported_by", user_id)
    return prepared.to_dict("records")


def _consumption_rows(frame: pd.DataFrame, import_batch: str, source_filename: str | None, user_id: int | None):
    return _records_from_frame(
        frame,
        ordered_columns=_CONSUMPTION_COLUMNS,
        import_batch=import_batch,
        source_filename=source_filename,
        user_id=user_id,
        imported_at=datetime.now(timezone.utc),
    )


def _procurement_rows(frame: pd.DataFrame, import_batch: str, source_filename: str | None, user_id: int | None):
    prepared = frame.copy()
    if "doc_date" in prepared.columns and pd.api.types.is_datetime64_any_dtype(prepared["doc_date"]):
        prepared["year"] = prepared["doc_date"].dt.year.astype("Int64")
        prepared["month"] = prepared["doc_date"].dt.month.astype("Int64")

    return _records_from_frame(
        prepared,
        ordered_columns=_PROCUREMENT_COLUMNS,
        import_batch=import_batch,
        source_filename=source_filename,
        user_id=user_id,
        imported_at=datetime.now(timezone.utc),
    )


def _bulk_insert(model, rows: list[dict], chunk_size: int = 2000) -> None:
    for start in range(0, len(rows), chunk_size):
        db.session.bulk_insert_mappings(model, rows[start:start + chunk_size])


def sync_inventory_seed_tables(
    consumption_frame: pd.DataFrame,
    procurement_frame: pd.DataFrame,
    *,
    consumption_filename: str | None = None,
    procurement_filename: str | None = None,
    user_id: int | None = None,
) -> dict:
    inspector = inspect(db.engine)
    if not (
        inspector.has_table("inventory_consumption_seed_rows")
        and inspector.has_table("inventory_procurement_seed_rows")
    ):
        logger.warning("Normalized inventory seed tables are not available yet; skipping DB sync")
        return {
            "import_batch": "",
            "consumption_rows": 0,
            "procurement_rows": 0,
        }

    import_batch = str(uuid.uuid4())

    cons_rows = _consumption_rows(consumption_frame, import_batch, consumption_filename, user_id)
    proc_rows = _procurement_rows(procurement_frame, import_batch, procurement_filename, user_id)

    db.session.query(InventoryConsumptionSeed).delete()
    db.session.query(InventoryProcurementSeed).delete()

    if cons_rows:
        _bulk_insert(InventoryConsumptionSeed, cons_rows)
    if proc_rows:
        _bulk_insert(InventoryProcurementSeed, proc_rows)

    db.session.commit()
    return {
        "import_batch": import_batch,
        "consumption_rows": len(cons_rows),
        "procurement_rows": len(proc_rows),
    }
