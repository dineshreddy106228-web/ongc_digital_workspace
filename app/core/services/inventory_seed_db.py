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


def _scalar(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _consumption_rows(frame: pd.DataFrame, import_batch: str, source_filename: str | None, user_id: int | None):
    imported_at = datetime.now(timezone.utc)
    records = []
    for row in frame.itertuples(index=False):
        record = {
            "import_batch": import_batch,
            "source_filename": source_filename,
            "imported_at": imported_at,
            "imported_by": user_id,
            "material_code": _scalar(getattr(row, "material_code", None)),
            "material_desc": _scalar(getattr(row, "material_desc", None)),
            "plant": _scalar(getattr(row, "plant", None)),
            "reporting_plant": _scalar(getattr(row, "reporting_plant", None)),
            "storage_location": _scalar(getattr(row, "storage_location", None)),
            "movement_type": _scalar(getattr(row, "movement_type", None)),
            "posting_date": _scalar(getattr(row, "posting_date", None)),
            "year": _scalar(getattr(row, "year", None)),
            "month": _scalar(getattr(row, "month", None)),
            "month_raw": _scalar(getattr(row, "month_raw", None)),
            "financial_year": _scalar(getattr(row, "financial_year", None)),
            "usage_qty": _scalar(getattr(row, "usage_qty", None)),
            "usage_value": _scalar(getattr(row, "usage_value", None)),
            "uom": _scalar(getattr(row, "uom", None)),
            "currency": _scalar(getattr(row, "currency", None)),
            "po_number": _scalar(getattr(row, "po_number", None)),
            "material_document": _scalar(getattr(row, "material_document", None)),
            "material_document_item": _scalar(getattr(row, "material_document_item", None)),
        }
        records.append(record)
    return records


def _procurement_rows(frame: pd.DataFrame, import_batch: str, source_filename: str | None, user_id: int | None):
    imported_at = datetime.now(timezone.utc)
    records = []
    for row in frame.itertuples(index=False):
        doc_date = _scalar(getattr(row, "doc_date", None))
        year = None
        month = None
        if isinstance(doc_date, datetime):
            year = doc_date.year
            month = doc_date.month
        record = {
            "import_batch": import_batch,
            "source_filename": source_filename,
            "imported_at": imported_at,
            "imported_by": user_id,
            "material_code": _scalar(getattr(row, "material_code", None)),
            "material_desc": _scalar(getattr(row, "material_desc", None)),
            "plant": _scalar(getattr(row, "plant", None)),
            "reporting_plant": _scalar(getattr(row, "reporting_plant", None)),
            "vendor": _scalar(getattr(row, "vendor", None)),
            "po_number": _scalar(getattr(row, "po_number", None)),
            "item": _scalar(getattr(row, "item", None)),
            "doc_date": doc_date,
            "year": year,
            "month": month,
            "financial_year": _scalar(getattr(row, "financial_year", None)),
            "order_qty": _scalar(getattr(row, "order_qty", None)),
            "order_unit": _scalar(getattr(row, "order_unit", None)),
            "net_price": _scalar(getattr(row, "net_price", None)),
            "unit_price": _scalar(getattr(row, "unit_price", None)),
            "price_unit": _scalar(getattr(row, "price_unit", None)),
            "effective_value": _scalar(getattr(row, "effective_value", None)),
            "currency": _scalar(getattr(row, "currency", None)),
            "release_indicator": _scalar(getattr(row, "release_indicator", None)),
        }
        records.append(record)
    return records


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
