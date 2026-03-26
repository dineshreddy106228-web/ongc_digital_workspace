from __future__ import annotations

from pathlib import Path
from datetime import datetime
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services.inventory_seed_db import _consumption_rows, _procurement_rows


def test_consumption_rows_preserve_nulls_and_timestamps() -> None:
    frame = pd.DataFrame(
        [
            {
                "material_code": "1001",
                "material_desc": "BARITE",
                "plant": "10D1",
                "reporting_plant": "MUMBAI",
                "posting_date": pd.Timestamp("2025-04-01"),
                "year": 2025,
                "month": 4,
                "financial_year": "FY25-26",
                "usage_qty": 10.5,
                "uom": "MT",
                "currency": None,
            }
        ]
    )

    rows = _consumption_rows(frame, "batch-1", "mb51.xlsx", 7)

    assert len(rows) == 1
    assert rows[0]["material_code"] == "1001"
    assert rows[0]["posting_date"] == datetime(2025, 4, 1)
    assert rows[0]["currency"] is None
    assert rows[0]["import_batch"] == "batch-1"
    assert rows[0]["source_filename"] == "mb51.xlsx"
    assert rows[0]["imported_by"] == 7


def test_procurement_rows_derive_year_month_from_doc_date() -> None:
    frame = pd.DataFrame(
        [
            {
                "material_code": "2002",
                "material_desc": "CEMENT",
                "plant": "10W1",
                "reporting_plant": "WIC",
                "vendor": "ABC",
                "doc_date": pd.Timestamp("2025-07-15"),
                "financial_year": "FY25-26",
                "order_qty": 20.0,
                "procured_qty": 15.0,
                "currency": "INR",
            }
        ]
    )

    rows = _procurement_rows(frame, "batch-2", "me2m.xlsx", None)

    assert len(rows) == 1
    assert rows[0]["doc_date"] == datetime(2025, 7, 15)
    assert rows[0]["year"] == 2025
    assert rows[0]["month"] == 7
    assert rows[0]["source_filename"] == "me2m.xlsx"
