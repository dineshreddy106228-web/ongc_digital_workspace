from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services.inventory_seed_audit import build_seed_audit_report

import pandas as pd


def _minimal_mb51_frame(material_code: str, qty: float = 10.0, value: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Material": material_code,
                "Material Description": "MUD CHEMICAL BARYTES",
                "Movement Type": "201",
                "Posting Date": "2025-04-15",
                "Qty in unit of entry": qty,
                "Unit of Entry": "KG",
                "Amt.in Loc.Cur.": value,
            }
        ]
    )


def _minimal_me2m_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Purchasing Document": "450000001",
                "Document Date": "2025-04-01",
                "Material": "090001043",
                "Supplier/Supplying Plant": "TEST VENDOR",
                "Short Text": "MUD CHEMICAL BARYTES",
                "Order Quantity": 10.0,
                "Still to be delivered (qty)": 0.0,
                "Order Unit": "KG",
                "Price Unit": 1.0,
                "Effective value": 100.0,
                "Currency": "INR",
            }
        ]
    )


def _minimal_mc9_frame(material_label: str, qty: float = 10.0, value: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Plant": "10D1 TEST PLANT",
                "Material": material_label,
                "Storage Location": "0001",
                "Month": "04.2025",
                "ValStckVal": 500.0,
                "ValStckVal.1": "INR",
                "Val. stock": 25.0,
                "Val. stock.1": "KG",
                "Tot. usage": qty,
                "Tot. usage.1": "KG",
                "Tot.us.val": value,
                "Tot.us.val.1": "INR",
            }
        ]
    )


def test_mc9_reconciliation_matches_material_codes_even_when_leading_zero_differs() -> None:
    report = build_seed_audit_report(
        _minimal_mb51_frame("90001043"),
        _minimal_me2m_frame(),
        _minimal_mc9_frame("090001043 MUD CHEMICAL BARYTES"),
        "mb51.xlsx",
        "me2m.xlsx",
        "mc9.xlsx",
    )

    checks = {finding["check"] for finding in report["findings"] if finding["scope"] == "mc9"}
    assert "missing_in_mc9" not in checks
    assert "missing_in_mb51" not in checks
    assert report["summary"]["mc9"]["matched_rows"] == 1


def test_mc9_reconciliation_still_flags_truly_different_material_codes() -> None:
    report = build_seed_audit_report(
        _minimal_mb51_frame("90001043"),
        _minimal_me2m_frame(),
        _minimal_mc9_frame("090001044 MUD CHEMICAL BARYTES"),
        "mb51.xlsx",
        "me2m.xlsx",
        "mc9.xlsx",
    )

    findings = [finding for finding in report["findings"] if finding["check"] == "missing_in_mc9"]
    assert findings
