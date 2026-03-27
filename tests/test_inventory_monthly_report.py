from __future__ import annotations

from io import BytesIO
from pathlib import Path
import sys

from openpyxl import load_workbook
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services.inventory_monthly_report import build_monthly_inventory_report_package


def _xlsx_bytes(frame: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False)
    return buffer.getvalue()


def test_build_monthly_inventory_report_package_generates_excel_and_pdf() -> None:
    mb51 = pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Material": "090001043",
                "Material Description": "BARYTES",
                "Movement Type": "201",
                "Posting Date": "2026-03-15",
                "Qty in unit of entry": 2.0,
                "Unit of Entry": "MT",
                "Amt.in Loc.Cur.": 500000.0,
                "Storage Location": "0001",
            },
            {
                "Plant": "10D1",
                "Material": "090001043",
                "Material Description": "BARYTES",
                "Movement Type": "201",
                "Posting Date": "2026-02-10",
                "Qty in unit of entry": 1.0,
                "Unit of Entry": "MT",
                "Amt.in Loc.Cur.": 250000.0,
                "Storage Location": "0001",
            },
        ]
    )
    me2m = pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Net Price": 100000.0,
                "Purchasing Document": "4500001",
                "Document Date": "2026-03-05",
                "Material": "090001043",
                "Item": "10",
                "Supplier/Supplying Plant": "Vendor A",
                "Short Text": "BARYTES",
                "Order Quantity": 5.0,
                "Still to be delivered (qty)": 1.0,
                "Order Unit": "MT",
                "Currency": "INR",
                "Price Unit": 1.0,
                "Effective value": 500000.0,
                "Release indicator": "R",
            }
        ]
    )
    mc9 = pd.DataFrame(
        [
            {
                "Plant": "10D1 PLANT",
                "Material": "090001043 BARYTES",
                "Storage Location": "0001",
                "Month": "3.2026",
                "ValStckVal": 900000.0,
                "ValStckVal.1": "INR",
                "Val. stock": 9.0,
                "Val. stock.1": "MT",
                "Tot. usage": 2.0,
                "Tot. usage.1": "MT",
                "Tot.us.val": 500000.0,
                "Tot.us.val.1": "INR",
            },
            {
                "Plant": "10D1 PLANT",
                "Material": "090001043 BARYTES",
                "Storage Location": "0001",
                "Month": "2.2026",
                "ValStckVal": 700000.0,
                "ValStckVal.1": "INR",
                "Val. stock": 7.0,
                "Val. stock.1": "MT",
                "Tot. usage": 1.0,
                "Tot. usage.1": "MT",
                "Tot.us.val": 250000.0,
                "Tot.us.val.1": "INR",
            },
        ]
    )

    package = build_monthly_inventory_report_package(
        report_year=2026,
        report_month=3,
        mb51_bytes=_xlsx_bytes(mb51),
        mb51_filename="mb51_mar_2026.xlsx",
        me2m_bytes=_xlsx_bytes(me2m),
        me2m_filename="me2m_mar_2026.xlsx",
        mc9_bytes=_xlsx_bytes(mc9),
        mc9_filename="mc9_mar_2026.xlsx",
    )

    assert package.report_label == "Mar 2026"
    assert package.mb51_row_count == 1
    assert package.me2m_row_count == 1
    assert package.mc9_row_count == 1
    assert package.pdf_bytes.startswith(b"%PDF-")

    workbook = load_workbook(BytesIO(package.excel_bytes))
    assert workbook.sheetnames == [
        "Overview",
        "Material Rollup",
        "Stock Movement",
        "MB51 Consumption",
        "MC9 Stock",
        "Opening Stock",
        "ME2M Snapshot",
        "MB51 Raw",
        "ME2M Raw",
        "MC9 Raw",
    ]
    rollup = workbook["Material Rollup"]
    assert rollup["A2"].value == "90001043"
    assert rollup["B2"].value == "BARYTES"
    assert rollup["C2"].value == "MT"
    assert rollup["D2"].value == 7
    assert rollup["G2"].value == "MT"
    assert rollup["H2"].value == 2
    assert rollup["J2"].value == 500000

    stock_movement = workbook["Stock Movement"]
    assert stock_movement["A2"].value == "90001043"
    assert stock_movement["B2"].value == "BARYTES"
    assert stock_movement["D2"].value == 7
    assert stock_movement["G2"].value == 2
    assert stock_movement["J2"].value == 9

    opening_stock = workbook["Opening Stock"]
    assert opening_stock["A2"].value == "90001043"
    assert opening_stock["D2"].value == 7
    assert opening_stock["E2"].value == 7


def test_build_monthly_inventory_report_package_rejects_missing_month_rows() -> None:
    mb51 = pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Material": "090001043",
                "Material Description": "BARYTES",
                "Movement Type": "201",
                "Posting Date": "2026-02-15",
                "Qty in unit of entry": 2.0,
                "Unit of Entry": "MT",
                "Amt.in Loc.Cur.": 500000.0,
            }
        ]
    )
    me2m = pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Net Price": 100000.0,
                "Purchasing Document": "4500001",
                "Document Date": "2026-03-05",
                "Material": "090001043",
                "Item": "10",
                "Supplier/Supplying Plant": "Vendor A",
                "Short Text": "BARYTES",
                "Order Quantity": 5.0,
                "Still to be delivered (qty)": 1.0,
                "Order Unit": "MT",
                "Currency": "INR",
                "Price Unit": 1.0,
                "Effective value": 500000.0,
            }
        ]
    )
    mc9 = pd.DataFrame(
        [
            {
                "Plant": "10D1 PLANT",
                "Material": "090001043 BARYTES",
                "Storage Location": "0001",
                "Month": "3.2026",
                "ValStckVal": 900000.0,
                "ValStckVal.1": "INR",
                "Val. stock": 9.0,
                "Val. stock.1": "MT",
                "Tot. usage": 2.0,
                "Tot. usage.1": "MT",
                "Tot.us.val": 500000.0,
                "Tot.us.val.1": "INR",
            }
        ]
    )

    try:
        build_monthly_inventory_report_package(
            report_year=2026,
            report_month=3,
            mb51_bytes=_xlsx_bytes(mb51),
            mb51_filename="mb51_feb_2026.xlsx",
            me2m_bytes=_xlsx_bytes(me2m),
            me2m_filename="me2m_mar_2026.xlsx",
            mc9_bytes=_xlsx_bytes(mc9),
            mc9_filename="mc9_mar_2026.xlsx",
        )
    except ValueError as exc:
        assert "MB51 file does not contain rows for Mar 2026" in str(exc)
    else:
        raise AssertionError("Expected monthly report builder to reject missing MB51 month rows")


def test_build_monthly_inventory_report_package_estimates_opening_stock_when_prior_mc9_month_missing() -> None:
    mb51 = pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Material": "090001043",
                "Material Description": "BARYTES",
                "Movement Type": "201",
                "Posting Date": "2026-03-15",
                "Qty in unit of entry": 2.0,
                "Unit of Entry": "MT",
                "Amt.in Loc.Cur.": 500000.0,
                "Storage Location": "0001",
            }
        ]
    )
    me2m = pd.DataFrame(
        [
            {
                "Plant": "10D1",
                "Net Price": 100000.0,
                "Purchasing Document": "4500001",
                "Document Date": "2026-03-05",
                "Material": "090001043",
                "Item": "10",
                "Supplier/Supplying Plant": "Vendor A",
                "Short Text": "BARYTES",
                "Order Quantity": 5.0,
                "Still to be delivered (qty)": 1.0,
                "Order Unit": "MT",
                "Currency": "INR",
                "Price Unit": 1.0,
                "Effective value": 500000.0,
            }
        ]
    )
    mc9 = pd.DataFrame(
        [
            {
                "Plant": "10D1 PLANT",
                "Material": "090001043 BARYTES",
                "Storage Location": "0001",
                "Month": "3.2026",
                "ValStckVal": 900000.0,
                "ValStckVal.1": "INR",
                "Val. stock": 9.0,
                "Val. stock.1": "MT",
                "Tot. usage": 2.0,
                "Tot. usage.1": "MT",
                "Tot.us.val": 500000.0,
                "Tot.us.val.1": "INR",
            }
        ]
    )

    package = build_monthly_inventory_report_package(
        report_year=2026,
        report_month=3,
        mb51_bytes=_xlsx_bytes(mb51),
        mb51_filename="mb51_mar_2026.xlsx",
        me2m_bytes=_xlsx_bytes(me2m),
        me2m_filename="me2m_mar_2026.xlsx",
        mc9_bytes=_xlsx_bytes(mc9),
        mc9_filename="mc9_mar_2026.xlsx",
    )

    workbook = load_workbook(BytesIO(package.excel_bytes))
    stock_movement = workbook["Stock Movement"]
    assert stock_movement["C2"].value == "MT"
    assert stock_movement["D2"].value == 7
    assert stock_movement["E2"].value == 7
    assert stock_movement["I2"].value == "MT"
    assert stock_movement["J2"].value == 9
