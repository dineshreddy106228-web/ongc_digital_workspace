from datetime import date
from io import BytesIO

from openpyxl import Workbook

from app.core.services.quality_control import build_summary, parse_weekly_qc_workbook


def test_parses_standard_weekly_qc_workbook_and_preserves_missing_outcome():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["", "Weekly Data (06.08.2026 to 12.08.2026)"])
    sheet.append([
        "Sl. No.", "Name of Chemical", "Corporate Specification No. / Tentative Specification",
        "Bulk/ Payment", "PO No.", "Lot/ Stack", "Notification No.* (in case of LABIMS)",
        "Pass/ Fail", "Date of sample receipt", "Date of issue of report", "Time taken for testing",
        "Delay reason if more than 09 days / Remarks",
    ])
    sheet.append([1, "PLA Granules", "Tentative", "Bulk", "4075028123", "1/1", "", "", "09.07.2026", "12.08.2026", "24 days", "External testing"])
    sheet.append([2, "PAC-RG", "ONGC / MC / 48 /2015", "Bulk", "4075028028", "1/1", "20000036052", "Fail", "01.07.2026", "07.08.2026", "27 days", "Out of specification"])
    stream = BytesIO()
    workbook.save(stream)

    payload = parse_weekly_qc_workbook(stream.getvalue())

    assert payload.week_start == date(2026, 8, 6)
    assert payload.week_end == date(2026, 8, 12)
    assert len(payload.rows) == 2
    assert payload.rows[0]["result_status"] == "report_issued"
    assert payload.rows[1]["result_status"] == "fail"
    assert build_summary(payload.rows, payload.week_end) == {
        "total": 2, "under_testing": 0, "issued": 2, "passed": 0, "failed": 1,
        "delayed_open": 0, "average_turnaround": 25.5,
    }


def test_uses_reporting_period_in_filename_when_workbook_has_no_title():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Sl. No.", "Name of Chemical", "Date of sample receipt", "Pass/ Fail"])
    sheet.append([1, "Methanol", "06.08.2026", "Pass"])
    stream = BytesIO()
    workbook.save(stream)

    payload = parse_weekly_qc_workbook(
        stream.getvalue(), "RGL Rajahmundry_Weekly QC data_06.08.2026 to 12.08.2026.xlsx",
    )

    assert payload.week_start == date(2026, 8, 6)
    assert payload.week_end == date(2026, 8, 12)
    assert payload.rows[0]["chemical_name"] == "Methanol"
