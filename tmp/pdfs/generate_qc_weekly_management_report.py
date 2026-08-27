from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak

from app import create_app
from app.core.services.quality_control import portfolio_management_data


OUTPUT = Path("output/pdf/QC Portfolio Monitoring Report.pdf")
NAVY = colors.HexColor("#102A43")
BLUE = colors.HexColor("#1D4ED8")
MUTED = colors.HexColor("#5F6C80")
RED = colors.HexColor("#B42318")
GREEN = colors.HexColor("#027A48")


def cell(value, style):
    return Paragraph(str(value), style)


def page_number(canvas, document):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(MUTED)
    canvas.drawString(14 * mm, 10 * mm, "ONGC Laboratories - Corporate Chemistry | QC Portfolio Monitoring Report")
    canvas.drawRightString(283 * mm, 10 * mm, f"Page {document.page}")
    canvas.restoreState()


def make_table(rows, widths, header_style, body_style):
    table = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D7DEE8")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FAFC")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def period_label(period):
    if not period:
        return "No reporting data available"
    return f"{period['start']:%d %b %Y} to {period['end']:%d %b %Y}"


def movement(value):
    if value is None:
        return "-"
    return f"{'+' if value > 0 else ''}{value}"


def main():
    app = create_app()
    with app.app_context():
        data = portfolio_management_data()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(str(OUTPUT), pagesize=landscape(A4), rightMargin=12*mm, leftMargin=12*mm, topMargin=14*mm, bottomMargin=16*mm)
    styles = getSampleStyleSheet()
    title = ParagraphStyle("title", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=20, leading=24, textColor=NAVY, spaceAfter=4)
    subtitle = ParagraphStyle("subtitle", parent=styles["Normal"], fontSize=9, leading=12, textColor=MUTED, spaceAfter=10)
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=13, textColor=NAVY, spaceBefore=8, spaceAfter=5)
    header = ParagraphStyle("header", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=7, leading=8, textColor=colors.white)
    body = ParagraphStyle("body", parent=styles["Normal"], fontSize=6.6, leading=8)
    small = ParagraphStyle("small", parent=body, textColor=MUTED)
    validity = period_label(data["reporting_period"])
    reporting_labs = data["reporting_labs"]
    story = [
        Paragraph("QC Portfolio Monitoring Report", title),
        Paragraph(
            f"Corporate Chemistry | Data validity: <b>{validity}</b> | "
            f"Latest local reporting records from {reporting_labs} {'laboratory' if reporting_labs == 1 else 'laboratories'} | "
            f"Generated {datetime.now().strftime('%d %b %Y, %I:%M %p')}",
            subtitle,
        ),
    ]
    summary = data["summary"]
    kpis = [[cell("SAMPLE LOAD", header), cell("OPEN WORKLOAD", header), cell("QUALITY OUTCOME", header), cell("AVERAGE TURNAROUND", header)], [cell(f"<b>{summary['total']}</b> latest samples", body), cell(f"<b>{summary['under_testing']}</b> open; {summary['delayed_open']} aged >9 days", body), cell(f"<b>{summary['passed']} pass / {summary['failed']} fail</b>", body), cell(f"<b>{summary['average_turnaround'] or 'N/A'} days</b> for completed reports", body)]]
    kpi_table = Table(kpis, colWidths=[67*mm]*4)
    kpi_table.setStyle(TableStyle([("BACKGROUND", (0,0), (-1,0), BLUE), ("BACKGROUND", (0,1), (-1,1), colors.HexColor("#EFF6FF")), ("GRID", (0,0), (-1,-1), .25, colors.HexColor("#B8D2F5")), ("VALIGN", (0,0), (-1,-1), "MIDDLE"), ("TOPPADDING", (0,0), (-1,-1), 7), ("BOTTOMPADDING", (0,0), (-1,-1), 7), ("LEFTPADDING", (0,0), (-1,-1), 6)]))
    story += [kpi_table, Spacer(1, 6), Paragraph("Period-on-Period Comparison", h2)]
    comparison = data["week_on_week"]
    if comparison["available"]:
        current_period = period_label(comparison["current_period"])
        previous_period = period_label(comparison["previous_period"])
        current, previous = comparison["current"], comparison["previous"]
        comparison_rows = [[cell(x, header) for x in ["Metric", f"Current ({current_period})", f"Previous ({previous_period})", "Movement"]]]
        for label, key, suffix in [
            ("Sample load", "total", ""), ("Open workload", "under_testing", ""),
            ("Aged open samples", "delayed_open", ""), ("Pass results", "passed", ""),
            ("Fail results", "failed", ""), ("Average turnaround", "average_turnaround", " days"),
        ]:
            current_value, previous_value = current.get(key), previous.get(key)
            delta = None if current_value is None or previous_value is None else round(current_value - previous_value, 1)
            comparison_rows.append([cell(label, body), cell(f"{current_value if current_value is not None else '-'}{suffix}", body), cell(f"{previous_value if previous_value is not None else '-'}{suffix}", body), cell(f"{movement(delta)}{suffix}" if delta is not None else "-", body)])
        story += [make_table(comparison_rows, [55*mm, 70*mm, 70*mm, 45*mm], header, body), Spacer(1, 6)]
    else:
        story += [Paragraph("No prior local reporting record is available for a like-for-like comparison. This section will populate automatically after a laboratory's next local workbook is imported.", small), Spacer(1, 6)]

    story += [Paragraph("Laboratory Performance", h2)]
    lab_rows = [[cell(x, header) for x in ["Laboratory", "Period", "Samples", "Open", "Aged >9", "Pass", "Fail", "Average TAT"]]]
    for review in data["laboratory_reviews"]:
        b, s = review["batch"], review["summary"]
        period = f"{b.week_start:%d %b} - {b.week_end:%d %b %Y}" if b else "No data"
        lab_rows.append([cell(x, body) for x in [review["laboratory"]["name"], period, s["total"], s["under_testing"], s["delayed_open"], s["passed"], s["failed"], f"{s['average_turnaround']} days" if s["average_turnaround"] is not None else "-"]])
    story += [make_table(lab_rows, [42*mm, 39*mm, 17*mm, 17*mm, 19*mm, 15*mm, 15*mm, 25*mm], header, body)]
    if comparison["available"]:
        story += [Paragraph("Laboratory Period-on-Period Movement", h2)]
        lab_movement = [[cell(x, header) for x in ["Laboratory", "Sample load", "Open workload", "Pass results", "Fail results", "Average TAT"]]]
        for item in comparison["laboratory_metrics"]:
            lab_movement.append([cell(x, body) for x in [
                item["laboratory"]["name"], movement(item["sample_change"]), movement(item["open_change"]),
                movement(item["pass_change"]), movement(item["fail_change"]),
                f"{movement(item['tat_change'])} days" if item["tat_change"] is not None else "-",
            ]])
        story += [make_table(lab_movement, [55*mm, 38*mm, 42*mm, 38*mm, 38*mm, 45*mm], header, body)]
    story += [PageBreak(), Paragraph("Completed Testing: Actual vs Standard", h2)]
    completed = [[cell(x, header) for x in ["Laboratory", "Chemical", "Outcome", "Actual", "Standard", "Variance"]]]
    for item in data["completed_testing"]:
        sample, variance = item["sample"], item["variance_days"]
        completed.append([cell(x, body) for x in [data["laboratories_by_code"][sample.lab_code]["name"], sample.chemical_name, sample.result_status.title(), f"{sample.turnaround_days} days" if sample.turnaround_days is not None else "-", f"{item['standard_days']} days" if item["standard_days"] is not None else "Not defined", f"{'+' if variance > 0 else ''}{variance} days" if variance is not None else "-"]])
    story += [make_table(completed, [35*mm, 110*mm, 25*mm, 23*mm, 28*mm, 28*mm], header, body), PageBreak(), Paragraph("Reporting-Period Chemical Performance Analytics", h2)]
    chemicals = [[cell(x, header) for x in ["Chemical", "Laboratories", "Samples", "Pass", "Fail", "Open", "Avg Actual", "Standard", "Variance"]]]
    for item in data["weekly_chemical_metrics"]:
        variance = item["variance_days"]
        chemicals.append([cell(x, body) for x in [item["chemical_name"], ", ".join(item["laboratories"]), item["total"], item["passed"], item["failed"], item["under_testing"], f"{item['average_actual']} days" if item["average_actual"] is not None else "-", f"{item['standard_days']} days" if item["standard_days"] is not None else "Not defined", f"{'+' if variance > 0 else ''}{variance} days" if variance is not None else "-"]])
    story += [make_table(chemicals, [62*mm, 55*mm, 14*mm, 13*mm, 13*mm, 13*mm, 21*mm, 23*mm, 20*mm], header, body), Paragraph("Aged-Open Sample Register", h2)]
    overdue = [[cell(x, header) for x in ["Laboratory", "Chemical", "Receipt date", "Age", "Follow-up remark"]]]
    for sample in data["overdue_samples"]:
        overdue.append([cell(x, body) for x in [data["laboratories_by_code"][sample.lab_code]["name"], sample.chemical_name, sample.sample_receipt_date.strftime("%d %b %Y") if sample.sample_receipt_date else "-", f"{sample.days_open} days", sample.delay_reason or "No delay remark recorded"]])
    story += [make_table(overdue, [35*mm, 65*mm, 25*mm, 18*mm, 91*mm], header, body)]
    doc.build(story, onFirstPage=page_number, onLaterPages=page_number)


if __name__ == "__main__":
    main()
