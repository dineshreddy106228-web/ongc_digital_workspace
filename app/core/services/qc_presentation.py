"""Lazy, on-demand PowerPoint exports for QC laboratory dashboards."""
from __future__ import annotations

from datetime import timedelta
from io import BytesIO
from pathlib import Path

from app.models.quality_control.qc_sample import QCSample
from app.models.quality_control.qc_testing_standard import QCTestingStandard


def build_lab_performance_presentation(lab_code: str, static_folder: str) -> tuple[BytesIO, str]:
    """Create a lab-specific review deck without loading PPTX libraries at startup."""
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.util import Inches, Pt
    from app.core.services.quality_control import CLOSED_SAMPLE_REVIEW_SLA_DAYS, _normalized_chemical, latest_dashboard_data

    data = latest_dashboard_data(lab_code)
    batch = data["batch"]
    if batch is None:
        raise ValueError("Import a weekly QC workbook before downloading a presentation.")
    standards = {item.normalized_name: item.standard_days for item in QCTestingStandard.query.all()}
    month_start = batch.week_end.replace(day=1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    completed = QCSample.query.filter(QCSample.lab_code == lab_code, QCSample.report_issue_date >= month_start, QCSample.report_issue_date < next_month).all()
    completed = [s for s in completed if s.result_status in {"pass", "fail", "report_issued"} and s.turnaround_days is not None]
    def sla(sample): return standards.get(_normalized_chemical(sample.chemical_name)) or CLOSED_SAMPLE_REVIEW_SLA_DAYS
    late, within = [s for s in completed if s.turnaround_days > sla(s)], [s for s in completed if s.turnaround_days <= sla(s)]
    prs = Presentation(); prs.slide_width = Inches(13.333); prs.slide_height = Inches(7.5); blank = prs.slide_layouts[6]
    navy, blue, red, green, grey = "071D42", "1976D2", "C53B3B", "18794E", "526173"
    border = "D7E2EE"
    def color(value): return RGBColor.from_string(value)
    def overview_text(value, limit=74):
        """Keep narrative spreadsheet remarks readable in overview slides."""
        text = " ".join(str(value or "Not recorded").split())
        if len(text) <= limit:
            return text
        return f"{text[:limit + 1].rsplit(' ', 1)[0].rstrip('.,;:')}…"
    def add_text(slide, value, x, y, w, h, size=18, tone=navy, bold=False):
        shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h)); p = shape.text_frame.paragraphs[0]
        p.text = str(value); p.font.size = Pt(size); p.font.bold = bold; p.font.color.rgb = color(tone); p.font.name = "Arial"; return shape
    def add_wrapped_text(slide, value, x, y, w, h, size=18, tone=navy, bold=False):
        shape = add_text(slide, value, x, y, w, h, size, tone, bold)
        shape.text_frame.word_wrap = True
        return shape
    logo = Path(static_folder) / "images" / "ongc-corporate-chemistry-logo.png"
    def rectangle(slide, x, y, w, h, fill, line=None):
        shape = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
        shape.fill.solid(); shape.fill.fore_color.rgb = color(fill)
        shape.line.color.rgb = color(line or fill)
        return shape
    def canvas_background(slide):
        slide.background.fill.solid(); slide.background.fill.fore_color.rgb = color("FFFFFF")
        rectangle(slide, 0, 0, 13.333, .08, navy)
        rectangle(slide, .42, 1.26, 12.45, .015, border)
        rectangle(slide, .42, 6.93, 12.45, .015, border)
    def header(slide, title, page):
        canvas_background(slide)
        add_text(slide, "ONGC CORPORATE CHEMISTRY · QC LABORATORY MONITORING", .42, .27, 7.4, .24, 11, blue, True); add_text(slide, title, .42, .7, 11.5, .46, 27, navy, True)
        if logo.exists(): slide.shapes.add_picture(str(logo), Inches(12.24), Inches(.16), width=Inches(.55), height=Inches(.55))
        add_text(slide, f"Source: {data['laboratory']['name']} QC data · {data['month_label']}", .42, 7.08, 8.6, .16, 8, grey); add_text(slide, f"{page:02d}", 12.5, 7.08, .25, .16, 8, grey)
    def metric(slide, x, y, value, label, tone=navy):
        card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x-.18), Inches(y-.18), Inches(3.45), Inches(1.35))
        card.fill.solid(); card.fill.fore_color.rgb = color("EAF4FF" if tone == blue else "FCEBEC" if tone == red else "EAF7F0" if tone == green else "F2F4F7")
        card.line.color.rgb = color(border)
        add_text(slide, value, x, y, 2.7, .4, 27, tone, True); add_text(slide, label, x, y+.52, 3.0, .25, 13, navy, True)
    def table_slide(title, rows, page, reason=False):
        slide = prs.slides.add_slide(blank); header(slide, title, page)
        if not rows:
            add_text(slide, "No completed samples fall in this category for the selected calendar month.", .8, 2.0, 10.5, .35, 18, green, True)
            return
        if len(rows) == 1:
            sample = rows[0]
            add_text(slide, "Sample detail", .8, 1.5, 3.5, .3, 18, navy, True)
            rectangle(slide, .8, 1.95, 11.6, 2.25, "FFFFFF", border)
            rectangle(slide, .8, 1.95, 11.6, .06, red if reason else green)
            add_text(slide, sample.chemical_name, 1.1, 2.3, 5.2, .35, 24, navy, True)
            add_text(slide, f"Notification: {sample.notification_no or sample.po_number or '—'}", 1.1, 2.78, 3.8, .25, 14, grey)
            add_text(slide, f"Received: {sample.sample_receipt_date:%d %b %Y} · Reported: {sample.report_issue_date:%d %b %Y}", 5.0, 2.78, 4.8, .25, 14, grey)
            add_text(slide, f"Actual TAT: {sample.turnaround_days} days · SLA: {sla(sample)} days · Variance: {sample.turnaround_days-sla(sample):+d} days", 1.1, 3.2, 6.8, .25, 15, red if reason else green, True)
            add_text(slide, f"Delay reason: {sample.delay_reason or 'Not recorded'}" if reason else f"Outcome: {sample.result_status.replace('_', ' ').title()}", 1.1, 3.62, 9.8, .25, 14, navy)
            return
        heads = ["Chemical", "Notification", "Received", "Reported", "TAT", "SLA", "Variance", "Delay reason" if reason else "Outcome"]
        table_height = min(5.35, .31*(len(rows)+1))
        table = slide.shapes.add_table(len(rows)+1, len(heads), Inches(.42), Inches(1.4), Inches(12.45), Inches(table_height)).table
        widths = [2.6,1.25,.85,.85,.55,.55,.7,5.1] if reason else [3.6,1.45,1,1,.65,.65,.85,2.9]
        for i,w in enumerate(widths): table.columns[i].width = Inches(w)
        for c,h in enumerate(heads): table.cell(0,c).text=h; table.cell(0,c).fill.solid(); table.cell(0,c).fill.fore_color.rgb=color(navy)
        for r,s in enumerate(rows,1):
            values=[s.chemical_name, s.notification_no or s.po_number or "—", s.sample_receipt_date.strftime("%d %b") if s.sample_receipt_date else "—", s.report_issue_date.strftime("%d %b") if s.report_issue_date else "—", f"{s.turnaround_days}d", f"{sla(s)}d", f"+{max(s.turnaround_days-sla(s),0)}d", (s.delay_reason or "Not recorded") if reason else s.result_status.replace("_", " ").title()]
            for c,v in enumerate(values):
                table.cell(r,c).text=str(v)
                table.cell(r,c).fill.solid()
                table.cell(r,c).fill.fore_color.rgb = color("F8FBFE" if r % 2 == 0 else "FFFFFF")
        for row in table.rows:
            for cell in row.cells:
                for p in cell.text_frame.paragraphs: p.font.size=Pt(8); p.font.name="Arial"; p.font.color.rgb=color(navy)
        for cell in table.rows[0].cells:
            for p in cell.text_frame.paragraphs: p.font.size=Pt(8); p.font.bold=True; p.font.color.rgb=color("FFFFFF")
    slide = prs.slides.add_slide(blank)
    canvas_background(slide)
    slide.background.fill.solid(); slide.background.fill.fore_color.rgb = color("EAF4FF")
    rectangle(slide, 12.48, .08, .85, 6.85, "D7EAFB")
    add_text(slide, "QC LABORATORY MONITORING", .76, 1.28, 5.5, .28, 14, blue, True)
    add_text(slide, data["laboratory"]["name"], .76, 1.82, 10.9, .7, 50, navy, True)
    rectangle(slide, .76, 2.75, 1.15, .045, blue)
    add_text(slide, f"Performance Review · {month_start:%B %Y}", .76, 3.08, 8.5, .36, 24, navy, True)
    add_text(slide, "Weekly quality-control performance, Service Level Agreement compliance and exception review", .76, 3.68, 9.9, .36, 16, grey)
    if logo.exists():
        rectangle(slide, 11.08, .46, 1.2, 1.2, "FFFFFF", border)
        slide.shapes.add_picture(str(logo), Inches(11.15), Inches(.53), width=Inches(1.05), height=Inches(1.05))
    add_text(slide, "Office of Head Corporate Chemistry | Mumbai / Dehradun", .76, 6.45, 7.2, .2, 11, grey)

    slide=prs.slides.add_slide(blank); header(slide, f"{data['laboratory']['name']} | Performance metrics", 2); month=data['month_sla']
    for i,(v,l,t) in enumerate([(data['month_intake'],"Monthly intake",blue),(month['closed'],"Closed reports",navy),(month['within_standard'],"Within SLA",green),(month['late'],"Late closures",red),(f"{month['compliance_rate']}%","SLA achieved",blue),(f"{month['average_turnaround']} d","Average TAT",navy)]): metric(slide,.6+(i%3)*4.2,1.7+(i//3)*2.0,v,l,t)
    slide=prs.slides.add_slide(blank); header(slide,"Current-week workload and SLA exceptions",3); week=data['week_sla']
    for i,(v,l,t) in enumerate([(data['summary']['total'],"Samples in review",blue),(data['summary']['under_testing'],"Open workload",navy),(week['closed'],"Closed reports",navy),(week['late'],"Closed above SLA",red),(f"{week['compliance_rate']}%","Weekly SLA",blue),(len(data['overdue_samples']),"Aged open samples",red)]): metric(slide,.6+(i%3)*4.2,1.7+(i//3)*2.0,v,l,t)
    slide=prs.slides.add_slide(blank); header(slide,"Monthly SLA and delay analytics",4)
    metric(slide,.8,1.7,month['within_standard'],"Within applicable SLA",green); metric(slide,4.5,1.7,month['late'],"Late closures",red); metric(slide,8.2,1.7,f"{month['compliance_rate']}%","SLA achieved",blue)
    reasons={}
    for sample in late: reasons[sample.delay_reason or "No reason recorded"] = reasons.get(sample.delay_reason or "No reason recorded",0)+1
    add_text(slide,"Delay reason distribution",.8,3.45,5.5,.3,18,navy,True)
    for i,(reason,count) in enumerate(sorted(reasons.items(), key=lambda item:(-item[1],item[0]))[:5]): add_text(slide,str(count),.8,3.9+i*.42,.3,.2,16,red if "No reason" in reason else blue,True); add_text(slide,overview_text(reason),1.2,3.9+i*.42,10.9,.25,13,navy)
    slide=prs.slides.add_slide(blank); header(slide,"Exception concentration and review focus",5)
    open_exceptions=data['overdue_samples']; add_text(slide,"Current open samples above the 9-day review threshold",.8,1.55,8.5,.3,18,navy,True)
    if open_exceptions:
        sample=open_exceptions[0]; metric(slide,.8,2.35,len(open_exceptions),"Open SLA exceptions",red); add_text(slide,sample.chemical_name,4.5,2.35,4.5,.3,22,navy,True); add_text(slide,f"Notification: {sample.notification_no or sample.po_number or '—'} · Age: {sample.days_open} days",4.5,2.77,5.8,.25,13,grey); add_wrapped_text(slide,f"Reason: {overview_text(sample.delay_reason, 82)}",4.5,3.1,7.7,.5,13,grey)
    else: add_text(slide,"No current open samples are beyond the 9-day threshold.",.8,2.25,7,.3,16,green,True)
    add_text(slide,"Questions for the review",.8,4.35,5,.3,18,navy,True)
    for i,question in enumerate(["Which external testing dependencies require agreed result dates?", "Which late closures need complete delay documentation?", "Who owns each current exception and target closure date?"]): add_text(slide,f"{i+1}",.8,4.8+i*.48,.25,.2,15,blue,True); add_text(slide,question,1.2,4.8+i*.48,9.6,.25,14,navy)
    table_slide("Completed samples above applicable SLA", late, 6, True); table_slide("Completed samples within applicable SLA", within, 7)
    slide=prs.slides.add_slide(blank); header(slide,"Completed-sample product outcomes",8); passed=sum(s.result_status=="pass" for s in completed); failed=sum(s.result_status=="fail" for s in completed); report_issued=sum(s.result_status=="report_issued" for s in completed)
    metric(slide,.7,1.7,passed,"Pass (product outcome)",green); metric(slide,3.8,1.7,failed,"Fail (product outcome)",red); metric(slide,6.9,1.7,report_issued,"Report issued only",blue); metric(slide,10.0,1.7,len(completed),"Completed samples",navy); add_text(slide,"Product pass/fail is quality context only; report-issued records do not have a recorded pass/fail result.",.8,3.25,11.2,.3,14,grey)
    slide=prs.slides.add_slide(blank); header(slide,"SLA assessment basis",9)
    metric(slide,.8,1.7,month['material_standard_count'],"Material-specific SLA",blue); metric(slide,4.5,1.7,month['fallback_sla_count'],"9-day review fallback",blue); metric(slide,8.2,1.7,month['assessed'],"Assessed closures",navy)
    add_text(slide,"Material-specific SLA where defined; otherwise the 9-day management-review SLA is applied.",.8,3.25,10.8,.35,16,grey)
    output=BytesIO(); prs.save(output); output.seek(0); return output, f"{data['laboratory']['name']} Performance Review {month_start:%b %Y}.pptx"


def build_portfolio_management_presentation(static_folder: str, reporting_week_end=None) -> tuple[BytesIO, str]:
    """Create an on-demand, consolidated management-review presentation."""
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.util import Inches, Pt
    from app.core.services.quality_control import portfolio_management_data

    data = portfolio_management_data(reporting_week_end)
    if not data["reporting_labs"]:
        raise ValueError("Import a weekly QC workbook for at least one laboratory before downloading a presentation.")

    prs = Presentation(); prs.slide_width = Inches(13.333); prs.slide_height = Inches(7.5); blank = prs.slide_layouts[6]
    navy, blue, red, green, grey, border = "071D42", "1976D2", "C53B3B", "18794E", "526173", "D7E2EE"
    def color(value): return RGBColor.from_string(value)
    def add_text(slide, value, x, y, w, h, size=18, tone=navy, bold=False, wrap=False):
        shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h)); shape.text_frame.word_wrap = wrap
        paragraph = shape.text_frame.paragraphs[0]; paragraph.text = str(value); paragraph.font.size = Pt(size); paragraph.font.bold = bold; paragraph.font.color.rgb = color(tone); paragraph.font.name = "Arial"
        return shape
    def rectangle(slide, x, y, w, h, fill, line=None):
        shape = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h)); shape.fill.solid(); shape.fill.fore_color.rgb = color(fill); shape.line.color.rgb = color(line or fill)
        return shape
    def concise(value, limit=55):
        text = " ".join(str(value or "Not recorded").split())
        return text if len(text) <= limit else f"{text[:limit + 1].rsplit(' ', 1)[0]}…"
    logo = Path(static_folder) / "images" / "ongc-corporate-chemistry-logo.png"
    period = data["reporting_period"]
    period_label = f"{period['start']:%d %b} – {period['end']:%d %b %Y}" if period else "Latest reporting weeks"
    def background(slide):
        slide.background.fill.solid(); slide.background.fill.fore_color.rgb = color("FFFFFF")
        rectangle(slide, 0, 0, 13.333, .08, navy); rectangle(slide, .42, 1.26, 12.45, .015, border); rectangle(slide, .42, 6.93, 12.45, .015, border)
    def header(slide, title, page):
        background(slide); add_text(slide, "ONGC CORPORATE CHEMISTRY · QC LABORATORY MONITORING", .42, .27, 7.6, .24, 11, blue, True); add_text(slide, title, .42, .7, 11.35, .46, 27, navy, True)
        if logo.exists(): slide.shapes.add_picture(str(logo), Inches(12.24), Inches(.16), width=Inches(.55), height=Inches(.55))
        add_text(slide, f"Source: Selected reporting week · {period_label}", .42, 7.08, 9.6, .16, 8, grey); add_text(slide, f"{page:02d}", 12.5, 7.08, .25, .16, 8, grey)
    def metric(slide, x, y, value, label, tone=navy):
        card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x-.18), Inches(y-.18), Inches(3.45), Inches(1.35)); card.fill.solid(); card.fill.fore_color.rgb = color("EAF4FF" if tone == blue else "FCEBEC" if tone == red else "EAF7F0" if tone == green else "F2F4F7"); card.line.color.rgb = color(border)
        add_text(slide, value, x, y, 2.8, .4, 27, tone, True); add_text(slide, label, x, y+.52, 3.0, .25, 13, navy, True)
    def table(slide, headers, rows, widths, y=1.45, font_size=9):
        if not rows:
            add_text(slide, "No records require management attention for this view.", .8, 2.1, 10.5, .35, 18, green, True); return
        height = min(5.25, .34 * (len(rows) + 1)); table_shape = slide.shapes.add_table(len(rows) + 1, len(headers), Inches(.42), Inches(y), Inches(12.45), Inches(height)).table
        for index, width in enumerate(widths): table_shape.columns[index].width = Inches(width)
        for col, label in enumerate(headers):
            cell = table_shape.cell(0, col); cell.text = label; cell.fill.solid(); cell.fill.fore_color.rgb = color(navy)
        for row_index, values in enumerate(rows, 1):
            for col, value in enumerate(values):
                cell = table_shape.cell(row_index, col); cell.text = str(value); cell.fill.solid(); cell.fill.fore_color.rgb = color("F8FBFE" if row_index % 2 == 0 else "FFFFFF")
        for row in table_shape.rows:
            for cell in row.cells:
                for paragraph in cell.text_frame.paragraphs:
                    paragraph.font.size = Pt(font_size); paragraph.font.name = "Arial"; paragraph.font.color.rgb = color(navy)
        for cell in table_shape.rows[0].cells:
            for paragraph in cell.text_frame.paragraphs:
                paragraph.font.bold = True; paragraph.font.color.rgb = color("FFFFFF")

    def paginated_table(title, headers, rows, widths, font_size=8, rows_per_slide=12):
        """Add every register row, continuing the exception table across slides."""
        if not rows:
            slide = prs.slides.add_slide(blank)
            header(slide, title, len(prs.slides))
            table(slide, headers, [], widths, font_size=font_size)
            return
        total = len(rows)
        for start in range(0, total, rows_per_slide):
            end = min(start + rows_per_slide, total)
            slide = prs.slides.add_slide(blank)
            suffix = f" ({start + 1}–{end} of {total})" if total > rows_per_slide else ""
            header(slide, f"{title}{suffix}", len(prs.slides))
            table(slide, headers, rows[start:end], widths, font_size=font_size)

    slide = prs.slides.add_slide(blank); background(slide); slide.background.fill.solid(); slide.background.fill.fore_color.rgb = color("EAF4FF"); rectangle(slide, 12.48, .08, .85, 6.85, "D7EAFB")
    add_text(slide, "QC LABORATORY MONITORING", .76, 1.28, 5.5, .28, 14, blue, True); add_text(slide, "Portfolio Management Review", .76, 1.82, 10.5, .7, 50, navy, True); rectangle(slide, .76, 2.75, 1.15, .045, blue)
    add_text(slide, period_label, .76, 3.08, 8.5, .36, 24, navy, True); add_text(slide, f"Consolidated position across {data['reporting_labs']} reporting laboratories", .76, 3.68, 8.5, .36, 16, grey)
    if logo.exists(): rectangle(slide, 11.08, .46, 1.2, 1.2, "FFFFFF", border); slide.shapes.add_picture(str(logo), Inches(11.15), Inches(.53), width=Inches(1.05), height=Inches(1.05))
    add_text(slide, "Office of Head Corporate Chemistry | Mumbai / Dehradun", .76, 6.45, 7.2, .2, 11, grey)

    summary = data["summary"]
    slide = prs.slides.add_slide(blank); header(slide, "Portfolio delivery position", 2)
    for index, item in enumerate([(summary["total"], "Samples in review", blue), (summary["under_testing"], "Open workload", navy), (summary["delayed_open"], "Aged open > 9 days", red), (f"{summary['passed']} / {summary['failed']}", "Pass / fail reports", green), (f"{summary['average_turnaround']} d" if summary["average_turnaround"] is not None else "—", "Average turnaround", blue), (data["reporting_labs"], "Laboratories submitted", navy)]):
        metric(slide, .6 + (index % 3) * 4.2, 1.7 + (index // 3) * 2.0, item[0], item[1], item[2])
    if data["missing_submissions"]:
        missing_names = ", ".join(review["laboratory"]["name"] for review in data["missing_submissions"])
        add_text(slide, f"Excluded from this reporting week (no submission): {missing_names}", .8, 5.0, 11.2, .3, 13, red, True)
    else:
        add_text(slide, "All configured laboratories submitted data for the selected reporting week.", .8, 5.0, 11.2, .3, 13, green, True)

    slide = prs.slides.add_slide(blank); header(slide, "Laboratory performance at a glance", 3)
    rows = []
    for review in data["laboratory_reviews"]:
        batch, item = review["batch"], review["summary"]
        status = "Submitted" if batch else (f"Not received · last {review['latest_available_batch'].week_end:%d %b}" if review["latest_available_batch"] else "No submission")
        rows.append([review["laboratory"]["name"], status, item["total"] if batch else "—", item["under_testing"] if batch else "—", item["delayed_open"] if batch else "—", f"{item['passed']} / {item['failed']}" if batch else "—", f"{item['average_turnaround']} d" if batch and item["average_turnaround"] is not None else "—"])
    table(slide, ["Laboratory", "Submission status", "Samples", "Open", "Aged >9d", "Pass / fail", "Average TAT"], rows, [2.45, 2.1, .85, .8, .95, 1.15, 1.1], font_size=9)

    standard_rows = [item for item in data["completed_testing"] if item["standard_days"] is not None and item["variance_days"] is not None]
    late_rows = [item for item in standard_rows if item["variance_days"] > 0]
    within = sum(item["variance_days"] <= 0 for item in standard_rows)
    compliance = round(within / len(standard_rows) * 100, 1) if standard_rows else None
    slide = prs.slides.add_slide(blank); header(slide, "Service Level Agreement performance", 4)
    for index, item in enumerate([(len(standard_rows), "Completed tests assessed", navy), (within, "Within approved standard", green), (len(late_rows), "Above approved standard", red), (f"{compliance}%" if compliance is not None else "—", "Service Level Agreement achieved", blue)]):
        metric(slide, .7 + index * 3.1, 1.7, item[0], item[1], item[2])
    add_text(slide, "A material-specific approved standard is used where available; no fallback is included in this cross-laboratory comparison.", .8, 3.35, 11.1, .3, 14, grey)

    late_table_rows = [[data["laboratories_by_code"].get(item["sample"].lab_code, {"name": item["sample"].lab_code})["name"], item["sample"].chemical_name, item["sample"].notification_no or item["sample"].po_number or "—", f"{item['sample'].turnaround_days} d", f"{item['standard_days']} d", f"+{item['variance_days']} d", concise(item["sample"].delay_reason)] for item in late_rows]
    paginated_table("Completed tests above SLA", ["Laboratory", "Chemical", "Notification", "Actual", "Standard", "Variance", "Delay reason"], late_table_rows, [1.75, 2.15, 1.2, .75, .85, .85, 4.9])

    open_rows = [[data["laboratories_by_code"].get(sample.lab_code, {"name": sample.lab_code})["name"], sample.chemical_name, sample.notification_no or sample.po_number or "—", sample.sample_receipt_date.strftime("%d %b %Y") if sample.sample_receipt_date else "—", f"{sample.days_open} d", concise(sample.delay_reason)] for sample in data["overdue_samples"]]
    paginated_table("Open samples above 9-day threshold", ["Laboratory", "Material", "Notification", "Received", "Age", "Remarks"], open_rows, [1.85, 2.4, 1.35, 1.15, .7, 4.85])

    chemical_rows = [[item["chemical_name"], ", ".join(item["laboratories"]), item["total"], item["passed"], item["failed"], item["under_testing"], f"{item['average_actual']} d" if item["average_actual"] is not None else "—", f"{item['standard_days']} d" if item["standard_days"] is not None else "—"] for item in data["weekly_chemical_metrics"]]
    paginated_table("Weekly chemical performance", ["Chemical", "Laboratories", "Samples", "Pass", "Fail", "Open", "Average time", "Standard"], chemical_rows, [2.35, 3.35, .75, .65, .65, .65, 1.15, 1.15])

    output = BytesIO(); prs.save(output); output.seek(0)
    return output, f"QC Portfolio Management Review {period['end']:%b %Y}.pptx"
