"""Lazy, on-demand PowerPoint exports for QC laboratory dashboards."""
from __future__ import annotations

from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from app.models.quality_control.qc_sample import QCSample
from app.models.quality_control.qc_testing_standard import QCTestingStandard
from app.core.services.qc_data_scope import QC_DATA_START_DATE


# Deck chrome shared by every QC export: one navy rule at the top, a thin border
# above and below the body, the ONGC and Corporate Chemistry marks, and a footer
# carrying the data source.
# Kept module-level so the lab decks cannot drift apart visually.
class _DeckChrome:
    """Slide furniture for a QC deck, bound to one Presentation."""

    NAVY, BLUE, RED, GREEN, GREY = "071D42", "1976D2", "C53B3B", "18794E", "526173"
    BORDER = "D7E2EE"

    def __init__(self, prs, static_folder: str, source_line: str):
        from pptx.util import Inches, Pt
        self.prs = prs
        self.blank = prs.slide_layouts[6]
        self.source_line = source_line
        self.corporate_chemistry_logo = Path(static_folder) / "images" / "ongc-corporate-chemistry-logo.png"
        self.ongc_logo = Path(static_folder) / "images" / "ongc-official-logo.png"
        self._Inches, self._Pt = Inches, Pt

    def color(self, value):
        from pptx.dml.color import RGBColor
        return RGBColor.from_string(value)

    def add_text(self, slide, value, x, y, w, h, size=18, tone=None, bold=False):
        from pptx.util import Inches, Pt
        shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
        p = shape.text_frame.paragraphs[0]
        p.text = str(value)
        p.font.size = Pt(size)
        p.font.bold = bold
        p.font.color.rgb = self.color(tone or self.NAVY)
        p.font.name = "Arial"
        return shape

    def add_wrapped_text(self, slide, value, x, y, w, h, size=18, tone=None, bold=False):
        shape = self.add_text(slide, value, x, y, w, h, size, tone, bold)
        shape.text_frame.word_wrap = True
        return shape

    def rectangle(self, slide, x, y, w, h, fill, line=None):
        from pptx.enum.shapes import MSO_SHAPE
        from pptx.util import Inches
        shape = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
        shape.fill.solid()
        shape.fill.fore_color.rgb = self.color(fill)
        shape.line.color.rgb = self.color(line or fill)
        return shape

    def canvas_background(self, slide):
        slide.background.fill.solid()
        slide.background.fill.fore_color.rgb = self.color("FFFFFF")
        self.rectangle(slide, 0, 0, 13.333, .08, self.NAVY)
        self.rectangle(slide, .42, 1.26, 12.45, .015, self.BORDER)
        self.rectangle(slide, .42, 6.93, 12.45, .015, self.BORDER)

    def add_header_branding(self, slide):
        """Place both approved marks in the deck header without crowding it."""
        if self.ongc_logo.exists():
            slide.shapes.add_picture(str(self.ongc_logo), self._Inches(11.15), self._Inches(.21), width=self._Inches(.9), height=self._Inches(.51))
        if self.corporate_chemistry_logo.exists():
            slide.shapes.add_picture(str(self.corporate_chemistry_logo), self._Inches(12.24), self._Inches(.16), width=self._Inches(.55), height=self._Inches(.55))

    def add_cover_branding(self, slide, background="FFFFFF"):
        """Use the same ONGC + Corporate Chemistry pairing on title slides."""
        self.rectangle(slide, 10.0, .46, 2.32, 1.2, background, self.BORDER)
        if self.ongc_logo.exists():
            slide.shapes.add_picture(str(self.ongc_logo), self._Inches(10.16), self._Inches(.68), width=self._Inches(.9), height=self._Inches(.51))
        if self.corporate_chemistry_logo.exists():
            slide.shapes.add_picture(str(self.corporate_chemistry_logo), self._Inches(11.16), self._Inches(.53), width=self._Inches(1.05), height=self._Inches(1.05))

    def header(self, slide, title, page, source_line=None):
        from pptx.util import Inches
        self.canvas_background(slide)
        self.add_text(slide, "ONGC CORPORATE CHEMISTRY \u00b7 QC LABORATORY MONITORING", .42, .27, 7.4, .24, 11, self.BLUE, True)
        self.add_text(slide, title, .42, .7, 11.5, .46, 27, self.NAVY, True)
        self.add_header_branding(slide)
        self.add_text(slide, source_line if source_line is not None else self.source_line, .42, 7.08, 8.6, .16, 8, self.GREY)
        self.add_text(slide, f"{page:02d}", 12.5, 7.08, .25, .16, 8, self.GREY)

    def metric(self, slide, x, y, value, label, tone=None):
        from pptx.enum.shapes import MSO_SHAPE
        from pptx.util import Inches
        tone = tone or self.NAVY
        card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(x - .18), Inches(y - .18), Inches(3.45), Inches(1.35))
        card.fill.solid()
        card.fill.fore_color.rgb = self.color(
            "EAF4FF" if tone == self.BLUE else "FCEBEC" if tone == self.RED
            else "EAF7F0" if tone == self.GREEN else "F2F4F7"
        )
        card.line.color.rgb = self.color(self.BORDER)
        self.add_text(slide, value, x, y, 2.7, .4, 27, tone, True)
        self.add_text(slide, label, x, y + .52, 3.0, .25, 13, self.NAVY, True)

    def new_slide(self, title, page, source_line=None):
        slide = self.prs.slides.add_slide(self.blank)
        self.header(slide, title, page, source_line=source_line)
        return slide


class _WeeklyReviewChrome(_DeckChrome):
    """QC review furniture matched to the supplied Weekly QC Review model."""

    GOLD = "D7A600"
    TITLE_BLUE = "1F2A6B"
    MODEL_RED = "B4151B"
    MODEL_GREEN = "2E6F31"

    def __init__(self, prs, static_folder: str, source_line: str):
        super().__init__(prs, static_folder, source_line)
        self.cover_images = [
            Path(static_folder) / "images" / "qc-presentation" / "onshore-well.png",
            Path(static_folder) / "images" / "qc-presentation" / "seismic-section.jfif",
            Path(static_folder) / "images" / "qc-presentation" / "subsurface-model.jfif",
            Path(static_folder) / "images" / "qc-presentation" / "drilling-rig.jpeg",
            Path(static_folder) / "images" / "qc-presentation" / "pumpjack.png",
        ]

    def _gradient_rule(self, slide, y, *, x=0, width=13.333):
        """Add a renderer-safe red-to-green rule using native solid fills.

        PowerPoint can discard hand-authored ``gradFill`` XML even when other
        preview engines accept it.  Closely spaced native rectangles retain
        the supplied model's visual gradient and display in desktop PowerPoint,
        LibreOffice and browser previews alike.
        """
        stops = ((180, 21, 27), (241, 216, 194), (46, 111, 49))
        segments = 96
        segment_width = width / segments

        def channel_mix(start, end, progress):
            return round(start + (end - start) * progress)

        for index in range(segments):
            progress = index / (segments - 1)
            if progress <= .5:
                local = progress * 2
                start, end = stops[0], stops[1]
            else:
                local = (progress - .5) * 2
                start, end = stops[1], stops[2]
            fill = "".join(
                f"{channel_mix(start[channel], end[channel], local):02X}"
                for channel in range(3)
            )
            self.rectangle(
                slide, x + index * segment_width, y,
                segment_width + .002, .065, fill,
            )

    def canvas_background(self, slide):
        slide.background.fill.solid()
        slide.background.fill.fore_color.rgb = self.color("FFFFFF")
        if self.ongc_logo.exists():
            slide.shapes.add_picture(
                str(self.ongc_logo), self._Inches(.14), self._Inches(.08),
                width=self._Inches(1.28), height=self._Inches(.72),
            )
        if self.corporate_chemistry_logo.exists():
            slide.shapes.add_picture(
                str(self.corporate_chemistry_logo), self._Inches(1.58), self._Inches(.06),
                width=self._Inches(.68), height=self._Inches(.68),
            )
        self._gradient_rule(slide, .76)

    def header(self, slide, title, page, source_line=None):
        from pptx.enum.text import PP_ALIGN

        self.canvas_background(slide)
        self.add_text(slide, title, .52, .95, 12.1, .48, 25, self.TITLE_BLUE, True)
        self.add_text(slide, source_line if source_line is not None else self.source_line, .52, 7.09, 11.1, .24, 10, self.TITLE_BLUE)
        page_number = self.add_text(slide, f"{page:02d}", 12.05, 7.09, .7, .24, 10, self.GREY)
        page_number.text_frame.word_wrap = False
        page_number.text_frame.paragraphs[0].alignment = PP_ALIGN.RIGHT

    def cover(self, scope_label, as_of_label, title="Weekly QC Review"):
        from pptx.enum.text import PP_ALIGN
        from pptx.util import Inches

        slide = self.prs.slides.add_slide(self.blank)
        slide.background.fill.solid()
        slide.background.fill.fore_color.rgb = self.color(self.GOLD)

        # One clean triangular rail matches the model and avoids the stray gold
        # triangle created when a rectangle and rotated triangle overlap.
        builder = slide.shapes.build_freeform(Inches(0), Inches(0))
        builder.add_line_segments([
            (Inches(1.92), Inches(0)),
            (Inches(0), Inches(5.82)),
        ], close=True)
        wedge = builder.convert_to_shape()
        wedge.fill.solid()
        wedge.fill.fore_color.rgb = self.color("FFFFFF")
        wedge.line.fill.background()

        if self.ongc_logo.exists():
            slide.shapes.add_picture(
                str(self.ongc_logo), Inches(.06), Inches(.06),
                width=Inches(1.48), height=Inches(.83),
            )
        if self.corporate_chemistry_logo.exists():
            slide.shapes.add_picture(
                str(self.corporate_chemistry_logo), Inches(.18), Inches(1.03),
                width=Inches(1.02), height=Inches(1.02),
            )

        title_shape = self.add_text(slide, title, 3.1, 1.82, 7.8, .65, 34, self.TITLE_BLUE, True)
        title_shape.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
        scope = self.add_text(slide, scope_label, 4.42, 2.62, 5.2, .4, 20, self.TITLE_BLUE, True)
        scope.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER
        self._gradient_rule(slide, 3.7, x=2.92, width=8.58)
        date_line = self.add_text(
            slide, f"SAP position as on {as_of_label}",
            2.92, 3.91, 8.58, .32, 13, self.TITLE_BLUE, True,
        )
        date_line.text_frame.paragraphs[0].alignment = PP_ALIGN.CENTER

        existing_images = [path for path in self.cover_images if path.exists()]
        if existing_images:
            image_width = 13.333 / len(existing_images)
            for index, image in enumerate(existing_images):
                slide.shapes.add_picture(
                    str(image), Inches(index * image_width), Inches(5.87),
                    width=Inches(image_width), height=Inches(1.63),
                )
            self.rectangle(slide, 0, 5.72, 13.333, .09, self.MODEL_RED)
        return slide

    def closing(self, page):
        slide = self.new_slide("", page)
        self.add_text(slide, "Thank You", 5.05, 3.18, 3.25, .65, 31, self.TITLE_BLUE, True)
        return slide


def _paginated_rows(rows, page_size: int):
    """Split an operational register without ever dropping the final rows."""
    if page_size < 1:
        raise ValueError("A presentation page must contain at least one row.")
    return [rows[index:index + page_size] for index in range(0, len(rows), page_size)] or [[]]


def _sap_presentation_action_groups(data):
    """Order SAP notification groups and rows by newest notification date.

    Keep the laboratory and specification sections intact, ordered by each
    section's latest notification date. Within each section, notification rows
    are newest first.
    """
    from app.core.services.csc_utils import SPEC_SUBSET_ORDER
    from app.core.services.sap_quality_control import CORPORATE_SPECIFICATION_UNMATCHED_KEY

    laboratory_rank = {
        laboratory["code"]: index
        for index, laboratory in enumerate(data["scope_laboratories"])
    }
    subgroup_rank = {key: index for index, key in enumerate(SPEC_SUBSET_ORDER)}
    grouped: dict[tuple[str, str], list] = {}
    labels: dict[tuple[str, str], str] = {}
    laboratories: dict[str, dict] = {}

    for review in data["laboratory_reviews"]:
        for entry in review["records"]:
            record = entry["record"]
            if not record.notification_no:
                continue
            laboratory = entry["laboratory"]
            subgroup_key = entry["subgroup_key"] or CORPORATE_SPECIFICATION_UNMATCHED_KEY
            key = (laboratory["code"], subgroup_key)
            grouped.setdefault(key, []).append(entry)
            labels[key] = entry["subgroup_label"]
            laboratories[laboratory["code"]] = laboratory

    groups = []
    for key in sorted(
        grouped,
        key=lambda value: (
            laboratory_rank.get(value[0], len(laboratory_rank)),
            99 if value[1] == CORPORATE_SPECIFICATION_UNMATCHED_KEY else subgroup_rank.get(value[1], 90),
            value[1],
        ),
    ):
        entries = sorted(
            grouped[key],
            key=lambda entry: (
                entry["record"].notification_start_date or date.min,
                entry["record"].notification_no or "",
                entry["record"].id,
            ),
            reverse=True,
        )
        groups.append({
            "laboratory": laboratories[key[0]],
            "subgroup_key": key[1],
            "subgroup_label": labels[key],
            "latest_notification_date": max(
                (entry["record"].notification_start_date for entry in grouped[key]
                 if entry["record"].notification_start_date),
                default=date.min,
            ),
            "entries": entries,
        })
    return sorted(
        groups,
        key=lambda group: (
            group["latest_notification_date"],
            group["laboratory"]["name"].casefold(),
            group["subgroup_label"].casefold(),
        ),
        reverse=True,
    )


def build_lab_performance_presentation(lab_code: str, static_folder: str, notification_date_from: date | None = None) -> tuple[BytesIO, str]:
    """Create a lab-specific review deck without loading PPTX libraries at startup."""
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.util import Inches, Pt
    from app.core.services.quality_control import CLOSED_SAMPLE_REVIEW_STT_DAYS, _normalized_chemical, latest_dashboard_data

    data = latest_dashboard_data(lab_code)
    if notification_date_from:
        data["samples"] = [s for s in data["samples"] if s.sample_receipt_date and s.sample_receipt_date >= notification_date_from]
        data["overdue_samples"] = [s for s in data["overdue_samples"] if s.sample_receipt_date and s.sample_receipt_date >= notification_date_from]
        from app.core.services.quality_control import build_summary
        data["summary"] = build_summary(data["samples"], date.today())
    batch = data["batch"]
    if batch is None:
        raise ValueError("Import a local status workbook before downloading a presentation.")
    standards = {item.normalized_name: item.standard_days for item in QCTestingStandard.query.all()}
    month_start = batch.week_end.replace(day=1)
    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    if notification_date_from:
        intake_query = QCSample.query.filter(
            QCSample.lab_code == lab_code,
            QCSample.sample_receipt_date >= max(month_start, notification_date_from),
            QCSample.sample_receipt_date < next_month,
        )
        data["month_intake"] = intake_query.count()
    completed_query = QCSample.query.filter(QCSample.lab_code == lab_code, QCSample.report_issue_date >= month_start, QCSample.report_issue_date < next_month)
    if notification_date_from:
        completed_query = completed_query.filter(QCSample.sample_receipt_date >= notification_date_from)
    completed = completed_query.all()
    completed = [s for s in completed if s.result_status in {"pass", "fail", "report_issued"} and s.turnaround_days is not None]
    def stt(sample): return standards.get(_normalized_chemical(sample.chemical_name)) or CLOSED_SAMPLE_REVIEW_STT_DAYS
    late, within = [s for s in completed if s.turnaround_days > stt(s)], [s for s in completed if s.turnaround_days <= stt(s)]
    prs = Presentation(); prs.slide_width = Inches(13.333); prs.slide_height = Inches(7.5)
    chrome = _DeckChrome(prs, static_folder, f"Source: {data['laboratory']['name']} QC data · From {QC_DATA_START_DATE:%d %b %Y} · {data['month_label']}")
    blank = chrome.blank
    navy, blue, red, green, grey = chrome.NAVY, chrome.BLUE, chrome.RED, chrome.GREEN, chrome.GREY
    border = chrome.BORDER
    color, add_text, add_wrapped_text = chrome.color, chrome.add_text, chrome.add_wrapped_text
    rectangle, header, metric = chrome.rectangle, chrome.header, chrome.metric
    canvas_background = chrome.canvas_background
    def overview_text(value, limit=74):
        """Keep narrative spreadsheet remarks readable in overview slides."""
        text = " ".join(str(value or "Not recorded").split())
        if len(text) <= limit:
            return text
        return f"{text[:limit + 1].rsplit(' ', 1)[0].rstrip('.,;:')}…"
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
            add_text(slide, f"Actual TAT: {sample.turnaround_days} days · STT: {stt(sample)} days · Variance: {sample.turnaround_days-stt(sample):+d} days", 1.1, 3.2, 6.8, .25, 15, red if reason else green, True)
            add_text(slide, f"Delay reason: {sample.delay_reason or 'Not recorded'}" if reason else f"Outcome: {sample.result_status.replace('_', ' ').title()}", 1.1, 3.62, 9.8, .25, 14, navy)
            return
        heads = ["Chemical", "Notification", "Received", "Reported", "TAT", "STT", "Variance", "Delay reason" if reason else "Outcome"]
        table_height = min(5.35, .31*(len(rows)+1))
        table = slide.shapes.add_table(len(rows)+1, len(heads), Inches(.42), Inches(1.4), Inches(12.45), Inches(table_height)).table
        widths = [2.6,1.25,.85,.85,.55,.55,.7,5.1] if reason else [3.6,1.45,1,1,.65,.65,.85,2.9]
        for i,w in enumerate(widths): table.columns[i].width = Inches(w)
        for c,h in enumerate(heads): table.cell(0,c).text=h; table.cell(0,c).fill.solid(); table.cell(0,c).fill.fore_color.rgb=color(navy)
        for r,s in enumerate(rows,1):
            values=[s.chemical_name, s.notification_no or s.po_number or "—", s.sample_receipt_date.strftime("%d %b") if s.sample_receipt_date else "—", s.report_issue_date.strftime("%d %b") if s.report_issue_date else "—", f"{s.turnaround_days}d", f"{stt(s)}d", f"+{max(s.turnaround_days-stt(s),0)}d", (s.delay_reason or "Not recorded") if reason else s.result_status.replace("_", " ").title()]
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
    add_text(slide, "Current quality-control performance, Service Level Agreement compliance and exception review", .76, 3.68, 9.9, .36, 16, grey)
    chrome.add_cover_branding(slide)
    add_text(slide, "Office of Head Corporate Chemistry | Mumbai / Dehradun", .76, 6.45, 7.2, .2, 11, grey)

    slide=prs.slides.add_slide(blank); header(slide, f"{data['laboratory']['name']} | Performance metrics", 2); month=data['month_stt']
    for i,(v,l,t) in enumerate([(data['month_intake'],"Monthly intake",blue),(month['closed'],"Closed reports",navy),(month['within_standard'],"Within STT",green),(month['late'],"Late closures",red),(f"{month['compliance_rate']}%","STT achieved",blue),(f"{month['average_turnaround']} d","Average TAT",navy)]): metric(slide,.6+(i%3)*4.2,1.7+(i//3)*2.0,v,l,t)
    slide=prs.slides.add_slide(blank); header(slide,"Current workload and STT exceptions",3); week=data['week_stt']
    for i,(v,l,t) in enumerate([(data['summary']['total'],"Samples in review",blue),(data['summary']['under_testing'],"Open workload",navy),(week['closed'],"Closed reports",navy),(week['late'],"Closed above STT",red),(f"{week['compliance_rate']}%","Period STT",blue),(len(data['overdue_samples']),"Aged open samples",red)]): metric(slide,.6+(i%3)*4.2,1.7+(i//3)*2.0,v,l,t)
    slide=prs.slides.add_slide(blank); header(slide,"Monthly STT and delay analytics",4)
    metric(slide,.8,1.7,month['within_standard'],"Within applicable STT",green); metric(slide,4.5,1.7,month['late'],"Late closures",red); metric(slide,8.2,1.7,f"{month['compliance_rate']}%","STT achieved",blue)
    reasons={}
    for sample in late: reasons[sample.delay_reason or "No reason recorded"] = reasons.get(sample.delay_reason or "No reason recorded",0)+1
    add_text(slide,"Delay reason distribution",.8,3.45,5.5,.3,18,navy,True)
    for i,(reason,count) in enumerate(sorted(reasons.items(), key=lambda item:(-item[1],item[0]))[:5]): add_text(slide,str(count),.8,3.9+i*.42,.3,.2,16,red if "No reason" in reason else blue,True); add_text(slide,overview_text(reason),1.2,3.9+i*.42,10.9,.25,13,navy)
    slide=prs.slides.add_slide(blank); header(slide,"Exception concentration and review focus",5)
    open_exceptions=data['overdue_samples']; add_text(slide,"Current open samples above the 9-day review threshold",.8,1.55,8.5,.3,18,navy,True)
    if open_exceptions:
        sample=open_exceptions[0]; metric(slide,.8,2.35,len(open_exceptions),"Open STT exceptions",red); add_text(slide,sample.chemical_name,4.5,2.35,4.5,.3,22,navy,True); add_text(slide,f"Notification: {sample.notification_no or sample.po_number or '—'} · Age: {sample.days_open} days",4.5,2.77,5.8,.25,13,grey); add_wrapped_text(slide,f"Reason: {overview_text(sample.delay_reason, 82)}",4.5,3.1,7.7,.5,13,grey)
    else: add_text(slide,"No current open samples are beyond the 9-day threshold.",.8,2.25,7,.3,16,green,True)
    add_text(slide,"Questions for the review",.8,4.35,5,.3,18,navy,True)
    for i,question in enumerate(["Which external testing dependencies require agreed result dates?", "Which late closures need complete delay documentation?", "Who owns each current exception and target closure date?"]): add_text(slide,f"{i+1}",.8,4.8+i*.48,.25,.2,15,blue,True); add_text(slide,question,1.2,4.8+i*.48,9.6,.25,14,navy)
    table_slide("Completed samples above applicable STT", late, 6, True); table_slide("Completed samples within applicable STT", within, 7)
    slide=prs.slides.add_slide(blank); header(slide,"Completed-sample product outcomes",8); passed=sum(s.result_status=="pass" for s in completed); failed=sum(s.result_status=="fail" for s in completed); report_issued=sum(s.result_status=="report_issued" for s in completed)
    metric(slide,.7,1.7,passed,"Pass (product outcome)",green); metric(slide,3.8,1.7,failed,"Fail (product outcome)",red); metric(slide,6.9,1.7,report_issued,"Report issued only",blue); metric(slide,10.0,1.7,len(completed),"Completed samples",navy); add_text(slide,"Product pass/fail is quality context only; report-issued records do not have a recorded pass/fail result.",.8,3.25,11.2,.3,14,grey)
    slide=prs.slides.add_slide(blank); header(slide,"Standard Testing Time (STT) assessment basis",9)
    metric(slide,.8,1.7,month['material_standard_count'],"Material-specific STT",blue); metric(slide,4.5,1.7,month['fallback_stt_count'],"9-day review fallback",blue); metric(slide,8.2,1.7,month['assessed'],"Assessed closures",navy)
    add_text(slide,"Standard Testing Time (STT): the material-specific standard where defined; otherwise the 9-day management-review STT is applied.",.8,3.25,10.8,.35,16,grey)
    output=BytesIO(); prs.save(output); output.seek(0); return output, f"{data['laboratory']['name']} Performance Review {month_start:%b %Y}.pptx"


def build_lab_brief_presentation(lab_code: str, static_folder: str, notification_date_from: date | None = None) -> tuple[BytesIO, str]:
    """The local-reporting management brief as a deck, mirroring the page section for section.

    Deliberately narrower than the performance review: this is the one reporting
    week the brief covers, in the same order the page presents it, so a reader
    can follow the screen and the slide interchangeably.
    """
    from pptx import Presentation
    from pptx.util import Inches
    from app.core.services.quality_control import latest_dashboard_data

    data = latest_dashboard_data(lab_code)
    if notification_date_from:
        data["samples"] = [s for s in data["samples"] if s.sample_receipt_date and s.sample_receipt_date >= notification_date_from]
        from app.core.services.quality_control import build_summary
        data["summary"] = build_summary(data["samples"], date.today())
    batch = data["batch"]
    if batch is None:
        raise ValueError("Import a local status workbook before downloading the management brief.")

    laboratory, summary, samples = data["laboratory"], data["summary"], data["samples"]
    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
    chrome = _DeckChrome(
        prs, static_folder,
        f"Source: {laboratory['name']} local status workbook · From {QC_DATA_START_DATE:%d %b %Y} · {batch.report_label}",
    )
    navy, blue, red, green, grey = chrome.NAVY, chrome.BLUE, chrome.RED, chrome.GREEN, chrome.GREY

    # 01 · Cover
    slide = chrome.new_slide("Quality Control Laboratory — Management Brief", 1)
    chrome.add_text(slide, laboratory["name"], .42, 1.85, 9.0, .6, 34, navy, True)
    chrome.add_text(slide, laboratory["location"], .42, 2.55, 9.0, .35, 16, grey)
    chrome.add_text(slide, batch.report_label, .42, 3.05, 11.0, .4, 20, blue, True)
    chrome.rectangle(slide, .42, 3.62, 3.2, .05, blue)

    # 02 · The four figures the brief leads with
    slide = chrome.new_slide("Current reporting position", 2)
    turnaround = f"{summary['average_turnaround']} d" if summary["average_turnaround"] is not None else "—"
    chrome.metric(slide, .8, 1.9, summary["total"], "Reported sample load", blue)
    chrome.metric(slide, 4.3, 1.9, summary["under_testing"], "Open workload", red if summary["under_testing"] else navy)
    chrome.metric(slide, 7.8, 1.9, f"{summary['passed']}/{summary['failed']}", "Pass / fail reports issued", green)
    chrome.metric(slide, .8, 3.9, turnaround, "Average turnaround, issued", navy)
    chrome.metric(slide, 4.3, 3.9, summary["delayed_open"], "Aged beyond target", red if summary["delayed_open"] else green)
    chrome.metric(slide, 7.8, 3.9, summary["issued"], "Reports issued in this period", navy)

    # 03 · Management attention required
    attention = [s for s in samples if s.result_status == "under_testing" and s.delay_reason]
    slide = chrome.new_slide("Management attention required", 3)
    if attention:
        y = 1.75
        for sample in attention[:8]:
            received = sample.sample_receipt_date.strftime("%d %b %Y") if sample.sample_receipt_date else "date not recorded"
            chrome.rectangle(slide, .8, y, 11.6, .04, red)
            chrome.add_text(slide, sample.chemical_name, .8, y + .12, 5.4, .3, 16, navy, True)
            chrome.add_text(slide, f"Received {received}", 6.4, y + .14, 3.0, .25, 12, grey)
            chrome.add_wrapped_text(slide, " ".join(str(sample.delay_reason).split()), .8, y + .48, 11.5, .42, 12, grey)
            y += 1.06
        if len(attention) > 8:
            chrome.add_text(slide, f"+ {len(attention) - 8} further open samples with recorded remarks", .8, y + .05, 8.0, .3, 12, grey)
    else:
        chrome.add_text(slide, "No open sample has a recorded delay remark in this reporting period.", .8, 2.0, 10.5, .35, 18, green, True)

    # 04 · Issued reports
    issued = [s for s in samples if s.result_status != "under_testing"]
    slide = chrome.new_slide("Issued reports", 4)
    if issued:
        rows = [["Material", "Outcome", "Report date", "Turnaround"]] + [
            [
                s.chemical_name,
                "Report issued" if s.result_status == "report_issued" else s.result_status.title(),
                s.report_issue_date.strftime("%d %b %Y") if s.report_issue_date else "—",
                f"{s.turnaround_days} days" if s.turnaround_days is not None else "—",
            ]
            for s in issued[:14]
        ]
        table = slide.shapes.add_table(
            len(rows), 4, Inches(.8), Inches(1.7), Inches(11.6), Inches(.34 * len(rows)),
        ).table
        for width, index in zip((5.0, 2.4, 2.2, 2.0), range(4)):
            table.columns[index].width = Inches(width)
        for r, row in enumerate(rows):
            for c, value in enumerate(row):
                cell = table.cell(r, c)
                cell.text = str(value)
                cell.fill.solid()
                cell.fill.fore_color.rgb = chrome.color(navy if r == 0 else ("FFFFFF" if r % 2 else "F2F4F7"))
                paragraph = cell.text_frame.paragraphs[0]
                paragraph.font.size = chrome._Pt(12 if r else 11)
                paragraph.font.bold = r == 0
                paragraph.font.name = "Arial"
                paragraph.font.color.rgb = chrome.color("FFFFFF" if r == 0 else navy)
        if len(issued) > 14:
            chrome.add_text(slide, f"+ {len(issued) - 14} further issued reports in the full register", .8, 1.75 + .34 * len(rows), 8.0, .3, 12, grey)
    else:
        chrome.add_text(slide, "No reports were issued in this reporting period.", .8, 2.0, 10.5, .35, 18, grey, True)

    output = BytesIO()
    prs.save(output)
    output.seek(0)
    return output, f"{laboratory['name']} Management Brief {batch.week_end:%d %b %Y}.pptx"


def build_portfolio_management_presentation(static_folder: str, reporting_week_end=None, lab_codes: set[str] | None = None) -> tuple[BytesIO, str]:
    """Create an on-demand, consolidated management-review presentation.

    ``lab_codes`` builds it for the chosen laboratories instead of the whole
    network. Every figure then counts only those laboratories, and the deck says
    so on its cover, on every slide's source line, and in its filename.
    """
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.enum.shapes import MSO_SHAPE
    from pptx.util import Inches, Pt
    from app.core.services.quality_control import portfolio_management_data

    data = portfolio_management_data(reporting_week_end, lab_codes)
    if not data["reporting_labs"]:
        raise ValueError(
            "None of the selected laboratories submitted a workbook for this reporting period."
            if lab_codes is not None else
            "Import a local status workbook for at least one laboratory before downloading a presentation."
        )
    names = [review["laboratory"]["name"] for review in data["laboratory_reviews"]]
    if lab_codes is None:
        scope_heading, scope_note, scope_stem = "ALL ONGC", "the whole laboratory network", "QC Portfolio"
    elif len(names) == 1:
        scope_heading = scope_note = scope_stem = names[0]
    else:
        listed = ", ".join(names[:4]) + (f" and {len(names) - 4} more" if len(names) > 4 else "")
        scope_heading, scope_stem = f"{len(names)} LABORATORIES", f"{len(names)} laboratories"
        scope_note = f"{len(names)} laboratories — {listed}"

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
    corporate_chemistry_logo = Path(static_folder) / "images" / "ongc-corporate-chemistry-logo.png"
    ongc_logo = Path(static_folder) / "images" / "ongc-official-logo.png"
    period = data["reporting_period"]
    period_label = f"{period['start']:%d %b} – {period['end']:%d %b %Y}" if period else "Latest reporting period"
    def background(slide):
        slide.background.fill.solid(); slide.background.fill.fore_color.rgb = color("FFFFFF")
        rectangle(slide, 0, 0, 13.333, .08, navy); rectangle(slide, .42, 1.26, 12.45, .015, border); rectangle(slide, .42, 6.93, 12.45, .015, border)
    def header(slide, title, page):
        background(slide); add_text(slide, "ONGC CORPORATE CHEMISTRY · QC LABORATORY MONITORING", .42, .27, 7.6, .24, 11, blue, True); add_text(slide, title, .42, .7, 11.35, .46, 27, navy, True)
        if ongc_logo.exists(): slide.shapes.add_picture(str(ongc_logo), Inches(11.15), Inches(.21), width=Inches(.9), height=Inches(.51))
        if corporate_chemistry_logo.exists(): slide.shapes.add_picture(str(corporate_chemistry_logo), Inches(12.24), Inches(.16), width=Inches(.55), height=Inches(.55))
        add_text(slide, f"Source: Selected reporting period · {period_label} · Scope: {scope_note}", .42, 7.08, 9.6, .16, 8, grey); add_text(slide, f"{page:02d}", 12.5, 7.08, .25, .16, 8, grey)
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
    add_text(slide, f"QC LABORATORY MONITORING · {scope_heading}", .76, 1.28, 8.5, .28, 14, blue, True); add_text(slide, "Portfolio Management Review", .76, 1.82, 10.5, .7, 50, navy, True); rectangle(slide, .76, 2.75, 1.15, .045, blue)
    add_text(slide, period_label, .76, 3.08, 8.5, .36, 24, navy, True); add_text(slide, f"Consolidated position across {data['reporting_labs']} reporting laborator{'y' if data['reporting_labs'] == 1 else 'ies'} — {scope_note}", .76, 3.68, 10.5, .36, 16, grey, wrap=True)
    rectangle(slide, 10.0, .46, 2.32, 1.2, "FFFFFF", border)
    if ongc_logo.exists(): slide.shapes.add_picture(str(ongc_logo), Inches(10.16), Inches(.68), width=Inches(.9), height=Inches(.51))
    if corporate_chemistry_logo.exists(): slide.shapes.add_picture(str(corporate_chemistry_logo), Inches(11.16), Inches(.53), width=Inches(1.05), height=Inches(1.05))
    add_text(slide, "Office of Head Corporate Chemistry | Mumbai / Dehradun", .76, 6.45, 7.2, .2, 11, grey)

    summary = data["summary"]
    slide = prs.slides.add_slide(blank); header(slide, "Portfolio delivery position", 2)
    for index, item in enumerate([(summary["total"], "Samples in review", blue), (summary["under_testing"], "Open workload", navy), (summary["delayed_open"], "Aged open > 9 days", red), (f"{summary['passed']} / {summary['failed']}", "Pass / fail reports", green), (f"{summary['average_turnaround']} d" if summary["average_turnaround"] is not None else "—", "Average turnaround", blue), (data["reporting_labs"], "Laboratories submitted", navy)]):
        metric(slide, .6 + (index % 3) * 4.2, 1.7 + (index // 3) * 2.0, item[0], item[1], item[2])
    if data["missing_submissions"]:
        missing_names = ", ".join(review["laboratory"]["name"] for review in data["missing_submissions"])
        add_text(slide, f"Excluded from this reporting period (no submission): {missing_names}", .8, 5.0, 11.2, .3, 13, red, True)
    else:
        add_text(slide, "All configured laboratories submitted data for the selected reporting period.", .8, 5.0, 11.2, .3, 13, green, True)

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
    paginated_table("Completed tests above STT", ["Laboratory", "Chemical", "Notification", "Actual", "Standard", "Variance", "Delay reason"], late_table_rows, [1.75, 2.15, 1.2, .75, .85, .85, 4.9])

    open_rows = [[data["laboratories_by_code"].get(sample.lab_code, {"name": sample.lab_code})["name"], sample.chemical_name, sample.notification_no or sample.po_number or "—", sample.sample_receipt_date.strftime("%d %b %Y") if sample.sample_receipt_date else "—", f"{sample.days_open} d", concise(sample.delay_reason)] for sample in data["overdue_samples"]]
    paginated_table("Open samples above 9-day threshold", ["Laboratory", "Material", "Notification", "Received", "Age", "Remarks"], open_rows, [1.85, 2.4, 1.35, 1.15, .7, 4.85])

    chemical_rows = [[item["chemical_name"], ", ".join(item["laboratories"]), item["total"], item["passed"], item["failed"], item["under_testing"], f"{item['average_actual']} d" if item["average_actual"] is not None else "—", f"{item['standard_days']} d" if item["standard_days"] is not None else "—"] for item in data["weekly_chemical_metrics"]]
    paginated_table("Reporting-period chemical performance", ["Chemical", "Laboratories", "Samples", "Pass", "Fail", "Open", "Average time", "Standard"], chemical_rows, [2.35, 3.35, .75, .65, .65, .65, 1.15, 1.15])

    output = BytesIO(); prs.save(output); output.seek(0)
    return output, f"{scope_stem} Management Review {period['end']:%b %Y}.pptx"


def build_sap_portfolio_management_presentation(
    static_folder: str, lab_codes: set[str] | None = None,
    notification_date_from: date | None = None,
) -> tuple[BytesIO, str]:
    """Create the senior-management deck from the current SAP snapshots only."""
    from pptx import Presentation
    from pptx.util import Inches, Pt
    from app.core.services.sap_quality_control import (
        SAP_MONITORING_START_DATE, non_sap_register_data, sap_management_data,
        sap_turnaround_days,
    )

    data = sap_management_data(lab_codes, notification_date_from)
    # The declared non-SAP register is read separately and stays separate: it
    # never enters the SAP counts, but it is the rest of the bench's load and
    # the management deck is now the only deck that carries it.
    non_sap = non_sap_register_data(lab_codes)
    if not data["reporting_labs"]:
        raise ValueError("Import paired SAP exports for at least one laboratory before downloading the management presentation.")

    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
    scope_labs = data["scope_laboratories"]
    scope_label = "All SAP laboratories" if lab_codes is None else ", ".join(lab["name"] for lab in scope_labs)
    position_label = (
        f"SAP position as on {data['source_dates'][0]:%d %B %Y}"
        if len(data["source_dates"]) == 1
        else "SAP position as on each laboratory's latest snapshot"
    )
    chrome = _WeeklyReviewChrome(
        prs, static_folder,
        position_label,
    )
    navy, blue, red, green, grey = chrome.NAVY, chrome.BLUE, chrome.RED, chrome.GREEN, chrome.GREY

    def concise(value, limit=36):
        text = " ".join(str(value or "—").split())
        return text if len(text) <= limit else f"{text[:limit].rsplit(' ', 1)[0]}…"

    def table(slide, headers, rows, widths, *, y=1.5, font_size=9, height=None):
        if not rows:
            chrome.add_text(slide, "No SAP records are available for this view.", .8, 2.1, 10.5, .35, 18, green, True)
            return
        height = height or min(5.15, .34 * (len(rows) + 1))
        shape = slide.shapes.add_table(len(rows) + 1, len(headers), Inches(.42), Inches(y), Inches(12.45), Inches(height))
        table_shape = shape.table
        for index, width in enumerate(widths):
            table_shape.columns[index].width = Inches(width)
        for column, label in enumerate(headers):
            cell = table_shape.cell(0, column)
            cell.text = label
            cell.fill.solid()
            cell.fill.fore_color.rgb = chrome.color(navy)
        for row_index, values in enumerate(rows, 1):
            for column, value in enumerate(values):
                cell = table_shape.cell(row_index, column)
                cell.text = str(value)
                cell.fill.solid()
                cell.fill.fore_color.rgb = chrome.color("F8FBFE" if row_index % 2 == 0 else "FFFFFF")
        for row in table_shape.rows:
            for cell in row.cells:
                cell.text_frame.word_wrap = True
                for paragraph in cell.text_frame.paragraphs:
                    paragraph.font.size = Pt(font_size)
                    paragraph.font.name = "Arial"
                    paragraph.font.color.rgb = chrome.color(navy)
        for cell in table_shape.rows[0].cells:
            for paragraph in cell.text_frame.paragraphs:
                paragraph.font.bold = True
                paragraph.font.color.rgb = chrome.color("FFFFFF")

    # 01 · Cover, following the supplied Weekly QC Review model.
    cover_date = (
        data["source_dates"][0].strftime("%d %B %Y")
        if len(data["source_dates"]) == 1 else "latest paired SAP snapshots"
    )
    chrome.cover(scope_label, cover_date, title="QC SAP Management Review")

    # 02 · Overall executive summary for the selected notification window.
    effective_date_from = max(
        SAP_MONITORING_START_DATE,
        notification_date_from or SAP_MONITORING_START_DATE,
    )
    def summary_bar_chart(slide, title, rows, x, y, width, color, *, label_width=1.65, max_rows=5):
        chrome.add_text(slide, title, x, y, width, .24, 13, navy, True)
        chart_rows = rows[:max_rows]
        max_value = max((value for _, value in chart_rows), default=0)
        for index, (label, value) in enumerate(chart_rows):
            row_y = y + .4 + index * .43
            chrome.add_text(slide, concise(label, 38), x, row_y, label_width, .2, 8, grey)
            bar_x = x + label_width + .08
            bar_max_width = max(.25, width - label_width - .85)
            bar_width = bar_max_width * value / max_value if max_value else 0
            if bar_width:
                chrome.rectangle(slide, bar_x, row_y + .015, bar_width, .17, color)
            chrome.add_text(slide, str(value), x + width - .58, row_y - .01, .52, .2, 8, navy, True)
    def next_page_number():
        return len(prs.slides) + 1

    def scoped_entries(review):
        return [
            entry for entry in review["records"]
            if entry["record"].notification_no
            and entry["record"].notification_start_date
            and entry["record"].notification_start_date >= effective_date_from
        ]

    def summarize_entries(entries):
        status_counts = {"Accepted": 0, "Rejected": 0, "Under Testing": 0}
        groups = {}
        completion_times = []
        for entry in entries:
            record = entry["record"]
            usage = (record.usage_decision_code or "").strip().upper()
            status = {"A": "Accepted", "R": "Rejected"}.get(usage, "Under Testing")
            status_counts[status] += 1
            label = entry["subgroup_label"] or "Not grouped"
            group = groups.setdefault(label, {"total": 0, **{key: 0 for key in status_counts}})
            group["total"] += 1
            group[status] += 1
            elapsed_days = sap_turnaround_days(record)
            if record.completion_date and elapsed_days is not None:
                completion_times.append({
                    "days": elapsed_days,
                    "notification": record.notification_no,
                    "material": entry["specification_chemical_name"] or record.material_description or "Material not stated in SAP",
                    "notification_date": record.notification_start_date,
                    "completion_date": record.completion_date,
                    "laboratory": entry["laboratory"]["name"],
                })
        return status_counts, groups, completion_times

    def add_executive_summary(entries, page, title, snapshot_note):
        status_counts, groups, completion_times = summarize_entries(entries)
        slide = chrome.new_slide(title, page)
        chrome.add_text(
            slide,
            f"Notifications created on or after {effective_date_from:%d %b %Y} · {len(entries):,} total",
            .62, 1.43, 11.8, .22, 12, grey, True,
        )
        chrome.add_text(slide, snapshot_note, .62, 1.66, 11.8, .22, 10, grey)
        cards = [
            (len(entries), "Total samples", navy),
            (status_counts["Accepted"], "Accepted", green),
            (status_counts["Rejected"], "Rejected", red),
            (status_counts["Under Testing"], "Under Testing", blue),
        ]
        for index, (value, label, tone) in enumerate(cards):
            x = .55 + index * 3.18
            fill = "F2F4F7" if tone == navy else "EAF7F0" if tone == green else "FCEBEC" if tone == red else "EAF4FF"
            chrome.rectangle(slide, x, 1.96, 2.92, .78, fill, chrome.BORDER)
            chrome.add_text(slide, f"{value:,}", x + .16, 2.06, 2.55, .3, 23, tone, True)
            chrome.add_text(slide, label, x + .16, 2.41, 2.55, .2, 10, navy, True)

        chrome.add_text(slide, "Status mix", .82, 3.0, 3.1, .24, 13, navy, True)
        bar_x, bar_width = .82, 11.55
        chrome.rectangle(slide, bar_x, 3.38, bar_width, .22, "E8EDF2")
        for label, tone in (("Accepted", green), ("Rejected", red), ("Under Testing", blue)):
            width = bar_width * status_counts[label] / len(entries) if entries else 0
            if width:
                chrome.rectangle(slide, bar_x, 3.38, width, .22, tone)
                bar_x += width
        for index, (label, tone) in enumerate((("Accepted", green), ("Rejected", red), ("Under Testing", blue))):
            x = .82 + index * 3.82
            chrome.rectangle(slide, x, 3.76, .13, .13, tone)
            chrome.add_text(slide, f"{label} {status_counts[label]:,}", x + .22, 3.72, 3.3, .22, 10, navy, True)

        chrome.add_text(slide, "Management attention", .82, 4.27, 4.0, .25, 13, navy, True)
        attention = [
            (sum(bool(entry["is_actionable"]) for entry in entries), "Actionable SAP-open"),
            (sum(bool(entry["stt_overdue"]) for entry in entries), "Past STT"),
            (sum(bool(entry["is_actionable"] and entry["reconciliation_key"] == "awaiting_lab") for entry in entries), "Awaiting lab follow-up"),
        ]
        for index, (value, label) in enumerate(attention):
            x = .82 + index * 4.0
            chrome.add_text(slide, f"{value:,}", x, 4.61, 3.5, .36, 23, red if index == 1 and value else navy, True)
            chrome.add_text(slide, label, x, 5.02, 3.5, .22, 10, grey)

        longest = max(completion_times, key=lambda item: item["days"], default=None)
        if longest:
            chrome.add_text(slide, "Longest completion", .82, 5.47, 3.1, .25, 13, navy, True)
            chrome.add_wrapped_text(
                slide,
                f"{longest['days']} days · Notification {longest['notification']} · {concise(longest['material'], 70)}",
                .82, 5.85, 11.4, .44, 14, red, True,
            )
            average = sum(item["days"] for item in completion_times) / len(completion_times)
            chrome.add_text(
                slide,
                f"{len(completion_times):,} completed notifications with both dates · Average {average:.1f} days",
                .82, 6.36, 11.4, .24, 10, grey,
            )
        else:
            has_completion = any(
                entry["record"].completion_date or entry["record"].official_status == "completed"
                for entry in entries
            )
            message = (
                "Completion duration unavailable for the completed notifications in this scope."
                if has_completion else "No notification completions are recorded in this scope."
            )
            chrome.add_text(slide, message, .82, 5.55, 11.4, .3, 12, grey)
        completed_without_ud = sum(
            entry["record"].official_status == "completed"
            and (entry["record"].usage_decision_code or "").strip().upper() not in {"A", "R"}
            for entry in entries
        )
        if completed_without_ud:
            chrome.add_text(
                slide,
                f"{completed_without_ud} SAP-complete notifications have no UD; included under Under Testing.",
                .82, 6.68, 11.4, .19, 9, grey,
            )
        return groups, completion_times

    def add_group_status(groups, page, title):
        slide = chrome.new_slide(title, page)
        chrome.add_text(slide, "Notification counts by Corporate Specification group and usage decision.", .62, 1.48, 11.0, .22, 11, grey)
        rows = [
            [label, values["total"], values["Accepted"], values["Rejected"], values["Under Testing"]]
            for label, values in sorted(groups.items(), key=lambda pair: (-pair[1]["total"], pair[0].casefold()))
        ]
        if not rows:
            chrome.add_text(slide, "No notifications match the selected date.", .8, 2.1, 10.5, .35, 18, green, True)
            return
        shape = slide.shapes.add_table(
            len(rows) + 1, 5, Inches(.55), Inches(1.95), Inches(6.25), Inches(min(4.85, .4 * (len(rows) + 1))),
        )
        group_table = shape.table
        for index, width in enumerate([2.45, .78, .93, .9, 1.19]):
            group_table.columns[index].width = Inches(width)
        for row_index, values in enumerate([["Sample group", "Total", "Accepted", "Rejected", "Testing"], *rows]):
            for column, value in enumerate(values):
                cell = group_table.cell(row_index, column)
                cell.text = str(value)
                cell.fill.solid()
                cell.fill.fore_color.rgb = chrome.color(navy if row_index == 0 else ("F8FBFE" if row_index % 2 == 0 else "FFFFFF"))
                cell.text_frame.word_wrap = True
                for paragraph in cell.text_frame.paragraphs:
                    paragraph.font.name = "Arial"
                    paragraph.font.size = Pt(8)
                    paragraph.font.color.rgb = chrome.color("FFFFFF" if row_index == 0 else navy)
                    paragraph.font.bold = row_index == 0

        chrome.add_text(slide, "Samples by group · status mix", 7.15, 1.95, 5.5, .24, 13, navy, True)
        for index, (label, tone) in enumerate((("Accepted", green), ("Rejected", red), ("Under Testing", blue))):
            x = 7.18 + index * 1.77
            chrome.rectangle(slide, x, 2.29, .13, .13, tone)
            chrome.add_text(slide, label, x + .19, 2.26, 1.52, .2, 8, grey)
        ordered = sorted(groups.items(), key=lambda pair: (-pair[1]["total"], pair[0].casefold()))
        max_total = max((values["total"] for _, values in ordered), default=0)
        step = min(.43, 3.85 / max(1, len(ordered)))
        for index, (label, values) in enumerate(ordered):
            y = 2.68 + index * step
            chrome.add_text(slide, concise(label, 23), 7.15, y, 1.78, .2, 8, grey)
            bar_x = 9.0
            for status, tone in (("Accepted", green), ("Rejected", red), ("Under Testing", blue)):
                width = 1.62 * values[status] / max_total if max_total else 0
                if width:
                    chrome.rectangle(slide, bar_x, y + .015, width, .17, tone)
                    bar_x += width
            for offset, (short_label, status, tone) in enumerate((
                ("A", "Accepted", green), ("R", "Rejected", red), ("T", "Under Testing", blue),
            )):
                chrome.add_text(slide, f"{short_label} {values[status]}", 10.9 + offset * .56, y, .55, .2, 8, tone, True)

    def add_completion_analysis(completion_times, page, title):
        slide = chrome.new_slide(title, page)
        chrome.add_text(slide, "Calendar days from notification creation to notification completion.", .62, 1.48, 11.0, .22, 11, grey)
        if not completion_times:
            chrome.add_text(slide, "No completed notifications have both dates in this scope.", .8, 2.1, 10.5, .35, 18, green, True)
            return
        average = sum(item["days"] for item in completion_times) / len(completion_times)
        longest = max(completion_times, key=lambda item: item["days"])
        chrome.metric(slide, .9, 2.02, f"{len(completion_times):,}", "Completed with both dates", blue)
        chrome.metric(slide, 4.65, 2.02, f"{average:.1f}", "Average days to complete", green)
        chrome.metric(slide, 8.4, 2.02, f"{longest['days']:,}", "Longest completion · days", red)
        longest_rows = sorted(completion_times, key=lambda item: (-item["days"], item["notification"]))[:7]
        chart_rows = [(f"{item['notification']} · {item['material']}", item["days"]) for item in longest_rows]
        summary_bar_chart(slide, "Longest notification completion times", chart_rows, .75, 3.83, 11.8, red, label_width=4.2, max_rows=7)

    overall_entries = [entry for review in data["laboratory_reviews"] for entry in scoped_entries(review)]
    multiple_labs = lab_codes is None or len(scope_labs) > 1
    summary_title = "Overall executive summary" if multiple_labs else f"Executive summary · {scope_labs[0]['name']}"
    snapshot_note = (
        f"SAP snapshots available for {data['reporting_labs']} of {data['configured_labs']} laboratories · "
        f"{sum(bool(scoped_entries(review)) for review in data['laboratory_reviews'])} with notifications in this period"
        if multiple_labs else f"Laboratory snapshot as on {data['source_dates'][0]:%d %b %Y}"
    )
    add_executive_summary(overall_entries, next_page_number(), summary_title, snapshot_note)

    # Keep each laboratory's title, executive summary, analytics and SAP
    # notification register together in the combined presentation.
    for review in data["laboratory_reviews"]:
        laboratory = review["laboratory"]
        batch = review["batch"]
        if batch is None:
            continue
        lab_entries = scoped_entries(review)
        if multiple_labs:
            chrome.cover(
                laboratory["name"], batch.as_of_date.strftime("%d %B %Y"),
                title="QC SAP Management Review",
            )
            groups, lab_completion_times = add_executive_summary(
                lab_entries, next_page_number(), f"Executive summary · {laboratory['name']}",
                f"Laboratory snapshot as on {batch.as_of_date:%d %b %Y}",
            )
        else:
            _, groups, lab_completion_times = summarize_entries(lab_entries)
        if lab_entries:
            add_group_status(groups, next_page_number(), f"{laboratory['name']} · Sample group status")
        if lab_completion_times:
            add_completion_analysis(
                lab_completion_times, next_page_number(), f"{laboratory['name']} · Notification completion time",
            )

        lab_data = {
            "scope_laboratories": [laboratory],
            "laboratory_reviews": [{**review, "records": lab_entries}],
        }
        for group in _sap_presentation_action_groups(lab_data):
            total = len(group["entries"])
            for start, entries in enumerate(_paginated_rows(group["entries"], 10)):
                first = start * 10 + 1
                last = first + len(entries) - 1
                suffix = f" ({first}–{last} of {total})" if total > 10 else ""
                slide = chrome.new_slide(
                    f"{laboratory['name']} · {group['subgroup_label']}{suffix}",
                    next_page_number(),
                )
                chrome.add_text(
                    slide,
                    "Latest notification first · Actual: notification to completion · STT: SAP receipt to due date (notification if receipt missing)",
                    .45, 1.37, 11.8, .17, 8, grey,
                )
                rows = []
                for item in entries:
                    record = item["record"]
                    if item["stt_due_date"]:
                        stt_due = item["stt_due_date"].strftime("%d %b %Y")
                        if item["stt_overdue"]:
                            stt_due += f" · {item['stt_variance_days']} d over"
                    elif item["stt_days"] is not None:
                        stt_due = f"STT {item['stt_days']} d · no start"
                    else:
                        stt_due = "STT not defined"
                    usage_decision = (record.usage_decision_code or "").strip().upper()
                    ud_label = {"A": "Accepted", "R": "Rejected"}.get(usage_decision, "Under Testing")
                    if usage_decision in {"A", "R"} or record.official_status == "completed":
                        completion_date = record.completion_date
                        elapsed_days = sap_turnaround_days(record)
                        if completion_date:
                            follow_up = f"Completed {completion_date:%d %b %Y}"
                            if usage_decision not in {"A", "R"}:
                                follow_up += " · no UD"
                        else:
                            follow_up = "SAP complete · date missing"
                        actual_days = f"Actual {elapsed_days} d" if elapsed_days is not None else "Actual unavailable"
                        stt_days = f"STT {item['stt_days']} d" if item["stt_days"] is not None else "STT not defined"
                        follow_up += f"\n{actual_days} · {stt_days}"
                    else:
                        follow_up = "Lab follow-up requested"
                    rows.append([
                        record.inspection_lot_number or "—", record.notification_no or "—",
                        record.notification_start_date.strftime("%d %b %Y") if record.notification_start_date else "—",
                        item["specification_chemical_name"] or record.material_description or "Material not stated in SAP",
                        concise(item["specification_no"] or "Not in Corporate Specification", 28),
                        stt_due, ud_label, follow_up,
                    ])
                table(
                    slide,
                    ["Inspection lot", "Notification", "Notification date", "Material", "Specification", "STT due", "UD", "Lab follow-up"],
                    rows, [1.05, 1.05, 1.15, 2.75, 1.45, 1.25, 1.1, 2.65],
                    y=1.55, font_size=8, height=5.15 * (len(rows) + 1) / 11,
                )

        lab_non_sap = [item for item in non_sap["non_sap_entries"] if item["laboratory"]["code"] == laboratory["code"]]
        if lab_non_sap:
            pending = [item for item in lab_non_sap if not item["is_closed"]]
            declared_pass = sum(item["sample"].current_status == "closed_pass" for item in lab_non_sap)
            declared_fail = sum(item["sample"].current_status == "closed_fail" for item in lab_non_sap)
            slide = chrome.new_slide(f"{laboratory['name']} · Non-SAP sample summary", next_page_number())
            cards = [
                (len(lab_non_sap), "Declared non-SAP samples", blue),
                (len(pending), "Still with the laboratory", red if pending else green),
                (sum(item["is_overdue"] for item in lab_non_sap), "Past the declared ETA", red if any(item["is_overdue"] for item in lab_non_sap) else green),
            ]
            for index, (value, label, tone) in enumerate(cards):
                chrome.metric(slide, .8 + index * 4.2, 1.62, value, label, tone)
            chrome.add_text(slide, f"Declared results: {declared_pass} pass · {declared_fail} fail. These samples are separate from SAP totals.", .75, 3.35, 11.6, .28, 13, navy, True)
            if pending:
                pending = sorted(pending, key=lambda item: (not item["is_overdue"], item["sample"].expected_completion_date or date.max))
                for page_index, entries in enumerate(_paginated_rows(pending, 10)):
                    first = page_index * 10 + 1
                    last = first + len(entries) - 1
                    suffix = f" ({first}–{last} of {len(pending)})" if len(pending) > 10 else ""
                    detail_slide = chrome.new_slide(
                        f"{laboratory['name']} · Non-SAP samples awaiting return{suffix}",
                        next_page_number(),
                    )
                    rows = []
                    for item in entries:
                        sample = item["sample"]
                        eta = sample.expected_completion_date.strftime("%d %b %Y") if sample.expected_completion_date else "ETA not declared"
                        if item["is_overdue"]:
                            eta += " · overdue"
                        rows.append([
                            concise(sample.sample_reference, 18), concise(sample.chemical_name, 34),
                            concise(item["status_label"], 26), eta,
                            concise(sample.action_owner or sample.delay_reason or "—", 32),
                        ])
                    table(detail_slide, ["Local reference", "Material / sample", "Declared stage", "Expected completion", "Owner / constraint"], rows, [2.0, 3.0, 2.2, 2.1, 3.15], y=1.55, font_size=9)

        # Leave an editable page for laboratory-declared work that is absent
        # from SAP, even when no such samples are recorded in the application.
        input_slide = chrome.new_slide(
            f"{laboratory['name']} · Out-of-SAP sample details", next_page_number(),
            source_line="Laboratory-declared Out-of-SAP input · separate from SAP snapshot",
        )
        chrome.add_text(
            input_slide,
            "Laboratory input · complete one row per sample without an SAP notification",
            .55, 1.46, 11.8, .24, 12, grey,
        )
        chrome.add_text(
            input_slide,
            "Reporting date: ____________________     Prepared by: ______________________________",
            .55, 1.83, 11.8, .25, 11, navy,
        )
        table(
            input_slide,
            ["Local reference", "Material / sample", "Received", "STT (days)",
             "Declared status / result", "Expected / completed", "Lab follow-up / owner"],
            [[""] * 7 for _ in range(8)],
            [1.45, 2.65, 1.15, 1.0, 2.05, 1.65, 2.5],
            y=2.23, height=4.12, font_size=9,
        )
        input_table = next(shape.table for shape in input_slide.shapes if shape.has_table)
        for row_index, row in enumerate(list(input_table.rows)[1:], 1):
            for cell in row.cells:
                cell.fill.fore_color.rgb = chrome.color(
                    "EAF1F7" if row_index % 2 else "F7FAFD"
                )
        chrome.add_text(
            input_slide,
            "Laboratory-declared information only · Keep these samples separate from SAP totals and SAP usage decisions.",
            .55, 6.58, 11.8, .22, 10, grey,
        )

    chrome.closing(next_page_number())

    output = BytesIO()
    prs.save(output)
    output.seek(0)
    filename_date = data["source_dates"][0].strftime("%d %b %Y") if len(data["source_dates"]) == 1 else "Latest"
    if lab_codes is None:
        filename_scope = "QC SAP"
    elif len(scope_labs) == 1:
        filename_scope = scope_labs[0]["name"]
    else:
        filename_scope = f"{len(scope_labs)} SAP Laboratories"
    return output, f"{filename_scope} Management Review {filename_date}.pptx"


def build_sap_portfolio_management_zip(
    static_folder: str, notification_date_from: date | None = None,
) -> tuple[BytesIO, str]:
    """Package one SAP management presentation per reporting laboratory."""
    from app.core.services.sap_quality_control import sap_management_data

    data = sap_management_data()
    lab_codes = [
        review["laboratory"]["code"]
        for review in data["laboratory_reviews"]
        if review["batch"] is not None
    ]
    if not lab_codes:
        raise ValueError("Import paired SAP exports for at least one laboratory before downloading a presentation.")

    output = BytesIO()
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for lab_code in lab_codes:
            deck, filename = build_sap_portfolio_management_presentation(
                static_folder, {lab_code}, notification_date_from,
            )
            archive.writestr(Path(filename).name, deck.getvalue())
    output.seek(0)
    filename_date = (
        f"from-{notification_date_from:%Y-%m-%d}"
        if notification_date_from else "latest"
    )
    return output, f"QC Management Reviews by Laboratory {filename_date}.zip"
