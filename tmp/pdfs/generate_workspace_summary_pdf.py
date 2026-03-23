from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas


PAGE_WIDTH, PAGE_HEIGHT = landscape(letter)
MARGIN = 36
GAP = 24
CONTENT_TOP = PAGE_HEIGHT - 88
CONTENT_BOTTOM = 34
COL_WIDTH = (PAGE_WIDTH - (2 * MARGIN) - GAP) / 2

TITLE = "ONGC Digital Workspace"
SUBTITLE = "One-page repo summary"

LEFT_SECTIONS = [
    (
        "What It Is",
        [
            (
                "paragraph",
                "ONGC Digital Workspace is a modular Flask web application for ONGC offices. "
                "It combines task tracking, inventory analytics, material master workflows, "
                "committee coordination, notifications, and admin controls in one authenticated workspace.",
            ),
        ],
    ),
    (
        "Who It's For",
        [
            (
                "paragraph",
                "Primary persona: ONGC office operations staff and module admins who need one controlled "
                "system to manage day-to-day work, data uploads, committee actions, and governed master-data changes.",
            ),
        ],
    ),
    (
        "What It Does",
        [
            (
                "bullet",
                "Role-based dashboard access with login, password change, CSRF, security headers, and audit/activity logging.",
            ),
            (
                "bullet",
                "Office task tracking with recurring tasks, collaborators, scoped visibility, updates, and command dashboards.",
            ),
            (
                "bullet",
                "Inventory Intelligence for materials, procurement/consumption drilldowns, forecasting, seed-file audit, and Excel export.",
            ),
            (
                "bullet",
                "Material Master Management / CSC workflow with PDF or DOCX ingest, draft editing, issue flags, revision review, and DOCX export.",
            ),
            (
                "bullet",
                "Committee task management with office mapping, member assignment, comments, status tracking, and attachments.",
            ),
            (
                "bullet",
                "Notifications, announcements, and polls, plus admin user/office/module management and backup flows.",
            ),
        ],
    ),
]

RIGHT_SECTIONS = [
    (
        "How It Works",
        [
            (
                "bullet",
                "run.py starts a Flask app factory in app/__init__.py; environment-driven config lives in config.py.",
            ),
            (
                "bullet",
                "Core extensions are Flask-SQLAlchemy, Flask-Migrate, Flask-Login, Flask-WTF CSRF, and Flask-Caching.",
            ),
            (
                "bullet",
                "Auth and notifications blueprints register directly; business modules are registered through app/core/module_registry.py using ENABLE_* feature flags.",
            ),
            (
                "bullet",
                "Routes delegate business logic to app/core/services/ for recurring tasks, inventory analytics and seed audit, CSC extract/export, announcements, and backups.",
            ),
            (
                "bullet",
                "Data persists through SQLAlchemy models in app/models/ with Alembic migrations in migrations/; Jinja templates and static JS/CSS render the UI.",
            ),
        ],
    ),
    (
        "How To Run",
        [
            ("bullet", "Use Python 3.9-3.12, create a virtualenv, and activate it."),
            ("bullet", "pip install -r requirements.txt"),
            ("bullet", "cp .env.example .env, then set SECRET_KEY and local MySQL credentials."),
            (
                "bullet",
                "Create the local MySQL database, then run flask db upgrade and flask seed-initial-data.",
            ),
            ("bullet", "Start with python run.py and open http://localhost:5000."),
        ],
    ),
]

FOOTER = (
    "Based on repo evidence from README.md, run.py, config.py, app/__init__.py, "
    "app/core/module_registry.py, app/modules/*/routes.py, and app/cli/__init__.py."
)


def _draw_title(pdf: canvas.Canvas) -> None:
    pdf.setFillColor(colors.HexColor("#0f172a"))
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(MARGIN, PAGE_HEIGHT - 40, TITLE)

    pdf.setFillColor(colors.HexColor("#475569"))
    pdf.setFont("Helvetica", 10)
    pdf.drawString(MARGIN, PAGE_HEIGHT - 56, SUBTITLE)

    line_width = stringWidth(TITLE, "Helvetica-Bold", 22)
    pdf.setStrokeColor(colors.HexColor("#1d4ed8"))
    pdf.setLineWidth(2)
    pdf.line(MARGIN, PAGE_HEIGHT - 64, MARGIN + line_width, PAGE_HEIGHT - 64)


def _draw_section_heading(pdf: canvas.Canvas, x: float, y: float, title: str) -> float:
    band_height = 18
    pdf.setFillColor(colors.HexColor("#e2e8f0"))
    pdf.roundRect(x, y - band_height + 3, COL_WIDTH, band_height, 6, fill=1, stroke=0)
    pdf.setFillColor(colors.HexColor("#0f172a"))
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(x + 8, y - 10, title)
    return y - 24


def _draw_paragraph(pdf: canvas.Canvas, text: str, x: float, y: float) -> float:
    pdf.setFillColor(colors.HexColor("#111827"))
    pdf.setFont("Helvetica", 9)
    lines = simpleSplit(text, "Helvetica", 9, COL_WIDTH)
    for line in lines:
        pdf.drawString(x, y, line)
        y -= 11
    return y - 3


def _draw_bullet(pdf: canvas.Canvas, text: str, x: float, y: float) -> float:
    font_name = "Helvetica"
    font_size = 8.8
    bullet_x = x
    text_x = x + 10
    wrap_width = COL_WIDTH - 10
    lines = simpleSplit(text, font_name, font_size, wrap_width)

    pdf.setFillColor(colors.HexColor("#111827"))
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(bullet_x, y, "-")

    pdf.setFont(font_name, font_size)
    for index, line in enumerate(lines):
        draw_x = text_x if index == 0 else text_x
        pdf.drawString(draw_x, y, line)
        y -= 10
    return y - 2


def _draw_sections(pdf: canvas.Canvas, sections: list[tuple[str, list[tuple[str, str]]]], x: float) -> float:
    y = CONTENT_TOP
    for title, items in sections:
        y = _draw_section_heading(pdf, x, y, title)
        for item_type, text in items:
            if item_type == "paragraph":
                y = _draw_paragraph(pdf, text, x, y)
            else:
                y = _draw_bullet(pdf, text, x, y)
        y -= 4
    return y


def _draw_footer(pdf: canvas.Canvas) -> None:
    pdf.setStrokeColor(colors.HexColor("#cbd5e1"))
    pdf.setLineWidth(1)
    pdf.line(MARGIN, 26, PAGE_WIDTH - MARGIN, 26)
    pdf.setFillColor(colors.HexColor("#64748b"))
    pdf.setFont("Helvetica", 7.5)
    footer_lines = simpleSplit(FOOTER, "Helvetica", 7.5, PAGE_WIDTH - (2 * MARGIN))
    y = 16
    for line in footer_lines:
        pdf.drawString(MARGIN, y, line)
        y -= 8


def build_pdf(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = canvas.Canvas(str(output_path), pagesize=landscape(letter))
    pdf.setTitle("ONGC Digital Workspace Summary")
    pdf.setAuthor("OpenAI Codex")

    pdf.setFillColor(colors.white)
    pdf.rect(0, 0, PAGE_WIDTH, PAGE_HEIGHT, fill=1, stroke=0)

    _draw_title(pdf)
    left_y = _draw_sections(pdf, LEFT_SECTIONS, MARGIN)
    right_y = _draw_sections(pdf, RIGHT_SECTIONS, MARGIN + COL_WIDTH + GAP)
    _draw_footer(pdf)

    lowest_y = min(left_y, right_y)
    if lowest_y < CONTENT_BOTTOM:
        raise RuntimeError(f"Content overflowed the page (lowest y={lowest_y:.1f}).")

    pdf.showPage()
    pdf.save()


if __name__ == "__main__":
    target = Path("output/pdf/ongc_digital_workspace_summary.pdf")
    build_pdf(target)
    print(target)
