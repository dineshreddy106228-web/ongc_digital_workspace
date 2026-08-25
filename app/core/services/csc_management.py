"""Management review and analytics for Corporate Specifications Management.

Both pages read the same live catalogue the register pages read. The review
answers "where does the specification estate stand today"; the analytics answers
"how has it moved, and where is the depth thin". Nothing here writes.
"""

from __future__ import annotations

from collections import Counter
from io import BytesIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from app.core.services.corporate_specifications import (
    PARAMETER_TYPES, UNCATEGORISED_KEY, catalogue, category_label, landing_data,
    parameter_rows,
)
from app.core.services.csc_administration import authorized_labs_index, laboratory_options
from app.core.services.csc_utils import SPEC_SUBSET_ORDER
from app.extensions import db
from app.models.csc.impact_analysis import CSCImpactAnalysis
from app.models.csc.issue_flag import CSCIssueFlag
from app.models.csc.spec_version import CSCSpecVersion

# Buckets the board reads specification ageing in. ONGC reviews a corporate
# specification on a five-year cycle, so the last bucket is the overdue one.
AGE_BUCKETS = (
    ("Within a year", 0, 365),
    ("1 – 3 years", 365, 3 * 365),
    ("3 – 5 years", 3 * 365, 5 * 365),
    ("Over 5 years", 5 * 365, None),
)

IMPACT_GRADES = ("HIGH", "MEDIUM", "LOW")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _msds_covered_materials(material_codes: set[str]) -> set[str]:
    """Material codes with at least one MSDS on file, empty if the store is unreachable."""
    from app.core.services.msds_service import MSDSError, get_msds_material_index

    if not material_codes:
        return set()
    try:
        return set(get_msds_material_index(sorted(material_codes)))
    except MSDSError:
        return set()
    except Exception:  # a missing MSDS store must not take the board view down
        return set()


def _share(part: int, whole: int) -> int:
    return round(part / whole * 100) if whole else 0


def _ordered(codes) -> list[str]:
    known = [code for code in SPEC_SUBSET_ORDER if code in codes]
    return known + sorted(code for code in codes if code not in SPEC_SUBSET_ORDER)


def _subgroup_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per chemical sub-group, with everything held against it."""
    labs_by_ref = authorized_labs_index()
    covered = _msds_covered_materials({e["material_code"] for e in entries if e["material_code"]})

    buckets: dict[str, dict[str, Any]] = {}
    for entry in entries:
        bucket = buckets.setdefault(
            entry["category"],
            {
                "code": entry["category"],
                "label": entry["category_label"],
                "is_unspecified": entry["category"] == UNCATEGORISED_KEY,
                "chemicals": 0,
                "specified": 0,
                "awaiting": 0,
                "msds": 0,
                "with_labs": 0,
                "standard_days": [],
            },
        )
        bucket["chemicals"] += 1
        bucket["specified" if entry["has_parameters"] else "awaiting"] += 1
        if entry["material_code"] and entry["material_code"] in covered:
            bucket["msds"] += 1
        if labs_by_ref.get(entry["ref"]):
            bucket["with_labs"] += 1
        if entry["standard_days"] is not None:
            bucket["standard_days"].append(entry["standard_days"])

    rows = [buckets[code] for code in _ordered(buckets)]
    for row in rows:
        days = row.pop("standard_days")
        row["average_days"] = round(sum(days) / len(days), 1) if days else None
        row["coverage"] = _share(row["specified"], row["chemicals"])
        row["msds_share"] = _share(row["msds"], row["chemicals"])
        row["lab_share"] = _share(row["with_labs"], row["chemicals"])
    return rows


def _age_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Specifications in force, bucketed by how long since they were last revised."""
    now = _now()
    counts = {label: 0 for label, _lo, _hi in AGE_BUCKETS}
    counted: set[int] = set()
    for entry in entries:
        record_id = entry["record_id"]
        if not record_id or record_id in counted or not entry["has_parameters"]:
            continue
        counted.add(record_id)
        revised = entry["updated_at"]
        if revised is None:
            counts[AGE_BUCKETS[-1][0]] += 1
            continue
        age = (now - revised.replace(tzinfo=None)).days
        for label, low, high in AGE_BUCKETS:
            if age >= low and (high is None or age < high):
                counts[label] += 1
                break
    total = sum(counts.values())
    return [
        {
            "label": label,
            "count": counts[label],
            "share": _share(counts[label], total),
            "is_overdue": high is None,
        }
        for label, _low, high in AGE_BUCKETS
    ]


def _impact_rows() -> tuple[list[dict[str, Any]], int]:
    """Impact classification of every specification that has been assessed."""
    counts = Counter()
    for analysis in CSCImpactAnalysis.query.all():
        if not (analysis.checklist_state_json or "").strip():
            continue
        counts[(analysis.impact_grade or "LOW").strip().upper()] += 1
    total = sum(counts.values())
    grades = list(IMPACT_GRADES) + sorted(set(counts) - set(IMPACT_GRADES))
    return (
        [
            {"grade": grade, "count": counts[grade], "share": _share(counts[grade], total)}
            for grade in grades
            if counts.get(grade)
        ],
        total,
    )


def _open_issue_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Specifications carrying a flagged issue on their issue register."""
    from app.core.services.corporate_specifications import ISSUE_FLAG_LABELS

    labels = dict(ISSUE_FLAG_LABELS)
    by_record: dict[int, list[str]] = {}
    for flag in CSCIssueFlag.query.filter(CSCIssueFlag.is_present.is_(True)).all():
        by_record.setdefault(flag.draft_id, []).append(
            labels.get(flag.issue_type, flag.issue_type)
        )
    rows = []
    seen: set[int] = set()
    for entry in entries:
        record_id = entry["record_id"]
        if not record_id or record_id in seen or record_id not in by_record:
            continue
        seen.add(record_id)
        rows.append(
            {
                "ref": entry["ref"],
                "chemical_name": entry["chemical_name"],
                "spec_number": entry["spec_number"],
                "category_label": entry["category_label"],
                "issues": sorted(set(by_record[record_id])),
            }
        )
    rows.sort(key=lambda row: (-len(row["issues"]), row["chemical_name"].casefold()))
    return rows


def management_review_data() -> dict[str, Any]:
    """Everything the standing management review of the register reports."""
    entries = catalogue()
    headline = landing_data()
    impact_rows, impact_total = _impact_rows()
    now = _now()

    stale = []
    seen: set[int] = set()
    for entry in entries:
        record_id = entry["record_id"]
        if not record_id or record_id in seen or not entry["has_parameters"]:
            continue
        seen.add(record_id)
        revised = entry["updated_at"]
        age = (now - revised.replace(tzinfo=None)).days if revised else None
        if age is None or age >= 5 * 365:
            stale.append({**_brief(entry), "age_days": age})
    stale.sort(key=lambda row: (-(row["age_days"] or 10**6), row["chemical_name"].casefold()))

    awaiting = [_brief(entry) for entry in entries if not entry["has_parameters"]]
    issue_rows = _open_issue_rows(entries)

    return {
        **headline,
        "subgroups": _subgroup_rows(entries),
        "age_rows": _age_rows(entries),
        "impact_rows": impact_rows,
        "impact_total": impact_total,
        "impact_unassessed": max(headline["specification_total"] - impact_total, 0),
        "issue_rows": issue_rows[:12],
        "issue_total": len(issue_rows),
        "stale_rows": stale[:12],
        "stale_total": len(stale),
        "awaiting_rows": awaiting[:12],
        "awaiting_count": len(awaiting),
        "coverage": _share(headline["specification_total"], headline["chemical_total"]),
        "generated_at": datetime.now(timezone.utc),
    }


def _brief(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "ref": entry["ref"],
        "chemical_name": entry["chemical_name"],
        "spec_number": entry["spec_number"],
        "material_code": entry["material_code"],
        "category_label": entry["category_label"],
        "updated_at": entry["updated_at"],
        "version": entry["version"],
    }


def _revision_activity(years: int = 6) -> list[dict[str, Any]]:
    """Published specification versions per calendar year."""
    cutoff = _now() - timedelta(days=365 * years)
    counts = Counter()
    for created_at, in db.session.query(CSCSpecVersion.created_at).all():
        if created_at is None:
            continue
        stamp = created_at.replace(tzinfo=None)
        if stamp < cutoff:
            continue
        counts[stamp.year] += 1
    if not counts:
        return []
    span = range(min(counts), max(counts) + 1)
    peak = max(counts.values())
    return [
        {"year": year, "count": counts.get(year, 0), "share": _share(counts.get(year, 0), peak)}
        for year in span
    ]


def _parameter_mix(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """The Vital / Essential / Desirable split, overall and per sub-group."""
    overall = Counter()
    by_group: dict[str, Counter] = {}
    depth: list[dict[str, Any]] = []
    seen: set[int] = set()

    for entry in entries:
        record = entry["record"]
        if record is None or record.id in seen or not entry["has_parameters"]:
            continue
        seen.add(record.id)
        rows = parameter_rows(record)
        counts = Counter(
            (row["parameter_type"] or "Unclassified").strip().title() or "Unclassified"
            for row in rows
        )
        overall.update(counts)
        by_group.setdefault(entry["category"], Counter()).update(counts)
        depth.append({**_brief(entry), "parameters": len(rows), "vital": counts.get("Vital", 0)})

    types = list(PARAMETER_TYPES) + sorted(set(overall) - set(PARAMETER_TYPES))
    total = sum(overall.values())
    depth.sort(key=lambda row: (-row["parameters"], row["chemical_name"].casefold()))
    return {
        "types": [
            {"label": name, "count": overall[name], "share": _share(overall[name], total)}
            for name in types
            if overall.get(name)
        ],
        "total": total,
        "by_group": [
            {
                "code": code,
                "label": category_label(None if code == UNCATEGORISED_KEY else code),
                "total": sum(by_group[code].values()),
                "counts": [
                    {
                        "label": name,
                        "count": by_group[code].get(name, 0),
                        "share": _share(by_group[code].get(name, 0), sum(by_group[code].values())),
                    }
                    for name in types
                ],
            }
            for code in _ordered(by_group)
        ],
        "deepest": depth[:10],
        "thinnest": [row for row in reversed(depth)][:10],
        "average": round(total / len(depth), 1) if depth else 0,
    }


def _testing_time_rows(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Standard testing time spread per sub-group, for the chemicals that carry one."""
    buckets: dict[str, list[int]] = {}
    for entry in entries:
        if entry["standard_days"] is None:
            continue
        buckets.setdefault(entry["category"], []).append(entry["standard_days"])
    peak = max((max(days) for days in buckets.values()), default=0)
    return [
        {
            "code": code,
            "label": category_label(None if code == UNCATEGORISED_KEY else code),
            "chemicals": len(buckets[code]),
            "fastest": min(buckets[code]),
            "slowest": max(buckets[code]),
            "average": round(sum(buckets[code]) / len(buckets[code]), 1),
            "share": _share(round(sum(buckets[code]) / len(buckets[code])), peak or 1),
        }
        for code in _ordered(buckets)
    ]


def _laboratory_load(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """How many chemicals each laboratory is authorised to test."""
    labs_by_ref = authorized_labs_index()
    counts = Counter()
    for entry in entries:
        for code in labs_by_ref.get(entry["ref"], []):
            counts[code] += 1
    peak = max(counts.values(), default=0)
    return [
        {
            **option,
            "chemicals": counts.get(option["code"], 0),
            "share": _share(counts.get(option["code"], 0), peak or 1),
        }
        for option in laboratory_options()
    ]


def laboratory_assignment_analytics(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """The complete, two-way view of controlled laboratory authorisations.

    The administration register is deliberately keyed by chemical. Management
    also needs the reverse question answered without having to manually compare
    rows: what can a laboratory test, and where can a chemical be tested? This
    builds both readings from the same current authorisation records.
    """
    options = {option["code"]: option for option in laboratory_options()}
    labs_by_ref = authorized_labs_index()
    laboratory_rows = {
        code: {**option, "assignments": [], "categories": Counter()}
        for code, option in options.items()
    }
    chemical_rows: list[dict[str, Any]] = []
    assignment_total = 0

    for entry in entries:
        codes = sorted(
            {code for code in labs_by_ref.get(entry["ref"], []) if code in options},
            key=lambda code: options[code]["name"].casefold(),
        )
        laboratories = [options[code] for code in codes]
        chemical = {
            "ref": entry["ref"],
            "chemical_name": entry["chemical_name"],
            "spec_number": entry["spec_number"],
            "material_code": entry["material_code"],
            "category": entry["category"],
            "category_label": entry["category_label"],
            "standard_days": entry["standard_days"],
            "laboratories": laboratories,
            "lab_codes": codes,
            "is_authorised": bool(codes),
        }
        chemical_rows.append(chemical)
        for code in codes:
            laboratory_rows[code]["assignments"].append(chemical)
            laboratory_rows[code]["categories"][entry["category"]] += 1
            assignment_total += 1

    labs = []
    for row in laboratory_rows.values():
        assigned = row.pop("assignments")
        category_counts = row.pop("categories")
        row["chemicals"] = assigned
        row["chemical_total"] = len(assigned)
        row["subgroups"] = [
            {
                "code": code,
                "label": category_label(None if code == UNCATEGORISED_KEY else code),
                "chemicals": category_counts[code],
            }
            for code in _ordered(category_counts)
        ]
        labs.append(row)
    labs.sort(key=lambda row: (-row["chemical_total"], row["name"].casefold()))

    assigned_chemicals = sum(1 for row in chemical_rows if row["is_authorised"])
    multi_lab_chemicals = sum(1 for row in chemical_rows if len(row["lab_codes"]) > 1)
    return {
        "laboratories": labs,
        "chemicals": chemical_rows,
        "summary": {
            "chemicals": len(chemical_rows),
            "assigned_chemicals": assigned_chemicals,
            "unassigned_chemicals": len(chemical_rows) - assigned_chemicals,
            "laboratories": len(labs),
            "active_laboratories": sum(1 for row in labs if row["chemical_total"] > 0),
            "assignments": assignment_total,
            "multi_lab_chemicals": multi_lab_chemicals,
        },
    }


def build_authorised_laboratory_list_pdf(
    entries: list[dict[str, Any]] | None = None,
) -> bytes:
    """Build the controlled laboratory list as a branded, printable PDF.

    The PDF is a chemical-to-laboratory directory: it includes every catalogue
    chemical, explicitly showing gaps rather than silently omitting them. The
    on-screen analytics supplies the reverse laboratory-to-chemical view.
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    assignments = laboratory_assignment_analytics(catalogue() if entries is None else entries)
    page_width, page_height = landscape(A4)
    margin = 16 * mm
    content_width = page_width - (2 * margin)
    navy = colors.HexColor("#0F3B63")
    green = colors.HexColor("#1E5734")
    amber = colors.HexColor("#B96A15")
    slate = colors.HexColor("#536271")
    pale = colors.HexColor("#F4F7F9")
    line = colors.HexColor("#CDD7DF")
    styles = {
        "title": ParagraphStyle("CSCAuthTitle", fontName="Helvetica-Bold", fontSize=17, leading=21, textColor=navy),
        "sub": ParagraphStyle("CSCAuthSub", fontName="Helvetica", fontSize=8.5, leading=12, textColor=slate),
        "heading": ParagraphStyle("CSCAuthHeading", fontName="Helvetica-Bold", fontSize=8, leading=10, textColor=colors.white),
        "cell": ParagraphStyle("CSCAuthCell", fontName="Helvetica", fontSize=7.1, leading=9.2, textColor=colors.black),
        "muted": ParagraphStyle("CSCAuthMuted", fontName="Helvetica-Oblique", fontSize=7.1, leading=9.2, textColor=amber),
    }

    def paragraph(value: Any, style: str = "cell", *, markup: bool = False) -> Paragraph:
        text = str(value or "-") if markup else escape(str(value or "-")).replace("\n", "<br/>")
        return Paragraph(text, styles[style])

    output = BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=landscape(A4),
        leftMargin=margin,
        rightMargin=margin,
        topMargin=27 * mm,
        bottomMargin=18 * mm,
        title="Authorised Laboratory List",
        author="ONGC Corporate Chemistry",
    )
    summary = assignments["summary"]
    story = [
        Paragraph("Authorised Laboratory List", styles["title"]),
        Spacer(1, 2 * mm),
        Paragraph(
            "Corporate Specifications Management - controlled chemical-to-laboratory directory. "
            "The live Corporate Specifications register remains the authoritative source.",
            styles["sub"],
        ),
        Spacer(1, 4 * mm),
    ]
    summary_table = Table(
        [[
            paragraph(f"<b>{summary['chemicals']}</b><br/>Catalogue chemicals", markup=True),
            paragraph(f"<b>{summary['assigned_chemicals']}</b><br/>With authorised labs", markup=True),
            paragraph(f"<b>{summary['unassigned_chemicals']}</b><br/>Awaiting authorisation", markup=True),
            paragraph(f"<b>{summary['active_laboratories']}</b><br/>Laboratories assigned", markup=True),
            paragraph(f"<b>{summary['assignments']}</b><br/>Laboratory assignments", markup=True),
        ]],
        colWidths=[content_width / 5] * 5,
    )
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), pale),
        ("BOX", (0, 0), (-1, -1), 0.5, line),
        ("INNERGRID", (0, 0), (-1, -1), 0.35, line),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
    ]))
    story.extend([summary_table, Spacer(1, 5 * mm), Paragraph("Chemical to laboratory authorisation", styles["sub"]), Spacer(1, 2 * mm)])

    rows = [[
        paragraph("Sub-group", "heading"),
        paragraph("Chemical", "heading"),
        paragraph("Specification / material", "heading"),
        paragraph("Authorised laboratories", "heading"),
    ]]
    for chemical in assignments["chemicals"]:
        specification = escape(chemical["spec_number"])
        if chemical["material_code"]:
            specification += f"<br/><font color='#536271'>Material: {escape(chemical['material_code'])}</font>"
        if chemical["laboratories"]:
            laboratories = "<br/>".join(
                f"<b>{escape(lab['name'])}</b> - {escape(lab['location'])}"
                for lab in chemical["laboratories"]
            )
            laboratory_style = "cell"
        else:
            laboratories = "No authorised laboratory set"
            laboratory_style = "muted"
        rows.append([
            paragraph(
                f"<b>{escape(chemical['category'])}</b><br/>{escape(chemical['category_label'])}",
                markup=True,
            ),
            paragraph(chemical["chemical_name"]),
            paragraph(specification, markup=True),
            paragraph(laboratories, laboratory_style, markup=bool(chemical["laboratories"])),
        ])
    table = LongTable(
        rows,
        colWidths=[31 * mm, 71 * mm, 47 * mm, content_width - (149 * mm)],
        repeatRows=1,
        hAlign="LEFT",
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), navy),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.35, line),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, pale]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, 0), 1.1, green),
    ]))
    story.append(table)

    logo = Path(__file__).resolve().parents[2] / "static" / "images" / "ongc-corporate-chemistry-logo.png"

    def header_footer(canvas, doc) -> None:
        canvas.saveState()
        canvas.setStrokeColor(green)
        canvas.setLineWidth(1.2)
        canvas.line(margin, page_height - (19 * mm), page_width - margin, page_height - (19 * mm))
        canvas.setFillColor(navy)
        canvas.setFont("Helvetica-Bold", 8.5)
        canvas.drawString(margin, page_height - (12 * mm), "ONGC CORPORATE CHEMISTRY")
        canvas.setFillColor(slate)
        canvas.setFont("Helvetica", 7.2)
        canvas.drawString(margin, page_height - (16 * mm), "Authorised Laboratory List")
        if logo.is_file():
            canvas.drawImage(
                str(logo), page_width - margin - (15 * mm), page_height - (17 * mm),
                width=13 * mm, height=13 * mm, preserveAspectRatio=True, mask="auto",
            )
        canvas.setStrokeColor(line)
        canvas.setLineWidth(0.5)
        canvas.line(margin, 12 * mm, page_width - margin, 12 * mm)
        canvas.setFillColor(slate)
        canvas.setFont("Helvetica", 6.8)
        canvas.drawString(margin, 7.5 * mm, "Controlled laboratory authorisations - internal management use")
        canvas.drawRightString(page_width - margin, 7.5 * mm, f"Page {doc.page}  |  Generated {datetime.now(timezone.utc).strftime('%d %b %Y')}")
        canvas.restoreState()

    document.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
    return output.getvalue()


def management_analytics_data() -> dict[str, Any]:
    """The analytical read of the same register the review reports on."""
    entries = catalogue()
    headline = landing_data()
    mix = _parameter_mix(entries)
    with_time = sum(1 for entry in entries if entry["standard_days"] is not None)
    labs_by_ref = authorized_labs_index()
    with_labs = sum(1 for entry in entries if labs_by_ref.get(entry["ref"]))
    assignments = laboratory_assignment_analytics(entries)

    return {
        **headline,
        "revision_activity": _revision_activity(),
        "parameter_mix": mix,
        "testing_time_rows": _testing_time_rows(entries),
        "laboratory_load": _laboratory_load(entries),
        "laboratory_assignments": assignments,
        "coverage": _share(headline["specification_total"], headline["chemical_total"]),
        "time_coverage": _share(with_time, headline["chemical_total"]),
        "lab_coverage": _share(with_labs, headline["chemical_total"]),
        "with_time": with_time,
        "with_labs": with_labs,
        "generated_at": datetime.now(timezone.utc),
    }
