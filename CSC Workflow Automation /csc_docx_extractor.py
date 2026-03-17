"""CSC Docx Extractor — parses the master Corporate Specifications .docx.

Extracts all specifications (DFC/01, CCA/01, WCF/01 …) from a single
multi-spec Word document into SpecDocument objects, one per specification.

Entry point:
    extract_all_specs_from_docx(docx_bytes: bytes) -> list[SpecDocument]
    extract_all_specs_from_path(path: str | Path)  -> list[SpecDocument]

Each returned SpecDocument is identical to what the PDF extractor produces,
so it slots directly into the existing admin PDF-import workflow.

Handles all five body formats found in the master document:
  Format A  ListParagraph + ":" value paragraphs   (vast majority of specs)
  Format B  Multi-grade N-column parameter table    (Barytes, Proppants, …)
  Format C  2-column table (name + value)           (HPHT PBDFS, BASE OIL …)
  Format D  Guar-Gum-style Sl.No./Parameter/Grade   (matrix tables)
  Format E  Empty / procurement-note specs          (stored with note text)
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Reuse the shared data-class definitions from the PDF extractor
from csc_pdf_extractor import ExtractedParameter, SpecDocument

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Word XML namespace shortcut
# ---------------------------------------------------------------------------
_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _para_text(el: Any) -> str:
    """Return full plain text of a <w:p> element."""
    return "".join(t.text or "" for t in el.findall(f".//{_W}t"))


def _para_style(el: Any) -> str:
    """Return the paragraph style name (val) or '' if none."""
    s = el.find(f".//{_W}pStyle")
    return (s.get(f"{_W}val", "") if s is not None else "").strip()


def _cell_text(cell: Any) -> str:
    """Return full plain text of a table cell."""
    return cell.text.strip()


def _normalize_spec_number(raw: str) -> str:
    """Normalise 'ONGC / DFC / 01 / 2026' → 'ONGC/DFC/01/2026'."""
    # Take only the first line — revision text often sits on line 2
    first_line = raw.strip().splitlines()[0].strip()
    # Collapse spaces around slashes
    val = re.sub(r"\s*/\s*", "/", first_line)
    # Drop trailing revision notes like '(1st Revision)' or '3RD Revision 2026'
    val = re.sub(r"\s*\(.*?[Rr]evision.*?\)", "", val).strip()
    val = re.sub(r"\s+\d*\s*[Rr][Dd]?\s+[Rr]evision.*$", "", val).strip()
    return val


def _normalize_param_type(raw: str) -> str:
    txt = (raw or "").strip().lower()
    if "desirable" in txt:
        return "Desirable"
    if "vital" in txt:
        return "Desirable"
    if "inform" in txt:
        return "Desirable"
    return "Essential"


# Patterns that mark lines/paragraphs that are NOT parameters:
#   • footnotes starting with * or "Note:"
#   • section-header paragraphs ("SECTION 1 …")
#   • blank lines
_FOOTNOTE_RE = re.compile(r"^\s*[\*\†]|^Note\s*:", re.IGNORECASE)
_SECTION_RE  = re.compile(r"^SECTION\s+\d+", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_all_specs_from_path(path: str | Path) -> list[SpecDocument]:
    """Load a .docx file by path and extract all specifications."""
    data = Path(path).read_bytes()
    return extract_all_specs_from_docx(data)


def extract_all_specs_from_docx(docx_bytes: bytes) -> list[SpecDocument]:
    """Parse a master Corporate Specifications .docx and return one
    SpecDocument per specification found.

    Raises ImportError if python-docx is not installed.
    """
    try:
        from docx import Document
        from docx.table import Table as DocxTable
    except ImportError as exc:
        raise ImportError(
            "python-docx is required. Install it with: pip install python-docx"
        ) from exc

    doc = Document(io.BytesIO(docx_bytes))
    body_elements = list(doc.element.body)

    # ── Locate every spec header table ──────────────────────────────────────
    # A spec header is a table whose first cell reads "CHEMICAL NAME".
    spec_starts: list[int] = []
    for idx, el in enumerate(body_elements):
        if el.tag.split("}")[-1] != "tbl":
            continue
        tbl = DocxTable(el, doc)
        if (
            tbl.rows
            and tbl.rows[0].cells[0].text.strip().upper() == "CHEMICAL NAME"
        ):
            spec_starts.append(idx)

    if not spec_starts:
        return []

    results: list[SpecDocument] = []

    for si, start_idx in enumerate(spec_starts):
        end_idx = spec_starts[si + 1] if si + 1 < len(spec_starts) else len(body_elements)

        header_tbl = DocxTable(body_elements[start_idx], doc)
        spec = _parse_header_table(header_tbl)

        # Elements between this header table and the next spec header
        body_els = body_elements[start_idx + 1 : end_idx]

        _parse_body(spec, body_els, doc)

        if not spec.parameters:
            spec.parse_warnings.append(
                "No parameters extracted — check document structure."
            )

        logger.debug(
            "Docx extracted: %s — %s  (%d params, %d warnings)",
            spec.spec_number,
            spec.chemical_name,
            len(spec.parameters),
            len(spec.parse_warnings),
        )
        results.append(spec)

    logger.info(
        "Docx extraction complete: %d specs extracted from document",
        len(results),
    )
    return results


# ---------------------------------------------------------------------------
# Header table parsing
# ---------------------------------------------------------------------------

def _parse_header_table(tbl: Any) -> SpecDocument:
    """Extract metadata from the 4-row identity table.

    Handles the standard 4-row layout and the 'Note:' procurement-reference
    specs where the Specification No. cell contains a free-text note instead
    of an ONGC/…/… code.
    """
    spec = SpecDocument()
    for row in tbl.rows:
        cells = row.cells
        if len(cells) < 2:
            continue
        key = cells[0].text.strip().upper()
        # Collapse multi-line cell text to a single space-separated string
        val = " ".join(cells[1].text.strip().splitlines()).strip()
        # Collapse excessive whitespace
        val = re.sub(r"[ \t]{2,}", " ", val)
        if "CHEMICAL NAME" in key:
            spec.chemical_name = val
        elif "SPECIFICATION" in key:
            raw = val
            if raw.upper().startswith("NOTE:") or raw.upper().startswith("NOTE "):
                # Store the full note text as a parse warning; spec_number stays blank
                spec.parse_warnings.append(f"Procurement note spec: {raw[:200]}")
                spec.spec_number = raw   # keep full text so bulk_ingest can display it
            else:
                spec.spec_number = _normalize_spec_number(raw)
        elif "TEST PROCEDURE" in key:
            spec.test_procedure = val
        elif "MATERIAL CODE" in key:
            spec.material_code = val
    return spec


# ---------------------------------------------------------------------------
# Body parsing — dispatcher
# ---------------------------------------------------------------------------

def _parse_body(spec: SpecDocument, body_els: list[Any], doc: Any) -> None:
    """Dispatch to the correct parameter parser based on body content."""
    from docx.table import Table as DocxTable

    # Separate immediate elements into paragraphs vs tables
    tables = [
        DocxTable(el, doc)
        for el in body_els
        if el.tag.split("}")[-1] == "tbl"
    ]

    # Paragraphs that carry ListParagraph or related styles
    list_paras = [
        el for el in body_els
        if el.tag.split("}")[-1] == "p"
        and _para_style(el) in ("ListParagraph", "ListParagraph0", "List Paragraph")
    ]

    if list_paras:
        # Format A: paragraph-based (and possibly an appended multi-grade table)
        _parse_format_a(spec, body_els, tables, doc)
    elif tables:
        # Formats B / C / D: table-only body
        _parse_table_body(spec, tables)
    # else: empty spec — no parameters to add


# ---------------------------------------------------------------------------
# Format A — ListParagraph + ":" value paragraph pairs
# ---------------------------------------------------------------------------
# Structure:
#   ListParagraph: "Parameter name …"          ← may include inline ":value"
#   style='':      ":Required Value text"       ← leading colon
#   style='':      "continuation of name"      ← no leading colon → name wrap
#   BodyText:      "…name wrap continuation"
# Multi-grade tables may follow the paragraph block for some specs.

def _parse_format_a(
    spec: SpecDocument,
    body_els: list[Any],
    tables: list[Any],
    doc: Any,
) -> None:
    """Parse paragraph-based parameter body."""

    params: list[ExtractedParameter] = []
    current_group = ""
    param_num = 0

    # State machine
    cur_name  = ""
    cur_value = ""
    cur_type  = "Essential"
    in_param  = False

    def _flush() -> None:
        nonlocal in_param, cur_name, cur_value, cur_type
        if cur_name.strip():
            params.append(ExtractedParameter(
                number         = len(params) + 1,
                name           = cur_name.strip(),
                unit_condition = "",
                existing_value = cur_value.strip(),
                parameter_type = cur_type,
                group          = current_group,
                raw_left       = cur_name,
                raw_right      = cur_value,
            ))
        cur_name  = ""
        cur_value = ""
        cur_type  = "Essential"
        in_param  = False

    for el in body_els:
        tag = el.tag.split("}")[-1]

        if tag == "tbl":
            # A table inside a paragraph-based spec = multi-grade grades table.
            # Check if it's another spec header (should not happen here, but guard).
            from docx.table import Table as DocxTable
            tbl = DocxTable(el, doc)
            if tbl.rows and tbl.rows[0].cells[0].text.strip().upper() == "CHEMICAL NAME":
                break   # Hit the next spec — stop
            # Try to extract grade columns and append as extra parameters
            _flush()
            _parse_table_body(spec, [tbl])
            continue

        if tag != "p":
            continue

        txt   = _para_text(el)
        style = _para_style(el)
        line  = txt.strip()

        if not line:
            continue

        # Skip footnotes and section headings
        if _FOOTNOTE_RE.match(line) or _SECTION_RE.match(line):
            continue

        # ── ListParagraph = start of a new parameter ────────────────────────
        if style in ("ListParagraph", "ListParagraph0", "List Paragraph"):
            _flush()

            # Check for group-header line: text like "Borate Sensitivity Test"
            # These appear as ListParagraph but have no colon-value, no number prefix
            if _is_group_header(line):
                current_group = line.rstrip(":")
                continue

            # Inline colon split: "Purity as CaCl2, percent:90.0 (Minimum)"
            # Use rfind to find the *last* colon that likely separates name from value
            inline_val = _try_inline_split(line)
            if inline_val is not None:
                name_part, val_part = inline_val
                cur_name  = name_part.strip()
                cur_value = val_part.strip()
                in_param  = True
            else:
                cur_name = line
                cur_value = ""
                in_param = True
            continue

        # ── BodyText = continuation of the previous parameter name ───────────
        if style in ("BodyText", "Body Text") and in_param and not cur_value:
            cur_name = (cur_name + " " + line).strip()
            continue

        # ── Unstyled paragraph ───────────────────────────────────────────────
        if style == "":
            if not in_param:
                continue
            if line.startswith(":"):
                # Value line for the current parameter
                val_text = line[1:].strip()
                if cur_value:
                    cur_value = (cur_value + " " + val_text).strip()
                else:
                    cur_value = val_text
            else:
                # Continuation of parameter name OR continuation of value
                if cur_value:
                    # Already have a value — this continues it
                    cur_value = (cur_value + " " + line).strip()
                else:
                    # Still in the name
                    cur_name = (cur_name + " " + line).strip()

    _flush()

    # Assign sequential numbers
    for i, p in enumerate(params):
        p.number = i + 1

    spec.parameters.extend(params)


def _is_group_header(line: str) -> bool:
    """Return True if a ListParagraph line looks like a section group header."""
    # Group headers: no leading digit, relatively short, often end with ":"
    # e.g. "Borate Sensitivity Test:", "Rheological Properties"
    if re.match(r"^\d+[\.\)]", line):
        return False
    if line.endswith(":") and len(line) < 80 and not re.search(r":\s*\S", line):
        return True
    return False


def _try_inline_split(line: str) -> tuple[str, str] | None:
    """Split 'Name text:Value text' when the value is inline.

    Returns (name, value) or None if no credible split found.
    The heuristic: a colon that is preceded by a letter/digit and followed
    by a non-whitespace character, with the right side looking like a value
    (starts with digit, parenthesis, or uppercase letter after stripping).
    Only splits on the LAST such colon to avoid splitting on colons within
    the name (e.g. "pH value at 25°C: …").
    """
    # Find all candidate colons
    candidates = [m.start() for m in re.finditer(r"(?<=[A-Za-z0-9\)\]%°])\s*:\s*(?=\S)", line)]
    if not candidates:
        return None
    # Use the last candidate
    idx = candidates[-1]
    # Expand to include any whitespace around the colon
    m = re.search(r"\s*:\s*", line[idx:])
    if not m:
        return None
    split_at = idx + m.end()
    name_part = line[:idx].strip()
    val_part  = line[split_at:].strip()
    # Sanity: name must be at least a few chars, value must be non-empty
    if len(name_part) >= 3 and val_part:
        return name_part, val_part
    return None


# ---------------------------------------------------------------------------
# Format B / C / D — table-only bodies
# ---------------------------------------------------------------------------

def _parse_table_body(spec: SpecDocument, tables: list[Any]) -> None:
    """Parse parameter data from tables (multi-grade or 2-col formats)."""
    for tbl in tables:
        rows = tbl.rows
        if not rows:
            continue
        ncols = len(tbl.columns)

        # Detect header row (contains grade names or column labels)
        first_cells = [c.text.strip() for c in rows[0].cells]

        # ── 2-column: [name, value] or [serial, name+value] ─────────────────
        if ncols == 2:
            _parse_2col_table(spec, tbl)
            continue

        # ── 3+-column: check if row 0 is a grade-header row ─────────────────
        # Grade header: first cell blank/serial, other cells are grade names
        if ncols >= 3 and (not first_cells[0] or first_cells[0].upper() in ("", "SL. NO.", "SL.NO.", "S. NO.", "S.NO.")):
            _parse_multigrade_table(spec, tbl)
            continue

        # ── Structured Sl.No/Parameter/Values matrix (Guar Gum style) ────────
        if ncols >= 4 and first_cells[0].upper() in ("SL. NO.", "SL.NO.", "S. NO."):
            _parse_slno_matrix_table(spec, tbl)
            continue

        # Fallback: treat as 2-col (col 0 = name, col 1 = value)
        _parse_2col_table(spec, tbl)


def _parse_2col_table(spec: SpecDocument, tbl: Any) -> None:
    """Parse a 2-column or 3-column (serial | name | value) table."""
    params = spec.parameters
    ncols = len(tbl.columns)
    name_col  = 0 if ncols == 2 else 1
    value_col = 1 if ncols == 2 else 2

    cur_name  = ""
    cur_value = ""

    def _flush() -> None:
        nonlocal cur_name, cur_value
        n = cur_name.strip().rstrip("\t:")
        v = cur_value.strip()
        if n:
            params.append(ExtractedParameter(
                number         = len(params) + 1,
                name           = n,
                unit_condition = "",
                existing_value = v,
                parameter_type = "Essential",
                group          = "",
                raw_left       = n,
                raw_right      = v,
            ))
        cur_name  = ""
        cur_value = ""

    for row in tbl.rows:
        cells = row.cells
        if len(cells) <= max(name_col, value_col):
            continue
        nc = _cell_text(cells[name_col])
        vc = _cell_text(cells[value_col]) if value_col < len(cells) else ""

        # Skip header-like rows
        if nc.upper() in ("PARAMETER", "PARAMETERS", "S. NO.", "SL. NO.", ""):
            continue

        # A cell may contain multiple parameters separated by newlines
        name_lines  = nc.split("\n")
        value_lines = vc.split("\n") if vc else [""]

        for li, nline in enumerate(name_lines):
            nline = nline.strip()
            if not nline:
                continue
            vline = value_lines[li].strip() if li < len(value_lines) else ""

            # Strip trailing tab+colon used as name/value separator within cell
            nline = nline.rstrip("\t").rstrip(":").strip()

            # New numbered parameter
            num_m = re.match(r"^(\d+)[\.\)]\s*(.*)", nline)
            if num_m:
                _flush()
                cur_name  = num_m.group(2).strip()
                cur_value = vline
            else:
                # Continuation line
                if cur_name:
                    cur_name  = (cur_name + " " + nline).strip()
                    if vline and not cur_value:
                        cur_value = vline
                    elif vline:
                        cur_value = (cur_value + " " + vline).strip()

    _flush()


def _parse_multigrade_table(spec: SpecDocument, tbl: Any) -> None:
    """Parse a multi-grade table where col 0 = name, cols 1+ = grade values.

    Row 0 is treated as the grade-name header.
    One SpecDocument parameter is created per (parameter, grade) combination,
    with the grade name appended to the parameter name for clarity.
    """
    rows = tbl.rows
    if len(rows) < 2:
        return

    # Row 0: grade names (skip the first cell which is blank or "Parameter")
    grade_names = [
        _cell_text(rows[0].cells[i]).strip() or f"Grade {i}"
        for i in range(1, len(tbl.columns))
    ]

    params = spec.parameters

    for row in rows[1:]:
        cells = row.cells
        if not cells:
            continue

        raw_name = _cell_text(cells[0]).strip().rstrip("\t:").strip()
        if not raw_name:
            continue
        # Skip header/label rows
        if raw_name.upper() in ("PARAMETER", "S. NO.", "SL. NO.", "MATERIAL CODE"):
            continue
        # Strip leading number if present
        num_m = re.match(r"^(\d+)[\.\)]\s*(.*)", raw_name)
        param_name = num_m.group(2).strip() if num_m else raw_name

        for gi, grade in enumerate(grade_names):
            col_idx = gi + 1
            val = _cell_text(cells[col_idx]).strip() if col_idx < len(cells) else ""
            if not val:
                continue
            display_name = f"{param_name} [{grade}]" if grade else param_name
            params.append(ExtractedParameter(
                number         = len(params) + 1,
                name           = display_name,
                unit_condition = "",
                existing_value = val,
                parameter_type = "Essential",
                group          = "",
                raw_left       = param_name,
                raw_right      = val,
            ))


def _parse_slno_matrix_table(spec: SpecDocument, tbl: Any) -> None:
    """Parse a Sl.No / Parameter / Grade1 / Grade2 … matrix table.

    Row 0 = column headers including grade names.
    Col 0 = serial number, Col 1 = parameter name, Cols 2+ = grade values.
    """
    rows = tbl.rows
    if len(rows) < 2:
        return

    # Extract grade names from header row (cols 2+)
    header_cells = rows[0].cells
    grade_names = [
        _cell_text(header_cells[i]).strip() or f"Grade {i - 1}"
        for i in range(2, len(tbl.columns))
    ]

    params = spec.parameters
    for row in rows[1:]:
        cells = row.cells
        if len(cells) < 3:
            continue
        param_name = _cell_text(cells[1]).strip().rstrip("\t:").strip()
        if not param_name or param_name.upper() in ("PARAMETER", "MATERIAL CODE"):
            continue

        for gi, grade in enumerate(grade_names):
            col_idx = gi + 2
            val = _cell_text(cells[col_idx]).strip() if col_idx < len(cells) else ""
            if not val:
                continue
            display_name = f"{param_name} [{grade}]" if grade else param_name
            params.append(ExtractedParameter(
                number         = len(params) + 1,
                name           = display_name,
                unit_condition = "",
                existing_value = val,
                parameter_type = "Essential",
                group          = "",
                raw_left       = param_name,
                raw_right      = val,
            ))


# ---------------------------------------------------------------------------
# Helper: convert SpecDocument to repository-compatible parameter dicts
# (mirrors the one in csc_pdf_extractor for consistency)
# ---------------------------------------------------------------------------

def spec_to_parameter_dicts(spec: SpecDocument) -> list[dict]:
    """Convert extracted parameters to the format expected by CSCRepository."""
    rows = []
    for p in spec.parameters:
        rows.append({
            "parameter_name":  p.name,
            "parameter_type":  "Essential",
            "existing_value":  p.existing_value,
            "proposed_value":  p.existing_value,   # pre-fill proposed = existing
            "test_method":     "",
            "justification":   "",
            "remarks":         "",
        })
    return rows
