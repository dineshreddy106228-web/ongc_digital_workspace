"""CSC PDF Extractor — parses ONGC Corporate Specification PDFs.

Extracts:
  - Metadata: chemical name, spec number, test procedure, material code
  - All numbered parameters: name (with unit/condition), existing requirement value
  - Section groups (e.g. "Borate Sensitivity Test:")

Entry point:
    extract_spec_from_pdf(pdf_bytes: bytes) -> SpecDocument
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExtractedParameter:
    """One numbered parameter row extracted from the spec PDF."""
    number: int                        # 1, 2, 3 …
    name: str                          # "Physical state"
    unit_condition: str                # "at 105±2°C, percent by mass"  (may be "")
    existing_value: str                # "15.0 (Maximum)"
    parameter_type: str = "Desirable"  # Vital / Desirable
    group: str = ""                    # Section group header, e.g. "Borate Sensitivity Test"
    raw_left: str = ""                 # full left-side text as extracted
    raw_right: str = ""                # full right-side text as extracted


@dataclass
class SpecDocument:
    """Parsed ONGC Corporate Specification document."""
    chemical_name: str = ""
    spec_number: str = ""
    test_procedure: str = ""
    material_code: str = ""
    parameters: list[ExtractedParameter] = field(default_factory=list)
    raw_text: str = ""
    parse_warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_spec_from_pdf(pdf_bytes: bytes) -> SpecDocument:
    """Parse an ONGC spec PDF and return a SpecDocument.

    Raises ImportError if PyMuPDF is not installed.
    Returns a SpecDocument with parse_warnings populated if extraction
    was only partially successful.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError as e:
        raise ImportError(
            "PyMuPDF is required for PDF parsing. "
            "Install it with: pip install PyMuPDF"
        ) from e

    doc_result = SpecDocument()

    try:
        pdf = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:
        doc_result.parse_warnings.append(f"Could not open PDF: {exc}")
        return doc_result

    all_blocks: list[dict] = []
    raw_lines: list[str] = []

    for page in pdf:
        page_dict = page.get_text("dict")
        all_blocks.append({"page_dict": page_dict, "page_number": page.number + 1})
        raw_lines.append(page.get_text("text"))

    doc_result.raw_text = "\n".join(raw_lines)

    # ---- Extract metadata from header table ----------------------------------
    _extract_metadata(doc_result, doc_result.raw_text)

    # ---- Extract parameters --------------------------------------------------
    _extract_parameters(doc_result, all_blocks)
    _append_parameter_review_warnings(doc_result)

    pdf.close()

    if not doc_result.parameters:
        doc_result.parse_warnings.append(
            "No numbered parameters could be extracted from this PDF. "
            "Please fill the parameter table manually."
        )

    logger.info(
        "PDF extraction complete: %s params, %d warnings",
        len(doc_result.parameters),
        len(doc_result.parse_warnings),
    )
    return doc_result


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

_META_PATTERNS = {
    "chemical_name": [
        r"CHEMICAL\s+NAME\s*[\n\r]+([^\n\r]+)",
        r"CHEMICAL\s+NAME\s+([A-Z][A-Z0-9 /,&()\-]+)",
    ],
    "spec_number": [
        r"SPECIFICATION\s+No\.?\s*[\n\r]+([^\n\r]+)",
        r"SPECIFICATION\s+No\.?\s+([A-Z0-9 /\-]+)",
    ],
    "test_procedure": [
        r"TEST\s+PROCEDURE\s*[\n\r]+([^\n\r]+)",
        r"TEST\s+PROCEDURE\s+([A-Z0-9 /\-]+)",
    ],
    "material_code": [
        r"MATERIAL\s+CODE\s*[\n\r]+([^\n\r]+)",
        r"MATERIAL\s+CODE\s+([0-9]+)",
    ],
}


def _extract_metadata(result: SpecDocument, raw: str) -> None:
    for field_name, patterns in _META_PATTERNS.items():
        for pat in patterns:
            m = re.search(pat, raw, re.IGNORECASE)
            if m:
                val = m.group(1).strip()
                # Remove revision suffix from spec number if it bled in
                if field_name == "spec_number":
                    val = re.sub(r"\s*\(.*?Revision.*?\)", "", val).strip()
                setattr(result, field_name, val)
                break


# ---------------------------------------------------------------------------
# Parameter extraction — column-aware table reader
# ---------------------------------------------------------------------------
#
# The ONGC spec PDF has a 4-column table:
#   Col A  S. No.          x ≈ 35 – 73
#   Col B  Parameter       x ≈ 74 – 277
#   Col C  Category        x ≈ 278 – 356   ("Essential", "Vital", …)
#   Col D  Required Value  x ≥ 374         (the ":"  separator lives at x ≈ 357–373)
#
# The plain-text fallback used to merge all four columns into one string and
# then tried to split on ":" — which meant "Essential" ended up fused into
# either the parameter name or the required-value text.
#
# The fix: read spans with their x-coordinates and assign each span to its
# correct column *before* reconstructing text.  Continuation rows (no row
# number) are appended to the previous parameter's columns.

# X boundaries that separate the four columns.
# Calibrated from the DFC_01.pdf span dump; a ±20 pt tolerance handles
# minor layout variation across different spec PDFs.
_COL_B_MIN_X  =  60.0   # Parameter column starts here
_COL_C_MIN_X  = 260.0   # Category column starts here  (Essential / Vital / …)
_COL_SEP_MIN_X= 345.0   # The ":" separator column
_COL_D_MIN_X  = 360.0   # Required Value column starts here

# Section group headers — un-numbered lines that end with ":" and introduce
# a sub-section within the parameter table (e.g. "Borate Sensitivity Test:")
_GROUP_HEADER_RE = re.compile(
    r"^([A-Za-z][A-Za-z0-9 /\-()&]+):$"
)

# Skip patterns for header / footer text that must not be treated as params
_SKIP_PATTERNS = re.compile(
    r"^(?:"
    r"CORPORATE SPECIFICATIONS FOR OIL FIELD CHEMICALS"
    r"|CHEMICAL\s+NAME"
    r"|SPECIFICATION\s+No\."
    r"|TEST\s+PROCEDURE"
    r"|MATERIAL\s+CODE"
    r"|S\.\s*No\."          # table header row
    r"|Parameter"           # table header row
    r"|Category"            # table header row
    r"|Required\s+Value"    # table header row
    r"|\d+$"                # bare page numbers
    r"|ONGC"
    r"|\(\d+"               # "(1st Revision…)"
    r")",
    re.IGNORECASE,
)


def _extract_parameters(result: SpecDocument, all_blocks: list[dict]) -> None:
    """Walk all pages and extract parameter rows using x-coordinate column mapping."""

    all_spans: list[dict] = []
    for page_info in all_blocks:
        page_dict = page_info["page_dict"]
        page_num  = page_info["page_number"]
        for block in page_dict.get("blocks", []):
            if "lines" not in block:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    txt = span["text"].strip()
                    if txt:
                        all_spans.append({
                            "text":  txt,
                            "x":     span["origin"][0],
                            "y":     span["origin"][1],
                            "page":  page_num,
                            "font":  span["font"],
                        })

    # Group spans into visual rows (same page, y within ~4 pts)
    rows = _group_spans_to_rows(all_spans)

    # Parse rows into parameter records
    _parse_rows_into_params(result, rows)


def _group_spans_to_rows(spans: list[dict]) -> list[list[dict]]:
    """Group spans that share approximately the same y-coordinate into rows."""
    if not spans:
        return []

    rows: list[list[dict]] = []
    current: list[dict] = [spans[0]]
    cy = spans[0]["y"]
    cp = spans[0]["page"]

    for span in spans[1:]:
        if span["page"] != cp or abs(span["y"] - cy) > 4:
            rows.append(sorted(current, key=lambda s: s["x"]))
            current = [span]
            cy = span["y"]
            cp = span["page"]
        else:
            current.append(span)

    if current:
        rows.append(sorted(current, key=lambda s: s["x"]))

    return rows


def _spans_in_col(row: list[dict], x_min: float, x_max: float = 1e9) -> str:
    """Concatenate text from spans whose x falls within [x_min, x_max)."""
    parts = [s["text"] for s in row if x_min <= s["x"] < x_max]
    return " ".join(parts).strip()


def _normalize_param_type_label(raw: str) -> str:
    txt = (raw or "").strip().lower()
    if "desirable" in txt:
        return "Desirable"
    if "vital" in txt:
        return "Vital"
    if "inform" in txt:
        return "Desirable"
    if "essential" in txt:
        return "Desirable"
    return "Desirable"


def _split_leading_param_type(name: str) -> tuple[str, str]:
    """Fallback: extract type label fused into parameter text (plain-text PDFs).

    Returns (clean_name, parameter_type). When the category column is read
    separately (column-aware path) this function is not needed for type
    detection, but is kept as a safety net for any residual merged text.
    """
    _LEADING_TYPE_RE = re.compile(
        r"""^\s*\[?\s*
        (?P<ptype>
            essential(?:\s*[-/]\s*informational)? |
            essential\s+informational |
            vital
        )
        \s*\]?
        \s*(?:[:\-–—]|\b)\s*
        (?P<rest>.+?)\s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )
    _TRAILING_TYPE_RE = re.compile(
        r"""^\s*
        (?P<rest>.+?)
        \s*(?:,?\s*as)?\s*
        \[?\s*
        (?P<ptype>
            essential(?:\s*[-/]\s*informational)? |
            essential\s+informational |
            vital
        )
        \s*\]?
        \s*$
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    raw = (name or "").strip()
    if not raw:
        return "", "Desirable"

    cleaned = raw
    detected_types: list[str] = []

    while True:
        changed = False
        lead = _LEADING_TYPE_RE.match(cleaned)
        if lead:
            rest = (lead.group("rest") or "").strip()
            if rest and not re.match(r"^[a-z]{2,}\b", rest):
                detected_types.append(_normalize_param_type_label(lead.group("ptype") or ""))
                cleaned = rest
                changed = True
        trail = _TRAILING_TYPE_RE.match(cleaned)
        if trail:
            rest = (trail.group("rest") or "").strip().rstrip(",:;- ")
            if rest:
                detected_types.append(_normalize_param_type_label(trail.group("ptype") or ""))
                cleaned = rest
                changed = True
        if not changed:
            break

    if not detected_types:
        return raw, "Desirable"
    if "Desirable" in detected_types:
        return cleaned, "Desirable"
    if "Vital" in detected_types:
        return cleaned, "Vital"
    return cleaned, "Desirable"


def _parse_rows_into_params(result: SpecDocument, rows: list[list[dict]]) -> None:
    """Convert visual rows into ExtractedParameter entries using column x-positions.

    Each data row in the ONGC table has four columns:
      Col A  S. No.          (x < _COL_B_MIN_X)
      Col B  Parameter name  (_COL_B_MIN_X ≤ x < _COL_C_MIN_X)
      Col C  Category        (_COL_C_MIN_X ≤ x < _COL_D_MIN_X)
      Col D  Required Value  (x ≥ _COL_D_MIN_X)

    Multi-line values: continuation rows have no number in Col A; their
    Col B text is appended to the current parameter name, and Col D text
    is appended to the current required value.
    """
    params: list[ExtractedParameter] = []
    current_group = ""

    # Running state for multi-line rows
    cur_num:   int  = 0
    cur_name:  str  = ""
    cur_cat:   str  = ""
    cur_value: str  = ""
    in_param:  bool = False

    def _flush() -> None:
        """Commit the accumulated parameter to the list."""
        if not cur_num or not cur_name.strip():
            return
        clean_name, fallback_type = _split_leading_param_type(cur_name.strip())
        param_type = _normalize_param_type_label(cur_cat) if cur_cat.strip() else fallback_type
        params.append(ExtractedParameter(
            number         = cur_num,
            name           = clean_name,
            unit_condition = "",
            existing_value = cur_value.strip(),
            parameter_type = param_type,
            group          = current_group,
            raw_left       = cur_name,
            raw_right      = cur_value,
        ))

    for row in rows:
        # ── Reconstruct text for the full row (for skip / group-header checks)
        full_text = " ".join(s["text"] for s in row).strip()
        if not full_text:
            continue
        if _SKIP_PATTERNS.match(full_text):
            continue

        # ── Extract text per column using x boundaries ──────────────────────
        col_a = _spans_in_col(row, 0,              _COL_B_MIN_X)   # S. No.
        col_b = _spans_in_col(row, _COL_B_MIN_X,  _COL_C_MIN_X)   # Parameter
        col_c = _spans_in_col(row, _COL_C_MIN_X,  _COL_D_MIN_X)   # Category
        col_d = _spans_in_col(row, _COL_D_MIN_X)                   # Required Value

        # ── Group header detection (un-numbered line ending with ":") ────────
        num_match = re.match(r"^(\d+)\.$", col_a.strip())
        if not num_match and not col_a.strip():
            # Could be a section group header carried entirely in col_b
            grp_m = _GROUP_HEADER_RE.match(col_b)
            if grp_m and not col_c and not col_d:
                _flush()
                in_param = False
                cur_num = 0
                current_group = grp_m.group(1).strip()
                continue

        # ── New numbered row ─────────────────────────────────────────────────
        if num_match:
            _flush()
            cur_num   = int(num_match.group(1))
            cur_name  = col_b
            cur_cat   = col_c
            cur_value = col_d
            in_param  = True
            continue

        # ── Continuation row (no number in col A) ────────────────────────────
        if in_param:
            if col_b:
                cur_name  = (cur_name + " " + col_b).strip()
            if col_d:
                cur_value = (cur_value + " " + col_d).strip()
            # col_c on continuation rows is typically blank or repeats category;
            # keep the first-row category as authoritative.
            continue

        # ── Unrecognised line outside a parameter block ───────────────────────
        # (e.g. stray metadata that slipped past the skip filter — ignore)

    # Flush the last accumulated parameter
    _flush()

    # Sort by number and assign to result
    params.sort(key=lambda p: p.number)
    result.parameters = params


def _append_parameter_review_warnings(result: SpecDocument) -> None:
    """Flag rows that likely need manual review before draft creation.

    Some legacy CSC Format A PDFs wrap parameter-name fragments into the
    required-value column. We still stage those rows, but warn the user so the
    review screen can be used to correct them before a draft is created.
    """
    flagged_rows: list[str] = []
    for parameter in result.parameters:
        name = (parameter.name or "").strip()
        value = (parameter.existing_value or "").strip()
        if not name:
            flagged_rows.append(str(parameter.number))
            continue
        suspicious_name = (
            name.count("(") > name.count(")")
            or name.endswith("%")
            or bool(re.search(r"\bat\s+\d+$", name))
        )
        suspicious_value = not value
        if suspicious_name or suspicious_value:
            flagged_rows.append(str(parameter.number))

    if flagged_rows:
        result.parse_warnings.append(
            "Review extracted rows "
            + ", ".join(flagged_rows)
            + " before creating the draft. This PDF uses wrapped table lines, so some fields may need correction."
        )


# ---------------------------------------------------------------------------
# Helper: convert SpecDocument to repository-compatible parameter dicts
# ---------------------------------------------------------------------------

def spec_to_parameter_dicts(spec: SpecDocument) -> list[dict]:
    """Convert extracted parameters to the format expected by CSCRepository.upsert_parameters."""
    rows = []
    for p in spec.parameters:
        # Group label becomes a prefix in parameter_name if present
        base_name, parsed_type = _split_leading_param_type(p.name)
        name = base_name
        if p.group:
            name = f"[{p.group}] {name}"
        rows.append({
            "parameter_name":  name,
            "parameter_type":  "Desirable",
            "existing_value":  p.existing_value,
            "proposed_value":  p.existing_value,   # pre-fill proposed = existing
            "test_method":     "",
            "justification":   "",
            "remarks":         "",
        })
    return rows
