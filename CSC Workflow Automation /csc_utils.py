"""CSC Drafting Workspace — constants and utility helpers."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone, timedelta

from occ_utils import (
    get_utc_now,
    sanitize_text,
    sanitize_multiline_text,
    truncate_for_log,
    is_rate_limited,
)

__all__ = [
    "new_uuid",
    "fmt_ist",
    "get_utc_now",
    "sanitize_text",
    "sanitize_multiline_text",
    "truncate_for_log",
    "is_rate_limited",
    # Section name constants
    "SECTION_BACKGROUND",
    "SECTION_EXISTING_SPEC",
    "SECTION_ISSUES",
    "SECTION_PARAMETERS",
    "SECTION_PROPOSED",
    "SECTION_JUSTIFICATION",
    "SECTION_IMPACT",
    "SECTION_RECOMMENDATION",
    "SECTION_PREVIEW",
    "SECTION_ORDER",
    # Structured data constants
    "ISSUE_TYPES",
    "IMPACT_SUBTYPES",
    "PARAMETER_TYPES",
    "DRAFT_STATUS_OPTIONS",
    "DEFAULT_PREPARED_BY",
    # Role / mode constants
    "ROLE_ADMIN",
    "ROLE_USER",
    "ADMIN_PASSPHRASE",
    "ADMIN_PUBLISHED_STATUS",
    "SPEC_SUBSET_ORDER",
    "SPEC_SUBSET_LABELS",
    "parse_spec_number",
    "spec_sort_key",
    "sort_specs_by_subset_order",
    # Recommendation section keys
    "REC_MAIN_KEY",
    "REC_REMARKS_KEY",
]

# ---------------------------------------------------------------------------
# Primary key generation
# ---------------------------------------------------------------------------

def new_uuid() -> str:
    """Generate a new UUID4 string for use as a primary key."""
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Timestamp formatting
# ---------------------------------------------------------------------------

_IST_OFFSET = timedelta(hours=5, minutes=30)
_IST_TZ = timezone(_IST_OFFSET)


def fmt_ist(dt_str: str | None) -> str:
    """Format ISO UTC datetime string as IST for display."""
    if not dt_str:
        return "—"
    try:
        # Handle both offset-aware and naive ISO strings
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        if "+" in dt_str[10:] or dt_str.endswith("UTC"):
            dt = datetime.fromisoformat(dt_str.replace("UTC", "+00:00"))
        else:
            dt = datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
        ist = dt.astimezone(_IST_TZ)
        return ist.strftime("%d %b %Y, %H:%M IST")
    except Exception:
        return dt_str[:16] if dt_str else "—"


# ---------------------------------------------------------------------------
# Section name constants
# ---------------------------------------------------------------------------

SECTION_BACKGROUND      = "Background"
SECTION_EXISTING_SPEC   = "Existing Specification Summary"
SECTION_ISSUES          = "Issues Observed"
SECTION_PARAMETERS      = "Parameter Review"
SECTION_PROPOSED        = "Proposed Changes"
SECTION_JUSTIFICATION   = "Justification"
SECTION_IMPACT          = "Impact Analysis"
SECTION_RECOMMENDATION  = "Committee Recommendation"
SECTION_PREVIEW         = "Preview & Export"

SECTION_ORDER: list[str] = [
    SECTION_BACKGROUND,
    SECTION_EXISTING_SPEC,
    SECTION_ISSUES,
    SECTION_PARAMETERS,
    SECTION_PROPOSED,
    SECTION_JUSTIFICATION,
    SECTION_IMPACT,
    SECTION_RECOMMENDATION,
    SECTION_PREVIEW,
]

# ---------------------------------------------------------------------------
# Issue flag types
# ---------------------------------------------------------------------------

# Each tuple: (db_key, display_label)
ISSUE_TYPES: list[tuple[str, str]] = [
    ("operational", "Operational Issue"),
    ("quality",     "Quality Issue"),
    ("supply",      "Supply Issue"),
    ("testing",     "Testing / Evaluation Issue"),
]

# ---------------------------------------------------------------------------
# Impact Analysis sub-section keys (stored in csc_sections with prefixed names)
# ---------------------------------------------------------------------------

# Each tuple: (section_name in DB, display_label)
IMPACT_SUBTYPES: list[tuple[str, str]] = [
    ("Impact::operational",     "Operational Impact"),
    ("Impact::supply_chain",    "Supply Chain Impact"),
    ("Impact::cost",            "Cost Impact"),
    ("Impact::quality_testing", "Quality / Testing Impact"),
    ("Impact::safety_regulatory", "Safety / Regulatory Impact"),
]

# ---------------------------------------------------------------------------
# Committee Recommendation section keys (stored in csc_sections)
# ---------------------------------------------------------------------------

REC_MAIN_KEY    = "Recommendation::main"
REC_REMARKS_KEY = "Recommendation::remarks"

# ---------------------------------------------------------------------------
# Parameter and draft status options
# ---------------------------------------------------------------------------

PARAMETER_TYPES: list[str] = ["Essential", "Desirable"]

DEFAULT_PREPARED_BY: str = "Dinesh Reddy_106228"

DRAFT_STATUS_OPTIONS: list[str] = ["Draft", "Under Review", "Committee Review", "Final"]

# ---------------------------------------------------------------------------
# Role / mode constants
# ---------------------------------------------------------------------------

ROLE_ADMIN = "Admin"
ROLE_USER  = "User"

# Simple passphrase gate for admin mode (no auth server needed for v1).
# Change this value to your preferred passphrase.
ADMIN_PASSPHRASE: str = "ongc-csc-admin"

# Drafts that have been "published" by admin are locked for committee users
# (they skip Phase 1 verification and go straight to Phase 2).
ADMIN_PUBLISHED_STATUS = "Committee Review"   # status value set by admin on publish

# ---------------------------------------------------------------------------
# Specification number helpers / subset ordering
# ---------------------------------------------------------------------------

SPEC_SUBSET_ORDER: list[str] = [
    "DFC",
    "CCA",
    "WCF",
    "WS",
    "PC",
    "WIC",
    "WM",
    "UTL",
    "LPG",
    "API",
    "DISCONTINUED",
]

SPEC_SUBSET_LABELS: dict[str, str] = {
    "DFC": "Drilling Fluid Chemicals",
    "CCA": "Cement & Cement Additives",
    "WCF": "Well Completion Fluid Chemicals",
    "WS": "Well Stimulation Chemicals",
    "PC": "Production Chemicals",
    "WIC": "Water Injection Chemicals",
    "WM": "Water Maker Chemicals",
    "UTL": "Utility Chemicals",
    "LPG": "LPG and Plant Chemicals",
    "API": "API",
    "DISCONTINUED": "Discontinued Specifications",
}

_SPEC_NUMBER_RE = re.compile(
    r"^\s*ONGC/([A-Za-z0-9\-]+)/([0-9]{1,4})/([0-9]{4})\s*$"
)


def parse_spec_number(spec_number: str) -> tuple[str | None, int | None, int | None]:
    """Parse ONGC spec number like ONGC/DFC/01/2026 -> (subset, sequence, year)."""
    match = _SPEC_NUMBER_RE.match((spec_number or "").strip())
    if not match:
        return None, None, None
    subset = match.group(1).upper()
    sequence = int(match.group(2))
    year = int(match.group(3))
    return subset, sequence, year


def spec_sort_key(spec_number: str) -> tuple[int, int, int, str]:
    """Sort key: subset order -> numeric sequence -> year -> spec_number text."""
    subset, sequence, year = parse_spec_number(spec_number)
    subset_rank = (
        SPEC_SUBSET_ORDER.index(subset)
        if subset in SPEC_SUBSET_ORDER
        else len(SPEC_SUBSET_ORDER)
    )
    seq_rank = sequence if sequence is not None else 9999
    year_rank = year if year is not None else 9999
    text_rank = (spec_number or "").strip().upper()
    return subset_rank, seq_rank, year_rank, text_rank


def sort_specs_by_subset_order(
    rows: list[dict],
    spec_key: str = "spec_number",
) -> list[dict]:
    """Return a new list sorted by ONGC subset order and spec sequence."""
    return sorted(rows, key=lambda row: spec_sort_key(str(row.get(spec_key, "") or "")))

# ---------------------------------------------------------------------------
# Section guidance prompts
# ---------------------------------------------------------------------------

SECTION_GUIDANCE: dict[str, str] = {
    SECTION_BACKGROUND: (
        "Describe the origin of this specification review request, the current applicable "
        "standard/specification number, and the reason for review."
    ),
    SECTION_EXISTING_SPEC: (
        "Summarize the current specification requirements, applicable grades, and key "
        "parameters currently in force."
    ),
    SECTION_PROPOSED: (
        "List the proposed modifications to the specification parameters, grades, or test "
        "methods with a clear before/after comparison."
    ),
    SECTION_JUSTIFICATION: (
        "Provide technical and operational justification for each proposed change, "
        "referencing field data, laboratory results, or supply constraints."
    ),
}
