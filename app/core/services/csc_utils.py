"""CSC Workflow — constants and utility helpers."""

from __future__ import annotations
import json
import re
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta

# Section name constants
SECTION_BACKGROUND = "Background"
SECTION_EXISTING_SPEC = "Existing Specification Summary"
SECTION_ISSUES = "Issues Observed"
SECTION_PARAMETERS = "Parameter Review"
SECTION_PROPOSED = "Proposed Changes"
SECTION_JUSTIFICATION = "Justification"
SECTION_IMPACT = "Impact Analysis"
SECTION_RECOMMENDATION = "Committee Recommendation"
SECTION_PREVIEW = "Preview & Export"

SECTION_ORDER = [
    SECTION_BACKGROUND, SECTION_EXISTING_SPEC, SECTION_ISSUES,
    SECTION_PARAMETERS, SECTION_PROPOSED, SECTION_JUSTIFICATION,
    SECTION_IMPACT, SECTION_RECOMMENDATION, SECTION_PREVIEW,
]

# Issue types: (db_key, display_label)
ISSUE_TYPES = [
    ("operational", "Operational Issue"),
    ("quality", "Quality Issue"),
    ("supply", "Supply Issue"),
    ("testing", "Evaluation Issue"),
]

# Impact sub-sections
IMPACT_SUBTYPES = [
    ("Impact::operational", "Operational Impact"),
    ("Impact::supply_chain", "Supply Chain Impact"),
    ("Impact::cost", "Cost Impact"),
    ("Impact::quality_testing", "Quality / Evaluation Impact"),
    ("Impact::safety_regulatory", "Safety / Regulatory Impact"),
]

# Recommendation keys
REC_MAIN_KEY = "Recommendation::main"
REC_REMARKS_KEY = "Recommendation::remarks"

PARAMETER_TYPES = ["Vital", "Desirable"]
TEST_PROCEDURE_OPTIONS = [
    "ASTIM",
    "API",
    "IS",
    "Inhouse",
    "Any other National / International Standard",
]
DRAFT_STATUS_OPTIONS = ["Drafting", "Published"]
WORKFLOW_DRAFTING_STAGING = "staging"
WORKFLOW_DRAFTING_OPEN = "open"
WORKFLOW_DRAFTING_SUBMITTED = "submitted"
WORKFLOW_DRAFTING_HEAD_APPROVED = "head_approved"
WORKFLOW_DRAFTING_RETURNED = "returned"
WORKFLOW_DRAFTING_APPROVED = "approved"
WORKFLOW_DRAFTING_REJECTED = "rejected"
WORKFLOW_DRAFTING_STATE_OPTIONS = [
    WORKFLOW_DRAFTING_STAGING,
    WORKFLOW_DRAFTING_OPEN,
    WORKFLOW_DRAFTING_SUBMITTED,
    WORKFLOW_DRAFTING_HEAD_APPROVED,
    WORKFLOW_DRAFTING_RETURNED,
    WORKFLOW_DRAFTING_APPROVED,
    WORKFLOW_DRAFTING_REJECTED,
]
DEFAULT_PREPARED_BY = "Dinesh Reddy_106228"

# Spec subset ordering
SPEC_SUBSET_ORDER = ["DFC", "CCA", "WCF", "WS", "PC", "WIC", "WM", "UTL", "LPG"]
SPEC_SUBSET_LABELS = {
    "DFC": "Drilling Fluid Chemicals",
    "CCA": "Cement & Cement Additive",
    "WCF": "Well Completion Fluid",
    "WS": "Well Stimulation Chemicals",
    "PC": "Production Chemicals",
    "WIC": "Water Injection Chemicals",
    "WM": "Well Maker Chemicals",
    "UTL": "Utility Chemicals",
    "LPG": "Plant Chemiclas (LPG)",
}

# Admin stage constants
ADMIN_STAGE_DRAFTING = "drafting"
ADMIN_STAGE_PUBLISHED = "published"
ADMIN_STAGE_OPTIONS = [ADMIN_STAGE_DRAFTING, ADMIN_STAGE_PUBLISHED]
ADMIN_PUBLISHED_STATUS = "Published"

# Type normalization aliases
TYPE_CLASS_ALIASES = {
    "essential": "Desirable",
    "desirable": "Desirable",
    "vital": "Vital",
    "essential-informational": "Desirable",
    "essential informational": "Desirable",
    "essential / informational": "Desirable",
    "essential - informational": "Desirable",
    "essential/informational": "Desirable",
    "informational": "Desirable",
}

TEST_PROCEDURE_TYPE_ALIASES = {
    "astm": "ASTIM", "astim": "ASTIM", "api": "API", "is": "IS",
    "inhouse": "Inhouse", "in-house": "Inhouse",
    "other national / international": "Any other National / International Standard",
    "other national/international": "Any other National / International Standard",
    "other national international": "Any other National / International Standard",
    "any other national / international standard": "Any other National / International Standard",
}

IMPACT_CHECKLIST_VERSION = 2

# Valid 4-state flag values
VALID_FLAG_VALUES = ["YES", "PROVISIONAL", "REVIEW", "NO"]

# Flag group membership
RED_FLAG_IDS = ["hse_flag", "op_acute_flag", "op_chronic_flag", "safety_flag"]
AMBER_FLAG_IDS = ["env_flag", "regulatory_flag", "supply_flag", "seasonal_flag", "financial_flag"]
ALL_FLAG_IDS = RED_FLAG_IDS + AMBER_FLAG_IDS

IMPACT_CHECKLIST_FLAGS = [
    # ── Red flags ──────────────────────────────────────────────────────────────
    {
        "id": "hse_flag",
        "order": 1,
        "dimension": "HSE / Hazard",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Is the chemical classified as Toxic, Corrosive, Flammable, or Carcinogenic in its SDS?",
        "source": "MSDS",
        "detail": (
            "Check SDS Section 2 for GHS pictograms — skull/crossbones (toxic), flame (flammable), "
            "corrosion symbol (corrosive), health hazard diamond (carcinogen/mutagen). "
            "Any one of these = YES."
        ),
    },
    {
        "id": "op_acute_flag",
        "order": 2,
        "dimension": "Op-Acute",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Will production or well operation stop within 7 consecutive days if this chemical is unavailable?",
        "source": "User Department",
        "detail": (
            "The 7-day window reflects realistic emergency procurement lead time. "
            "If a substitute can be deployed within 7 days, flag = NO. "
            "Must be confirmed by Chemistry discipline, not stores or procurement."
        ),
    },
    {
        "id": "op_chronic_flag",
        "order": 3,
        "dimension": "Op-Chronic",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Will prolonged absence of this chemical (30+ days) cause irreversible damage to pipelines, equipment, wells, or reservoirs — even if operations continue normally in the short term?",
        "source": "User Department",
        "detail": (
            "Covers corrosion inhibitors, scale inhibitors, biocides (MIC), wax inhibitors, "
            "asphaltene inhibitors, pour point depressants. Asset damage is irreversible and "
            "potentially catastrophic — operationally critical regardless of 7-day production impact."
        ),
    },
    {
        "id": "safety_flag",
        "order": 4,
        "dimension": "Safety-critical",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Is this chemical used for safety, emergency response, or personnel protection where its absence creates a statutory or life-safety risk under OISD or DGMS — even if production is unaffected?",
        "source": "User Department",
        "detail": (
            "Firefighting foam (AFFF), H2S neutralisation chemicals, ETP treatment chemicals, "
            "spill absorbents, emergency response agents. Their absence on the day of an incident "
            "is a direct regulatory breach under OISD-STD-109/117."
        ),
    },
    # ── Amber flags ────────────────────────────────────────────────────────────
    {
        "id": "env_flag",
        "order": 5,
        "dimension": "Environment",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (no red flags) = Medium Impact",
        "question": "Does disposal of this chemical require hazardous waste treatment or authorisation from CPCB or State PCB?",
        "source": "MSDS + CPCB Rules",
        "detail": (
            "Cross-reference with Hazardous Waste Management Rules 2016 Schedule I and II. "
            "If spent chemical or container must go to a CPCB-authorised TSDF = YES. "
            "Source is committee knowledge — SDS Section 13 serves as verification once available."
        ),
    },
    {
        "id": "regulatory_flag",
        "order": 6,
        "dimension": "Regulatory",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (no red flags) = Medium Impact",
        "question": "Does storage or use require a statutory licence, DGMS approval, or OISD compliance?",
        "source": "Indentor",
        "detail": (
            "Petroleum Act 1934, Explosives Act 1884, DGMS Circulars for drilling/mining chemicals, "
            "OISD-STD-109/117 for petroleum installations."
        ),
    },
    {
        "id": "supply_flag",
        "order": 7,
        "dimension": "Supply Chain",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (no red flags) = Medium Impact",
        "question": "Is there only ONE qualified vendor OR is the chemical exclusively imported with no domestic supply?",
        "source": "Vendor Master",
        "detail": (
            "If only 1 vendor registered and no alternate qualified in last 3 years = YES. "
            "All-import sourcing = YES regardless of importer count."
        ),
    },
    {
        "id": "seasonal_flag",
        "order": 8,
        "dimension": "Seasonal",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (no red flags) = Medium Impact",
        "question": "Is this chemical required only during specific seasons or operational campaigns where a procurement delay during that window causes disproportionate operational loss?",
        "source": "User Department",
        "detail": (
            "Hydrate inhibitors (winter), wax inhibitors (temperature drop), workover fluids (campaign), "
            "stimulation acids (drilling campaign). Even if average consumption appears low, "
            "unavailability at the critical window = major consequence."
        ),
    },
    {
        "id": "financial_flag",
        "order": 9,
        "dimension": "Financial",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (no red flags) = Medium Impact",
        "question": "Does this chemical show expiry/wastage loss > Rs. 1 lakh in 2 years OR disposal cost > 20% of purchase price?",
        "source": "Indentor",
        "detail": (
            "Note — stores write-off register data may be unreliable due to institutional practice of "
            "booking expired stock as consumed. This flag may be pre-filled from MB51 movement analysis "
            "as PROVISIONAL where only one risk signal is present. Committee owner should confirm or override."
        ),
    },
]

# Section guidance prompts
SECTION_GUIDANCE = {
    SECTION_BACKGROUND: "Describe the origin of this specification review request, the current applicable standard/specification number, and the reason for review.",
    SECTION_EXISTING_SPEC: "Summarize the current specification requirements, applicable grades, and key parameters currently in force.",
    SECTION_PROPOSED: "List the proposed modifications to the specification parameters, grades, or test methods with a clear before/after comparison.",
    SECTION_JUSTIFICATION: "Provide technical and operational justification for each proposed change, referencing field data, laboratory results, or supply constraints.",
}

DEFAULT_BACKGROUND_VERSION_HISTORY = "Version 0 : Legacy 2015 Specification"
DEFAULT_VERSION_CHANGE_REASON = (
    "Migration to 2026 version with type classification of all parameters into Vital and Desirable"
)
SPEC_VERSION_DECIMAL_STEP = 1
SPEC_VERSION_WHOLE_STEP = 10


_SPEC_NUMBER_RE = re.compile(r"^\s*ONGC/([A-Za-z0-9\-]+)/([A-Za-z0-9]{1,8})/([0-9]{4})\s*$")
_SPEC_SEQUENCE_RE = re.compile(r"^(\d+)([A-Za-z]*)$")


def parse_spec_number(spec_number: str) -> tuple:
    match = _SPEC_NUMBER_RE.match((spec_number or "").strip())
    if not match:
        return None, None, None
    return match.group(1).upper(), match.group(2).upper(), int(match.group(3))


def sequence_sort_key(sequence: str | None) -> tuple:
    token = (sequence or "").strip().upper()
    if not token:
        return 9999, "ZZZ"

    match = _SPEC_SEQUENCE_RE.match(token)
    if not match:
        return 9999, token

    return int(match.group(1)), match.group(2) or ""


def spec_sort_key(spec_number: str) -> tuple:
    subset, sequence, year = parse_spec_number(spec_number)
    subset_rank = SPEC_SUBSET_ORDER.index(subset) if subset in SPEC_SUBSET_ORDER else len(SPEC_SUBSET_ORDER)
    sequence_number, sequence_suffix = sequence_sort_key(sequence)
    return (
        subset_rank,
        sequence_number,
        sequence_suffix,
        year or 9999,
        (spec_number or "").strip().upper(),
    )


def sort_specs_by_subset_order(rows, spec_key="spec_number"):
    return sorted(rows, key=lambda r: spec_sort_key(str(r.get(spec_key, "") or "")))


def normalize_parameter_type_label(raw: str) -> str:
    txt = str(raw or "").strip().lower()
    return TYPE_CLASS_ALIASES.get(txt, "Desirable") if txt else "Desirable"


def normalize_test_procedure_type(raw: str) -> str:
    txt = str(raw or "").strip()
    lowered = txt.lower()
    if not txt:
        return ""
    if lowered in TEST_PROCEDURE_TYPE_ALIASES:
        return TEST_PROCEDURE_TYPE_ALIASES[lowered]
    if lowered.startswith("astm") or lowered.startswith("astim"):
        return "ASTIM"
    if lowered.startswith("api"):
        return "API"
    if lowered.startswith("is"):
        return "IS"
    if lowered.startswith("inhouse") or lowered.startswith("in-house"):
        return "Inhouse"
    if "national" in lowered or "international" in lowered:
        return "Any other National / International Standard"
    return txt


def calculate_impact_score(operational: int, safety: int, supply: int) -> float:
    return float(operational) * 0.5 + float(safety) * 0.3 + float(supply) * 0.2


def get_impact_grade(score: float, no_substitute: bool = False) -> str:
    if score >= 4.0:
        grade = "CRITICAL"
    elif score >= 3.0:
        grade = "HIGH"
    elif score >= 2.0:
        grade = "MODERATE"
    else:
        grade = "LOW"
    if no_substitute and grade in ("LOW", "MODERATE"):
        return "HIGH"
    return grade


def sanitize_text(text: str, max_length: int = 2000) -> str:
    import re as _re
    txt = str(text or "").strip()
    txt = _re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", txt)
    return txt[:max_length]


def sanitize_multiline_text(text: str, max_length: int = 20000) -> str:
    import re as _re
    txt = str(text or "").strip()
    txt = _re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", txt)
    return txt[:max_length]


def format_required_value(
    required_value_type: str | None,
    required_value_text: str | None = None,
    operator_1: str | None = None,
    value_1: str | None = None,
    operator_2: str | None = None,
    value_2: str | None = None,
) -> str:
    """Build a display string for structured required-value fields."""
    mode = str(required_value_type or "text").strip().lower()
    if mode != "comparison":
        return sanitize_multiline_text(required_value_text or "", max_length=2000)

    parts = []
    if str(operator_1 or "").strip() and str(value_1 or "").strip():
        parts.append(f"{str(operator_1).strip()} {str(value_1).strip()}")
    if str(operator_2 or "").strip() and str(value_2 or "").strip():
        parts.append(f"{str(operator_2).strip()} {str(value_2).strip()}")
    return " and ".join(parts)


def normalize_spec_version(raw_value) -> int:
    """Store spec versions as tenths so whole/decimal bumps fit the legacy integer column."""
    if raw_value is None or raw_value == "":
        return 0
    if isinstance(raw_value, bool):
        return int(raw_value)
    if isinstance(raw_value, int):
        return raw_value
    try:
        decimal_value = Decimal(str(raw_value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return 0
    return int((decimal_value * Decimal("10")).quantize(Decimal("1")))


def format_spec_version(raw_value) -> str:
    """Render tenths-backed spec versions as user-facing strings."""
    stored_value = normalize_spec_version(raw_value)
    whole = stored_value // 10
    remainder = stored_value % 10
    if remainder == 0:
        return str(whole)
    return f"{whole}.{remainder}"


def increment_spec_version(raw_value, increment_type: str | None) -> int:
    """Increment version by a whole step or decimal step."""
    stored_value = normalize_spec_version(raw_value)
    increment_key = str(increment_type or "").strip().lower()
    if increment_key == "decimal":
        return stored_value + SPEC_VERSION_DECIMAL_STEP
    return stored_value + SPEC_VERSION_WHOLE_STEP


def _normalize_impact_flag_answer(value) -> str:
    """Normalize any flag value representation to one of the 4 canonical states."""
    if isinstance(value, str):
        token = value.strip().upper()
        if token in {"YES", "PROVISIONAL", "REVIEW", "NO"}:
            return token
        # Legacy boolean strings
        if token in {"TRUE", "1"}:
            return "YES"
        if token in {"FALSE", "0"}:
            return "NO"
    if isinstance(value, bool):
        return "YES" if value else "NO"
    if isinstance(value, (int, float)):
        return "YES" if int(value) == 1 else ("NO" if int(value) == 0 else "REVIEW")
    return "REVIEW"


def _make_flag_entry(
    value=None,
    source_type: str = "COMMITTEE",
    answered_by: str | None = None,
    answered_on: str | None = None,
    fin_reason: str = "",
) -> dict:
    """Build a single flag metadata entry with a canonical 4-state value."""
    return {
        "value": _normalize_impact_flag_answer(value) if value is not None else "REVIEW",
        "source_type": source_type or "COMMITTEE",
        "answered_by": answered_by or "",
        "answered_on": answered_on or "",
        "fin_reason": fin_reason or "",
    }


def build_default_impact_checklist_state(chemical_name: str | None = None) -> dict:
    return {
        "version": IMPACT_CHECKLIST_VERSION,
        "chemical_name": sanitize_text(chemical_name or "", max_length=255),
        "flags": {flag["id"]: _make_flag_entry() for flag in IMPACT_CHECKLIST_FLAGS},
    }


# ── Legacy ID → new ID mapping (for v1 → v2 migration) ──────────────────────
_LEGACY_FLAG_MAP: dict[str, str] = {
    "flag_1": "hse_flag",
    "flag_2": "env_flag",
    "flag_3": "op_acute_flag",
    "flag_4": "regulatory_flag",
    "flag_5": "supply_flag",
    "flag_6": "financial_flag",
}


def deserialize_impact_checklist_state(
    raw_state: str | dict | None,
    chemical_name: str | None = None,
) -> dict:
    state = build_default_impact_checklist_state(chemical_name)
    payload = None

    if isinstance(raw_state, dict):
        payload = raw_state
    elif isinstance(raw_state, str) and raw_state.strip():
        try:
            payload = json.loads(raw_state)
        except json.JSONDecodeError:
            payload = None

    if not isinstance(payload, dict):
        return state

    stored_version = int(payload.get("version", 1) or 1)
    state["version"] = IMPACT_CHECKLIST_VERSION
    state["chemical_name"] = sanitize_text(
        payload.get("chemical_name", state["chemical_name"]) or state["chemical_name"],
        max_length=255,
    )

    # ── v2 format: flags dict with metadata objects ────────────────────────
    if stored_version >= 2 and isinstance(payload.get("flags"), dict):
        for flag in IMPACT_CHECKLIST_FLAGS:
            raw = payload["flags"].get(flag["id"])
            if isinstance(raw, dict):
                state["flags"][flag["id"]] = _make_flag_entry(
                    value=raw.get("value"),
                    source_type=raw.get("source_type", "COMMITTEE"),
                    answered_by=raw.get("answered_by", ""),
                    answered_on=raw.get("answered_on", ""),
                    fin_reason=raw.get("fin_reason", ""),
                )
        return state

    # ── v1 format: "answers" dict with boolean/null values ────────────────
    answers_raw = payload.get("answers") or payload.get("flags") or {}
    if isinstance(answers_raw, dict):
        for flag in IMPACT_CHECKLIST_FLAGS:
            # Try current flag id first, then legacy mapping
            raw_val = answers_raw.get(flag["id"])
            if raw_val is None:
                # Reverse-lookup legacy id
                for legacy_id, new_id in _LEGACY_FLAG_MAP.items():
                    if new_id == flag["id"]:
                        raw_val = answers_raw.get(legacy_id)
                        break

            if isinstance(raw_val, dict):
                raw_val = raw_val.get("answer", raw_val.get("value"))

            if raw_val is not None:
                state["flags"][flag["id"]] = _make_flag_entry(
                    value=raw_val,
                    source_type="COMMITTEE",
                    answered_by="migrated",
                    answered_on=datetime.now(timezone.utc).isoformat(),
                )

    return state


def summarize_impact_checklist_state(state: dict | None) -> dict:
    normalized = deserialize_impact_checklist_state(state)
    red_yes_count = 0
    amber_yes_count = 0
    amber_prov_count = 0
    answered_count = 0
    flag_rows = []

    for flag in IMPACT_CHECKLIST_FLAGS:
        entry = normalized["flags"].get(flag["id"]) or {}
        val = entry.get("value", "REVIEW") if isinstance(entry, dict) else "REVIEW"
        # "answered" means the user moved it out of REVIEW state
        if val != "REVIEW":
            answered_count += 1
        if val == "YES" and flag["type"] == "RED":
            red_yes_count += 1
        if val == "YES" and flag["type"] == "AMBER":
            amber_yes_count += 1
        if val == "PROVISIONAL" and flag["type"] == "AMBER":
            amber_prov_count += 1

        flag_rows.append({
            **flag,
            "meta": entry,
            "answer": val,
            "answer_label": val,
        })

    total_flags = len(IMPACT_CHECKLIST_FLAGS)
    yes_total = red_yes_count + amber_yes_count

    if red_yes_count > 0:
        classification = "HIGH IMPACT"
        grade = "HIGH"
        confidence = "Confirmed"
        rule = f"{red_yes_count} red flag(s) answered YES"
        tone = "red"
        score = 3.0
        triggered = [f["id"] for f in flag_rows if f["type"] == "RED" and f["answer"] == "YES"]
    elif amber_yes_count > 0:
        classification = "MEDIUM IMPACT"
        grade = "MEDIUM"
        confidence = "Confirmed"
        rule = f"No red flags, but {amber_yes_count} amber flag(s) answered YES"
        tone = "amber"
        score = 2.0
        triggered = [f["id"] for f in flag_rows if f["type"] == "AMBER" and f["answer"] == "YES"]
    elif amber_prov_count > 0:
        classification = "MEDIUM IMPACT"
        grade = "MEDIUM"
        confidence = "Provisional"
        rule = f"No confirmed red flags, but {amber_prov_count} amber flag(s) are PROVISIONAL"
        tone = "amber"
        score = 2.0
        triggered = [f["id"] for f in flag_rows if f["type"] == "AMBER" and f["answer"] == "PROVISIONAL"]
    elif answered_count == total_flags:
        classification = "LOW IMPACT"
        grade = "LOW"
        confidence = "Confirmed"
        rule = f"All {total_flags} flags answered — no triggers"
        tone = "green"
        score = 1.0
        triggered = []
    else:
        classification = "LOW IMPACT"
        grade = "LOW"
        confidence = "Provisional"
        rule = f"No triggers found — {total_flags - answered_count} flag(s) still at REVIEW"
        tone = "green"
        score = 1.0
        triggered = []

    return {
        "chemical_name": normalized["chemical_name"],
        "flags": flag_rows,
        "red_yes_count": red_yes_count,
        "amber_yes_count": amber_yes_count,
        "amber_prov_count": amber_prov_count,
        "yes_total": yes_total,
        "answered_count": answered_count,
        "total_flags": total_flags,
        "classification": classification,
        "grade": grade,
        "confidence": confidence,
        "triggered_flags": triggered,
        "rule": rule,
        "tone": tone,
        "score": score,
    }


def compute_impact_classification(flags: dict) -> dict:
    """
    Compute impact classification from a flat dict of flag_id → value strings.
    flags: e.g. {'hse_flag': 'YES', 'op_acute_flag': 'REVIEW', ...}
    Returns: dict with tier, confidence, triggered_flags, flags_answered, flags_total
    """
    red_yes    = [f for f in RED_FLAG_IDS   if flags.get(f) == "YES"]
    amber_yes  = [f for f in AMBER_FLAG_IDS if flags.get(f) == "YES"]
    amber_prov = [f for f in AMBER_FLAG_IDS if flags.get(f) == "PROVISIONAL"]
    answered   = [f for f in ALL_FLAG_IDS   if flags.get(f) in ("YES", "PROVISIONAL", "NO")]

    if red_yes:
        tier       = "HIGH"
        confidence = "Confirmed"
        triggered  = red_yes
    elif amber_yes:
        tier       = "MEDIUM"
        confidence = "Confirmed"
        triggered  = amber_yes
    elif amber_prov:
        tier       = "MEDIUM"
        confidence = "Provisional"
        triggered  = amber_prov
    else:
        tier       = "LOW"
        confidence = "Confirmed" if len(answered) >= len(ALL_FLAG_IDS) else "Provisional"
        triggered  = []

    return {
        "tier":            tier,
        "confidence":      confidence,
        "triggered_flags": triggered,
        "flags_answered":  len(answered),
        "flags_total":     len(ALL_FLAG_IDS),
    }


def migrate_impact_flags(checklist_state_json: str | None) -> dict:
    """
    Migrate a v1 checklist_state_json (6-flag boolean) to the new v2 9-flag 4-state structure.
    Safe to run multiple times (idempotent).
    Returns the new deserialized state dict (not JSON string).
    """
    state = deserialize_impact_checklist_state(checklist_state_json)
    # deserialize_impact_checklist_state already handles v1→v2 migration internally
    return state


def build_impact_legacy_payload(state: dict | None) -> dict:
    normalized = deserialize_impact_checklist_state(state)
    summary = summarize_impact_checklist_state(normalized)
    red_yes = [
        flag["question"] for flag in summary["flags"]
        if flag["type"] == "RED" and flag["answer"] == "YES"
    ]
    amber_yes = [
        flag["question"] for flag in summary["flags"]
        if flag["type"] == "AMBER" and flag["answer"] == "YES"
    ]

    operational_note = "\n".join([
        f"Classification: {summary['classification']} ({summary['confidence']})",
        f"Rule: {summary['rule']}",
        f"Chemical Name: {summary['chemical_name'] or 'Not specified'}",
    ])
    safety_note = "Red flags YES: " + (", ".join(red_yes) if red_yes else "None")
    supply_note = "Amber flags YES: " + (", ".join(amber_yes) if amber_yes else "None")

    return {
        "operational_impact_score": summary["red_yes_count"],
        "safety_environment_score": summary["amber_yes_count"],
        "supply_risk_score": summary["yes_total"],
        "no_substitute_flag": summary["red_yes_count"] > 0,
        "impact_score_total": summary["score"],
        "impact_grade": summary["grade"],
        "operational_note": operational_note,
        "safety_environment_note": safety_note,
        "supply_risk_note": supply_note,
        "checklist_state_json": json.dumps(normalized),
        "checklist_summary": summary,
    }
