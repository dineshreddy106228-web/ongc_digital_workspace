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
    ("testing", "Testing / Evaluation Issue"),
]

# Impact sub-sections
IMPACT_SUBTYPES = [
    ("Impact::operational", "Operational Impact"),
    ("Impact::supply_chain", "Supply Chain Impact"),
    ("Impact::cost", "Cost Impact"),
    ("Impact::quality_testing", "Quality / Testing Impact"),
    ("Impact::safety_regulatory", "Safety / Regulatory Impact"),
]

# Recommendation keys
REC_MAIN_KEY = "Recommendation::main"
REC_REMARKS_KEY = "Recommendation::remarks"

PARAMETER_TYPES = ["Vital", "Essential"]
TEST_PROCEDURE_OPTIONS = [
    "ASTIM",
    "API",
    "IS",
    "Inhouse",
    "Any other National / International Standard",
]
DRAFT_STATUS_OPTIONS = ["Drafting", "Published"]
WORKFLOW_DRAFTING_OPEN = "open"
WORKFLOW_DRAFTING_SUBMITTED = "submitted"
WORKFLOW_DRAFTING_RETURNED = "returned"
WORKFLOW_DRAFTING_APPROVED = "approved"
WORKFLOW_DRAFTING_REJECTED = "rejected"
WORKFLOW_DRAFTING_STATE_OPTIONS = [
    WORKFLOW_DRAFTING_OPEN,
    WORKFLOW_DRAFTING_SUBMITTED,
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
    "essential": "Essential",
    "desirable": "Essential",
    "vital": "Vital",
    "essential-informational": "Essential",
    "essential informational": "Essential",
    "essential / informational": "Essential",
    "essential - informational": "Essential",
    "essential/informational": "Essential",
}

TEST_PROCEDURE_TYPE_ALIASES = {
    "astm": "ASTIM", "astim": "ASTIM", "api": "API", "is": "IS",
    "inhouse": "Inhouse", "in-house": "Inhouse",
    "other national / international": "Any other National / International Standard",
    "other national/international": "Any other National / International Standard",
    "other national international": "Any other National / International Standard",
    "any other national / international standard": "Any other National / International Standard",
}

IMPACT_CHECKLIST_VERSION = 1
IMPACT_CHECKLIST_FLAGS = [
    {
        "id": "flag_1",
        "order": 1,
        "dimension": "HSE / Hazard",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Is the chemical classified as Toxic, Corrosive, Flammable, or Carcinogenic in its SDS?",
        "source": "MSDS",
        "detail": "Check SDS Section 2 for GHS pictograms — skull/crossbones (toxic), flame (flammable), corrosion (corrosive), or health hazard (carcinogen). Any one of these = YES. Reference: Manufacture, Storage and Import of Hazardous Chemical Rules 1989.",
    },
    {
        "id": "flag_2",
        "order": 2,
        "dimension": "Environment",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Does disposal require hazardous waste treatment or CPCB / State PCB authorisation?",
        "source": "MSDS + CPCB Rules",
        "detail": "Check SDS Section 13 (Disposal Considerations) and cross-reference with Hazardous Waste Rules 2016 Schedule I and II. If spent chemical or container must be sent to a CPCB-authorised TSDF = YES.",
    },
    {
        "id": "flag_3",
        "order": 3,
        "dimension": "Operational",
        "type": "RED",
        "section_label": "Red flags — any YES = High Impact",
        "question": "Will production / well operation stop if this chemical is unavailable for 7 consecutive days?",
        "source": "User Department",
        "detail": "Must be confirmed in writing by the concerned Production or Drilling Engineer — not by stores or procurement. The 7-day window reflects realistic emergency procurement lead time. If a substitute can be deployed within 7 days, flag is NO. Document the engineer's name and date.",
    },
    {
        "id": "flag_4",
        "order": 4,
        "dimension": "Regulatory",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (with no red flags) = Medium Impact",
        "question": "Does storage or use require a statutory licence, DGMS approval, or OISD compliance?",
        "source": "Indentor",
        "detail": "Applicable statutes include Petroleum Act 1934, Explosives Act 1884, DGMS Circulars for drilling chemicals, and OISD-STD-109/117. If any licence or permit is required from a government authority for storage or use = YES.",
    },
    {
        "id": "flag_5",
        "order": 5,
        "dimension": "Supply Chain",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (with no red flags) = Medium Impact",
        "question": "Is there only ONE qualified vendor OR is the chemical exclusively imported?",
        "source": "Vendor Master",
        "detail": "Check vendor master for number of approved sources. If only 1 vendor is registered and no alternate has been qualified in the last 3 years = YES. Also YES if all supply is through import regardless of number of importers.",
    },
    {
        "id": "flag_6",
        "order": 6,
        "dimension": "Financial",
        "type": "AMBER",
        "section_label": "Amber flags — any YES (with no red flags) = Medium Impact",
        "question": "Expiry/wastage loss > Rs. 1 lakh in 2 years OR disposal cost > 20% of purchase price?",
        "source": "Indentor",
        "detail": "Two independent tests — either one triggers YES. Test 1: Check stores write-off register for last 24 months; if this material code appears with total value ≥ Rs. 1 lakh = YES. Test 2: Compute disposal cost divided by purchase price × 100; if ≥ 20% = YES.",
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
    return TYPE_CLASS_ALIASES.get(txt, "Essential") if txt else "Essential"


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


def _normalize_impact_flag_answer(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if int(value) == 1:
            return True
        if int(value) == 0:
            return False
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"yes", "true", "1"}:
            return True
        if token in {"no", "false", "0"}:
            return False
    return None


def build_default_impact_checklist_state(chemical_name: str | None = None) -> dict:
    return {
        "version": IMPACT_CHECKLIST_VERSION,
        "chemical_name": sanitize_text(chemical_name or "", max_length=255),
        "answers": {flag["id"]: None for flag in IMPACT_CHECKLIST_FLAGS},
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

    state["version"] = int(payload.get("version", IMPACT_CHECKLIST_VERSION) or IMPACT_CHECKLIST_VERSION)
    state["chemical_name"] = sanitize_text(
        payload.get("chemical_name", state["chemical_name"]) or state["chemical_name"],
        max_length=255,
    )

    answers = payload.get("answers")
    if not isinstance(answers, dict):
        answers = payload.get("flags")

    if isinstance(answers, dict):
        for flag in IMPACT_CHECKLIST_FLAGS:
            answer = answers.get(flag["id"])
            if isinstance(answer, dict):
                answer = answer.get("answer", answer.get("value"))
            state["answers"][flag["id"]] = _normalize_impact_flag_answer(answer)

    return state


def summarize_impact_checklist_state(state: dict | None) -> dict:
    normalized = deserialize_impact_checklist_state(state)
    red_yes_count = 0
    amber_yes_count = 0
    answered_count = 0
    flag_rows = []

    for flag in IMPACT_CHECKLIST_FLAGS:
        answer = _normalize_impact_flag_answer(normalized["answers"].get(flag["id"]))
        if answer is not None:
            answered_count += 1
        if answer is True and flag["type"] == "RED":
            red_yes_count += 1
        if answer is True and flag["type"] == "AMBER":
            amber_yes_count += 1
        flag_rows.append({
            **flag,
            "answer": answer,
            "answer_label": "YES" if answer is True else "NO" if answer is False else "PENDING",
        })

    total_flags = len(IMPACT_CHECKLIST_FLAGS)
    yes_total = red_yes_count + amber_yes_count

    if red_yes_count > 0:
        classification = "HIGH IMPACT"
        grade = "HIGH"
        rule = f"{red_yes_count} red flag(s) answered YES"
        tone = "red"
        score = 3.0
    elif amber_yes_count > 0:
        classification = "MEDIUM IMPACT"
        grade = "MEDIUM"
        rule = f"No red flags, but {amber_yes_count} amber flag(s) answered YES"
        tone = "amber"
        score = 2.0
    elif answered_count == 0:
        classification = "Pending"
        grade = "PENDING"
        rule = "Pending — answer flags above"
        tone = "pending"
        score = 0.0
    elif answered_count == total_flags:
        classification = "LOW IMPACT"
        grade = "LOW"
        rule = f"All {total_flags} flags answered NO"
        tone = "green"
        score = 1.0
    else:
        classification = "Pending"
        grade = "PENDING"
        rule = "Pending — answer remaining flags above"
        tone = "pending"
        score = 0.0

    return {
        "chemical_name": normalized["chemical_name"],
        "flags": flag_rows,
        "red_yes_count": red_yes_count,
        "amber_yes_count": amber_yes_count,
        "yes_total": yes_total,
        "answered_count": answered_count,
        "total_flags": total_flags,
        "classification": classification,
        "grade": grade,
        "rule": rule,
        "tone": tone,
        "score": score,
    }


def build_impact_legacy_payload(state: dict | None) -> dict:
    normalized = deserialize_impact_checklist_state(state)
    summary = summarize_impact_checklist_state(normalized)
    red_yes = [flag["question"] for flag in summary["flags"] if flag["type"] == "RED" and flag["answer"] is True]
    amber_yes = [flag["question"] for flag in summary["flags"] if flag["type"] == "AMBER" and flag["answer"] is True]

    operational_note = "\n".join([
        f"Classification: {summary['classification']}",
        f"Rule: {summary['rule']}",
        f"Chemical Name: {summary['chemical_name'] or 'Not specified'}",
    ])
    safety_note = "Red flags answered YES: " + (", ".join(red_yes) if red_yes else "None")
    supply_note = "Amber flags answered YES: " + (", ".join(amber_yes) if amber_yes else "None")

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
