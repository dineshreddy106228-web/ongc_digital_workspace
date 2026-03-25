from __future__ import annotations

"""CSC <-> inventory master-data bridge helpers."""

from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path
import re

from app.models.csc.draft import CSCDraft
from app.models.csc.section import CSCSection
from app.models.csc.spec_version import CSCSpecVersion
from app.models.inventory.material_master import MaterialMaster
from app.core.services.csc_utils import (
    deserialize_impact_checklist_state,
    summarize_impact_checklist_state,
)


MASTER_DATA_FIELDS = [
    ("short_text", "Short Text"),
    ("group", "Group"),
    ("material_type", "Type"),
    ("centralization", "Centralization"),
    ("physical_state", "Physical State"),
    ("volatility_ambient_temperature", "Volatility at Ambient Temperature"),
    ("sunlight_sensitivity_up_to_50c", "Sunlight Sensitivity (up to 50degC atmospheric temperature)"),
    ("moisture_sensitivity", "Moisture Sensitivity"),
    ("refrigeration_required", "Refrigeration required (Yes / No)"),
    ("reactivity", "Reactivity"),
    ("flammable", "Flammable"),
    ("toxic", "Toxic"),
    ("corrosive", "Corrosive"),
    ("storage_conditions_general", "Storage Conditions - General"),
    ("storage_conditions_special", "Storage Conditions - Special"),
    ("container_type", "Packing Type 1"),
    ("container_capacity", "Packing Size 1"),
    ("container_description", "Packing Description 1"),
    ("container_type_2", "Packing Type 2"),
    ("container_capacity_2", "Packing Size 2"),
    ("container_description_2", "Packing Description 2"),
    ("container_type_3", "Packing Type 3"),
    ("container_capacity_3", "Packing Size 3"),
    ("container_description_3", "Packing Description 3"),
    ("container_type_4", "Packing Type 4"),
    ("container_capacity_4", "Packing Size 4"),
    ("container_description_4", "Packing Description 4"),
    ("primary_storage_classification", "Primary Storage Classification"),
]

MASTER_EXTRA_FIELDS = [
    ("Density", "Density"),
]

MASTER_IMPACT_FIELDS = [
    ("impact_flag_1", "Impact Flag 1 — HSE / Hazard"),
    ("impact_flag_2", "Impact Flag 2 — Op-Acute"),
    ("impact_flag_3", "Impact Flag 3 — Op-Chronic"),
    ("impact_flag_4", "Impact Flag 4 — Safety-critical"),
    ("impact_flag_5", "Impact Flag 5 — Environment"),
    ("impact_flag_6", "Impact Flag 6 — Regulatory"),
    ("impact_flag_7", "Impact Flag 7 — Supply Chain"),
    ("impact_flag_8", "Impact Flag 8 — Seasonal"),
    ("impact_flag_9", "Impact Flag 9 — Financial"),
    ("impact_classification", "Impact Classification"),
    ("impact_confidence", "Impact Confidence"),
]

MASTER_GROUP_OPTIONS = [
    "Demulsifier",
    "Low Temperature Demulsifier",
    "Weighing Agent",
    "Flow Improver",
    "Corrosion Inhibitor",
    "Scale Inhibitor",
    "Biocide",
    "Hydrate Inhibitor",
    "Paraffin Inhibitor",
    "Asphaltene Dispersant",
    "Surfactant",
    "Defoamer",
    "Viscosifier",
    "Clay Stabilizer",
    "Mutual Solvent",
]

MASTER_TYPE_OPTIONS = [
    "Performance",
    "Other",
]

MASTER_CENTRALIZATION_OPTIONS = [
    "Yes",
    "No",
]

MATERIAL_CLASSIFICATION_SOURCE_PATH = Path(
    "/Users/dineshreddy/Downloads/Chemical_Properties_Register_Version_1.xlsx"
)

STORAGE_CONDITIONS_GENERAL_DEFAULT = (
    "Store in a cool, dry, well-ventilated place away from moisture, heat, and direct sunlight"
)
STAGED_MASTER_SECTION_NAME = "__workflow_master_data_json__"
_LEGACY_MASTER_FIELD_ALIASES = {
    "volatility": "volatility_ambient_temperature",
    "sunlight_sensitivity": "sunlight_sensitivity_up_to_50c",
    "temperature_sensitivity": "refrigeration_required",
}

MATERIAL_PROPERTIES_FIELDS = [
    {
        "field_name": "physical_state",
        "label": "Physical State",
        "section": "Properties & Stability",
        "dimension": "Material Form",
        "source": "Excel Validation",
    },
    {
        "field_name": "volatility_ambient_temperature",
        "label": "Volatility at Ambient Temperature",
        "section": "Properties & Stability",
        "dimension": "Handling",
        "source": "Excel Validation",
    },
    {
        "field_name": "sunlight_sensitivity_up_to_50c",
        "label": "Sunlight Sensitivity (up to 50degC atmospheric temperature)",
        "section": "Properties & Stability",
        "dimension": "Storage",
        "source": "Excel Validation",
    },
    {
        "field_name": "moisture_sensitivity",
        "label": "Moisture Sensitivity",
        "section": "Properties & Stability",
        "dimension": "Storage",
        "source": "Excel Validation",
    },
    {
        "field_name": "refrigeration_required",
        "label": "Refrigeration required (Yes / No)",
        "section": "Properties & Stability",
        "dimension": "Temperature",
        "source": "Excel Validation",
        "options": ["Yes", "No"],
    },
    {
        "field_name": "reactivity",
        "label": "Reactivity",
        "section": "Properties & Stability",
        "dimension": "Stability",
        "source": "Excel Validation",
    },
    {
        "field_name": "flammable",
        "label": "Flammable",
        "section": "Hazard Profile",
        "dimension": "Hazard",
        "source": "Excel Validation",
    },
    {
        "field_name": "toxic",
        "label": "Toxic",
        "section": "Hazard Profile",
        "dimension": "Hazard",
        "source": "Excel Validation",
    },
    {
        "field_name": "corrosive",
        "label": "Corrosive",
        "section": "Hazard Profile",
        "dimension": "Hazard",
        "source": "Excel Validation",
    },
    {
        "field_name": "extra__Density",
        "label": "Density",
        "section": "Properties & Stability",
        "dimension": "Liquid Only",
        "source": "User Entry",
        "input_type": "text",
        "placeholder": "Enter density for liquid materials",
        "show_if": {"field_name": "physical_state", "equals": "Liquid"},
    },
]

STORAGE_HANDLING_FIELDS = [
    {
        "field_name": "container_type",
        "label": "Packing Type 1",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
    },
    {
        "field_name": "container_capacity",
        "label": "Packing Size 1",
        "section": "Packing",
        "dimension": "Packing",
        "source": "User Entry",
        "input_type": "text",
        "placeholder": "Enter packing size",
    },
    {
        "field_name": "container_description",
        "label": "Packing Description 1",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
    },
    {
        "field_name": "container_type_2",
        "label": "Packing Type 2",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
        "option_source_field": "container_type",
    },
    {
        "field_name": "container_capacity_2",
        "label": "Packing Size 2",
        "section": "Packing",
        "dimension": "Packing",
        "source": "User Entry",
        "input_type": "text",
        "placeholder": "Enter packing size",
    },
    {
        "field_name": "container_description_2",
        "label": "Packing Description 2",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
        "option_source_field": "container_description",
    },
    {
        "field_name": "container_type_3",
        "label": "Packing Type 3",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
        "option_source_field": "container_type",
    },
    {
        "field_name": "container_capacity_3",
        "label": "Packing Size 3",
        "section": "Packing",
        "dimension": "Packing",
        "source": "User Entry",
        "input_type": "text",
        "placeholder": "Enter packing size",
    },
    {
        "field_name": "container_description_3",
        "label": "Packing Description 3",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
        "option_source_field": "container_description",
    },
    {
        "field_name": "container_type_4",
        "label": "Packing Type 4",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
        "option_source_field": "container_type",
    },
    {
        "field_name": "container_capacity_4",
        "label": "Packing Size 4",
        "section": "Packing",
        "dimension": "Packing",
        "source": "User Entry",
        "input_type": "text",
        "placeholder": "Enter packing size",
    },
    {
        "field_name": "container_description_4",
        "label": "Packing Description 4",
        "section": "Packing",
        "dimension": "Packing",
        "source": "Excel Validation",
        "option_source_field": "container_description",
    },
    {
        "field_name": "storage_conditions_general",
        "label": "Storage Conditions - General",
        "section": "Storage",
        "dimension": "Default Guidance",
        "source": "Default",
        "input_type": "textarea",
        "default_value": STORAGE_CONDITIONS_GENERAL_DEFAULT,
        "placeholder": "General storage guidance",
    },
    {
        "field_name": "storage_conditions_special",
        "label": "Storage Conditions - Special",
        "section": "Storage",
        "dimension": "Special Instructions",
        "source": "User Entry",
        "input_type": "textarea",
        "placeholder": "Enter any special storage handling requirements",
    },
    {
        "field_name": "primary_storage_classification",
        "label": "Primary Storage Classification",
        "section": "Storage",
        "dimension": "Zone",
        "source": "Excel Validation",
    },
]


def _collect_master_record_field_names() -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    for field_name, _ in MASTER_DATA_FIELDS:
        if field_name in seen:
            continue
        names.append(field_name)
        seen.add(field_name)

    for field in MATERIAL_PROPERTIES_FIELDS + STORAGE_HANDLING_FIELDS:
        field_name = str(field["field_name"])
        if field_name.startswith("extra__") or field_name in seen:
            continue
        names.append(field_name)
        seen.add(field_name)

    return names


MASTER_RECORD_FIELD_NAMES = _collect_master_record_field_names()

WORKFLOW_MASTER_VALUE_KEYS = {
    "material_code",
    "short_text",
}
WORKFLOW_MASTER_VALUE_KEYS.update(
    field_name for field_name in MASTER_RECORD_FIELD_NAMES if field_name != "short_text"
)
WORKFLOW_MASTER_VALUE_KEYS.update(
    f"extra__{field_name}" for field_name, _ in MASTER_EXTRA_FIELDS
)
WORKFLOW_MASTER_VALUE_KEYS.update(
    field_name for field_name, _ in MASTER_IMPACT_FIELDS
)

WORKFLOW_MANAGED_MASTER_FIELD_NAMES = {
    str(field["field_name"])
    for field in MATERIAL_PROPERTIES_FIELDS + STORAGE_HANDLING_FIELDS
}
WORKFLOW_MANAGED_MASTER_FIELD_NAMES.update(
    f"extra__{extra_key}" for extra_key, _ in MASTER_EXTRA_FIELDS
)
WORKFLOW_MANAGED_MASTER_FIELD_NAMES.update(
    impact_key for impact_key, _ in MASTER_IMPACT_FIELDS
)

ADMIN_MASTER_FIELDS = [
    (field_name, label)
    for field_name, label in MASTER_DATA_FIELDS
    if field_name not in WORKFLOW_MANAGED_MASTER_FIELD_NAMES
]

_ADMIN_MASTER_FIELD_OVERRIDES = {
    "group": {
        "label": "Group",
        "input_type": "suggest",
        "placeholder": "Enter group",
        "hint": "Examples: Demulsifier, Low Temperature Demulsifier, Weighing Agent, Flow Improver, Other.",
        "options": MASTER_GROUP_OPTIONS + ["Other"],
    },
    "material_type": {
        "label": "Performance / Other",
        "input_type": "select",
        "options": MASTER_TYPE_OPTIONS,
    },
    "centralization": {
        "label": "Centralization for Procurement (Yes / No)",
        "input_type": "select",
        "options": MASTER_CENTRALIZATION_OPTIONS,
    },
}

ADMIN_MASTER_FIELD_CONFIGS = [
    {
        "field_name": field_name,
        "label": _ADMIN_MASTER_FIELD_OVERRIDES.get(field_name, {}).get("label", label),
        "input_type": _ADMIN_MASTER_FIELD_OVERRIDES.get(field_name, {}).get("input_type", "text"),
        "options": list(_ADMIN_MASTER_FIELD_OVERRIDES.get(field_name, {}).get("options", [])),
        "placeholder": _ADMIN_MASTER_FIELD_OVERRIDES.get(field_name, {}).get("placeholder", ""),
        "hint": _ADMIN_MASTER_FIELD_OVERRIDES.get(field_name, {}).get("hint", ""),
    }
    for field_name, label in ADMIN_MASTER_FIELDS
]

ADMIN_MASTER_EXTRA_FIELDS = [
    (field_name, label)
    for field_name, label in MASTER_EXTRA_FIELDS
    if f"extra__{field_name}" not in WORKFLOW_MANAGED_MASTER_FIELD_NAMES
]

_MATERIAL_CLASSIFICATION_FALLBACK_OPTIONS = {
    "physical_state": ["Liquid", "Solid", "Gas"],
    "volatility_ambient_temperature": ["Yes", "No"],
    "sunlight_sensitivity_up_to_50c": ["Yes", "No"],
    "moisture_sensitivity": ["Yes", "No"],
    "refrigeration_required": ["Yes", "No"],
    "reactivity": ["Stable", "High"],
    "flammable": ["Yes", "No"],
    "toxic": ["Yes", "No"],
    "corrosive": ["Yes", "No"],
    "container_type": [
        "HDPE Drum",
        "Metal Drum",
        "Cylinder",
        "Jerrycan",
        "IBC",
        "Jumbo Bags",
        "Cardboard Box",
        "Tanker",
        "HDPE Containers",
        "HDPE Carbuoys",
        "Polythene Bags in MS Drums",
        "HDPE Bags",
        "Multi Walled Paper Bags",
        "Jute Bags",
        "Original Packing",
    ],
    "container_type_2": [],
    "container_type_3": [],
    "container_type_4": [],
    "container_description": [
        "New Jute bags strong enough to withstand rigours of transit and storage",
        "Multi walled new paper bags with at least two innermost layers, suitably water proofed, strong enough to withstand rigours of transit and storage",
        "New HDPE bag with moisture proof inner insert of polythene, strong enough to withstand rigours of transit and storage",
        "only HDPE Bags without inner lining",
        "New HDPE carbuoy with air tight lid, strong enough to withstand rigours of transit and storage",
        "New HDPE container with air tight lid, strong enough to withstand rigours of transit and storage",
        "New HDPE drums with leak proof stoppers, strong enough to withstand rigours of transit and storage",
        "Leak proof, sealed new M. S. Drums strong enough to withstand rigours of transit and storage",
        "New moisture proof polythene bag, which in turn is placed in air tight new M.S.Drum (with full mouth opening), strong enough to withstand rigours of transit and storage",
    ],
    "container_description_2": [],
    "container_description_3": [],
    "container_description_4": [],
    "primary_storage_classification": [
        "Zone-1: General Stable",
        "Zone-2: Environment Sensitive",
        "Zone-3: Flammable / Volatile",
        "Zone-4: Corrosive / Reactive / Toxic",
        "Zone-5: Special / Controlled",
    ],
}

CSC_ADMIN_DRAFT_FIELDS = [
    {
        "field_name": "id",
        "label": "Draft ID",
        "input_type": "display",
        "readonly": True,
        "help_text": "System primary key for the CSC draft.",
    },
    {
        "field_name": "spec_number",
        "label": "Specification Number",
        "input_type": "text",
    },
    {
        "field_name": "chemical_name",
        "label": "Chemical Name",
        "input_type": "text",
    },
    {
        "field_name": "committee_name",
        "label": "Committee Name",
        "input_type": "text",
    },
    {
        "field_name": "meeting_date",
        "label": "Meeting Date",
        "input_type": "text",
    },
    {
        "field_name": "prepared_by",
        "label": "Prepared By",
        "input_type": "text",
    },
    {
        "field_name": "reviewed_by",
        "label": "Reviewed By",
        "input_type": "text",
    },
    {
        "field_name": "material_code",
        "label": "Material Code",
        "input_type": "text",
        "readonly_when_present": True,
        "help_text": "Primary key of the material master table. It can be set only when blank.",
    },
    {
        "field_name": "status",
        "label": "Status",
        "input_type": "display",
        "readonly": True,
        "help_text": "Workflow-controlled. Published specs move to Drafting only through Manage Drafts.",
    },
    {
        "field_name": "created_by_role",
        "label": "Created By Role",
        "input_type": "text",
    },
    {
        "field_name": "is_admin_draft",
        "label": "Admin Draft",
        "input_type": "boolean",
    },
    {
        "field_name": "phase1_locked",
        "label": "Drafting Open",
        "input_type": "display",
        "readonly": True,
        "help_text": "Shown for reference only in admin master-data edit.",
    },
    {
        "field_name": "parent_draft_id",
        "label": "Parent Draft ID",
        "input_type": "display",
        "readonly": True,
    },
    {
        "field_name": "admin_stage",
        "label": "Workflow Stage",
        "input_type": "display",
        "readonly": True,
    },
    {
        "field_name": "spec_version",
        "label": "Specification Version",
        "input_type": "display",
        "readonly": True,
        "help_text": "Version is changed from Committee Workbench approval.",
    },
    {
        "field_name": "created_by_id",
        "label": "Created By User ID",
        "input_type": "number",
    },
]

MATERIAL_CODE_RE = re.compile(r"\b\d{6,}\b")


def _as_text(value) -> str:
    return str(value).strip() if value is not None else ""


def _normalize_material_code(value) -> str:
    raw = _as_text(value)
    if not raw:
        return ""

    match = MATERIAL_CODE_RE.search(raw)
    if match:
        return match.group(0)

    if len(raw) <= 50 and all(ch.isalnum() or ch in "-_/." for ch in raw):
        return raw

    return ""


def _normalize_validation_header(value: str | None) -> str:
    text = _as_text(value).lower()
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text)
    return text


@lru_cache(maxsize=1)
def _get_workbook_validation_options() -> dict[str, list[str]]:
    """Return workbook-backed option lists keyed by field name."""
    options_by_field = {key: list(values) for key, values in _MATERIAL_CLASSIFICATION_FALLBACK_OPTIONS.items()}

    try:
        from openpyxl import load_workbook
        from openpyxl.utils import range_boundaries

        workbook = load_workbook(MATERIAL_CLASSIFICATION_SOURCE_PATH, data_only=False)
        worksheet = workbook["Chemical Properties"]
        normalized_to_field = {
            _normalize_validation_header("Physical State"): "physical_state",
            _normalize_validation_header("Volatility"): "volatility_ambient_temperature",
            _normalize_validation_header("Volatility at Ambient Temperature"): "volatility_ambient_temperature",
            _normalize_validation_header("Sunlight Sensitivity"): "sunlight_sensitivity_up_to_50c",
            _normalize_validation_header("Sunlight Sensitivity (up to 50degC atmospheric temperature)"): "sunlight_sensitivity_up_to_50c",
            _normalize_validation_header("Moisture Sensitivity"): "moisture_sensitivity",
            _normalize_validation_header("Temperature Sensitivity"): "refrigeration_required",
            _normalize_validation_header("Refrigeration required (Yes / No)"): "refrigeration_required",
            _normalize_validation_header("Reactivity"): "reactivity",
            _normalize_validation_header("Flammable"): "flammable",
            _normalize_validation_header("Toxic"): "toxic",
            _normalize_validation_header("Corrosive"): "corrosive",
            _normalize_validation_header("Container Type"): "container_type",
            _normalize_validation_header("Container Description"): "container_description",
            _normalize_validation_header("Primary Storage Classification"): "primary_storage_classification",
        }

        headers = {
            column_index: _normalize_validation_header(cell.value)
            for column_index, cell in enumerate(worksheet[1], start=1)
        }

        def resolve_formula_values(formula: str | None) -> list[str]:
            if not formula:
                return []
            if formula.startswith('"') and formula.endswith('"'):
                return [item.strip() for item in formula[1:-1].split(",") if item.strip()]

            named_range = formula[1:] if formula.startswith("=") else formula
            defined_name = workbook.defined_names.get(named_range)
            if defined_name is not None:
                values: list[str] = []
                for sheet_name, coord in defined_name.destinations:
                    cells = workbook[sheet_name][coord]
                    if isinstance(cells, tuple):
                        for row in cells:
                            for cell in row:
                                if cell.value not in (None, ""):
                                    values.append(str(cell.value).strip())
                    elif cells.value not in (None, ""):
                        values.append(str(cells.value).strip())
                return values
            return []

        for validation in worksheet.data_validations.dataValidation:
            option_values = resolve_formula_values(validation.formula1)
            if not option_values:
                continue
            for rng in str(validation.sqref).split():
                min_col, _, max_col, _ = range_boundaries(rng)
                for column in range(min_col, max_col + 1):
                    field_name = normalized_to_field.get(headers.get(column, ""))
                    if field_name:
                        options_by_field[field_name] = option_values
    except Exception:
        pass

    return options_by_field


def _build_field_definitions(fields: list[dict[str, object]]) -> list[dict[str, object]]:
    options_by_field = _get_workbook_validation_options()
    rows = []
    for field in fields:
        option_source_field = str(field.get("option_source_field") or field["field_name"])
        rows.append({
            **field,
            "input_type": field.get("input_type", "options"),
            "options": list(field.get("options", options_by_field.get(option_source_field, []))),
            "default_value": field.get("default_value", ""),
            "placeholder": field.get("placeholder", ""),
            "show_if": field.get("show_if"),
        })
    return rows


@lru_cache(maxsize=1)
def get_material_properties_fields() -> list[dict[str, object]]:
    """Return workbook-backed material properties field definitions."""
    return _build_field_definitions(MATERIAL_PROPERTIES_FIELDS)


@lru_cache(maxsize=1)
def get_storage_handling_fields() -> list[dict[str, object]]:
    """Return workbook-backed storage and handling field definitions."""
    return _build_field_definitions(STORAGE_HANDLING_FIELDS)


def get_master_record_for_draft(draft: CSCDraft) -> MaterialMaster | None:
    """Return the linked material-master record for a CSC draft."""
    material_code = _normalize_material_code(getattr(draft, "material_code", ""))
    if not material_code:
        return None
    return MaterialMaster.query.get(material_code)


def _base_master_form_values(draft: CSCDraft) -> dict[str, str]:
    record = get_master_record_for_draft(draft)
    extra = dict(record.extra_data or {}) if record else {}
    values = {
        "material_code": _normalize_material_code(getattr(draft, "material_code", "")),
        "short_text": _as_text(record.short_text if record else draft.chemical_name),
    }
    for field_name in MASTER_RECORD_FIELD_NAMES:
        if field_name == "short_text":
            continue
        values[field_name] = _as_text(getattr(record, field_name, ""))
    if not values.get("storage_conditions_general"):
        values["storage_conditions_general"] = STORAGE_CONDITIONS_GENERAL_DEFAULT
    for extra_key, _ in MASTER_EXTRA_FIELDS:
        values[f"extra__{extra_key}"] = _as_text(extra.get(extra_key, ""))
    for impact_key, _ in MASTER_IMPACT_FIELDS:
        values[impact_key] = _as_text(extra.get(impact_key, ""))
    return values


def _get_staged_master_section(draft: CSCDraft) -> CSCSection | None:
    if not getattr(draft, "id", None) or not getattr(draft, "parent_draft_id", None):
        return None
    return CSCSection.query.filter_by(
        draft_id=draft.id,
        section_name=STAGED_MASTER_SECTION_NAME,
    ).first()


def _load_staged_master_payload(draft: CSCDraft) -> dict[str, str]:
    section = _get_staged_master_section(draft)
    if section is None or not (section.section_text or "").strip():
        return {}
    try:
        payload = json.loads(section.section_text or "{}")
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized_payload: dict[str, str] = {}
    for key, value in payload.items():
        normalized_key = _LEGACY_MASTER_FIELD_ALIASES.get(key, key)
        if normalized_key in WORKFLOW_MASTER_VALUE_KEYS:
            normalized_payload[normalized_key] = _as_text(value)
    return normalized_payload


def _write_staged_master_payload(draft: CSCDraft, payload: dict[str, str]) -> dict[str, str]:
    from app.extensions import db

    clean_payload: dict[str, str] = {}
    for key, value in payload.items():
        normalized_key = _LEGACY_MASTER_FIELD_ALIASES.get(key, key)
        if normalized_key in WORKFLOW_MASTER_VALUE_KEYS:
            clean_payload[normalized_key] = _as_text(value)
    if not clean_payload.get("storage_conditions_general"):
        clean_payload["storage_conditions_general"] = STORAGE_CONDITIONS_GENERAL_DEFAULT
    if _as_text(clean_payload.get("physical_state")) != "Liquid":
        clean_payload["extra__Density"] = ""

    section = _get_staged_master_section(draft)
    if section is None:
        section = CSCSection(
            draft_id=draft.id,
            section_name=STAGED_MASTER_SECTION_NAME,
            sort_order=9999,
        )
    section.section_text = json.dumps(clean_payload)
    section.sort_order = 9999
    if section not in db.session:
        db.session.add(section)
    return clean_payload


def clear_staged_master_payload(draft: CSCDraft) -> None:
    section = _get_staged_master_section(draft)
    if section is None:
        return
    from app.extensions import db

    db.session.delete(section)


def get_master_form_values(draft: CSCDraft) -> dict[str, str]:
    """Return editable master-data values for the given draft."""
    values = _base_master_form_values(draft)
    if getattr(draft, "parent_draft_id", None):
        values.update(_load_staged_master_payload(draft))
        if not values.get("material_code"):
            values["material_code"] = _normalize_material_code(getattr(draft, "material_code", ""))
    if not values.get("storage_conditions_general"):
        values["storage_conditions_general"] = STORAGE_CONDITIONS_GENERAL_DEFAULT
    return values


def get_material_properties_values(draft: CSCDraft) -> dict[str, str]:
    """Return workbook-backed material properties values."""
    values = get_master_form_values(draft)
    return {
        field["field_name"]: _as_text(values.get(field["field_name"], ""))
        for field in get_material_properties_fields()
    }


def get_storage_handling_values(draft: CSCDraft) -> dict[str, str]:
    """Return storage and handling values."""
    values = get_master_form_values(draft)
    return {
        field["field_name"]: _as_text(values.get(field["field_name"], field.get("default_value", "")))
        for field in get_storage_handling_fields()
    }


def upsert_master_record_from_form(
    draft: CSCDraft,
    form_data,
    user_id: int | None = None,
) -> MaterialMaster | None:
    """Create or update the linked material-master row from a CSC form submission."""
    if getattr(draft, "parent_draft_id", None):
        current = get_master_form_values(draft)
        for key in WORKFLOW_MASTER_VALUE_KEYS:
            if key in form_data:
                current[key] = _as_text(form_data.get(key))
        if "material_code" in form_data:
            current["material_code"] = _normalize_material_code(form_data.get("material_code"))
        current = _write_staged_master_payload(draft, current)
        draft.material_code = current.get("material_code") or None
        return None

    previous_material_code = _normalize_material_code(getattr(draft, "material_code", ""))
    material_code = _normalize_material_code(form_data.get("material_code"))
    if not material_code:
        draft.material_code = None
        return None

    previous_record = None
    if previous_material_code:
        previous_record = MaterialMaster.query.get(previous_material_code)
    target_record = MaterialMaster.query.get(material_code)

    record = target_record or previous_record

    if record is None:
        record = MaterialMaster(material=material_code)

    record.material = material_code
    if "short_text" in form_data or "chemical_name" in form_data:
        record.short_text = (
            _as_text(form_data.get("short_text"))
            or _as_text(form_data.get("chemical_name"))
            or record.short_text
        )

    for field_name in MASTER_RECORD_FIELD_NAMES:
        if field_name == "short_text":
            continue
        if field_name in form_data:
            setattr(record, field_name, _as_text(form_data.get(field_name)) or None)

    extra_data = dict(record.extra_data or {})
    for extra_key, _ in MASTER_EXTRA_FIELDS:
        form_key = f"extra__{extra_key}"
        if form_key in form_data:
            extra_data[extra_key] = _as_text(form_data.get(form_key))
    for impact_key, _ in MASTER_IMPACT_FIELDS:
        if impact_key in form_data:
            extra_data[impact_key] = _as_text(form_data.get(impact_key))
    record.extra_data = extra_data
    record.updated_at = datetime.now(timezone.utc)
    record.updated_by = user_id
    draft.material_code = material_code

    if (
        previous_record is not None
        and target_record is not None
        and previous_record is not target_record
        and previous_material_code != material_code
    ):
        # Material code changed to an already-existing master row. The target row
        # remains authoritative, so remove the superseded source row.
        from app.extensions import db

        db.session.delete(previous_record)

    return record


def update_material_properties_from_form(
    draft: CSCDraft,
    form_data: dict,
    user_id: int | None = None,
) -> MaterialMaster | None:
    """Update only workbook-driven material properties fields."""
    if getattr(draft, "parent_draft_id", None):
        current = get_master_form_values(draft)
        for field in get_material_properties_fields():
            field_name = str(field["field_name"])
            if field_name in form_data:
                current[field_name] = _as_text(form_data.get(field_name))
        _write_staged_master_payload(draft, current)
        return None

    material_code = _normalize_material_code(getattr(draft, "material_code", ""))
    if not material_code:
        return None

    record = MaterialMaster.query.get(material_code)
    if record is None:
        record = MaterialMaster(material=material_code)

    record.material = material_code
    if not _as_text(record.short_text):
        record.short_text = _as_text(getattr(draft, "chemical_name", ""))

    for field in get_material_properties_fields():
        field_name = str(field["field_name"])
        if field_name in form_data:
            if field_name.startswith("extra__"):
                continue
            setattr(record, field_name, _as_text(form_data.get(field_name)) or None)

    extra_data = dict(record.extra_data or {})
    physical_state = _as_text(form_data.get("physical_state") if "physical_state" in form_data else record.physical_state)
    if physical_state == "Liquid":
        extra_data["Density"] = _as_text(form_data.get("extra__Density"))
    else:
        extra_data["Density"] = ""
    record.extra_data = extra_data

    record.updated_at = datetime.now(timezone.utc)
    record.updated_by = user_id
    return record


def update_storage_handling_from_form(
    draft: CSCDraft,
    form_data: dict,
    user_id: int | None = None,
) -> MaterialMaster | None:
    """Update only storage and handling fields."""
    if getattr(draft, "parent_draft_id", None):
        current = get_master_form_values(draft)
        for field in get_storage_handling_fields():
            field_name = str(field["field_name"])
            if field_name in form_data or field.get("default_value"):
                value = _as_text(
                    form_data.get(field_name, current.get(field_name, field.get("default_value", "")))
                )
                if field_name == "storage_conditions_general" and not value:
                    value = STORAGE_CONDITIONS_GENERAL_DEFAULT
                current[field_name] = value
        _write_staged_master_payload(draft, current)
        return None

    material_code = _normalize_material_code(getattr(draft, "material_code", ""))
    if not material_code:
        return None

    record = MaterialMaster.query.get(material_code)
    if record is None:
        record = MaterialMaster(material=material_code)

    record.material = material_code
    if not _as_text(record.short_text):
        record.short_text = _as_text(getattr(draft, "chemical_name", ""))

    for field in get_storage_handling_fields():
        field_name = str(field["field_name"])
        value = _as_text(form_data.get(field_name, field.get("default_value", "")))
        if field_name == "storage_conditions_general" and not value:
            value = STORAGE_CONDITIONS_GENERAL_DEFAULT
        setattr(record, field_name, value or None)

    record.updated_at = datetime.now(timezone.utc)
    record.updated_by = user_id
    return record


def sync_impact_fields_to_master_record(
    draft: CSCDraft,
    checklist_state: dict | str | None,
    user_id: int | None = None,
) -> MaterialMaster | None:
    """Mirror impact checklist outputs into the linked material master row."""
    state = deserialize_impact_checklist_state(checklist_state, getattr(draft, "chemical_name", ""))
    summary = summarize_impact_checklist_state(state)

    def _flag_display(val):
        """Convert 4-state value to a displayable string for master record."""
        if val in ("YES", "PROVISIONAL", "REVIEW", "NO"):
            return val
        # Legacy boolean path (shouldn't occur after migration)
        if val is True:
            return "YES"
        if val is False:
            return "NO"
        return ""

    if getattr(draft, "parent_draft_id", None):
        current = get_master_form_values(draft)
        for index, flag in enumerate(summary["flags"], start=1):
            current[f"impact_flag_{index}"] = _flag_display(flag["answer"])
        current["impact_classification"] = summary["grade"]
        current["impact_confidence"] = summary.get("confidence", "")
        _write_staged_master_payload(draft, current)
        return None

    material_code = _normalize_material_code(getattr(draft, "material_code", ""))
    if not material_code:
        return None

    record = MaterialMaster.query.get(material_code)
    if record is None:
        record = MaterialMaster(material=material_code)

    extra_data = dict(record.extra_data or {})

    for index, flag in enumerate(summary["flags"], start=1):
        extra_data[f"impact_flag_{index}"] = _flag_display(flag["answer"])

    extra_data["impact_classification"] = summary["grade"]
    extra_data["impact_confidence"] = summary.get("confidence", "")
    record.extra_data = extra_data
    record.updated_at = datetime.now(timezone.utc)
    record.updated_by = user_id
    return record


def sync_master_record_from_draft(
    draft: CSCDraft,
    user_id: int | None = None,
) -> tuple[MaterialMaster | None, bool]:
    """Ensure a compatibility master-data row exists for the draft."""
    material_code = _normalize_material_code(getattr(draft, "material_code", ""))
    if not material_code:
        return None, False

    record = MaterialMaster.query.get(material_code)
    created = record is None
    if created:
        record = MaterialMaster(material=material_code)

    if not _as_text(record.short_text):
        record.short_text = _as_text(getattr(draft, "chemical_name", ""))

    record.material = material_code
    record.updated_at = datetime.now(timezone.utc)
    if user_id is not None:
        record.updated_by = user_id

    return record, created


def backfill_draft_material_codes() -> tuple[int, int]:
    """Populate CSCDraft.material_code from version payload snapshots."""
    updated = 0
    missing = 0

    drafts = CSCDraft.query.order_by(CSCDraft.id).all()
    for draft in drafts:
        if _normalize_material_code(getattr(draft, "material_code", "")):
            continue

        snapshots = draft.spec_versions.order_by(CSCSpecVersion.created_at.desc()).all()
        material_code = ""
        for snapshot in snapshots:
            try:
                payload = json.loads(snapshot.payload_json or "{}")
            except Exception:
                payload = {}
            material_code = _normalize_material_code(
                (payload.get("draft") or {}).get("material_code")
            )
            if material_code:
                break

        if material_code:
            draft.material_code = material_code
            updated += 1
        else:
            missing += 1

    return updated, missing


def sync_inventory_master_from_csc() -> dict[str, int]:
    """Create missing inventory master rows for CSC drafts with material codes."""
    created = 0
    updated = 0
    missing = 0
    duplicates = 0
    seen_codes: set[str] = set()

    drafts = CSCDraft.query.order_by(CSCDraft.id).all()
    for draft in drafts:
        material_code = _normalize_material_code(getattr(draft, "material_code", ""))
        if not material_code:
            missing += 1
            continue
        if material_code in seen_codes:
            duplicates += 1
            continue

        seen_codes.add(material_code)
        record, was_created = sync_master_record_from_draft(
            draft,
            user_id=getattr(draft, "created_by_id", None),
        )
        if record is None:
            missing += 1
            continue
        if was_created:
            created += 1
        else:
            updated += 1
        from app.extensions import db

        if record not in db.session:
            db.session.add(record)

    return {
        "created": created,
        "updated": updated,
        "missing_material_code": missing,
        "duplicate_material_code": duplicates,
        "linked_materials": len(seen_codes),
    }
