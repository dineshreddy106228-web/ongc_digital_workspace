"""Master Data service – import, upsert, and query material master records.

Public API
----------
import_master_data_xlsx(file_bytes, user_id)
    Parse an uploaded Master_data workbook and upsert every row.
    Returns a dict: {imported, updated, inserted, skipped, errors, extra_columns}

get_all_master_data()
    Return all MaterialMaster rows as a list of dicts.

get_master_record(material)
    Return a single MaterialMaster row as a dict, or None.

get_extra_column_keys()
    Return the sorted union of all extra_data keys across all rows.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

logger = logging.getLogger(__name__)


# ── Column mappings ───────────────────────────────────────────────────────────

# Maps normalised Excel header → MaterialMaster field name.
# Matching is case-insensitive and strip-whitespace-tolerant.
# Must stay in sync with MASTER_DATA_COLUMN_MAP in material_master.py.
_CORE_COLUMN_MAP: dict[str, str] = {
    "material":                        "material",                      # PK
    "short text":                      "short_text",
    "group":                           "group",
    "type":                            "material_type",
    "centralization":                  "centralization",
    "physical state":                  "physical_state",
    "volatility":                      "volatility_ambient_temperature",
    "volatility at ambient temperature": "volatility_ambient_temperature",
    "sunlight sensitivity":            "sunlight_sensitivity_up_to_50c",
    "sunlight sensitivity (up to 50degc atmospheric temperature)": "sunlight_sensitivity_up_to_50c",
    "moisture sensitivity":            "moisture_sensitivity",
    "temperature sensitivity":         "refrigeration_required",
    "refrigeration required (yes / no)": "refrigeration_required",
    "reactivity":                      "reactivity",
    "flammable":                       "flammable",
    "toxic":                           "toxic",
    "corrosive":                       "corrosive",
    "storage conditions \u2013 general":  "storage_conditions_general",   # –
    "storage conditions \u2013 special":  "storage_conditions_special",
    "container type":                  "container_type",
    "container capacity":              "container_capacity",
    "container description":           "container_description",
    "primary storage classification":  "primary_storage_classification",
}

_MATERIAL_HEADER = "material"   # normalised PK header


def _normalise_header(h: Any) -> str:
    return str(h).strip().lower() if h is not None else ""


# ── Import ────────────────────────────────────────────────────────────────────

def import_master_data_xlsx(
    file_bytes: bytes,
    user_id: int | None = None,
) -> dict:
    """Parse *file_bytes* (an xlsx workbook) and upsert MaterialMaster rows.

    Returns
    -------
    dict with keys:
        imported      – total rows processed without error
        updated       – rows that already existed and were updated
        inserted      – rows that were newly inserted
        skipped       – rows skipped (missing material number)
        errors        – list of short error strings for problem rows
        extra_columns – list of column names that went into extra_data
    """
    import pandas as pd
    from app.extensions import db
    from app.models.inventory.material_master import MaterialMaster

    result: dict = {
        "imported": 0,
        "updated": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": [],
        "extra_columns": [],
    }

    try:
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=0, dtype=str)
    except Exception as exc:
        raise ValueError(f"Could not read the Excel file: {exc}") from exc

    if df.empty:
        raise ValueError("The uploaded workbook appears to be empty.")

    # Strip column names
    df.columns = [str(c).strip() for c in df.columns]

    # Build header → field mapping (case-insensitive, whitespace-tolerant)
    normalised_headers = {_normalise_header(c): c for c in df.columns}

    # Verify the key column exists
    if _MATERIAL_HEADER not in normalised_headers:
        raise ValueError(
            "Could not find a 'Material' column in the uploaded file. "
            "Check the header row and try again."
        )

    raw_key_col = normalised_headers[_MATERIAL_HEADER]

    # Determine which columns are core vs. extra
    extra_columns: list[str] = []
    col_mapping: dict[str, str] = {}   # raw_col → model field OR "__extra__"

    for raw_col in df.columns:
        norm = _normalise_header(raw_col)
        if norm == _MATERIAL_HEADER:
            continue  # handled separately as PK
        if norm in _CORE_COLUMN_MAP and norm != _MATERIAL_HEADER:
            col_mapping[raw_col] = _CORE_COLUMN_MAP[norm]
        else:
            col_mapping[raw_col] = "__extra__"
            extra_columns.append(raw_col)

    result["extra_columns"] = extra_columns

    now = datetime.now(timezone.utc)

    for idx, row in df.iterrows():
        raw_val = row.get(raw_key_col)
        if not raw_val or (isinstance(raw_val, float) and raw_val != raw_val):
            result["skipped"] += 1
            continue

        material = str(raw_val).strip()
        if not material or material.lower() == "nan":
            result["skipped"] += 1
            continue

        # Skip summary / total rows — SAP material codes never contain spaces
        if " " in material:
            result["skipped"] += 1
            logger.debug("Skipping non-material row: %r", material)
            continue

        try:
            # Build core field updates and extra bucket
            core_fields: dict[str, str] = {}
            extra_data: dict[str, str] = {}

            for raw_col, field in col_mapping.items():
                val = row.get(raw_col)
                val_str = (
                    ""
                    if (val is None or (isinstance(val, float) and val != val))
                    else str(val).strip()
                )
                if field == "__extra__":
                    extra_data[raw_col] = val_str
                else:
                    core_fields[field] = val_str

            existing = MaterialMaster.query.get(material)
            if existing:
                for field, val in core_fields.items():
                    setattr(existing, field, val or None)
                # Merge extra_data: preserve existing keys, update/add new ones
                merged_extra = dict(existing.extra_data or {})
                merged_extra.update(extra_data)
                existing.extra_data = merged_extra
                existing.updated_at = now
                existing.updated_by = user_id
                result["updated"] += 1
            else:
                record = MaterialMaster(
                    material=material,
                    updated_at=now,
                    updated_by=user_id,
                    extra_data=extra_data or {},
                    **{k: v or None for k, v in core_fields.items()},
                )
                db.session.add(record)
                result["inserted"] += 1

            result["imported"] += 1

        except Exception as exc:
            logger.warning("Error processing master data row %s: %s", idx, exc)
            result["errors"].append(f"Row {idx + 2}: {exc}")

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        raise ValueError(f"Database error saving master data: {exc}") from exc

    return result


# ── Query helpers ─────────────────────────────────────────────────────────────

def get_all_master_data() -> list[dict]:
    """Return all MaterialMaster rows sorted by material number."""
    from app.models.inventory.material_master import MaterialMaster
    rows = MaterialMaster.query.order_by(MaterialMaster.material).all()
    return [r.to_dict() for r in rows]


def get_master_record(material: str) -> dict | None:
    """Return a single MaterialMaster row as a dict, or None if not found."""
    from app.models.inventory.material_master import MaterialMaster
    row = MaterialMaster.query.get(str(material).strip())
    return row.to_dict() if row else None


def get_extra_column_keys() -> list[str]:
    """Return the sorted union of all extra_data keys across all rows."""
    from app.models.inventory.material_master import MaterialMaster
    rows = MaterialMaster.query.with_entities(MaterialMaster.extra_data).all()
    keys: set[str] = set()
    for (extra,) in rows:
        if extra:
            keys.update(extra.keys())
    return sorted(keys)
