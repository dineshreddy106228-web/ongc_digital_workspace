"""Reference-register coverage for Corporate Specifications Management.

The bundled CSV is a normalized, read-only copy of the specification register
supplied in ``ONGC_Oil Field Chemical_Testing_Time-2.xlsx``.  It is deliberately
kept outside the workflow database: it is a comparison baseline, not a request
to create or modify published specifications.
"""

from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from app.core.services.csc_type_classification_import import canonicalize_spec_number
from app.core.services.csc_utils import SPEC_SUBSET_LABELS, SPEC_SUBSET_ORDER, spec_sort_key


REFERENCE_REGISTER_FILENAME = "ONGC_Oil Field Chemical_Testing_Time-2.xlsx"
_REFERENCE_REGISTER_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "csc_reference_specifications.csv"
)
_SUBSET_RANK = {code: index for index, code in enumerate(SPEC_SUBSET_ORDER)}


def _subset_for_specification(specification_no: str) -> str:
    parts = canonicalize_spec_number(specification_no).split("/")
    return parts[1] if len(parts) >= 4 and parts[0] == "ONGC" else "OTHER"


def _split_cell(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(value or "").split(";") if item.strip())


@lru_cache(maxsize=1)
def reference_specifications() -> tuple[dict[str, Any], ...]:
    """Load the attachment-backed reference register without touching the DB."""
    with _REFERENCE_REGISTER_PATH.open(encoding="utf-8", newline="") as source:
        records = []
        for row in csv.DictReader(source):
            specification_no = str(row.get("specification_no") or "").strip()
            specification_key = canonicalize_spec_number(specification_no)
            if not specification_key:
                continue
            subset = _subset_for_specification(specification_no)
            records.append(
                {
                    "specification_no": specification_no,
                    "specification_key": specification_key,
                    "subset": subset,
                    "subset_label": SPEC_SUBSET_LABELS.get(subset, "Needs source review"),
                    "chemical_names": _split_cell(row.get("chemical_names")),
                    "material_codes": _split_cell(row.get("material_codes")),
                    "reference_entry_count": int(row.get("reference_entry_count") or 0),
                }
            )
    return tuple(records)


def _coverage_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _SUBSET_RANK.get(row["subset"], len(_SUBSET_RANK)),
        spec_sort_key(row["specification_no"]),
        row["specification_no"],
    )


def build_reference_specification_coverage(
    published_drafts: Iterable[Any],
) -> dict[str, Any]:
    """Compare the attachment-backed register with currently published drafts.

    Comparison is by canonical specification number. Material codes are retained
    as a helpful reference but are never used to infer publication, avoiding a
    false positive where a material code has moved to a different specification.
    """
    published_by_key = {
        canonicalize_spec_number(getattr(draft, "spec_number", "")): draft
        for draft in published_drafts
        if canonicalize_spec_number(getattr(draft, "spec_number", ""))
    }
    reference_rows = []
    reference_keys = set()
    category_index: dict[str, dict[str, Any]] = {}

    for reference in reference_specifications():
        draft = published_by_key.get(reference["specification_key"])
        is_published = draft is not None
        reference_keys.add(reference["specification_key"])
        row = {
            **reference,
            "status": "published" if is_published else "not_published",
            "published_draft": draft,
            "published_material_code": (
                str(getattr(draft, "material_code", "") or "").strip()
                if draft is not None
                else ""
            ),
        }
        reference_rows.append(row)

        category = category_index.setdefault(
            reference["subset"],
            {
                "code": reference["subset"],
                "label": reference["subset_label"],
                "reference_count": 0,
                "published_count": 0,
            },
        )
        category["reference_count"] += 1
        category["published_count"] += int(is_published)

    reference_rows.sort(key=_coverage_sort_key)
    category_rows = sorted(
        (
            {
                **category,
                "not_published_count": category["reference_count"] - category["published_count"],
            }
            for category in category_index.values()
        ),
        key=lambda category: _SUBSET_RANK.get(category["code"], len(_SUBSET_RANK)),
    )
    outside_reference = sorted(
        (
            draft
            for key, draft in published_by_key.items()
            if key not in reference_keys
        ),
        key=lambda draft: spec_sort_key(getattr(draft, "spec_number", "")),
    )
    published_count = sum(row["status"] == "published" for row in reference_rows)

    return {
        "source_filename": REFERENCE_REGISTER_FILENAME,
        "reference_specification_count": len(reference_rows),
        "reference_entry_count": sum(row["reference_entry_count"] for row in reference_rows),
        "published_reference_count": published_count,
        "not_published_count": len(reference_rows) - published_count,
        "published_outside_reference": outside_reference,
        "coverage_percent": round((published_count / len(reference_rows)) * 100) if reference_rows else 0,
        "categories": category_rows,
        "reference_rows": reference_rows,
    }
