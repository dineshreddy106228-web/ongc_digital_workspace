from __future__ import annotations

from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services.csc_utils import (
    ALL_FLAG_IDS,
    IMPACT_CHECKLIST_FLAGS,
    RED_FLAG_IDS,
    deserialize_impact_checklist_state,
    summarize_impact_checklist_state,
)


def test_existing_impact_state_picks_up_new_environmental_critical_flag_as_review() -> None:
    legacy_v2_state = {
        "version": 2,
        "chemical_name": "DEOILER",
        "flags": {
            "hse_flag": {"value": "NO"},
            "op_acute_flag": {"value": "NO"},
            "op_chronic_flag": {"value": "NO"},
            "safety_flag": {"value": "NO"},
            "env_flag": {"value": "YES"},
            "regulatory_flag": {"value": "NO"},
            "supply_flag": {"value": "NO"},
            "seasonal_flag": {"value": "NO"},
            "financial_flag": {"value": "NO"},
        },
    }

    state = deserialize_impact_checklist_state(json.dumps(legacy_v2_state), "DEOILER")

    assert "env_critical_flag" in state["flags"]
    assert state["flags"]["env_critical_flag"]["value"] == "REVIEW"
    assert state["version"] == 3


def test_impact_summary_counts_new_environmental_critical_red_flag() -> None:
    state = {
        "version": 3,
        "chemical_name": "DEOILER",
        "flags": {
            flag_id: {"value": "NO"} for flag_id in ALL_FLAG_IDS
        },
    }
    state["flags"]["env_critical_flag"] = {"value": "YES"}

    summary = summarize_impact_checklist_state(state)

    assert len(RED_FLAG_IDS) == 5
    assert len(IMPACT_CHECKLIST_FLAGS) == 10
    assert summary["red_yes_count"] == 1
    assert summary["amber_yes_count"] == 0
    assert summary["total_flags"] == 10
    assert summary["grade"] == "HIGH"
    assert summary["triggered_flags"] == ["env_critical_flag"]
