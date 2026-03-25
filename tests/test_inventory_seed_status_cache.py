from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.services import inventory_intelligence as inv


def test_get_seed_upload_status_reuses_cached_results_when_signatures_match(monkeypatch) -> None:
    inv._SEED_UPLOAD_STATUS_CACHE["signature"] = None
    inv._SEED_UPLOAD_STATUS_CACHE["years"] = None
    inv._SEED_UPLOAD_STATUS_CACHE["results"] = None

    monkeypatch.setattr(inv.os.path, "exists", lambda path: True)
    monkeypatch.setattr(
        inv,
        "_source_signature",
        lambda path: {
            inv.CONS_FILE: (1, 1),
            inv.PROC_FILE: (2, 2),
            inv.MC9_FILE: (3, 3),
        }.get(path),
    )

    calls = {"cons": 0, "proc": 0, "mc9": 0}

    def _cons():
        calls["cons"] += 1
        return pd.DataFrame(
            [{"financial_year": "FY24-25", "material_desc": "A", "material_code": "1", "reporting_plant": "10D1"}]
        )

    def _proc():
        calls["proc"] += 1
        return pd.DataFrame(
            [{"financial_year": "FY24-25", "material_desc": "A", "material_code": "1", "reporting_plant": "10D1"}]
        )

    def _mc9():
        calls["mc9"] += 1
        return pd.DataFrame(
            [{"financial_year": "FY24-25", "material_desc": "A", "material_code": "1", "reporting_plant": "10D1"}]
        )

    monkeypatch.setattr(inv, "_load_consumption", _cons)
    monkeypatch.setattr(inv, "_load_procurement", _proc)
    monkeypatch.setattr(inv, "_load_mc9", _mc9)

    first = inv.get_seed_upload_status(years=5)
    second = inv.get_seed_upload_status(years=5)

    assert first == second
    assert calls == {"cons": 1, "proc": 1, "mc9": 1}
