"""Inventory Intelligence seed-data analytics engine."""

from __future__ import annotations

import calendar
import logging
import os
import re
import tempfile
from io import BytesIO

try:
    import numpy as np
    import pandas as pd
except ImportError:  # pragma: no cover – present only when ENABLE_INVENTORY=False
    np = None  # type: ignore[assignment]
    pd = None  # type: ignore[assignment]
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
CONS_FILE = os.path.join(_BASE_DIR, "Consumption_data.xlsx")
PROC_FILE = os.path.join(_BASE_DIR, "Procurement_data.xlsx")

_PERIOD_RE = re.compile(r"^\s*(\d{1,2})\.(\d{4})\s*$")
_PRICE_SWING_THRESHOLD_PCT = 15.0
_FORECAST_WEIGHT_PATTERNS = [
    (0.35, 0.25, 0.15, 0.10, 0.10, 0.05),
    (0.30, 0.25, 0.18, 0.12, 0.10, 0.05),
    (0.25, 0.22, 0.18, 0.15, 0.12, 0.08),
    (0.20, 0.20, 0.18, 0.16, 0.14, 0.12),
    (1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6),
]

# ---------------------------------------------------------------------------
# Adaptive multi-strategy forecasting engine
# ---------------------------------------------------------------------------
# Material demand profiles: each needs a different prediction strategy.
#   - STEADY:   CV < 0.5, few zeros  -> WMA / exponential smoothing
#   - VARIABLE: CV 0.5-1.0           -> ensemble of WMA + seasonal decomp
#   - SPORADIC: many zero months     -> Croston's method (separate demand
#                                       occurrence & demand size models)
#   - LUMPY:    high CV + zeros      -> Syntetos-Boylan Approximation (SBA)
# ---------------------------------------------------------------------------

_ZERO_FRACTION_THRESHOLD = 0.30   # >30% zeros = intermittent demand
_HIGH_CV_THRESHOLD = 0.50         # coefficient of variation threshold
_MIN_HISTORY_MONTHS = 7           # unchanged from original
_MIN_RECENT_ACTIVE = 3            # relaxed from 5 to handle sporadic items
_SEASONAL_CYCLE = 12              # months in a seasonal cycle


def _classify_demand(quantities: np.ndarray) -> str:
    """Classify demand pattern into steady / variable / sporadic / lumpy."""
    if len(quantities) < 4:
        return "insufficient"
    nonzero = quantities[quantities > 0]
    zero_frac = 1.0 - (len(nonzero) / len(quantities))
    cv = float(np.std(nonzero) / np.mean(nonzero)) if len(nonzero) > 1 and np.mean(nonzero) > 0 else 0.0
    if zero_frac > _ZERO_FRACTION_THRESHOLD and cv > _HIGH_CV_THRESHOLD:
        return "lumpy"
    if zero_frac > _ZERO_FRACTION_THRESHOLD:
        return "sporadic"
    if cv > _HIGH_CV_THRESHOLD:
        return "variable"
    return "steady"


def _exponential_smoothing(quantities: np.ndarray, alpha: float = 0.3) -> np.ndarray:
    """Simple exponential smoothing with optimisable alpha."""
    n = len(quantities)
    smoothed = np.zeros(n)
    smoothed[0] = quantities[0]
    for i in range(1, n):
        smoothed[i] = alpha * quantities[i] + (1 - alpha) * smoothed[i - 1]
    return smoothed


def _double_exponential_smoothing(
    quantities: np.ndarray, alpha: float = 0.3, beta: float = 0.1
) -> tuple[np.ndarray, np.ndarray]:
    """Holt's linear trend method."""
    n = len(quantities)
    level = np.zeros(n)
    trend = np.zeros(n)
    level[0] = quantities[0]
    trend[0] = quantities[1] - quantities[0] if n > 1 else 0.0
    for i in range(1, n):
        level[i] = alpha * quantities[i] + (1 - alpha) * (level[i - 1] + trend[i - 1])
        trend[i] = beta * (level[i] - level[i - 1]) + (1 - beta) * trend[i - 1]
    return level, trend


def _seasonal_indices(quantities: np.ndarray, cycle: int = 12) -> np.ndarray:
    """Compute multiplicative seasonal indices from full cycles."""
    n = len(quantities)
    if n < cycle:
        return np.ones(cycle)
    full_cycles = n // cycle
    usable = quantities[: full_cycles * cycle].reshape(full_cycles, cycle)
    row_means = usable.mean(axis=1, keepdims=True)
    row_means[row_means == 0] = 1.0  # avoid division by zero
    ratios = usable / row_means
    indices = ratios.mean(axis=0)
    # normalise so they sum to cycle
    total = indices.sum()
    if total > 0:
        indices = indices * (cycle / total)
    indices[indices == 0] = 0.01  # floor to prevent multiply-by-zero
    return indices


def _holt_winters_additive(
    quantities: np.ndarray, cycle: int = 12, alpha: float = 0.3,
    beta: float = 0.1, gamma: float = 0.2, horizon: int = 6,
) -> list[float]:
    """Holt-Winters additive seasonal model with forecast."""
    n = len(quantities)
    if n < cycle + 2:
        # fall back to simple ES forecast
        sm = _exponential_smoothing(quantities, alpha)
        return [max(float(sm[-1]), 0.0)] * horizon

    # initialise
    level = np.zeros(n)
    trend = np.zeros(n)
    season = np.zeros(n + horizon)

    # initial seasonal component from first cycle
    first_cycle_mean = quantities[:cycle].mean() or 1.0
    for i in range(cycle):
        season[i] = quantities[i] - first_cycle_mean

    level[0] = first_cycle_mean
    trend[0] = (quantities[cycle:2 * cycle].mean() - quantities[:cycle].mean()) / cycle if n >= 2 * cycle else 0.0

    for i in range(1, n):
        l_prev = level[i - 1]
        t_prev = trend[i - 1]
        s_prev = season[i - cycle] if i >= cycle else season[i]
        level[i] = alpha * (quantities[i] - s_prev) + (1 - alpha) * (l_prev + t_prev)
        trend[i] = beta * (level[i] - l_prev) + (1 - beta) * t_prev
        season[i] = gamma * (quantities[i] - level[i]) + (1 - gamma) * s_prev

    forecasts = []
    for h in range(1, horizon + 1):
        s_idx = n - cycle + ((h - 1) % cycle)
        fcast = level[n - 1] + h * trend[n - 1] + season[s_idx]
        forecasts.append(max(float(fcast), 0.0))
    return forecasts


def _croston_forecast(
    quantities: np.ndarray, alpha: float = 0.15, horizon: int = 6,
) -> list[float]:
    """Croston's method for intermittent demand."""
    demands = []
    intervals = []
    period_since_last = 0
    for val in quantities:
        period_since_last += 1
        if val > 0:
            demands.append(val)
            intervals.append(period_since_last)
            period_since_last = 0

    if not demands:
        return [0.0] * horizon

    # smooth demand sizes and intervals separately
    z = float(demands[0])  # demand size estimate
    p = float(intervals[0])  # inter-demand interval estimate
    for i in range(1, len(demands)):
        z = alpha * demands[i] + (1 - alpha) * z
        p = alpha * intervals[i] + (1 - alpha) * p

    p = max(p, 1.0)
    forecast_per_period = z / p
    return [max(float(forecast_per_period), 0.0)] * horizon


def _sba_forecast(
    quantities: np.ndarray, alpha: float = 0.15, horizon: int = 6,
) -> list[float]:
    """Syntetos-Boylan Approximation – bias-corrected Croston's."""
    demands = []
    intervals = []
    period_since_last = 0
    for val in quantities:
        period_since_last += 1
        if val > 0:
            demands.append(val)
            intervals.append(period_since_last)
            period_since_last = 0

    if not demands:
        return [0.0] * horizon

    z = float(demands[0])
    p = float(intervals[0])
    for i in range(1, len(demands)):
        z = alpha * demands[i] + (1 - alpha) * z
        p = alpha * intervals[i] + (1 - alpha) * p

    p = max(p, 1.0)
    # SBA bias correction factor
    forecast_per_period = (1 - alpha / 2) * (z / p)
    return [max(float(forecast_per_period), 0.0)] * horizon


def _optimize_alpha(quantities: np.ndarray, method: str, alphas: list[float] | None = None) -> tuple[float, float]:
    """Find best smoothing parameter via walk-forward validation."""
    if alphas is None:
        alphas = [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7]
    best_alpha = 0.3
    best_mape = float("inf")
    n = len(quantities)
    train_end = max(n - 6, int(n * 0.7))
    if train_end < 4:
        return best_alpha, best_mape

    for a in alphas:
        errors = []
        for t in range(train_end, n):
            train = quantities[:t]
            actual = float(quantities[t])
            if method == "ses":
                sm = _exponential_smoothing(train, alpha=a)
                pred = float(sm[-1])
            elif method == "croston":
                preds = _croston_forecast(train, alpha=a, horizon=1)
                pred = preds[0]
            elif method == "sba":
                preds = _sba_forecast(train, alpha=a, horizon=1)
                pred = preds[0]
            else:
                pred = float(np.mean(train[-6:])) if len(train) >= 6 else float(np.mean(train))
            if actual > 0:
                errors.append(abs(actual - pred) / actual)
            else:
                errors.append(0.0 if pred < 0.5 else 1.0)
        mape = float(np.mean(errors)) * 100 if errors else float("inf")
        if mape < best_mape:
            best_mape = mape
            best_alpha = a
    return best_alpha, best_mape


def _ensemble_forecast(
    quantities: np.ndarray, labels: list[str], horizon: int = 6,
) -> tuple[list[float], dict]:
    """Adaptive ensemble: classifies demand, picks best strategy, ensembles top-2."""
    demand_type = _classify_demand(quantities)
    n = len(quantities)
    candidates: dict[str, list[float]] = {}
    meta: dict = {"demand_type": demand_type, "candidate_methods": []}

    # Candidate 1: Best WMA (original logic)
    if n >= _MIN_HISTORY_MONTHS:
        best_weights, best_mae, _, _, _, _ = _backtest_forecast_weight_patterns(quantities, labels)
        series = list(quantities.astype(float))
        wma_forecasts: list[float] = []
        lookback = len(best_weights)
        for _ in range(horizon):
            window = np.asarray(series[-lookback:][::-1], dtype=float)
            predicted = max(_weighted_moving_average(window, best_weights), 0.0)
            wma_forecasts.append(predicted)
            series.append(predicted)
        candidates["wma"] = wma_forecasts

    # Candidate 2: Optimised exponential smoothing
    best_alpha_ses, _ = _optimize_alpha(quantities, "ses")
    sm = _exponential_smoothing(quantities, alpha=best_alpha_ses)
    candidates["ses"] = [max(float(sm[-1]), 0.0)] * horizon

    # Candidate 3: Holt's (trend)
    if n >= 6:
        level, trend = _double_exponential_smoothing(quantities, alpha=0.3, beta=0.1)
        holt_forecasts = [max(float(level[-1] + (h + 1) * trend[-1]), 0.0) for h in range(horizon)]
        candidates["holt"] = holt_forecasts

    # Candidate 4: Holt-Winters (seasonal) if enough data
    if n >= _SEASONAL_CYCLE + 2:
        hw_forecasts = _holt_winters_additive(quantities, cycle=_SEASONAL_CYCLE, horizon=horizon)
        candidates["holt_winters"] = hw_forecasts

    # Candidate 5: Croston's (intermittent)
    if demand_type in ("sporadic", "lumpy"):
        best_alpha_cr, _ = _optimize_alpha(quantities, "croston")
        candidates["croston"] = _croston_forecast(quantities, alpha=best_alpha_cr, horizon=horizon)

    # Candidate 6: SBA (intermittent, bias-corrected)
    if demand_type in ("sporadic", "lumpy"):
        best_alpha_sba, _ = _optimize_alpha(quantities, "sba")
        candidates["sba"] = _sba_forecast(quantities, alpha=best_alpha_sba, horizon=horizon)

    # Candidate 7: Seasonal Naive (same month last year)
    if n >= 13:
        seasonal_naive = []
        for h in range(horizon):
            idx = n - _SEASONAL_CYCLE + h
            if 0 <= idx < n:
                seasonal_naive.append(max(float(quantities[idx]), 0.0))
            else:
                seasonal_naive.append(max(float(np.mean(quantities[-6:])), 0.0))
        candidates["seasonal_naive"] = seasonal_naive

    # Walk-forward backtest each candidate to find best
    eval_start = max(n - 6, _MIN_HISTORY_MONTHS)
    method_scores: list[dict] = []
    for method_name, _ in candidates.items():
        errors = []
        for t in range(eval_start, n):
            actual = float(quantities[t])
            # Generate a 1-step forecast using data up to t
            train = quantities[:t]
            if len(train) < 3:
                continue
            if method_name == "wma" and len(train) >= _MIN_HISTORY_MONTHS:
                lbl = labels[:t]
                bw, _, _, _, _, _ = _backtest_forecast_weight_patterns(train, lbl)
                window = np.asarray(list(train[-len(bw):][::-1]), dtype=float)
                pred = max(_weighted_moving_average(window, bw), 0.0)
            elif method_name == "ses":
                sm_t = _exponential_smoothing(train, alpha=best_alpha_ses)
                pred = max(float(sm_t[-1]), 0.0)
            elif method_name == "holt" and len(train) >= 6:
                lv, tr = _double_exponential_smoothing(train)
                pred = max(float(lv[-1] + tr[-1]), 0.0)
            elif method_name == "holt_winters" and len(train) >= _SEASONAL_CYCLE + 2:
                hw = _holt_winters_additive(train, cycle=_SEASONAL_CYCLE, horizon=1)
                pred = hw[0]
            elif method_name == "croston":
                pred = _croston_forecast(train, alpha=best_alpha_cr if 'best_alpha_cr' in dir() else 0.15, horizon=1)[0]
            elif method_name == "sba":
                pred = _sba_forecast(train, alpha=best_alpha_sba if 'best_alpha_sba' in dir() else 0.15, horizon=1)[0]
            elif method_name == "seasonal_naive" and len(train) >= 13:
                pred = max(float(train[t - _SEASONAL_CYCLE]) if t >= _SEASONAL_CYCLE else float(np.mean(train[-6:])), 0.0)
            else:
                continue
            abs_err = abs(actual - pred)
            errors.append({"actual": actual, "pred": pred, "abs_error": abs_err})

        if errors:
            total_actual = sum(e["actual"] for e in errors)
            total_error = sum(e["abs_error"] for e in errors)
            gap_pct = (total_error / total_actual * 100) if total_actual > 0 else 100.0
            # For zero-actual periods, penalise overforecasting mildly
            score = max(0.0, 100.0 - min(gap_pct, 100.0))
        else:
            score = 0.0
            gap_pct = 100.0
        method_scores.append({
            "method": method_name,
            "confidence": round(score, 2),
            "gap_pct": round(gap_pct, 2),
            "eval_points": len(errors),
        })

    method_scores.sort(key=lambda x: x["confidence"], reverse=True)
    meta["candidate_methods"] = method_scores

    # Ensemble: weighted blend of top-2 methods (70/30)
    if len(method_scores) >= 2:
        top1_name = method_scores[0]["method"]
        top2_name = method_scores[1]["method"]
        top1_conf = method_scores[0]["confidence"]
        top2_conf = method_scores[1]["confidence"]
        total_conf = top1_conf + top2_conf
        if total_conf > 0:
            w1 = top1_conf / total_conf
            w2 = top2_conf / total_conf
        else:
            w1, w2 = 0.5, 0.5
        f1 = candidates[top1_name]
        f2 = candidates[top2_name]
        blended = [max(w1 * f1[i] + w2 * f2[i], 0.0) for i in range(horizon)]
        meta["ensemble_weights"] = {top1_name: round(w1, 3), top2_name: round(w2, 3)}
        meta["selected_method"] = f"ensemble({top1_name}+{top2_name})"
    elif method_scores:
        blended = candidates[method_scores[0]["method"]]
        meta["selected_method"] = method_scores[0]["method"]
    else:
        blended = [max(float(np.mean(quantities[-6:])), 0.0)] * horizon
        meta["selected_method"] = "simple_average"

    return blended, meta


def _compute_adaptive_confidence(
    quantities: np.ndarray,
    labels: list[str],
    forecast_func,
    months: int = 6,
) -> tuple[list[dict], str, float | None, float | None]:
    """Procurement-grade confidence via cumulative-horizon walk-forward.

    For inventory planning, what matters most is whether the *cumulative*
    forecast over the planning horizon tracks the actual cumulative demand.
    Individual months will always fluctuate – but procurement decisions
    are based on aggregate quantities over weeks/months.

    Scoring pillars
    ===============
    1. **Cumulative horizon accuracy** (40%) – slide a window of size
       `horizon` across evaluation months.  At each step compare
       *predicted total* vs *actual total*.  This is the single most
       important metric for procurement planning.

    2. **Demand-rate accuracy** (25%) – compare the *average predicted
       monthly rate* vs *average actual rate*.  Tolerates per-month
       noise while rewarding correct magnitude.

    3. **Directional accuracy** (15%) – did the model correctly predict
       the sign of change vs. the prior month?

    4. **Data quality & coverage** (20%) – more evaluation points,
       fewer missing months, and stable error variance.
    """
    n = len(quantities)
    demand_type = _classify_demand(quantities)

    # Evaluation window: use more history for intermittent items
    eval_months = months
    if demand_type in ("sporadic", "lumpy"):
        eval_months = min(n - 4, 18)  # up to 18 months
    elif demand_type == "variable":
        eval_months = min(n - 4, 12)
    eval_start = max(n - eval_months, 4)

    results: list[dict] = []
    pred_history: list[float] = []
    actual_history: list[float] = []
    direction_correct = direction_total = 0

    for t in range(eval_start, n):
        train = quantities[:t]
        train_labels = labels[:t]
        actual = float(quantities[t])

        if len(train) < 4:
            continue

        forecasted, _ = forecast_func(train, train_labels, horizon=1)
        pred = forecasted[0] if forecasted else 0.0
        abs_err = abs(actual - pred)
        results.append({
            "period": labels[t],
            "forecast_qty": round(pred, 2),
            "actual_qty": round(actual, 2),
            "abs_error": round(abs_err, 2),
        })
        pred_history.append(pred)
        actual_history.append(actual)

        if t >= eval_start + 1 and t - 1 >= 0:
            direction_total += 1
            ad = actual - float(quantities[t - 1])
            pd_ = pred - float(quantities[t - 1])
            if (ad >= 0 and pd_ >= 0) or (ad < 0 and pd_ < 0):
                direction_correct += 1

    if not results:
        return [], "", None, None

    # ================================================================
    # Pillar 1: Cumulative horizon accuracy (40%)
    # Slide windows of various sizes (3, 6, full) and measure how
    # close predicted totals track actual totals.
    # ================================================================
    horizon_scores: list[float] = []
    for win_size in [3, 6, len(actual_history)]:
        ws = min(win_size, len(actual_history))
        if ws < 2:
            continue
        for i in range(ws, len(actual_history) + 1):
            actual_sum = sum(actual_history[i - ws : i])
            pred_sum = sum(pred_history[i - ws : i])
            max_val = max(abs(actual_sum), abs(pred_sum), 1e-9)
            # Relative accuracy: 1 - |error|/max (bounded 0..1)
            rel_acc = 1.0 - (abs(actual_sum - pred_sum) / max_val)
            horizon_scores.append(max(rel_acc, 0.0))

    cumulative_score = float(np.mean(horizon_scores)) * 100.0 if horizon_scores else 50.0

    # ================================================================
    # Pillar 2: Demand-rate accuracy (25%)
    # How close is predicted avg monthly rate to actual avg rate?
    # ================================================================
    actual_rate = float(np.mean(actual_history)) if actual_history else 0.0
    pred_rate = float(np.mean(pred_history)) if pred_history else 0.0
    max_rate = max(abs(actual_rate), abs(pred_rate), 1e-9)
    rate_accuracy = 1.0 - (abs(actual_rate - pred_rate) / max_rate)
    rate_score = max(rate_accuracy, 0.0) * 100.0

    # ================================================================
    # Pillar 3: Directional accuracy (15%)
    # ================================================================
    if direction_total > 0:
        direction_score = (direction_correct / direction_total) * 100.0
    else:
        direction_score = 60.0

    # ================================================================
    # Pillar 4: Data quality & coverage (20%)
    # ================================================================
    coverage = min(len(results) / max(months, 1), 1.0)
    # Bonus for having lots of evaluation points
    depth_bonus = min(len(results) / 12.0, 1.0)  # up to 12 months
    # Penalize if model errors are wildly inconsistent
    pct_errors = []
    for a, p in zip(actual_history, pred_history):
        d = max(abs(a), abs(p), 1e-9)
        pct_errors.append(abs(a - p) / d)
    if len(pct_errors) > 1:
        err_consistency = 1.0 - min(float(np.std(pct_errors)), 1.0)
    else:
        err_consistency = 0.5
    quality_score = (0.4 * coverage + 0.3 * depth_bonus + 0.3 * err_consistency) * 100.0

    # ================================================================
    # Composite confidence
    # ================================================================
    confidence_pct = (
        0.40 * cumulative_score
        + 0.25 * rate_score
        + 0.15 * direction_score
        + 0.20 * quality_score
    )
    confidence_pct = max(0.0, min(100.0, confidence_pct))

    window_label = (
        f"{results[0]['period']} to {results[-1]['period']}"
        if results
        else ""
    )
    total_actual = sum(_safe_float(r["actual_qty"]) for r in results)
    total_error = sum(_safe_float(r["abs_error"]) for r in results)
    gap_pct = (total_error / total_actual * 100.0) if total_actual > 0 else None

    return (
        results,
        window_label,
        round(gap_pct, 2) if gap_pct is not None else None,
        round(confidence_pct, 2),
    )

SEED_UPLOAD_SCHEMA = {
    "consumption": {
        "title": "Consumption Seed File",
        "filename": os.path.basename(CONS_FILE),
        "required": [
            ("Plant", "SAP plant code"),
            ("Material", "Material code and description"),
            ("Month", "Reporting period in MM.YYYY style"),
            ("Usage Quantity", "Quantity consumed"),
            ("Usage UoM", "Consumption unit of measure"),
            ("Usage Value", "Consumption value"),
            ("Usage Currency", "Currency code, typically INR"),
        ],
        "optional": [
            ("Storage Location", "Storage location within the plant"),
        ],
        "allowed_exts": {".xlsx", ".xls", ".csv"},
    },
    "procurement": {
        "title": "Procurement Seed File",
        "filename": os.path.basename(PROC_FILE),
        "required": [
            ("Plant", "SAP plant code"),
            ("Net Price", "Procurement price"),
            ("Purchasing Document", "PO number"),
            ("Document Date", "PO document date"),
            ("Material", "Material code"),
            ("Supplier/Supplying Plant", "Vendor name or supplying plant"),
            ("Short Text", "Material description"),
            ("Order Quantity", "Ordered quantity"),
            ("Order Unit", "Order unit of measure"),
            ("Currency", "Currency code, typically INR"),
            ("Price Unit", "Price basis unit"),
            ("Effective value", "Procurement value"),
        ],
        "optional": [
            ("Item", "PO item number"),
            ("Release indicator", "Release status"),
        ],
        "allowed_exts": {".xlsx", ".xls", ".csv"},
    },
}


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_plant(plant: str | None) -> str | None:
    if plant is None:
        return None
    value = str(plant).strip().upper()
    return value or None


def _normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _format_period_label(year: int, month: int) -> str:
    return f"{calendar.month_abbr[int(month)]} {int(year)}"


def _looks_like_period(value) -> bool:
    return bool(_PERIOD_RE.match(_normalize_text(value)))


def _parse_period(value) -> tuple[pd.Series, pd.Series]:
    parts = value.fillna("").astype(str).str.extract(_PERIOD_RE)
    month = pd.to_numeric(parts[0], errors="coerce").astype("Int64")
    year = pd.to_numeric(parts[1], errors="coerce").astype("Int64")
    return month, year


def _clean_currency(value, fallback: str = "INR") -> str:
    text = _normalize_text(value).upper()
    if re.fullmatch(r"[A-Z]{3,5}", text):
        return text
    return fallback


def _build_period_index(df: pd.DataFrame) -> pd.PeriodIndex:
    return pd.PeriodIndex.from_fields(
        year=df["year"].astype(int),
        month=df["month"].astype(int),
        freq="M",
    )


def _repair_shifted_consumption_rows(df: pd.DataFrame) -> pd.DataFrame:
    repaired = df.copy()
    repaired[["month_raw", "usage_qty", "uom", "usage_value", "currency"]] = (
        repaired[["month_raw", "usage_qty", "uom", "usage_value", "currency"]].astype("object")
    )
    shifted_mask = (
        repaired["month_raw"].fillna("").astype(str).str.strip().eq("")
        & repaired["usage_qty"].apply(_looks_like_period)
    )
    if not shifted_mask.any():
        return repaired

    logger.info("Repairing %d shifted consumption rows", int(shifted_mask.sum()))
    shifted_values = repaired.loc[shifted_mask, ["usage_qty", "uom", "usage_value", "currency"]].copy()
    repaired.loc[shifted_mask, "month_raw"] = shifted_values["usage_qty"].to_numpy()
    repaired.loc[shifted_mask, "usage_qty"] = shifted_values["uom"].to_numpy()
    repaired.loc[shifted_mask, "uom"] = shifted_values["usage_value"].to_numpy()
    repaired.loc[shifted_mask, "usage_value"] = shifted_values["currency"].to_numpy()
    repaired.loc[shifted_mask, "currency"] = "INR"
    return repaired


def _normalize_consumption_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    col_map = {}
    for col in df.columns:
        key = col.lower().strip()
        if key == "plant":
            col_map[col] = "plant"
        elif key == "material":
            col_map[col] = "material"
        elif "storage" in key:
            col_map[col] = "storage_location"
        elif key == "month":
            col_map[col] = "month_raw"
        elif "usage quantity" in key:
            col_map[col] = "usage_qty"
        elif "usage uom" in key:
            col_map[col] = "uom"
        elif "usage value" in key:
            col_map[col] = "usage_value"
        elif "usage currency" in key or key == "currency":
            col_map[col] = "currency"
    df = df.rename(columns=col_map)

    for col in ["plant", "material", "storage_location", "month_raw", "usage_qty", "uom", "usage_value", "currency"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = _repair_shifted_consumption_rows(df)

    df["plant"] = df["plant"].map(_normalize_text).str.upper()
    df["material"] = df["material"].map(_normalize_text).str.upper()
    df["storage_location"] = df["storage_location"].map(_normalize_text)
    df["month_raw"] = df["month_raw"].map(_normalize_text)
    df["uom"] = df["uom"].map(_normalize_text).str.upper()
    df["currency"] = df["currency"].map(_clean_currency)

    month, year = _parse_period(df["month_raw"])
    df["month"] = month
    df["year"] = year

    for col in ["usage_qty", "usage_value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["material"] != ""].copy()

    def _split_material(value: str) -> tuple[str, str]:
        match = re.match(r"^(\d{6,12})\s+(.+)$", value.strip())
        if not match:
            return "", value.strip()
        return match.group(1), match.group(2).strip()

    split_values = df["material"].apply(_split_material)
    df["material_code"] = split_values.apply(lambda item: item[0])
    df["material_desc"] = split_values.apply(lambda item: item[1])
    return df


def _load_consumption() -> pd.DataFrame:
    df = pd.read_excel(CONS_FILE, sheet_name=0)
    return _normalize_consumption_frame(df)


def _normalize_procurement_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()

    col_map = {}
    for col in df.columns:
        key = col.lower().strip()
        if key == "plant":
            col_map[col] = "plant"
        elif key == "net price":
            col_map[col] = "net_price"
        elif "purchasing document" in key:
            col_map[col] = "po_number"
        elif "document date" in key:
            col_map[col] = "doc_date"
        elif key == "material":
            col_map[col] = "material_code"
        elif key == "item":
            col_map[col] = "item"
        elif "supplier" in key or "supplying" in key:
            col_map[col] = "vendor"
        elif "short text" in key:
            col_map[col] = "material_desc"
        elif "order quantity" in key:
            col_map[col] = "order_qty"
        elif "order unit" in key:
            col_map[col] = "order_unit"
        elif key == "currency":
            col_map[col] = "currency"
        elif "price unit" in key:
            col_map[col] = "price_unit"
        elif "effective value" in key:
            col_map[col] = "effective_value"
        elif "release" in key:
            col_map[col] = "release_indicator"
    df = df.rename(columns=col_map)

    for col in [
        "plant",
        "net_price",
        "po_number",
        "doc_date",
        "material_code",
        "item",
        "vendor",
        "material_desc",
        "order_qty",
        "order_unit",
        "currency",
        "price_unit",
        "effective_value",
        "release_indicator",
    ]:
        if col not in df.columns:
            df[col] = pd.NA

    df["plant"] = df["plant"].map(_normalize_text).str.upper()
    df["po_number"] = df["po_number"].map(_normalize_text)
    df["material_code"] = df["material_code"].map(_normalize_text)
    df["item"] = df["item"].map(_normalize_text)
    df["vendor"] = df["vendor"].map(_normalize_text)
    df["material_desc"] = df["material_desc"].map(_normalize_text).str.upper()
    df["order_unit"] = df["order_unit"].map(_normalize_text).str.upper()
    df["currency"] = df["currency"].map(_clean_currency)
    df["release_indicator"] = df["release_indicator"].map(_normalize_text)
    df["doc_date"] = pd.to_datetime(df["doc_date"], errors="coerce")

    for col in ["net_price", "order_qty", "price_unit", "effective_value"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    valid_price_unit = df["price_unit"].where(df["price_unit"].fillna(0) > 0, 1.0)
    df["unit_price"] = df["net_price"] / valid_price_unit
    df["unit_price"] = df["unit_price"].replace([np.inf, -np.inf], np.nan)
    df = df[df["material_desc"] != ""].copy()
    return df


def _load_procurement() -> pd.DataFrame:
    df = pd.read_excel(PROC_FILE, sheet_name=0)
    return _normalize_procurement_frame(df)


def _read_uploaded_seed_dataframe(file_bytes: bytes, filename: str) -> pd.DataFrame:
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".csv":
        return pd.read_csv(BytesIO(file_bytes))
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(BytesIO(file_bytes), sheet_name=0)
    raise ValueError(f"Unsupported file type: {ext or 'unknown'}")


def _detect_consumption_fields(columns: list[str]) -> set[str]:
    detected = set()
    for column in columns:
        key = column.lower().strip()
        if key == "plant":
            detected.add("plant")
        elif key == "material":
            detected.add("material")
        elif "storage" in key:
            detected.add("storage_location")
        elif key == "month":
            detected.add("month_raw")
        elif "usage quantity" in key:
            detected.add("usage_qty")
        elif "usage uom" in key:
            detected.add("uom")
        elif "usage value" in key:
            detected.add("usage_value")
        elif "usage currency" in key or key == "currency":
            detected.add("currency")
    return detected


def _detect_procurement_fields(columns: list[str]) -> set[str]:
    detected = set()
    for column in columns:
        key = column.lower().strip()
        if key == "plant":
            detected.add("plant")
        elif key == "net price":
            detected.add("net_price")
        elif "purchasing document" in key:
            detected.add("po_number")
        elif "document date" in key:
            detected.add("doc_date")
        elif key == "material":
            detected.add("material_code")
        elif key == "item":
            detected.add("item")
        elif "supplier" in key or "supplying" in key:
            detected.add("vendor")
        elif "short text" in key:
            detected.add("material_desc")
        elif "order quantity" in key:
            detected.add("order_qty")
        elif "order unit" in key:
            detected.add("order_unit")
        elif key == "currency":
            detected.add("currency")
        elif "price unit" in key:
            detected.add("price_unit")
        elif "effective value" in key:
            detected.add("effective_value")
        elif "release" in key:
            detected.add("release_indicator")
    return detected


def _validate_seed_frame(df: pd.DataFrame, seed_type: str) -> None:
    columns = [str(column).strip() for column in df.columns]
    if seed_type == "consumption":
        detected = _detect_consumption_fields(columns)
        required_fields = {"plant", "material", "month_raw", "usage_qty", "uom", "usage_value", "currency"}
        display_names = {
            "plant": "Plant",
            "material": "Material",
            "month_raw": "Month",
            "usage_qty": "Usage Quantity",
            "uom": "Usage UoM",
            "usage_value": "Usage Value",
            "currency": "Usage Currency",
        }
    else:
        detected = _detect_procurement_fields(columns)
        required_fields = {
            "plant",
            "net_price",
            "po_number",
            "doc_date",
            "material_code",
            "vendor",
            "material_desc",
            "order_qty",
            "order_unit",
            "currency",
            "price_unit",
            "effective_value",
        }
        display_names = {
            "plant": "Plant",
            "net_price": "Net Price",
            "po_number": "Purchasing Document",
            "doc_date": "Document Date",
            "material_code": "Material",
            "vendor": "Supplier/Supplying Plant",
            "material_desc": "Short Text",
            "order_qty": "Order Quantity",
            "order_unit": "Order Unit",
            "currency": "Currency",
            "price_unit": "Price Unit",
            "effective_value": "Effective value",
        }
    missing = sorted(required_fields - detected)
    if missing:
        missing_labels = [display_names.get(field, field) for field in missing]
        raise ValueError(
            "Missing required columns for "
            f"{seed_type} seed file: {', '.join(missing_labels)}."
        )


def get_seed_upload_schema() -> dict:
    return SEED_UPLOAD_SCHEMA


def _write_temp_seed_workbook(df: pd.DataFrame, target: str, sheet_name: str) -> str:
    suffix = os.path.splitext(target)[1] or ".xlsx"
    directory = os.path.dirname(target) or None
    with tempfile.NamedTemporaryFile(suffix=suffix, dir=directory, delete=False) as handle:
        temp_path = handle.name

    try:
        with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise

    return temp_path


def save_seed_workbook(file_bytes: bytes, filename: str, seed_type: str) -> str:
    df = _read_uploaded_seed_dataframe(file_bytes, filename)
    _validate_seed_frame(df, seed_type)

    target = CONS_FILE if seed_type == "consumption" else PROC_FILE
    sheet_name = "Sheet1" if seed_type == "consumption" else "Data"
    temp_path = _write_temp_seed_workbook(df, target, sheet_name)
    os.replace(temp_path, target)
    return target


def save_seed_workbooks(
    consumption_bytes: bytes,
    consumption_filename: str,
    procurement_bytes: bytes,
    procurement_filename: str,
) -> dict:
    cons_df = _read_uploaded_seed_dataframe(consumption_bytes, consumption_filename)
    proc_df = _read_uploaded_seed_dataframe(procurement_bytes, procurement_filename)
    _validate_seed_frame(cons_df, "consumption")
    _validate_seed_frame(proc_df, "procurement")

    targets = {
        "consumption": CONS_FILE,
        "procurement": PROC_FILE,
    }
    temp_files = {
        "consumption": _write_temp_seed_workbook(cons_df, CONS_FILE, "Sheet1"),
        "procurement": _write_temp_seed_workbook(proc_df, PROC_FILE, "Data"),
    }

    os.replace(temp_files["consumption"], targets["consumption"])
    os.replace(temp_files["procurement"], targets["procurement"])
    return targets


def _monthly_series(cons_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        cons_df.dropna(subset=["year", "month"])
        .groupby(["year", "month"], as_index=False)
        .agg(qty=("usage_qty", "sum"), value=("usage_value", "sum"))
        .sort_values(["year", "month"])
    )
    if monthly.empty:
        return monthly.assign(period=pd.Series(dtype="object"), label=pd.Series(dtype="object"))

    monthly["period"] = _build_period_index(monthly)
    full_periods = pd.period_range(monthly["period"].min(), monthly["period"].max(), freq="M")
    monthly = monthly.set_index("period").reindex(full_periods)
    monthly.index.name = "period"
    monthly["qty"] = monthly["qty"].fillna(0.0)
    monthly["value"] = monthly["value"].fillna(0.0)
    monthly["year"] = monthly.index.year
    monthly["month"] = monthly.index.month
    monthly["label"] = [_format_period_label(period.year, period.month) for period in monthly.index]
    return monthly.reset_index()


def _monthly_procurement_series(proc_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        proc_df.dropna(subset=["doc_date"])
        .assign(year=lambda df: df["doc_date"].dt.year, month=lambda df: df["doc_date"].dt.month)
        .groupby(["year", "month"], as_index=False)
        .agg(value=("effective_value", "sum"))
        .sort_values(["year", "month"])
    )
    if monthly.empty:
        return monthly.assign(period=pd.Series(dtype="object"), label=pd.Series(dtype="object"))

    monthly["period"] = _build_period_index(monthly)
    full_periods = pd.period_range(monthly["period"].min(), monthly["period"].max(), freq="M")
    monthly = monthly.set_index("period").reindex(full_periods)
    monthly.index.name = "period"
    monthly["value"] = monthly["value"].fillna(0.0)
    monthly["year"] = monthly.index.year
    monthly["month"] = monthly.index.month
    monthly["label"] = [_format_period_label(period.year, period.month) for period in monthly.index]
    return monthly.reset_index()


def _merge_monthly_views(cons_monthly: pd.DataFrame, proc_monthly: pd.DataFrame) -> pd.DataFrame:
    if cons_monthly.empty and proc_monthly.empty:
        return pd.DataFrame(columns=["period", "year", "month", "label", "qty", "proc_value"])

    periods = []
    if not cons_monthly.empty:
        periods.extend(cons_monthly["period"].tolist())
    if not proc_monthly.empty:
        periods.extend(proc_monthly["period"].tolist())
    full_periods = pd.period_range(min(periods), max(periods), freq="M")

    merged = pd.DataFrame({"period": full_periods})
    if not cons_monthly.empty:
        merged = merged.merge(cons_monthly[["period", "qty"]], on="period", how="left")
    else:
        merged["qty"] = 0.0
    if not proc_monthly.empty:
        merged = merged.merge(proc_monthly[["period", "value"]].rename(columns={"value": "proc_value"}), on="period", how="left")
    else:
        merged["proc_value"] = 0.0

    merged["qty"] = merged["qty"].fillna(0.0)
    merged["proc_value"] = merged["proc_value"].fillna(0.0)
    merged["year"] = merged["period"].dt.year
    merged["month"] = merged["period"].dt.month
    merged["label"] = [_format_period_label(period.year, period.month) for period in merged["period"]]
    return merged


def _actual_monthly_rows(cons_df: pd.DataFrame) -> pd.DataFrame:
    return (
        cons_df.dropna(subset=["year", "month"])
        .groupby(["year", "month"], as_index=False)
        .agg(qty=("usage_qty", "sum"), value=("usage_value", "sum"), uom=("uom", "first"))
        .sort_values(["year", "month"], ascending=[False, False])
    )


def _format_weight_pattern(weights: tuple[float, ...]) -> str:
    return ", ".join(f"{round(weight * 100, 2):g}%" for weight in weights)


def _weighted_moving_average(window: np.ndarray, weights: tuple[float, ...]) -> float:
    return float(np.dot(window, np.asarray(weights, dtype=float)))


def _build_backtest_results(
    quantities: np.ndarray,
    labels: list[str],
    weights: tuple[float, ...],
) -> list[dict]:
    lookback = len(weights)
    results: list[dict] = []
    for idx in range(lookback, len(quantities)):
        window = quantities[idx - lookback:idx][::-1]
        forecast_qty = _weighted_moving_average(window, weights)
        actual_qty = float(quantities[idx])
        abs_error = abs(actual_qty - forecast_qty)
        results.append(
            {
                "period": labels[idx],
                "forecast_qty": round(_safe_float(forecast_qty), 2),
                "actual_qty": round(_safe_float(actual_qty), 2),
                "abs_error": round(_safe_float(abs_error), 2),
            }
        )
    return results


def _summarize_recent_backtest(results: list[dict], months: int = 6) -> tuple[list[dict], str, float | None, float | None]:
    recent_results = results[-months:] if results else []
    if not recent_results:
        return [], "", None, None

    actual_total = sum(_safe_float(row.get("actual_qty")) for row in recent_results)
    error_total = sum(_safe_float(row.get("abs_error")) for row in recent_results)
    gap_pct = ((error_total / actual_total) * 100) if actual_total > 0 else None
    confidence_pct = max(0.0, 100.0 - min(_safe_float(gap_pct), 100.0)) if gap_pct is not None else None
    window_label = f"{recent_results[0]['period']} to {recent_results[-1]['period']}" if recent_results else ""
    return recent_results, window_label, round(_safe_float(gap_pct), 2) if gap_pct is not None else None, round(_safe_float(confidence_pct), 2) if confidence_pct is not None else None


def _backtest_forecast_weight_patterns(
    quantities: np.ndarray,
    labels: list[str],
) -> tuple[tuple[float, ...], float, int, str, str, list[dict]]:
    lookback = len(_FORECAST_WEIGHT_PATTERNS[0])
    evaluation_points = len(quantities) - lookback
    start_idx = len(quantities) - evaluation_points
    best_weights = _FORECAST_WEIGHT_PATTERNS[0]
    best_mae = float("inf")
    candidate_scores: list[dict] = []

    for weights in _FORECAST_WEIGHT_PATTERNS:
        errors: list[float] = []
        for idx in range(start_idx, len(quantities)):
            window = quantities[idx - len(weights):idx][::-1]
            predicted = _weighted_moving_average(window, weights)
            errors.append(abs(float(quantities[idx]) - predicted))
        mae = float(np.mean(errors)) if errors else float("inf")
        candidate_scores.append(
            {
                "weights": [round(weight, 6) for weight in weights],
                "weights_label": _format_weight_pattern(weights),
                "mae": round(_safe_float(mae), 2),
                "evaluation_points": int(len(errors)),
            }
        )
        if mae < best_mae:
            best_weights = weights
            best_mae = mae
    backtest_start_label = labels[start_idx] if evaluation_points > 0 else ""
    backtest_end_label = labels[-1] if evaluation_points > 0 else ""
    candidate_scores = sorted(candidate_scores, key=lambda item: item["mae"])
    for idx, item in enumerate(candidate_scores, start=1):
        item["rank"] = idx
        item["is_selected"] = item["weights_label"] == _format_weight_pattern(best_weights)
    return best_weights, best_mae, evaluation_points, backtest_start_label, backtest_end_label, candidate_scores


def _compute_prediction_bands(
    quantities: np.ndarray,
    labels: list[str],
    forecast_func,
    forecast_values: list[float],
    horizon: int = 6,
) -> dict:
    """Select either P10/P90 or P5/P95 using walk-forward backtesting."""
    n = len(quantities)
    eval_start = max(n - 18, 4)  # use up to 18 months of backtest data

    pct_errors: list[float] = []

    for t in range(eval_start, n):
        train = quantities[:t]
        train_labels = labels[:t]
        actual = float(quantities[t])
        if len(train) < 4:
            continue
        try:
            forecasted, _ = forecast_func(train, train_labels, horizon=1)
            pred = forecasted[0] if forecasted else 0.0
        except Exception:
            continue
        if pred > 0:
            pct_errors.append((actual - pred) / pred)
        elif actual > 0:
            pct_errors.append(1.0)  # underforecast
        else:
            pct_errors.append(0.0)

    p50 = [max(_safe_float(v), 0.0) for v in forecast_values]
    if len(pct_errors) < 3:
        p5_vals  = [max(v * 0.60, 0.0) for v in p50]
        p10_vals = [max(v * 0.70, 0.0) for v in p50]
        p90_vals = [max(v * 1.30, 0.0) for v in p50]
        p95_vals = [max(v * 1.40, 0.0) for v in p50]
        return {
            "selected_band_label": "P10/P90",
            "selected_lower_label": "P10",
            "selected_upper_label": "P90",
            "selected_lower_quantities": [round(v, 2) for v in p10_vals],
            "selected_upper_quantities": [round(v, 2) for v in p90_vals],
            "p5_quantities":  [round(v, 2) for v in p5_vals],
            "p10_quantities": [round(v, 2) for v in p10_vals],
            "p50_quantities": [round(v, 2) for v in p50],
            "p90_quantities": [round(v, 2) for v in p90_vals],
            "p95_quantities": [round(v, 2) for v in p95_vals],
            "selected_band_coverage_pct": None,
            "selected_band_target_coverage_pct": 80.0,
            "selected_band_fit_score": None,
            "band_selection_window_label": "",
            "candidate_bands": [],
        }

    err_array = np.array(pct_errors)
    p50_adj = float(np.percentile(err_array, 50))
    p50_vals = [max(v * (1.0 + p50_adj), 0.0) for v in p50]

    candidate_specs = [
        ("P10/P90", "P10", "P90", 10, 90, 80.0),
        ("P5/P95", "P5", "P95", 5, 95, 90.0),
    ]
    candidate_bands: list[dict] = []

    for band_label, lower_label, upper_label, lower_q, upper_q, target_coverage in candidate_specs:
        lower_adj = float(np.percentile(err_array, lower_q))
        upper_adj = float(np.percentile(err_array, upper_q))

        lower_vals = [max(v * (1.0 + lower_adj), 0.0) for v in p50]
        upper_vals = [max(v * (1.0 + upper_adj), 0.0) for v in p50]
        for idx in range(len(p50_vals)):
            lower_vals[idx] = min(lower_vals[idx], p50_vals[idx])
            upper_vals[idx] = max(upper_vals[idx], p50_vals[idx])

        in_band = 0
        miss_gap_pcts: list[float] = []
        width_pcts: list[float] = []
        backtest_rows = 0
        for t in range(eval_start, n):
            train = quantities[:t]
            train_labels = labels[:t]
            actual = float(quantities[t])
            if len(train) < 4:
                continue
            try:
                forecasted, _ = forecast_func(train, train_labels, horizon=1)
                pred = max(_safe_float(forecasted[0] if forecasted else 0.0), 0.0)
            except Exception:
                continue
            p50_bt = max(pred * (1.0 + p50_adj), 0.0)
            low = min(max(pred * (1.0 + lower_adj), 0.0), p50_bt)
            high = max(max(pred * (1.0 + upper_adj), 0.0), p50_bt)
            denom = max(actual, p50_bt, 1e-9)
            width_pcts.append(((high - low) / denom) * 100.0)
            if low <= actual <= high:
                in_band += 1
                miss_gap_pcts.append(0.0)
            elif actual < low:
                miss_gap_pcts.append(((low - actual) / denom) * 100.0)
            else:
                miss_gap_pcts.append(((actual - high) / denom) * 100.0)
            backtest_rows += 1

        coverage_pct = (in_band / backtest_rows * 100.0) if backtest_rows else 0.0
        avg_width_pct = float(np.mean(width_pcts)) if width_pcts else 0.0
        avg_miss_gap_pct = float(np.mean(miss_gap_pcts)) if miss_gap_pcts else 0.0
        fit_score = abs(coverage_pct - target_coverage) + (0.10 * avg_width_pct) + avg_miss_gap_pct
        candidate_bands.append(
            {
                "label": band_label,
                "lower_label": lower_label,
                "upper_label": upper_label,
                "lower_quantities": [round(v, 2) for v in lower_vals],
                "upper_quantities": [round(v, 2) for v in upper_vals],
                "target_coverage_pct": target_coverage,
                "coverage_pct": round(coverage_pct, 1),
                "avg_width_pct": round(avg_width_pct, 2),
                "avg_miss_gap_pct": round(avg_miss_gap_pct, 2),
                "fit_score": round(fit_score, 2),
            }
        )

    candidate_bands = sorted(candidate_bands, key=lambda item: (item["fit_score"], item["avg_width_pct"]))
    selected = candidate_bands[0]
    for band in candidate_bands:
        band["is_selected"] = band["label"] == selected["label"]

    # Extract all 5 percentiles explicitly from the two candidate bands
    band_map: dict[str, dict] = {b["label"]: b for b in candidate_bands}
    b_p10p90 = band_map.get("P10/P90", candidate_bands[0])
    b_p5p95  = band_map.get("P5/P95",  candidate_bands[-1])
    p10_quantities = b_p10p90["lower_quantities"]
    p90_quantities = b_p10p90["upper_quantities"]
    p5_quantities  = b_p5p95["lower_quantities"]
    p95_quantities = b_p5p95["upper_quantities"]
    p50_list = [round(v, 2) for v in p50_vals]

    window_label = f"{labels[eval_start]} to {labels[-1]}" if n > eval_start else ""
    return {
        "selected_band_label": selected["label"],
        "selected_lower_label": selected["lower_label"],
        "selected_upper_label": selected["upper_label"],
        "selected_lower_quantities": selected["lower_quantities"],
        "selected_upper_quantities": selected["upper_quantities"],
        "p5_quantities":  p5_quantities,
        "p10_quantities": p10_quantities,
        "p50_quantities": p50_list,
        "p90_quantities": p90_quantities,
        "p95_quantities": p95_quantities,
        "selected_band_coverage_pct": selected["coverage_pct"],
        "selected_band_target_coverage_pct": selected["target_coverage_pct"],
        "selected_band_fit_score": selected["fit_score"],
        "band_selection_window_label": window_label,
        "candidate_bands": candidate_bands,
    }


def _build_forecast(monthly: pd.DataFrame) -> dict:
    history_labels = monthly.get("label", pd.Series(dtype=str)).tolist()
    history_quantities = [round(_safe_float(v), 2) for v in monthly.get("qty", pd.Series(dtype=float)).tolist()]

    _empty_forecast_stub = {
        "forecast_labels": [],
        "forecast_quantities": [],
        "projected_total_qty": 0.0,
        "avg_forecast_qty": 0.0,
        "selected_band_label": "",
        "selected_lower_label": "",
        "selected_upper_label": "",
        "selected_lower_quantities": [],
        "selected_upper_quantities": [],
        "p5_quantities": [],
        "p10_quantities": [],
        "p50_quantities": [],
        "p90_quantities": [],
        "p95_quantities": [],
        "lower_total_qty": 0.0,
        "p50_total_qty": 0.0,
        "upper_total_qty": 0.0,
        "p5_total_qty": 0.0,
        "p95_total_qty": 0.0,
        "buffer_total_qty": 0.0,
        "band_selection_window_label": "",
        "selected_band_coverage_pct": None,
        "selected_band_target_coverage_pct": None,
        "selected_band_fit_score": None,
        "candidate_bands": [],
        "forecast_rows": [],
    }

    if monthly.empty or len(monthly) < _MIN_HISTORY_MONTHS:
        return {
            "method": "Insufficient history",
            "history_labels": history_labels,
            "history_quantities": history_quantities,
            "consistency_message": f"At least {_MIN_HISTORY_MONTHS} months of history are required for forecasting.",
            "recent_active_months": int((monthly.get("qty", pd.Series(dtype=float)).tail(6) > 0).sum()) if not monthly.empty else 0,
            **_empty_forecast_stub,
        }

    quantities = monthly["qty"].to_numpy(dtype=float)
    recent_active_months = int((monthly["qty"].tail(6) > 0).sum())

    # Relaxed gate: allow sporadic/lumpy items through (>=3 active months)
    if recent_active_months < _MIN_RECENT_ACTIVE:
        return {
            "method": "No consistent data for forecasting",
            "history_labels": history_labels,
            "history_quantities": history_quantities,
            "consistency_message": f"No consistent data for forecasting. At least {_MIN_RECENT_ACTIVE} of the last 6 months must have active consumption.",
            "recent_active_months": recent_active_months,
            **_empty_forecast_stub,
        }

    # --- Adaptive ensemble forecasting ---
    demand_type = _classify_demand(quantities)

    # Generate ensemble forecast
    def _ensemble_func(q, lbl, horizon=6):
        return _ensemble_forecast(q, lbl, horizon=horizon)

    forecast_values, ensemble_meta = _ensemble_forecast(quantities, history_labels, horizon=6)

    forecast_array = np.asarray(forecast_values, dtype=float)
    last_period = monthly["period"].iloc[-1]
    forecast_periods = pd.period_range(last_period + 1, periods=6, freq="M")
    prediction_bands = _compute_prediction_bands(
        quantities, history_labels, _ensemble_func,
        forecast_values, horizon=6,
    )
    lower_quantities = prediction_bands["selected_lower_quantities"]
    p50_quantities   = prediction_bands["p50_quantities"]
    upper_quantities = prediction_bands["selected_upper_quantities"]
    p5_quantities    = prediction_bands["p5_quantities"]
    p10_quantities   = prediction_bands["p10_quantities"]
    p90_quantities   = prediction_bands["p90_quantities"]
    p95_quantities   = prediction_bands["p95_quantities"]
    forecast_rows = []
    for idx, label in enumerate([_format_period_label(period.year, period.month) for period in forecast_periods]):
        p5_qty  = _safe_float(p5_quantities[idx])  if idx < len(p5_quantities)  else 0.0
        p10_qty = _safe_float(p10_quantities[idx]) if idx < len(p10_quantities) else 0.0
        p50_qty = _safe_float(p50_quantities[idx]) if idx < len(p50_quantities) else 0.0
        p90_qty = _safe_float(p90_quantities[idx]) if idx < len(p90_quantities) else 0.0
        p95_qty = _safe_float(p95_quantities[idx]) if idx < len(p95_quantities) else 0.0
        forecast_rows.append(
            {
                "period": label,
                "p5_qty":  round(p5_qty,  2),
                "p10_qty": round(p10_qty, 2),
                "p50_qty": round(p50_qty, 2),
                "p90_qty": round(p90_qty, 2),
                "p95_qty": round(p95_qty, 2),
                "buffer_qty": round(max(p95_qty - p5_qty, 0.0), 2),
                # legacy aliases for any existing callers
                "lower_qty": round(p10_qty, 2),
                "upper_qty": round(p90_qty, 2),
            }
        )

    method_label = f"Adaptive ensemble ({ensemble_meta.get('selected_method', 'blend')})"
    if demand_type in ("sporadic", "lumpy"):
        method_label += f" [intermittent: {demand_type}]"

    return {
        "method": method_label,
        "history_labels": monthly["label"].tolist(),
        "history_quantities": [round(_safe_float(v), 2) for v in monthly["qty"].tolist()],
        "forecast_labels": [_format_period_label(period.year, period.month) for period in forecast_periods],
        "forecast_quantities": [round(_safe_float(v), 2) for v in p50_quantities],
        "projected_total_qty": round(sum(p50_quantities), 2),
        "avg_forecast_qty": round((sum(p50_quantities) / len(p50_quantities)) if p50_quantities else 0.0, 2),
        "consistency_message": f"Adaptive {demand_type} forecast using the backtested {prediction_bands.get('selected_band_label', '')} interval.",
        "recent_active_months": recent_active_months,
        "selected_band_label": prediction_bands["selected_band_label"],
        "selected_lower_label": prediction_bands["selected_lower_label"],
        "selected_upper_label": prediction_bands["selected_upper_label"],
        "selected_lower_quantities": lower_quantities,
        "selected_upper_quantities": upper_quantities,
        "p5_quantities":  p5_quantities,
        "p10_quantities": p10_quantities,
        "p50_quantities": p50_quantities,
        "p90_quantities": p90_quantities,
        "p95_quantities": p95_quantities,
        "lower_total_qty": round(sum(lower_quantities), 2),
        "p50_total_qty": round(sum(p50_quantities), 2),
        "upper_total_qty": round(sum(upper_quantities), 2),
        "p5_total_qty":  round(sum(_safe_float(v) for v in p5_quantities), 2),
        "p95_total_qty": round(sum(_safe_float(v) for v in p95_quantities), 2),
        "buffer_total_qty": round(sum(max(_safe_float(p95_quantities[i]) - _safe_float(p5_quantities[i]), 0.0) for i in range(len(forecast_periods))), 2),
        "band_selection_window_label": prediction_bands["band_selection_window_label"],
        "selected_band_coverage_pct": prediction_bands["selected_band_coverage_pct"],
        "selected_band_target_coverage_pct": prediction_bands["selected_band_target_coverage_pct"],
        "selected_band_fit_score": prediction_bands["selected_band_fit_score"],
        "candidate_bands": prediction_bands["candidate_bands"],
        "forecast_rows": forecast_rows,
        "demand_type": demand_type,
        "ensemble_meta": ensemble_meta,
    }


def _build_yoy(cons_monthly: pd.DataFrame, proc_monthly: pd.DataFrame) -> dict:
    if cons_monthly.empty and proc_monthly.empty:
        return {"records": []}

    qty_yearly = (
        cons_monthly.groupby("year", as_index=False)
        .agg(
            total_qty=("qty", "sum"),
            months_observed=("month", "nunique"),
            active_months=("qty", lambda values: int((values > 0).sum())),
        )
        .sort_values("year")
    )
    proc_yearly = (
        proc_monthly.groupby("year", as_index=False)
        .agg(total_procurement_value=("value", "sum"))
        .sort_values("year")
    )

    if qty_yearly.empty:
        yearly = proc_yearly.copy()
        yearly["months_observed"] = 0
        yearly["active_months"] = 0
        yearly["total_qty"] = 0.0
    else:
        yearly = qty_yearly.copy()

    yearly = yearly.merge(proc_yearly, on="year", how="outer").fillna(
        {
            "months_observed": 0,
            "active_months": 0,
            "total_qty": 0.0,
            "total_procurement_value": 0.0,
        }
    ).sort_values("year")
    yearly["avg_monthly_qty"] = yearly["total_qty"] / yearly["months_observed"].replace(0, pd.NA)
    yearly["avg_monthly_qty"] = yearly["avg_monthly_qty"].fillna(0.0)

    records: list[dict] = []
    previous = None
    for _, row in yearly.iterrows():
        qty_change = None
        value_change = None
        if previous is not None and int(previous["months_observed"]) == int(row["months_observed"]) and int(row["months_observed"]) > 0:
            if _safe_float(previous["total_qty"]) > 0:
                qty_change = ((_safe_float(row["total_qty"]) - _safe_float(previous["total_qty"])) / _safe_float(previous["total_qty"])) * 100
        if previous is not None and _safe_float(previous["total_procurement_value"]) > 0 and _safe_float(row["total_procurement_value"]) > 0:
            value_change = (
                (_safe_float(row["total_procurement_value"]) - _safe_float(previous["total_procurement_value"]))
                / _safe_float(previous["total_procurement_value"])
            ) * 100
        records.append(
            {
                "year": int(row["year"]),
                "months_observed": int(row["months_observed"]),
                "active_months": int(row["active_months"]),
                "total_qty": round(_safe_float(row["total_qty"]), 2),
                "total_value": round(_safe_float(row["total_procurement_value"]), 2),
                "avg_monthly_qty": round(_safe_float(row["avg_monthly_qty"]), 2),
                "yoy_qty_pct": round(_safe_float(qty_change), 2) if qty_change is not None else None,
                "yoy_value_pct": round(_safe_float(value_change), 2) if value_change is not None else None,
            }
        )
        previous = row

    return {"records": records}


def _build_vendor_scores(proc_df: pd.DataFrame) -> list[dict]:
    pricing = proc_df.dropna(subset=["vendor", "unit_price", "doc_date"]).copy()
    if pricing.empty:
        return []

    grouped = pricing.groupby("vendor", as_index=False)
    summary = grouped.agg(
        avg_unit_price=("unit_price", "mean"),
        lowest_unit_price=("unit_price", "min"),
        highest_unit_price=("unit_price", "max"),
        latest_doc_date=("doc_date", "max"),
        transactions=("po_number", "count"),
        spend=("effective_value", "sum"),
    )
    latest_prices = (
        pricing.sort_values("doc_date")
        .groupby("vendor")
        .tail(1)[["vendor", "unit_price"]]
        .rename(columns={"unit_price": "latest_unit_price"})
    )
    volatility = (
        pricing.groupby("vendor")["unit_price"]
        .std(ddof=0)
        .fillna(0.0)
        .reset_index()
        .rename(columns={"unit_price": "price_std"})
    )
    summary = summary.merge(latest_prices, on="vendor", how="left")
    summary = summary.merge(volatility, on="vendor", how="left")

    best_avg_price = summary["avg_unit_price"].min()
    total_spend = summary["spend"].sum() or 1.0

    records: list[dict] = []
    for _, row in summary.sort_values(["avg_unit_price", "transactions", "vendor"]).iterrows():
        avg_unit_price = _safe_float(row["avg_unit_price"])
        price_std = _safe_float(row["price_std"])
        volatility_pct = (price_std / avg_unit_price * 100) if avg_unit_price else 0.0
        competitiveness = (best_avg_price / avg_unit_price) if avg_unit_price else 0.0
        reliability = min(_safe_float(row["transactions"]) / 5.0, 1.0)
        consistency = max(0.0, 1.0 - min(volatility_pct / 100.0, 1.0))
        score = 100.0 * ((0.7 * competitiveness) + (0.2 * consistency) + (0.1 * reliability))
        premium_pct = ((avg_unit_price - best_avg_price) / best_avg_price * 100) if best_avg_price else 0.0

        records.append(
            {
                "vendor": row["vendor"],
                "transactions": int(row["transactions"]),
                "avg_unit_price": round(avg_unit_price, 2),
                "latest_unit_price": round(_safe_float(row["latest_unit_price"]), 2),
                "lowest_unit_price": round(_safe_float(row["lowest_unit_price"]), 2),
                "highest_unit_price": round(_safe_float(row["highest_unit_price"]), 2),
                "volatility_pct": round(volatility_pct, 2),
                "premium_vs_best_pct": round(premium_pct, 2),
                "spend_share_pct": round((_safe_float(row["spend"]) / total_spend) * 100, 2),
                "latest_doc_date": row["latest_doc_date"].strftime("%d %b %Y") if pd.notna(row["latest_doc_date"]) else "",
                "score": round(max(0.0, min(score, 100.0)), 1),
            }
        )
    return records

def _build_cost_variance(proc_df: pd.DataFrame) -> dict:
    timeline = proc_df.dropna(subset=["doc_date", "unit_price"]).copy()
    if timeline.empty:
        return {
            "threshold_pct": _PRICE_SWING_THRESHOLD_PCT,
            "latest_unit_price": 0.0,
            "avg_unit_price": 0.0,
            "max_increase_pct": 0.0,
            "max_decrease_pct": 0.0,
            "significant_events": [],
            "significant_event_count": 0,
        }

    timeline = (
        timeline.groupby(["doc_date", "po_number", "vendor"], as_index=False)
        .agg(
            unit_price=("unit_price", "mean"),
            order_qty=("order_qty", "sum"),
            effective_value=("effective_value", "sum"),
        )
        .sort_values(["doc_date", "po_number", "vendor"])
    )

    events: list[dict] = []
    previous = None
    for _, row in timeline.iterrows():
        if previous is not None:
            prev_price = _safe_float(previous["unit_price"])
            current_price = _safe_float(row["unit_price"])
            pct_change = 0.0
            if prev_price > 0:
                pct_change = ((current_price - prev_price) / prev_price) * 100
            events.append(
                {
                    "from_date": previous["doc_date"].strftime("%d %b %Y"),
                    "to_date": row["doc_date"].strftime("%d %b %Y"),
                    "from_vendor": previous["vendor"],
                    "to_vendor": row["vendor"],
                    "from_price": round(prev_price, 2),
                    "to_price": round(current_price, 2),
                    "pct_change": round(pct_change, 2),
                    "direction": "Increase" if pct_change >= 0 else "Decrease",
                    "significant": abs(pct_change) >= _PRICE_SWING_THRESHOLD_PCT,
                }
            )
        previous = row

    significant_events = sorted(
        [event for event in events if event["significant"]],
        key=lambda event: abs(event["pct_change"]),
        reverse=True,
    )
    pct_changes = [event["pct_change"] for event in events]

    return {
        "threshold_pct": _PRICE_SWING_THRESHOLD_PCT,
        "latest_unit_price": round(_safe_float(timeline["unit_price"].iloc[-1]), 2),
        "avg_unit_price": round(_safe_float(timeline["unit_price"].mean()), 2),
        "max_increase_pct": round(max([change for change in pct_changes if change > 0] or [0.0]), 2),
        "max_decrease_pct": round(min([change for change in pct_changes if change < 0] or [0.0]), 2),
        "significant_events": significant_events[:8],
        "significant_event_count": len(significant_events),
    }


def _build_storage_breakdown(cons_df: pd.DataFrame, plant: str | None) -> list[dict]:
    if cons_df.empty:
        return []

    group_fields = ["storage_location"] if plant else ["plant", "storage_location"]
    grouped = (
        cons_df.groupby(group_fields, as_index=False)
        .agg(
            total_qty=("usage_qty", "sum"),
            active_months=("month_raw", "nunique"),
        )
        .sort_values("total_qty", ascending=False)
    )
    total_qty = grouped["total_qty"].sum() or 1.0

    records: list[dict] = []
    for _, row in grouped.head(12).iterrows():
        storage_location = _normalize_text(row.get("storage_location"))
        plant_code = _normalize_text(row.get("plant")) if not plant else plant
        label = storage_location if plant else f"{plant_code} / {storage_location}"
        records.append(
            {
                "plant": plant_code,
                "storage_location": storage_location,
                "label": label,
                "total_qty": round(_safe_float(row["total_qty"]), 2),
                "active_months": int(row["active_months"]),
                "share_qty_pct": round((_safe_float(row["total_qty"]) / total_qty) * 100, 2),
            }
        )
    return records


def _filter_material_rows(materials: list[dict], query: str = "") -> list[dict]:
    query = _normalize_text(query).lower()
    if not query:
        return list(materials)
    filtered = []
    for material in materials:
        haystacks = [
            _normalize_text(material.get("material")).lower(),
            _normalize_text(material.get("material_code")).lower(),
            " ".join(material.get("vendors", [])).lower(),
        ]
        if any(query in haystack for haystack in haystacks):
            filtered.append(material)
    return filtered


class _DataStore:
    """Lazy-loaded, cached data store for inventory intelligence."""

    def __init__(self):
        self._cons: pd.DataFrame | None = None
        self._proc: pd.DataFrame | None = None
        self._dashboard_cache: dict[str, dict] = {}

    def _ensure_loaded(self):
        if self._cons is None:
            logger.info("Loading consumption seed data from %s", CONS_FILE)
            self._cons = _load_consumption()
            logger.info("Loaded %d consumption records", len(self._cons))
        if self._proc is None:
            logger.info("Loading procurement seed data from %s", PROC_FILE)
            self._proc = _load_procurement()
            logger.info("Loaded %d procurement records", len(self._proc))

    @property
    def cons(self) -> pd.DataFrame:
        self._ensure_loaded()
        return self._cons

    @property
    def proc(self) -> pd.DataFrame:
        self._ensure_loaded()
        return self._proc

    def reload(self):
        self._cons = None
        self._proc = None
        self._dashboard_cache = {}

    def _cache_key(self, plant: str | None) -> str:
        return plant or "__all__"

    def get_filter_options(self) -> dict:
        self._ensure_loaded()
        plants = sorted(self._cons["plant"].dropna().astype(str).unique().tolist())
        return {"plants": plants}

    def _filtered_consumption(self, plant: str | None = None) -> pd.DataFrame:
        cons = self.cons
        normalized_plant = _normalize_plant(plant)
        if not normalized_plant:
            return cons
        return cons[cons["plant"] == normalized_plant].copy()

    def _filtered_procurement(self, plant: str | None = None) -> pd.DataFrame:
        proc = self.proc
        normalized_plant = _normalize_plant(plant)
        if not normalized_plant:
            return proc
        return proc[proc["plant"] == normalized_plant].copy()

    def _build_materials_table(self, cons: pd.DataFrame, proc: pd.DataFrame) -> list[dict]:
        if cons.empty:
            return []

        total_distinct_months = cons[["year", "month"]].drop_duplicates().shape[0] or 1
        proc_sorted = proc.sort_values("doc_date", ascending=False, na_position="last")

        mat_agg = (
            cons.groupby("material_desc", as_index=False)
            .agg(
                material_code=("material_code", "first"),
                uom=("uom", "first"),
                total_qty=("usage_qty", "sum"),
                n_months=("month_raw", "nunique"),
                plant_count=("plant", "nunique"),
                plants=("plant", lambda values: sorted(values.dropna().unique().tolist())),
                storage_location_count=("storage_location", "nunique"),
                currency=("currency", "first"),
            )
            .rename(columns={"material_desc": "material"})
        )
        mat_agg["avg_monthly_consumption"] = mat_agg["total_qty"] / total_distinct_months
        mat_agg["avg_annual_consumption"] = mat_agg["avg_monthly_consumption"] * 12

        last_proc = (
            proc_sorted.groupby("material_desc", as_index=False)
            .first()[
                [
                    "material_desc",
                    "order_qty",
                    "effective_value",
                    "po_number",
                    "doc_date",
                    "vendor",
                    "order_unit",
                    "unit_price",
                ]
            ]
            .rename(
                columns={
                    "material_desc": "material",
                    "order_qty": "last_proc_qty",
                    "effective_value": "last_proc_value",
                    "po_number": "last_po_number",
                    "doc_date": "last_proc_date",
                    "vendor": "last_vendor",
                    "order_unit": "last_proc_unit",
                    "unit_price": "last_unit_price",
                }
            )
        )
        vendor_list = (
            proc.groupby("material_desc")["vendor"]
            .apply(lambda values: sorted([value for value in values.dropna().unique().tolist() if value]))
            .reset_index()
            .rename(columns={"material_desc": "material", "vendor": "vendors"})
        )
        proc_totals = (
            proc.groupby("material_desc", as_index=False)
            .agg(
                total_value=("effective_value", "sum"),
                currency_proc=("currency", "first"),
            )
            .rename(columns={"material_desc": "material"})
        )

        merged = mat_agg.merge(last_proc, on="material", how="left")
        merged = merged.merge(vendor_list, on="material", how="left")
        merged = merged.merge(proc_totals, on="material", how="left")
        merged["total_value"] = merged["total_value"].fillna(0.0)
        merged["currency"] = merged["currency_proc"].fillna(merged["currency"])
        merged = merged.sort_values("total_value", ascending=False)

        materials: list[dict] = []
        for _, row in merged.iterrows():
            material_name = row["material"]
            cons_mat = cons[cons["material_desc"] == material_name]
            proc_mat = proc[proc["material_desc"] == material_name]
            forecast = _build_forecast(_monthly_series(cons_mat))
            vendor_scores = _build_vendor_scores(proc_mat)
            cost_variance = _build_cost_variance(proc_mat)
            avg_vendor_score = round(
                _safe_float(np.mean([score["score"] for score in vendor_scores])) if vendor_scores else 0.0,
                1,
            )
            dominant_swing = max(
                cost_variance["significant_events"],
                key=lambda event: abs(event["pct_change"]),
                default=None,
            )
            price_swing_pct = abs(_safe_float(dominant_swing["pct_change"])) if dominant_swing else 0.0
            price_swing_direction = ""
            if dominant_swing:
                if _safe_float(dominant_swing["pct_change"]) > 0:
                    price_swing_direction = "up"
                elif _safe_float(dominant_swing["pct_change"]) < 0:
                    price_swing_direction = "down"

            materials.append(
                {
                    "material": material_name,
                    "material_code": _normalize_text(row.get("material_code")),
                    "uom": _normalize_text(row.get("uom")),
                    "avg_monthly_consumption": round(_safe_float(row.get("avg_monthly_consumption")), 2),
                    "avg_annual_consumption": round(_safe_float(row.get("avg_annual_consumption")), 2),
                    "total_qty": round(_safe_float(row.get("total_qty")), 2),
                    "total_value": round(_safe_float(row.get("total_value")), 2),
                    "n_months": int(row.get("n_months", 0)),
                    "currency": _clean_currency(row.get("currency")),
                    "last_proc_qty": round(_safe_float(row.get("last_proc_qty")), 2),
                    "last_proc_value": round(_safe_float(row.get("last_proc_value")), 2),
                    "last_po_number": _normalize_text(row.get("last_po_number")),
                    "last_proc_date": row["last_proc_date"].strftime("%d %b %Y") if pd.notna(row.get("last_proc_date")) else "",
                    "last_vendor": _normalize_text(row.get("last_vendor")),
                    "last_proc_unit": _normalize_text(row.get("last_proc_unit")),
                    "last_unit_price": round(_safe_float(row.get("last_unit_price")), 2),
                    "vendors": row.get("vendors", []) if isinstance(row.get("vendors"), list) else [],
                    "vendor_count": len(row.get("vendors", []) if isinstance(row.get("vendors"), list) else []),
                    "plants": row.get("plants", []) if isinstance(row.get("plants"), list) else [],
                    "plant_count": int(row.get("plant_count", 0)),
                    "storage_location_count": int(row.get("storage_location_count", 0)),
                    "forecast_6m_qty": round(_safe_float(forecast["projected_total_qty"]), 2),
                    "forecast_method": forecast["method"],
                    "avg_vendor_score": avg_vendor_score,
                    "max_price_swing_pct": round(price_swing_pct, 2),
                    "max_price_swing_direction": price_swing_direction,
                    "significant_price_swings": int(cost_variance["significant_event_count"]),
                }
            )
        return materials

    def get_materials_table(self, plant: str | None = None) -> list[dict]:
        payload = self.get_dashboard_payload(plant=plant)
        return payload["materials"]

    def get_top_consumers(self, plant: str | None = None, limit: int = 10) -> list[dict]:
        materials = self.get_materials_table(plant=plant)
        return [
            {
                "material": material["material"],
                "material_code": material["material_code"],
                "total_value": material["total_value"],
                "total_qty": material["total_qty"],
                "uom": material["uom"],
            }
            for material in materials[:limit]
        ]

    def get_kpi_summary(self, plant: str | None = None) -> dict:
        return self.get_dashboard_payload(plant=plant)["kpis"]

    def get_dashboard_payload(self, plant: str | None = None) -> dict:
        self._ensure_loaded()
        normalized_plant = _normalize_plant(plant)
        cache_key = self._cache_key(normalized_plant)
        if cache_key in self._dashboard_cache:
            return self._dashboard_cache[cache_key]

        cons = self._filtered_consumption(normalized_plant)
        proc = self._filtered_procurement(normalized_plant)
        materials = self._build_materials_table(cons, proc)

        min_year = cons["year"].min() if not cons.empty else None
        min_month = cons.loc[cons["year"] == min_year, "month"].min() if pd.notna(min_year) else None
        max_year = cons["year"].max() if not cons.empty else None
        max_month = cons.loc[cons["year"] == max_year, "month"].max() if pd.notna(max_year) else None
        period_from = _format_period_label(int(min_year), int(min_month)) if pd.notna(min_year) and pd.notna(min_month) else ""
        period_to = _format_period_label(int(max_year), int(max_month)) if pd.notna(max_year) and pd.notna(max_month) else ""

        all_vendors = set()
        for material in materials:
            all_vendors.update(material.get("vendors", []))

        kpis = {
            "total_materials": len(materials),
            "total_vendors": len(all_vendors),
            "total_procurement_value": round(sum(material["total_value"] for material in materials), 2),
            "period_from": period_from,
            "period_to": period_to,
            "unique_plants": int(cons["plant"].nunique()) if not cons.empty else 0,
            "data_points": int(len(cons)),
            "total_months": int(cons[["year", "month"]].drop_duplicates().shape[0]) if not cons.empty else 0,
            "selected_plant": normalized_plant or "",
        }

        payload = {
            "kpis": kpis,
            "materials": materials,
            "top_consumers": self.get_top_consumers_from_materials(materials),
            "filters": self.get_filter_options(),
        }
        self._dashboard_cache[cache_key] = payload
        return payload

    def get_top_consumers_from_materials(self, materials: list[dict], limit: int = 10) -> list[dict]:
        return [
            {
                "material": material["material"],
                "material_code": material["material_code"],
                "total_value": material["total_value"],
                "total_qty": material["total_qty"],
                "uom": material["uom"],
            }
            for material in materials[:limit]
        ]

    def get_monthly_consumption(self, material: str, plant: str | None = None) -> dict:
        cons = self._filtered_consumption(plant)
        df = cons[cons["material_desc"] == material.upper()].copy()
        monthly = _monthly_series(df)
        return {
            "labels": monthly.get("label", pd.Series(dtype=str)).tolist(),
            "quantities": [round(_safe_float(value), 2) for value in monthly.get("qty", pd.Series(dtype=float)).tolist()],
            "values": [round(_safe_float(value), 2) for value in monthly.get("value", pd.Series(dtype=float)).tolist()],
        }

    def get_monthly_consumption_detail(self, material: str, plant: str | None = None) -> list[dict]:
        cons = self._filtered_consumption(plant)
        proc = self._filtered_procurement(plant)
        df = cons[cons["material_desc"] == material.upper()].copy()
        proc_df = proc[proc["material_desc"] == material.upper()].copy()
        qty_rows = _actual_monthly_rows(df).rename(columns={"value": "consumption_value"})
        proc_monthly = _monthly_procurement_series(proc_df).rename(columns={"value": "procurement_value"})
        monthly = qty_rows.merge(
            proc_monthly[["year", "month", "procurement_value"]],
            on=["year", "month"],
            how="outer",
        ).sort_values(["year", "month"], ascending=[False, False])
        monthly["qty"] = monthly["qty"].fillna(0.0)
        monthly["procurement_value"] = monthly["procurement_value"].fillna(0.0)
        monthly["uom"] = monthly["uom"].fillna(df["uom"].iloc[0] if not df.empty else "")
        return [
            {
                "period": _format_period_label(int(row["year"]), int(row["month"])),
                "year": int(row["year"]),
                "month": int(row["month"]),
                "quantity": round(_safe_float(row["qty"]), 2),
                "value": round(_safe_float(row["procurement_value"]), 2),
                "uom": _normalize_text(row["uom"]),
            }
            for _, row in monthly.iterrows()
        ]

    def get_procurement_history(self, material: str, plant: str | None = None) -> list[dict]:
        proc = self._filtered_procurement(plant)
        df = proc[proc["material_desc"] == material.upper()].copy()
        if df.empty:
            return []
        df = df.sort_values("doc_date", ascending=False, na_position="last")
        return [
            {
                "po_number": _normalize_text(row.get("po_number")),
                "doc_date": row["doc_date"].strftime("%d %b %Y") if pd.notna(row.get("doc_date")) else "",
                "vendor": _normalize_text(row.get("vendor")),
                "order_qty": round(_safe_float(row.get("order_qty")), 2),
                "order_unit": _normalize_text(row.get("order_unit")),
                "net_price": round(_safe_float(row.get("net_price")), 2),
                "unit_price": round(_safe_float(row.get("unit_price")), 2),
                "price_unit": round(_safe_float(row.get("price_unit"), 1.0), 2),
                "effective_value": round(_safe_float(row.get("effective_value")), 2),
                "plant": _normalize_text(row.get("plant")),
                "currency": _clean_currency(row.get("currency")),
            }
            for _, row in df.iterrows()
        ]

    def get_material_analytics(self, material: str, plant: str | None = None) -> dict:
        normalized_material = material.upper()
        cons = self._filtered_consumption(plant)
        proc = self._filtered_procurement(plant)
        cons_df = cons[cons["material_desc"] == normalized_material].copy()
        proc_df = proc[proc["material_desc"] == normalized_material].copy()
        monthly = _monthly_series(cons_df)
        proc_monthly = _monthly_procurement_series(proc_df)
        forecast = _build_forecast(monthly)
        yoy = _build_yoy(monthly, proc_monthly)
        vendor_scores = _build_vendor_scores(proc_df)
        cost_variance = _build_cost_variance(proc_df)
        storage_breakdown = _build_storage_breakdown(cons_df, _normalize_plant(plant))

        return {
            "material": normalized_material,
            "plant": _normalize_plant(plant) or "",
            "summary": {
                "total_qty": round(_safe_float(cons_df["usage_qty"].sum()), 2),
                "total_value": round(_safe_float(proc_df["effective_value"].sum()), 2),
                "active_months": int(cons_df["month_raw"].nunique()) if not cons_df.empty else 0,
                "storage_location_count": int(cons_df["storage_location"].nunique()) if not cons_df.empty else 0,
                "vendor_count": int(proc_df["vendor"].nunique()) if not proc_df.empty else 0,
                "last_unit_price": round(_safe_float(proc_df.sort_values("doc_date")["unit_price"].iloc[-1]), 2) if not proc_df.empty else 0.0,
            },
            "consumption": {
                "labels": monthly.get("label", pd.Series(dtype=str)).tolist(),
                "quantities": [round(_safe_float(value), 2) for value in monthly.get("qty", pd.Series(dtype=float)).tolist()],
                "values": [round(_safe_float(value), 2) for value in monthly.get("value", pd.Series(dtype=float)).tolist()],
            },
            "forecast": forecast,
            "yoy": yoy,
            "vendor_scores": vendor_scores,
            "storage_breakdown": storage_breakdown,
            "cost_variance": cost_variance,
        }

    def build_export_workbook(self, plant: str | None = None, query: str = "") -> tuple[BytesIO, str]:
        payload = self.get_dashboard_payload(plant=plant)
        materials = _filter_material_rows(payload["materials"], query)
        material_names = {material["material"] for material in materials}

        cons = self._filtered_consumption(plant)
        proc = self._filtered_procurement(plant)
        if material_names:
            cons = cons[cons["material_desc"].isin(material_names)].copy()
            proc = proc[proc["material_desc"].isin(material_names)].copy()
        else:
            cons = cons.iloc[0:0].copy()
            proc = proc.iloc[0:0].copy()

        workbook = Workbook()
        summary_sheet = workbook.active
        summary_sheet.title = "Materials Summary"

        summary_rows = [
            ["Plant Filter", _normalize_plant(plant) or "All Plants"],
            ["Search Filter", _normalize_text(query) or "None"],
            ["Value Basis", "Procurement effective value from SAP procurement export"],
            ["Records Exported", len(materials)],
            ["Consumption Rows", len(cons)],
            ["Procurement Rows", len(proc)],
            [],
        ]
        for row in summary_rows:
            summary_sheet.append(row)

        material_headers = [
            "Material",
            "Material Code",
            "UoM",
            "Avg Monthly Consumption",
            "Avg Annual Consumption",
            "Total Quantity",
            "Total Procurement Value",
            "Last Procurement Qty",
            "Last Procurement Value",
            "Last Vendor",
            "Last PO Number",
            "Last Procurement Date",
            "Vendor Score",
            "Forecast 6M Qty",
            "Max Price Swing %",
            "Price Swing Direction",
            "Plants",
        ]
        summary_sheet.append(material_headers)
        for material in materials:
            summary_sheet.append(
                [
                    material["material"],
                    material["material_code"],
                    material["uom"],
                    material["avg_monthly_consumption"],
                    material["avg_annual_consumption"],
                    material["total_qty"],
                    material["total_value"],
                    material["last_proc_qty"],
                    material["last_proc_value"],
                    material["last_vendor"],
                    material["last_po_number"],
                    material["last_proc_date"],
                    material["avg_vendor_score"],
                    material["forecast_6m_qty"],
                    material["max_price_swing_pct"],
                    material.get("max_price_swing_direction", ""),
                    ", ".join(material.get("plants", [])),
                ]
            )

        cons_sheet = workbook.create_sheet("Consumption Detail")
        cons_sheet.append(
            [
                "Plant",
                "Material Code",
                "Material",
                "Storage Location",
                "Month",
                "Usage Quantity",
                "UoM",
            ]
        )
        for _, row in cons.sort_values(["plant", "material_desc", "year", "month"]).iterrows():
            cons_sheet.append(
                [
                    _normalize_text(row.get("plant")),
                    _normalize_text(row.get("material_code")),
                    _normalize_text(row.get("material_desc")),
                    _normalize_text(row.get("storage_location")),
                    _normalize_text(row.get("month_raw")),
                    round(_safe_float(row.get("usage_qty")), 2),
                    _normalize_text(row.get("uom")),
                ]
            )

        proc_sheet = workbook.create_sheet("Procurement Detail")
        proc_sheet.append(
            [
                "Plant",
                "Material Code",
                "Material",
                "PO Number",
                "Document Date",
                "Vendor",
                "Order Quantity",
                "Order Unit",
                "Net Price",
                "Price Unit",
                "Unit Price",
                "Effective Value",
                "Currency",
            ]
        )
        for _, row in proc.sort_values(["plant", "material_desc", "doc_date"], ascending=[True, True, False]).iterrows():
            proc_sheet.append(
                [
                    _normalize_text(row.get("plant")),
                    _normalize_text(row.get("material_code")),
                    _normalize_text(row.get("material_desc")),
                    _normalize_text(row.get("po_number")),
                    row["doc_date"].strftime("%d %b %Y") if pd.notna(row.get("doc_date")) else "",
                    _normalize_text(row.get("vendor")),
                    round(_safe_float(row.get("order_qty")), 2),
                    _normalize_text(row.get("order_unit")),
                    round(_safe_float(row.get("net_price")), 2),
                    round(_safe_float(row.get("price_unit"), 1.0), 2),
                    round(_safe_float(row.get("unit_price")), 2),
                    round(_safe_float(row.get("effective_value")), 2),
                    _clean_currency(row.get("currency")),
                ]
            )

        self._format_export_sheet(summary_sheet, header_row=8)
        self._format_export_sheet(cons_sheet, header_row=1)
        self._format_export_sheet(proc_sheet, header_row=1)

        stream = BytesIO()
        workbook.save(stream)
        stream.seek(0)
        plant_suffix = (_normalize_plant(plant) or "all-plants").lower()
        filename = f"inventory-intelligence-{plant_suffix}.xlsx"
        return stream, filename

    def _format_export_sheet(self, sheet, header_row: int) -> None:
        header_fill = PatternFill("solid", fgColor="1F4E78")
        header_font = Font(color="FFFFFF", bold=True)
        for cell in sheet[header_row]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
        sheet.freeze_panes = f"A{header_row + 1}"
        sheet.auto_filter.ref = f"A{header_row}:{get_column_letter(sheet.max_column)}{sheet.max_row}"

        for column_cells in sheet.columns:
            max_length = 0
            column_letter = column_cells[0].column_letter
            for cell in column_cells:
                value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, len(value))
            sheet.column_dimensions[column_letter].width = min(max_length + 2, 32)


_store = _DataStore()


def get_data_store() -> _DataStore:
    return _store
