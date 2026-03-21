from __future__ import annotations

"""Percentile ladder, backtest scoring, and selection helpers.

This layer sits on top of an existing point-forecast / interval engine.
It does not replace the baseline forecast. Instead, it evaluates a ladder
of operational percentiles and selects the percentile that best balances
accuracy with stockout protection.
"""

from dataclasses import dataclass
from math import sqrt
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

try:
    import pandas as pd
except ImportError:  # pragma: no cover - pandas is available when inventory is enabled
    pd = None  # type: ignore[assignment]


DEFAULT_PERCENTILE_LADDER: tuple[int, ...] = (50, 55, 60, 65, 70, 75, 80, 85, 90)
DEFAULT_MIN_TRAIN_POINTS = 4
DEFAULT_BACKTEST_MONTHS = 60


@dataclass(frozen=True)
class PercentileScoreWeights:
    mae: float = 0.45
    bias: float = 0.20
    underforecast_penalty: float = 0.25
    rmse: float = 0.10
    underforecast_multiplier: float = 1.0


def build_percentile_ladder(
    base_percentiles: Mapping[int, float],
    ladder: Sequence[int] = DEFAULT_PERCENTILE_LADDER,
) -> dict[int, float]:
    """Interpolate a practical percentile ladder from existing forecast bands.

    The current forecast engine already produces P50 / P90 / P95. For the
    operating ladder between P50 and P90, linear interpolation is deliberate:
    it is explainable, stable, and easy to audit.
    """
    p50 = max(float(base_percentiles.get(50, 0.0)), 0.0)
    p90 = max(float(base_percentiles.get(90, p50)), p50)
    p95 = max(float(base_percentiles.get(95, p90)), p90)

    ladder_values: dict[int, float] = {}
    for percentile in ladder:
        if percentile <= 50:
            value = p50
        elif percentile <= 90:
            ratio = (percentile - 50) / 40.0
            value = p50 + ((p90 - p50) * ratio)
        else:
            ratio = (percentile - 90) / 5.0
            value = p90 + ((p95 - p90) * ratio)
        ladder_values[int(percentile)] = round(max(value, 0.0), 2)
    return ladder_values


def walk_forward_percentile_backtest(
    quantities: np.ndarray,
    labels: Sequence[str],
    candidate_builder,
    *,
    ladder: Sequence[int] = DEFAULT_PERCENTILE_LADDER,
    backtest_months: int = DEFAULT_BACKTEST_MONTHS,
    min_train_points: int = DEFAULT_MIN_TRAIN_POINTS,
) -> list[dict[str, Any]]:
    """Run rolling one-step walk-forward backtesting for candidate percentiles."""
    n = len(quantities)
    if n <= min_train_points:
        return []

    eval_start = max(n - backtest_months, min_train_points)
    backtest_rows: list[dict[str, Any]] = []

    for idx in range(eval_start, n):
        train = quantities[:idx]
        train_labels = list(labels[:idx])
        if len(train) < min_train_points:
            continue

        try:
            candidate_values = candidate_builder(train, train_labels, horizon=1)
        except Exception:
            continue

        if not candidate_values:
            continue

        predictions: dict[int, float] = {}
        for percentile in ladder:
            series = candidate_values.get(int(percentile), [])
            if not series:
                continue
            predictions[int(percentile)] = round(max(float(series[0]), 0.0), 2)

        if not predictions:
            continue

        actual = round(max(float(quantities[idx]), 0.0), 2)
        backtest_rows.append(
            {
                "period": labels[idx],
                "actual_qty": actual,
                "prev_actual_qty": round(max(float(quantities[idx - 1]), 0.0), 2) if idx > 0 else None,
                "predictions": predictions,
            }
        )

    return backtest_rows


def summarize_percentile_backtest(
    backtest_rows: Sequence[Mapping[str, Any]],
    *,
    material: str,
    material_code: str,
    plant: str,
    demand_type: str,
    ladder: Sequence[int] = DEFAULT_PERCENTILE_LADDER,
    weights: PercentileScoreWeights = PercentileScoreWeights(),
) -> list[dict[str, Any]]:
    """Summarize walk-forward rows into a percentile-scoring table."""
    summary_rows: list[dict[str, Any]] = []
    if not backtest_rows:
        return summary_rows

    actuals = np.asarray([max(float(row.get("actual_qty", 0.0)), 0.0) for row in backtest_rows], dtype=float)
    eval_points = int(len(actuals))

    for percentile in ladder:
        preds = np.asarray(
            [
                max(float((row.get("predictions") or {}).get(int(percentile), 0.0)), 0.0)
                for row in backtest_rows
            ],
            dtype=float,
        )
        errors = preds - actuals
        abs_errors = np.abs(errors)
        underforecast_mask = preds < actuals
        overforecast_mask = preds > actuals
        underforecast_penalty = np.maximum(actuals - preds, 0.0)

        mae = float(abs_errors.mean()) if eval_points else 0.0
        rmse = float(sqrt(np.square(errors).mean())) if eval_points else 0.0
        bias = float(errors.mean()) if eval_points else 0.0
        underforecast_pct = float(underforecast_mask.mean() * 100.0) if eval_points else 0.0
        overforecast_pct = float(overforecast_mask.mean() * 100.0) if eval_points else 0.0
        avg_underforecast_penalty = float(underforecast_penalty.mean()) if eval_points else 0.0

        weighted_score = (
            (weights.mae * mae)
            + (weights.bias * abs(bias))
            + (weights.underforecast_penalty * avg_underforecast_penalty * weights.underforecast_multiplier)
            + (weights.rmse * rmse)
        )

        summary_rows.append(
            {
                "material": material,
                "material_code": material_code,
                "plant": plant,
                "demand_type": demand_type,
                "percentile": int(percentile),
                "mae": round(mae, 2),
                "rmse": round(rmse, 2),
                "bias": round(bias, 2),
                "underforecast_pct": round(underforecast_pct, 2),
                "overforecast_pct": round(overforecast_pct, 2),
                "underforecast_penalty": round(avg_underforecast_penalty, 2),
                "weighted_score": round(weighted_score, 4),
                "evaluation_points": eval_points,
            }
        )

    return summary_rows


def aggregate_percentile_summary(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
) -> list[dict[str, Any]]:
    """Aggregate material-level percentile summaries to a wider grouping.

    This supports demand-type-level selection using the same scoring outputs.
    Metrics are weighted by evaluation point count for practical stability.
    """
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in summary_rows:
        key = tuple(row.get(field, "") for field in (*group_fields, "percentile"))
        grouped.setdefault(key, []).append(row)

    aggregated_rows: list[dict[str, Any]] = []
    metric_fields = (
        "mae",
        "rmse",
        "bias",
        "underforecast_pct",
        "overforecast_pct",
        "underforecast_penalty",
        "weighted_score",
    )

    for key, rows in grouped.items():
        total_points = sum(int(row.get("evaluation_points", 0) or 0) for row in rows)
        if total_points <= 0:
            total_points = len(rows)
        record = {
            field: key[idx]
            for idx, field in enumerate((*group_fields, "percentile"))
        }
        for metric in metric_fields:
            weighted_total = sum(float(row.get(metric, 0.0) or 0.0) * max(int(row.get("evaluation_points", 0) or 0), 1) for row in rows)
            record[metric] = round(weighted_total / max(total_points, 1), 4 if metric == "weighted_score" else 2)
        record["evaluation_points"] = int(total_points)
        aggregated_rows.append(record)

    return aggregated_rows


def select_best_percentile(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    material: str,
    material_code: str,
    plant: str,
    demand_type: str,
    backtest_window_start: str,
    backtest_window_end: str,
    selection_level: str = "material",
) -> dict[str, Any] | None:
    """Select the operational percentile using the composite score."""
    if not summary_rows:
        return None

    ranked = sorted(
        summary_rows,
        key=lambda row: (
            float(row.get("weighted_score", float("inf"))),
            float(row.get("underforecast_pct", float("inf"))),
            float(row.get("rmse", float("inf"))),
            -int(row.get("percentile", 0)),
        ),
    )
    best = ranked[0]
    return {
        "material": material,
        "material_code": material_code,
        "plant": plant,
        "demand_type": demand_type,
        "selection_level": selection_level,
        "selected_percentile": int(best.get("percentile", 50)),
        "selected_score": round(float(best.get("weighted_score", 0.0) or 0.0), 4),
        "backtest_window_start": backtest_window_start,
        "backtest_window_end": backtest_window_end,
        "evaluation_points": int(best.get("evaluation_points", 0) or 0),
    }


def build_production_forecast_rows(
    *,
    material: str,
    material_code: str,
    plant: str,
    demand_type: str,
    forecast_labels: Sequence[str],
    baseline_p50: Sequence[float],
    selected_percentile: int,
    percentile_candidates: Mapping[int, Sequence[float]],
    lower_bound: Sequence[float],
    upper_bound: Sequence[float],
) -> list[dict[str, Any]]:
    """Build production-facing rows for the chosen operational percentile."""
    selected_series = percentile_candidates.get(int(selected_percentile), [])
    rows: list[dict[str, Any]] = []
    for idx, label in enumerate(forecast_labels):
        rows.append(
            {
                "material": material,
                "material_code": material_code,
                "plant": plant,
                "demand_type": demand_type,
                "forecast_month": label,
                "baseline_p50": round(float(baseline_p50[idx]) if idx < len(baseline_p50) else 0.0, 2),
                "selected_percentile": int(selected_percentile),
                "selected_forecast": round(float(selected_series[idx]) if idx < len(selected_series) else 0.0, 2),
                "lower_bound": round(float(lower_bound[idx]) if idx < len(lower_bound) else 0.0, 2),
                "upper_bound": round(float(upper_bound[idx]) if idx < len(upper_bound) else 0.0, 2),
            }
        )
    return rows


def build_selected_percentile_backtest_rows(
    backtest_rows: Sequence[Mapping[str, Any]],
    *,
    material: str,
    material_code: str,
    plant: str,
    demand_type: str,
    selected_percentile: int,
    confidence_score: float | None = None,
    confidence_band: str = "",
    ladder: Sequence[int] = DEFAULT_PERCENTILE_LADDER,
) -> list[dict[str, Any]]:
    """Build row-level walk-forward detail for the selected percentile."""
    from app.core.services.buffer_policy import adjust_buffer_by_confidence
    from app.core.services.forecast_confidence import candidate_acceptable_range

    detail_rows: list[dict[str, Any]] = []
    percentiles_to_emit = sorted({int(p) for p in (*ladder, 95)})

    for row in backtest_rows:
        predictions = row.get("predictions") or {}
        actual_qty = round(max(float(row.get("actual_qty", 0.0) or 0.0), 0.0), 2)
        prev_actual_qty = row.get("prev_actual_qty")
        prev_actual = round(max(float(prev_actual_qty or 0.0), 0.0), 2) if prev_actual_qty is not None else None
        selected_forecast = round(max(float(predictions.get(int(selected_percentile), 0.0) or 0.0), 0.0), 2)
        p50_forecast = round(max(float(predictions.get(50, selected_forecast) or 0.0), 0.0), 2)
        p95_forecast = round(max(float(predictions.get(95, selected_forecast) or 0.0), 0.0), 2)
        coverage_low, coverage_high = candidate_acceptable_range(predictions, int(selected_percentile))
        coverage_low = round(max(float(coverage_low), 0.0), 2)
        coverage_high = round(max(float(coverage_high), coverage_low), 2)
        forecast_error_qty = round(selected_forecast - actual_qty, 2)
        abs_error_qty = round(abs(forecast_error_qty), 2)
        underforecast_qty = round(max(actual_qty - selected_forecast, 0.0), 2)
        overforecast_qty = round(max(selected_forecast - actual_qty, 0.0), 2)
        buffer_qty = round(max(p95_forecast - p50_forecast, 0.0), 2)
        adjusted_buffer_qty = adjust_buffer_by_confidence(buffer_qty, confidence_score)

        actual_direction = ""
        forecast_direction = ""
        directional_hit = None
        if prev_actual is not None:
            actual_delta = actual_qty - prev_actual
            forecast_delta = selected_forecast - prev_actual
            actual_direction = "up" if actual_delta > 0 else "down" if actual_delta < 0 else "flat"
            forecast_direction = "up" if forecast_delta > 0 else "down" if forecast_delta < 0 else "flat"
            directional_hit = actual_direction == forecast_direction

        record = {
            "material": material,
            "material_code": material_code,
            "plant": plant,
            "demand_type": demand_type,
            "backtest_period": row.get("period", ""),
            "prev_actual_consumption_qty": prev_actual,
            "actual_consumption_qty": actual_qty,
            "selected_percentile": int(selected_percentile),
            "selected_percentile_label": f"P{int(selected_percentile)}",
            "selected_forecast_qty": selected_forecast,
            "coverage_lower_qty": coverage_low,
            "coverage_upper_qty": coverage_high,
            "within_coverage_band": coverage_low <= actual_qty <= coverage_high,
            "forecast_error_qty": forecast_error_qty,
            "abs_error_qty": abs_error_qty,
            "underforecast_qty": underforecast_qty,
            "overforecast_qty": overforecast_qty,
            "buffer_qty": buffer_qty,
            "adjusted_buffer_qty": adjusted_buffer_qty,
            "actual_direction": actual_direction,
            "forecast_direction": forecast_direction,
            "directional_hit": directional_hit,
            "confidence_score": confidence_score,
            "confidence_band": confidence_band,
        }
        for percentile in percentiles_to_emit:
            record[f"p{int(percentile)}_forecast_qty"] = round(
                max(float(predictions.get(int(percentile), 0.0) or 0.0), 0.0),
                2,
            )
        detail_rows.append(record)

    return detail_rows


def rows_to_dataframe(rows: Iterable[Mapping[str, Any]]):
    """Convert row dicts to a DataFrame when pandas is available."""
    if pd is None:
        raise RuntimeError("pandas is not available for dataframe conversion")
    return pd.DataFrame(list(rows))
