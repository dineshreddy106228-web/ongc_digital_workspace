from __future__ import annotations

"""Walk-forward forecast confidence helpers.

Forecast confidence is defined here as the historical reliability of the
forecasting system under walk-forward validation. The implementation is
intentionally practical and explainable:

- coverage of an acceptable forecast range
- stability of forecast error
- consistency of forecast bias
- directional accuracy
"""

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class ConfidenceScoreWeights:
    coverage: float = 0.40
    error_volatility: float = 0.20
    abs_bias: float = 0.20
    directional_accuracy: float = 0.20


@dataclass(frozen=True)
class ConfidenceThresholds:
    high: float = 85.0
    moderate: float = 70.0


def candidate_acceptable_range(
    predictions: Mapping[int, float],
    percentile: int,
) -> tuple[float, float]:
    """Build a practical acceptable range around a percentile forecast.

    The operating percentiles are point forecasts. To score coverage we derive
    an acceptable interval using the midpoint to neighbouring percentile
    forecasts. This keeps the logic explainable and tied to the existing
    percentile ladder instead of inventing a separate statistical model.
    """
    ordered = sorted(int(value) for value in predictions.keys())
    if not ordered:
        return 0.0, 0.0

    current = max(float(predictions.get(int(percentile), 0.0) or 0.0), 0.0)
    if int(percentile) not in ordered:
        return current, current

    idx = ordered.index(int(percentile))
    prev_value = max(float(predictions.get(ordered[idx - 1], current) or 0.0), 0.0) if idx > 0 else None
    next_value = max(float(predictions.get(ordered[idx + 1], current) or 0.0), 0.0) if idx < len(ordered) - 1 else None

    if prev_value is not None and next_value is not None:
        lower = max((prev_value + current) / 2.0, 0.0)
        upper = max((current + next_value) / 2.0, lower)
        return lower, upper

    if next_value is not None:
        half_width = max((next_value - current) / 2.0, max(current * 0.05, 1.0))
        return max(current - half_width, 0.0), max(current + half_width, 0.0)

    if prev_value is not None:
        half_width = max((current - prev_value) / 2.0, max(current * 0.05, 1.0))
        return max(current - half_width, 0.0), max(current + half_width, 0.0)

    return current, current


def _directional_accuracy_pct(
    actuals: np.ndarray,
    predictions: np.ndarray,
    prev_actuals: Sequence[float | None],
) -> float:
    hits = 0
    total = 0
    for actual, pred, prev_actual in zip(actuals, predictions, prev_actuals):
        if prev_actual is None:
            continue
        actual_delta = float(actual) - float(prev_actual)
        pred_delta = float(pred) - float(prev_actual)
        if np.sign(actual_delta) == np.sign(pred_delta):
            hits += 1
        total += 1
    return float((hits / total) * 100.0) if total else 0.0


def _normalize_metric(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return float(min(max(value / scale, 0.0), 1.0))


def confidence_band_for_score(
    confidence_score: float,
    thresholds: ConfidenceThresholds = ConfidenceThresholds(),
) -> str:
    if confidence_score > thresholds.high:
        return "High Confidence"
    if confidence_score >= thresholds.moderate:
        return "Moderate Confidence"
    return "Low Confidence"


def summarize_confidence_by_percentile(
    backtest_rows: Sequence[Mapping[str, Any]],
    *,
    material: str,
    material_code: str,
    plant: str,
    demand_type: str,
    ladder: Sequence[int],
    score_weights: ConfidenceScoreWeights = ConfidenceScoreWeights(),
    thresholds: ConfidenceThresholds = ConfidenceThresholds(),
) -> list[dict[str, Any]]:
    """Build confidence metrics for each percentile candidate."""
    if not backtest_rows:
        return []

    actuals = np.asarray([max(float(row.get("actual_qty", 0.0) or 0.0), 0.0) for row in backtest_rows], dtype=float)
    prev_actuals = [row.get("prev_actual_qty") for row in backtest_rows]
    scale = max(float(actuals.mean()) if len(actuals) else 0.0, float(np.percentile(actuals, 75)) if len(actuals) else 0.0, 1.0)
    window_start = str(backtest_rows[0].get("period", "")) if backtest_rows else ""
    window_end = str(backtest_rows[-1].get("period", "")) if backtest_rows else ""

    results: list[dict[str, Any]] = []
    for percentile in ladder:
        preds = np.asarray(
            [
                max(float((row.get("predictions") or {}).get(int(percentile), 0.0) or 0.0), 0.0)
                for row in backtest_rows
            ],
            dtype=float,
        )
        errors = preds - actuals
        abs_errors = np.abs(errors)
        coverage_hits = 0
        for row, actual in zip(backtest_rows, actuals):
            low, high = candidate_acceptable_range(row.get("predictions") or {}, int(percentile))
            if low <= float(actual) <= high:
                coverage_hits += 1
        coverage_pct = float((coverage_hits / len(backtest_rows)) * 100.0) if backtest_rows else 0.0
        error_volatility = float(np.std(abs_errors)) if len(abs_errors) else 0.0
        bias = float(errors.mean()) if len(errors) else 0.0
        directional_accuracy_pct = _directional_accuracy_pct(actuals, preds, prev_actuals)

        normalized_error_volatility = _normalize_metric(error_volatility, scale)
        normalized_abs_bias = _normalize_metric(abs(bias), scale)
        confidence_score = (
            (score_weights.coverage * coverage_pct)
            + (score_weights.error_volatility * (1.0 - normalized_error_volatility) * 100.0)
            + (score_weights.abs_bias * (1.0 - normalized_abs_bias) * 100.0)
            + (score_weights.directional_accuracy * directional_accuracy_pct)
        )
        confidence_score = max(0.0, min(100.0, confidence_score))

        results.append(
            {
                "material": material,
                "material_code": material_code,
                "plant": plant,
                "demand_type": demand_type,
                "percentile": int(percentile),
                "coverage_pct": round(coverage_pct, 2),
                "error_volatility": round(error_volatility, 2),
                "bias": round(bias, 2),
                "directional_accuracy_pct": round(directional_accuracy_pct, 2),
                "confidence_score": round(confidence_score, 2),
                "confidence_band": confidence_band_for_score(confidence_score, thresholds=thresholds),
                "backtest_window_start": window_start,
                "backtest_window_end": window_end,
                "evaluation_points": int(len(backtest_rows)),
            }
        )

    return results


def aggregate_confidence_summary(
    summary_rows: Sequence[Mapping[str, Any]],
    *,
    group_fields: Sequence[str],
    thresholds: ConfidenceThresholds = ConfidenceThresholds(),
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in summary_rows:
        key = tuple(row.get(field, "") for field in (*group_fields, "percentile"))
        grouped.setdefault(key, []).append(row)

    aggregated: list[dict[str, Any]] = []
    metric_fields = (
        "coverage_pct",
        "error_volatility",
        "bias",
        "directional_accuracy_pct",
        "confidence_score",
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
            record[metric] = round(weighted_total / max(total_points, 1), 2)
        record["evaluation_points"] = int(total_points)
        window_start = sorted(str(row.get("backtest_window_start", "")) for row in rows if row.get("backtest_window_start"))
        window_end = sorted(str(row.get("backtest_window_end", "")) for row in rows if row.get("backtest_window_end"))
        record["backtest_window_start"] = window_start[0] if window_start else ""
        record["backtest_window_end"] = window_end[-1] if window_end else ""
        record["confidence_band"] = confidence_band_for_score(float(record.get("confidence_score", 0.0) or 0.0), thresholds=thresholds)
        aggregated.append(record)

    return aggregated


def select_confidence_summary(
    confidence_rows: Sequence[Mapping[str, Any]],
    *,
    selected_percentile: int,
    material: str,
    material_code: str,
    plant: str,
    demand_type: str,
) -> dict[str, Any] | None:
    for row in confidence_rows:
        if int(row.get("percentile", -1)) == int(selected_percentile):
            return {
                "material": material,
                "material_code": material_code,
                "plant": plant,
                "demand_type": demand_type,
                "selected_percentile": int(selected_percentile),
                "coverage_pct": row.get("coverage_pct"),
                "error_volatility": row.get("error_volatility"),
                "bias": row.get("bias"),
                "directional_accuracy_pct": row.get("directional_accuracy_pct"),
                "confidence_score": row.get("confidence_score"),
                "confidence_band": row.get("confidence_band"),
                "backtest_window_start": row.get("backtest_window_start", ""),
                "backtest_window_end": row.get("backtest_window_end", ""),
                "evaluation_points": row.get("evaluation_points", 0),
            }
    return None
