"""Inventory Intelligence — Advanced Forecasting Engine.

Ported from the ONGC Xylene Pilot forecast builder and merged with the
existing adaptive-ensemble framework.  Three optimised statistical models
plus bootstrapped prediction intervals, walk-forward validation, and an
automatic model selector.

Models
------
1.  Holt-Winters Triple Exponential Smoothing — grid-search + coordinate
    descent optimisation for alpha / beta / gamma  (pure numpy, no scipy).
2.  Seasonal Naive with linear trend correction — robust for intermittent
    or sparse monthly series.
3.  Weighted Moving Average with exponential-decay weights and a seasonal
    index — a lightweight fallback when data is very short.

Confidence intervals
--------------------
Block-bootstrapped residuals (model-agnostic, non-parametric, robust).

Walk-forward validation
-----------------------
Last *VAL_MONTHS* held out.  Metrics: MAPE, bias %, PI coverage.
Confidence score =
    0.5 × (100 − MAPE)  +  0.3 × coverage  +  0.2 × (100 − |bias|)
with asymmetric penalty (stockout error penalised 2×).
"""

from __future__ import annotations

import logging
import warnings
from typing import Any

import numpy as np

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════
HORIZON: int = 6            # months to forecast
SEAS_PERIOD: int = 12       # monthly seasonality cycle
CI_LEVEL: float = 0.90      # 90 % prediction interval
N_BOOTSTRAP: int = 500      # bootstrap replicates for CI
VAL_MONTHS: int = 6         # holdout window for walk-forward validation
MIN_NZ_HW: int = 18         # min non-zero months to run Holt-Winters
MIN_NZ_SEAS: int = 6        # min non-zero months for Seasonal Naive


# ═══════════════════════════════════════════════════════════════════════
# MODEL 1 — HOLT-WINTERS (optimised triple exponential smoothing)
# ═══════════════════════════════════════════════════════════════════════

def _hw_sse(
    params: tuple[float, float, float],
    y: np.ndarray,
    m: int,
) -> tuple[float, float, float, list[float]]:
    """Compute SSE for additive Holt-Winters given ``(alpha, beta, gamma)``."""
    alpha, beta, gamma = params
    n = len(y)
    # Initialise level, trend, seasonal from first two cycles
    L = float(np.mean(y[:m]))
    T = float((np.mean(y[m : 2 * m]) - np.mean(y[:m])) / m) if len(y) >= 2 * m else 0.0
    S: list[float] = [float(y[i]) - L for i in range(m)]

    fitted = np.zeros(n)
    Lp, Tp = L, T

    for i in range(n):
        fitted[i] = Lp + Tp + S[i]
        Lnew = alpha * (y[i] - S[i]) + (1 - alpha) * (Lp + Tp)
        Tnew = beta * (Lnew - Lp) + (1 - beta) * Tp
        Snew = gamma * (y[i] - Lnew) + (1 - gamma) * S[i]
        S.append(Snew)
        Lp, Tp = Lnew, Tnew

    sse = float(np.sum((y - fitted) ** 2))
    return sse, Lp, Tp, S


def fit_holt_winters(y: np.ndarray, m: int = SEAS_PERIOD) -> dict:
    """Optimise alpha / beta / gamma via grid search + coordinate descent.

    Uses a coarse grid to find a good starting region, then refines each
    parameter one at a time (coordinate descent) until convergence.
    """
    best_sse = np.inf
    best_p: tuple[float, float, float] = (0.3, 0.05, 0.3)

    # --- Phase 1: coarse grid search ---
    for a in (0.1, 0.3, 0.5, 0.7):
        for b in (0.01, 0.05, 0.1):
            for g in (0.1, 0.3, 0.5):
                sse, *_ = _hw_sse((a, b, g), y, m)
                if sse < best_sse:
                    best_sse = sse
                    best_p = (a, b, g)

    # --- Phase 2: coordinate descent refinement ---
    step = 0.05
    alpha, beta, gamma = best_p
    for _ in range(30):  # max iterations
        improved = False
        # Refine alpha
        for trial in np.clip(np.arange(alpha - 4 * step, alpha + 4 * step + step / 2, step), 0.01, 0.99):
            sse, *_ = _hw_sse((trial, beta, gamma), y, m)
            if sse < best_sse - 1e-9:
                best_sse, alpha, improved = sse, float(trial), True
        # Refine beta
        for trial in np.clip(np.arange(beta - 4 * step, beta + 4 * step + step / 2, step), 0.001, 0.5):
            sse, *_ = _hw_sse((alpha, trial, gamma), y, m)
            if sse < best_sse - 1e-9:
                best_sse, beta, improved = sse, float(trial), True
        # Refine gamma
        for trial in np.clip(np.arange(gamma - 4 * step, gamma + 4 * step + step / 2, step), 0.01, 0.99):
            sse, *_ = _hw_sse((alpha, beta, trial), y, m)
            if sse < best_sse - 1e-9:
                best_sse, gamma, improved = sse, float(trial), True
        if not improved:
            if step <= 0.005:
                break
            step /= 2.0

    sse, L_last, T_last, S_all = _hw_sse((alpha, beta, gamma), y, m)
    return {
        "alpha": alpha,
        "beta": beta,
        "gamma": gamma,
        "L": L_last,
        "T": T_last,
        "S": S_all,
        "n": len(y),
        "m": m,
        "sse": sse,
    }


def forecast_hw(model: dict, h: int = HORIZON) -> np.ndarray:
    """Generate *h*-step point forecasts from a fitted HW model."""
    L, T, S_all, m, n = model["L"], model["T"], model["S"], model["m"], model["n"]
    fc: list[float] = []
    for i in range(1, h + 1):
        s_idx = n + i - m
        while s_idx < 0:
            s_idx += m
        fc.append(max(0.0, L + i * T + S_all[s_idx]))
    return np.array(fc)


def _fitted_values_hw(
    params: tuple[float, float, float],
    y: np.ndarray,
    m: int,
) -> np.ndarray:
    """Return the in-sample fitted-values array for given HW parameters."""
    alpha, beta, gamma = params
    n = len(y)
    L = float(np.mean(y[:m]))
    T = float((np.mean(y[m : 2 * m]) - np.mean(y[:m])) / m) if n >= 2 * m else 0.0
    S = [float(y[i]) - L for i in range(m)]
    fitted = np.zeros(n)
    Lp, Tp = L, T
    for i in range(n):
        fitted[i] = Lp + Tp + S[i]
        Lnew = alpha * (y[i] - S[i]) + (1 - alpha) * (Lp + Tp)
        Tnew = beta * (Lnew - Lp) + (1 - beta) * Tp
        Snew = gamma * (y[i] - Lnew) + (1 - gamma) * S[i]
        S.append(Snew)
        Lp, Tp = Lnew, Tnew
    return fitted


# ═══════════════════════════════════════════════════════════════════════
# MODEL 2 — SEASONAL NAIVE + TREND  (sparse / intermittent series)
# ═══════════════════════════════════════════════════════════════════════

def fit_seasonal_naive(y: np.ndarray, m: int = SEAS_PERIOD) -> dict:
    """Seasonal index (per-month median of non-zero obs) plus linear trend."""
    n = len(y)
    seas = np.zeros(m)
    for i in range(m):
        vals = [y[j] for j in range(i, n, m) if y[j] > 0]
        seas[i] = float(np.median(vals)) if vals else 0.0

    # Linear trend on non-zero months
    nz_idx = np.where(y > 0)[0]
    if len(nz_idx) >= 4:
        x = nz_idx.astype(float)
        xm, ym = x.mean(), y[nz_idx].mean()
        trend = float(np.sum((x - xm) * (y[nz_idx] - ym)) / (np.sum((x - xm) ** 2) + 1e-9))
    else:
        trend = 0.0

    return {
        "seas": seas,
        "trend": trend,
        "n": n,
        "m": m,
        "mean_nz": float(y[y > 0].mean()) if (y > 0).any() else 0.0,
    }


def forecast_seasonal_naive(model: dict, h: int = HORIZON) -> np.ndarray:
    """Forecast using seasonal index + trend extrapolation."""
    seas, trend, n, m = model["seas"], model["trend"], model["n"], model["m"]
    fc: list[float] = []
    for i in range(1, h + 1):
        pos = (n + i - 1) % m
        fc.append(max(0.0, seas[pos] + trend * (n + i)))
    return np.array(fc)


# ═══════════════════════════════════════════════════════════════════════
# MODEL 3 — WEIGHTED MOVING AVERAGE + SEASONAL INDEX (fallback)
# ═══════════════════════════════════════════════════════════════════════

def fit_wma(y: np.ndarray, m: int = SEAS_PERIOD) -> dict:
    """12-month WMA with exponentially decaying weights + seasonal index."""
    seas_idx = np.zeros(m)
    global_mean = float(y[y > 0].mean()) if (y > 0).any() else 1.0
    for i in range(m):
        vals = [y[j] for j in range(i, len(y), m) if y[j] >= 0]
        seas_idx[i] = (float(np.mean(vals)) / global_mean) if vals and global_mean > 0 else 1.0

    window = min(12, len(y))
    weights = np.exp(np.linspace(-1, 0, window))
    weights /= weights.sum()
    level = float(np.dot(weights, y[-window:]))

    return {"level": level, "seas_idx": seas_idx, "n": len(y), "m": m}


def forecast_wma(model: dict, h: int = HORIZON) -> np.ndarray:
    """Forecast from a fitted WMA model."""
    level, seas_idx, n, m = model["level"], model["seas_idx"], model["n"], model["m"]
    fc: list[float] = []
    for i in range(1, h + 1):
        pos = (n + i - 1) % m
        fc.append(max(0.0, level * seas_idx[pos]))
    return np.array(fc)


# ═══════════════════════════════════════════════════════════════════════
# BOOTSTRAPPED PREDICTION INTERVALS
# ═══════════════════════════════════════════════════════════════════════

def bootstrap_ci(
    y: np.ndarray,
    model_type: str,
    model_params: dict,
    h: int = HORIZON,
    n_boot: int = N_BOOTSTRAP,
    ci: float = CI_LEVEL,
    m: int = SEAS_PERIOD,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Block-bootstrap prediction intervals (model-agnostic, robust).

    Returns ``(lower, upper, point_forecast)`` arrays of length *h*.
    """
    n = len(y)

    # Compute in-sample residuals
    if model_type == "HoltWinters":
        fitted = _fitted_values_hw(
            (model_params["alpha"], model_params["beta"], model_params["gamma"]),
            y,
            m,
        )
        residuals = y - fitted
    elif model_type == "SeasonalNaive":
        seas, trend = model_params["seas"], model_params["trend"]
        fitted = np.array([seas[i % m] + trend * i for i in range(n)])
        residuals = y - fitted
    else:  # WMA
        residuals = y - model_params["level"] * np.ones(n)

    # Point forecast
    if model_type == "HoltWinters":
        fc_point = forecast_hw(model_params, h)
    elif model_type == "SeasonalNaive":
        fc_point = forecast_seasonal_naive(model_params, h)
    else:
        fc_point = forecast_wma(model_params, h)

    # Block bootstrap (block_size=3 to preserve autocorrelation)
    block_size = 3
    boot_paths = np.zeros((n_boot, h))
    rng = np.random.default_rng(42)
    n_blocks = max(n // block_size, 1)

    for b in range(n_boot):
        noise = np.zeros(h)
        for j in range(0, h, block_size):
            blk = rng.integers(0, n_blocks)
            chunk = residuals[blk * block_size : blk * block_size + block_size]
            end = min(j + block_size, h)
            noise[j:end] = chunk[: end - j]
        boot_paths[b] = np.maximum(0, fc_point + noise)

    alpha_tail = (1 - ci) / 2
    lower = np.quantile(boot_paths, alpha_tail, axis=0)
    upper = np.quantile(boot_paths, 1 - alpha_tail, axis=0)
    return lower, upper, fc_point


# ═══════════════════════════════════════════════════════════════════════
# WALK-FORWARD VALIDATION  (last VAL_MONTHS held out)
# ═══════════════════════════════════════════════════════════════════════

def walk_forward_validate(
    y: np.ndarray,
    model_type: str,
    m: int = SEAS_PERIOD,
    val_months: int = VAL_MONTHS,
    ci: float = CI_LEVEL,
) -> dict:
    """Train on ``y[:-val_months]``, forecast forward, score against actuals.

    Returns a dict with MAPE, bias %, PI coverage, and a composite
    confidence score.
    """
    n_train = len(y) - val_months
    if n_train < m:
        return {
            "mape": 100.0,
            "bias_pct": 0.0,
            "coverage": 0.0,
            "conf_score": 0.0,
            "model_used": model_type,
        }

    y_train = y[:n_train]
    y_test = y[n_train:]

    if model_type == "HoltWinters" and (y_train > 0).sum() >= MIN_NZ_HW:
        mdl = fit_holt_winters(y_train, m)
        fc = forecast_hw(mdl, val_months)
        lo, hi, _ = bootstrap_ci(y_train, "HoltWinters", mdl, val_months)
    elif (y_train > 0).sum() >= MIN_NZ_SEAS:
        mdl = fit_seasonal_naive(y_train, m)
        fc = forecast_seasonal_naive(mdl, val_months)
        lo, hi, _ = bootstrap_ci(y_train, "SeasonalNaive", mdl, val_months)
        model_type = "SeasonalNaive"
    else:
        mdl = fit_wma(y_train, m)
        fc = forecast_wma(mdl, val_months)
        lo, hi, _ = bootstrap_ci(y_train, "WMA", mdl, val_months)
        model_type = "WMA"

    # Metrics on non-zero actuals
    nz_mask = y_test > 0
    if nz_mask.sum() > 0:
        mape = float(np.mean(np.abs(y_test[nz_mask] - fc[nz_mask]) / y_test[nz_mask]) * 100)
        bias = float(np.mean((fc[nz_mask] - y_test[nz_mask]) / y_test[nz_mask]) * 100)
    else:
        mape, bias = 0.0, 0.0

    coverage = float(np.mean((y_test >= lo) & (y_test <= hi)) * 100)

    # Confidence score with asymmetric stockout penalty
    under_ratio = float(np.mean(fc[nz_mask] < y_test[nz_mask])) if nz_mask.sum() > 0 else 0.0
    mape_adj = mape * (1 + 0.5 * under_ratio)
    conf_score = 0.5 * (100 - min(mape_adj, 100)) + 0.3 * coverage + 0.2 * (100 - min(abs(bias), 100))
    conf_score = max(0.0, min(100.0, conf_score))

    return {
        "mape": round(mape, 1),
        "bias_pct": round(bias, 1),
        "coverage": round(coverage, 1),
        "conf_score": round(conf_score, 1),
        "model_used": model_type,
    }


# ═══════════════════════════════════════════════════════════════════════
# AUTO MODEL SELECTOR
# ═══════════════════════════════════════════════════════════════════════

def select_best_model(
    y: np.ndarray,
    m: int = SEAS_PERIOD,
    horizon: int = HORIZON,
) -> dict:
    """Choose the best model based on data richness, fit, and validate.

    Returns a dict with forecast, lower/upper 90 %, validation metrics,
    model type, and model parameters.
    """
    nz = int((y > 0).sum())

    if nz >= MIN_NZ_HW:
        model_type = "HoltWinters"
        model = fit_holt_winters(y, m)
        fc = forecast_hw(model, horizon)
        lo, hi, _ = bootstrap_ci(y, "HoltWinters", model, horizon)
    elif nz >= MIN_NZ_SEAS:
        model_type = "SeasonalNaive"
        model = fit_seasonal_naive(y, m)
        fc = forecast_seasonal_naive(model, horizon)
        lo, hi, _ = bootstrap_ci(y, "SeasonalNaive", model, horizon)
    else:
        model_type = "WMA"
        model = fit_wma(y, m)
        fc = forecast_wma(model, horizon)
        lo, hi, _ = bootstrap_ci(y, "WMA", model, horizon)

    val = walk_forward_validate(y, model_type, m)

    return {
        "model_type": model_type,
        "model_params": model,
        "forecast": fc,
        "lower_90": lo,
        "upper_90": hi,
        "validation": val,
    }


# ═══════════════════════════════════════════════════════════════════════
# CONVENIENCE WRAPPERS  (used by the ensemble in inventory_intelligence)
# ═══════════════════════════════════════════════════════════════════════

def hw_forecast_list(quantities: np.ndarray, horizon: int = HORIZON) -> list[float]:
    """Fit optimised HW and return a plain list of *horizon* forecasts."""
    model = fit_holt_winters(quantities)
    return [max(float(v), 0.0) for v in forecast_hw(model, horizon)]


def seasonal_naive_forecast_list(quantities: np.ndarray, horizon: int = HORIZON) -> list[float]:
    """Fit seasonal-naive-with-trend and return a list of *horizon* forecasts."""
    model = fit_seasonal_naive(quantities)
    return [max(float(v), 0.0) for v in forecast_seasonal_naive(model, horizon)]


def wma_exp_forecast_list(quantities: np.ndarray, horizon: int = HORIZON) -> list[float]:
    """Fit exponential-decay WMA and return a list of *horizon* forecasts."""
    model = fit_wma(quantities)
    return [max(float(v), 0.0) for v in forecast_wma(model, horizon)]


def hw_one_step(quantities: np.ndarray) -> float:
    """One-step-ahead forecast using optimised Holt-Winters."""
    model = fit_holt_winters(quantities)
    return max(float(forecast_hw(model, 1)[0]), 0.0)


def seasonal_naive_one_step(quantities: np.ndarray) -> float:
    """One-step-ahead forecast using Seasonal Naive."""
    model = fit_seasonal_naive(quantities)
    return max(float(forecast_seasonal_naive(model, 1)[0]), 0.0)


def wma_exp_one_step(quantities: np.ndarray) -> float:
    """One-step-ahead forecast using exponential-decay WMA."""
    model = fit_wma(quantities)
    return max(float(forecast_wma(model, 1)[0]), 0.0)


def compute_bootstrap_bands(
    quantities: np.ndarray,
    forecast_values: list[float],
    horizon: int = HORIZON,
) -> dict:
    """Run ``select_best_model`` then bootstrap to produce 5 percentile bands.

    The result dict is keyed the same way as the existing
    ``_compute_prediction_bands`` so it can be merged transparently.
    """
    result = select_best_model(quantities, horizon=horizon)
    lo90, hi90, fc = result["lower_90"], result["upper_90"], result["forecast"]

    # Reuse bootstrap paths for P5/P10/P50/P90/P95
    model_type = result["model_type"]
    model_params = result["model_params"]
    n = len(quantities)
    m = SEAS_PERIOD

    # Residuals
    if model_type == "HoltWinters":
        fitted = _fitted_values_hw(
            (model_params["alpha"], model_params["beta"], model_params["gamma"]),
            quantities, m,
        )
        residuals = quantities - fitted
    elif model_type == "SeasonalNaive":
        seas, trend = model_params["seas"], model_params["trend"]
        fitted = np.array([seas[i % m] + trend * i for i in range(n)])
        residuals = quantities - fitted
    else:
        residuals = quantities - model_params["level"] * np.ones(n)

    # Full bootstrap
    block_size = 3
    n_boot = N_BOOTSTRAP
    rng = np.random.default_rng(42)
    n_blocks = max(n // block_size, 1)
    boot_paths = np.zeros((n_boot, horizon))
    for b in range(n_boot):
        noise = np.zeros(horizon)
        for j in range(0, horizon, block_size):
            blk = rng.integers(0, n_blocks)
            chunk = residuals[blk * block_size : blk * block_size + block_size]
            end = min(j + block_size, horizon)
            noise[j:end] = chunk[: end - j]
        boot_paths[b] = np.maximum(0, fc + noise)

    p5  = [round(float(v), 2) for v in np.quantile(boot_paths, 0.05, axis=0)]
    p10 = [round(float(v), 2) for v in np.quantile(boot_paths, 0.10, axis=0)]
    p50 = [round(float(v), 2) for v in np.quantile(boot_paths, 0.50, axis=0)]
    p90 = [round(float(v), 2) for v in np.quantile(boot_paths, 0.90, axis=0)]
    p95 = [round(float(v), 2) for v in np.quantile(boot_paths, 0.95, axis=0)]

    return {
        "model_type": model_type,
        "validation": result["validation"],
        "p5_quantities": p5,
        "p10_quantities": p10,
        "p50_quantities": p50,
        "p90_quantities": p90,
        "p95_quantities": p95,
    }
