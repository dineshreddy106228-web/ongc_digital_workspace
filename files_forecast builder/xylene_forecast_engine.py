"""
ONGC Xylene Pilot — Stage 2: Forecasting Engine
------------------------------------------------
Models implemented (pure numpy + scipy, no external forecast libs):
  - Holt-Winters Triple Exponential Smoothing (optimised via L-BFGS-B)
  - Seasonal Naive with trend correction (for intermittent / sparse series)
  - Weighted Moving Average with seasonal index (fallback)

Confidence intervals: bootstrapped residuals (model-agnostic, robust)
Walk-forward validation: last 6 months held out for MAPE + coverage scoring
Confidence Score = 0.5×(100−MAPE) + 0.3×coverage + 0.2×(100−|bias|)
"""

import numpy as np
import pandas as pd
import pickle
import warnings
from scipy.optimize import minimize
from scipy.stats import norm
warnings.filterwarnings('ignore')

# ══════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════
HORIZON     = 6          # months to forecast
SEAS_PERIOD = 12         # monthly seasonality
CI_LEVEL    = 0.90       # 90% prediction interval
N_BOOTSTRAP = 500        # bootstrap samples for CI
VAL_MONTHS  = 6          # holdout for walk-forward validation
MIN_NZ_HW   = 18         # min non-zero months to run Holt-Winters
MIN_NZ_SEAS = 6          # min non-zero months for seasonal naive

FORECAST_UNITS = ['11A1','12A1','13A1','20A1','21A1',
                  '22A1','25A1','41A1','50A1','51A1']

UNIT_LABELS = {
    '11A1': 'UT Offshore — Mumbai High North',
    '12A1': 'UT Offshore — Heera / Ratna',
    '13A1': 'UT Offshore — Bassein / Tapti / Panna Mukta',
    '20A1': 'Ahmedabad Asset',
    '21A1': 'Ankleshwar Asset',
    '22A1': 'Mehsana Asset',
    '25A1': 'Cambay Asset',
    '41A1': 'Rajahmundry Asset',
    '50A1': 'Assam Asset, Nazira',
    '51A1': 'Jorhat Asset',
}

# ══════════════════════════════════════════════════════════════════════
# MODEL 1 — HOLT-WINTERS (optimised triple exponential smoothing)
# ══════════════════════════════════════════════════════════════════════

def _hw_sse(params, y, m):
    """Compute SSE for Holt-Winters given params [alpha, beta, gamma]."""
    alpha, beta, gamma = params
    n  = len(y)
    # Initialise level, trend, seasonal from first two cycles
    L  = np.mean(y[:m])
    T  = (np.mean(y[m:2*m]) - np.mean(y[:m])) / m if len(y) >= 2*m else 0.0
    # Seasonal indices: deviation of each month from first-cycle mean
    S  = np.array([y[i] - L for i in range(m)], dtype=float)

    fitted = np.zeros(n)
    Lp, Tp = L, T
    S_all  = list(S)

    for i in range(n):
        si   = i % m
        Lnew = alpha * (y[i] - S_all[i]) + (1-alpha) * (Lp + Tp)
        Tnew = beta  * (Lnew - Lp)       + (1-beta)  * Tp
        Snew = gamma * (y[i] - Lnew)     + (1-gamma) * S_all[i]
        fitted[i]  = Lp + Tp + S_all[i]
        S_all.append(Snew)
        Lp, Tp = Lnew, Tnew

    return np.sum((y - fitted)**2), Lp, Tp, S_all


def fit_holt_winters(y, m=SEAS_PERIOD):
    """Optimise alpha/beta/gamma via L-BFGS-B minimising SSE."""
    best_sse, best_p = np.inf, (0.3, 0.05, 0.3)

    # Grid search starting points → pick best → fine-tune
    for a in [0.1, 0.3, 0.5, 0.7]:
        for b in [0.01, 0.05, 0.1]:
            for g in [0.1, 0.3, 0.5]:
                res = minimize(
                    lambda p: _hw_sse(p, y, m)[0],
                    x0=[a, b, g],
                    method='L-BFGS-B',
                    bounds=[(0.01,0.99),(0.001,0.5),(0.01,0.99)],
                    options={'maxiter':200, 'ftol':1e-9}
                )
                if res.fun < best_sse:
                    best_sse = res.fun
                    best_p   = tuple(res.x)

    alpha, beta, gamma = best_p
    sse, L_last, T_last, S_all = _hw_sse((alpha,beta,gamma), y, m)
    return {'alpha':alpha,'beta':beta,'gamma':gamma,
            'L':L_last,'T':T_last,'S':S_all,
            'n':len(y),'m':m,'sse':sse}


def forecast_hw(model, h=HORIZON):
    """Generate h-step point forecasts from fitted HW model."""
    L, T, S_all, m = model['L'], model['T'], model['S'], model['m']
    n = model['n']
    fc = []
    for i in range(1, h+1):
        s_idx = n + i - m          # index into S_all for seasonal component
        # Wrap back if s_idx still < m (shouldn't happen with 36 months)
        while s_idx < 0:
            s_idx += m
        fc.append(max(0.0, L + i*T + S_all[s_idx]))
    return np.array(fc)


def fitted_hw(model):
    """Re-compute fitted values for residual bootstrap."""
    y, m   = np.array([]), model['m']
    alpha, beta, gamma = model['alpha'], model['beta'], model['gamma']
    # We don't store y in model — caller passes it
    return None   # fitted values computed inline in bootstrap


def _fitted_values_hw(params, y, m):
    """Return fitted values array for given HW params."""
    alpha, beta, gamma = params
    n = len(y)
    L  = np.mean(y[:m])
    T  = (np.mean(y[m:2*m]) - np.mean(y[:m])) / m if len(y) >= 2*m else 0.0
    S  = list(y[:m] - L)
    fitted = np.zeros(n)
    Lp, Tp = L, T
    for i in range(n):
        Lnew = alpha*(y[i]-S[i]) + (1-alpha)*(Lp+Tp)
        Tnew = beta*(Lnew-Lp)    + (1-beta)*Tp
        Snew = gamma*(y[i]-Lnew) + (1-gamma)*S[i]
        fitted[i] = Lp+Tp+S[i]
        S.append(Snew)
        Lp, Tp = Lnew, Tnew
    return fitted


# ══════════════════════════════════════════════════════════════════════
# MODEL 2 — SEASONAL NAIVE + TREND  (sparse / intermittent series)
# ══════════════════════════════════════════════════════════════════════

def fit_seasonal_naive(y, m=SEAS_PERIOD):
    """
    For each month position, compute median of non-zero observations.
    Apply a simple linear trend correction over the full series.
    """
    n = len(y)
    # Seasonal index: median consumption by calendar month position
    seas = np.zeros(m)
    for i in range(m):
        vals = [y[j] for j in range(i, n, m) if y[j] > 0]
        seas[i] = np.median(vals) if vals else 0.0

    # Overall trend: linear regression on non-zero months
    nz_idx = np.where(y > 0)[0]
    if len(nz_idx) >= 4:
        x = nz_idx.astype(float)
        xm, ym = x.mean(), y[nz_idx].mean()
        trend = np.sum((x-xm)*(y[nz_idx]-ym)) / (np.sum((x-xm)**2) + 1e-9)
    else:
        trend = 0.0

    return {'seas': seas, 'trend': trend, 'n': n, 'm': m,
            'mean_nz': y[y>0].mean() if (y>0).any() else 0.0}


def forecast_seasonal_naive(model, h=HORIZON):
    """Forecast using seasonal index + trend extrapolation."""
    seas, trend, n, m = model['seas'], model['trend'], model['n'], model['m']
    fc = []
    for i in range(1, h+1):
        pos = (n + i - 1) % m
        val = seas[pos] + trend * (n + i)
        fc.append(max(0.0, val))
    return np.array(fc)


# ══════════════════════════════════════════════════════════════════════
# MODEL 3 — WEIGHTED MOVING AVERAGE + SEASONAL INDEX (fallback)
# ══════════════════════════════════════════════════════════════════════

def fit_wma(y, m=SEAS_PERIOD):
    """12-month WMA with exponentially decaying weights + seasonal index."""
    # Seasonal index from available data
    seas_idx = np.zeros(m)
    global_mean = y[y>0].mean() if (y>0).any() else 1.0
    for i in range(m):
        vals = [y[j] for j in range(i, len(y), m) if y[j] >= 0]
        seas_idx[i] = (np.mean(vals) / global_mean) if vals and global_mean>0 else 1.0

    # WMA level: exponentially weighted last 12 non-zero obs
    window = min(12, len(y))
    weights = np.exp(np.linspace(-1, 0, window))
    weights /= weights.sum()
    level = np.dot(weights, y[-window:])

    return {'level': level, 'seas_idx': seas_idx, 'n': len(y), 'm': m}


def forecast_wma(model, h=HORIZON):
    level, seas_idx, n, m = model['level'], model['seas_idx'], model['n'], model['m']
    fc = []
    for i in range(1, h+1):
        pos = (n + i - 1) % m
        fc.append(max(0.0, level * seas_idx[pos]))
    return np.array(fc)


# ══════════════════════════════════════════════════════════════════════
# BOOTSTRAPPED PREDICTION INTERVALS
# ══════════════════════════════════════════════════════════════════════

def bootstrap_ci(y, model_type, model_params, h=HORIZON,
                 n_boot=N_BOOTSTRAP, ci=CI_LEVEL, m=SEAS_PERIOD):
    """
    Block bootstrap on residuals to produce CI for each forecast horizon.
    Avoids parametric assumptions — robust for seasonal, intermittent series.
    """
    n = len(y)

    # Compute in-sample residuals
    if model_type == 'HoltWinters':
        fitted = _fitted_values_hw(
            (model_params['alpha'], model_params['beta'], model_params['gamma']),
            y, m)
        residuals = y - fitted
    elif model_type == 'SeasonalNaive':
        seas, trend = model_params['seas'], model_params['trend']
        fitted = np.array([seas[(i)%m] + trend*i for i in range(n)])
        residuals = y - fitted
    else:  # WMA
        residuals = y - model_params['level'] * np.ones(n)

    # Block bootstrap: resample blocks of 3 to preserve autocorrelation
    block_size = 3
    fc_point = (forecast_hw(model_params, h)
                if model_type == 'HoltWinters'
                else (forecast_seasonal_naive(model_params, h)
                      if model_type == 'SeasonalNaive'
                      else forecast_wma(model_params, h)))

    boot_paths = np.zeros((n_boot, h))
    rng = np.random.default_rng(42)
    n_blocks = n // block_size

    for b in range(n_boot):
        noise = np.zeros(h)
        for j in range(0, h, block_size):
            blk = rng.integers(0, n_blocks)
            chunk = residuals[blk*block_size : blk*block_size+block_size]
            end   = min(j+block_size, h)
            noise[j:end] = chunk[:end-j]
        boot_paths[b] = np.maximum(0, fc_point + noise)

    alpha_tail = (1 - ci) / 2
    lower = np.quantile(boot_paths, alpha_tail,   axis=0)
    upper = np.quantile(boot_paths, 1-alpha_tail, axis=0)
    return lower, upper, fc_point


# ══════════════════════════════════════════════════════════════════════
# WALK-FORWARD VALIDATION (last VAL_MONTHS held out)
# ══════════════════════════════════════════════════════════════════════

def walk_forward_validate(y, model_type, m=SEAS_PERIOD,
                          val_months=VAL_MONTHS, ci=CI_LEVEL):
    """
    Train on y[:-val_months], forecast val_months steps,
    compare to y[-val_months:].
    Returns MAPE, bias%, PI coverage, confidence score.
    """
    n_train = len(y) - val_months
    y_train = y[:n_train]
    y_test  = y[n_train:]

    if model_type == 'HoltWinters' and (y_train>0).sum() >= MIN_NZ_HW:
        mdl = fit_holt_winters(y_train, m)
        fc  = forecast_hw(mdl, val_months)
        lo, hi, _ = bootstrap_ci(y_train, 'HoltWinters', mdl, val_months)
    elif (y_train>0).sum() >= MIN_NZ_SEAS:
        mdl = fit_seasonal_naive(y_train, m)
        fc  = forecast_seasonal_naive(mdl, val_months)
        lo, hi, _ = bootstrap_ci(y_train, 'SeasonalNaive', mdl, val_months)
        model_type = 'SeasonalNaive'
    else:
        mdl = fit_wma(y_train, m)
        fc  = forecast_wma(mdl, val_months)
        lo, hi, _ = bootstrap_ci(y_train, 'WMA', mdl, val_months)
        model_type = 'WMA'

    # Metrics — only on non-zero actuals
    nz_mask = y_test > 0
    if nz_mask.sum() > 0:
        mape  = np.mean(np.abs(y_test[nz_mask] - fc[nz_mask]) /
                        y_test[nz_mask]) * 100
        bias  = np.mean((fc[nz_mask] - y_test[nz_mask]) /
                        y_test[nz_mask]) * 100
    else:
        mape, bias = 0.0, 0.0

    coverage = np.mean((y_test >= lo) & (y_test <= hi)) * 100

    # Confidence score: asymmetric — stockout error penalised 2×
    mape_adj = mape * (1 + 0.5 * np.mean(fc[nz_mask] < y_test[nz_mask])
                       if nz_mask.sum()>0 else mape)
    conf_score = (0.5*(100-min(mape_adj,100)) +
                  0.3*coverage +
                  0.2*(100-min(abs(bias),100)))
    conf_score = max(0, min(100, conf_score))

    return {
        'mape'      : round(mape,   1),
        'bias_pct'  : round(bias,   1),
        'coverage'  : round(coverage,1),
        'conf_score': round(conf_score,1),
        'model_used': model_type,
        'val_actuals': y_test,
        'val_forecast': fc,
        'val_lo' : lo,
        'val_hi' : hi,
    }


# ══════════════════════════════════════════════════════════════════════
# AUTO MODEL SELECTOR
# ══════════════════════════════════════════════════════════════════════

def select_and_fit(y, plant_code, m=SEAS_PERIOD):
    """Choose model based on data richness, fit, validate, forecast."""
    nz = (y > 0).sum()

    # Model selection
    if nz >= MIN_NZ_HW:
        model_type = 'HoltWinters'
        model      = fit_holt_winters(y, m)
        fc         = forecast_hw(model, HORIZON)
        lo,hi,_    = bootstrap_ci(y, 'HoltWinters', model, HORIZON)
    elif nz >= MIN_NZ_SEAS:
        model_type = 'SeasonalNaive'
        model      = fit_seasonal_naive(y, m)
        fc         = forecast_seasonal_naive(model, HORIZON)
        lo,hi,_    = bootstrap_ci(y, 'SeasonalNaive', model, HORIZON)
    else:
        model_type = 'WMA'
        model      = fit_wma(y, m)
        fc         = forecast_wma(model, HORIZON)
        lo,hi,_    = bootstrap_ci(y, 'WMA', model, HORIZON)

    # Validate
    val = walk_forward_validate(y, model_type, m)

    return {
        'plant_code' : plant_code,
        'model'      : model_type,
        'model_params': model,
        'forecast'   : fc,
        'lower_90'   : lo,
        'upper_90'   : hi,
        'validation' : val,
    }


# ══════════════════════════════════════════════════════════════════════
# RUN ALL 10 UNITS
# ══════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    with open('/home/claude/stitched_data.pkl','rb') as f:
        d = pickle.load(f)
    stitched = d['stitched']

    # Forecast horizon: Apr-2026 to Sep-2026
    last_date     = pd.Timestamp('2026-03-01')
    forecast_dates = pd.date_range(
        last_date + pd.DateOffset(months=1), periods=HORIZON, freq='MS')

    results = {}
    print(f"\n{'='*65}")
    print(f"{'Unit':<8} {'Model':<16} {'MAPE%':>7} {'Bias%':>7} "
          f"{'Cov%':>6} {'Score':>6} {'Status'}")
    print(f"{'='*65}")

    for plant in FORECAST_UNITS:
        s = (stitched[stitched['plant_code']==plant]
             .sort_values('date')['consumption_L'].values)

        r = select_and_fit(s, plant)
        results[plant] = r
        v = r['validation']

        status = ('✅ AUTO' if v['conf_score']>=90
                  else '⚠️  REVIEW' if v['conf_score']>=75
                  else '❌ FALLBACK')
        print(f"{plant:<8} {r['model']:<16} {v['mape']:>7.1f} "
              f"{v['bias_pct']:>7.1f} {v['coverage']:>6.1f} "
              f"{v['conf_score']:>6.1f}  {status}")

    print(f"{'='*65}\n")

    # Forecast summary table
    print(f"\n{'FORECAST: Apr-2026 to Sep-2026 (Litres)'}")
    print(f"{'='*65}")
    header = f"{'Unit':<8}" + "".join([f" {d.strftime('%b-%y'):>10}" for d in forecast_dates])
    print(header)
    print("-"*65)
    for plant in FORECAST_UNITS:
        fc = results[plant]['forecast']
        row = f"{plant:<8}" + "".join([f" {v:>10,.0f}" for v in fc])
        print(row)

    # Save results
    with open('/home/claude/forecast_results.pkl','wb') as f:
        pickle.dump({'results':results,
                     'forecast_dates':forecast_dates,
                     'stitched':stitched}, f)
    print("\nResults saved to forecast_results.pkl")
