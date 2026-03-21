# Demand Forecast: Executive-Level Explanation of Current Logic

## Purpose

The current Forecast module in Inventory Intelligence is designed to answer one practical planning question:

"Based on recent monthly consumption behavior, what is the expected demand for the next 6 months, and what buffer range should planners keep around that expectation?"

It is not a single-model forecast. It is a layered forecasting workflow that:

- builds a clean monthly demand history from consumption data,
- checks whether there is enough usable history to forecast responsibly,
- classifies the demand pattern,
- tests multiple forecasting methods,
- blends the best-performing methods into a central forecast,
- adds probabilistic demand bands around that forecast, and
- presents the result as a 6-month planning view with P5, P10, P50, P90, and P95 quantities.


## What Data It Uses

The active forecast logic uses normalized monthly consumption history, not procurement history, as the main forecasting signal.

- Source used for forecasting:
  - MB51-derived monthly consumption quantities
- How it is prepared:
  - monthly demand is aggregated by year and month
  - missing months inside the historical range are filled as zero-demand months
  - the forecast therefore sees both active months and silent months

Procurement data is not the primary driver of the forecast itself. It is used elsewhere in the module for spend analytics and for deciding which few materials to precompute first for cache warm-up.


## Where The Active Logic Lives

The current Forecast page and material forecast panels use the forecast builder in:

- `app/core/services/inventory_intelligence.py`

Supporting statistical interval logic lives in:

- `app/core/services/inventory_forecast.py`

There is an older forecast helper in:

- `app/modules/inventory/analytics/forecast.py`

That older helper is not the main logic behind the current executive forecast view.


## High-Level Flow

For each material, the current forecast flow is:

1. Build a continuous monthly consumption time series.
2. Check whether the history is sufficient and recent enough to justify forecasting.
3. Classify the demand behavior as steady, variable, sporadic, or lumpy.
4. Generate forecasts from multiple candidate methods.
5. Backtest those methods on recent history.
6. Select the strongest methods and blend them into a single central forecast.
7. Build percentile bands around the forecast using backtested error behavior and bootstrap validation.
8. Return a 6-month plan with a central forecast plus lower and upper planning bands.


## Forecast Entry Gates

The system intentionally refuses to forecast weak histories.

Current gates:

- Minimum history:
  - at least 7 months of monthly history are required
- Minimum recent activity:
  - at least 3 of the last 6 months must show positive consumption

If a material fails either gate, the module does not force a forecast. Instead, it returns a "not enough reliable data" outcome.

This is an important control. It prevents the interface from showing false precision for materials with very thin or stale demand history.


## Demand Pattern Classification

Before choosing methods, the system classifies the material’s demand shape.

Current classes:

- `steady`
  - relatively stable usage, few or no zero months
- `variable`
  - demand exists regularly, but quantity swings are larger
- `sporadic`
  - many zero-demand months, but non-zero demand is not highly erratic
- `lumpy`
  - many zero-demand months and high variability when demand does occur

This matters because the system does not treat all materials the same. Stable materials and intermittent materials need different forecasting techniques.


## Candidate Forecasting Methods

The current engine evaluates several candidate approaches, depending on data richness and demand pattern.

Methods currently in the candidate pool:

- Weighted moving average
- Simple exponential smoothing
- Holt’s trend method
- Holt-Winters seasonal forecasting
- Seasonal naive
- Seasonal naive with trend correction
- Exponential-decay weighted moving average with seasonal index
- Croston’s method for intermittent demand
- SBA (Syntetos-Boylan Approximation) for intermittent/lumpy demand

Not every material gets every method. For example:

- richer histories can use seasonal methods,
- intermittent materials can use Croston/SBA,
- weak seasonal histories fall back to simpler methods.


## How The System Chooses The Forecast

The current design is an adaptive ensemble, not a winner-takes-all model.

In plain business terms, the system does this:

- it runs the eligible methods,
- it backtests each one on recent actual history,
- it scores them based on forecast gap versus actuals,
- it ranks the methods by recent performance,
- it then blends the top two methods into the final forecast.

The blend is confidence-weighted. Stronger recent performers receive more weight.

The result is a central forecast that is generally more robust than trusting a single technique for every material.


## What "Backtesting" Means Here

Backtesting means the system pretends it is in the recent past, forecasts one step ahead using only information that would have existed at that time, and compares that forecast with what actually happened next.

This is done repeatedly over recent months.

That backtest is used for two separate purposes:

- to rank candidate forecast methods for the central forecast
- to determine which uncertainty band is more appropriate for planning


## How Uncertainty Bands Are Built

The current module does not stop at a single number. It returns a probabilistic range.

Key outputs:

- P50:
  - the median or expected forecast
- P10 / P90:
  - narrower planning band
- P5 / P95:
  - wider planning band

The system evaluates candidate band widths using recent backtest behavior and chooses the band that best balances:

- coverage of actual demand,
- width of the band,
- and miss severity when actual demand falls outside the band.

In simple terms:

- if the narrower band is reliable enough, it prefers that,
- if recent behavior is more volatile, it can favor the wider band.

This makes the buffer logic adaptive instead of fixed.


## Why There Are Two Forecast Layers

The current design combines two layers:

1. Adaptive ensemble layer
   - builds the central forward forecast
   - this is the main engine driving the displayed P50 expectation

2. Statistical bootstrap layer
   - validates model behavior and helps produce probabilistic percentile bands
   - this comes from the dedicated forecasting engine in `inventory_forecast.py`

This means the current module is effectively using:

- an operational ensemble for the central plan, and
- a statistical validation engine for interval discipline.

That is stronger than a basic single-line forecast, but it also means the overall design is more complex and should be treated as a managed forecasting framework rather than one formula.


## What The User Sees

For each material, the UI presents:

- historical monthly demand,
- a 6-month forecast horizon,
- P5, P10, P50, P90, and P95 forecast quantities,
- selected planning band labels,
- buffer quantity by month,
- total projected quantity across 6 months,
- recent activity count,
- demand type classification,
- and backtest/validation metadata.

Operationally:

- P50 is the expected demand plan
- Buffer is the incremental upside protection above plan
- Range Width is the full uncertainty spread


## Buffer Logic

The module currently defines the displayed monthly buffer as:

- `P95 - P50`

This is the more defensible procurement buffer because it answers one operational question:

"How much stock should we carry above the expected demand plan to protect against a high-demand case?"

The module also exposes a separate uncertainty measure:

- `Range Width = P95 - P5`

This represents the full spread between a low-demand and high-demand planning scenario.

For leadership discussions, the distinction is:

- `P50` = expected demand plan
- `Buffer (P95 - P50)` = extra cover above plan
- `Range Width (P95 - P5)` = total uncertainty width


## Current Strengths

From an executive perspective, the current design has five strong qualities:

- It avoids forecasting materials with obviously inadequate history.
- It adjusts method choice based on the actual demand pattern.
- It does not depend on a single forecasting model.
- It uses backtesting rather than assumption-based confidence.
- It provides a range-based planning view instead of a false single-point certainty.


## Current Practical Limitations

The current logic is strong for operational planning, but it has clear boundaries.

- It is driven by historical consumption only.
  - It does not yet directly incorporate business drivers such as shutdown plans, project schedules, campaign maintenance, or one-off procurement programs.
- It forecasts from internal history, not external causation.
  - It does not currently model price changes, supplier disruption, policy change, or field activity plans as explicit inputs.
- It is monthly, not weekly.
  - It is suitable for medium-term planning, not short-cycle dispatch planning.
- It requires recent activity.
  - very slow-moving items may be screened out even if they are strategically important
- It currently fills missing historical months as zero demand.
  - that is correct in many cases, but it assumes the absence of recorded demand is real demand silence


## Important Current Design Detail

The forecast cache is precomputed only for a small set of top materials initially, then expanded lazily as users open additional material forecasts.

This means:

- high-priority materials load faster,
- lower-priority materials are still supported,
- but some forecasts are generated on demand rather than fully refreshed upfront.


## Executive Summary

The current Forecast module is a disciplined 6-month demand planning engine built on monthly consumption history. It first checks that the data is strong enough to forecast, then classifies the demand pattern, evaluates multiple forecasting methods, blends the best recent performers, and wraps that result in backtested probabilistic ranges.

In business terms, it is best understood as:

"an adaptive, range-based planning forecast for medium-term inventory decisions"

not as:

"a deterministic promise of future monthly demand."

That distinction is important. The current design is appropriate for planning, prioritization, and buffer-setting. It is not yet a full business-driver forecast model.
