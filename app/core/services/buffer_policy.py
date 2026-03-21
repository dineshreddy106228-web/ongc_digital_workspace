from __future__ import annotations

"""Buffer policy hooks for future confidence-driven inventory planning."""


def adjust_buffer_by_confidence(base_buffer: float, confidence_score: float | None) -> float:
    """Return a confidence-adjusted buffer quantity.

    This is intentionally simple and acts as a hook for later policy tuning.
    Current default behaviour:

    - low confidence increases the buffer
    - high confidence slightly trims the buffer
    - moderate confidence leaves the buffer unchanged
    """
    if confidence_score is None:
        return max(float(base_buffer), 0.0)

    buffer_qty = max(float(base_buffer), 0.0)
    score = float(confidence_score)
    if score < 50:
        return round(buffer_qty * 1.25, 2)
    if score < 70:
        return round(buffer_qty * 1.15, 2)
    if score > 90:
        return round(buffer_qty * 0.90, 2)
    if score > 80:
        return round(buffer_qty * 0.95, 2)
    return round(buffer_qty, 2)
