"""Retention of recoverable Inventory and QC workbook payloads.

Import batches keep their parsed rows and source metadata permanently.  The
binary workbook is deliberately short-lived: it is useful for a controlled
rollback, but should not turn the audit trail into an unlimited file store.

Inventory and the weekly QC workbook keep theirs for the rollback window. SAP
daily exports keep only the pairs that still back a laboratory's latest
snapshot, rather than treating the upload most recently received as globally
authoritative.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading

from flask import current_app
from sqlalchemy.orm import undefer

from app.extensions import db
from app.models.inventory.monitoring import InventoryMonitoringUploadBatch
from app.models.quality_control.qc_upload_batch import QCUploadBatch


DEFAULT_ROLLBACK_DAYS = 15


def workbook_rollback_days() -> int:
    """Return the controlled rollback period, defaulting to fifteen days."""
    try:
        days = int(current_app.config.get("AUDIT_WORKBOOK_RETENTION_DAYS", DEFAULT_ROLLBACK_DAYS))
    except (TypeError, ValueError):
        days = DEFAULT_ROLLBACK_DAYS
    return max(days, 1)


def workbook_rollback_cutoff(now: datetime | None = None) -> datetime:
    """Timestamp before which an uploaded workbook payload is no longer kept."""
    reference = now or datetime.now(timezone.utc)
    return reference - timedelta(days=workbook_rollback_days())


def purge_expired_audit_workbook_payloads(
    *, now: datetime | None = None,
) -> dict[str, int]:
    """Clear expired workbook bytes while retaining every audit-row attribute.

    The caller owns the transaction.  ``source_purged_at`` makes this
    idempotent and means users can distinguish an expired source from a missing
    upload.  Empty bytes preserve the existing non-null binary columns and work
    across MySQL and SQLite.
    """
    reference = now or datetime.now(timezone.utc)
    cutoff = workbook_rollback_cutoff(reference)
    counts = {"inventory": 0, "qc_weekly": 0, "qc_sap": 0}

    inventory_batches = InventoryMonitoringUploadBatch.query.options(
        undefer(InventoryMonitoringUploadBatch.source_data)
    ).filter(
        InventoryMonitoringUploadBatch.uploaded_at < cutoff,
        InventoryMonitoringUploadBatch.source_purged_at.is_(None),
    ).all()
    for batch in inventory_batches:
        batch.source_data = b""
        batch.source_purged_at = reference
        counts["inventory"] += 1

    weekly_batches = QCUploadBatch.query.options(
        undefer(QCUploadBatch.source_data)
    ).filter(
        QCUploadBatch.uploaded_at < cutoff,
        QCUploadBatch.source_purged_at.is_(None),
    ).all()
    for batch in weekly_batches:
        batch.source_data = b""
        batch.source_purged_at = reference
        counts["qc_weekly"] += 1

    # SAP daily exports are not kept for the time window. The source pairs for
    # active laboratory snapshots remain, and the sweep clears anything no
    # latest snapshot references so a purge missed at import time is repaired.
    from app.core.services.sap_quality_control import purge_superseded_sap_source_documents

    counts["qc_sap"] = purge_superseded_sap_source_documents(now=reference)

    return counts


def start_audit_workbook_retention_scheduler(app) -> None:
    """Run the retention sweep at startup and at a bounded recurring interval."""
    if getattr(app, "_audit_workbook_retention_scheduler_started", False):
        return
    app._audit_workbook_retention_scheduler_started = True

    try:
        interval_seconds = int(app.config.get("AUDIT_WORKBOOK_RETENTION_CHECK_INTERVAL_SECONDS", 3600))
    except (TypeError, ValueError):
        interval_seconds = 3600
    interval_seconds = max(interval_seconds, 60)

    def _run() -> None:
        stop_event = getattr(app, "_audit_workbook_retention_scheduler_stop", None)
        if stop_event is None:
            stop_event = threading.Event()
            app._audit_workbook_retention_scheduler_stop = stop_event
        while not stop_event.is_set():
            with app.app_context():
                try:
                    counts = purge_expired_audit_workbook_payloads()
                    if any(counts.values()):
                        db.session.commit()
                        app.logger.info("Expired workbook rollback payloads purged: %s", counts)
                    else:
                        db.session.rollback()
                except Exception:  # noqa: BLE001 - next scheduled sweep can retry safely
                    db.session.rollback()
                    app.logger.exception("Automatic workbook-retention sweep failed")
            stop_event.wait(interval_seconds)

    thread = threading.Thread(
        target=_run,
        name="ongc-workbook-retention-scheduler",
        daemon=True,
    )
    app._audit_workbook_retention_scheduler_thread = thread
    thread.start()
