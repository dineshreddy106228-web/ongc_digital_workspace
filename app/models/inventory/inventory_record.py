"""Inventory Record model – one row per SAP usage export line."""

from datetime import datetime, timezone
from app.extensions import db


class InventoryRecord(db.Model):
    """Stores an individual line from an SAP material-usage export."""

    __tablename__ = "inventory_records"
    __table_args__ = (
        db.Index("ix_inv_records_upload_id", "upload_id"),
        db.Index("ix_inv_records_plant", "plant"),
        db.Index("ix_inv_records_material", "material"),
        db.Index("ix_inv_records_material_group", "material_group"),
        db.Index("ix_inv_records_business_area", "business_area"),
        db.Index("ix_inv_records_material_type", "material_type"),
        db.Index("ix_inv_records_division", "division"),
        db.Index("ix_inv_records_period", "period_year", "period_month"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    upload_id = db.Column(
        db.BigInteger,
        db.ForeignKey("inventory_uploads.id", ondelete="CASCADE"),
        nullable=False,
    )

    # ── SAP export columns ────────────────────────────────────────
    plant = db.Column(db.String(50), nullable=True)
    material = db.Column(db.String(100), nullable=True)
    storage_location = db.Column(db.String(50), nullable=True)
    material_type = db.Column(db.String(50), nullable=True)
    material_group = db.Column(db.String(100), nullable=True)
    business_area = db.Column(db.String(50), nullable=True)
    division = db.Column(db.String(50), nullable=True)

    # Raw "Month" value as it appeared in the SAP export (e.g. "2024.01")
    month_raw = db.Column(db.String(20), nullable=True)
    # Parsed period for easy aggregation
    period_year = db.Column(db.SmallInteger, nullable=True)
    period_month = db.Column(db.SmallInteger, nullable=True)

    # Tot. usage / Tot. usage UoM
    total_usage = db.Column(db.Numeric(18, 3), nullable=True)
    usage_uom = db.Column(db.String(20), nullable=True)

    # Tot.us.val / Tot.us.val Currency
    total_usage_value = db.Column(db.Numeric(18, 2), nullable=True)
    currency = db.Column(db.String(10), nullable=True)

    created_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Helpers ───────────────────────────────────────────────────
    @property
    def period_label(self) -> str:
        """Human-readable period, e.g. 'Jan 2024'."""
        if self.period_year and self.period_month:
            import calendar
            return f"{calendar.month_abbr[self.period_month]} {self.period_year}"
        return self.month_raw or "—"

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<InventoryRecord material={self.material!r} "
            f"plant={self.plant!r} "
            f"period={self.period_year}/{self.period_month}>"
        )
