"""Normalized inventory consumption seed rows persisted for fast analytics reads."""

from datetime import datetime, timezone

from app.extensions import db


class InventoryConsumptionSeed(db.Model):
    __tablename__ = "inventory_consumption_seed_rows"
    __table_args__ = (
        db.Index("ix_inv_cons_seed_reporting_plant", "reporting_plant"),
        db.Index("ix_inv_cons_seed_material_desc", "material_desc"),
        db.Index("ix_inv_cons_seed_material_code", "material_code"),
        db.Index("ix_inv_cons_seed_posting_date", "posting_date"),
        db.Index("ix_inv_cons_seed_year_month", "year", "month"),
        db.Index("ix_inv_cons_seed_batch", "import_batch"),
    )

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    import_batch = db.Column(db.String(36), nullable=False)
    source_filename = db.Column(db.String(255), nullable=True)
    imported_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    imported_by = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    material_code = db.Column(db.String(50), nullable=True)
    material_desc = db.Column(db.String(255), nullable=True)
    plant = db.Column(db.String(50), nullable=True)
    reporting_plant = db.Column(db.String(50), nullable=True)
    storage_location = db.Column(db.String(50), nullable=True)
    movement_type = db.Column(db.String(20), nullable=True)
    posting_date = db.Column(db.DateTime, nullable=True)
    year = db.Column(db.Integer, nullable=True)
    month = db.Column(db.Integer, nullable=True)
    month_raw = db.Column(db.String(20), nullable=True)
    financial_year = db.Column(db.String(16), nullable=True)
    usage_qty = db.Column(db.Numeric(18, 3), nullable=True)
    usage_value = db.Column(db.Numeric(18, 2), nullable=True)
    uom = db.Column(db.String(20), nullable=True)
    currency = db.Column(db.String(10), nullable=True)
    po_number = db.Column(db.String(50), nullable=True)
    material_document = db.Column(db.String(50), nullable=True)
    material_document_item = db.Column(db.String(20), nullable=True)

