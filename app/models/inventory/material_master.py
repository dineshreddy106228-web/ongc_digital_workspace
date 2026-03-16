"""MaterialMaster model – per-material configuration data (Master Data).

Primary key: ``material`` (the SAP material number – matches the ``material``
field used throughout inventory_records and procurement data).

Core attributes map directly to the Master_data.xlsx columns.
Any column in an uploaded workbook that is not mapped here lands in the
``extra_data`` JSON bucket – no further migrations needed.
"""

from datetime import datetime, timezone
from app.extensions import db


# ── Column mapping: Excel header → model field name ──────────────────────────
# Keys must exactly match the header row of Master_data.xlsx (stripped).
# Any column NOT listed here is stored in extra_data.
MASTER_DATA_COLUMN_MAP: dict[str, str] = {
    # ── Core identification ───────────────────────────────────────
    "Material":                       "material",           # primary key
    "Short Text":                     "short_text",
    "Group":                          "group",
    "Type":                           "material_type",
    "Centralization":                 "centralization",
    "Physical State":                 "physical_state",
    # ── Physical / chemical properties ───────────────────────────
    "Volatility":                     "volatility",
    "Sunlight Sensitivity":           "sunlight_sensitivity",
    "Moisture Sensitivity":           "moisture_sensitivity",
    "Temperature Sensitivity":        "temperature_sensitivity",
    "Reactivity":                     "reactivity",
    "Flammable":                      "flammable",
    "Toxic":                          "toxic",
    "Corrosive":                      "corrosive",
    # ── Storage ───────────────────────────────────────────────────
    "Storage Conditions \u2013 General":  "storage_conditions_general",   # –
    "Storage Conditions \u2013 Special":  "storage_conditions_special",
    "Container Type":                 "container_type",
    "Container Capacity":             "container_capacity",
    "Container Description":          "container_description",
    "Primary Storage Classification": "primary_storage_classification",
}

# Fields that are NOT the primary key (used by import logic)
CORE_FIELDS = frozenset(MASTER_DATA_COLUMN_MAP.values()) - {"material"}


class MaterialMaster(db.Model):
    """Stores master configuration parameters for each tracked material."""

    __tablename__ = "material_master"

    # ── Primary key ───────────────────────────────────────────────
    # "material" matches the SAP field name used in inventory_records
    # and procurement data, enabling direct joins.
    material = db.Column(db.String(50), primary_key=True)

    # ── Core identification ───────────────────────────────────────
    short_text     = db.Column(db.String(255), nullable=True)
    group          = db.Column(db.String(100), nullable=True)
    material_type  = db.Column(db.String(100), nullable=True)
    centralization = db.Column(db.String(50),  nullable=True)
    physical_state = db.Column(db.String(50),  nullable=True)

    # ── Physical / chemical properties ───────────────────────────
    volatility              = db.Column(db.String(100), nullable=True)
    sunlight_sensitivity    = db.Column(db.String(100), nullable=True)
    moisture_sensitivity    = db.Column(db.String(100), nullable=True)
    temperature_sensitivity = db.Column(db.String(100), nullable=True)
    reactivity              = db.Column(db.String(100), nullable=True)
    flammable               = db.Column(db.String(50),  nullable=True)
    toxic                   = db.Column(db.String(50),  nullable=True)
    corrosive               = db.Column(db.String(50),  nullable=True)

    # ── Storage ───────────────────────────────────────────────────
    storage_conditions_general      = db.Column(db.String(255), nullable=True)
    storage_conditions_special      = db.Column(db.String(255), nullable=True)
    container_type                  = db.Column(db.String(100), nullable=True)
    container_capacity              = db.Column(db.String(100), nullable=True)
    container_description           = db.Column(db.String(255), nullable=True)
    primary_storage_classification  = db.Column(db.String(100), nullable=True)

    # ── Flexible bucket for truly unknown future columns ──────────
    extra_data = db.Column(db.JSON, nullable=True, default=dict)

    # ── Audit columns ─────────────────────────────────────────────
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_by = db.Column(
        db.BigInteger,
        db.ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    # ── Relationship ──────────────────────────────────────────────
    editor = db.relationship(
        "User",
        foreign_keys=[updated_by],
        lazy="joined",
    )

    # ── Helpers ───────────────────────────────────────────────────
    def to_dict(self) -> dict:
        """Return a JSON-safe dict for the master data API."""
        editor_name = ""
        if self.editor:
            editor_name = self.editor.full_name or self.editor.username or ""
        return {
            "material":                       self.material,
            "short_text":                     self.short_text                    or "",
            "group":                          self.group                         or "",
            "material_type":                  self.material_type                 or "",
            "centralization":                 self.centralization                or "",
            "physical_state":                 self.physical_state                or "",
            "volatility":                     self.volatility                    or "",
            "sunlight_sensitivity":           self.sunlight_sensitivity          or "",
            "moisture_sensitivity":           self.moisture_sensitivity          or "",
            "temperature_sensitivity":        self.temperature_sensitivity       or "",
            "reactivity":                     self.reactivity                    or "",
            "flammable":                      self.flammable                     or "",
            "toxic":                          self.toxic                         or "",
            "corrosive":                      self.corrosive                     or "",
            "storage_conditions_general":     self.storage_conditions_general    or "",
            "storage_conditions_special":     self.storage_conditions_special    or "",
            "container_type":                 self.container_type                or "",
            "container_capacity":             self.container_capacity            or "",
            "container_description":          self.container_description         or "",
            "primary_storage_classification": self.primary_storage_classification or "",
            "extra_data":                     self.extra_data or {},
            "updated_at": (
                self.updated_at.strftime("%d %b %Y, %H:%M")
                if self.updated_at else ""
            ),
            "updated_by": editor_name,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<MaterialMaster material={self.material!r} "
            f"text={self.short_text!r}>"
        )
