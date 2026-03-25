"""MaterialMaster model – per-material configuration data (Master Data).

Primary key: ``material`` (the SAP material number – matches the ``material``
field used throughout inventory_records and procurement data).

Core attributes map directly to the Master_data.xlsx columns.
Any column in an uploaded workbook that is not mapped here lands in the
``extra_data`` JSON bucket – no further migrations needed.
"""

from datetime import datetime, timezone
from app.core.utils.datetime import format_datetime_ist
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
    "Volatility":                     "volatility_ambient_temperature",
    "Volatility at Ambient Temperature": "volatility_ambient_temperature",
    "Sunlight Sensitivity":           "sunlight_sensitivity_up_to_50c",
    "Sunlight Sensitivity (up to 50degC atmospheric temperature)": "sunlight_sensitivity_up_to_50c",
    "Moisture Sensitivity":           "moisture_sensitivity",
    "Temperature Sensitivity":        "refrigeration_required",
    "Refrigeration required (Yes / No)": "refrigeration_required",
    "Reactivity":                     "reactivity",
    "Flammable":                      "flammable",
    "Toxic":                          "toxic",
    "Corrosive":                      "corrosive",
    # ── Storage ───────────────────────────────────────────────────
    "Storage Conditions \u2013 General":  "storage_conditions_general",   # –
    "Storage Conditions \u2013 Special":  "storage_conditions_special",
    "Container Type":                 "container_type",
    "Container Type 1":               "container_type",
    "Packing Type 1":                 "container_type",
    "Container Capacity":             "container_capacity",
    "Container Capacity 1":           "container_capacity",
    "Packing Size 1":                 "container_capacity",
    "Container Type 2":               "container_type_2",
    "Packing Type 2":                 "container_type_2",
    "Container Capacity 2":           "container_capacity_2",
    "Packing Size 2":                 "container_capacity_2",
    "Container Type 3":               "container_type_3",
    "Packing Type 3":                 "container_type_3",
    "Container Capacity 3":           "container_capacity_3",
    "Packing Size 3":                 "container_capacity_3",
    "Container Type 4":               "container_type_4",
    "Packing Type 4":                 "container_type_4",
    "Container Capacity 4":           "container_capacity_4",
    "Packing Size 4":                 "container_capacity_4",
    "Container Description":          "container_description",
    "Container Description 1":        "container_description",
    "Packing Description 1":          "container_description",
    "Container Description 2":        "container_description_2",
    "Packing Description 2":          "container_description_2",
    "Container Description 3":        "container_description_3",
    "Packing Description 3":          "container_description_3",
    "Container Description 4":        "container_description_4",
    "Packing Description 4":          "container_description_4",
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
    volatility_ambient_temperature = db.Column(db.String(100), nullable=True)
    sunlight_sensitivity_up_to_50c = db.Column(db.String(100), nullable=True)
    moisture_sensitivity    = db.Column(db.String(100), nullable=True)
    refrigeration_required  = db.Column(db.String(100), nullable=True)
    reactivity              = db.Column(db.String(100), nullable=True)
    flammable               = db.Column(db.String(50),  nullable=True)
    toxic                   = db.Column(db.String(50),  nullable=True)
    corrosive               = db.Column(db.String(50),  nullable=True)

    # ── Storage ───────────────────────────────────────────────────
    storage_conditions_general      = db.Column(db.String(255), nullable=True)
    storage_conditions_special      = db.Column(db.String(255), nullable=True)
    container_type                  = db.Column(db.String(100), nullable=True)
    container_capacity              = db.Column(db.String(100), nullable=True)
    container_type_2                = db.Column(db.String(100), nullable=True)
    container_capacity_2            = db.Column(db.String(100), nullable=True)
    container_type_3                = db.Column(db.String(100), nullable=True)
    container_capacity_3            = db.Column(db.String(100), nullable=True)
    container_type_4                = db.Column(db.String(100), nullable=True)
    container_capacity_4            = db.Column(db.String(100), nullable=True)
    container_description           = db.Column(db.String(255), nullable=True)
    container_description_2         = db.Column(db.String(255), nullable=True)
    container_description_3         = db.Column(db.String(255), nullable=True)
    container_description_4         = db.Column(db.String(255), nullable=True)
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
            "volatility_ambient_temperature": self.volatility_ambient_temperature or "",
            "sunlight_sensitivity_up_to_50c": self.sunlight_sensitivity_up_to_50c or "",
            "moisture_sensitivity":           self.moisture_sensitivity          or "",
            "refrigeration_required":         self.refrigeration_required        or "",
            "reactivity":                     self.reactivity                    or "",
            "flammable":                      self.flammable                     or "",
            "toxic":                          self.toxic                         or "",
            "corrosive":                      self.corrosive                     or "",
            "storage_conditions_general":     self.storage_conditions_general    or "",
            "storage_conditions_special":     self.storage_conditions_special    or "",
            "container_type":                 self.container_type                or "",
            "container_capacity":             self.container_capacity            or "",
            "container_type_2":               self.container_type_2              or "",
            "container_capacity_2":           self.container_capacity_2          or "",
            "container_type_3":               self.container_type_3              or "",
            "container_capacity_3":           self.container_capacity_3          or "",
            "container_type_4":               self.container_type_4              or "",
            "container_capacity_4":           self.container_capacity_4          or "",
            "container_description":          self.container_description         or "",
            "container_description_2":        self.container_description_2       or "",
            "container_description_3":        self.container_description_3       or "",
            "container_description_4":        self.container_description_4       or "",
            "primary_storage_classification": self.primary_storage_classification or "",
            "extra_data":                     self.extra_data or {},
            "updated_at": (
                format_datetime_ist(self.updated_at, "%d %b %Y")
                if self.updated_at else ""
            ),
            "updated_by": editor_name,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<MaterialMaster material={self.material!r} "
            f"text={self.short_text!r}>"
        )
