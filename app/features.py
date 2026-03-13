"""Feature-flag registry for ONGC business modules."""

from dataclasses import dataclass
from importlib import import_module

from flask import current_app, url_for


@dataclass(frozen=True)
class ModuleDefinition:
    """Metadata used for blueprint registration and module UI rendering."""

    code: str
    label: str
    description: str
    icon: str
    feature_flag: str | None = None
    blueprint_import: str | None = None
    url_prefix: str | None = None
    endpoint: str | None = None
    nav_label: str | None = None
    dashboard_badge: str = "Under Controlled Development"


MODULE_REGISTRY = {
    "dashboard": ModuleDefinition(
        code="dashboard",
        label="Dashboard",
        description="Landing page for workspace modules and summary metrics.",
        icon="🏠",
        endpoint="main.dashboard",
        nav_label="Dashboard",
    ),
    "tasks": ModuleDefinition(
        code="tasks",
        label="Office Management / Task Tracker",
        description="Create tasks, track progress, and maintain task update history.",
        icon="🏢",
        feature_flag="ENABLE_OFFICE_MANAGEMENT",
        blueprint_import="app.tasks:tasks_bp",
        url_prefix="/tasks",
        endpoint="tasks.list_tasks",
        nav_label="Task Tracker",
        dashboard_badge="Active",
    ),
    "inventory": ModuleDefinition(
        code="inventory",
        label="Inventory Control",
        description="Track chemicals, equipment, and consumable stock levels.",
        icon="📦",
        feature_flag="ENABLE_INVENTORY",
        blueprint_import="app.inventory:inventory_bp",
        url_prefix="/inventory",
        endpoint="inventory.index",
    ),
    "csc": ModuleDefinition(
        code="csc",
        label="CSC Workflow",
        description="Sample registration, testing workflows, and certificate generation.",
        icon="🔬",
        feature_flag="ENABLE_CSC",
        blueprint_import="app.csc:csc_bp",
        url_prefix="/csc",
        endpoint="csc.index",
    ),
    "reports": ModuleDefinition(
        code="reports",
        label="Reports",
        description="Operational dashboards, audit trails, and data exports.",
        icon="📊",
        feature_flag="ENABLE_REPORTS",
        blueprint_import="app.reports:reports_bp",
        url_prefix="/reports",
        endpoint="reports.index",
    ),
    "admin_users": ModuleDefinition(
        code="admin_users",
        label="User Management",
        description="Create, edit, and manage user accounts and access permissions.",
        icon="👥",
        endpoint="admin.users",
        nav_label="Users",
        dashboard_badge="Active",
    ),
}

FEATURE_CONTROLLED_MODULES = ("tasks", "inventory", "csc", "reports")
ASSIGNABLE_MODULE_CODES = ("dashboard", "tasks", "inventory", "csc", "reports", "admin_users")


def get_module_definition(module_code: str) -> ModuleDefinition | None:
    """Return module metadata for a known module code."""
    return MODULE_REGISTRY.get(module_code)


def is_module_enabled(module_code: str, app=None) -> bool:
    """Return True when a module is enabled in the current environment."""
    definition = get_module_definition(module_code)
    if definition is None:
        return False
    if not definition.feature_flag:
        return True

    target_app = app or current_app._get_current_object()
    return bool(target_app.config.get(definition.feature_flag, False))


def _load_blueprint(import_path: str):
    module_name, attribute_name = import_path.split(":", 1)
    module = import_module(module_name)
    return getattr(module, attribute_name)


def register_feature_blueprints(app) -> None:
    """Register only the feature-enabled business modules."""
    for module_code in FEATURE_CONTROLLED_MODULES:
        definition = MODULE_REGISTRY[module_code]

        # Feature-controlled blueprints stay unregistered when disabled.
        if not is_module_enabled(module_code, app):
            app.logger.info("Skipping disabled module blueprint: %s", module_code)
            continue

        blueprint = _load_blueprint(definition.blueprint_import)
        if definition.url_prefix:
            app.register_blueprint(blueprint, url_prefix=definition.url_prefix)
        else:
            app.register_blueprint(blueprint)


def get_admin_module_options(app=None) -> list[dict]:
    """Return module choices with feature-flag status for admin forms."""
    target_app = app or current_app._get_current_object()
    return [
        {
            "code": code,
            "label": MODULE_REGISTRY[code].label,
            "enabled": is_module_enabled(code, target_app),
            "feature_flag": MODULE_REGISTRY[code].feature_flag,
        }
        for code in ASSIGNABLE_MODULE_CODES
    ]


def get_dashboard_module_cards(user, app=None) -> list[dict]:
    """Build dashboard cards that reflect both feature flags and user access."""
    target_app = app or current_app._get_current_object()
    cards = []

    for code in FEATURE_CONTROLLED_MODULES:
        definition = MODULE_REGISTRY[code]
        enabled = is_module_enabled(code, target_app)

        if enabled and not user.has_module_access(code):
            continue

        cards.append(
            {
                "code": code,
                "label": definition.label,
                "description": definition.description,
                "icon": definition.icon,
                "enabled": enabled,
                "clickable": enabled,
                "href": url_for(definition.endpoint) if enabled and definition.endpoint else None,
                "badge": "Active" if enabled else definition.dashboard_badge,
            }
        )

    return cards
