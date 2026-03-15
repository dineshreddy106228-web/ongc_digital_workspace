"""Central registry for ONGC workspace modules.

This registry is the single source of truth for:
  - blueprint registration
  - navbar visibility
  - dashboard module cards
  - feature-flag activation
  - role compatibility

Keep existing permission codes stable (for example ``tasks``) so current
database rows and route guards continue to work, while allowing business-facing
module keys such as ``office_management`` for future expansion.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING

from flask import current_app, has_app_context, url_for

from app.extensions import cache, db
from app.core.roles import ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE, canonicalize_role_name

if TYPE_CHECKING:
    from app.models.user import User


@dataclass(frozen=True)
class ModuleDefinition:
    """Immutable runtime view of a module registry entry."""

    key: str
    name: str
    blueprint_import: str | None
    url_prefix: str | None
    feature_flag: str | None
    nav_visible: bool
    dashboard_visible: bool
    roles_allowed: tuple[str, ...]
    status: str
    description: str
    permission_code: str | None = None
    endpoint: str | None = None
    nav_label: str | None = None
    icon: str = "🧩"

    @property
    def code(self) -> str:
        """Backward-compatible permission code used elsewhere in the app."""
        return self.permission_code or self.key

    @property
    def label(self) -> str:
        """Backward-compatible alias used by older feature helpers."""
        return self.name


MODULES = [
    {
        "key": "office_management",
        "name": "Office Management",
        "permission_code": "tasks",
        "blueprint_import": "app.tasks:tasks_bp",
        "url_prefix": "/tasks",
        "endpoint": "tasks.task_dashboard",
        "feature_flag": "ENABLE_OFFICE_MANAGEMENT",
        "nav_visible": True,
        "dashboard_visible": True,
        "roles_allowed": [ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE],
        "status": "active",
        "description": "Task tracking and office workflow management",
        "nav_label": "Task Tracker",
        "icon": "🏢",
    },
    {
        "key": "inventory",
        "name": "Inventory Intelligence",
        "permission_code": "inventory",
        "blueprint_import": "app.inventory:inventory_bp",
        "url_prefix": "/inventory",
        "endpoint": "inventory.index",
        "feature_flag": "ENABLE_INVENTORY",
        "nav_visible": True,
        "dashboard_visible": True,
        "roles_allowed": [ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE],
        "status": "active",
        "description": "SAP material consumption & procurement analytics with interactive drilldowns",
        "nav_label": "Inventory Intelligence",
        "icon": "📦",
    },
    {
        "key": "csc_workflow",
        "name": "CSC Workflow",
        "permission_code": "csc",
        "blueprint_import": "app.csc:csc_bp",
        "url_prefix": "/csc",
        "endpoint": "csc.index",
        "feature_flag": "ENABLE_CSC",
        "nav_visible": True,
        "dashboard_visible": True,
        "roles_allowed": [ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE],
        "status": "planned",
        "description": "Sample registration, testing workflows, and certificate generation",
        "icon": "🔬",
    },
    {
        "key": "reports",
        "name": "Reports",
        "permission_code": "reports",
        "blueprint_import": "app.reports:reports_bp",
        "url_prefix": "/reports",
        "endpoint": "reports.index",
        "feature_flag": "ENABLE_REPORTS",
        "nav_visible": True,
        "dashboard_visible": True,
        "roles_allowed": [ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE],
        "status": "planned",
        "description": "Operational dashboards, audit trails, and data exports",
        "icon": "📊",
    },
    {
        "key": "forecasting",
        "name": "Forecasting",
        "permission_code": "forecasting",
        "blueprint_import": "app.forecasting:forecasting_bp",
        "url_prefix": "/forecasting",
        "endpoint": "forecasting.index",
        "feature_flag": "ENABLE_FORECASTING",
        "nav_visible": True,
        "dashboard_visible": True,
        "roles_allowed": [ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE],
        "status": "planned",
        "description": "Demand planning, projections, and forecasting workflows",
        "icon": "📈",
    },
    {
        "key": "admin_users",
        "name": "User Management",
        "permission_code": "admin_users",
        "blueprint_import": "app.admin:admin_bp",
        "url_prefix": "/admin",
        "endpoint": "admin.users",
        "feature_flag": None,
        "nav_visible": True,
        "dashboard_visible": True,
        "roles_allowed": [ADMIN_ROLE],
        "status": "active",
        "description": "Create, edit, and manage user accounts and access permissions",
        "nav_label": "Users",
        "icon": "👥",
    },
]


def _to_definition(module: dict) -> ModuleDefinition:
    return ModuleDefinition(
        key=module["key"],
        name=module["name"],
        blueprint_import=module.get("blueprint_import"),
        url_prefix=module.get("url_prefix"),
        feature_flag=module.get("feature_flag"),
        nav_visible=bool(module.get("nav_visible", False)),
        dashboard_visible=bool(module.get("dashboard_visible", False)),
        roles_allowed=tuple(module.get("roles_allowed", [])),
        status=module.get("status", "planned"),
        description=module.get("description", ""),
        permission_code=module.get("permission_code"),
        endpoint=module.get("endpoint"),
        nav_label=module.get("nav_label"),
        icon=module.get("icon", "🧩"),
    )


MODULE_DEFINITIONS = tuple(_to_definition(module) for module in MODULES)
MODULES_BY_KEY = {module.key: module for module in MODULE_DEFINITIONS}
MODULES_BY_CODE = {module.code: module for module in MODULE_DEFINITIONS}
FEATURE_CONTROLLED_MODULES = tuple(
    module.code for module in MODULE_DEFINITIONS if module.feature_flag
)


def _get_module_definition_uncached(module_key_or_code: str) -> ModuleDefinition | None:
    """Resolve a module by business key or legacy permission code without cache."""
    return MODULES_BY_KEY.get(module_key_or_code) or MODULES_BY_CODE.get(module_key_or_code)


@cache.memoize(timeout=300)
def _get_module_definition_cached(module_key_or_code: str) -> ModuleDefinition | None:
    """Memoized module lookup for request-time registry access."""
    return _get_module_definition_uncached(module_key_or_code)


def get_module_definition(module_key_or_code: str) -> ModuleDefinition | None:
    """Resolve a module by business key or legacy permission code."""
    if has_app_context():
        return _get_module_definition_cached(module_key_or_code)
    return _get_module_definition_uncached(module_key_or_code)


def get_permission_code(module_key_or_code: str) -> str | None:
    definition = get_module_definition(module_key_or_code)
    if definition is None:
        return None
    return definition.code


def is_module_enabled(module_key_or_code: str, app=None) -> bool:
    """Return True when a module's feature flag is enabled for the target app."""
    definition = get_module_definition(module_key_or_code)
    if definition is None:
        return False
    if not definition.feature_flag:
        return True

    target_app = app or current_app._get_current_object()
    return bool(target_app.config.get(definition.feature_flag, False))


def is_role_allowed(module_key_or_code: str, user) -> bool:
    """Check registry role compatibility for the current user."""
    definition = get_module_definition(module_key_or_code)
    if definition is None:
        return False
    if not definition.roles_allowed:
        return True

    role = getattr(user, "role", None)
    role_name = canonicalize_role_name(getattr(role, "name", None))
    return role_name in definition.roles_allowed


def _load_blueprint(import_path: str):
    module_name, attribute_name = import_path.split(":", 1)
    module = import_module(module_name)
    return getattr(module, attribute_name)


def _is_missing_blueprint_import(module_name: str, exc: ModuleNotFoundError) -> bool:
    return exc.name == module_name


def register_module_blueprints(app) -> None:
    """Register enabled module blueprints from the central registry."""
    registry_state = app.extensions.setdefault("module_registry", {})
    registered_keys: set[str] = set()
    missing_keys: set[str] = set()

    for definition in MODULE_DEFINITIONS:
        if not definition.blueprint_import:
            continue

        if not is_module_enabled(definition.key, app):
            app.logger.info("Skipping disabled module blueprint: %s", definition.key)
            continue

        module_name, _attribute_name = definition.blueprint_import.split(":", 1)
        try:
            blueprint = _load_blueprint(definition.blueprint_import)
        except ModuleNotFoundError as exc:
            if _is_missing_blueprint_import(module_name, exc):
                app.logger.warning(
                    "Skipping module '%s' because blueprint module '%s' does not exist yet.",
                    definition.key,
                    module_name,
                )
                missing_keys.add(definition.key)
                continue
            raise
        except AttributeError:
            app.logger.warning(
                "Skipping module '%s' because blueprint '%s' could not be resolved.",
                definition.key,
                definition.blueprint_import,
            )
            missing_keys.add(definition.key)
            continue

        if definition.url_prefix:
            app.register_blueprint(blueprint, url_prefix=definition.url_prefix)
        else:
            app.register_blueprint(blueprint)
        registered_keys.add(definition.key)

    registry_state["registered_keys"] = registered_keys
    registry_state["missing_keys"] = missing_keys


def is_module_registered(module_key_or_code: str, app=None) -> bool:
    """Return True when the module blueprint was registered successfully."""
    definition = get_module_definition(module_key_or_code)
    if definition is None:
        return False

    target_app = app or current_app._get_current_object()
    registry_state = target_app.extensions.get("module_registry", {})
    return definition.key in registry_state.get("registered_keys", set())


def user_can_access_module(module_key_or_code: str, user, app=None) -> bool:
    """Combined check for feature flags, role compatibility, access grants, and routing."""
    definition = get_module_definition(module_key_or_code)
    if definition is None:
        return False
    if user is None:
        return False
    if not getattr(user, "is_authenticated", False):
        return False
    if not getattr(user, "is_active", False):
        return False
    if not is_module_enabled(definition.key, app):
        return False
    if not is_module_registered(definition.key, app):
        return False
    if not is_role_allowed(definition.key, user):
        return False

    has_module_access = getattr(user, "has_module_access", None)
    if not callable(has_module_access):
        return False
    return bool(has_module_access(definition.code))


def _status_badge(definition: ModuleDefinition) -> str:
    return "Active" if definition.status == "active" else definition.status.replace("_", " ").title()


def _user_cache_version(user: "User") -> str:
    role_name = canonicalize_role_name(user.role.name) if user.role else ""
    permission_codes = ",".join(
        sorted(permission.module_code for permission in user.module_permissions.filter_by(can_access=True).all())
    )
    return "|".join(
        [
            role_name,
            "1" if bool(user.is_active) else "0",
            str(user.updated_at.isoformat() if getattr(user, "updated_at", None) else ""),
            permission_codes,
        ]
    )


def _build_nav_modules(user, app=None) -> list[dict]:
    modules = []
    for definition in MODULE_DEFINITIONS:
        if not definition.nav_visible:
            continue
        if not definition.endpoint:
            continue
        if not user_can_access_module(definition.key, user, app):
            continue
        modules.append(
            {
                "key": definition.key,
                "code": definition.code,
                "endpoint": definition.endpoint,
                "name": definition.name,
                "label": definition.nav_label or definition.name,
                "href": url_for(definition.endpoint),
                "icon": definition.icon,
                "status": definition.status,
            }
        )
    return modules


@cache.memoize(timeout=300)
def _get_nav_modules_cached(user_id: int, cache_version: str) -> list[dict]:
    from app.models.user import User

    user = db.session.get(User, user_id)
    if user is None:
        return []
    return _build_nav_modules(user)


def get_nav_modules(user, app=None) -> list[dict]:
    """Return navbar-ready module links based on registry metadata."""
    if not getattr(user, "is_authenticated", False):
        return []
    return _get_nav_modules_cached(user.id, _user_cache_version(user))


def _build_dashboard_module_cards(user, app=None) -> list[dict]:
    cards = []
    for definition in MODULE_DEFINITIONS:
        if not definition.dashboard_visible:
            continue
        if not definition.endpoint:
            continue
        if not user_can_access_module(definition.key, user, app):
            continue
        cards.append(
            {
                "key": definition.key,
                "code": definition.code,
                "label": definition.name,
                "description": definition.description,
                "icon": definition.icon,
                "enabled": True,
                "clickable": True,
                "href": url_for(definition.endpoint),
                "badge": _status_badge(definition),
            }
        )
    return cards


@cache.memoize(timeout=300)
def _get_dashboard_module_cards_cached(user_id: int, cache_version: str) -> list[dict]:
    from app.models.user import User

    user = db.session.get(User, user_id)
    if user is None:
        return []
    return _build_dashboard_module_cards(user)


def get_dashboard_module_cards(user, app=None) -> list[dict]:
    """Return dashboard-ready module cards from the registry."""
    if not getattr(user, "is_authenticated", False):
        return []
    return _get_dashboard_module_cards_cached(user.id, _user_cache_version(user))


def get_admin_module_options(app=None) -> list[dict]:
    """Return admin-facing permission choices, preserving existing module codes."""
    target_app = app or current_app._get_current_object()
    options = [
        {
            "code": "dashboard",
            "label": "Dashboard",
            "enabled": True,
            "feature_flag": None,
        }
    ]

    for definition in MODULE_DEFINITIONS:
        options.append(
            {
                "code": definition.code,
                "label": definition.name,
                "enabled": is_module_enabled(definition.key, target_app),
                "feature_flag": definition.feature_flag,
            }
        )

    return options


def invalidate_user_module_access_cache(user_id: int, user_cache_version: str | None = None) -> None:
    """Clear cached nav and dashboard card data for a user."""
    if user_id is None:
        return
    cache.delete_memoized(_get_nav_modules_cached)
    cache.delete_memoized(_get_dashboard_module_cards_cached)
