"""Backward-compatible feature helpers backed by the central module registry."""

from app.core.module_registry import (
    FEATURE_CONTROLLED_MODULES,
    MODULES_BY_CODE,
    MODULE_DEFINITIONS,
    get_admin_module_options,
    get_dashboard_module_cards,
    get_module_definition,
    get_nav_modules,
    is_module_enabled,
    is_module_registered,
    is_role_allowed,
    register_module_blueprints,
    user_can_access_module,
)


# Backward-compatible exports used elsewhere in the app.
MODULE_REGISTRY = {
    "dashboard": {"enabled": True},
    "office_management": {"enabled": True},
    "inventory_intelligence": {"enabled": False},
    "csc_workflow": {"enabled": False},
}
ASSIGNABLE_MODULE_CODES = tuple(module.code for module in MODULE_DEFINITIONS)


def register_feature_blueprints(app) -> None:
    """Compatibility wrapper around the module registry bootstrap."""
    register_module_blueprints(app)


__all__ = [
    "ASSIGNABLE_MODULE_CODES",
    "FEATURE_CONTROLLED_MODULES",
    "MODULE_REGISTRY",
    "get_admin_module_options",
    "get_dashboard_module_cards",
    "get_module_definition",
    "get_nav_modules",
    "is_module_enabled",
    "is_module_registered",
    "is_role_allowed",
    "register_feature_blueprints",
    "user_can_access_module",
]
