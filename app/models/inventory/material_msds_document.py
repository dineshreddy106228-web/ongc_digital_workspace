"""Compatibility alias for the legacy MSDS model import path."""

from app.models.inventory.msds_file import MSDSFile

MaterialMSDSDocument = MSDSFile

__all__ = ["MSDSFile", "MaterialMSDSDocument"]
