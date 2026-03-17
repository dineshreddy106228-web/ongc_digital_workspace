"""Base loader for SAP export files.

Handles file-type detection, header normalisation, column validation, chunked
database insertion, and audit logging.  Concrete loaders (MB51, ME2M, MC9)
subclass ``BaseLoader`` and define source-specific mapping and transformation.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import IO, Any, Sequence

import pandas as pd

from app.extensions import db

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000


# ---------------------------------------------------------------------------
# Value objects
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    """Outcome of a single file ingestion attempt."""

    rows_loaded: int = 0
    rows_rejected: int = 0
    rejection_reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.rows_loaded == 0 and self.rows_rejected > 0:
            return "failed"
        if self.rows_rejected > 0:
            return "partial"
        return "success"

    def to_dict(self) -> dict:
        return {
            "rows_loaded": self.rows_loaded,
            "rows_rejected": self.rows_rejected,
            "rejection_reasons": self.rejection_reasons,
            "warnings": self.warnings,
            "status": self.status,
        }


class ValidationError(Exception):
    """Raised when required columns are missing after header normalisation."""

    def __init__(self, missing_columns: list[str], message: str | None = None):
        self.missing_columns = missing_columns
        super().__init__(
            message
            or f"Missing required columns after normalisation: {', '.join(missing_columns)}"
        )


# ---------------------------------------------------------------------------
# Fiscal-year helpers (April–March)
# ---------------------------------------------------------------------------

def fiscal_year_from_date(d: datetime | pd.Timestamp) -> int:
    """Return the fiscal year for a date.  FY2024 = Apr 2023 – Mar 2024."""
    if pd.isna(d):
        return 0
    month = d.month
    year = d.year
    return year + 1 if month >= 4 else year


def fiscal_period_from_date(d: datetime | pd.Timestamp) -> int:
    """Return fiscal period (1–12).  April = 1, March = 12."""
    if pd.isna(d):
        return 0
    month = d.month
    return month - 3 if month >= 4 else month + 9


def fiscal_year_from_period(period_str: str) -> int:
    """Derive fiscal year from a YYYY-MM period string."""
    year, month = int(period_str[:4]), int(period_str[5:7])
    return year + 1 if month >= 4 else year


def fiscal_period_from_period(period_str: str) -> int:
    """Derive fiscal period (1-12) from a YYYY-MM period string."""
    month = int(period_str[5:7])
    return month - 3 if month >= 4 else month + 9


# ---------------------------------------------------------------------------
# BaseLoader
# ---------------------------------------------------------------------------

class BaseLoader:
    """Abstract base for SAP file loaders.

    Subclasses must define:
      - ``SOURCE``:           one of 'MB51', 'ME2M', 'MC9'
      - ``TABLE_NAME``:       target MySQL table
      - ``REQUIRED_COLUMNS``: list of canonical column names that must be present
      - ``HEADER_ALIASES``:   dict mapping known SAP variants → canonical name
      - ``UPSERT_COLUMNS``:   list of columns for the INSERT clause
      - ``UNIQUE_COLUMNS``:   columns forming the UNIQUE KEY (for ON DUPLICATE)
      - ``transform(df)``     → transformed DataFrame ready for insertion
    """

    SOURCE: str = ""
    TABLE_NAME: str = ""
    REQUIRED_COLUMNS: list[str] = []
    HEADER_ALIASES: dict[str, str] = {}
    UPSERT_COLUMNS: list[str] = []
    UNIQUE_COLUMNS: list[str] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def load(
        self,
        file: str | IO[bytes],
        fiscal_year: int | None = None,
        loaded_by: str | None = None,
    ) -> LoadResult:
        """Ingest an SAP export file and return a :class:`LoadResult`."""
        result = LoadResult()
        filename = ""

        try:
            df, filename = self._read_file(file)
            df = self._normalise_headers(df)
            self._validate_columns(df)
            df = self.transform(df, fiscal_year=fiscal_year)
            self._insert_chunks(df, result)
        except ValidationError as exc:
            result.rows_rejected = -1
            result.rejection_reasons.append(str(exc))
            self._log_load(filename, fiscal_year, result, loaded_by, str(exc))
            raise
        except Exception as exc:
            logger.exception("Unexpected error loading %s file", self.SOURCE)
            result.rejection_reasons.append(str(exc))
            self._log_load(filename, fiscal_year, result, loaded_by, str(exc))
            raise

        self._log_load(filename, fiscal_year, result, loaded_by)
        return result

    # ------------------------------------------------------------------
    # File reading
    # ------------------------------------------------------------------

    def _read_file(self, file: str | IO[bytes]) -> tuple[pd.DataFrame, str]:
        """Detect file type and return (DataFrame, filename)."""
        if isinstance(file, str):
            filename = file
            if file.lower().endswith(".csv"):
                df = pd.read_csv(file, dtype=str, keep_default_na=False)
            else:
                df = pd.read_excel(file, dtype=str, engine="openpyxl")
            return df, filename

        # File-like object (BytesIO from Flask upload)
        filename = getattr(file, "name", getattr(file, "filename", "upload"))
        raw = file.read() if hasattr(file, "read") else file
        if isinstance(raw, bytes):
            buf = io.BytesIO(raw)
        else:
            buf = io.BytesIO(raw.encode())

        if filename.lower().endswith(".csv"):
            buf.seek(0)
            df = pd.read_csv(buf, dtype=str, keep_default_na=False)
        else:
            buf.seek(0)
            df = pd.read_excel(buf, dtype=str, engine="openpyxl")

        return df, filename

    # ------------------------------------------------------------------
    # Header normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_header(name: str) -> str:
        """Strip whitespace, lowercase, collapse non-alphanum to underscore."""
        name = name.strip().lower()
        name = re.sub(r"[^a-z0-9]+", "_", name)
        return name.strip("_")

    def _normalise_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map raw SAP column names to canonical column names."""
        # Build a reverse lookup from cleaned alias → canonical
        alias_map: dict[str, str] = {}
        for alias, canonical in self.HEADER_ALIASES.items():
            alias_map[self._clean_header(alias)] = canonical

        new_columns = []
        for col in df.columns:
            cleaned = self._clean_header(col)
            canonical = alias_map.get(cleaned, cleaned)
            new_columns.append(canonical)

        df.columns = new_columns
        return df

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_columns(self, df: pd.DataFrame) -> None:
        """Raise ``ValidationError`` if any required column is absent."""
        present = set(df.columns)
        missing = [c for c in self.REQUIRED_COLUMNS if c not in present]
        if missing:
            raise ValidationError(missing)

    # ------------------------------------------------------------------
    # Transformation (overridden by subclasses)
    # ------------------------------------------------------------------

    def transform(
        self, df: pd.DataFrame, fiscal_year: int | None = None
    ) -> pd.DataFrame:
        """Apply source-specific transformations.  Must be overridden."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Chunked upsert
    # ------------------------------------------------------------------

    def _insert_chunks(self, df: pd.DataFrame, result: LoadResult) -> None:
        """Insert rows in chunks using INSERT … ON DUPLICATE KEY UPDATE."""
        if df.empty:
            result.warnings.append("DataFrame is empty after transformation — nothing to load.")
            return

        columns = [c for c in self.UPSERT_COLUMNS if c in df.columns]
        placeholders = ", ".join([f"%({c})s" for c in columns])
        col_list = ", ".join(columns)
        update_clause = ", ".join(
            [f"{c} = VALUES({c})" for c in columns if c not in self.UNIQUE_COLUMNS]
        )
        sql = (
            f"INSERT INTO {self.TABLE_NAME} ({col_list}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

        connection = db.engine.raw_connection()
        try:
            cursor = connection.cursor()
            for start in range(0, len(df), CHUNK_SIZE):
                chunk = df.iloc[start : start + CHUNK_SIZE]
                rows = chunk.where(pd.notna(chunk), None).to_dict("records")
                for row in rows:
                    try:
                        cursor.execute(sql, row)
                        result.rows_loaded += 1
                    except Exception as exc:
                        result.rows_rejected += 1
                        reason = f"Row rejected ({row.get('mat_doc_no', row.get('po_number', '?'))}): {exc}"
                        if len(result.rejection_reasons) < 50:
                            result.rejection_reasons.append(reason)
                        logger.debug(reason)
                connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------

    def _log_load(
        self,
        filename: str,
        fiscal_year: int | None,
        result: LoadResult,
        loaded_by: str | None,
        error_summary: str | None = None,
    ) -> None:
        """Write a row to data_load_log."""
        sql = (
            "INSERT INTO data_load_log "
            "(source, filename, fiscal_year, rows_loaded, rows_rejected, "
            " load_status, error_summary, loaded_by) "
            "VALUES (%(source)s, %(filename)s, %(fiscal_year)s, %(rows_loaded)s, "
            "%(rows_rejected)s, %(load_status)s, %(error_summary)s, %(loaded_by)s)"
        )
        params = {
            "source": self.SOURCE,
            "filename": filename[:255] if filename else None,
            "fiscal_year": fiscal_year,
            "rows_loaded": max(result.rows_loaded, 0),
            "rows_rejected": max(result.rows_rejected, 0),
            "load_status": result.status,
            "error_summary": (error_summary or "; ".join(result.rejection_reasons[:5]))[:5000] or None,
            "loaded_by": loaded_by,
        }
        try:
            connection = db.engine.raw_connection()
            try:
                cursor = connection.cursor()
                cursor.execute(sql, params)
                connection.commit()
            finally:
                cursor.close()
                connection.close()
        except Exception:
            logger.exception("Failed to write data_load_log entry for %s", self.SOURCE)
