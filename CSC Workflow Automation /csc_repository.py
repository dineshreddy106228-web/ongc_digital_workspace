"""CSC Drafting Workspace — SQLite data access layer.

All queries use parameterized placeholders. All PKs are UUID strings.
Database: data/CSC_Data.db (separate from OCC_HCC_Data.db).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from csc_utils import (
    new_uuid,
    get_utc_now,
    sanitize_text,
    sanitize_multiline_text,
    truncate_for_log,
    ISSUE_TYPES,
    sort_specs_by_subset_order,
    DEFAULT_PREPARED_BY,
)

logger = logging.getLogger(__name__)

# Standardized max length for parameter text fields.
_PARAM_TEXT_MAX_LENGTH = 10_000

_TYPE_CLASS_ALIASES = {
    "essential": "Essential",
    "desirable": "Desirable",
    "vital": "Desirable",
    "essential-informational": "Desirable",
    "essential informational": "Desirable",
    "essential / informational": "Desirable",
    "essential - informational": "Desirable",
    "essential/informational": "Desirable",
}

_TEST_PROCEDURE_TYPE_ALIASES = {
    "astm": "ASTM",
    "api": "API",
    "is": "IS",
    "inhouse": "Inhouse",
    "in-house": "Inhouse",
    "other national / international": "Other National / International",
    "other national/international": "Other National / International",
    "other national international": "Other National / International",
}


def normalize_parameter_type_label(raw: str) -> str:
    """Normalize user/file-provided type labels to canonical values."""
    txt = str(raw or "").strip().lower()
    if not txt:
        return "Essential"
    return _TYPE_CLASS_ALIASES.get(txt, "Essential")


def normalize_prepared_by(raw: str) -> str:
    """Normalize Prepared By with project-level default fallback."""
    txt = str(raw or "").strip()
    if not txt:
        return DEFAULT_PREPARED_BY
    return txt


def parse_yes_no_flag(raw: Any) -> bool:
    """Parse flexible yes/no cell values into a bool."""
    txt = str(raw or "").strip().lower()
    if txt in {"yes", "y", "true", "1"}:
        return True
    if txt in {"no", "n", "false", "0", ""}:
        return False
    return False


def normalize_test_procedure_type(raw: str) -> str:
    """Normalize user/file-provided test procedure type labels."""
    txt = str(raw or "").strip()
    if not txt:
        return ""
    key = txt.lower()
    return _TEST_PROCEDURE_TYPE_ALIASES.get(key, txt)


def calculate_impact_score(
    operational_impact_score: int,
    safety_environment_score: int,
    supply_risk_score: int,
) -> float:
    """Calculate weighted impact score using fixed framework weights."""
    return (
        float(operational_impact_score) * 0.5
        + float(safety_environment_score) * 0.3
        + float(supply_risk_score) * 0.2
    )


def get_impact_grade(
    impact_score_total: float,
    no_substitute_flag: int = 0,
) -> str:
    """Return final impact grade, applying no-substitute override rule."""
    if impact_score_total >= 4.0:
        grade = "CRITICAL"
    elif impact_score_total >= 3.0:
        grade = "HIGH"
    elif impact_score_total >= 2.0:
        grade = "MODERATE"
    else:
        grade = "LOW"

    if int(no_substitute_flag or 0) == 1 and grade in {"LOW", "MODERATE"}:
        return "HIGH"
    return grade

# ---------------------------------------------------------------------------
# Schema SQL
# ---------------------------------------------------------------------------

_SCHEMA_V1 = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 30000;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS T_CSC_Schema_Version (
    version     INTEGER NOT NULL,
    applied_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS csc_drafts (
    draft_id        TEXT PRIMARY KEY,
    spec_number     TEXT NOT NULL,
    chemical_name   TEXT NOT NULL,
    committee_name  TEXT NOT NULL DEFAULT 'Chemical Specification Committee',
    meeting_date    TEXT,
    prepared_by     TEXT NOT NULL DEFAULT 'Dinesh Reddy_106228',
    reviewed_by     TEXT,
    status          TEXT NOT NULL DEFAULT 'Draft',
    created_by_role TEXT NOT NULL DEFAULT 'User',
    created_by_user TEXT NOT NULL DEFAULT '',
    is_admin_draft  INTEGER NOT NULL DEFAULT 0,
    phase1_locked   INTEGER NOT NULL DEFAULT 0,
    parent_draft_id TEXT,
    submitted_for_approval_at TEXT,
    admin_stage     TEXT NOT NULL DEFAULT 'ingested',
    spec_version    INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS csc_sections (
    section_id      TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL,
    section_name    TEXT NOT NULL,
    section_text    TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE,
    UNIQUE (draft_id, section_name)
);

CREATE TABLE IF NOT EXISTS csc_parameters (
    parameter_id    TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL,
    parameter_name  TEXT NOT NULL,
    parameter_type  TEXT NOT NULL DEFAULT 'Essential',
    existing_value  TEXT,
    proposed_value  TEXT,
    test_method     TEXT,
    test_procedure_type TEXT NOT NULL DEFAULT '',
    test_procedure_text TEXT,
    justification   TEXT,
    remarks         TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS csc_issue_flags (
    issue_id        TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL,
    issue_type      TEXT NOT NULL,
    is_present      INTEGER NOT NULL DEFAULT 0,
    note            TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS csc_audit (
    audit_id        TEXT PRIMARY KEY,
    draft_id        TEXT,
    action          TEXT NOT NULL,
    user_name       TEXT NOT NULL,
    action_time     TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    remarks         TEXT
);

CREATE TABLE IF NOT EXISTS impact_analysis (
    impact_id                    TEXT PRIMARY KEY,
    draft_id                     TEXT NOT NULL,
    operational_impact_score     INTEGER NOT NULL,
    safety_environment_score     INTEGER NOT NULL,
    supply_risk_score            INTEGER NOT NULL,
    no_substitute_flag           INTEGER NOT NULL DEFAULT 0,
    impact_score_total           REAL NOT NULL,
    impact_grade                 TEXT NOT NULL,
    operational_note             TEXT,
    safety_environment_note      TEXT,
    supply_risk_note             TEXT,
    reviewed_by                  TEXT,
    review_date                  TEXT,
    created_at                   TEXT,
    updated_at                   TEXT,
    FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE,
    UNIQUE (draft_id)
);

CREATE INDEX IF NOT EXISTS idx_csc_sections_draft
    ON csc_sections(draft_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_csc_params_draft
    ON csc_parameters(draft_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_csc_issues_draft
    ON csc_issue_flags(draft_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_csc_audit_draft
    ON csc_audit(draft_id);
CREATE INDEX IF NOT EXISTS idx_csc_audit_time
    ON csc_audit(action_time);
CREATE INDEX IF NOT EXISTS idx_impact_analysis_draft
    ON impact_analysis(draft_id);

CREATE TABLE IF NOT EXISTS csc_spec_versions (
    snapshot_id    TEXT PRIMARY KEY,
    draft_id       TEXT NOT NULL,
    spec_version   INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    created_by     TEXT NOT NULL DEFAULT 'system',
    source_action  TEXT NOT NULL DEFAULT '',
    remarks        TEXT,
    payload_json   TEXT NOT NULL,
    FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE,
    UNIQUE (draft_id, spec_version)
);

CREATE INDEX IF NOT EXISTS idx_csc_spec_versions_draft
    ON csc_spec_versions(draft_id, spec_version);
"""

_SCHEMA_V2_MIGRATIONS = [
    # SQLite ALTER TABLE only supports ADD COLUMN
    "ALTER TABLE csc_drafts ADD COLUMN created_by_role TEXT NOT NULL DEFAULT 'User';",
    "ALTER TABLE csc_drafts ADD COLUMN is_admin_draft  INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE csc_drafts ADD COLUMN phase1_locked   INTEGER NOT NULL DEFAULT 0;",
]

_SCHEMA_V3_MIGRATIONS = [
    # admin_stage tracks the Member Secretary's pipeline:
    # 'ingested' → 'structuring' → 'published'
    "ALTER TABLE csc_drafts ADD COLUMN admin_stage TEXT NOT NULL DEFAULT 'ingested';",
]

_SCHEMA_V4_MIGRATIONS = [
    # spec_version: integer version of the master data for this spec.
    # Starts at 0 on first ingest/creation, increments on each admin save.
    "ALTER TABLE csc_drafts ADD COLUMN spec_version INTEGER NOT NULL DEFAULT 0;",
]

_SCHEMA_V5_MIGRATIONS = [
    "ALTER TABLE csc_drafts ADD COLUMN created_by_user TEXT NOT NULL DEFAULT '';",
    "ALTER TABLE csc_drafts ADD COLUMN parent_draft_id TEXT;",
    "ALTER TABLE csc_drafts ADD COLUMN submitted_for_approval_at TEXT;",
    "CREATE INDEX IF NOT EXISTS idx_csc_drafts_parent ON csc_drafts(parent_draft_id);",
]

_SCHEMA_V6_MIGRATIONS = [
    """
    CREATE TABLE IF NOT EXISTS impact_analysis (
        impact_id                    TEXT PRIMARY KEY,
        draft_id                     TEXT NOT NULL,
        operational_impact_score     INTEGER NOT NULL,
        safety_environment_score     INTEGER NOT NULL,
        supply_risk_score            INTEGER NOT NULL,
        no_substitute_flag           INTEGER NOT NULL DEFAULT 0,
        impact_score_total           REAL NOT NULL,
        impact_grade                 TEXT NOT NULL,
        operational_note             TEXT,
        safety_environment_note      TEXT,
        supply_risk_note             TEXT,
        reviewed_by                  TEXT,
        review_date                  TEXT,
        created_at                   TEXT,
        updated_at                   TEXT,
        FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE,
        UNIQUE (draft_id)
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_impact_analysis_unique_draft ON impact_analysis(draft_id);",
]

_SCHEMA_V7_MIGRATIONS = [
    """
    CREATE TABLE IF NOT EXISTS csc_spec_versions (
        snapshot_id    TEXT PRIMARY KEY,
        draft_id       TEXT NOT NULL,
        spec_version   INTEGER NOT NULL,
        created_at     TEXT NOT NULL,
        created_by     TEXT NOT NULL DEFAULT 'system',
        source_action  TEXT NOT NULL DEFAULT '',
        remarks        TEXT,
        payload_json   TEXT NOT NULL,
        FOREIGN KEY (draft_id) REFERENCES csc_drafts(draft_id) ON DELETE CASCADE,
        UNIQUE (draft_id, spec_version)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_csc_spec_versions_draft ON csc_spec_versions(draft_id, spec_version);",
]

_SCHEMA_V8_MIGRATIONS = [
    "ALTER TABLE csc_parameters ADD COLUMN test_procedure_type TEXT NOT NULL DEFAULT '';",
    "ALTER TABLE csc_parameters ADD COLUMN test_procedure_text TEXT;",
]

_SCHEMA_V9_MIGRATIONS = [
    "UPDATE csc_drafts SET spec_version = 0;",
    "UPDATE csc_drafts SET prepared_by = 'Dinesh Reddy_106228';",
    "UPDATE csc_parameters SET parameter_type = 'Essential';",
]

# Admin workflow stage constants (3-stage: no separate reviewing stage)
ADMIN_STAGE_INGESTED    = "ingested"     # Just extracted from PDF, not yet structured
ADMIN_STAGE_STRUCTURING = "structuring"  # MS is actively editing / correcting
ADMIN_STAGE_REVIEWING   = "reviewing"    # kept for DB backward compat — treated as structuring
ADMIN_STAGE_PUBLISHED   = "published"    # Master data finalised; visible in master export
ADMIN_STAGE_PENDING_APPROVAL = "pending_approval"  # User revision awaiting admin action


# ---------------------------------------------------------------------------
# Repository class
# ---------------------------------------------------------------------------

class CSCRepository:
    """Data access layer for CSC Drafting Workspace."""

    SCHEMA_VERSION = 9

    def __init__(self, db_file: str) -> None:
        self._db_file = db_file
        Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_file, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    # ------------------------------------------------------------------
    # Schema initialization / migration
    # ------------------------------------------------------------------

    def ensure_db(self) -> None:
        """Create tables and run any pending migrations."""
        _MIGRATIONS: list[tuple[int, list[str]]] = [
            (2, _SCHEMA_V2_MIGRATIONS),
            (3, _SCHEMA_V3_MIGRATIONS),
            (4, _SCHEMA_V4_MIGRATIONS),
            (5, _SCHEMA_V5_MIGRATIONS),
            (6, _SCHEMA_V6_MIGRATIONS),
            (7, _SCHEMA_V7_MIGRATIONS),
            (8, _SCHEMA_V8_MIGRATIONS),
            (9, _SCHEMA_V9_MIGRATIONS),
        ]
        with self._connect() as conn:
            conn.executescript(_SCHEMA_V1)
            current = self._current_version(conn)
            for target_version, stmts in _MIGRATIONS:
                if current >= target_version:
                    continue
                for sql in stmts:
                    try:
                        conn.execute(sql)
                    except sqlite3.OperationalError as exc:
                        # ALTER TABLE ADD COLUMN on an existing column is expected
                        # when the DB was already partially migrated; ignore it.
                        if "duplicate column name" in str(exc).lower() or "already exists" in str(exc).lower():
                            logger.debug("Migration v%d skipped (already applied): %s", target_version, exc)
                        else:
                            logger.warning("Migration v%d statement failed: %s — %s", target_version, sql[:80], exc)
                self._stamp_version(conn, target_version)
                current = target_version
            conn.commit()

    def _current_version(self, conn: sqlite3.Connection) -> int:
        try:
            row = conn.execute(
                "SELECT MAX(version) FROM T_CSC_Schema_Version"
            ).fetchone()
            return row[0] or 0
        except sqlite3.OperationalError:
            return 0

    def _stamp_version(self, conn: sqlite3.Connection, version: int) -> None:
        now = get_utc_now().isoformat()
        conn.execute(
            "INSERT INTO T_CSC_Schema_Version (version, applied_at) VALUES (?, ?)",
            (version, now),
        )

    # ------------------------------------------------------------------
    # Draft CRUD
    # ------------------------------------------------------------------

    def create_draft(
        self,
        spec_number: str,
        chemical_name: str,
        committee_name: str,
        meeting_date: str,
        prepared_by: str,
        reviewed_by: str,
        user_name: str,
        test_procedure: str = "",
        material_code: str = "",
        created_by_role: str = "User",
        created_by_user: str = "",
        is_admin_draft: bool = False,
        phase1_locked: bool = False,
        admin_stage: str = "ingested",
        parent_draft_id: str | None = None,
        submitted_for_approval_at: str | None = None,
        spec_version_start: int = 0,
    ) -> str:
        """Create a new draft record and default issue flag stubs. Returns draft_id."""
        draft_id = new_uuid()
        now = get_utc_now().isoformat()
        initial_status = "Draft"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO csc_drafts
                    (draft_id, spec_number, chemical_name, committee_name,
                     meeting_date, prepared_by, reviewed_by, status,
                     created_by_role, created_by_user, is_admin_draft, phase1_locked,
                     parent_draft_id, submitted_for_approval_at,
                     admin_stage, spec_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    draft_id,
                    sanitize_text(spec_number, max_length=200),
                    sanitize_text(chemical_name, max_length=300),
                    sanitize_text(committee_name, max_length=300),
                    sanitize_text(meeting_date or "", max_length=50),
                    sanitize_text(normalize_prepared_by(prepared_by), max_length=200),
                    sanitize_text(reviewed_by or "", max_length=200),
                    initial_status,
                    sanitize_text(created_by_role, max_length=20),
                    sanitize_text(created_by_user or user_name, max_length=200),
                    1 if is_admin_draft else 0,
                    1 if phase1_locked else 0,
                    parent_draft_id,
                    sanitize_text(submitted_for_approval_at or "", max_length=50) or None,
                    sanitize_text(admin_stage, max_length=20),
                    max(0, int(spec_version_start)),
                    now,
                    now,
                ),
            )

            # Insert default issue flag stubs (is_present=0 / No)
            for idx, (issue_type, _label) in enumerate(ISSUE_TYPES):
                conn.execute(
                    """
                    INSERT INTO csc_issue_flags
                        (issue_id, draft_id, issue_type, is_present, note, sort_order)
                    VALUES (?, ?, ?, 0, '', ?)
                    """,
                    (new_uuid(), draft_id, issue_type, idx),
                )

            # Store test_procedure and material_code as special meta sections
            for meta_key, meta_val, meta_order in [
                ("Meta::test_procedure", test_procedure, 90),
                ("Meta::material_code",  material_code,  91),
            ]:
                if meta_val:
                    conn.execute(
                        """INSERT INTO csc_sections
                               (section_id, draft_id, section_name, section_text, sort_order)
                           VALUES (?, ?, ?, ?, ?)
                           ON CONFLICT(draft_id, section_name)
                           DO UPDATE SET section_text = excluded.section_text""",
                        (new_uuid(), draft_id,
                         meta_key,
                         sanitize_text(meta_val, max_length=500),
                         meta_order),
                    )

            conn.commit()

        # Capture initial baseline for admin master specs (v0 by default).
        if is_admin_draft and parent_draft_id is None:
            self.capture_admin_version_snapshot(
                draft_id=draft_id,
                user_name=user_name,
                source_action="CREATE_BASELINE",
                remarks="Initial admin baseline snapshot",
                force_version=max(0, int(spec_version_start)),
                replace_existing=True,
            )

        self.log_audit(draft_id, "CREATE_DRAFT", user_name,
                       new_value=f"{spec_number} — {chemical_name}")
        logger.info("Draft created: %s (%s)", draft_id, spec_number)
        return draft_id

    def load_draft(self, draft_id: str) -> dict[str, Any] | None:
        """Load draft metadata row as a dict, injecting test_procedure and material_code."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM csc_drafts WHERE draft_id = ?", (draft_id,)
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            # Hydrate meta fields from csc_sections
            for meta_key, field_name in [
                ("Meta::test_procedure", "test_procedure"),
                ("Meta::material_code",  "material_code"),
            ]:
                meta_row = conn.execute(
                    "SELECT section_text FROM csc_sections WHERE draft_id=? AND section_name=?",
                    (draft_id, meta_key),
                ).fetchone()
                result[field_name] = meta_row[0] if meta_row else ""
        return result

    def list_drafts(
        self,
        admin_only: bool = False,
        committee_visible_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return summary list of drafts.

        admin_only=True  → only admin-created drafts (for admin workspace).
        committee_visible_only=True → only drafts published for committee review.
        Default: all drafts (backward compat).
        """
        _BASE_QUERY = """
            SELECT draft_id, spec_number, chemical_name,
                   committee_name, status, meeting_date,
                   prepared_by, created_at, updated_at,
                   is_admin_draft, phase1_locked, created_by_role,
                   created_by_user, parent_draft_id, submitted_for_approval_at,
                   admin_stage, spec_version
            FROM csc_drafts
        """
        with self._connect() as conn:
            if committee_visible_only:
                rows = conn.execute(
                    _BASE_QUERY + " WHERE is_admin_draft = 1 AND admin_stage = ? ORDER BY updated_at DESC",
                    (ADMIN_STAGE_PUBLISHED,),
                ).fetchall()
            elif admin_only:
                rows = conn.execute(
                    _BASE_QUERY + " WHERE is_admin_draft = 1 ORDER BY updated_at DESC",
                ).fetchall()
            else:
                rows = conn.execute(
                    _BASE_QUERY + " ORDER BY updated_at DESC",
                ).fetchall()
        items = [dict(r) for r in rows]
        if admin_only or committee_visible_only:
            return sort_specs_by_subset_order(items)
        return items

    def update_draft_metadata(
        self,
        draft_id: str,
        spec_number: str,
        chemical_name: str,
        committee_name: str,
        meeting_date: str,
        prepared_by: str,
        reviewed_by: str,
        status: str,
        user_name: str,
    ) -> bool:
        """Update draft metadata. Returns True if a row was affected."""
        now = get_utc_now().isoformat()
        old_draft = self.load_draft(draft_id)
        if not old_draft:
            return False

        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE csc_drafts SET
                    spec_number   = ?,
                    chemical_name = ?,
                    committee_name = ?,
                    meeting_date  = ?,
                    prepared_by   = ?,
                    reviewed_by   = ?,
                    status        = ?,
                    updated_at    = ?
                WHERE draft_id = ?
                """,
                (
                    sanitize_text(spec_number, max_length=200),
                    sanitize_text(chemical_name, max_length=300),
                    sanitize_text(committee_name, max_length=300),
                    sanitize_text(meeting_date or "", max_length=50),
                    sanitize_text(normalize_prepared_by(prepared_by), max_length=200),
                    sanitize_text(reviewed_by or "", max_length=200),
                    sanitize_text(status, max_length=50),
                    now,
                    draft_id,
                ),
            )
            conn.commit()

        affected = cur.rowcount > 0
        if affected:
            self.log_audit(
                draft_id, "UPDATE_METADATA", user_name,
                old_value=truncate_for_log(old_draft.get("spec_number")),
                new_value=truncate_for_log(spec_number),
            )
        return affected

    def delete_draft(self, draft_id: str, user_name: str) -> bool:
        """Delete a draft and all child rows (cascade). Returns True if deleted."""
        old = self.load_draft(draft_id)
        if not old:
            return False
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM csc_drafts WHERE draft_id = ?", (draft_id,)
            )
            conn.commit()
        if cur.rowcount > 0:
            self.log_audit(
                None, "DELETE_DRAFT", user_name,
                old_value=truncate_for_log(f"{old['spec_number']} / {old['chemical_name']}"),
            )
            return True
        return False

    def delete_user_revision_draft(self, draft_id: str, user_name: str) -> bool:
        """Delete a committee-owned revision draft.

        Allows committee user cleanup of orphaned or in-progress revisions.
        Returns True when deleted.
        """
        safe_user = sanitize_text(user_name or "", max_length=200)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT draft_id, spec_number, chemical_name, status
                FROM csc_drafts
                WHERE draft_id = ?
                  AND is_admin_draft = 0
                  AND parent_draft_id IS NOT NULL
                  AND created_by_user = ?
                """,
                (draft_id, safe_user),
            ).fetchone()
            if not row:
                return False

            cur = conn.execute(
                "DELETE FROM csc_drafts WHERE draft_id = ?",
                (draft_id,),
            )
            conn.commit()

        if cur.rowcount > 0:
            self.log_audit(
                None,
                "DELETE_REVISION_DRAFT",
                safe_user or "unknown",
                old_value=truncate_for_log(
                    f"{row['spec_number']} / {row['chemical_name']} / {row['status']}"
                ),
                remarks="Deleted by committee user",
            )
            return True
        return False

    def return_revision_to_committee(
        self,
        revision_draft_id: str,
        user_name: str,
        remarks: str = "",
    ) -> bool:
        """Return any revision draft back to committee editing state."""
        now = get_utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE csc_drafts
                SET status = 'Draft',
                    admin_stage = ?,
                    submitted_for_approval_at = NULL,
                    updated_at = ?
                WHERE draft_id = ?
                  AND is_admin_draft = 0
                  AND parent_draft_id IS NOT NULL
                  AND status != 'Approved'
                """,
                (ADMIN_STAGE_STRUCTURING, now, revision_draft_id),
            )
            conn.commit()
        if cur.rowcount > 0:
            self.log_audit(
                revision_draft_id,
                "REVISION_RETURNED_TO_COMMITTEE",
                user_name,
                remarks=sanitize_text(remarks or "Returned to committee for rework", max_length=500),
            )
            return True
        return False

    # ------------------------------------------------------------------
    # Admin-specific operations
    # ------------------------------------------------------------------

    def publish_draft(self, draft_id: str, user_name: str) -> bool:
        """Mark an admin draft as 'Committee Review' and lock Phase 1.

        Committee users loading this draft will skip Phase 1 (verification is
        already done by admin) and go directly to Phase 2 (propose changes).
        """
        now = get_utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE csc_drafts
                SET status = 'Committee Review',
                    phase1_locked = 1,
                    updated_at = ?
                WHERE draft_id = ?
                """,
                (now, draft_id),
            )
            conn.commit()
        if cur.rowcount > 0:
            self.log_audit(draft_id, "ADMIN_PUBLISH", user_name,
                           remarks="Published for committee review; Phase 1 locked")
            return True
        return False

    def unpublish_draft(self, draft_id: str, user_name: str) -> bool:
        """Revert a published draft back to Draft status (admin retains it)."""
        now = get_utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE csc_drafts
                SET status = 'Draft',
                    phase1_locked = 0,
                    updated_at = ?
                WHERE draft_id = ?
                """,
                (now, draft_id),
            )
            conn.commit()
        if cur.rowcount > 0:
            self.log_audit(draft_id, "ADMIN_UNPUBLISH", user_name,
                           remarks="Reverted to Draft; Phase 1 unlocked")
            return True
        return False

    def list_reviewable_specs(self) -> list[dict[str, Any]]:
        """Return published admin master specs available for committee revision."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT draft_id, spec_number, chemical_name, status, admin_stage,
                       spec_version, updated_at, prepared_by, created_by_user
                FROM csc_drafts
                WHERE is_admin_draft = 1
                  AND admin_stage = ?
                ORDER BY updated_at DESC
                """,
                (ADMIN_STAGE_PUBLISHED,),
            ).fetchall()
        return sort_specs_by_subset_order([dict(r) for r in rows])

    def list_user_revision_drafts(self, user_name: str) -> list[dict[str, Any]]:
        """Return revision drafts created by a committee user."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT d.draft_id, d.parent_draft_id, d.spec_number, d.chemical_name,
                       d.status, d.updated_at, d.submitted_for_approval_at, d.spec_version,
                       p.spec_number AS parent_spec_number, p.spec_version AS parent_spec_version
                FROM csc_drafts d
                LEFT JOIN csc_drafts p ON p.draft_id = d.parent_draft_id
                WHERE d.is_admin_draft = 0
                  AND d.parent_draft_id IS NOT NULL
                  AND d.created_by_user = ?
                ORDER BY d.updated_at DESC
                """,
                (sanitize_text(user_name or "", max_length=200),),
            ).fetchall()
        return [dict(r) for r in rows]

    def apply_type_classification_exercise(
        self,
        updates: dict[str, list[dict[str, Any]]],
        user_name: str,
    ) -> dict[str, Any]:
        """Apply committee type-classification workbook updates to master data.

        `updates` is keyed by admin draft_id. Each row in the list should include:
            parameter_id, type, test_procedure_type, test_procedure_text, any_more_changes (bool)
        Returns a summary payload for UI display.
        """
        summary: dict[str, Any] = {
            "processed_specs": 0,
            "updated_specs": [],
            "flagged_specs": [],
            "errors": [],
        }
        safe_user = sanitize_text(user_name or "unknown", max_length=200)
        if not updates:
            return summary

        for draft_id, rows in updates.items():
            draft = self.load_draft(draft_id)
            if not draft:
                summary["errors"].append(f"{draft_id}: specification not found")
                continue
            if not bool(draft.get("is_admin_draft")):
                summary["errors"].append(f"{draft.get('spec_number', draft_id)}: not a master specification")
                continue
            if draft.get("parent_draft_id"):
                summary["errors"].append(f"{draft.get('spec_number', draft_id)}: revision draft cannot be updated")
                continue
            if str(draft.get("admin_stage", "") or "") != ADMIN_STAGE_PUBLISHED:
                summary["errors"].append(f"{draft.get('spec_number', draft_id)}: not in published stage")
                continue

            params = self.load_parameters(draft_id)
            param_by_id = {str(p.get("parameter_id", "")): p for p in params}
            changed_count = 0
            flagged_names: list[str] = []

            now = get_utc_now().isoformat()
            with self._connect() as conn:
                for row in rows:
                    pid = str(row.get("parameter_id", "") or "")
                    if not pid or pid not in param_by_id:
                        continue
                    current = param_by_id[pid]
                    new_type = normalize_parameter_type_label(str(row.get("type", "") or ""))
                    old_type = normalize_parameter_type_label(
                        str(current.get("parameter_type", "Essential") or "Essential")
                    )
                    new_tp_type = normalize_test_procedure_type(
                        str(
                            row.get(
                                "test_procedure_type",
                                current.get("test_procedure_type", ""),
                            ) or ""
                        )
                    )
                    old_tp_type = normalize_test_procedure_type(
                        str(current.get("test_procedure_type", "") or "")
                    )
                    new_tp_text = str(
                        row.get(
                            "test_procedure_text",
                            current.get("test_procedure_text", ""),
                        ) or ""
                    )
                    old_tp_text = str(current.get("test_procedure_text", "") or "")
                    any_more = parse_yes_no_flag(row.get("any_more_changes", False))

                    if any_more:
                        p_name = str(current.get("parameter_name", "") or "").strip()
                        if p_name:
                            flagged_names.append(p_name)

                    if (
                        new_type != old_type
                        or new_tp_type != old_tp_type
                        or new_tp_text != old_tp_text
                    ):
                        conn.execute(
                            """
                            UPDATE csc_parameters
                            SET parameter_type = ?,
                                test_procedure_type = ?,
                                test_procedure_text = ?
                            WHERE parameter_id = ?
                              AND draft_id = ?
                            """,
                            (
                                sanitize_text(new_type, max_length=80),
                                sanitize_text(new_tp_type, max_length=120),
                                sanitize_multiline_text(
                                    new_tp_text, max_length=_PARAM_TEXT_MAX_LENGTH
                                ),
                                pid,
                                draft_id,
                            ),
                        )
                        changed_count += 1

                if changed_count > 0:
                    conn.execute(
                        "UPDATE csc_drafts SET updated_at = ? WHERE draft_id = ?",
                        (now, draft_id),
                    )
                conn.commit()

            spec_label = f"{draft.get('spec_number', '—')} — {draft.get('chemical_name', '—')}"
            summary["processed_specs"] += 1

            if changed_count > 0:
                new_ver = self.increment_version(
                    draft_id,
                    safe_user,
                    reason="Type Classification Exercise update",
                )
                summary["updated_specs"].append(
                    {
                        "draft_id": draft_id,
                        "spec_label": spec_label,
                        "changed_count": int(changed_count),
                        "new_version": int(new_ver or 0),
                    }
                )
                self.log_audit(
                    draft_id,
                    "TYPE_CLASSIFICATION_APPLY",
                    safe_user,
                    new_value=f"{changed_count} parameter type updates",
                    remarks=f"Flagged for more changes: {len(flagged_names)}",
                )

            if flagged_names:
                summary["flagged_specs"].append(
                    {
                        "draft_id": draft_id,
                        "spec_label": spec_label,
                        "flagged_count": int(len(flagged_names)),
                        "flagged_parameters": flagged_names,
                    }
                )
                self.log_audit(
                    draft_id,
                    "TYPE_CLASSIFICATION_FLAGGED",
                    safe_user,
                    remarks="; ".join(flagged_names[:20]),
                )

        return summary

    def create_or_get_revision_draft(
        self,
        parent_draft_id: str,
        user_name: str,
    ) -> tuple[str, bool]:
        """Create (or reuse) a committee revision draft from a published master spec.

        Returns (draft_id, created_new).
        """
        safe_user = sanitize_text(user_name or "", max_length=200)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT draft_id
                FROM csc_drafts
                WHERE parent_draft_id = ?
                  AND created_by_user = ?
                  AND is_admin_draft = 0
                  AND status IN ('Draft', 'Rejected', 'Pending Approval')
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (parent_draft_id, safe_user),
            ).fetchone()
        if row:
            return row["draft_id"], False

        parent = self.load_draft(parent_draft_id)
        if not parent:
            raise ValueError("Parent specification not found.")
        if not bool(parent.get("is_admin_draft")) or parent.get("admin_stage") != ADMIN_STAGE_PUBLISHED:
            raise ValueError("Revision can only be created from a published master specification.")

        new_draft_id = self.create_draft(
            spec_number=parent.get("spec_number", ""),
            chemical_name=parent.get("chemical_name", ""),
            committee_name=parent.get("committee_name", "Chemical Specification Committee"),
            meeting_date=parent.get("meeting_date", ""),
            prepared_by=parent.get("prepared_by", ""),
            reviewed_by=parent.get("reviewed_by", ""),
            user_name=user_name,
            test_procedure=parent.get("test_procedure", ""),
            material_code=parent.get("material_code", ""),
            created_by_role="User",
            created_by_user=safe_user,
            is_admin_draft=False,
            phase1_locked=True,  # committee starts directly from change proposal mode
            admin_stage=ADMIN_STAGE_STRUCTURING,
            parent_draft_id=parent_draft_id,
            spec_version_start=int(parent.get("spec_version", 0) or 0),
        )

        parent_sections = self.load_sections(parent_draft_id)
        parent_params = self.load_parameters(parent_draft_id)
        parent_flags = self.load_issue_flags(parent_draft_id)
        parent_impact = self.load_impact_analysis(parent_draft_id)

        for sec in parent_sections:
            self.upsert_section(
                new_draft_id,
                sec.get("section_name", ""),
                sec.get("section_text", "") or "",
                int(sec.get("sort_order", 0) or 0),
                user_name,
            )
        if parent_params:
            self.upsert_parameters(new_draft_id, parent_params, user_name)
        if parent_flags:
            self.upsert_issue_flags(new_draft_id, parent_flags, user_name)
        if parent_impact:
            self.save_impact_analysis(
                draft_id=new_draft_id,
                operational_impact_score=int(parent_impact.get("operational_impact_score", 1) or 1),
                safety_environment_score=int(parent_impact.get("safety_environment_score", 1) or 1),
                supply_risk_score=int(parent_impact.get("supply_risk_score", 1) or 1),
                no_substitute_flag=int(parent_impact.get("no_substitute_flag", 0) or 0),
                operational_note=str(parent_impact.get("operational_note", "") or ""),
                safety_environment_note=str(parent_impact.get("safety_environment_note", "") or ""),
                supply_risk_note=str(parent_impact.get("supply_risk_note", "") or ""),
                reviewed_by=str(parent_impact.get("reviewed_by", user_name) or user_name),
            )

        self.log_audit(
            new_draft_id,
            "REVISION_DRAFT_CREATED",
            user_name,
            old_value=parent_draft_id,
            remarks=f"Created from published spec {parent.get('spec_number', '')}",
        )
        return new_draft_id, True

    def submit_revision_for_approval(
        self,
        revision_draft_id: str,
        user_name: str,
    ) -> bool:
        """Submit a user revision draft into pending admin approval queue."""
        now = get_utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE csc_drafts
                SET status = 'Pending Approval',
                    admin_stage = ?,
                    submitted_for_approval_at = ?,
                    updated_at = ?
                WHERE draft_id = ?
                  AND is_admin_draft = 0
                  AND parent_draft_id IS NOT NULL
                """,
                (ADMIN_STAGE_PENDING_APPROVAL, now, now, revision_draft_id),
            )
            conn.commit()
        if cur.rowcount > 0:
            self.log_audit(
                revision_draft_id,
                "SUBMIT_FOR_APPROVAL",
                user_name,
                remarks="Submitted committee revision for admin approval",
            )
            return True
        return False

    def reject_revision_request(
        self,
        revision_draft_id: str,
        user_name: str,
        remarks: str = "",
    ) -> bool:
        """Reject a pending revision request and return it to user for rework."""
        revision = self.load_draft(revision_draft_id)
        if not revision:
            return False
        if revision.get("status") != "Pending Approval":
            return False

        now = get_utc_now().isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE csc_drafts
                SET status = 'Rejected',
                    admin_stage = ?,
                    updated_at = ?
                WHERE draft_id = ?
                  AND is_admin_draft = 0
                  AND parent_draft_id IS NOT NULL
                  AND status = 'Pending Approval'
                """,
                (ADMIN_STAGE_STRUCTURING, now, revision_draft_id),
            )
            conn.commit()
        if cur.rowcount > 0:
            self.log_audit(
                revision_draft_id,
                "REVISION_REJECTED",
                user_name,
                remarks=sanitize_text(remarks or "Rejected by admin", max_length=500),
            )
            return True
        return False

    def approve_revision_request(
        self,
        revision_draft_id: str,
        user_name: str,
        remarks: str = "",
    ) -> int:
        """Approve a pending revision and publish it into the parent master spec.

        Returns the new parent spec version, or 0 when approval did not apply.
        """
        revision = self.load_draft(revision_draft_id)
        if not revision:
            return 0
        if revision.get("status") != "Pending Approval":
            return 0
        parent_draft_id = revision.get("parent_draft_id")
        if not parent_draft_id:
            return 0

        parent = self.load_draft(parent_draft_id)
        if not parent:
            return 0

        sections = self.load_sections(revision_draft_id)
        flags = self.load_issue_flags(revision_draft_id)
        source_params = self.load_parameters(revision_draft_id)
        revision_impact = self.load_impact_analysis(revision_draft_id)

        approved_params: list[dict[str, Any]] = []
        for p in source_params:
            proposed = (p.get("proposed_value") or "").strip()
            existing = (p.get("existing_value") or "").strip()
            approved_value = proposed or existing
            approved_params.append({
                "parameter_name": p.get("parameter_name", "") or "",
                "parameter_type": p.get("parameter_type", "Essential") or "Essential",
                "existing_value": approved_value,
                "proposed_value": approved_value,
                "test_method": p.get("test_method", "") or "",
                "test_procedure_type": p.get("test_procedure_type", "") or "",
                "test_procedure_text": p.get("test_procedure_text", "") or "",
                "justification": p.get("justification", "") or "",
                "remarks": p.get("remarks", "") or "",
            })

        now = get_utc_now().isoformat()
        with self._connect() as conn:
            # Update master metadata and publish new version.
            conn.execute(
                """
                UPDATE csc_drafts
                SET spec_number   = ?,
                    chemical_name = ?,
                    committee_name = ?,
                    meeting_date  = ?,
                    prepared_by   = ?,
                    reviewed_by   = ?,
                    status        = 'Master Data',
                    admin_stage   = ?,
                    phase1_locked = 1,
                    spec_version  = spec_version + 1,
                    updated_at    = ?
                WHERE draft_id = ?
                  AND is_admin_draft = 1
                """,
                (
                    sanitize_text(revision.get("spec_number", ""), max_length=200),
                    sanitize_text(revision.get("chemical_name", ""), max_length=300),
                    sanitize_text(revision.get("committee_name", ""), max_length=300),
                    sanitize_text(revision.get("meeting_date", "") or "", max_length=50),
                    sanitize_text(normalize_prepared_by(revision.get("prepared_by", "") or ""), max_length=200),
                    sanitize_text(revision.get("reviewed_by", "") or "", max_length=200),
                    ADMIN_STAGE_PUBLISHED,
                    now,
                    parent_draft_id,
                ),
            )

            # Replace section data on parent with approved revision content.
            conn.execute("DELETE FROM csc_sections WHERE draft_id = ?", (parent_draft_id,))
            for sec in sections:
                conn.execute(
                    """
                    INSERT INTO csc_sections
                        (section_id, draft_id, section_name, section_text, sort_order)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        parent_draft_id,
                        sanitize_text(sec.get("section_name", ""), max_length=200),
                        sanitize_multiline_text(sec.get("section_text", "") or "", max_length=20000),
                        int(sec.get("sort_order", 0) or 0),
                    ),
                )

            # Replace parameter baseline on parent (approved proposed => existing).
            conn.execute("DELETE FROM csc_parameters WHERE draft_id = ?", (parent_draft_id,))
            for idx, p in enumerate(approved_params):
                conn.execute(
                    """
                    INSERT INTO csc_parameters
                        (parameter_id, draft_id, parameter_name, parameter_type,
                         existing_value, proposed_value, test_method,
                         test_procedure_type, test_procedure_text,
                         justification, remarks, sort_order)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        parent_draft_id,
                        sanitize_multiline_text(p.get("parameter_name", ""), max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_text(
                            normalize_parameter_type_label(p.get("parameter_type", "Essential")),
                            max_length=80,
                        ),
                        sanitize_multiline_text(p.get("existing_value", ""), max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_multiline_text(p.get("proposed_value", ""), max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_multiline_text(p.get("test_method", ""), max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_text(
                            normalize_test_procedure_type(p.get("test_procedure_type", "") or ""),
                            max_length=120,
                        ),
                        sanitize_multiline_text(
                            p.get("test_procedure_text", "") or "",
                            max_length=_PARAM_TEXT_MAX_LENGTH,
                        ),
                        sanitize_multiline_text(p.get("justification", ""), max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_multiline_text(p.get("remarks", ""), max_length=_PARAM_TEXT_MAX_LENGTH),
                        idx,
                    ),
                )

            conn.execute("DELETE FROM csc_issue_flags WHERE draft_id = ?", (parent_draft_id,))
            for idx, f in enumerate(flags):
                conn.execute(
                    """
                    INSERT INTO csc_issue_flags
                        (issue_id, draft_id, issue_type, is_present, note, sort_order)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        parent_draft_id,
                        sanitize_text(f.get("issue_type", ""), max_length=100),
                        1 if bool(f.get("is_present", 0)) else 0,
                        sanitize_multiline_text(f.get("note", "") or "", max_length=4000),
                        idx,
                    ),
                )

            # Mark revision as approved/closed.
            conn.execute(
                """
                UPDATE csc_drafts
                SET status = 'Approved',
                    admin_stage = ?,
                    updated_at = ?
                WHERE draft_id = ?
                  AND status = 'Pending Approval'
                """,
                (ADMIN_STAGE_PUBLISHED, now, revision_draft_id),
            )

            row = conn.execute(
                "SELECT spec_version FROM csc_drafts WHERE draft_id = ?",
                (parent_draft_id,),
            ).fetchone()
            conn.commit()

        if revision_impact:
            self.save_impact_analysis(
                draft_id=parent_draft_id,
                operational_impact_score=int(revision_impact.get("operational_impact_score", 1) or 1),
                safety_environment_score=int(revision_impact.get("safety_environment_score", 1) or 1),
                supply_risk_score=int(revision_impact.get("supply_risk_score", 1) or 1),
                no_substitute_flag=int(revision_impact.get("no_substitute_flag", 0) or 0),
                operational_note=str(revision_impact.get("operational_note", "") or ""),
                safety_environment_note=str(revision_impact.get("safety_environment_note", "") or ""),
                supply_risk_note=str(revision_impact.get("supply_risk_note", "") or ""),
                reviewed_by=str(revision_impact.get("reviewed_by", user_name) or user_name),
            )

        new_ver = int(row[0]) if row else 0
        if new_ver:
            self.capture_admin_version_snapshot(
                draft_id=parent_draft_id,
                user_name=user_name,
                source_action="REVISION_APPROVED",
                remarks=f"Approved revision {revision_draft_id}",
                force_version=int(new_ver),
                replace_existing=True,
            )
            self.log_audit(
                parent_draft_id,
                "REVISION_APPROVED",
                user_name,
                new_value=f"v{new_ver}",
                remarks=(
                    f"Approved revision {revision_draft_id} by "
                    f"{revision.get('created_by_user', 'committee user')}. "
                    f"{sanitize_text(remarks or '', max_length=300)}".strip()
                ),
            )
            self.log_audit(
                revision_draft_id,
                "APPROVED_BY_ADMIN",
                user_name,
                new_value=parent_draft_id,
                remarks=sanitize_text(remarks or "Revision approved and published", max_length=500),
            )
        return new_ver

    def get_pending_revision_summary(self) -> list[dict[str, Any]]:
        """Return committee revision queue rows with parent/master context.

        Includes:
        - Pending approval submissions (actionable by admin)
        - In-progress committee revisions not yet submitted
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT r.draft_id, r.parent_draft_id, r.spec_number, r.chemical_name,
                       r.created_by_user, r.status, r.updated_at, r.submitted_for_approval_at,
                       r.spec_version AS revision_base_version,
                       p.spec_version AS parent_spec_version,
                       p.spec_number AS parent_spec_number,
                       CASE WHEN p.draft_id IS NULL THEN 1 ELSE 0 END AS parent_missing,
                       COUNT(cp.parameter_id) AS param_count
                FROM csc_drafts r
                LEFT JOIN csc_drafts p ON p.draft_id = r.parent_draft_id
                LEFT JOIN csc_parameters cp ON cp.draft_id = r.draft_id
                WHERE r.is_admin_draft = 0
                  AND r.parent_draft_id IS NOT NULL
                  AND UPPER(TRIM(COALESCE(r.status, ''))) IN ('DRAFT', 'REJECTED', 'PENDING APPROVAL')
                GROUP BY r.draft_id
                ORDER BY CASE WHEN UPPER(TRIM(COALESCE(r.status, ''))) = 'PENDING APPROVAL' THEN 0 ELSE 1 END,
                         COALESCE(r.submitted_for_approval_at, r.updated_at) DESC
                """
            ).fetchall()
        summaries = [dict(r) for r in rows]
        for row in summaries:
            row["changed_count"] = len(self.get_revision_change_details(row["draft_id"]))
        return summaries

    @staticmethod
    def _param_identity(param: dict[str, Any]) -> tuple[str, str, str]:
        """Stable key for revision-vs-parent comparison even if rows are reordered."""
        return (
            str(param.get("parameter_name", "") or "").strip().casefold(),
            str(param.get("existing_value", "") or "").strip(),
            str(param.get("test_method", "") or "").strip(),
        )

    def get_revision_change_details(self, revision_draft_id: str) -> list[dict[str, Any]]:
        """Return changed rows for a revision, including value and type changes."""
        revision = self.load_draft(revision_draft_id)
        if not revision:
            return []
        parent_draft_id = revision.get("parent_draft_id")
        if not parent_draft_id:
            return []

        parent_params = self.load_parameters(parent_draft_id)
        revision_params = self.load_parameters(revision_draft_id)

        parent_type_lookup: dict[tuple[str, str, str], list[str]] = {}
        parent_tp_type_lookup: dict[tuple[str, str, str], list[str]] = {}
        parent_tp_text_lookup: dict[tuple[str, str, str], list[str]] = {}
        for p in parent_params:
            key = self._param_identity(p)
            parent_type_lookup.setdefault(key, []).append(
                str(p.get("parameter_type", "") or "").strip()
            )
            parent_tp_type_lookup.setdefault(key, []).append(
                str(p.get("test_procedure_type", "") or "").strip()
            )
            parent_tp_text_lookup.setdefault(key, []).append(
                str(p.get("test_procedure_text", "") or "").strip()
            )

        changed: list[dict[str, Any]] = []
        for p in revision_params:
            existing = str(p.get("existing_value", "") or "").strip()
            proposed = str(p.get("proposed_value", "") or "").strip()
            approved = proposed or existing
            current_type = str(p.get("parameter_type", "") or "").strip()
            current_tp_type = str(p.get("test_procedure_type", "") or "").strip()
            current_tp_text = str(p.get("test_procedure_text", "") or "").strip()

            key = self._param_identity(p)
            parent_types = parent_type_lookup.get(key, [])
            parent_tp_types = parent_tp_type_lookup.get(key, [])
            parent_tp_texts = parent_tp_text_lookup.get(key, [])
            row_has_name = bool(str(p.get("parameter_name", "") or "").strip())
            parent_type = parent_types.pop(0) if parent_types else current_type
            parent_tp_type = parent_tp_types.pop(0) if parent_tp_types else current_tp_type
            parent_tp_text = parent_tp_texts.pop(0) if parent_tp_texts else current_tp_text

            value_changed = approved != existing
            type_changed = current_type != parent_type
            proc_type_changed = current_tp_type != parent_tp_type
            proc_text_changed = current_tp_text != parent_tp_text
            if row_has_name and not parent_types and key not in parent_type_lookup:
                # New row or identity fields changed vs parent baseline.
                value_changed = True
            if value_changed or type_changed or proc_type_changed or proc_text_changed:
                changed.append(
                    {
                        "parameter_name": str(p.get("parameter_name", "") or ""),
                        "existing_value": existing,
                        "proposed_value": approved,
                        "current_type": current_type,
                        "parent_type": parent_type,
                        "test_method": str(p.get("test_method", "") or ""),
                        "test_procedure_type": current_tp_type,
                        "parent_test_procedure_type": parent_tp_type,
                        "test_procedure_text": current_tp_text,
                        "parent_test_procedure_text": parent_tp_text,
                        "justification": str(p.get("justification", "") or ""),
                        "remarks": str(p.get("remarks", "") or ""),
                        "value_changed": value_changed,
                        "type_changed": type_changed,
                        "test_procedure_type_changed": proc_type_changed,
                        "test_procedure_text_changed": proc_text_changed,
                    }
                )
        return changed

    def bulk_create_admin_drafts(
        self,
        specs: list[dict[str, Any]],
        user_name: str,
    ) -> list[str]:
        """Create multiple admin drafts from a list of spec dicts.

        Each spec dict must have keys matching create_draft() arguments
        plus an optional 'parameters' list of parameter dicts.
        Returns list of created draft_ids.
        """
        created_ids: list[str] = []
        for spec in specs:
            draft_id = self.create_draft(
                spec_number    = spec.get("spec_number", ""),
                chemical_name  = spec.get("chemical_name", ""),
                committee_name = spec.get("committee_name", "Chemical Specification Committee"),
                meeting_date   = spec.get("meeting_date", ""),
                prepared_by    = spec.get("prepared_by", ""),
                reviewed_by    = spec.get("reviewed_by", ""),
                user_name      = user_name,
                test_procedure = spec.get("test_procedure", ""),
                material_code  = spec.get("material_code", ""),
                created_by_role= "Admin",
                is_admin_draft = True,
                phase1_locked  = False,
            )
            params = spec.get("parameters", [])
            if params:
                self.upsert_parameters(draft_id, params, user_name)
            created_ids.append(draft_id)
        self.log_audit(
            None, "ADMIN_BULK_CREATE", user_name,
            new_value=f"Created {len(created_ids)} admin drafts",
        )
        return created_ids

    def get_admin_draft_summary(self) -> list[dict[str, Any]]:
        """Return full summary of all admin drafts with parameter counts, stage and version."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT d.draft_id, d.spec_number, d.chemical_name,
                       d.status, d.prepared_by, d.updated_at,
                       d.admin_stage, d.spec_version,
                       COUNT(p.parameter_id) AS param_count
                FROM csc_drafts d
                LEFT JOIN csc_parameters p ON p.draft_id = d.draft_id
                WHERE d.is_admin_draft = 1
                  AND d.parent_draft_id IS NULL
                GROUP BY d.draft_id
                ORDER BY d.updated_at DESC
                """
            ).fetchall()
        return sort_specs_by_subset_order([dict(r) for r in rows])

    def set_admin_stage(
        self, draft_id: str, stage: str, user_name: str
    ) -> bool:
        """Advance or retreat a draft's admin_stage.

        Stages: 'ingested' → 'structuring' → 'published'
        Publishing increments spec_version (v0 → v1 → v2 …).  Saving parameters
        during Structuring no longer changes the version; the version only moves
        forward at the moment the spec is published to master data.
        """
        now = get_utc_now().isoformat()
        # Only two meaningful status values for admin master data
        new_status = "Master Data" if stage == ADMIN_STAGE_PUBLISHED else "Draft"
        with self._connect() as conn:
            if stage == ADMIN_STAGE_PUBLISHED:
                # Increment version atomically with the stage change.
                conn.execute(
                    """
                    UPDATE csc_drafts
                    SET admin_stage  = ?,
                        status       = ?,
                        spec_version = spec_version + 1,
                        updated_at   = ?
                    WHERE draft_id = ?
                    """,
                    (stage, new_status, now, draft_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE csc_drafts
                    SET admin_stage = ?,
                        status      = ?,
                        updated_at  = ?
                    WHERE draft_id = ?
                    """,
                    (stage, new_status, now, draft_id),
                )
            cur = conn.execute("SELECT changes()").fetchone()
            new_ver_row = conn.execute(
                "SELECT spec_version FROM csc_drafts WHERE draft_id = ?",
                (draft_id,),
            ).fetchone()
            conn.commit()

        rows_changed = cur[0] if cur else 0
        if rows_changed > 0:
            new_ver = int(new_ver_row[0]) if new_ver_row else 0
            self.log_audit(
                draft_id,
                f"ADMIN_STAGE_{stage.upper()}",
                user_name,
                new_value=f"v{new_ver}" if stage == ADMIN_STAGE_PUBLISHED else None,
                remarks=f"Stage → {stage}",
            )
            if stage == ADMIN_STAGE_PUBLISHED:
                self.capture_admin_version_snapshot(
                    draft_id=draft_id,
                    user_name=user_name,
                    source_action="ADMIN_STAGE_PUBLISHED",
                    remarks=f"Published to master data (v{new_ver})",
                    force_version=new_ver,
                    replace_existing=True,
                )
            return True
        return False

    def increment_version(self, draft_id: str, user_name: str, reason: str = "") -> int:
        """Increment the spec_version of an admin draft and return the new version number.

        Called only on publish-level events (e.g. approving a committee revision).
        Routine parameter saves during Structuring do NOT call this — the version
        advances only when the spec is published via set_admin_stage or when a
        committee revision is approved.
        Returns the new version number, or 0 if draft not found.
        """
        now = get_utc_now().isoformat()
        with self._connect() as conn:
            meta = conn.execute(
                """
                SELECT spec_version, is_admin_draft, parent_draft_id
                FROM csc_drafts
                WHERE draft_id = ?
                """,
                (draft_id,),
            ).fetchone()
            if not meta:
                return 0

            cur_ver = int(meta["spec_version"] or 0)
            is_admin_master = bool(meta["is_admin_draft"]) and not meta["parent_draft_id"]

            legacy_first_save_fix = False
            if is_admin_master and cur_ver == 1:
                has_v0 = conn.execute(
                    """
                    SELECT 1
                    FROM csc_spec_versions
                    WHERE draft_id = ? AND spec_version = 0
                    LIMIT 1
                    """,
                    (draft_id,),
                ).fetchone()
                has_increment = conn.execute(
                    """
                    SELECT 1
                    FROM csc_audit
                    WHERE draft_id = ? AND action = 'VERSION_INCREMENT'
                    LIMIT 1
                    """,
                    (draft_id,),
                ).fetchone()
                if not has_v0 and not has_increment:
                    legacy_first_save_fix = True

            if legacy_first_save_fix:
                conn.execute(
                    """
                    UPDATE csc_drafts
                    SET updated_at = ?
                    WHERE draft_id = ? AND is_admin_draft = 1
                    """,
                    (now, draft_id),
                )
                new_ver = 1
            else:
                conn.execute(
                    """
                    UPDATE csc_drafts
                    SET spec_version = spec_version + 1,
                        updated_at   = ?
                    WHERE draft_id = ? AND is_admin_draft = 1
                    """,
                    (now, draft_id),
                )
                row = conn.execute(
                    "SELECT spec_version FROM csc_drafts WHERE draft_id = ?",
                    (draft_id,),
                ).fetchone()
                new_ver = int(row[0]) if row else 0
            conn.commit()

        if new_ver:
            if legacy_first_save_fix:
                self.capture_admin_version_snapshot(
                    draft_id=draft_id,
                    user_name=user_name,
                    source_action="BACKFILL_V0",
                    remarks="Auto-created v0 baseline for legacy draft",
                    force_version=0,
                    replace_existing=False,
                )
            self.log_audit(
                draft_id, "VERSION_INCREMENT", user_name,
                new_value=f"v{new_ver}",
                remarks=reason or "Parameter data updated",
            )
            self.capture_admin_version_snapshot(
                draft_id=draft_id,
                user_name=user_name,
                source_action="VERSION_INCREMENT",
                remarks=reason or "Parameter data updated",
                force_version=int(new_ver),
                replace_existing=True,
            )
        return new_ver

    def get_version_changelog(self) -> list[dict[str, Any]]:
        """Return a changelog of all admin draft version increments from the audit log.

        Returns rows ordered by action_time DESC, each row containing:
            spec_number, chemical_name, spec_version, action_time, user_name, remarks
        Used by the master export change-log page.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT a.draft_id, d.spec_number, d.chemical_name,
                       d.spec_version, a.new_value AS version_label,
                       a.action_time, a.user_name, a.remarks
                FROM csc_audit a
                JOIN csc_drafts d ON d.draft_id = a.draft_id
                WHERE a.action IN ('VERSION_INCREMENT', 'ADMIN_STAGE_PUBLISHED',
                                   'REVISION_APPROVED',
                                   'RESTORE_VERSION',
                                   'ADMIN_PDF_IMPORT', 'CREATE_DRAFT')
                  AND d.is_admin_draft = 1
                ORDER BY a.action_time DESC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Admin version snapshots / compare / restore
    # ------------------------------------------------------------------

    def _build_admin_snapshot_payload(self, draft_id: str) -> dict[str, Any] | None:
        """Build a JSON-serializable payload for one admin master draft."""
        draft = self.load_draft(draft_id)
        if not draft:
            return None
        if not bool(draft.get("is_admin_draft")) or draft.get("parent_draft_id"):
            return None

        payload = {
            "draft": {
                "spec_number": str(draft.get("spec_number", "") or ""),
                "chemical_name": str(draft.get("chemical_name", "") or ""),
                "committee_name": str(draft.get("committee_name", "") or ""),
                "meeting_date": str(draft.get("meeting_date", "") or ""),
                "prepared_by": str(draft.get("prepared_by", "") or ""),
                "reviewed_by": str(draft.get("reviewed_by", "") or ""),
                "test_procedure": str(draft.get("test_procedure", "") or ""),
                "material_code": str(draft.get("material_code", "") or ""),
            },
            "sections": [
                {
                    "section_name": str(s.get("section_name", "") or ""),
                    "section_text": str(s.get("section_text", "") or ""),
                    "sort_order": int(s.get("sort_order", 0) or 0),
                }
                for s in self.load_sections(draft_id)
            ],
            "parameters": [
                {
                    "parameter_name": str(p.get("parameter_name", "") or ""),
                    "parameter_type": str(p.get("parameter_type", "Essential") or "Essential"),
                    "existing_value": str(p.get("existing_value", "") or ""),
                    "proposed_value": str(p.get("proposed_value", "") or ""),
                    "test_method": str(p.get("test_method", "") or ""),
                    "test_procedure_type": str(p.get("test_procedure_type", "") or ""),
                    "test_procedure_text": str(p.get("test_procedure_text", "") or ""),
                    "justification": str(p.get("justification", "") or ""),
                    "remarks": str(p.get("remarks", "") or ""),
                    "sort_order": int(p.get("sort_order", 0) or 0),
                }
                for p in self.load_parameters(draft_id)
            ],
            "issue_flags": [
                {
                    "issue_type": str(f.get("issue_type", "") or ""),
                    "is_present": 1 if bool(f.get("is_present", 0)) else 0,
                    "note": str(f.get("note", "") or ""),
                    "sort_order": int(f.get("sort_order", 0) or 0),
                }
                for f in self.load_issue_flags(draft_id)
            ],
            "impact_analysis": None,
        }
        impact = self.load_impact_analysis(draft_id)
        if impact:
            payload["impact_analysis"] = {
                "operational_impact_score": int(impact.get("operational_impact_score", 1) or 1),
                "safety_environment_score": int(impact.get("safety_environment_score", 1) or 1),
                "supply_risk_score": int(impact.get("supply_risk_score", 1) or 1),
                "no_substitute_flag": 1 if int(impact.get("no_substitute_flag", 0) or 0) == 1 else 0,
                "impact_score_total": float(impact.get("impact_score_total", 0.0) or 0.0),
                "impact_grade": str(impact.get("impact_grade", "") or ""),
                "operational_note": str(impact.get("operational_note", "") or ""),
                "safety_environment_note": str(impact.get("safety_environment_note", "") or ""),
                "supply_risk_note": str(impact.get("supply_risk_note", "") or ""),
                "reviewed_by": str(impact.get("reviewed_by", "") or ""),
                "review_date": str(impact.get("review_date", "") or ""),
            }
        return payload

    def capture_admin_version_snapshot(
        self,
        draft_id: str,
        user_name: str,
        source_action: str = "",
        remarks: str = "",
        force_version: int | None = None,
        replace_existing: bool = True,
    ) -> int:
        """Persist a full snapshot for one admin master version."""
        draft = self.load_draft(draft_id)
        if not draft:
            return 0
        if not bool(draft.get("is_admin_draft")) or draft.get("parent_draft_id"):
            return 0

        snapshot_version = int(
            force_version if force_version is not None else draft.get("spec_version", 0) or 0
        )
        snapshot_version = max(0, snapshot_version)
        payload = self._build_admin_snapshot_payload(draft_id)
        if payload is None:
            return 0

        now = get_utc_now().isoformat()
        safe_user = sanitize_text(user_name or "system", max_length=200)
        safe_action = sanitize_text(source_action or "", max_length=100)
        safe_remarks = sanitize_multiline_text(remarks or "", max_length=2000) or None
        payload_json = json.dumps(payload, ensure_ascii=True, sort_keys=True)

        with self._connect() as conn:
            if replace_existing:
                conn.execute(
                    """
                    INSERT INTO csc_spec_versions
                        (snapshot_id, draft_id, spec_version, created_at,
                         created_by, source_action, remarks, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(draft_id, spec_version)
                    DO UPDATE SET
                        created_at = excluded.created_at,
                        created_by = excluded.created_by,
                        source_action = excluded.source_action,
                        remarks = excluded.remarks,
                        payload_json = excluded.payload_json
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        snapshot_version,
                        now,
                        safe_user,
                        safe_action,
                        safe_remarks,
                        payload_json,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO csc_spec_versions
                        (snapshot_id, draft_id, spec_version, created_at,
                         created_by, source_action, remarks, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(draft_id, spec_version) DO NOTHING
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        snapshot_version,
                        now,
                        safe_user,
                        safe_action,
                        safe_remarks,
                        payload_json,
                    ),
                )
            conn.commit()
        return snapshot_version

    def _load_admin_snapshot_payload(
        self,
        draft_id: str,
        spec_version: int,
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json
                FROM csc_spec_versions
                WHERE draft_id = ? AND spec_version = ?
                """,
                (draft_id, int(spec_version)),
            ).fetchone()
        if not row:
            return None
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def get_spec_version_history(self, draft_id: str) -> list[dict[str, Any]]:
        """List all captured versions for one admin master spec in ascending order."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT spec_version, created_at, created_by, source_action, remarks, payload_json
                FROM csc_spec_versions
                WHERE draft_id = ?
                ORDER BY spec_version ASC
                """,
                (draft_id,),
            ).fetchall()
        if not rows:
            # Best-effort seed for old data created before snapshot support.
            self.capture_admin_version_snapshot(
                draft_id=draft_id,
                user_name="system",
                source_action="AUTO_BACKFILL",
                remarks="Backfilled from current draft state",
                replace_existing=False,
            )
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT spec_version, created_at, created_by, source_action, remarks, payload_json
                    FROM csc_spec_versions
                    WHERE draft_id = ?
                    ORDER BY spec_version ASC
                    """,
                    (draft_id,),
                ).fetchall()

        history: list[dict[str, Any]] = []
        for row in rows:
            param_count = 0
            try:
                payload = json.loads(str(row["payload_json"] or "{}"))
                if isinstance(payload, dict):
                    param_count = len(payload.get("parameters", []) or [])
            except Exception:
                param_count = 0
            history.append(
                {
                    "spec_version": int(row["spec_version"] or 0),
                    "created_at": str(row["created_at"] or ""),
                    "created_by": str(row["created_by"] or ""),
                    "source_action": str(row["source_action"] or ""),
                    "remarks": str(row["remarks"] or ""),
                    "parameter_count": param_count,
                }
            )
        return history

    def compare_spec_versions(
        self,
        draft_id: str,
        from_version: int,
        to_version: int,
    ) -> dict[str, Any] | None:
        """Compare two saved versions for an admin master spec."""
        from_payload = self._load_admin_snapshot_payload(draft_id, from_version)
        to_payload = self._load_admin_snapshot_payload(draft_id, to_version)
        if not from_payload or not to_payload:
            return None

        field_labels = {
            "spec_number": "Spec Number",
            "chemical_name": "Chemical Name",
            "committee_name": "Committee Name",
            "meeting_date": "Meeting Date",
            "prepared_by": "Prepared By",
            "reviewed_by": "Reviewed By",
            "test_procedure": "Test Procedure",
            "material_code": "Material Code",
        }
        from_draft = from_payload.get("draft", {}) or {}
        to_draft = to_payload.get("draft", {}) or {}
        metadata_changes: list[dict[str, str]] = []
        for key, label in field_labels.items():
            old_val = str(from_draft.get(key, "") or "")
            new_val = str(to_draft.get(key, "") or "")
            if old_val != new_val:
                metadata_changes.append({"field": label, "from": old_val, "to": new_val})

        tracked_param_fields = [
            ("parameter_type", "Type"),
            ("existing_value", "Requirement"),
            ("proposed_value", "Proposed Value"),
            ("test_method", "Test Method"),
            ("test_procedure_type", "Test Procedure Type"),
            ("test_procedure_text", "Test Procedure Text"),
            ("justification", "Justification"),
            ("remarks", "Remarks"),
            ("sort_order", "Sort Order"),
        ]

        def _param_key(row: dict[str, Any]) -> tuple[str, str]:
            return (
                str(row.get("parameter_name", "") or "").strip().lower(),
                str(row.get("test_method", "") or "").strip().lower(),
            )

        def _canon_param(row: dict[str, Any]) -> dict[str, Any]:
            return {
                "parameter_name": str(row.get("parameter_name", "") or ""),
                "parameter_type": str(row.get("parameter_type", "") or ""),
                "existing_value": str(row.get("existing_value", "") or ""),
                "proposed_value": str(row.get("proposed_value", "") or ""),
                "test_method": str(row.get("test_method", "") or ""),
                "test_procedure_type": str(row.get("test_procedure_type", "") or ""),
                "test_procedure_text": str(row.get("test_procedure_text", "") or ""),
                "justification": str(row.get("justification", "") or ""),
                "remarks": str(row.get("remarks", "") or ""),
                "sort_order": int(row.get("sort_order", 0) or 0),
            }

        from_params_raw = [_canon_param(p) for p in (from_payload.get("parameters", []) or [])]
        to_params_raw = [_canon_param(p) for p in (to_payload.get("parameters", []) or [])]
        from_params = sorted(from_params_raw, key=lambda p: (p["sort_order"], p["parameter_name"], p["test_method"]))
        to_params = sorted(to_params_raw, key=lambda p: (p["sort_order"], p["parameter_name"], p["test_method"]))

        from_map: dict[tuple[str, str], list[dict[str, Any]]] = {}
        to_map: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in from_params:
            from_map.setdefault(_param_key(row), []).append(row)
        for row in to_params:
            to_map.setdefault(_param_key(row), []).append(row)

        added: list[dict[str, Any]] = []
        removed: list[dict[str, Any]] = []
        changed: list[dict[str, Any]] = []

        all_keys = sorted(set(from_map.keys()) | set(to_map.keys()))
        for key in all_keys:
            from_rows = from_map.get(key, [])
            to_rows = to_map.get(key, [])
            common_len = min(len(from_rows), len(to_rows))
            for idx in range(common_len):
                old_row = from_rows[idx]
                new_row = to_rows[idx]
                field_changes: list[dict[str, Any]] = []
                for fld, label in tracked_param_fields:
                    old_val = old_row.get(fld)
                    new_val = new_row.get(fld)
                    if old_val != new_val:
                        field_changes.append(
                            {
                                "field": label,
                                "from": str(old_val if old_val is not None else ""),
                                "to": str(new_val if new_val is not None else ""),
                            }
                        )
                if field_changes:
                    changed.append(
                        {
                            "parameter_name": old_row.get("parameter_name", ""),
                            "test_method": old_row.get("test_method", ""),
                            "from": old_row,
                            "to": new_row,
                            "field_changes": field_changes,
                        }
                    )
            if len(to_rows) > common_len:
                added.extend(to_rows[common_len:])
            if len(from_rows) > common_len:
                removed.extend(from_rows[common_len:])

        unchanged_count = max(
            0,
            min(len(from_params), len(to_params)) - len(changed),
        )

        return {
            "from_version": int(from_version),
            "to_version": int(to_version),
            "metadata_changes": metadata_changes,
            "parameter_changes": {
                "added": added,
                "removed": removed,
                "changed": changed,
            },
            "summary": {
                "from_param_count": len(from_params),
                "to_param_count": len(to_params),
                "added": len(added),
                "removed": len(removed),
                "changed": len(changed),
                "unchanged": unchanged_count,
                "metadata_changed": len(metadata_changes),
            },
        }

    def restore_spec_to_version(
        self,
        draft_id: str,
        target_version: int,
        user_name: str,
        remarks: str = "",
    ) -> int:
        """Restore admin master spec content from a prior snapshot and create a new version."""
        draft = self.load_draft(draft_id)
        if not draft:
            return 0
        if not bool(draft.get("is_admin_draft")) or draft.get("parent_draft_id"):
            return 0

        payload = self._load_admin_snapshot_payload(draft_id, target_version)
        if not payload:
            return 0

        meta = payload.get("draft", {}) or {}
        sections = payload.get("sections", []) or []
        params = payload.get("parameters", []) or []
        flags = payload.get("issue_flags", []) or []
        impact = payload.get("impact_analysis")

        now = get_utc_now().isoformat()
        safe_remarks = sanitize_multiline_text(remarks or "", max_length=500)
        with self._connect() as conn:
            cur_row = conn.execute(
                "SELECT spec_version FROM csc_drafts WHERE draft_id = ?",
                (draft_id,),
            ).fetchone()
            if not cur_row:
                return 0
            new_version = int(cur_row["spec_version"] or 0) + 1

            conn.execute(
                """
                UPDATE csc_drafts
                SET spec_number   = ?,
                    chemical_name = ?,
                    committee_name = ?,
                    meeting_date  = ?,
                    prepared_by   = ?,
                    reviewed_by   = ?,
                    spec_version  = ?,
                    updated_at    = ?
                WHERE draft_id = ?
                  AND is_admin_draft = 1
                  AND parent_draft_id IS NULL
                """,
                (
                    sanitize_text(meta.get("spec_number", "") or "", max_length=200),
                    sanitize_text(meta.get("chemical_name", "") or "", max_length=300),
                    sanitize_text(meta.get("committee_name", "") or "", max_length=300),
                    sanitize_text(meta.get("meeting_date", "") or "", max_length=50),
                    sanitize_text(normalize_prepared_by(meta.get("prepared_by", "") or ""), max_length=200),
                    sanitize_text(meta.get("reviewed_by", "") or "", max_length=200),
                    int(new_version),
                    now,
                    draft_id,
                ),
            )

            conn.execute("DELETE FROM csc_sections WHERE draft_id = ?", (draft_id,))
            for sec in sections:
                conn.execute(
                    """
                    INSERT INTO csc_sections
                        (section_id, draft_id, section_name, section_text, sort_order)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        sanitize_text(sec.get("section_name", "") or "", max_length=200),
                        sanitize_multiline_text(sec.get("section_text", "") or "", max_length=20000),
                        int(sec.get("sort_order", 0) or 0),
                    ),
                )

            conn.execute("DELETE FROM csc_parameters WHERE draft_id = ?", (draft_id,))
            for idx, p in enumerate(params):
                conn.execute(
                    """
                    INSERT INTO csc_parameters
                        (parameter_id, draft_id, parameter_name, parameter_type,
                         existing_value, proposed_value, test_method,
                         test_procedure_type, test_procedure_text,
                         justification, remarks, sort_order)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        sanitize_multiline_text(p.get("parameter_name", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_text(
                            normalize_parameter_type_label(p.get("parameter_type", "Essential") or "Essential"),
                            max_length=80,
                        ),
                        sanitize_multiline_text(p.get("existing_value", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_multiline_text(p.get("proposed_value", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_multiline_text(p.get("test_method", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_text(
                            normalize_test_procedure_type(p.get("test_procedure_type", "") or ""),
                            max_length=120,
                        ),
                        sanitize_multiline_text(
                            p.get("test_procedure_text", "") or "",
                            max_length=_PARAM_TEXT_MAX_LENGTH,
                        ),
                        sanitize_multiline_text(p.get("justification", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH),
                        sanitize_multiline_text(p.get("remarks", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH),
                        int(p.get("sort_order", idx) or idx),
                    ),
                )

            conn.execute("DELETE FROM csc_issue_flags WHERE draft_id = ?", (draft_id,))
            for idx, f in enumerate(flags):
                conn.execute(
                    """
                    INSERT INTO csc_issue_flags
                        (issue_id, draft_id, issue_type, is_present, note, sort_order)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        sanitize_text(f.get("issue_type", "") or "", max_length=50),
                        1 if int(f.get("is_present", 0) or 0) == 1 else 0,
                        sanitize_multiline_text(f.get("note", "") or "", max_length=3000),
                        int(f.get("sort_order", idx) or idx),
                    ),
                )

            conn.execute("DELETE FROM impact_analysis WHERE draft_id = ?", (draft_id,))
            if isinstance(impact, dict) and impact:
                op = int(impact.get("operational_impact_score", 1) or 1)
                se = int(impact.get("safety_environment_score", 1) or 1)
                sr = int(impact.get("supply_risk_score", 1) or 1)
                no_sub = 1 if int(impact.get("no_substitute_flag", 0) or 0) == 1 else 0
                total = round(calculate_impact_score(op, se, sr), 2)
                final_grade = get_impact_grade(total, no_sub)
                conn.execute(
                    """
                    INSERT INTO impact_analysis (
                        impact_id, draft_id,
                        operational_impact_score, safety_environment_score, supply_risk_score,
                        no_substitute_flag, impact_score_total, impact_grade,
                        operational_note, safety_environment_note, supply_risk_note,
                        reviewed_by, review_date, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        op,
                        se,
                        sr,
                        no_sub,
                        float(total),
                        sanitize_text(final_grade, max_length=30),
                        sanitize_multiline_text(impact.get("operational_note", "") or "", max_length=5000),
                        sanitize_multiline_text(impact.get("safety_environment_note", "") or "", max_length=5000),
                        sanitize_multiline_text(impact.get("supply_risk_note", "") or "", max_length=5000),
                        sanitize_text(impact.get("reviewed_by", "") or "", max_length=200),
                        sanitize_text(impact.get("review_date", "") or get_utc_now().date().isoformat(), max_length=20),
                        now,
                        now,
                    ),
                )

            conn.commit()

        new_ver = self.capture_admin_version_snapshot(
            draft_id=draft_id,
            user_name=user_name,
            source_action="RESTORE_VERSION",
            remarks=f"Restored from v{int(target_version)}" + (f" | {safe_remarks}" if safe_remarks else ""),
            force_version=new_version,
            replace_existing=True,
        )
        if new_ver:
            self.log_audit(
                draft_id,
                "RESTORE_VERSION",
                user_name,
                old_value=f"v{int(target_version)}",
                new_value=f"v{int(new_ver)}",
                remarks=safe_remarks or "Content restored from historical version",
            )
        return int(new_ver or 0)

    # ------------------------------------------------------------------
    # Section operations
    # ------------------------------------------------------------------

    def upsert_section(
        self,
        draft_id: str,
        section_name: str,
        section_text: str,
        sort_order: int,
        user_name: str,
    ) -> None:
        """Insert or replace a section row (UNIQUE on draft_id + section_name)."""
        old_row = self._load_single_section(draft_id, section_name)
        old_text = old_row.get("section_text", "") if old_row else ""

        now = get_utc_now().isoformat()
        safe_text = sanitize_multiline_text(section_text or "", max_length=20000)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO csc_sections
                    (section_id, draft_id, section_name, section_text, sort_order)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(draft_id, section_name)
                DO UPDATE SET
                    section_text = excluded.section_text,
                    sort_order   = excluded.sort_order
                """,
                (new_uuid(), draft_id, section_name, safe_text, sort_order),
            )
            conn.execute(
                "UPDATE csc_drafts SET updated_at = ? WHERE draft_id = ?",
                (now, draft_id),
            )
            conn.commit()

        self.log_audit(
            draft_id, "UPDATE_SECTION", user_name,
            old_value=truncate_for_log(old_text),
            new_value=truncate_for_log(safe_text),
            remarks=section_name,
        )

    def load_sections(self, draft_id: str) -> list[dict[str, Any]]:
        """Return all section rows for a draft, ordered by sort_order."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM csc_sections
                WHERE draft_id = ?
                ORDER BY sort_order ASC
                """,
                (draft_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def _load_single_section(
        self, draft_id: str, section_name: str
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM csc_sections
                WHERE draft_id = ? AND section_name = ?
                """,
                (draft_id, section_name),
            ).fetchone()
        return dict(row) if row else None

    def get_section_text(
        self, draft_id: str, section_name: str
    ) -> str:
        """Return section_text for a specific section, or empty string."""
        row = self._load_single_section(draft_id, section_name)
        return row.get("section_text", "") if row else ""

    # ------------------------------------------------------------------
    # Impact analysis operations
    # ------------------------------------------------------------------

    def load_impact_analysis(self, draft_id: str) -> dict[str, Any] | None:
        """Return structured impact analysis for a draft, or None if missing."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM impact_analysis
                WHERE draft_id = ?
                """,
                (draft_id,),
            ).fetchone()
        if not row:
            return None

        data = dict(row)
        score_total = float(data.get("impact_score_total", 0.0) or 0.0)
        no_substitute = int(data.get("no_substitute_flag", 0) or 0)
        computed_grade = get_impact_grade(score_total, 0)
        final_grade = str(data.get("impact_grade", "") or "").upper()
        override_applied = bool(
            no_substitute == 1
            and computed_grade in {"LOW", "MODERATE"}
            and final_grade == "HIGH"
        )
        data["computed_grade"] = computed_grade
        data["override_applied"] = override_applied
        return data

    def save_impact_analysis(
        self,
        draft_id: str,
        operational_impact_score: int,
        safety_environment_score: int,
        supply_risk_score: int,
        no_substitute_flag: int,
        operational_note: str = "",
        safety_environment_note: str = "",
        supply_risk_note: str = "",
        reviewed_by: str = "",
    ) -> dict[str, Any]:
        """Insert or update one active impact-analysis record for a draft."""
        op = int(operational_impact_score)
        se = int(safety_environment_score)
        sr = int(supply_risk_score)
        no_sub = 1 if int(no_substitute_flag or 0) == 1 else 0
        if op not in {1, 2, 3, 4, 5}:
            raise ValueError("Operational Impact score must be between 1 and 5.")
        if se not in {1, 2, 3, 4, 5}:
            raise ValueError("Safety / Environmental score must be between 1 and 5.")
        if sr not in {1, 2, 3, 4, 5}:
            raise ValueError("Supply Risk score must be between 1 and 5.")

        total = round(calculate_impact_score(op, se, sr), 2)
        final_grade = get_impact_grade(total, no_sub)

        now_iso = get_utc_now().isoformat()
        review_date = get_utc_now().date().isoformat()
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT impact_id, created_at
                FROM impact_analysis
                WHERE draft_id = ?
                """,
                (draft_id,),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE impact_analysis
                    SET operational_impact_score = ?,
                        safety_environment_score = ?,
                        supply_risk_score = ?,
                        no_substitute_flag = ?,
                        impact_score_total = ?,
                        impact_grade = ?,
                        operational_note = ?,
                        safety_environment_note = ?,
                        supply_risk_note = ?,
                        reviewed_by = ?,
                        review_date = ?,
                        updated_at = ?
                    WHERE draft_id = ?
                    """,
                    (
                        op,
                        se,
                        sr,
                        no_sub,
                        float(total),
                        sanitize_text(final_grade, max_length=30),
                        sanitize_multiline_text(operational_note or "", max_length=5000),
                        sanitize_multiline_text(safety_environment_note or "", max_length=5000),
                        sanitize_multiline_text(supply_risk_note or "", max_length=5000),
                        sanitize_text(reviewed_by or "", max_length=200),
                        review_date,
                        now_iso,
                        draft_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO impact_analysis (
                        impact_id, draft_id,
                        operational_impact_score, safety_environment_score, supply_risk_score,
                        no_substitute_flag, impact_score_total, impact_grade,
                        operational_note, safety_environment_note, supply_risk_note,
                        reviewed_by, review_date, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        op,
                        se,
                        sr,
                        no_sub,
                        float(total),
                        sanitize_text(final_grade, max_length=30),
                        sanitize_multiline_text(operational_note or "", max_length=5000),
                        sanitize_multiline_text(safety_environment_note or "", max_length=5000),
                        sanitize_multiline_text(supply_risk_note or "", max_length=5000),
                        sanitize_text(reviewed_by or "", max_length=200),
                        review_date,
                        now_iso,
                        now_iso,
                    ),
                )

            conn.execute(
                "UPDATE csc_drafts SET updated_at = ? WHERE draft_id = ?",
                (now_iso, draft_id),
            )
            conn.commit()

        self.log_audit(
            draft_id,
            "UPDATE_IMPACT_ANALYSIS",
            reviewed_by or "system",
            new_value=f"Score {total:.2f} / {final_grade}",
        )
        return self.load_impact_analysis(draft_id) or {}

    # ------------------------------------------------------------------
    # Parameter operations
    # ------------------------------------------------------------------

    def upsert_parameters(
        self,
        draft_id: str,
        parameters: list[dict[str, Any]],
        user_name: str,
    ) -> None:
        """Surgically sync parameter rows for a draft.

        Strategy (avoids the revert-on-reload problem):
        - Rows that carry a known ``parameter_id`` → UPDATE in-place (sort_order + all fields).
        - Rows with a blank/unknown ``parameter_id`` → INSERT as new rows with a fresh UUID.
        - Existing DB rows whose ``parameter_id`` is no longer present in the incoming
          list → DELETE (genuine removals).

        This preserves IDs across saves, so the editor always reads back the same
        rows it wrote and does not revert to stale data.
        Only row additions and deletions are counted as structural changes in the
        audit log; field edits are recorded separately as modifications.
        """
        now = get_utc_now().isoformat()

        # Load current DB state keyed by parameter_id.
        with self._connect() as conn:
            db_rows = {
                str(r["parameter_id"]): dict(r)
                for r in conn.execute(
                    "SELECT * FROM csc_parameters WHERE draft_id = ? ORDER BY sort_order ASC",
                    (draft_id,),
                ).fetchall()
            }

        incoming_ids: set[str] = set()
        added: int = 0
        modified: int = 0

        with self._connect() as conn:
            for idx, p in enumerate(parameters):
                pid = str(p.get("parameter_id") or "").strip()
                param_name = sanitize_multiline_text(p.get("parameter_name", ""), max_length=_PARAM_TEXT_MAX_LENGTH)
                param_type = sanitize_text(
                    normalize_parameter_type_label(p.get("parameter_type", "Essential")),
                    max_length=80,
                )
                existing_val = sanitize_multiline_text(p.get("existing_value", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH)
                proposed_val = sanitize_multiline_text(p.get("proposed_value", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH)
                test_method_val = sanitize_multiline_text(p.get("test_method", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH)
                test_proc_type_val = sanitize_text(
                    normalize_test_procedure_type(str(p.get("test_procedure_type", "") or "")),
                    max_length=120,
                )
                test_proc_text_val = sanitize_multiline_text(p.get("test_procedure_text", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH)
                justification_val = sanitize_multiline_text(p.get("justification", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH)
                remarks_val = sanitize_multiline_text(p.get("remarks", "") or "", max_length=_PARAM_TEXT_MAX_LENGTH)

                if pid and pid in db_rows:
                    # Existing row — UPDATE in-place to preserve parameter_id.
                    incoming_ids.add(pid)
                    conn.execute(
                        """
                        UPDATE csc_parameters SET
                            parameter_name      = ?,
                            parameter_type      = ?,
                            existing_value      = ?,
                            proposed_value      = ?,
                            test_method         = ?,
                            test_procedure_type = ?,
                            test_procedure_text = ?,
                            justification       = ?,
                            remarks             = ?,
                            sort_order          = ?
                        WHERE parameter_id = ? AND draft_id = ?
                        """,
                        (
                            param_name, param_type, existing_val, proposed_val,
                            test_method_val, test_proc_type_val, test_proc_text_val,
                            justification_val, remarks_val, idx,
                            pid, draft_id,
                        ),
                    )
                    modified += 1
                else:
                    # New row — INSERT with a fresh UUID.
                    new_pid = new_uuid()
                    incoming_ids.add(new_pid)
                    conn.execute(
                        """
                        INSERT INTO csc_parameters
                            (parameter_id, draft_id, parameter_name, parameter_type,
                             existing_value, proposed_value, test_method,
                             test_procedure_type, test_procedure_text,
                             justification, remarks, sort_order)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            new_pid, draft_id, param_name, param_type,
                            existing_val, proposed_val, test_method_val,
                            test_proc_type_val, test_proc_text_val,
                            justification_val, remarks_val, idx,
                        ),
                    )
                    added += 1

            # Delete rows that are no longer in the incoming list (genuine removals).
            removed_ids = set(db_rows.keys()) - incoming_ids
            deleted = 0
            for orphan_id in removed_ids:
                conn.execute(
                    "DELETE FROM csc_parameters WHERE parameter_id = ? AND draft_id = ?",
                    (orphan_id, draft_id),
                )
                deleted += 1

            conn.execute(
                "UPDATE csc_drafts SET updated_at = ? WHERE draft_id = ?",
                (now, draft_id),
            )
            conn.commit()

        # Audit: structural changes (adds/deletes) and field edits logged separately.
        old_count = len(db_rows)
        new_count = len(parameters)
        if added or deleted:
            self.log_audit(
                draft_id, "PARAMETERS_STRUCTURE_CHANGED", user_name,
                old_value=f"{old_count} rows",
                new_value=f"{new_count} rows",
                remarks=f"+{added} added, -{deleted} deleted, {modified} edited",
            )
        elif modified:
            self.log_audit(
                draft_id, "PARAMETERS_EDITED", user_name,
                new_value=f"{modified} row(s) modified",
            )

    def load_parameters(self, draft_id: str) -> list[dict[str, Any]]:
        """Return all parameter rows for a draft, ordered by sort_order."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM csc_parameters
                WHERE draft_id = ?
                ORDER BY sort_order ASC
                """,
                (draft_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Issue flag operations
    # ------------------------------------------------------------------

    def upsert_issue_flags(
        self,
        draft_id: str,
        flags: list[dict[str, Any]],
        user_name: str,
    ) -> None:
        """Atomically replace all issue flag rows for a draft."""
        now = get_utc_now().isoformat()

        with self._connect() as conn:
            conn.execute(
                "DELETE FROM csc_issue_flags WHERE draft_id = ?", (draft_id,)
            )
            for idx, f in enumerate(flags):
                conn.execute(
                    """
                    INSERT INTO csc_issue_flags
                        (issue_id, draft_id, issue_type, is_present, note, sort_order)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        sanitize_text(f.get("issue_type", ""), max_length=50),
                        1 if f.get("is_present") else 0,
                        sanitize_multiline_text(f.get("note", "") or "", max_length=3000),
                        idx,
                    ),
                )
            conn.execute(
                "UPDATE csc_drafts SET updated_at = ? WHERE draft_id = ?",
                (now, draft_id),
            )
            conn.commit()

        self.log_audit(draft_id, "UPDATE_ISSUES", user_name)

    def load_issue_flags(self, draft_id: str) -> list[dict[str, Any]]:
        """Return all issue flag rows for a draft, ordered by sort_order."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM csc_issue_flags
                WHERE draft_id = ?
                ORDER BY sort_order ASC
                """,
                (draft_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------

    def log_audit(
        self,
        draft_id: str | None,
        action: str,
        user_name: str,
        old_value: str | None = None,
        new_value: str | None = None,
        remarks: str | None = None,
    ) -> None:
        """Append an audit entry. Failures are logged but never raised."""
        try:
            now = get_utc_now().isoformat()
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO csc_audit
                        (audit_id, draft_id, action, user_name, action_time,
                         old_value, new_value, remarks)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_uuid(),
                        draft_id,
                        sanitize_text(action, max_length=100),
                        sanitize_text(user_name or "unknown", max_length=200),
                        now,
                        truncate_for_log(old_value) if old_value else None,
                        truncate_for_log(new_value) if new_value else None,
                        truncate_for_log(remarks) if remarks else None,
                    ),
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Audit log failed: %s", exc)

    def load_audit(
        self, draft_id: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Return most-recent audit entries for a draft."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM csc_audit
                WHERE draft_id = ?
                ORDER BY action_time DESC
                LIMIT ?
                """,
                (draft_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Backup / restore helpers
    # ------------------------------------------------------------------

    def get_db_file_path(self) -> str:
        """Return the absolute path to the active SQLite file."""
        return self._db_file

    def create_database_backup_bytes(self) -> bytes:
        """Create a consistent full backup snapshot and return raw .db bytes."""
        db_path = Path(self._db_file)
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")

        fd, tmp_name = tempfile.mkstemp(prefix="csc_backup_", suffix=".db")
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            with sqlite3.connect(str(db_path), timeout=30) as src_conn:
                src_conn.execute("PRAGMA busy_timeout = 30000")
                src_conn.execute("PRAGMA foreign_keys = ON")
                src_conn.execute("PRAGMA synchronous = NORMAL")
                src_conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                with sqlite3.connect(str(tmp_path), timeout=30) as dst_conn:
                    src_conn.backup(dst_conn)
                    dst_conn.commit()
            return tmp_path.read_bytes()
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def validate_database_backup_bytes(self, backup_bytes: bytes) -> tuple[bool, str]:
        """Validate an uploaded .db payload before restore."""
        if not backup_bytes:
            return False, "Uploaded file is empty."
        if not backup_bytes.startswith(b"SQLite format 3"):
            return False, "File is not a valid SQLite database."

        fd, tmp_name = tempfile.mkstemp(prefix="csc_validate_", suffix=".db")
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            tmp_path.write_bytes(backup_bytes)
            return self._validate_database_backup_file(tmp_path)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _validate_database_backup_file(self, db_path: Path) -> tuple[bool, str]:
        """Return validation status for a sqlite file path."""
        try:
            with sqlite3.connect(str(db_path), timeout=30) as conn:
                integrity_row = conn.execute("PRAGMA integrity_check").fetchone()
                integrity = str(integrity_row[0] if integrity_row else "").lower()
                if integrity != "ok":
                    return False, f"Integrity check failed: {integrity or 'unknown'}"

                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
                tables = {str(r[0]) for r in rows}
                required = {
                    "T_CSC_Schema_Version",
                    "csc_drafts",
                    "csc_sections",
                    "csc_parameters",
                    "csc_issue_flags",
                    "csc_audit",
                }
                missing = sorted(required - tables)
                if missing:
                    return False, "Backup missing required tables: " + ", ".join(missing)
            return True, "Backup file validated successfully."
        except Exception as exc:
            return False, f"Backup validation failed: {exc}"

    def restore_database_from_bytes(
        self,
        backup_bytes: bytes,
        pre_restore_backup_dir: str | None = None,
    ) -> str:
        """Replace active DB with uploaded backup. Returns safety-backup path when created."""
        ok, message = self.validate_database_backup_bytes(backup_bytes)
        if not ok:
            raise ValueError(message)

        db_path = Path(self._db_file)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        safety_backup_path = ""
        if pre_restore_backup_dir:
            safety_dir = Path(pre_restore_backup_dir)
            safety_dir.mkdir(parents=True, exist_ok=True)
            stamp = get_utc_now().strftime("%Y%m%d_%H%M%S")
            safety_file = safety_dir / f"PRE_RESTORE_BACKUP_{stamp}.db"
            safety_file.write_bytes(self.create_database_backup_bytes())
            safety_backup_path = str(safety_file)

        fd, tmp_name = tempfile.mkstemp(prefix="csc_restore_", suffix=".db")
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            tmp_path.write_bytes(backup_bytes)

            # Repository connections are short-lived per operation; checkpoint and replace.
            try:
                with self._connect() as conn:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass

            # Explicitly close a local handle before replacing the DB file.
            close_conn = sqlite3.connect(str(db_path), timeout=30)
            close_conn.close()

            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{db_path}{suffix}")
                if sidecar.exists():
                    sidecar.unlink(missing_ok=True)

            os.replace(str(tmp_path), str(db_path))
            self.ensure_db()
            return safety_backup_path
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def get_master_data_report_payload(
        self,
        parameter_preview_limit: int = 5,
    ) -> dict[str, Any]:
        """Return aggregate data used for master-data PDF reporting."""
        preview_limit = max(1, int(parameter_preview_limit))
        with self._connect() as conn:
            specs_rows = conn.execute(
                """
                SELECT
                    draft_id,
                    spec_number,
                    chemical_name,
                    committee_name,
                    prepared_by,
                    spec_version,
                    meeting_date,
                    status,
                    admin_stage,
                    updated_at
                FROM csc_drafts
                WHERE is_admin_draft = 1
                ORDER BY spec_number ASC
                """
            ).fetchall()

            total_specs = len(specs_rows)
            total_params_row = conn.execute(
                """
                SELECT COUNT(*)
                FROM csc_parameters p
                JOIN csc_drafts d ON d.draft_id = p.draft_id
                WHERE d.is_admin_draft = 1
                """
            ).fetchone()
            total_revisions_row = conn.execute(
                "SELECT COUNT(*) FROM csc_drafts WHERE parent_draft_id IS NOT NULL"
            ).fetchone()

            specs: list[dict[str, Any]] = []
            parameter_preview: dict[str, list[dict[str, Any]]] = {}
            parameters_by_draft: dict[str, list[dict[str, Any]]] = {}
            impact_by_draft: dict[str, dict[str, Any]] = {}
            for row in specs_rows:
                draft_id = str(row["draft_id"])
                specs.append(dict(row))
                p_rows = conn.execute(
                    """
                    SELECT
                        parameter_name,
                        parameter_type,
                        existing_value,
                        test_method,
                        test_procedure_type,
                        test_procedure_text,
                        sort_order
                    FROM csc_parameters
                    WHERE draft_id = ?
                    ORDER BY sort_order ASC
                    """,
                    (draft_id,),
                ).fetchall()
                all_params = [dict(p) for p in p_rows]
                parameters_by_draft[draft_id] = all_params
                parameter_preview[draft_id] = all_params[:preview_limit]

                impact_row = conn.execute(
                    """
                    SELECT
                        operational_impact_score,
                        safety_environment_score,
                        supply_risk_score,
                        no_substitute_flag,
                        impact_score_total,
                        impact_grade,
                        operational_note,
                        safety_environment_note,
                        supply_risk_note,
                        reviewed_by,
                        review_date
                    FROM impact_analysis
                    WHERE draft_id = ?
                    """,
                    (draft_id,),
                ).fetchone()
                impact_by_draft[draft_id] = dict(impact_row) if impact_row else {}

        specs_with_impact = sum(1 for v in impact_by_draft.values() if v)

        return {
            "totals": {
                "specifications": int(total_specs),
                "parameters": int(total_params_row[0] if total_params_row else 0),
                "revisions": int(total_revisions_row[0] if total_revisions_row else 0),
                "specs_with_impact": int(specs_with_impact),
            },
            "specifications": specs,
            "parameters_by_draft": parameters_by_draft,
            "impact_by_draft": impact_by_draft,
            "parameter_preview": parameter_preview,
            "parameter_preview_limit": preview_limit,
        }

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def health_check(self) -> bool:
        """Return True if the database is accessible."""
        try:
            with self._connect() as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except Exception as exc:
            logger.error("DB health check failed: %s", exc)
            return False
