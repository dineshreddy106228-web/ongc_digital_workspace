from __future__ import annotations

"""CSC legacy import CLI commands."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import sqlite3

import click
from flask.cli import with_appcontext
from sqlalchemy import text

from app.extensions import db
from app.core.services.csc_master_data import (
    backfill_draft_material_codes,
    sync_inventory_master_from_csc,
)
from app.core.services.csc_utils import spec_sort_key
from app.models.core.user import User
from app.models.csc import (
    CSCAudit,
    CSCDraft,
    CSCImpactAnalysis,
    CSCIssueFlag,
    CSCParameter,
    CSCRevision,
    CSCSection,
    CSCSpecVersion,
)


CSC_TABLES_IN_DELETE_ORDER = [
    CSCRevision,
    CSCSpecVersion,
    CSCAudit,
    CSCImpactAnalysis,
    CSCIssueFlag,
    CSCSection,
    CSCParameter,
    CSCDraft,
]

STATUS_MAP = {
    "draft": "Draft",
    "under review": "Under Review",
    "committee review": "Committee Review",
    "final": "Final",
    "published": "Published",
    "master data": "Published",
}

EMPLOYEE_CODE_RE = re.compile(r"(\d{5,})$")


@dataclass
class ImportSummary:
    drafts: int = 0
    parameters: int = 0
    sections: int = 0
    issue_flags: int = 0
    impact_analysis: int = 0
    audit_entries: int = 0
    revisions: int = 0
    spec_versions: int = 0
    owner_matches_by_username: int = 0
    owner_matches_by_employee_code: int = 0
    owner_matches_by_default: int = 0
    owner_unresolved: int = 0


@dataclass
class ResequenceSummary:
    changed: bool = False
    draft_count: int = 0
    old_first_id: int | None = None
    new_first_id: int | None = None


def _connect_sqlite(source_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _parse_bool(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    parsed = datetime.fromisoformat(value.strip())
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _normalize_status(status: str | None, admin_stage: str | None) -> str:
    normalized = (status or "").strip().lower()
    if normalized in STATUS_MAP:
        return STATUS_MAP[normalized]
    if (admin_stage or "").strip().lower() == "published":
        return "Published"
    return (status or "Draft").strip() or "Draft"


def _extract_employee_code(value: str | None) -> str | None:
    if not value:
        return None
    match = EMPLOYEE_CODE_RE.search(value.strip())
    if not match:
        return None
    return match.group(1)


def _resolve_owner_id(
    created_by_user: str | None,
    prepared_by: str | None,
    users_by_username: dict[str, int],
    users_by_employee_code: dict[str, int],
    default_owner_id: int | None,
    summary: ImportSummary,
) -> int | None:
    username_key = (created_by_user or "").strip().lower()
    if username_key and username_key in users_by_username:
        summary.owner_matches_by_username += 1
        return users_by_username[username_key]

    employee_code = _extract_employee_code(prepared_by) or _extract_employee_code(created_by_user)
    if employee_code and employee_code in users_by_employee_code:
        summary.owner_matches_by_employee_code += 1
        return users_by_employee_code[employee_code]

    if default_owner_id is not None:
        summary.owner_matches_by_default += 1
        return default_owner_id

    summary.owner_unresolved += 1
    return None


def _table_has_rows(model) -> bool:
    return db.session.query(model.id).limit(1).first() is not None


def _target_has_existing_csc_data() -> bool:
    return any(_table_has_rows(model) for model in CSC_TABLES_IN_DELETE_ORDER)


def _clear_existing_csc_data() -> None:
    for model in CSC_TABLES_IN_DELETE_ORDER:
        db.session.query(model).delete()
    db.session.flush()


def _sqlite_table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _load_users() -> tuple[dict[str, int], dict[str, int]]:
    users = User.query.all()
    users_by_username = {
        user.username.strip().lower(): user.id
        for user in users
        if user.username and user.id is not None
    }
    users_by_employee_code = {
        user.employee_code.strip(): user.id
        for user in users
        if user.employee_code and user.id is not None
    }
    return users_by_username, users_by_employee_code


def _resolve_default_owner_id(default_owner: str | None) -> int | None:
    if not default_owner:
        return None
    user = User.query.filter_by(username=default_owner.strip()).first()
    if user is None:
        raise click.ClickException(
            f"Default owner '{default_owner}' was not found in the target users table."
        )
    return user.id


def _import_drafts(
    connection: sqlite3.Connection,
    users_by_username: dict[str, int],
    users_by_employee_code: dict[str, int],
    default_owner_id: int | None,
    summary: ImportSummary,
) -> dict[str, int]:
    source_rows = connection.execute(
        """
        SELECT draft_id, spec_number, chemical_name, committee_name, meeting_date,
               prepared_by, reviewed_by, status, created_by_role, created_by_user,
               is_admin_draft, phase1_locked, parent_draft_id, admin_stage,
               spec_version, created_at, updated_at
        FROM csc_drafts
        """
    ).fetchall()
    source_rows = sorted(source_rows, key=lambda row: spec_sort_key(row["spec_number"] or ""))

    draft_id_map: dict[str, int] = {}
    imported_drafts: list[CSCDraft] = []

    for next_id, row in enumerate(source_rows, start=1):
        source_draft_id = row["draft_id"]
        draft_id_map[source_draft_id] = next_id
        imported_drafts.append(
            CSCDraft(
                id=next_id,
                spec_number=(row["spec_number"] or "").strip(),
                chemical_name=(row["chemical_name"] or "").strip(),
                committee_name=(row["committee_name"] or "").strip()
                or "Corporate Specifiction Committee",
                meeting_date=row["meeting_date"],
                prepared_by=(row["prepared_by"] or "").strip() or "system",
                reviewed_by=(row["reviewed_by"] or "").strip() or None,
                status=_normalize_status(row["status"], row["admin_stage"]),
                created_by_role=(row["created_by_role"] or "").strip() or "Admin",
                is_admin_draft=_parse_bool(row["is_admin_draft"]),
                phase1_locked=_parse_bool(row["phase1_locked"]),
                admin_stage=(row["admin_stage"] or "").strip() or "ingested",
                spec_version=int(row["spec_version"] or 0),
                created_by_id=_resolve_owner_id(
                    row["created_by_user"],
                    row["prepared_by"],
                    users_by_username,
                    users_by_employee_code,
                    default_owner_id,
                    summary,
                ),
                created_at=_parse_datetime(row["created_at"]) or datetime.utcnow(),
                updated_at=_parse_datetime(row["updated_at"]) or datetime.utcnow(),
            )
        )

    db.session.add_all(imported_drafts)
    db.session.flush()

    for row in source_rows:
        source_parent_id = (row["parent_draft_id"] or "").strip()
        if not source_parent_id:
            continue
        target_draft = db.session.get(CSCDraft, draft_id_map[row["draft_id"]])
        if target_draft is not None:
            target_draft.parent_draft_id = draft_id_map.get(source_parent_id)

    summary.drafts = len(source_rows)
    return draft_id_map


def _import_parameters(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    rows = connection.execute(
        """
        SELECT draft_id, parameter_name, parameter_type, existing_value, proposed_value,
               test_method, test_procedure_type, test_procedure_text, justification,
               remarks, sort_order
        FROM csc_parameters
        ORDER BY draft_id, sort_order, parameter_id
        """
    ).fetchall()

    objects = [
        CSCParameter(
            id=index,
            draft_id=draft_id_map[row["draft_id"]],
            parameter_name=(row["parameter_name"] or "").strip(),
            parameter_type=(row["parameter_type"] or "").strip() or "Essential",
            existing_value=row["existing_value"],
            proposed_value=row["proposed_value"],
            test_method=row["test_method"],
            test_procedure_type=(row["test_procedure_type"] or "").strip(),
            test_procedure_text=row["test_procedure_text"],
            justification=row["justification"],
            remarks=row["remarks"],
            sort_order=int(row["sort_order"] or 0),
        )
        for index, row in enumerate(rows, start=1)
        if row["draft_id"] in draft_id_map
    ]
    db.session.add_all(objects)
    summary.parameters = len(objects)


def _import_sections(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    rows = connection.execute(
        """
        SELECT draft_id, section_name, section_text, sort_order
        FROM csc_sections
        ORDER BY draft_id, sort_order, section_id
        """
    ).fetchall()

    objects = [
        CSCSection(
            id=index,
            draft_id=draft_id_map[row["draft_id"]],
            section_name=(row["section_name"] or "").strip(),
            section_text=row["section_text"],
            sort_order=int(row["sort_order"] or 0),
        )
        for index, row in enumerate(rows, start=1)
        if row["draft_id"] in draft_id_map
    ]
    db.session.add_all(objects)
    summary.sections = len(objects)


def _import_issue_flags(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    rows = connection.execute(
        """
        SELECT draft_id, issue_type, is_present, note, sort_order
        FROM csc_issue_flags
        ORDER BY draft_id, sort_order, issue_id
        """
    ).fetchall()

    objects = [
        CSCIssueFlag(
            id=index,
            draft_id=draft_id_map[row["draft_id"]],
            issue_type=(row["issue_type"] or "").strip(),
            is_present=_parse_bool(row["is_present"]),
            note=row["note"],
            sort_order=int(row["sort_order"] or 0),
        )
        for index, row in enumerate(rows, start=1)
        if row["draft_id"] in draft_id_map
    ]
    db.session.add_all(objects)
    summary.issue_flags = len(objects)


def _import_impact_analysis(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    source_table = "csc_impact_analysis" if _sqlite_table_exists(
        connection, "csc_impact_analysis"
    ) else "impact_analysis"
    if not _sqlite_table_exists(connection, source_table):
        return

    rows = connection.execute(
        f"""
        SELECT draft_id, operational_impact_score, safety_environment_score,
               supply_risk_score, no_substitute_flag, impact_score_total,
               impact_grade, operational_note, safety_environment_note,
               supply_risk_note, reviewed_by, review_date, created_at, updated_at
        FROM {source_table}
        ORDER BY draft_id
        """
    ).fetchall()

    objects = [
        CSCImpactAnalysis(
            id=index,
            draft_id=draft_id_map[row["draft_id"]],
            operational_impact_score=int(row["operational_impact_score"] or 0),
            safety_environment_score=int(row["safety_environment_score"] or 0),
            supply_risk_score=int(row["supply_risk_score"] or 0),
            no_substitute_flag=_parse_bool(row["no_substitute_flag"]),
            impact_score_total=float(row["impact_score_total"] or 0.0),
            impact_grade=(row["impact_grade"] or "").strip() or "LOW",
            operational_note=row["operational_note"],
            safety_environment_note=row["safety_environment_note"],
            supply_risk_note=row["supply_risk_note"],
            reviewed_by=(row["reviewed_by"] or "").strip() or None,
            review_date=row["review_date"],
            created_at=_parse_datetime(row["created_at"]) or datetime.utcnow(),
            updated_at=_parse_datetime(row["updated_at"]) or datetime.utcnow(),
        )
        for index, row in enumerate(rows, start=1)
        if row["draft_id"] in draft_id_map
    ]
    db.session.add_all(objects)
    summary.impact_analysis = len(objects)


def _import_audit_entries(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    rows = connection.execute(
        """
        SELECT draft_id, action, user_name, action_time, old_value, new_value, remarks
        FROM csc_audit
        ORDER BY datetime(action_time), audit_id
        """
    ).fetchall()

    objects = []
    for index, row in enumerate(rows, start=1):
        source_draft_id = (row["draft_id"] or "").strip()
        objects.append(
            CSCAudit(
                id=index,
                draft_id=draft_id_map.get(source_draft_id),
                action=(row["action"] or "").strip(),
                user_name=(row["user_name"] or "").strip() or "system",
                action_time=_parse_datetime(row["action_time"]) or datetime.utcnow(),
                old_value=row["old_value"],
                new_value=row["new_value"],
                remarks=row["remarks"],
            )
        )

    db.session.add_all(objects)
    summary.audit_entries = len(objects)


def _import_revisions(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    if not _sqlite_table_exists(connection, "csc_revisions"):
        summary.revisions = 0
        return

    rows = connection.execute(
        """
        SELECT parent_draft_id, child_draft_id, submitted_at, reviewed_at, status,
               reviewer_notes, created_at, updated_at
        FROM csc_revisions
        ORDER BY datetime(created_at), id
        """
    ).fetchall()

    objects = [
        CSCRevision(
            id=index,
            parent_draft_id=draft_id_map[row["parent_draft_id"]],
            child_draft_id=draft_id_map[row["child_draft_id"]],
            submitted_at=_parse_datetime(row["submitted_at"]),
            reviewed_at=_parse_datetime(row["reviewed_at"]),
            status=(row["status"] or "").strip() or "pending",
            reviewer_notes=row["reviewer_notes"],
            created_at=_parse_datetime(row["created_at"]) or datetime.utcnow(),
            updated_at=_parse_datetime(row["updated_at"]) or datetime.utcnow(),
        )
        for index, row in enumerate(rows, start=1)
        if row["parent_draft_id"] in draft_id_map and row["child_draft_id"] in draft_id_map
    ]
    db.session.add_all(objects)
    summary.revisions = len(objects)


def _import_spec_versions(
    connection: sqlite3.Connection,
    draft_id_map: dict[str, int],
    summary: ImportSummary,
) -> None:
    rows = connection.execute(
        """
        SELECT draft_id, spec_version, created_at, created_by, source_action,
               remarks, payload_json
        FROM csc_spec_versions
        ORDER BY draft_id, spec_version, snapshot_id
        """
    ).fetchall()

    objects = [
        CSCSpecVersion(
            id=index,
            draft_id=draft_id_map[row["draft_id"]],
            spec_version=int(row["spec_version"] or 0),
            created_at=_parse_datetime(row["created_at"]) or datetime.utcnow(),
            created_by=(row["created_by"] or "").strip() or "system",
            source_action=(row["source_action"] or "").strip(),
            remarks=row["remarks"],
            payload_json=row["payload_json"] or "{}",
        )
        for index, row in enumerate(rows, start=1)
        if row["draft_id"] in draft_id_map
    ]
    db.session.add_all(objects)
    summary.spec_versions = len(objects)


def import_legacy_csc_data(
    source_path: Path,
    default_owner: str | None = None,
    replace_existing: bool = False,
) -> ImportSummary:
    if not source_path.exists():
        raise click.ClickException(f"Legacy SQLite database not found: {source_path}")

    if _target_has_existing_csc_data():
        if not replace_existing:
            raise click.ClickException(
                "Target CSC tables already contain data. Re-run with --replace-existing to overwrite them."
            )
        _clear_existing_csc_data()

    summary = ImportSummary()
    default_owner_id = _resolve_default_owner_id(default_owner)
    users_by_username, users_by_employee_code = _load_users()

    with _connect_sqlite(source_path) as connection:
        try:
            draft_id_map = _import_drafts(
                connection,
                users_by_username,
                users_by_employee_code,
                default_owner_id,
                summary,
            )
            _import_parameters(connection, draft_id_map, summary)
            _import_sections(connection, draft_id_map, summary)
            _import_issue_flags(connection, draft_id_map, summary)
            _import_impact_analysis(connection, draft_id_map, summary)
            _import_audit_entries(connection, draft_id_map, summary)
            _import_revisions(connection, draft_id_map, summary)
            _import_spec_versions(connection, draft_id_map, summary)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            raise click.ClickException(f"CSC legacy import failed: {exc}") from exc

    return summary


def resequence_csc_draft_ids() -> ResequenceSummary:
    drafts = sorted(CSCDraft.query.all(), key=lambda draft: spec_sort_key(draft.spec_number or ""))
    summary = ResequenceSummary(draft_count=len(drafts))
    if not drafts:
        return summary

    mapping = {draft.id: index for index, draft in enumerate(drafts, start=1)}
    summary.old_first_id = drafts[0].id
    summary.new_first_id = 1

    if all(old_id == new_id for old_id, new_id in mapping.items()):
        return summary

    summary.changed = True
    max_existing_id = max(mapping)
    offset = max_existing_id + len(mapping) + 1000
    columns_to_update = [
        ("csc_drafts", "id"),
        ("csc_drafts", "parent_draft_id"),
        ("csc_parameters", "draft_id"),
        ("csc_sections", "draft_id"),
        ("csc_issue_flags", "draft_id"),
        ("csc_impact_analysis", "draft_id"),
        ("csc_audit", "draft_id"),
        ("csc_revisions", "parent_draft_id"),
        ("csc_revisions", "child_draft_id"),
        ("csc_spec_versions", "draft_id"),
    ]

    with db.engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            for old_id, new_id in mapping.items():
                temp_id = new_id + offset
                for table_name, column_name in columns_to_update:
                    connection.execute(
                        text(f"UPDATE {table_name} SET {column_name} = :new_value WHERE {column_name} = :old_value"),
                        {"new_value": temp_id, "old_value": old_id},
                    )

            for old_id, new_id in mapping.items():
                temp_id = new_id + offset
                for table_name, column_name in columns_to_update:
                    connection.execute(
                        text(f"UPDATE {table_name} SET {column_name} = :new_value WHERE {column_name} = :old_value"),
                        {"new_value": new_id, "old_value": temp_id},
                    )

            transaction.commit()
        except Exception:
            transaction.rollback()
            raise
        finally:
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    db.session.remove()
    return summary


@click.command("import-csc-legacy")
@click.option(
    "--source",
    "source_path",
    default="CSC Workflow Automation /data/CSC_Data.db",
    show_default=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to the legacy CSC SQLite database.",
)
@click.option(
    "--default-owner",
    default=None,
    help="Fallback username for imported drafts when legacy ownership cannot be resolved.",
)
@click.option(
    "--replace-existing",
    is_flag=True,
    default=False,
    help="Delete existing CSC data before importing.",
)
@with_appcontext
def import_csc_legacy(source_path: Path, default_owner: str | None, replace_existing: bool) -> None:
    """Import CSC workflow data from the legacy Streamlit SQLite database."""
    summary = import_legacy_csc_data(
        source_path=source_path,
        default_owner=default_owner,
        replace_existing=replace_existing,
    )

    click.echo(f"Imported legacy CSC data from: {source_path}")
    click.echo(
        "Rows: "
        f"drafts={summary.drafts}, "
        f"parameters={summary.parameters}, "
        f"sections={summary.sections}, "
        f"issue_flags={summary.issue_flags}, "
        f"impact_analysis={summary.impact_analysis}, "
        f"audit={summary.audit_entries}, "
        f"revisions={summary.revisions}, "
        f"spec_versions={summary.spec_versions}"
    )
    click.echo(
        "Owner resolution: "
        f"username={summary.owner_matches_by_username}, "
        f"employee_code={summary.owner_matches_by_employee_code}, "
        f"default={summary.owner_matches_by_default}, "
        f"unresolved={summary.owner_unresolved}"
    )


@click.command("resequence-csc-draft-ids")
@with_appcontext
def resequence_csc_draft_ids_command() -> None:
    """Rewrite CSC draft IDs to match the current canonical list order."""
    summary = resequence_csc_draft_ids()
    if not summary.draft_count:
        click.echo("No CSC drafts found; nothing to resequence.")
        return
    if not summary.changed:
        click.echo(
            f"CSC draft IDs already match the current list order ({summary.draft_count} drafts)."
        )
        return

    click.echo(
        "CSC draft IDs resequenced successfully: "
        f"{summary.draft_count} drafts updated; first draft id {summary.old_first_id} -> {summary.new_first_id}."
    )


@click.command("backfill-csc-material-codes")
@with_appcontext
def backfill_csc_material_codes_command() -> None:
    """Populate CSC draft material codes from imported spec-version snapshots."""
    updated, missing = backfill_draft_material_codes()
    db.session.commit()
    click.echo(
        "CSC material-code backfill complete: "
        f"updated={updated}, missing={missing}."
    )


@click.command("sync-csc-master-data")
@with_appcontext
def sync_csc_master_data_command() -> None:
    """Mirror linked CSC draft material rows into Inventory Master Data."""
    backfilled, missing_before_sync = backfill_draft_material_codes()
    summary = sync_inventory_master_from_csc()
    db.session.commit()
    click.echo(
        "CSC -> Inventory master-data sync complete: "
        f"backfilled_material_codes={backfilled}, "
        f"drafts_missing_material_code={missing_before_sync}, "
        f"created_rows={summary['created']}, "
        f"updated_rows={summary['updated']}, "
        f"linked_materials={summary['linked_materials']}, "
        f"duplicate_material_codes={summary['duplicate_material_code']}."
    )
