#!/usr/bin/env python3
"""Bulk ingest all Corporate Specifications from a .docx file into the CSC database.

Usage (from the project root):
    python3 bulk_ingest_docx.py
    python3 bulk_ingest_docx.py --docx "Corporate Specifications updated.docx"
    python3 bulk_ingest_docx.py --docx path/to/file.docx --dry-run
    python3 bulk_ingest_docx.py --docx path/to/file.docx --user "MS Name" --skip-existing

Options:
    --docx          Path to the .docx file. Defaults to
                    "Corporate Specifications updated.docx" in the same folder.
    --db            Path to the SQLite database. Defaults to data/CSC_Data.db.
    --user          User name recorded in the audit log. Default: "bulk_ingest".
    --dry-run       Parse and print what would be ingested; do not write to DB.
    --skip-existing Skip specs whose spec_number already exists in the DB (default).
    --overwrite     Overwrite (re-ingest) specs that already exist in the DB.
    --committee     Default committee name for all ingested specs.
                    Default: "Chemical Specification Committee".

Exit codes:
    0   Success (all specs ingested / dry-run complete).
    1   Fatal error (file not found, DB error, etc.).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so local modules resolve correctly.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).parent.resolve()
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from csc_docx_extractor import extract_all_specs_from_path, spec_to_parameter_dicts
from csc_repository import CSCRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bulk_ingest")

# ---------------------------------------------------------------------------
# Default paths
# ---------------------------------------------------------------------------
_DEFAULT_DOCX = _PROJECT_ROOT / "Corporate Specifications updated.docx"
_DEFAULT_DB   = _PROJECT_ROOT / "data" / "CSC_Data.db"


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk-ingest Corporate Specifications .docx into the CSC database."
    )
    p.add_argument("--docx",           default=str(_DEFAULT_DOCX),
                   help="Path to the .docx file")
    p.add_argument("--db",             default=str(_DEFAULT_DB),
                   help="Path to the SQLite database")
    p.add_argument("--user",           default="bulk_ingest",
                   help="User name for audit log")
    p.add_argument("--committee",      default="Chemical Specification Committee",
                   help="Default committee name for ingested specs")
    p.add_argument("--dry-run",        action="store_true",
                   help="Parse and report; do not write to the database")
    p.add_argument("--skip-existing",  action="store_true", default=True,
                   help="Skip specs already in the DB (default)")
    p.add_argument("--overwrite",      action="store_true",
                   help="Re-ingest specs that already exist (deletes old draft first)")
    p.add_argument("--verbose",        action="store_true",
                   help="Print per-parameter detail for each spec")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    docx_path = Path(args.docx)
    if not docx_path.exists():
        logger.error("Docx file not found: %s", docx_path)
        return 1

    db_path = Path(args.db)

    # ── Extract all specs from the .docx ────────────────────────────────────
    logger.info("Reading: %s", docx_path.name)
    try:
        specs = extract_all_specs_from_path(docx_path)
    except Exception as exc:
        logger.exception("Extraction failed: %s", exc)
        return 1

    logger.info("Extracted %d specifications from document", len(specs))

    # ── Summary table in dry-run mode ────────────────────────────────────────
    if args.dry_run:
        _print_dry_run_summary(specs, args.verbose)
        return 0

    # ── Connect to DB ────────────────────────────────────────────────────────
    db_path.parent.mkdir(parents=True, exist_ok=True)
    repo = CSCRepository(str(db_path))
    repo.ensure_db()
    logger.info("Database: %s", db_path)

    # Build a set of spec numbers already in the DB for fast lookup
    existing_specs = {
        d["spec_number"]: d["draft_id"]
        for d in repo.list_drafts(admin_only=True)
        if d.get("spec_number")
    }

    # ── Ingest loop ──────────────────────────────────────────────────────────
    ingested  = 0
    skipped   = 0
    overwritten = 0
    errors    = 0
    no_params = 0

    for spec in specs:
        spec_no = spec.spec_number or "(no spec number)"

        if not spec.chemical_name:
            logger.warning("  SKIP  %-30s — no chemical name", spec_no)
            skipped += 1
            continue

        # Skip procurement-note specs (spec_number starts with "Note:")
        if spec.spec_number.upper().startswith("NOTE:") or spec.spec_number.upper().startswith("NOTE "):
            logger.info("  SKIP  %-30s — procurement reference note (no parameters)", spec_no[:50])
            skipped += 1
            continue

        # Warn about empty specs but still ingest the header
        if not spec.parameters:
            logger.warning("  WARN  %-30s — 0 parameters", spec_no)
            no_params += 1

        # Handle existing spec
        if spec.spec_number in existing_specs:
            if args.overwrite:
                old_id = existing_specs[spec.spec_number]
                repo.delete_draft(old_id, args.user)
                logger.info("  DEL   %-30s — removed old draft %s", spec_no, old_id)
                overwritten += 1
            else:
                logger.info("  SKIP  %-30s — already in DB", spec_no)
                skipped += 1
                continue

        # ── Create admin draft ───────────────────────────────────────────────
        try:
            draft_id = repo.create_draft(
                spec_number=spec.spec_number,
                chemical_name=spec.chemical_name,
                committee_name=args.committee,
                meeting_date="",
                prepared_by=args.user,
                reviewed_by="",
                user_name=args.user,
                is_admin_draft=True,
                admin_stage="structuring",
                test_procedure=spec.test_procedure,
                material_code=spec.material_code,
            )
        except Exception as exc:
            logger.error("  ERR   %-30s — create_draft failed: %s", spec_no, exc)
            errors += 1
            continue

        # ── Upsert parameters ────────────────────────────────────────────────
        if spec.parameters:
            param_dicts = spec_to_parameter_dicts(spec)
            try:
                repo.upsert_parameters(draft_id, param_dicts, args.user)
            except Exception as exc:
                logger.error("  ERR   %-30s — upsert_parameters failed: %s", spec_no, exc)
                # Draft was created; leave it in structuring so admin can fix
                errors += 1
                continue

        n_params = len(spec.parameters)
        n_warn   = len(spec.parse_warnings)
        warn_str = f"  [{n_warn} warnings]" if n_warn else ""
        logger.info(
            "  OK    %-30s  %3d params%s",
            spec_no, n_params, warn_str,
        )
        if args.verbose and spec.parameters:
            for p in spec.parameters:
                logger.info("          [%s] %s → %r", p.parameter_type, p.name[:60], p.existing_value[:40])

        ingested += 1

    # ── Final report ─────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("BULK INGEST COMPLETE")
    print(f"  Ingested   : {ingested}")
    print(f"  Skipped    : {skipped}")
    print(f"  Overwritten: {overwritten}")
    print(f"  No params  : {no_params}")
    print(f"  Errors     : {errors}")
    print("=" * 60)
    print()
    print("Next step: open the app, go to Admin workspace, and")
    print("review each spec in Structuring stage before Publishing.")
    print()

    return 0 if errors == 0 else 1


def _print_dry_run_summary(specs: list, verbose: bool) -> None:
    print()
    print("=" * 70)
    print(f"DRY RUN — {len(specs)} specs parsed (nothing written to database)")
    print("=" * 70)
    total_params = 0
    for spec in specs:
        n = len(spec.parameters)
        total_params += n
        warn = f"  [{len(spec.parse_warnings)} warn]" if spec.parse_warnings else ""
        print(f"  {spec.spec_number or '(no number)':<35}  {spec.chemical_name[:35]:<35}  {n:3d} params{warn}")
        if verbose:
            for p in spec.parameters:
                print(f"      [{p.parameter_type}] {p.name[:60]}")
                print(f"             → {p.existing_value[:60]}")
            for w in spec.parse_warnings:
                print(f"      ⚠ {w}")
    print("=" * 70)
    print(f"Total parameters: {total_params}")
    print()


if __name__ == "__main__":
    sys.exit(main())
