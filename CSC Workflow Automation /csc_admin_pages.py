"""CSC Admin Workspace — Member Secretary master data pipeline.

MEMBER SECRETARY WORKFLOW (3 stages per spec):
───────────────────────────────────────────────────────────────────────────────
  Stage 1 — INGEST
    Upload one or more existing (unstructured) ONGC spec PDFs, or
    a master Corporate Specifications .docx.
    System auto-extracts metadata and parameters.
    MS can also create a spec manually with no PDF.

  Stage 2 — STRUCTURING
    Open each spec individually.
    Fix all extraction errors: parameter names, existing values, types,
    test procedure type/text. Add missing rows. Remove duplicates. Reorder as needed.
    Stage 2 starts at v0 baseline; version only increments when the spec is Published.

  Stage 3 — PUBLISHED (Master Data)
    Spec is finalised. Included in master export.
    MS can retract at any time → goes back to Stage 2.

MASTER EXPORT:
    Generates a single .docx containing all published specs one-by-one,
    followed by a final Change Log page summarising version changes.
───────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from csc_repository import (
    CSCRepository,
    ADMIN_STAGE_INGESTED,
    ADMIN_STAGE_STRUCTURING,
    ADMIN_STAGE_PUBLISHED,
    calculate_impact_score,
    get_impact_grade,
    normalize_parameter_type_label,
    normalize_test_procedure_type,
)
from csc_export import build_master_spec_document, build_word_document
from csc_utils import (
    SECTION_BACKGROUND,
    SECTION_EXISTING_SPEC,
    SECTION_PROPOSED,
    SECTION_JUSTIFICATION,
    REC_MAIN_KEY,
    REC_REMARKS_KEY,
    ISSUE_TYPES,
    PARAMETER_TYPES,
    DEFAULT_PREPARED_BY,
    fmt_ist,
    SPEC_SUBSET_ORDER,
    parse_spec_number,
    sort_specs_by_subset_order,
)

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent
EXPORTS_DIR = BASE_DIR / "exports"
BACKUPS_DIR = BASE_DIR / "backups"
REPORT_LOGO_PATH = BASE_DIR / "ONGC Logo.jpeg"

# ── Stage display metadata (3 stages only) ────────────────────────────────────
_STAGE_META = {
    ADMIN_STAGE_INGESTED:    {"label": "Ingested",    "icon": "📥", "color": "#6B7280", "bg": "#F3F4F6"},
    ADMIN_STAGE_STRUCTURING: {"label": "Structuring", "icon": "✏️", "color": "#92400E", "bg": "#FEF3C7"},
    ADMIN_STAGE_PUBLISHED:   {"label": "Published",   "icon": "✅", "color": "#065F46", "bg": "#D1FAE5"},
}

_STAGE_ORDER = [
    ADMIN_STAGE_INGESTED,
    ADMIN_STAGE_STRUCTURING,
    ADMIN_STAGE_PUBLISHED,
]

_PARAM_SL_COL = "sl_no"
_PARAM_MOVE_COL = "move_row"
_PARAM_CELL_MAX_CHARS = 10_000
_PARAM_EDITOR_ROW_HEIGHT = 58
_PARAM_TEXT_FIELDS = [
    "parameter_name",
    "parameter_type",
    "existing_value",
    "test_procedure_type",
    "test_procedure_text",
]
_TEST_PROCEDURE_TYPE_OPTIONS = [
    "ASTM",
    "API",
    "IS",
    "Inhouse",
    "Other National / International",
]
_PDF_PARSE_CACHE_VER = "v3"

# ── CSS ───────────────────────────────────────────────────────────────────────
_CSS = """
<style>
.adm-section-hdr {
    font-size:1.05rem;font-weight:800;color:#A54B23;
    border-bottom:2px solid #A54B23;padding-bottom:.3rem;margin:1.2rem 0 .8rem;
}
.adm-spec-card {
    border:1px solid #E5E7EB;border-left:4px solid #A54B23;border-radius:8px;
    padding:0.7rem 1rem;margin:0.4rem 0;background:#FFFCFA;
}
.adm-spec-no   {font-size:0.72rem;font-weight:800;letter-spacing:.07em;text-transform:uppercase;color:#7A3318;}
.adm-spec-name {font-size:0.95rem;font-weight:700;color:#1F2937;margin:.15rem 0;}
.adm-spec-ver  {font-size:0.72rem;font-weight:700;color:#1E40AF;margin:.1rem 0;}
.adm-stage-pill {
    display:inline-block;padding:3px 12px;border-radius:999px;
    font-size:0.72rem;font-weight:700;letter-spacing:.04em;
}
.adm-pipeline-step {
    border-radius:8px;padding:0.6rem 1rem;text-align:center;
    font-size:0.8rem;font-weight:700;margin:0 2px;
}
/* Keep wide parameter editors horizontally scrollable. */
div[data-testid="stDataEditor"] {
    overflow-x: auto !important;
}
div[data-testid="stDataEditor"] [role="grid"] {
    min-width: 1400px !important;
}
</style>
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _stage_pill(stage: str) -> str:
    m = _STAGE_META.get(stage, _STAGE_META[ADMIN_STAGE_INGESTED])
    return (
        f'<span class="adm-stage-pill" '
        f'style="background:{m["bg"]};color:{m["color"]};">'
        f'{m["icon"]} {m["label"]}</span>'
    )


def _pipeline_strip(current_stage: str) -> str:
    """Render a visual 3-step pipeline indicator."""
    parts = []
    for stage in _STAGE_ORDER:
        m = _STAGE_META[stage]
        if stage == current_stage:
            style = f"background:{m['bg']};color:{m['color']};border:2px solid {m['color']};"
        elif _STAGE_ORDER.index(stage) < _STAGE_ORDER.index(current_stage):
            style = "background:#D1FAE5;color:#065F46;border:2px solid #6EE7B7;"
        else:
            style = "background:#F9FAFB;color:#9CA3AF;border:2px solid #E5E7EB;"
        parts.append(
            f'<div class="adm-pipeline-step" style="{style}">'
            f'{m["icon"]}&nbsp;{m["label"]}</div>'
        )
    return (
        '<div style="display:flex;gap:4px;margin:.6rem 0 1rem;">'
        + "".join(parts)
        + "</div>"
    )


def _prepare_param_editor_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure row-move controls and serial numbers exist for the editor."""
    work = df.copy().reset_index(drop=True)
    for col in _PARAM_TEXT_FIELDS:
        if col in work.columns:
            work[col] = work[col].fillna("").astype(str)
    if _PARAM_MOVE_COL not in work.columns:
        work.insert(0, _PARAM_MOVE_COL, False)
    else:
        work[_PARAM_MOVE_COL] = work[_PARAM_MOVE_COL].fillna(False).astype(bool)
    if _PARAM_SL_COL not in work.columns:
        work.insert(0, _PARAM_SL_COL, 0)
    work[_PARAM_SL_COL] = list(range(1, len(work) + 1))
    return work


def _move_selected_parameter_row(
    edited_df: pd.DataFrame,
    direction: int,
) -> tuple[pd.DataFrame | None, str | None]:
    selected_idx = [
        int(i)
        for i, selected in enumerate(edited_df.get(_PARAM_MOVE_COL, []))
        if bool(selected)
    ]
    if len(selected_idx) != 1:
        return None, "Select exactly one row in 'Move' to reorder."

    src = selected_idx[0]
    dst = src + direction
    if dst < 0 or dst >= len(edited_df):
        edge = "top" if direction < 0 else "bottom"
        return None, f"Selected row is already at the {edge}."

    rows = edited_df.to_dict(orient="records")
    rows[src], rows[dst] = rows[dst], rows[src]
    for row in rows:
        row[_PARAM_MOVE_COL] = False
    return _prepare_param_editor_df(pd.DataFrame(rows)), None


def _param_editor_height(row_count: int) -> int:
    """Return a dynamic data_editor height that shows most/all rows."""
    visible_rows = max(8, int(row_count) + 1)
    # Keep sane bounds while avoiding constant inner scrolling for typical specs.
    return max(420, min(10000, visible_rows * _PARAM_EDITOR_ROW_HEIGHT + 64))


def _resolve_data_editor_state_df(
    base_df: pd.DataFrame,
    widget_state: Any,
) -> pd.DataFrame:
    """Resolve Streamlit data_editor delta-state into a concrete DataFrame."""
    if isinstance(widget_state, pd.DataFrame):
        return _prepare_param_editor_df(widget_state)
    if not isinstance(widget_state, dict):
        return _prepare_param_editor_df(base_df)

    rows = _prepare_param_editor_df(base_df).to_dict(orient="records")

    edited_rows = widget_state.get("edited_rows", {}) or {}
    for idx_raw, changes in edited_rows.items():
        try:
            idx = int(idx_raw)
        except Exception:
            continue
        if idx < 0 or idx >= len(rows) or not isinstance(changes, dict):
            continue
        for col, val in changes.items():
            rows[idx][str(col)] = val

    deleted_rows = widget_state.get("deleted_rows", []) or []
    for idx in sorted(
        [int(i) for i in deleted_rows if isinstance(i, int) or (isinstance(i, str) and str(i).isdigit())],
        reverse=True,
    ):
        if 0 <= idx < len(rows):
            rows.pop(idx)

    added_rows = widget_state.get("added_rows", []) or []
    for added in added_rows:
        if isinstance(added, dict):
            rows.append(dict(added))

    return _prepare_param_editor_df(pd.DataFrame(rows))


def _sync_param_editor_state(editor_key: str, state_key: str) -> None:
    """On-change callback: persist the latest data_editor delta into table state."""
    base_df = st.session_state.get(state_key)
    if not isinstance(base_df, pd.DataFrame):
        base_df = pd.DataFrame()
    st.session_state[state_key] = _resolve_data_editor_state_df(
        _prepare_param_editor_df(base_df),
        st.session_state.get(editor_key),
    )


def _df_to_params(df: pd.DataFrame) -> list[dict]:
    """Convert a data_editor DataFrame to parameter dicts (master-data columns only)."""
    def _txt(row: pd.Series, key: str, default: str = "") -> str:
        val = row.get(key, default)
        if pd.isna(val):
            return default
        return str(val or default)

    params = []
    for _, row in df.iterrows():
        name = _txt(row, "parameter_name").strip()
        if not name:
            continue
        params.append({
            "parameter_name": name,
            "parameter_type": _txt(row, "parameter_type", "Essential"),
            "existing_value": _txt(row, "existing_value"),
            "proposed_value": _txt(row, "existing_value"),  # keep in sync
            "test_method":    _txt(row, "test_procedure_text", _txt(row, "test_method")),
            "test_procedure_type": normalize_test_procedure_type(_txt(row, "test_procedure_type")),
            "test_procedure_text": _txt(row, "test_procedure_text"),
            # explicitly blank — not used in master data
            "justification":  "",
            "remarks":        "",
        })
    return params


# ── Public entry point ────────────────────────────────────────────────────────

def render_admin_workspace(repo: CSCRepository, user_name: str) -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
    st.markdown(
        '<div class="adm-section-hdr">🛠&nbsp; Member Secretary — Master Data Workspace</div>',
        unsafe_allow_html=True,
    )

    tab_ingest, tab_pipeline, tab_export, tab_backup = st.tabs([
        "📥  Stage 1 — Ingest Files",
        "✏️  Stage 2 — Structure & Publish",
        "📤  Master Export",
        "🗄️  Backup & Recovery",
    ])

    with tab_ingest:
        _render_ingest(repo, user_name)

    with tab_pipeline:
        _render_pipeline(repo, user_name)

    with tab_export:
        _render_master_export(repo, user_name)

    with tab_backup:
        _render_backup_recovery(repo, user_name)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — INGEST
# ═══════════════════════════════════════════════════════════════════════════════

def _render_ingest(repo: CSCRepository, user_name: str) -> None:
    st.markdown("#### Stage 1 — Ingest Specification Files")
    st.info(
        "Upload ONGC spec PDFs or the master Corporate Specifications .docx. "
        "The system extracts metadata and parameters automatically. "
        "Fix any extraction errors in **Stage 2 — Structure**. "
        "New admin specs start at **v0** baseline.",
        icon="📥",
    )

    # ── Local folder PDF loader (recommended for large batches) ─────────────
    st.markdown("##### Bulk PDF Ingest from Local Folder")
    st.caption(
        "Use this for large batches to avoid browser upload limits "
        "(the multi-file uploader may stop around 60-70 PDFs depending on total size)."
    )
    default_pdf_dir = str((BASE_DIR / "generated_pdfs").resolve())
    with st.form("adm_pdf_folder_form"):
        folder_path = st.text_input(
            "Folder path containing PDF files",
            value=st.session_state.get("adm_pdf_folder_path", default_pdf_dir),
        )
        include_subfolders = st.checkbox("Include subfolders", value=False)
        load_folder = st.form_submit_button("Load PDFs from Folder")

    if load_folder:
        st.session_state["adm_pdf_folder_path"] = folder_path
        target = Path(folder_path.strip()).expanduser()
        if not target.exists() or not target.is_dir():
            st.error(f"Folder not found: {target}")
        else:
            pdf_paths = sorted(target.rglob("*.pdf") if include_subfolders else target.glob("*.pdf"))
            if not pdf_paths:
                st.warning("No PDF files found in the selected folder.")
                st.session_state["adm_folder_parsed_specs"] = []
            else:
                loaded_specs: list[dict[str, Any]] = []
                parse_progress = st.progress(0.0)
                parse_status = st.empty()
                from csc_pdf_extractor import extract_spec_from_pdf

                total_pdf = len(pdf_paths)
                for idx, pdf_path in enumerate(pdf_paths, start=1):
                    cache_key = (
                        f"adm_ext_{_PDF_PARSE_CACHE_VER}_{pdf_path.name}_"
                        f"{pdf_path.stat().st_size}_{int(pdf_path.stat().st_mtime)}"
                    )
                    parse_status.caption(
                        f"Parsing {idx}/{total_pdf}: {pdf_path.name} "
                        f"(loaded: {len(loaded_specs)})"
                    )
                    if cache_key not in st.session_state:
                        try:
                            st.session_state[cache_key] = extract_spec_from_pdf(pdf_path.read_bytes())
                        except Exception as exc:
                            logger.exception("Admin folder PDF extraction error: %s", exc)
                            st.session_state[cache_key] = None
                    spec_doc = st.session_state.get(cache_key)
                    if spec_doc:
                        loaded_specs.append({"file_name": pdf_path.name, "spec_doc": spec_doc})
                    parse_progress.progress(idx / total_pdf)
                st.session_state["adm_folder_parsed_specs"] = loaded_specs
                parse_status.success(
                    f"Loaded {len(loaded_specs)} / {total_pdf} PDF(s) from folder."
                )

    # ── PDF Upload ────────────────────────────────────────────────────────────
    st.markdown("---")
    uploaded_files = st.file_uploader(
        "Select one or more ONGC spec PDF files",
        type=["pdf"],
        accept_multiple_files=True,
        key="admin_pdf_upload",
        help="Upload multiple PDFs at once. Each becomes a separate spec.",
    )

    parsed_specs: list[dict[str, Any]] = []

    if uploaded_files:
        for uf in uploaded_files:
            cache_key = f"adm_ext_{_PDF_PARSE_CACHE_VER}_{uf.name}_{uf.size}"
            if cache_key not in st.session_state:
                with st.spinner(f"Extracting **{uf.name}**…"):
                    try:
                        from csc_pdf_extractor import extract_spec_from_pdf
                        spec_doc = extract_spec_from_pdf(uf.read())
                        st.session_state[cache_key] = spec_doc
                    except Exception as exc:
                        st.error(f"Failed to parse **{uf.name}**: {exc}")
                        logger.exception("Admin PDF extraction error: %s", exc)
                        st.session_state[cache_key] = None

            spec_doc = st.session_state.get(cache_key)
            if spec_doc:
                parsed_specs.append({"file_name": uf.name, "spec_doc": spec_doc})

    folder_specs = st.session_state.get("adm_folder_parsed_specs") or []
    if folder_specs:
        st.caption(f"Loaded from folder: **{len(folder_specs)}** PDF(s)")
        parsed_specs.extend(folder_specs)

    # Persistent PDF ingest queue: allows batched ingestion across multiple runs.
    queue_key = "adm_pdf_ingest_queue"
    queue_sig_key = "adm_pdf_ingest_queue_sig"
    source_sig = tuple(
        (
            str(item.get("file_name", "") or ""),
            str(getattr(item.get("spec_doc"), "spec_number", "") or ""),
            str(getattr(item.get("spec_doc"), "chemical_name", "") or ""),
            int(len(getattr(item.get("spec_doc"), "parameters", []) or [])),
        )
        for item in parsed_specs
    )
    if parsed_specs and st.session_state.get(queue_sig_key) != source_sig:
        st.session_state[queue_key] = list(parsed_specs)
        st.session_state[queue_sig_key] = source_sig

    # ── Manual creation (no PDF) ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**Or — create a blank spec manually (no PDF):**")
    with st.expander("➕  Create blank spec", expanded=False):
        with st.form("adm_blank_spec_form"):
            c1, c2 = st.columns(2)
            with c1:
                b_spec = st.text_input("Spec Number *", max_chars=200,
                                       placeholder="ONGC/DFC/XX/2026")
                b_chem = st.text_input("Chemical Name *", max_chars=300)
                b_tp   = st.text_input("Test Procedure", max_chars=300)
            with c2:
                b_mc   = st.text_input("Material Code", max_chars=100)
                b_prep = st.text_input("Prepared By", value=DEFAULT_PREPARED_BY, max_chars=200)
            b_submit = st.form_submit_button("Create Blank Spec", type="primary")
        if b_submit:
            if not b_spec.strip() or not b_chem.strip():
                st.error("Spec Number and Chemical Name are required.")
            else:
                subset, seq, year = parse_spec_number(b_spec.strip())
                if subset not in SPEC_SUBSET_ORDER or seq is None or year is None:
                    st.error(
                        "Spec Number must follow ONGC/<subset>/<number>/<year>, "
                        "for example ONGC/DFC/01/2026."
                    )
                else:
                    try:
                        repo.create_draft(
                            spec_number     = b_spec.strip(),
                            chemical_name   = b_chem.strip(),
                            committee_name  = "Chemical Specification Committee",
                            meeting_date    = "",
                            prepared_by     = b_prep.strip(),
                            reviewed_by     = "",
                            user_name       = user_name,
                            test_procedure  = b_tp.strip(),
                            material_code   = b_mc.strip(),
                            created_by_role = "Admin",
                            is_admin_draft  = True,
                            admin_stage     = ADMIN_STAGE_STRUCTURING,
                            spec_version_start = 0,
                        )
                        st.success(
                            f"Created blank spec **{b_spec.strip()}**. "
                            "Go to **Stage 2** to add parameters."
                        )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Failed: {exc}")

    # ── Bulk DOCX upload ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**Or — bulk ingest from master Corporate Specifications (.docx):**")
    uploaded_docx = st.file_uploader(
        "Upload master .docx",
        type=["docx"],
        key="admin_docx_upload",
        help="Parses all specifications in one .docx and ingests them to Stage 2.",
    )
    if uploaded_docx is not None:
        docx_cache_key = f"adm_docx_ext_v1_{uploaded_docx.name}_{uploaded_docx.size}"
        if docx_cache_key not in st.session_state:
            with st.spinner(f"Extracting specifications from **{uploaded_docx.name}**…"):
                try:
                    from csc_docx_extractor import extract_all_specs_from_docx
                    st.session_state[docx_cache_key] = extract_all_specs_from_docx(uploaded_docx.getvalue())
                except Exception as exc:
                    st.error(f"DOCX extraction failed: {exc}")
                    logger.exception("Admin DOCX extraction error: %s", exc)
                    st.session_state[docx_cache_key] = None

        docx_specs = st.session_state.get(docx_cache_key) or []
        if docx_specs:
            total_params = sum(len(s.parameters) for s in docx_specs)
            total_warn = sum(len(s.parse_warnings) for s in docx_specs)
            st.caption(
                f"Parsed {len(docx_specs)} specification(s), {total_params} parameter row(s), "
                f"{total_warn} warning(s)."
            )
            preview_rows = [
                {
                    "Spec No.": s.spec_number or "—",
                    "Chemical": s.chemical_name or "—",
                    "Parameters": int(len(s.parameters)),
                    "Warnings": int(len(s.parse_warnings)),
                }
                for s in docx_specs
            ]
            st.dataframe(pd.DataFrame(preview_rows), use_container_width=True, hide_index=True)

            with st.form("admin_docx_ingest_form"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    docx_prepared_by = st.text_input(
                        "Prepared By (for DOCX ingest)",
                        value=DEFAULT_PREPARED_BY,
                        max_chars=200,
                    )
                with c2:
                    docx_skip_existing = st.checkbox(
                        "Skip existing spec numbers",
                        value=True,
                    )
                with c3:
                    docx_overwrite_existing = st.checkbox(
                        "Overwrite existing specs",
                        value=False,
                        help="Deletes existing draft with same Spec Number and re-ingests.",
                    )
                docx_submit = st.form_submit_button(
                    f"📥  Ingest {len(docx_specs)} Spec(s) from DOCX → Stage 2",
                    type="primary",
                )

            if docx_submit:
                existing_map = {
                    str(d.get("spec_number", "") or "").strip(): str(d.get("draft_id", "") or "")
                    for d in repo.list_drafts(admin_only=True)
                    if str(d.get("spec_number", "") or "").strip()
                }
                from csc_docx_extractor import spec_to_parameter_dicts

                created, skipped, overwritten, errors = 0, 0, 0, 0
                total_docx_specs = len(docx_specs)
                docx_progress = st.progress(0.0)
                docx_status = st.empty()
                for idx, spec in enumerate(docx_specs, start=1):
                    spec_no = str(spec.spec_number or "").strip()
                    chemical = str(spec.chemical_name or "").strip()
                    spec_label = spec_no or "(no spec number)"
                    docx_status.caption(
                        f"Ingesting DOCX spec {idx}/{total_docx_specs}: {spec_label} "
                        f"(created={created}, skipped={skipped}, overwritten={overwritten}, errors={errors})"
                    )

                    if not chemical:
                        skipped += 1
                        st.warning(f"Skipped {spec_label}: chemical name missing.")
                        docx_progress.progress(idx / total_docx_specs)
                        continue

                    if spec_no.upper().startswith("NOTE:") or spec_no.upper().startswith("NOTE "):
                        skipped += 1
                        st.info(f"Skipped {spec_label}: procurement note entry.")
                        docx_progress.progress(idx / total_docx_specs)
                        continue

                    if spec_no and spec_no in existing_map:
                        if docx_overwrite_existing:
                            try:
                                repo.delete_draft(existing_map[spec_no], user_name)
                                overwritten += 1
                            except Exception as exc:
                                errors += 1
                                st.error(f"Failed to overwrite {spec_no}: {exc}")
                                docx_progress.progress(idx / total_docx_specs)
                                continue
                        elif docx_skip_existing:
                            skipped += 1
                            docx_progress.progress(idx / total_docx_specs)
                            continue

                    try:
                        draft_id = repo.create_draft(
                            spec_number=spec_no or f"UNKNOWN_{created + skipped + errors + 1}",
                            chemical_name=chemical,
                            committee_name="Chemical Specification Committee",
                            meeting_date="",
                            prepared_by=docx_prepared_by.strip(),
                            reviewed_by="",
                            user_name=user_name,
                            test_procedure=str(spec.test_procedure or ""),
                            material_code=str(spec.material_code or ""),
                            created_by_role="Admin",
                            is_admin_draft=True,
                            admin_stage=ADMIN_STAGE_STRUCTURING,
                            spec_version_start=0,
                        )
                        if spec.parameters:
                            repo.upsert_parameters(draft_id, spec_to_parameter_dicts(spec), user_name)
                        repo.capture_admin_version_snapshot(
                            draft_id=draft_id,
                            user_name=user_name,
                            source_action="ADMIN_DOCX_IMPORT",
                            remarks=f"Initial baseline from {uploaded_docx.name}",
                            force_version=0,
                            replace_existing=True,
                        )
                        repo.log_audit(
                            draft_id,
                            "ADMIN_DOCX_IMPORT",
                            user_name,
                            new_value=f"{len(spec.parameters)} params from {uploaded_docx.name}",
                        )
                        if spec_no:
                            existing_map[spec_no] = draft_id
                        created += 1
                    except Exception as exc:
                        errors += 1
                        st.error(f"Failed to ingest {spec_label}: {exc}")
                        logger.exception("Admin DOCX ingest failed for %s: %s", spec_label, exc)
                    docx_progress.progress(idx / total_docx_specs)

                if created:
                    docx_status.success(
                        f"DOCX ingest complete: created={created}, skipped={skipped}, "
                        f"overwritten={overwritten}, errors={errors}."
                    )
                    st.success(
                        f"✅ DOCX ingest complete: {created} created, {skipped} skipped, "
                        f"{overwritten} overwritten, {errors} errors."
                    )
                    st.rerun()
                elif skipped and not errors:
                    docx_status.info(
                        f"DOCX ingest finished with no creates: skipped={skipped}, overwritten={overwritten}."
                    )
                    st.info(f"DOCX ingest skipped {skipped} existing/non-ingestable entries.")
                elif errors:
                    docx_status.warning(
                        f"DOCX ingest finished with errors: created={created}, skipped={skipped}, "
                        f"overwritten={overwritten}, errors={errors}."
                    )

    ingest_queue = st.session_state.get(queue_key) or []
    if not ingest_queue:
        if not parsed_specs:
            return
        ingest_queue = list(parsed_specs)
        st.session_state[queue_key] = ingest_queue

    # ── Show pending queue ───────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"**PDF ingest queue: {len(ingest_queue)} pending**")
    preview_queue = [
        {
            "Spec No.": str((item.get("spec_doc") and item["spec_doc"].spec_number) or "—"),
            "Chemical": str((item.get("spec_doc") and item["spec_doc"].chemical_name) or "—"),
            "Parameters": int(len((item.get("spec_doc") and item["spec_doc"].parameters) or [])),
            "File": str(item.get("file_name", "") or ""),
        }
        for item in ingest_queue[:200]
    ]
    if preview_queue:
        st.dataframe(pd.DataFrame(preview_queue), use_container_width=True, hide_index=True)
        if len(ingest_queue) > len(preview_queue):
            st.caption(f"Showing first {len(preview_queue)} pending items.")

    col_reset_q, col_clear_q = st.columns(2)
    with col_reset_q:
        if st.button("Reset Queue from Loaded PDFs", key="adm_reset_pdf_queue"):
            st.session_state[queue_key] = list(parsed_specs)
            st.session_state[queue_sig_key] = source_sig
            st.success(f"Queue reset to {len(parsed_specs)} loaded PDF(s).")
            st.rerun()
    with col_clear_q:
        if st.button("Clear Queue", key="adm_clear_pdf_queue"):
            st.session_state[queue_key] = []
            st.success("PDF ingest queue cleared.")
            st.rerun()

    # ── Ingest settings form ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("##### PDF Ingest Settings (Batch Mode)")
    with st.form("admin_ingest_form_batch"):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            prepared_by = st.text_input(
                "Prepared By (Member Secretary)",
                max_chars=200,
                value=DEFAULT_PREPARED_BY,
            )
        with c2:
            skip_existing = st.checkbox(
                "Skip existing spec numbers",
                value=True,
            )
        with c3:
            overwrite_existing = st.checkbox(
                "Overwrite existing specs",
                value=False,
                help="Deletes existing draft with same Spec Number, then re-ingests.",
            )
        with c4:
            batch_size = int(
                st.number_input(
                    "Batch size",
                    min_value=1,
                    max_value=500,
                    value=50,
                    step=1,
                )
            )

        ingest_now = st.form_submit_button(
            f"📥  Ingest Next Batch ({min(batch_size, len(ingest_queue))}) → Stage 2",
            type="primary",
        )

    if not ingest_now:
        return

    # ── Run batch ingestion ──────────────────────────────────────────────────
    existing_map = {
        str(d.get("spec_number", "") or "").strip(): str(d.get("draft_id", "") or "")
        for d in repo.list_drafts(admin_only=True)
        if str(d.get("spec_number", "") or "").strip()
    }
    from csc_pdf_extractor import spec_to_parameter_dicts

    batch = ingest_queue[:batch_size]
    created, skipped, overwritten, errors = 0, 0, 0, 0
    failed_items: list[dict[str, Any]] = []
    ingest_progress = st.progress(0.0)
    ingest_status = st.empty()
    total_specs = len(batch)

    for idx, item in enumerate(batch, start=1):
        spec_doc = item["spec_doc"]
        spec_no = str(spec_doc.spec_number or "").strip()
        chemical = str(spec_doc.chemical_name or "").strip()
        spec_label = spec_no or str(item.get("file_name", "") or f"Item {idx}")
        ingest_status.caption(
            f"Ingesting {idx}/{total_specs}: {spec_label}  "
            f"(created={created}, skipped={skipped}, overwritten={overwritten}, errors={errors})"
        )

        if not chemical:
            skipped += 1
            st.warning(f"Skipped {spec_label}: chemical name missing.")
            ingest_progress.progress(idx / total_specs)
            continue

        if spec_no and spec_no in existing_map:
            if overwrite_existing:
                try:
                    repo.delete_draft(existing_map[spec_no], user_name)
                    overwritten += 1
                except Exception as exc:
                    errors += 1
                    failed_items.append(item)
                    st.error(f"Failed to overwrite {spec_no}: {exc}")
                    logger.exception("Admin overwrite before PDF ingest failed: %s", exc)
                    ingest_progress.progress(idx / total_specs)
                    continue
            elif skip_existing:
                skipped += 1
                ingest_progress.progress(idx / total_specs)
                continue

        try:
            draft_id = repo.create_draft(
                spec_number=spec_no or f"UNKNOWN_{item['file_name']}",
                chemical_name=chemical or "Unknown Chemical",
                committee_name="Chemical Specification Committee",
                meeting_date="",
                prepared_by=prepared_by.strip(),
                reviewed_by="",
                user_name=user_name,
                test_procedure=spec_doc.test_procedure or "",
                material_code=spec_doc.material_code or "",
                created_by_role="Admin",
                is_admin_draft=True,
                admin_stage=ADMIN_STAGE_STRUCTURING,
                spec_version_start=0,
            )
            if spec_doc.parameters:
                params = spec_to_parameter_dicts(spec_doc)
                repo.upsert_parameters(draft_id, params, user_name)
            repo.capture_admin_version_snapshot(
                draft_id=draft_id,
                user_name=user_name,
                source_action="ADMIN_PDF_IMPORT",
                remarks=f"Initial baseline from {item['file_name']}",
                force_version=0,
                replace_existing=True,
            )
            repo.log_audit(
                draft_id,
                "ADMIN_PDF_IMPORT",
                user_name,
                new_value=f"{len(spec_doc.parameters)} params from {item['file_name']}",
            )
            if spec_no:
                existing_map[spec_no] = draft_id
            created += 1
        except Exception as exc:
            errors += 1
            failed_items.append(item)
            st.error(f"Failed to create draft for **{spec_label}**: {exc}")
            logger.exception("Admin PDF batch ingest failed: %s", exc)

        ingest_progress.progress(idx / total_specs)

    # Remove processed items from queue; keep only failures for retry.
    st.session_state[queue_key] = failed_items + ingest_queue[batch_size:]
    pending_after = len(st.session_state.get(queue_key) or [])

    ingest_status.success(
        f"Batch complete: created={created}, skipped={skipped}, overwritten={overwritten}, "
        f"errors={errors}, pending={pending_after}."
    )
    st.success(
        "✅ PDF batch ingest complete."
        + f" Created {created}."
        + (f" Skipped {skipped}." if skipped else "")
        + (f" Overwritten {overwritten}." if overwritten else "")
        + (f" Errors {errors}." if errors else "")
        + f" Pending queue: {pending_after}."
    )
    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PIPELINE (Stage 2: Structure  →  Stage 3: Published)
# ═══════════════════════════════════════════════════════════════════════════════

def _render_pipeline(repo: CSCRepository, user_name: str) -> None:
    summaries = repo.get_admin_draft_summary()
    pending_revisions = repo.get_pending_revision_summary()

    if not summaries and not pending_revisions:
        st.info(
            "No specs yet. Use **Stage 1 — Ingest Files** to create them.",
            icon="📥",
        )
        return

    # ── Pending approval queue (committee revisions) ─────────────────────────
    st.markdown("#### ⏳ Pending Committee Revisions")
    if pending_revisions:
        actionable_count = sum(1 for r in pending_revisions if (r.get("status") or "").strip() == "Pending Approval")
        st.caption(
            f"{len(pending_revisions)} revision draft(s) in queue "
            f"({actionable_count} awaiting admin approval)."
        )
        q_rows = []
        for r in pending_revisions:
            status = (r.get("status") or "").strip() or "Draft"
            parent_state = "Missing" if int(r.get("parent_missing", 0) or 0) == 1 else "OK"
            q_rows.append({
                "Spec No.": r.get("spec_number", "—"),
                "Chemical": r.get("chemical_name", "—"),
                "Requested By": r.get("created_by_user", "—"),
                "Queue Status": status,
                "Parent": parent_state,
                "Changed Params": int(r.get("changed_count", 0) or 0),
                "Total Params": int(r.get("param_count", 0) or 0),
                "Submitted": fmt_ist(r.get("submitted_for_approval_at", "")),
            })
        st.dataframe(pd.DataFrame(q_rows), use_container_width=True, hide_index=True)

        pending_options = {
            (
                f"{r.get('spec_number','—')} — {r.get('chemical_name','—')}  "
                f"[{r.get('created_by_user','User')} | {(r.get('status') or 'Draft')}"
                f"{' | Parent Missing' if int(r.get('parent_missing', 0) or 0) == 1 else ''}]"
            ): r["draft_id"]
            for r in pending_revisions
        }
        sel_pending = st.selectbox(
            "Open pending revision:",
            list(pending_options.keys()),
            key="adm_pending_select",
        )
        if sel_pending:
            rev_id = pending_options[sel_pending]
            rev = repo.load_draft(rev_id)
            if rev:
                _render_pending_revision(repo, rev, user_name)
        st.markdown("---")
    else:
        st.caption("No pending revisions right now.")
        st.markdown("---")

    if not summaries:
        return

    # ── Overview dashboard ────────────────────────────────────────────────────
    stage_counts = {s: 0 for s in _STAGE_ORDER}
    for d in summaries:
        raw_stage = d.get("admin_stage", ADMIN_STAGE_INGESTED)
        # 'reviewing' is legacy — treat as structuring in dashboard
        display_stage = (
            ADMIN_STAGE_STRUCTURING
            if raw_stage == "reviewing"
            else raw_stage
        )
        stage_counts[display_stage] = stage_counts.get(display_stage, 0) + 1

    cols = st.columns(3)
    for i, stage in enumerate(_STAGE_ORDER):
        m = _STAGE_META[stage]
        with cols[i]:
            st.markdown(
                f'<div style="background:{m["bg"]};border-radius:10px;padding:.7rem 1rem;'
                f'text-align:center;border:1.5px solid {m["color"]}20;">'
                f'<div style="font-size:1.5rem;">{m["icon"]}</div>'
                f'<div style="font-size:1.4rem;font-weight:800;color:{m["color"]};">'
                f'{stage_counts.get(stage, 0)}</div>'
                f'<div style="font-size:.72rem;font-weight:700;color:{m["color"]};'
                f'text-transform:uppercase;letter-spacing:.05em;">{m["label"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("")

    # ── Spec selector (filter by stage) ──────────────────────────────────────
    stage_filter = st.radio(
        "Show specs in stage:",
        ["All"] + [_STAGE_META[s]["label"] for s in _STAGE_ORDER],
        horizontal=True,
        key="adm_stage_filter",
    )

    label_to_stage = {_STAGE_META[s]["label"]: s for s in _STAGE_ORDER}

    if stage_filter == "All":
        filtered = summaries
    else:
        target = label_to_stage.get(stage_filter)
        filtered = [
            d for d in summaries
            if _normalise_stage(d.get("admin_stage", ADMIN_STAGE_INGESTED)) == target
        ]

    if not filtered:
        st.info("No specs in this stage yet.")
        return

    # Summary table
    rows = []
    for d in filtered:
        display_stage = _normalise_stage(d.get("admin_stage", ADMIN_STAGE_INGESTED))
        m = _STAGE_META.get(display_stage, _STAGE_META[ADMIN_STAGE_INGESTED])
        rows.append({
            "Spec No.":     d["spec_number"],
            "Chemical":     d["chemical_name"],
            "Params":       d.get("param_count", 0),
            "Version":      f"v{d.get('spec_version', 0)}",
            "Stage":        f"{m['icon']} {m['label']}",
            "Last Updated": fmt_ist(d.get("updated_at", "")),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Select one spec to work on ────────────────────────────────────────────
    spec_options = {
        f"{d['spec_number']} — {d['chemical_name']}  [v{d.get('spec_version',0)}]": d["draft_id"]
        for d in filtered
    }
    selected_label = st.selectbox(
        "Select a spec to open:",
        list(spec_options.keys()),
        key="adm_pipeline_select",
    )
    if not selected_label:
        return

    draft_id = spec_options[selected_label]
    draft    = repo.load_draft(draft_id)
    if not draft:
        st.error("Draft not found.")
        return

    stage = _normalise_stage(draft.get("admin_stage", ADMIN_STAGE_STRUCTURING))

    st.markdown(_pipeline_strip(stage), unsafe_allow_html=True)

    if stage == ADMIN_STAGE_INGESTED:
        _render_stage_ingested(repo, draft, user_name)
    elif stage == ADMIN_STAGE_STRUCTURING:
        _render_stage_structuring(repo, draft, user_name)
    elif stage == ADMIN_STAGE_PUBLISHED:
        _render_stage_published(repo, draft, user_name)

    if stage in {ADMIN_STAGE_STRUCTURING, ADMIN_STAGE_PUBLISHED}:
        st.markdown("---")
        _render_version_history_panel(repo, draft, user_name)


def _normalise_stage(stage: str) -> str:
    """Map legacy 'reviewing' stage to 'structuring' for display."""
    if stage == "reviewing":
        return ADMIN_STAGE_STRUCTURING
    return stage if stage in _STAGE_META else ADMIN_STAGE_STRUCTURING


def _render_version_history_panel(repo: CSCRepository, draft: dict, user_name: str) -> None:
    """Render compare + restore controls for admin spec versions."""
    draft_id = draft["draft_id"]
    st.markdown("#### 🕓 Version History, Compare & Restore")

    history = repo.get_spec_version_history(draft_id)
    if not history:
        st.caption("No version snapshots available yet.")
        return

    timeline = " -> ".join(f"v{int(h.get('spec_version', 0) or 0)}" for h in history)
    st.caption(f"Timeline: {timeline}")

    hist_df = pd.DataFrame(
        [
            {
                "Version": f"v{int(h.get('spec_version', 0) or 0)}",
                "Captured": fmt_ist(h.get("created_at", "")),
                "By": h.get("created_by", "") or "system",
                "Source": h.get("source_action", "") or "-",
                "Parameters": int(h.get("parameter_count", 0) or 0),
                "Remarks": h.get("remarks", "") or "-",
            }
            for h in history
        ]
    )
    st.dataframe(hist_df, use_container_width=True, hide_index=True)

    versions = [int(h.get("spec_version", 0) or 0) for h in history]
    latest_version = max(versions) if versions else 0

    st.markdown("##### Compare Versions")
    if len(versions) < 2:
        st.caption("At least two versions are required for comparison.")
    else:
        default_from = versions[-2]
        default_to = versions[-1]
        col_from, col_to = st.columns(2)
        with col_from:
            from_ver = st.selectbox(
                "From version",
                options=versions,
                index=versions.index(default_from),
                format_func=lambda v: f"v{v}",
                key=f"adm_ver_from_{draft_id}",
            )
        with col_to:
            to_ver = st.selectbox(
                "To version",
                options=versions,
                index=versions.index(default_to),
                format_func=lambda v: f"v{v}",
                key=f"adm_ver_to_{draft_id}",
            )

        if int(from_ver) == int(to_ver):
            st.info("Select two different versions to compare.")
        else:
            diff = repo.compare_spec_versions(draft_id, int(from_ver), int(to_ver))
            if not diff:
                st.error("Unable to compute version diff for the selected pair.")
            else:
                summary = diff.get("summary", {}) or {}
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Added", int(summary.get("added", 0) or 0))
                c2.metric("Removed", int(summary.get("removed", 0) or 0))
                c3.metric("Changed", int(summary.get("changed", 0) or 0))
                c4.metric("Unchanged", int(summary.get("unchanged", 0) or 0))
                c5.metric("Metadata", int(summary.get("metadata_changed", 0) or 0))

                metadata_changes = diff.get("metadata_changes", []) or []
                if metadata_changes:
                    st.markdown("**Metadata Changes**")
                    st.dataframe(
                        pd.DataFrame(
                            [
                                {
                                    "Field": r.get("field", ""),
                                    f"v{from_ver}": r.get("from", ""),
                                    f"v{to_ver}": r.get("to", ""),
                                }
                                for r in metadata_changes
                            ]
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

                param_changes = diff.get("parameter_changes", {}) or {}
                added_rows = param_changes.get("added", []) or []
                removed_rows = param_changes.get("removed", []) or []
                changed_rows = param_changes.get("changed", []) or []

                if added_rows:
                    st.markdown(f"**Added Parameters (v{to_ver})**")
                    st.dataframe(
                        pd.DataFrame(
                            [
                                {
                                    "Parameter": r.get("parameter_name", ""),
                                    "Type": r.get("parameter_type", ""),
                                    "Requirement": r.get("existing_value", ""),
                                    "Test Procedure Type": r.get("test_procedure_type", ""),
                                    "Test Procedure Text": (
                                        r.get("test_procedure_text", "")
                                        or r.get("test_method", "")
                                    ),
                                }
                                for r in added_rows
                            ]
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
                if removed_rows:
                    st.markdown(f"**Removed Parameters (from v{from_ver})**")
                    st.dataframe(
                        pd.DataFrame(
                            [
                                {
                                    "Parameter": r.get("parameter_name", ""),
                                    "Type": r.get("parameter_type", ""),
                                    "Requirement": r.get("existing_value", ""),
                                    "Test Procedure Type": r.get("test_procedure_type", ""),
                                    "Test Procedure Text": (
                                        r.get("test_procedure_text", "")
                                        or r.get("test_method", "")
                                    ),
                                }
                                for r in removed_rows
                            ]
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
                if changed_rows:
                    st.markdown("**Changed Parameter Fields**")
                    flattened: list[dict[str, Any]] = []
                    for row in changed_rows:
                        for fc in row.get("field_changes", []) or []:
                            flattened.append(
                                {
                                    "Parameter": row.get("parameter_name", ""),
                                    "Test Procedure Text": (
                                        row.get("test_procedure_text", "")
                                        or row.get("test_method", "")
                                    ),
                                    "Field": fc.get("field", ""),
                                    f"v{from_ver}": fc.get("from", ""),
                                    f"v{to_ver}": fc.get("to", ""),
                                }
                            )
                    if flattened:
                        st.dataframe(pd.DataFrame(flattened), use_container_width=True, hide_index=True)

    st.markdown("##### Restore")
    restorable_versions = [v for v in versions if v < latest_version]
    if not restorable_versions:
        st.caption("No previous version available to restore.")
        return

    restore_col1, restore_col2 = st.columns([2, 3])
    with restore_col1:
        restore_target = st.selectbox(
            "Restore from version",
            options=restorable_versions,
            index=len(restorable_versions) - 1,
            format_func=lambda v: f"v{v}",
            key=f"adm_restore_target_{draft_id}",
        )
    with restore_col2:
        restore_reason = st.text_input(
            "Reason (optional)",
            key=f"adm_restore_reason_{draft_id}",
            placeholder="Why are you restoring this version?",
        )
    restore_confirm = st.checkbox(
        f"I confirm restore from v{restore_target}. This will create v{latest_version + 1}.",
        key=f"adm_restore_confirm_{draft_id}",
    )
    if st.button(
        "Restore Selected Version",
        key=f"adm_restore_btn_{draft_id}",
        type="primary",
        disabled=not restore_confirm,
    ):
        new_ver = repo.restore_spec_to_version(
            draft_id=draft_id,
            target_version=int(restore_target),
            user_name=user_name,
            remarks=restore_reason,
        )
        if new_ver:
            st.success(f"Restored v{restore_target}. New active version is v{new_ver}.")
            st.rerun()
        else:
            st.error("Restore failed.")


# ── Stage editors ─────────────────────────────────────────────────────────────

def _render_pending_revision(repo: CSCRepository, revision: dict, user_name: str) -> None:
    """Render admin approve/reject controls for one pending committee revision."""
    rev_id = revision["draft_id"]
    rev_status = (revision.get("status") or "").strip() or "Draft"
    parent_id = revision.get("parent_draft_id")
    parent = repo.load_draft(parent_id) if parent_id else None
    if not parent:
        st.error(
            "Parent master specification not found for this revision. "
            "This is an orphaned committee draft and cannot be approved until the parent master spec exists.",
            icon="⚠️",
        )
        st.caption("You can either delete this orphaned draft or return it to committee for manual cleanup.")
        orphan_remarks = st.text_area(
            "Admin remarks (optional)",
            key=f"adm_orphan_remarks_{rev_id}",
            height=80,
            placeholder="Reason for delete/return...",
        )
        col_del, col_ret = st.columns(2)
        with col_del:
            if st.button(
                "Delete Orphan Draft",
                key=f"adm_orphan_del_{rev_id}",
                type="primary",
                use_container_width=True,
            ):
                ok = repo.delete_draft(rev_id, user_name)
                if ok:
                    st.success("Orphaned revision draft deleted.")
                    st.rerun()
                else:
                    st.error("Delete failed.")
        with col_ret:
            if st.button(
                "Return to Committee",
                key=f"adm_orphan_return_{rev_id}",
                use_container_width=True,
            ):
                ok = repo.return_revision_to_committee(
                    rev_id,
                    user_name,
                    remarks=orphan_remarks,
                )
                if ok:
                    st.success("Revision returned to committee as Draft.")
                    st.rerun()
                else:
                    st.error("Return action failed.")
        return

    st.markdown(
        f"##### Review Request | {revision.get('spec_number','-')}  "
        f"(requested by {revision.get('created_by_user','User')})"
    )
    st.caption(
        f"Base master version: v{parent.get('spec_version', 0)}  |  "
        f"Submitted: {fmt_ist(revision.get('submitted_for_approval_at', ''))}  |  "
        f"Status: {rev_status}"
    )

    rev_params = repo.load_parameters(rev_id)
    rev_sections = repo.load_sections(rev_id)
    rev_flags = repo.load_issue_flags(rev_id)
    rev_impact = repo.load_impact_analysis(rev_id)
    section_map = {s["section_name"]: s.get("section_text", "") or "" for s in rev_sections}

    with st.expander("Specification Review Committee - Draft Note (Review)", expanded=True):
        meta_df = pd.DataFrame([
            {"Field": "Specification No.", "Value": revision.get("spec_number", "-")},
            {"Field": "Chemical Name", "Value": revision.get("chemical_name", "-")},
            {"Field": "Prepared By", "Value": revision.get("prepared_by", "-")},
            {"Field": "Reviewed By", "Value": revision.get("reviewed_by", "-")},
            {"Field": "Meeting Date", "Value": revision.get("meeting_date", "-")},
        ])
        st.dataframe(meta_df, use_container_width=True, hide_index=True)

        text_sections = [
            ("1. Background", SECTION_BACKGROUND),
            ("2. Existing Specification Summary", SECTION_EXISTING_SPEC),
            ("4. Proposed Changes", SECTION_PROPOSED),
            ("5. Justification", SECTION_JUSTIFICATION),
        ]
        for title, key in text_sections:
            st.markdown(f"**{title}**")
            st.write((section_map.get(key, "") or "-").strip() or "-")

        st.markdown("**3. Issues Observed**")
        flag_map = {f["issue_type"]: f for f in rev_flags}
        issue_rows = []
        for issue_key, issue_label in ISSUE_TYPES:
            flg = flag_map.get(issue_key, {})
            issue_rows.append({
                "Issue": issue_label,
                "Present": "Yes" if bool(flg.get("is_present", 0)) else "No",
                "Note": (flg.get("note", "") or "-").strip() or "-",
            })
        st.dataframe(pd.DataFrame(issue_rows), use_container_width=True, hide_index=True)

        st.markdown("**6. Impact Analysis**")
        if rev_impact:
            st.markdown(f"- **Operational Impact Score:** {rev_impact.get('operational_impact_score', '—')}")
            st.markdown(f"- **Operational Impact Note:** {(rev_impact.get('operational_note', '') or '—').strip() or '—'}")
            st.markdown(f"- **Safety / Environmental Score:** {rev_impact.get('safety_environment_score', '—')}")
            st.markdown(f"- **Safety / Environmental Note:** {(rev_impact.get('safety_environment_note', '') or '—').strip() or '—'}")
            st.markdown(f"- **Supply Risk Score:** {rev_impact.get('supply_risk_score', '—')}")
            st.markdown(f"- **Supply Risk Note:** {(rev_impact.get('supply_risk_note', '') or '—').strip() or '—'}")
            st.markdown(
                f"- **No Substitute Available:** {'Yes' if int(rev_impact.get('no_substitute_flag', 0) or 0) == 1 else 'No'}"
            )
            st.markdown(
                f"- **Total Impact Score:** {float(rev_impact.get('impact_score_total', 0.0) or 0.0):.2f}"
            )
            st.markdown(f"- **Final Impact Grade:** {rev_impact.get('impact_grade', '—')}")
            if bool(rev_impact.get("override_applied")):
                st.markdown("- **Note:** No Substitute Available override applied.")
        else:
            st.caption("No structured impact analysis entered.")

        st.markdown("**7. Committee Recommendation**")
        st.write((section_map.get(REC_MAIN_KEY, "") or "-").strip() or "-")
        st.markdown("**Additional Remarks**")
        st.write((section_map.get(REC_REMARKS_KEY, "") or "-").strip() or "-")

        try:
            doc_bytes = build_word_document(
                revision,
                rev_sections,
                rev_params,
                rev_flags,
                impact_analysis=rev_impact,
            )
            date_str = datetime.now().strftime("%Y%m%d")
            spec_safe = "".join(
                c if c.isalnum() or c in "-_" else "_"
                for c in revision.get("spec_number", "SPEC")
            )
            st.download_button(
                label="Download Draft Note (.docx) for Review",
                data=doc_bytes,
                file_name=f"SRC_DRAFT_NOTE_{spec_safe}_{date_str}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"adm_pending_docx_{rev_id}",
            )
        except Exception as exc:
            st.error(f"Draft note export failed: {exc}")

    revision_changes = repo.get_revision_change_details(rev_id)
    changed_rows = []
    for c in revision_changes:
        change_bits = []
        if c.get("value_changed"):
            change_bits.append("Value")
        if c.get("type_changed"):
            change_bits.append("Type")
        changed_rows.append({
            "Parameter": c.get("parameter_name", ""),
            "Current Value": c.get("existing_value", "") or "-",
            "Proposed Value": c.get("proposed_value", "") or "-",
            "Current Type": c.get("parent_type", "") or "-",
            "Proposed Type": c.get("current_type", "") or "-",
            "Change In": ", ".join(change_bits) if change_bits else "-",
            "Justification": c.get("justification", "") or "-",
            "Remarks": c.get("remarks", "") or "-",
        })

    if changed_rows:
        st.markdown("**Changed Parameters**")
        st.dataframe(pd.DataFrame(changed_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No parameter value/type changes detected; approval will still publish latest revision data.")

    if rev_status != "Pending Approval":
        st.info(
            "This revision is not yet submitted by the committee for admin approval. "
            "Admin can review the draft content, but approve/reject actions are enabled only after submission.",
            icon="ℹ️",
        )
    else:
        remarks = st.text_area(
            "Admin remarks (optional)",
            key=f"adm_pending_remarks_{rev_id}",
            height=90,
            placeholder="Decision remarks for audit trail...",
        )
        review_confirmed = st.checkbox(
            "I have reviewed the Specification Review Committee - Draft Note.",
            key=f"adm_review_confirm_{rev_id}",
        )
        col_appr, col_rej = st.columns(2)
        with col_appr:
            if st.button(
                "Approve and Publish Version",
                key=f"adm_appr_{rev_id}",
                type="primary",
                use_container_width=True,
                disabled=not review_confirmed,
            ):
                new_ver = repo.approve_revision_request(rev_id, user_name, remarks=remarks)
                if new_ver:
                    st.success(f"Revision approved. Master specification updated to v{new_ver}.")
                    st.rerun()
                else:
                    st.error("Approval failed.")
        with col_rej:
            if st.button("Reject Revision", key=f"adm_rej_{rev_id}", use_container_width=True):
                ok = repo.reject_revision_request(rev_id, user_name, remarks=remarks)
                if ok:
                    st.warning("Revision rejected and returned to committee user.")
                    st.rerun()
                else:
                    st.error("Reject action failed (request may already be processed).")

    st.markdown("")
    with st.expander("Open full revision issue flags", expanded=False):
        if rev_flags:
            df_flags = pd.DataFrame(rev_flags)[["issue_type", "is_present", "note"]]
            st.dataframe(df_flags, use_container_width=True, hide_index=True)
        else:
            st.caption("No issue flags available.")


def _render_stage_ingested(
    repo: CSCRepository, draft: dict, user_name: str
) -> None:
    draft_id = draft["draft_id"]
    _spec_header(draft)
    st.info(
        "This spec was just ingested. Click below to start structuring the data.",
        icon="📥",
    )
    if st.button("✏️  Start Structuring →", type="primary", key=f"to_struct_{draft_id}"):
        repo.set_admin_stage(draft_id, ADMIN_STAGE_STRUCTURING, user_name)
        st.rerun()


def _render_stage_structuring(
    repo: CSCRepository, draft: dict, user_name: str
) -> None:
    """Stage 2 — MS corrects the parameter table. Version increments only on publish."""
    draft_id = draft["draft_id"]

    _spec_header(draft)
    _render_metadata_editor(repo, draft, user_name)

    st.markdown("##### 📝  Parameter Table")
    st.caption(
        "Edit parameter names, existing requirement values, types, "
        "test procedure type and test procedure text. "
        "Add or delete rows as needed. **Saving keeps version at v0; publishing moves v0 -> v1.**"
    )

    _render_param_editor(repo, draft_id, user_name)
    _render_impact_analysis_editor(repo, draft_id, user_name)

    st.markdown("---")
    col_a, col_b = st.columns([2, 3])
    with col_a:
        st.markdown("##### Ready to publish?")
        st.caption(
            "Publish when the parameter table is complete and accurate. "
            "Publishing increments the spec version (v0 -> v1, v1 -> v2, …) "
            "and makes the spec visible in the master export."
        )
        if st.button(
            "✅  Publish to Master Data",
            type="primary",
            key=f"to_publish_{draft_id}",
        ):
            # Save any unsaved editor state first
            live_df = st.session_state.get(f"_adm_edited_df_{draft_id}")
            if live_df is not None:
                params_now = _df_to_params(live_df)
                if params_now:
                    repo.upsert_parameters(draft_id, params_now, user_name)
            repo.set_admin_stage(draft_id, ADMIN_STAGE_PUBLISHED, user_name)
            st.success(
                f"**{draft['spec_number']}** published to master data."
            )
            st.rerun()

    with col_b:
        _render_danger_zone(repo, draft_id, user_name)


def _render_stage_published(
    repo: CSCRepository, draft: dict, user_name: str
) -> None:
    """Stage 3 — Published / Master Data. Read-only view with retract option."""
    draft_id = draft["draft_id"]

    _spec_header(draft)

    ver = draft.get("spec_version", 0)
    st.success(
        f"**{draft['spec_number']}** is published as master data (v{ver}). "
        "This spec is included in the master export.",
        icon="✅",
    )

    # Read-only parameter display
    params = repo.load_parameters(draft_id)
    if params:
        view_rows: list[dict[str, Any]] = []
        for p in params:
            row = dict(p)
            row["test_procedure_type"] = normalize_test_procedure_type(
                str(row.get("test_procedure_type", "") or "")
            )
            row["test_procedure_text"] = str(
                row.get("test_procedure_text", "") or row.get("test_method", "") or ""
            )
            view_rows.append(row)
        df = pd.DataFrame(view_rows)[[
            "parameter_name",
            "parameter_type",
            "existing_value",
            "test_procedure_type",
            "test_procedure_text",
        ]].rename(columns={
            "parameter_name": "Parameter",
            "parameter_type": "Type",
            "existing_value": "Requirement",
            "test_procedure_type": "Test Procedure Type",
            "test_procedure_text": "Test Procedure Text",
        })
        st.dataframe(df, use_container_width=True, hide_index=True)

    col_retract, col_del = st.columns([3, 1])

    with col_retract:
        st.markdown("")
        if st.button(
            "↩  Retract to Structuring",
            key=f"retract_{draft_id}",
            use_container_width=True,
            help="Pull back to Stage 2 for further edits.",
        ):
            repo.set_admin_stage(draft_id, ADMIN_STAGE_STRUCTURING, user_name)
            st.warning("Retracted. Spec is back in Structuring.")
            st.rerun()

    with col_del:
        _render_danger_zone(repo, draft_id, user_name)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — MASTER EXPORT
# ═══════════════════════════════════════════════════════════════════════════════

def _render_master_export(repo: CSCRepository, user_name: str) -> None:
    st.markdown("#### Master Specification Document Export")
    st.info(
        "Generate a single .docx containing all published specs followed by a "
        "Change Log page listing every version change across all specs.",
        icon="📤",
    )

    summaries = repo.get_admin_draft_summary()
    if not summaries:
        st.warning("No specs yet. Ingest PDFs first.", icon="⚠️")
        return

    # ── Spec selection ────────────────────────────────────────────────────────
    published = [d for d in summaries
                 if _normalise_stage(d.get("admin_stage", "")) == ADMIN_STAGE_PUBLISHED]
    unpublished = [d for d in summaries
                   if _normalise_stage(d.get("admin_stage", "")) != ADMIN_STAGE_PUBLISHED]

    st.markdown(f"**{len(published)} published spec(s) available for export.**")
    st.caption(
        "Ordered by subset: "
        + " → ".join(SPEC_SUBSET_ORDER)
        + " (then numeric spec sequence)."
    )
    if unpublished:
        st.caption(
            f"{len(unpublished)} spec(s) not yet published: "
            + ", ".join(d["spec_number"] for d in unpublished[:5])
            + ("…" if len(unpublished) > 5 else "")
        )

    if not published:
        st.warning("Publish at least one spec first (Stage 2 → Publish).")
        return

    all_labels = [
        f"v{d.get('spec_version',0)}  {d['spec_number']} — {d['chemical_name']}"
        for d in published
    ]
    selected_labels = st.multiselect(
        "Specs to include (default: all published):",
        all_labels,
        default=all_labels,
        key="adm_export_select",
    )

    label_to_draft = {
        f"v{d.get('spec_version',0)}  {d['spec_number']} — {d['chemical_name']}": d
        for d in published
    }
    selected_drafts = [label_to_draft[lbl] for lbl in selected_labels if lbl in label_to_draft]
    selected_drafts = sort_specs_by_subset_order(selected_drafts)

    if not selected_drafts:
        st.warning("Select at least one spec to export.")
        return

    st.caption(
        f"{len(selected_drafts)} spec(s) selected  ·  "
        f"{sum(d.get('param_count', 0) for d in selected_drafts)} total parameters"
    )

    if st.button(
        f"📄  Generate Master Document ({len(selected_drafts)} spec(s) + Change Log)",
        type="primary",
        key="adm_export_btn",
    ):
        with st.spinner("Building document…"):
            try:
                all_specs: list[dict] = []
                for d in selected_drafts:
                    draft_full = repo.load_draft(d["draft_id"])
                    if not draft_full:
                        continue
                    params = repo.load_parameters(d["draft_id"])
                    all_specs.append({
                        "draft":      draft_full,
                        "sections":   repo.load_sections(d["draft_id"]),
                        "parameters": params,
                        "flags":      repo.load_issue_flags(d["draft_id"]),
                        "impact_analysis": repo.load_impact_analysis(d["draft_id"]),
                    })

                # Get changelog for the final page
                changelog = repo.get_version_changelog()

                doc_bytes = build_master_spec_document(
                    all_specs,
                    include_draft_note=False,
                    changelog=changelog,
                )
                repo.log_audit(None, "ADMIN_MASTER_EXPORT", user_name,
                               new_value=f"Exported {len(all_specs)} specs")

                date_str = datetime.now().strftime("%Y%m%d")
                filename = f"ONGC_CSC_MasterSpec_{date_str}.docx"

                st.download_button(
                    label=f"⬇  Download {filename}",
                    data=doc_bytes,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key="adm_export_dl",
                )
                st.success(
                    f"Document ready — {len(all_specs)} spec(s), "
                    f"{sum(len(s['parameters']) for s in all_specs)} total parameters, "
                    f"+ Change Log ({len(changelog)} entries)."
                )
            except Exception as exc:
                st.error(f"Export failed: {exc}")
                logger.exception("Master export error: %s", exc)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — BACKUP & RECOVERY
# ═══════════════════════════════════════════════════════════════════════════════

def _build_master_data_report_pdf(
    payload: dict[str, Any],
    exported_at: datetime,
) -> bytes:
    """Build a compact PDF summary of the current master database contents."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.lib.utils import ImageReader
        from reportlab.platypus import (
            Image,
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except Exception as exc:
        raise RuntimeError(
            "reportlab is required for PDF export. Install it on the intranet host."
        ) from exc

    totals = payload.get("totals", {}) or {}
    specs = payload.get("specifications", []) or []
    parameters_by_draft = payload.get("parameters_by_draft", {}) or {}
    impact_by_draft = payload.get("impact_by_draft", {}) or {}
    logo_path = REPORT_LOGO_PATH if REPORT_LOGO_PATH.exists() else None
    logo_reader = None
    if logo_path is not None:
        try:
            logo_reader = ImageReader(str(logo_path))
        except Exception:
            logo_reader = None

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
        title="Master Data Report",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TitleCenter",
        parent=styles["Title"],
        alignment=1,
        fontSize=19,
        leading=24,
        spaceAfter=14,
    )
    subtitle_style = ParagraphStyle(
        "SubtitleCenter",
        parent=styles["Normal"],
        alignment=1,
        fontSize=11,
        leading=14,
        spaceAfter=4,
    )
    section_style = ParagraphStyle(
        "SectionHeader",
        parent=styles["Heading2"],
        fontSize=12,
        leading=16,
        spaceAfter=8,
        textColor=colors.HexColor("#1F2937"),
    )
    body_style = ParagraphStyle(
        "BodyText",
        parent=styles["BodyText"],
        fontSize=9.5,
        leading=12,
    )
    table_cell_style = ParagraphStyle(
        "TableCellBody",
        parent=body_style,
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        wordWrap="CJK",
    )
    table_header_cell_style = ParagraphStyle(
        "TableCellHeader",
        parent=body_style,
        fontName="Helvetica-Bold",
        fontSize=8.5,
        leading=10,
        textColor=colors.white,
        wordWrap="CJK",
    )
    table_subhead_style = ParagraphStyle(
        "TableSubhead",
        parent=styles["Heading3"],
        fontSize=10.5,
        leading=13,
        spaceBefore=4,
        spaceAfter=4,
        textColor=colors.HexColor("#374151"),
    )

    def _canonical_parameter_type(raw_type: Any) -> str:
        return normalize_parameter_type_label(str(raw_type or ""))

    def _cell_para(value: Any, style: ParagraphStyle) -> Paragraph:
        text = str(value if value is not None else "")
        safe_text = (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        return Paragraph(safe_text or "—", style)

    story: list[Any] = []
    story.append(Spacer(1, 1.2 * cm))
    if logo_path is not None:
        try:
            story.append(Image(str(logo_path), width=4.8 * cm, height=1.3 * cm, hAlign="CENTER"))
        except Exception:
            story.append(Paragraph("ONGC | HCC", subtitle_style))
    else:
        story.append(Paragraph("ONGC | HCC", subtitle_style))
    story.append(Spacer(1, 1.5 * cm))
    story.append(Paragraph("CSC Drafting Workspace", title_style))
    story.append(Paragraph("MASTER DATA REPORT", title_style))
    story.append(Paragraph(f"Export Date: {exported_at.strftime('%Y-%m-%d %H:%M:%S')}", subtitle_style))
    story.append(Spacer(1, 0.35 * cm))
    story.append(Paragraph(f"Total Specifications: {int(totals.get('specifications', 0) or 0)}", subtitle_style))
    story.append(Paragraph(f"Total Parameters: {int(totals.get('parameters', 0) or 0)}", subtitle_style))
    story.append(Paragraph(f"Total Revisions: {int(totals.get('revisions', 0) or 0)}", subtitle_style))
    story.append(Paragraph(f"Specs with Impact Analysis: {int(totals.get('specs_with_impact', 0) or 0)}", subtitle_style))
    story.append(PageBreak())

    story.append(Paragraph("Specification Summary", section_style))
    summary_rows = [[
        _cell_para("Spec Number", table_header_cell_style),
        _cell_para("Chemical Name", table_header_cell_style),
        _cell_para("Revision", table_header_cell_style),
        _cell_para("Stage", table_header_cell_style),
        _cell_para("Issue Date", table_header_cell_style),
        _cell_para("Status", table_header_cell_style),
        _cell_para("Last Updated", table_header_cell_style),
    ]]
    for row in specs:
        stage_raw = str(row.get("admin_stage", "") or "").strip().lower()
        stage_label = stage_raw.title() if stage_raw else "—"
        summary_rows.append([
            _cell_para(str(row.get("spec_number", "") or "—"), table_cell_style),
            _cell_para(str(row.get("chemical_name", "") or "—"), table_cell_style),
            _cell_para(f"v{int(row.get('spec_version', 0) or 0)}", table_cell_style),
            _cell_para(stage_label, table_cell_style),
            _cell_para(str(row.get("meeting_date", "") or "—"), table_cell_style),
            _cell_para(str(row.get("status", "") or "—"), table_cell_style),
            _cell_para(fmt_ist(str(row.get("updated_at", "") or "")), table_cell_style),
        ])

    summary_tbl = Table(
        summary_rows,
        repeatRows=1,
        colWidths=[3.2 * cm, 4.4 * cm, 1.3 * cm, 1.9 * cm, 2.2 * cm, 1.8 * cm, 2.7 * cm],
    )
    summary_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#A54B23")),
        ("ALIGN", (2, 1), (2, -1), "CENTER"),
        ("ALIGN", (3, 1), (3, -1), "CENTER"),
        ("ALIGN", (4, 1), (5, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#D1D5DB")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(summary_tbl)
    story.append(Spacer(1, 0.5 * cm))

    story.append(Paragraph("Impact Score Summary", section_style))
    impact_rows = [[
        _cell_para("Spec Number", table_header_cell_style),
        _cell_para("Op", table_header_cell_style),
        _cell_para("Safety", table_header_cell_style),
        _cell_para("Supply", table_header_cell_style),
        _cell_para("No Sub", table_header_cell_style),
        _cell_para("Total", table_header_cell_style),
        _cell_para("Grade", table_header_cell_style),
        _cell_para("Reviewed", table_header_cell_style),
    ]]
    for row in specs:
        draft_id = str(row.get("draft_id", "") or "")
        impact = impact_by_draft.get(draft_id, {}) or {}
        impact_rows.append([
            _cell_para(str(row.get("spec_number", "") or "—"), table_cell_style),
            _cell_para(str(impact.get("operational_impact_score", "—") or "—"), table_cell_style),
            _cell_para(str(impact.get("safety_environment_score", "—") or "—"), table_cell_style),
            _cell_para(str(impact.get("supply_risk_score", "—") or "—"), table_cell_style),
            _cell_para(
                ("Yes" if int(impact.get("no_substitute_flag", 0) or 0) == 1 else "No") if impact else "—",
                table_cell_style,
            ),
            _cell_para(
                (
                    f"{float(impact.get('impact_score_total', 0.0) or 0.0):.2f}"
                    if impact else "—"
                ),
                table_cell_style,
            ),
            _cell_para(str(impact.get("impact_grade", "—") or "—"), table_cell_style),
            _cell_para(str(impact.get("review_date", "—") or "—"), table_cell_style),
        ])

    impact_tbl = Table(
        impact_rows,
        repeatRows=1,
        colWidths=[4.0 * cm, 1.2 * cm, 1.6 * cm, 1.4 * cm, 1.3 * cm, 1.4 * cm, 2.2 * cm, 3.0 * cm],
    )
    impact_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F2937")),
        ("ALIGN", (1, 1), (5, -1), "CENTER"),
        ("ALIGN", (7, 1), (7, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#D1D5DB")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(impact_tbl)
    story.append(Spacer(1, 0.5 * cm))

    story.append(PageBreak())
    story.append(Paragraph("Parameter Tables by Specification", section_style))
    for idx, row in enumerate(specs, start=1):
        spec_no = str(row.get("spec_number", "") or "—")
        chem = str(row.get("chemical_name", "") or "—")
        draft_id = str(row.get("draft_id", "") or "")
        story.append(Paragraph(f"{idx}. {spec_no} — {chem}", section_style))
        impact = impact_by_draft.get(draft_id, {}) or {}

        meta_rows = [
            [_cell_para("Version", table_header_cell_style), _cell_para(f"v{int(row.get('spec_version', 0) or 0)}", table_cell_style),
             _cell_para("Stage", table_header_cell_style), _cell_para(str(row.get("admin_stage", "—") or "—").title(), table_cell_style)],
            [_cell_para("Status", table_header_cell_style), _cell_para(str(row.get("status", "—") or "—"), table_cell_style),
             _cell_para("Issue Date", table_header_cell_style), _cell_para(str(row.get("meeting_date", "—") or "—"), table_cell_style)],
            [_cell_para("Committee", table_header_cell_style), _cell_para(str(row.get("committee_name", "—") or "—"), table_cell_style),
             _cell_para("Prepared By", table_header_cell_style), _cell_para(str(row.get("prepared_by", "—") or "—"), table_cell_style)],
            [_cell_para("Last Updated", table_header_cell_style), _cell_para(fmt_ist(str(row.get("updated_at", "") or "")), table_cell_style),
             _cell_para("Impact Grade", table_header_cell_style), _cell_para(str(impact.get("impact_grade", "—") or "—"), table_cell_style)],
        ]
        meta_table = Table(meta_rows, colWidths=[2.4 * cm, 5.2 * cm, 2.6 * cm, 5.8 * cm])
        meta_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#1F2937")),
            ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#1F2937")),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D1D5DB")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        story.append(meta_table)
        story.append(Spacer(1, 0.12 * cm))

        impact_detail_rows = [[
            _cell_para("Operational", table_header_cell_style),
            _cell_para("Safety", table_header_cell_style),
            _cell_para("Supply", table_header_cell_style),
            _cell_para("No Sub", table_header_cell_style),
            _cell_para("Total", table_header_cell_style),
            _cell_para("Grade", table_header_cell_style),
            _cell_para("Reviewed By", table_header_cell_style),
        ]]
        impact_detail_rows.append([
            _cell_para(str(impact.get("operational_impact_score", "—") or "—"), table_cell_style),
            _cell_para(str(impact.get("safety_environment_score", "—") or "—"), table_cell_style),
            _cell_para(str(impact.get("supply_risk_score", "—") or "—"), table_cell_style),
            _cell_para(
                ("Yes" if int(impact.get("no_substitute_flag", 0) or 0) == 1 else "No") if impact else "—",
                table_cell_style,
            ),
            _cell_para(
                (f"{float(impact.get('impact_score_total', 0.0) or 0.0):.2f}" if impact else "—"),
                table_cell_style,
            ),
            _cell_para(str(impact.get("impact_grade", "—") or "—"), table_cell_style),
            _cell_para(str(impact.get("reviewed_by", "—") or "—"), table_cell_style),
        ])
        impact_detail_table = Table(
            impact_detail_rows,
            colWidths=[2.0 * cm, 1.8 * cm, 1.6 * cm, 1.5 * cm, 1.5 * cm, 2.1 * cm, 5.5 * cm],
        )
        impact_detail_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#374151")),
            ("ALIGN", (0, 1), (5, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D1D5DB")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#F9FAFB"), colors.white]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        story.append(impact_detail_table)
        story.append(Spacer(1, 0.18 * cm))

        notes_rows = [
            [
                _cell_para("Operational Note", table_header_cell_style),
                _cell_para(str(impact.get("operational_note", "") or "—"), table_cell_style),
            ],
            [
                _cell_para("Safety / Environmental Note", table_header_cell_style),
                _cell_para(str(impact.get("safety_environment_note", "") or "—"), table_cell_style),
            ],
            [
                _cell_para("Supply Risk Note", table_header_cell_style),
                _cell_para(str(impact.get("supply_risk_note", "") or "—"), table_cell_style),
            ],
            [
                _cell_para("No Substitute Context", table_header_cell_style),
                _cell_para(
                    "No substitute available (flagged by committee/admin)."
                    if int(impact.get("no_substitute_flag", 0) or 0) == 1
                    else "Not flagged.",
                    table_cell_style,
                ),
            ],
        ]
        notes_table = Table(notes_rows, colWidths=[4.2 * cm, 10.4 * cm])
        notes_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#1F2937")),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D1D5DB")),
            ("ROWBACKGROUNDS", (1, 0), (1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        story.append(notes_table)
        story.append(Spacer(1, 0.18 * cm))

        all_params = parameters_by_draft.get(draft_id, []) or []
        grouped: dict[str, list[dict[str, Any]]] = {
            "Essential": [],
            "Desirable": [],
        }
        for p in all_params:
            grouped[_canonical_parameter_type(p.get("parameter_type"))].append(p)

        for category in ("Essential", "Desirable"):
            story.append(Paragraph(category, table_subhead_style))
            cat_rows = [[
                _cell_para("S. No.", table_header_cell_style),
                _cell_para("Parameter Name", table_header_cell_style),
                _cell_para("Required Value", table_header_cell_style),
                _cell_para("Test Procedure Text", table_header_cell_style),
            ]]
            for i_cat, p in enumerate(grouped.get(category, []), start=1):
                cat_rows.append([
                    _cell_para(str(i_cat), table_cell_style),
                    _cell_para(str(p.get("parameter_name", "") or "—"), table_cell_style),
                    _cell_para(str(p.get("existing_value", "") or "—"), table_cell_style),
                    _cell_para(
                        str(
                            p.get("test_procedure_text", "")
                            or p.get("test_method", "")
                            or "—"
                        ),
                        table_cell_style,
                    ),
                ])
            if len(cat_rows) == 1:
                cat_rows.append([
                    _cell_para("—", table_cell_style),
                    _cell_para(f"No {category} parameters", table_cell_style),
                    _cell_para("—", table_cell_style),
                    _cell_para("—", table_cell_style),
                ])

            cat_table = Table(
                cat_rows,
                repeatRows=1,
                colWidths=[1.5 * cm, 6.5 * cm, 4.5 * cm, 4.0 * cm],
            )
            cat_table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F2937")),
                ("ALIGN", (0, 1), (0, -1), "CENTER"),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D1D5DB")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 3),
                ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]))
            story.append(cat_table)
            story.append(Spacer(1, 0.15 * cm))

        if idx < len(specs):
            story.append(PageBreak())

    def _draw_page_logo(canvas_obj: Any, doc_obj: Any) -> None:
        x_right = doc_obj.pagesize[0] - doc_obj.rightMargin
        y_top = doc_obj.pagesize[1] - 0.65 * cm
        if logo_reader is not None:
            logo_w = 3.4 * cm
            logo_h = 0.95 * cm
            canvas_obj.drawImage(
                logo_reader,
                x_right - logo_w,
                y_top - logo_h,
                width=logo_w,
                height=logo_h,
                preserveAspectRatio=True,
                mask="auto",
            )
        else:
            canvas_obj.setFont("Helvetica-Bold", 9)
            canvas_obj.drawRightString(x_right, y_top - 0.55 * cm, "ONGC | HCC")

    doc.build(story, onFirstPage=_draw_page_logo, onLaterPages=_draw_page_logo)
    buf.seek(0)
    return buf.getvalue()


def _render_backup_recovery(repo: CSCRepository, user_name: str) -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

    st.markdown("#### Backup & Recovery")
    st.info(
        "Download full database backups, restore from a backup file, "
        "and export a PDF master-data summary.",
        icon="🗄️",
    )

    restored_msg = st.session_state.pop("adm_restore_success_msg", "")
    if restored_msg:
        st.success(restored_msg)

    st.markdown("##### Download Database Backup")
    if st.button("Download Database Backup", key="adm_prepare_backup_btn", type="primary"):
        try:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"MASTER_DATA_BACKUP_{stamp}.db"
            backup_bytes = repo.create_database_backup_bytes()
            (BACKUPS_DIR / filename).write_bytes(backup_bytes)
            st.session_state["adm_backup_filename"] = filename
            st.session_state["adm_backup_bytes"] = backup_bytes
            logger.info("Database backup prepared by %s: %s", user_name, filename)
        except Exception as exc:
            st.error(f"Backup generation failed: {exc}")
            logger.exception("Backup generation failed")

    backup_bytes = st.session_state.get("adm_backup_bytes")
    backup_filename = st.session_state.get("adm_backup_filename")
    if backup_bytes and backup_filename:
        did_download = st.download_button(
            label="Download Prepared Backup File",
            data=backup_bytes,
            file_name=str(backup_filename),
            mime="application/octet-stream",
            key="adm_backup_download_btn",
        )
        if did_download:
            logger.info("Database backup downloaded by %s: %s", user_name, backup_filename)

    st.markdown("---")
    st.markdown("##### Upload Database Backup")
    uploaded_db = st.file_uploader(
        "Upload Database Backup",
        type=["db"],
        key="adm_restore_upload",
        help="Upload a previously downloaded .db backup file.",
    )

    uploaded_bytes: bytes | None = None
    is_valid_backup = False
    if uploaded_db is not None:
        try:
            uploaded_bytes = uploaded_db.getvalue()
            is_valid_backup, validation_msg = repo.validate_database_backup_bytes(uploaded_bytes)
            if is_valid_backup:
                st.success(validation_msg)
            else:
                st.error(validation_msg)
        except Exception as exc:
            st.error(f"Backup validation failed: {exc}")
            logger.exception("Backup validation failed")

    confirm_restore = st.checkbox(
        "I confirm this will overwrite the current database and cannot be undone.",
        key="adm_restore_confirm",
    )
    if st.button(
        "Restore Uploaded Backup",
        key="adm_restore_btn",
        disabled=not (uploaded_bytes and is_valid_backup and confirm_restore),
    ):
        try:
            safety_path = repo.restore_database_from_bytes(
                uploaded_bytes or b"",
                pre_restore_backup_dir=str(BACKUPS_DIR),
            )
            logger.info(
                "Database restored by %s from uploaded backup. Pre-restore safety backup: %s",
                user_name,
                safety_path or "not-created",
            )
            st.session_state["adm_restore_success_msg"] = (
                "Database restored successfully. Application reloaded."
            )
            st.rerun()
        except Exception as exc:
            st.error(f"Restore failed: {exc}")
            logger.exception("Backup restore failed")

    st.markdown("---")
    st.markdown("##### Download Master Data Report")
    if st.button("Download Master Data Report", key="adm_report_prepare_btn"):
        try:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"MASTER_DATA_REPORT_{stamp}.pdf"
            payload = repo.get_master_data_report_payload()
            pdf_bytes = _build_master_data_report_pdf(payload, datetime.now())
            (EXPORTS_DIR / filename).write_bytes(pdf_bytes)
            st.session_state["adm_report_filename"] = filename
            st.session_state["adm_report_bytes"] = pdf_bytes
            logger.info("Master data PDF generated by %s: %s", user_name, filename)
        except Exception as exc:
            st.error(f"PDF generation failed: {exc}")
            logger.exception("Master data PDF generation failed")

    report_bytes = st.session_state.get("adm_report_bytes")
    report_filename = st.session_state.get("adm_report_filename")
    if report_bytes and report_filename:
        did_download = st.download_button(
            label="Download Prepared Master Data Report",
            data=report_bytes,
            file_name=str(report_filename),
            mime="application/pdf",
            key="adm_report_download_btn",
        )
        if did_download:
            logger.info("Master data PDF downloaded by %s: %s", user_name, report_filename)


# ═══════════════════════════════════════════════════════════════════════════════
# Shared sub-renderers
# ═══════════════════════════════════════════════════════════════════════════════

def _spec_header(draft: dict) -> None:
    stage = _normalise_stage(draft.get("admin_stage", ADMIN_STAGE_INGESTED))
    ver   = draft.get("spec_version", 0)
    st.markdown(
        f'<div class="adm-spec-card">'
        f'<div class="adm-spec-no">{draft["spec_number"]}</div>'
        f'<div class="adm-spec-name">{draft["chemical_name"]}</div>'
        f'<div class="adm-spec-ver">Version {ver}</div>'
        f'<div style="font-size:.78rem;color:#6B7280;">'
        f'Test Proc: {draft.get("test_procedure") or "—"} &nbsp;·&nbsp; '
        f'Mat. Code: {draft.get("material_code") or "—"} &nbsp;·&nbsp; '
        f'Prepared by: {draft.get("prepared_by") or "—"}'
        f'&nbsp;&nbsp;{_stage_pill(stage)}'
        f'</div></div>',
        unsafe_allow_html=True,
    )


def _render_metadata_editor(
    repo: CSCRepository, draft: dict, user_name: str
) -> None:
    """Minimal metadata editor — only spec identity and test procedure fields."""
    draft_id = draft["draft_id"]
    with st.expander("✏️  Edit Metadata", expanded=False):
        with st.form(f"adm_meta_{draft_id}"):
            c1, c2 = st.columns(2)
            with c1:
                spec_no = st.text_input("Spec Number",    value=draft.get("spec_number", ""),   max_chars=200)
                chem    = st.text_input("Chemical Name",  value=draft.get("chemical_name", ""), max_chars=300)
                test_pr = st.text_input("Test Procedure", value=draft.get("test_procedure", ""),max_chars=300)
            with c2:
                mat_cd  = st.text_input("Material Code",  value=draft.get("material_code", ""), max_chars=100)
                prep    = st.text_input(
                    "Prepared By",
                    value=draft.get("prepared_by", DEFAULT_PREPARED_BY) or DEFAULT_PREPARED_BY,
                    max_chars=200,
                )
            if st.form_submit_button("Save Metadata", type="primary"):
                subset, seq, year = parse_spec_number(spec_no.strip())
                if spec_no.strip() and (
                    subset not in SPEC_SUBSET_ORDER or seq is None or year is None
                ):
                    st.error(
                        "Spec Number must follow ONGC/<subset>/<number>/<year>, "
                        "for example ONGC/DFC/01/2026."
                    )
                else:
                    try:
                        repo.update_draft_metadata(
                            draft_id=draft_id, spec_number=spec_no, chemical_name=chem,
                            committee_name=draft.get("committee_name", ""),
                            meeting_date="", prepared_by=prep, reviewed_by="",
                            status=draft.get("status", "Draft"), user_name=user_name,
                        )
                        repo.upsert_section(draft_id, "Meta::test_procedure", test_pr, 90, user_name)
                        repo.upsert_section(draft_id, "Meta::material_code",  mat_cd, 91, user_name)
                        st.success("Metadata saved.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Save failed: {exc}")


def _render_param_editor(
    repo: CSCRepository, draft_id: str, user_name: str,
) -> None:
    """Parameter editor — master-data columns only: name, type, existing value and test procedure fields.

    Saving parameters does not increment version; publish does.
    """
    existing_params = repo.load_parameters(draft_id)
    normalized_params: list[dict[str, Any]] = []
    for p in existing_params:
        row = dict(p)
        if not str(row.get("test_procedure_text", "") or "").strip():
            row["test_procedure_text"] = str(row.get("test_method", "") or "")
        row["test_procedure_type"] = normalize_test_procedure_type(
            str(row.get("test_procedure_type", "") or "")
        )
        normalized_params.append(row)
    display_cols = [
        "parameter_name",
        "parameter_type",
        "existing_value",
        "test_procedure_type",
        "test_procedure_text",
    ]
    df = (
        pd.DataFrame(normalized_params)[display_cols]
        if normalized_params
        else pd.DataFrame(columns=display_cols)
    )
    for col in display_cols:
        df[col] = df[col].fillna("").astype(str)

    col_cfg = {
        _PARAM_SL_COL: st.column_config.NumberColumn(
            "S. No.", width="small", disabled=True,
        ),
        _PARAM_MOVE_COL: st.column_config.CheckboxColumn(
            "Move", width="small",
            help="Select one row, then use Move Up / Move Down.",
        ),
        "parameter_name": st.column_config.TextColumn(
            "Parameter Name ✏️",
            width="large",
            required=True,
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "parameter_type": st.column_config.SelectboxColumn(
            "Type ✏️", options=PARAMETER_TYPES, required=True, width="small",
        ),
        "existing_value": st.column_config.TextColumn(
            "Requirement ✏️", width="medium",
            help="The verified requirement value from the current specification.",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "test_procedure_type": st.column_config.SelectboxColumn(
            "Test Procedure Type ✏️",
            options=_TEST_PROCEDURE_TYPE_OPTIONS,
            width="medium",
            required=False,
        ),
        "test_procedure_text": st.column_config.TextColumn(
            "Test Procedure Text ✏️",
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
    }

    editor_key = f"adm_param_ed_{draft_id}"
    state_key = f"_adm_param_state_{draft_id}"
    if state_key not in st.session_state:
        st.session_state[state_key] = _prepare_param_editor_df(df)
    source_df = st.session_state[state_key]
    working_df = _prepare_param_editor_df(source_df)

    edited_df = st.data_editor(
        working_df,
        column_order=[_PARAM_SL_COL, _PARAM_MOVE_COL] + display_cols,
        column_config=col_cfg,
        num_rows="dynamic",
        use_container_width=False,
        height=_param_editor_height(len(working_df)),
        hide_index=True,
        key=editor_key,
        on_change=_sync_param_editor_state,
        args=(editor_key, state_key),
        row_height=_PARAM_EDITOR_ROW_HEIGHT,
    )
    latest_state_df = st.session_state.get(state_key)
    if isinstance(latest_state_df, pd.DataFrame):
        edited_df = _prepare_param_editor_df(latest_state_df)
    else:
        edited_df = _prepare_param_editor_df(edited_df)
        st.session_state[state_key] = edited_df

    move_up_col, move_down_col, move_note_col = st.columns([1, 1, 3])
    with move_up_col:
        if st.button("⬆ Move Up", key=f"adm_move_up_{draft_id}", use_container_width=True):
            moved_df, err = _move_selected_parameter_row(edited_df, direction=-1)
            if err:
                st.warning(err)
            else:
                st.session_state[state_key] = moved_df
                st.session_state.pop(editor_key, None)
                st.rerun()
    with move_down_col:
        if st.button("⬇ Move Down", key=f"adm_move_down_{draft_id}", use_container_width=True):
            moved_df, err = _move_selected_parameter_row(edited_df, direction=1)
            if err:
                st.warning(err)
            else:
                st.session_state[state_key] = moved_df
                st.session_state.pop(editor_key, None)
                st.rerun()
    with move_note_col:
        st.caption("Row order drives final S. No. sequence in the generated specification sheet.")

    # Persist editor state for the advance/reset buttons
    st.session_state[f"_adm_edited_df_{draft_id}"] = edited_df

    save_lock_key = f"_adm_save_locked_{draft_id}"
    save_locked = bool(st.session_state.get(save_lock_key, False))

    col_save, col_reset = st.columns([1, 2])
    with col_save:
        if save_locked:
            st.caption("Save is locked after one successful save. Refresh the page to enable it again.")
        if st.button(
            "💾  Save Parameters",
            key=f"adm_save_{draft_id}",
            type="primary",
            use_container_width=True,
            disabled=save_locked,
        ):
            save_df = st.session_state.get(state_key)
            if not isinstance(save_df, pd.DataFrame):
                save_df = _resolve_data_editor_state_df(working_df, st.session_state.get(editor_key))
            params = _df_to_params(save_df)
            if params:
                try:
                    repo.upsert_parameters(draft_id, params, user_name)
                    st.session_state.pop(state_key, None)
                    st.session_state.pop(editor_key, None)
                    st.session_state[save_lock_key] = True
                    st.success(
                        f"Saved {len(params)} parameter(s). "
                        "Version will increment when you publish."
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Save failed: {exc}")
            else:
                st.error("No valid rows to save — ensure at least one Parameter Name is filled.")

    with col_reset:
        if st.button(
            "🔄  Reset — reload from saved data",
            key=f"adm_reset_{draft_id}",
            help="Discard unsaved edits and reload parameters from the database.",
        ):
            # Clear the editor's session state key so it re-loads from DB
            st.session_state.pop(editor_key, None)
            st.session_state.pop(state_key, None)
            st.session_state.pop(f"_adm_edited_df_{draft_id}", None)
            st.rerun()


def _render_danger_zone(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    with st.expander("🗑  Delete", expanded=False):
        ck = f"adm_del_confirm_{draft_id}"
        if st.checkbox("Confirm permanent deletion", key=ck):
            if st.button("Delete", key=f"adm_del_{draft_id}", type="primary"):
                repo.delete_draft(draft_id, user_name)
                st.success("Deleted.")
                st.session_state.pop(ck, None)
                st.rerun()


def _render_impact_analysis_editor(
    repo: CSCRepository,
    draft_id: str,
    user_name: str,
) -> None:
    st.markdown("##### 📊  Impact Analysis")
    st.caption("Capture structured impact scoring for this specification.")
    existing = repo.load_impact_analysis(draft_id) or {}

    score_options: list[int | None] = [None, 1, 2, 3, 4, 5]

    def _score_index(current: Any) -> int:
        try:
            cur = int(current)
            return score_options.index(cur) if cur in score_options else 0
        except Exception:
            return 0

    c1, c2 = st.columns(2)
    with c1:
        op_score = st.selectbox(
            "Operational Impact",
            options=score_options,
            index=_score_index(existing.get("operational_impact_score")),
            format_func=lambda x: "Select score" if x is None else str(x),
            key=f"adm_impact_op_{draft_id}",
        )
        op_note = st.text_area(
            "Operational Impact Note",
            value=str(existing.get("operational_note", "") or ""),
            height=90,
            key=f"adm_impact_op_note_{draft_id}",
        )
        se_score = st.selectbox(
            "Safety / Environmental Impact",
            options=score_options,
            index=_score_index(existing.get("safety_environment_score")),
            format_func=lambda x: "Select score" if x is None else str(x),
            key=f"adm_impact_se_{draft_id}",
        )
        se_note = st.text_area(
            "Safety / Environmental Note",
            value=str(existing.get("safety_environment_note", "") or ""),
            height=90,
            key=f"adm_impact_se_note_{draft_id}",
        )
    with c2:
        sr_score = st.selectbox(
            "Supply Risk",
            options=score_options,
            index=_score_index(existing.get("supply_risk_score")),
            format_func=lambda x: "Select score" if x is None else str(x),
            key=f"adm_impact_sr_{draft_id}",
        )
        sr_note = st.text_area(
            "Supply Risk Note",
            value=str(existing.get("supply_risk_note", "") or ""),
            height=90,
            key=f"adm_impact_sr_note_{draft_id}",
        )
        no_sub = st.radio(
            "No Substitute Available",
            options=["No", "Yes"],
            index=1 if int(existing.get("no_substitute_flag", 0) or 0) == 1 else 0,
            horizontal=True,
            key=f"adm_impact_no_sub_{draft_id}",
            help="Select Yes only if no technically acceptable substitute exists for the intended application.",
        )
    no_sub_flag = 1 if no_sub == "Yes" else 0

    if None not in {op_score, se_score, sr_score}:
        op_weighted = float(op_score) * 0.5
        se_weighted = float(se_score) * 0.3
        sr_weighted = float(sr_score) * 0.2
        total_score = round(
            calculate_impact_score(int(op_score), int(se_score), int(sr_score)),
            2,
        )
        computed_grade = get_impact_grade(total_score, 0)
        final_grade = get_impact_grade(total_score, no_sub_flag)
        override_applied = bool(
            no_sub_flag == 1
            and computed_grade in {"LOW", "MODERATE"}
            and final_grade == "HIGH"
        )
        st.markdown(
            f"""
<div style="background:#F8FAFC;border:1px solid #CBD5E1;padding:0.75rem 0.9rem;border-radius:8px;margin-bottom:.4rem;">
<div><strong>Operational Impact:</strong> {op_score} × 0.5 = {op_weighted:.2f}</div>
<div><strong>Safety / Environmental Impact:</strong> {se_score} × 0.3 = {se_weighted:.2f}</div>
<div><strong>Supply Risk:</strong> {sr_score} × 0.2 = {sr_weighted:.2f}</div>
<div style="margin-top:.3rem;"><strong>Total Impact Score = {total_score:.2f}</strong></div>
<div><strong>Computed Grade = {computed_grade}</strong></div>
{"<div style='color:#92400E;font-weight:700;'>No Substitute Override Applied → Final Grade raised to HIGH</div>" if override_applied else ""}
<div style="margin-top:.2rem;"><strong>Final Impact Grade = {final_grade}</strong></div>
</div>
""",
            unsafe_allow_html=True,
        )
    else:
        st.info("Select all three scores to calculate impact total and grade.")

    if st.button("💾  Save Impact Analysis", key=f"adm_impact_save_{draft_id}", use_container_width=False):
        if None in {op_score, se_score, sr_score}:
            st.error("Select all three scores before saving.")
        else:
            try:
                repo.save_impact_analysis(
                    draft_id=draft_id,
                    operational_impact_score=int(op_score),
                    safety_environment_score=int(se_score),
                    supply_risk_score=int(sr_score),
                    no_substitute_flag=no_sub_flag,
                    operational_note=op_note,
                    safety_environment_note=se_note,
                    supply_risk_note=sr_note,
                    reviewed_by=user_name,
                )
                st.success("Impact Analysis saved.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")
