"""CSC Drafting Workspace — section-by-section UI renderers.

Each render_section_* function receives the Streamlit module (st),
the active draft_id, a CSCRepository instance, and the current user_name.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from csc_repository import (
    CSCRepository,
    calculate_impact_score,
    get_impact_grade,
    normalize_test_procedure_type,
)
from csc_utils import (
    SECTION_BACKGROUND,
    SECTION_EXISTING_SPEC,
    SECTION_ISSUES,
    SECTION_PARAMETERS,
    SECTION_PROPOSED,
    SECTION_JUSTIFICATION,
    SECTION_IMPACT,
    SECTION_RECOMMENDATION,
    SECTION_PREVIEW,
    SECTION_ORDER,
    SECTION_GUIDANCE,
    ISSUE_TYPES,
    PARAMETER_TYPES,
    REC_MAIN_KEY,
    REC_REMARKS_KEY,
    fmt_ist,
    sanitize_text,
    sanitize_multiline_text,
)
from csc_export import build_word_document

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section router
# ---------------------------------------------------------------------------

def render_active_section(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    """Dispatch to the correct section renderer based on session state."""
    draft = repo.load_draft(draft_id)
    if not draft:
        st.error("Draft not found.")
        return

    # Guardrail: published master specs are read-only for committee users.
    if bool(draft.get("is_admin_draft")) and draft.get("admin_stage") == "published":
        st.warning(
            "This is the published master specification and is read-only. "
            "Create/open a revision draft from the sidebar to propose updates.",
            icon="🔒",
        )
        if st.button("Create / Open Revision Draft", key=f"create_revision_{draft_id}", type="primary"):
            rev_id, _created = repo.create_or_get_revision_draft(draft_id, user_name)
            st.session_state["csc_active_draft_id"] = rev_id
            st.session_state["csc_active_section"] = SECTION_BACKGROUND
            st.rerun()
        return

    if draft.get("status") in {"Pending Approval", "Approved"}:
        st.info(
            f"Revision status: **{draft.get('status')}**. "
            "Sections are locked while this request is in workflow.",
            icon="🔒",
        )
        st.session_state["csc_active_section"] = SECTION_PREVIEW
        render_section_preview_export(repo, draft_id, user_name)
        return

    active = st.session_state.get("csc_active_section", SECTION_BACKGROUND)

    renderers = {
        SECTION_BACKGROUND:     render_section_background,
        SECTION_EXISTING_SPEC:  render_section_existing_spec,
        SECTION_ISSUES:         render_section_issues,
        SECTION_PARAMETERS:     render_section_parameters,
        SECTION_PROPOSED:       render_section_proposed,
        SECTION_JUSTIFICATION:  render_section_justification,
        SECTION_IMPACT:         render_section_impact,
        SECTION_RECOMMENDATION: render_section_recommendation,
        SECTION_PREVIEW:        render_section_preview_export,
    }

    fn = renderers.get(active)
    if fn:
        fn(repo, draft_id, user_name)
    else:
        st.info("Select a section from the navigator.")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _section_header(title: str, subtitle: str = "") -> None:
    st.markdown(f"## {title}")
    if subtitle:
        st.caption(subtitle)
    st.markdown("---")


def _guidance(text: str) -> None:
    st.info(f"**Guidance:** {text}", icon="ℹ️")


def _save_button(label: str, key: str) -> bool:
    col1, col2 = st.columns([1, 5])
    with col1:
        return st.button(label, key=key, type="primary", use_container_width=True)


def _advance_to_next_section(current_section: str) -> None:
    """Move navigator to next section in SECTION_ORDER and rerun."""
    try:
        idx = SECTION_ORDER.index(current_section)
    except ValueError:
        return
    if idx < len(SECTION_ORDER) - 1:
        st.session_state["csc_active_section"] = SECTION_ORDER[idx + 1]
        st.rerun()


def _load_section_text(repo: CSCRepository, draft_id: str, section_name: str) -> str:
    return repo.get_section_text(draft_id, section_name)


def _text_section(
    repo: CSCRepository,
    draft_id: str,
    user_name: str,
    section_name: str,
    section_index: int,
    title: str,
    guidance: str,
    height: int = 280,
) -> None:
    """Generic renderer for plain text sections."""
    _section_header(title)
    _guidance(guidance)

    existing = _load_section_text(repo, draft_id, section_name)
    text = st.text_area(
        "Section content",
        value=existing,
        height=height,
        placeholder="Enter text here…",
        key=f"section_text_{draft_id}_{section_name}",
        label_visibility="collapsed",
    )

    if _save_button("Save Section", key=f"save_{draft_id}_{section_name}"):
        try:
            repo.upsert_section(draft_id, section_name, text, section_index, user_name)
            st.success("Section saved.")
            _advance_to_next_section(section_name)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
            logger.exception("Section save error: %s", exc)


# ---------------------------------------------------------------------------
# Section 1 — Background
# ---------------------------------------------------------------------------

def render_section_background(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _text_section(
        repo, draft_id, user_name,
        section_name=SECTION_BACKGROUND,
        section_index=0,
        title="1. Background",
        guidance=SECTION_GUIDANCE[SECTION_BACKGROUND],
    )


# ---------------------------------------------------------------------------
# Section 2 — Existing Specification Summary
# ---------------------------------------------------------------------------

def render_section_existing_spec(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _text_section(
        repo, draft_id, user_name,
        section_name=SECTION_EXISTING_SPEC,
        section_index=1,
        title="2. Existing Specification Summary",
        guidance=SECTION_GUIDANCE[SECTION_EXISTING_SPEC],
    )


# ---------------------------------------------------------------------------
# Section 3 — Issues Observed
# ---------------------------------------------------------------------------

def render_section_issues(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _section_header(
        "3. Issues Observed",
        "Select Yes for each issue type that applies, and provide a brief note.",
    )

    existing_flags = {
        f["issue_type"]: f for f in repo.load_issue_flags(draft_id)
    }

    updated_flags: list[dict[str, Any]] = []

    for idx, (key, label) in enumerate(ISSUE_TYPES):
        current_flag = existing_flags.get(key, {})
        current_present = bool(current_flag.get("is_present", 0))
        current_note    = current_flag.get("note", "") or ""

        st.markdown(f"**{label}**")
        col_radio, col_note = st.columns([1, 3])

        with col_radio:
            choice = st.radio(
                label,
                options=["No", "Yes"],
                index=1 if current_present else 0,
                key=f"issue_radio_{draft_id}_{key}",
                horizontal=True,
                label_visibility="collapsed",
            )

        with col_note:
            note = ""
            if choice == "Yes":
                note = st.text_area(
                    f"Note for {label}",
                    value=current_note,
                    height=80,
                    placeholder="Describe the issue briefly…",
                    key=f"issue_note_{draft_id}_{key}",
                    label_visibility="collapsed",
                )
            else:
                st.caption("No issue — note not required.")

        updated_flags.append({
            "issue_type": key,
            "is_present": 1 if choice == "Yes" else 0,
            "note":       note,
            "sort_order": idx,
        })

        st.divider()

    if _save_button("Save Issues", key=f"save_issues_{draft_id}"):
        try:
            repo.upsert_issue_flags(draft_id, updated_flags, user_name)
            st.success("Issues saved.")
            _advance_to_next_section(SECTION_ISSUES)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
            logger.exception("Issue flag save error: %s", exc)


# ---------------------------------------------------------------------------
# Section 4 — Parameter Review  (two-phase workflow)
# ---------------------------------------------------------------------------

_PHASE_KEY = "csc_param_phase_{}"   # session state key per draft
_PARAM_EDITOR_STATE_KEY = "csc_param_editor_state_{}_{}"
_PARAM_EDITOR_WIDGET_KEY = "params_editor_{}_{}"
_PARAM_SL_COL = "sl_no"
_PARAM_MOVE_COL = "move_row"
_PARAM_ID_COL = "parameter_id"      # hidden identity column — kept in df, not shown
_PARAM_CELL_MAX_CHARS = 10_000
_PARAM_EDITOR_ROW_HEIGHT = 58
_PARAM_TEXT_FIELDS = [
    "parameter_name",
    "existing_value",
    "proposed_value",
    "parameter_type",
    "test_procedure_type",
    "test_procedure_text",
    "justification",
    "remarks",
]
_TEST_PROCEDURE_TYPE_OPTIONS = [
    "ASTM",
    "API",
    "IS",
    "Inhouse",
    "Other National / International",
]


def _param_editor_state_key(draft_id: str, phase_tag: str) -> str:
    return _PARAM_EDITOR_STATE_KEY.format(phase_tag, draft_id)


def _param_editor_widget_key(draft_id: str, phase_tag: str) -> str:
    return _PARAM_EDITOR_WIDGET_KEY.format(phase_tag, draft_id)


def _clear_param_editor_state(draft_id: str, phase_tag: str) -> None:
    st.session_state.pop(_param_editor_state_key(draft_id, phase_tag), None)
    st.session_state.pop(_param_editor_widget_key(draft_id, phase_tag), None)


def _prepare_param_editor_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure move controls, serial numbers, and identity column exist for parameter editors."""
    work = df.copy().reset_index(drop=True)
    for col in _PARAM_TEXT_FIELDS:
        if col in work.columns:
            work[col] = work[col].fillna("").astype(str)
    # Preserve parameter_id so the repo can UPDATE existing rows instead of DELETE+INSERT.
    if _PARAM_ID_COL not in work.columns:
        work[_PARAM_ID_COL] = ""
    else:
        work[_PARAM_ID_COL] = work[_PARAM_ID_COL].fillna("").astype(str)
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
    """Move exactly one selected row up/down. direction: -1 (up), +1 (down)."""
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
    moved_df = _prepare_param_editor_df(pd.DataFrame(rows))
    return moved_df, None


def _param_editor_height(row_count: int) -> int:
    """Return a dynamic data_editor height that shows most/all rows."""
    visible_rows = max(8, int(row_count) + 1)
    return max(420, min(10000, visible_rows * _PARAM_EDITOR_ROW_HEIGHT + 64))


def _cell_text(row: dict[str, Any] | pd.Series, key: str, default: str = "") -> str:
    val = row.get(key, default)
    if pd.isna(val):
        return default
    return str(val or default)


def _param_identity(row: dict[str, Any] | pd.Series) -> tuple[str, str, str]:
    return (
        _cell_text(row, "parameter_name").strip().casefold(),
        _cell_text(row, "existing_value").strip(),
        _cell_text(row, "test_procedure_text", _cell_text(row, "test_method")).strip(),
    )


def _build_parent_type_lookup(
    parent_params: list[dict[str, Any]],
) -> dict[tuple[str, str, str], set[tuple[str, str, str]]]:
    lookup: dict[tuple[str, str, str], set[tuple[str, str, str]]] = {}
    for p in parent_params:
        key = _param_identity(p)
        lookup.setdefault(key, set()).add(
            (
                _cell_text(p, "parameter_type").strip(),
                normalize_test_procedure_type(_cell_text(p, "test_procedure_type").strip()),
                _cell_text(p, "test_procedure_text", _cell_text(p, "test_method")).strip(),
            )
        )
    return lookup


def _row_change_flags(
    row: dict[str, Any] | pd.Series,
    parent_type_lookup: dict[tuple[str, str, str], set[tuple[str, str, str]]] | None,
) -> tuple[bool, bool]:
    existing = _cell_text(row, "existing_value").strip()
    proposed = _cell_text(row, "proposed_value").strip()
    approved = proposed or existing
    value_changed = approved != existing

    type_changed = False
    if parent_type_lookup is not None:
        key = _param_identity(row)
        row_has_name = bool(_cell_text(row, "parameter_name").strip())
        if row_has_name and key not in parent_type_lookup:
            # New row or identity fields edited vs parent baseline.
            value_changed = True
        current_signature = (
            _cell_text(row, "parameter_type").strip(),
            normalize_test_procedure_type(_cell_text(row, "test_procedure_type").strip()),
            _cell_text(row, "test_procedure_text", _cell_text(row, "test_method")).strip(),
        )
        parent_signatures = parent_type_lookup.get(key, {current_signature})
        type_changed = current_signature not in parent_signatures

    return value_changed, type_changed


def render_section_parameters(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    """Two-phase parameter review.

    Phase 1 — Verify & Correct:
        All columns fully editable. User fixes any PDF extraction errors.
        Skipped automatically when admin has pre-locked Phase 1 (phase1_locked=1).

    Phase 2 — Propose Changes:
        Parameter rows remain editable and all changes are tracked against parent baseline.
    """
    # ── Check admin phase-lock from DB ────────────────────────────────
    draft = repo.load_draft(draft_id)
    phase1_locked_by_admin = bool(draft.get("phase1_locked", 0)) if draft else False

    phase_key = _PHASE_KEY.format(draft_id)
    if phase_key not in st.session_state:
        # If admin has locked Phase 1, committee users start directly at Phase 2
        st.session_state[phase_key] = 2 if phase1_locked_by_admin else 1

    # If admin locked Phase 1 after user loaded, force to Phase 2
    if phase1_locked_by_admin and st.session_state[phase_key] == 1:
        st.session_state[phase_key] = 2

    phase = st.session_state[phase_key]
    has_params = bool(repo.load_parameters(draft_id))

    _section_header("4. Parameter Review")

    # ── Admin-published banner ─────────────────────────────────────────
    if phase1_locked_by_admin:
        st.info(
            "This specification was prepared and verified by admin. "
            "You can edit parameters and provide change justifications below.",
            icon="📋",
        )

    # ── Phase indicator strip ─────────────────────────────────────────
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        if phase1_locked_by_admin:
            step1_style = "background:#E5E7EB;color:#9CA3AF;text-decoration:line-through;"
        elif phase == 1:
            step1_style = "background:#A54B23;color:white;font-weight:700;"
        else:
            step1_style = "background:#D1FAE5;color:#065F46;font-weight:600;"
        step1_label = "Step 1 — Verify & Correct" + (" 🔒" if phase1_locked_by_admin else (" ✓" if phase == 2 else ""))
        st.markdown(
            f'<div style="{step1_style}padding:0.5rem 1rem;border-radius:8px 0 0 8px;'
            f'text-align:center;font-size:0.88rem;">{step1_label}</div>',
            unsafe_allow_html=True,
        )
    with col_p2:
        active_style = (
            "background:#A54B23;color:white;font-weight:700;"
            if phase == 2
            else "background:#F3F4F6;color:#6B7280;"
        )
        st.markdown(
            f'<div style="{active_style}padding:0.5rem 1rem;border-radius:0 8px 8px 0;'
            f'text-align:center;font-size:0.88rem;">Step 2 — Propose Changes</div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    if phase == 1:
        _render_phase1(repo, draft_id, user_name, has_params)
    else:
        _render_phase2(repo, draft_id, user_name, allow_back=not phase1_locked_by_admin)


def _render_phase1(
    repo: CSCRepository, draft_id: str, user_name: str, has_params: bool
) -> None:
    """Phase 1 — all columns editable; user verifies extraction accuracy."""
    st.warning(
        "**Step 1 — Verify & Correct the parameter table before proposing any changes.**\n\n"
        "PDF extraction is automated and may not be perfect. "
        "Check every row carefully against the actual specification document and correct:\n"
        "- Parameter names (clean up garbled or truncated text)\n"
        "- **Existing Req.** values (fix merged lines, wrong values, stray text)\n"
        "- Parameter Type (Essential / Desirable)\n"
        "- Test Procedure Type and Test Procedure Text\n\n"
        "Once the table exactly matches the source specification, click "
        "**\"Confirm & Proceed to Step 2\"** below.",
        icon="⚠️",
    )

    existing = repo.load_parameters(draft_id)
    normalized_rows: list[dict[str, Any]] = []
    for p in existing:
        row = dict(p)
        row["test_procedure_type"] = normalize_test_procedure_type(
            _cell_text(row, "test_procedure_type").strip()
        )
        row["test_procedure_text"] = _cell_text(
            row, "test_procedure_text", _cell_text(row, "test_method")
        )
        normalized_rows.append(row)
    display_cols = [
        "parameter_name", "existing_value", "proposed_value",
        "parameter_type", "test_procedure_type", "test_procedure_text", "justification", "remarks",
    ]
    all_cols = [_PARAM_ID_COL] + display_cols
    if normalized_rows:
        raw_df = pd.DataFrame(normalized_rows)
        # Ensure parameter_id column is present even if not in normalized_rows keys
        if _PARAM_ID_COL not in raw_df.columns:
            raw_df[_PARAM_ID_COL] = ""
        df = raw_df[all_cols]
    else:
        df = pd.DataFrame(columns=all_cols)
    for col in display_cols:
        df[col] = df[col].fillna("").astype(str)
    phase_tag = "p1"
    editor_state_key = _param_editor_state_key(draft_id, phase_tag)
    editor_widget_key = _param_editor_widget_key(draft_id, phase_tag)
    if editor_state_key not in st.session_state:
        st.session_state[editor_state_key] = _prepare_param_editor_df(df)
    widget_state_df = st.session_state.get(editor_widget_key)
    source_df = (
        widget_state_df
        if isinstance(widget_state_df, pd.DataFrame)
        else st.session_state[editor_state_key]
    )
    working_df = _prepare_param_editor_df(source_df)

    column_config = {
        _PARAM_ID_COL: None,  # hide identity column from user
        _PARAM_SL_COL: st.column_config.NumberColumn(
            "S. No.",
            disabled=True,
            width="small",
        ),
        _PARAM_MOVE_COL: st.column_config.CheckboxColumn(
            "Move",
            help="Select one row, then use Move Up / Move Down.",
            width="small",
        ),
        "parameter_name": st.column_config.TextColumn(
            "Parameter Name ✏️",
            help="Correct any garbled or truncated names from PDF extraction",
            required=True,
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "existing_value": st.column_config.TextColumn(
            "Existing Req. ✏️",
            help="Fix any merged or incorrect values — verify against the actual spec document",
            width="medium",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "proposed_value": st.column_config.TextColumn(
            "Proposed Req.",
            help="Leave blank for now — fill in Step 2",
            width="medium",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "parameter_type": st.column_config.SelectboxColumn(
            "Type ✏️",
            options=PARAMETER_TYPES,
            required=True,
            width="small",
            help="Essential / Desirable",
        ),
        "test_procedure_type": st.column_config.SelectboxColumn(
            "Test Procedure Type ✏️",
            options=_TEST_PROCEDURE_TYPE_OPTIONS,
            help="Select the procedure family.",
            width="medium",
            required=False,
        ),
        "test_procedure_text": st.column_config.TextColumn(
            "Test Procedure Text ✏️",
            help="Enter the full procedure reference text.",
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "justification": st.column_config.TextColumn(
            "Justification",
            help="Leave blank for now — fill in Step 2",
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "remarks": st.column_config.TextColumn(
            "Remarks",
            width="medium",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
    }

    edited_df = st.data_editor(
        working_df,
        column_order=[_PARAM_SL_COL, _PARAM_MOVE_COL] + display_cols,
        column_config=column_config,
        num_rows="dynamic",
        use_container_width=False,
        height=_param_editor_height(len(working_df)),
        hide_index=True,
        key=editor_widget_key,
        row_height=_PARAM_EDITOR_ROW_HEIGHT,
    )
    edited_df = _prepare_param_editor_df(edited_df)
    st.session_state[editor_state_key] = edited_df

    move_up_col, move_down_col, move_note_col = st.columns([1, 1, 3])
    with move_up_col:
        if st.button("⬆ Move Up", key=f"move_up_p1_{draft_id}", use_container_width=True):
            moved_df, err = _move_selected_parameter_row(edited_df, direction=-1)
            if err:
                st.warning(err)
            else:
                st.session_state[editor_state_key] = moved_df
                st.session_state.pop(editor_widget_key, None)
                st.rerun()
    with move_down_col:
        if st.button("⬇ Move Down", key=f"move_down_p1_{draft_id}", use_container_width=True):
            moved_df, err = _move_selected_parameter_row(edited_df, direction=1)
            if err:
                st.warning(err)
            else:
                st.session_state[editor_state_key] = moved_df
                st.session_state.pop(editor_widget_key, None)
                st.rerun()
    with move_note_col:
        st.caption("Select one row in `Move` to reposition parameters and insert new rows anywhere.")

    st.markdown("")
    col_save1, col_confirm = st.columns([1, 2])

    with col_save1:
        if st.button(
            "Save Corrections",
            key=f"save_p1_{draft_id}",
            type="primary",
            use_container_width=True,
        ):
            params = _rows_to_params(edited_df)
            if params:
                try:
                    repo.upsert_parameters(draft_id, params, user_name)
                    _clear_param_editor_state(draft_id, phase_tag)
                    st.success(
                        f"Saved {len(params)} parameter(s). "
                        "Continue checking, then confirm below."
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Save failed: {exc}")
                    logger.exception("Phase 1 save error: %s", exc)
            else:
                st.error("No valid parameter rows to save.")

    with col_confirm:
        phase_key = _PHASE_KEY.format(draft_id)
        if st.button(
            "✓  Existing values are correct — Proceed to Step 2 →",
            key=f"confirm_p1_{draft_id}",
            disabled=not has_params,
            use_container_width=True,
            help="Only click once you have verified every row against the actual spec document.",
        ):
            params = _rows_to_params(edited_df)
            if params:
                try:
                    repo.upsert_parameters(draft_id, params, user_name)
                    repo.log_audit(
                        draft_id, "PHASE1_CONFIRMED", user_name,
                        remarks=f"{len(params)} parameters verified",
                    )
                    _clear_param_editor_state(draft_id, phase_tag)
                    _clear_param_editor_state(draft_id, "p2")
                except Exception as exc:
                    st.error(f"Could not save before confirming: {exc}")
                    logger.exception("Phase 1 confirm save error: %s", exc)
                    st.stop()
            st.session_state[phase_key] = 2
            st.rerun()

    if not has_params:
        st.caption("⬆ Add at least one parameter row and save before confirming.")


def _render_phase2(
    repo: CSCRepository, draft_id: str, user_name: str,
    allow_back: bool = True,
) -> None:
    """Phase 2 — propose changes across value, type and test procedure fields.

    allow_back=False when admin has locked Phase 1 (committee users cannot
    go back to edit parameter names / existing values).
    """
    phase_key = _PHASE_KEY.format(draft_id)
    existing = repo.load_parameters(draft_id)
    draft = repo.load_draft(draft_id) or {}
    parent_type_lookup = None
    parent_draft_id = draft.get("parent_draft_id")
    if parent_draft_id:
        parent_params = repo.load_parameters(parent_draft_id)
        parent_type_lookup = _build_parent_type_lookup(parent_params)

    changed_count = sum(
        1
        for p in existing
        if any(_row_change_flags(p, parent_type_lookup))
    )
    total = len(existing)

    badge_col, back_col = st.columns([3, 1])
    with badge_col:
        if changed_count > 0:
            st.markdown(
                f'<div style="background:#FEF3C7;border:1px solid #F59E0B;'
                f'padding:0.45rem 1rem;border-radius:8px;display:inline-block;'
                f'font-size:0.88rem;font-weight:700;color:#92400E;">'
                f'⚠ {changed_count} / {total} parameters have detected changes</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div style="background:#F0FDF4;border:1px solid #86EFAC;'
                f'padding:0.45rem 1rem;border-radius:8px;display:inline-block;'
                f'font-size:0.88rem;color:#166534;">'
                f'✓ {total} parameters verified · no changes proposed yet</div>',
                unsafe_allow_html=True,
            )
    with back_col:
        if allow_back:
            if st.button(
                "← Back to Step 1",
                key=f"back_p1_{draft_id}",
                use_container_width=True,
                help="Return to Step 1 to further correct existing values or parameter names",
            ):
                _clear_param_editor_state(draft_id, "p2")
                st.session_state[phase_key] = 1
                st.rerun()

    st.info(
        "**Step 2 — Propose Changes.**  "
        "Edit any parameter fields to record your changes.  "
        "Fill in **Justification** for every changed row.  "
        "Changes are detected when proposed value differs from existing value, "
        "or when Type / Test Procedure fields differ from the parent master specification.",
        icon="📝",
    )

    normalized_rows: list[dict[str, Any]] = []
    for p in existing:
        row = dict(p)
        row["test_procedure_type"] = normalize_test_procedure_type(
            _cell_text(row, "test_procedure_type").strip()
        )
        row["test_procedure_text"] = _cell_text(
            row, "test_procedure_text", _cell_text(row, "test_method")
        )
        normalized_rows.append(row)
    display_cols = [
        "parameter_name", "existing_value", "proposed_value",
        "parameter_type", "test_procedure_type", "test_procedure_text", "justification", "remarks",
    ]
    all_cols = [_PARAM_ID_COL] + display_cols
    if normalized_rows:
        raw_df = pd.DataFrame(normalized_rows)
        if _PARAM_ID_COL not in raw_df.columns:
            raw_df[_PARAM_ID_COL] = ""
        df = raw_df[all_cols]
    else:
        df = pd.DataFrame(columns=all_cols)
    for col in display_cols:
        df[col] = df[col].fillna("").astype(str)
    phase_tag = "p2"
    editor_state_key = _param_editor_state_key(draft_id, phase_tag)
    editor_widget_key = _param_editor_widget_key(draft_id, phase_tag)
    if editor_state_key not in st.session_state:
        st.session_state[editor_state_key] = _prepare_param_editor_df(df)
    widget_state_df = st.session_state.get(editor_widget_key)
    source_df = (
        widget_state_df
        if isinstance(widget_state_df, pd.DataFrame)
        else st.session_state[editor_state_key]
    )
    working_df = _prepare_param_editor_df(source_df)

    column_config = {
        _PARAM_ID_COL: None,  # hide identity column from user
        _PARAM_SL_COL: st.column_config.NumberColumn(
            "S. No.",
            disabled=True,
            width="small",
        ),
        _PARAM_MOVE_COL: st.column_config.CheckboxColumn(
            "Move",
            help="Select one row, then use Move Up / Move Down.",
            width="small",
        ),
        "parameter_name": st.column_config.TextColumn(
            "Parameter Name",
            help="Edit parameter name if needed.",
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "existing_value": st.column_config.TextColumn(
            "Existing Req. (Verified)",
            help="Editable requirement value.",
            width="medium",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "proposed_value": st.column_config.TextColumn(
            "Proposed Req. ✏️",
            help="Enter the proposed new value. Leave identical to Existing if no change.",
            width="medium",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "parameter_type": st.column_config.SelectboxColumn(
            "Type ✏️",
            options=PARAMETER_TYPES,
            required=True,
            width="small",
            help="Essential / Desirable",
        ),
        "test_procedure_type": st.column_config.SelectboxColumn(
            "Test Procedure Type ✏️",
            options=_TEST_PROCEDURE_TYPE_OPTIONS,
            required=False,
            help="Select the procedure family.",
            width="medium",
        ),
        "test_procedure_text": st.column_config.TextColumn(
            "Test Procedure Text ✏️",
            help="Editable full procedure reference text.",
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "justification": st.column_config.TextColumn(
            "Justification ✏️",
            help="Required for every changed row — state the technical rationale",
            width="large",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
        "remarks": st.column_config.TextColumn(
            "Remarks",
            width="medium",
            max_chars=_PARAM_CELL_MAX_CHARS,
        ),
    }

    edited_df = st.data_editor(
        working_df,
        column_order=[_PARAM_SL_COL, _PARAM_MOVE_COL] + display_cols,
        column_config=column_config,
        num_rows="dynamic",
        use_container_width=False,
        height=_param_editor_height(len(working_df)),
        hide_index=True,
        key=editor_widget_key,
        row_height=_PARAM_EDITOR_ROW_HEIGHT,
    )
    edited_df = _prepare_param_editor_df(edited_df)
    st.session_state[editor_state_key] = edited_df

    move_up_col, move_down_col, move_note_col = st.columns([1, 1, 3])
    with move_up_col:
        if st.button("⬆ Move Up", key=f"move_up_p2_{draft_id}", use_container_width=True):
            moved_df, err = _move_selected_parameter_row(edited_df, direction=-1)
            if err:
                st.warning(err)
            else:
                st.session_state[editor_state_key] = moved_df
                st.session_state.pop(editor_widget_key, None)
                st.rerun()
    with move_down_col:
        if st.button("⬇ Move Down", key=f"move_down_p2_{draft_id}", use_container_width=True):
            moved_df, err = _move_selected_parameter_row(edited_df, direction=1)
            if err:
                st.warning(err)
            else:
                st.session_state[editor_state_key] = moved_df
                st.session_state.pop(editor_widget_key, None)
                st.rerun()
    with move_note_col:
        st.caption("Row order controls update S. No. automatically based on final sorting.")

    # Warn about changed rows missing justification
    missing_just: list[str] = []
    for _, row in edited_df.iterrows():
        value_changed, type_changed = _row_change_flags(row, parent_type_lookup)
        if (value_changed or type_changed) and not _cell_text(row, "justification").strip():
            name = _cell_text(row, "parameter_name").strip()
            if name:
                missing_just.append(name[:40])
    if missing_just:
        st.warning(
            f"**{len(missing_just)} changed row(s) missing justification:**  \n"
            + "  \n".join(f"• {n}" for n in missing_just),
            icon="⚠️",
        )

    st.markdown("")
    col_save2, col_reset2 = st.columns([1, 2])

    with col_save2:
        if st.button(
            "Save Parameters",
            key=f"save_p2_{draft_id}",
            type="primary",
            use_container_width=True,
        ):
            params = _rows_to_params(edited_df)
            if params:
                try:
                    repo.upsert_parameters(draft_id, params, user_name)
                    _clear_param_editor_state(draft_id, phase_tag)
                    n_changed = sum(
                        1
                        for p in params
                        if any(_row_change_flags(p, parent_type_lookup))
                    )
                    msg = f"Saved {len(params)} parameter(s)."
                    if n_changed:
                        msg += f"  **{n_changed}** with detected changes."
                    st.success(msg)
                    _advance_to_next_section(SECTION_PARAMETERS)
                except Exception as exc:
                    st.error(f"Save failed: {exc}")
                    logger.exception("Phase 2 save error: %s", exc)
            else:
                st.error("No valid rows to save.")

    with col_reset2:
        if st.button(
            "Reset all Proposed → Existing",
            key=f"reset_p2_{draft_id}",
            help="Clears all proposed values back to the verified existing spec values",
        ):
            reset_params = [
                {**p, "proposed_value": p.get("existing_value", "")}
                for p in existing
            ]
            try:
                repo.upsert_parameters(draft_id, reset_params, user_name)
                _clear_param_editor_state(draft_id, phase_tag)
                st.success("All proposed values reset to existing spec values.")
                st.rerun()
            except Exception as exc:
                st.error(f"Reset failed: {exc}")


def _rows_to_params(edited_df: "pd.DataFrame") -> list[dict]:
    """Convert a data_editor DataFrame to parameter dicts, skipping blank rows.

    Each dict carries ``parameter_id`` (empty string for new rows) so that
    the repository can UPDATE existing rows and INSERT only genuinely new ones.
    """
    def _txt(row: pd.Series, key: str, default: str = "") -> str:
        val = row.get(key, default)
        if pd.isna(val):
            return default
        return str(val or default)

    params = []
    for _, row in edited_df.iterrows():
        name = _txt(row, "parameter_name").strip()
        if not name:
            continue
        params.append({
            "parameter_id":  _txt(row, _PARAM_ID_COL),   # "" for new rows
            "parameter_name": name,
            "parameter_type": _txt(row, "parameter_type", "Essential"),
            "existing_value": _txt(row, "existing_value"),
            "proposed_value": _txt(row, "proposed_value"),
            "test_method":    _txt(row, "test_procedure_text", _txt(row, "test_method")),
            "test_procedure_type": normalize_test_procedure_type(_txt(row, "test_procedure_type")),
            "test_procedure_text": _txt(row, "test_procedure_text", _txt(row, "test_method")),
            "justification":  _txt(row, "justification"),
            "remarks":        _txt(row, "remarks"),
        })
    return params



# ---------------------------------------------------------------------------
# Section 5 — Proposed Changes
# ---------------------------------------------------------------------------

def render_section_proposed(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _section_header("5. Proposed Changes")

    _guidance(SECTION_GUIDANCE[SECTION_PROPOSED])

    existing = _load_section_text(repo, draft_id, SECTION_PROPOSED)

    # Optional structured checkboxes
    st.markdown("**Nature of Changes** *(select all that apply)*")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        chk_add = st.checkbox("Addition",   key=f"chk_add_{draft_id}")
    with c2:
        chk_del = st.checkbox("Deletion",   key=f"chk_del_{draft_id}")
    with c3:
        chk_rev = st.checkbox("Revision",   key=f"chk_rev_{draft_id}")
    with c4:
        chk_cla = st.checkbox("Clarification", key=f"chk_cla_{draft_id}")

    st.markdown("**Summary of Proposed Changes**")
    text = st.text_area(
        "Proposed changes text",
        value=existing,
        height=280,
        placeholder="Summarize the proposed changes…",
        key=f"section_text_{draft_id}_{SECTION_PROPOSED}",
        label_visibility="collapsed",
    )

    if _save_button("Save Section", key=f"save_{draft_id}_{SECTION_PROPOSED}"):
        # Prepend selected change types as a brief header in the saved text
        types_selected = []
        if chk_add: types_selected.append("Addition")
        if chk_del: types_selected.append("Deletion")
        if chk_rev: types_selected.append("Revision")
        if chk_cla: types_selected.append("Clarification")
        prefix = ""
        if types_selected:
            prefix = f"Nature of Changes: {', '.join(types_selected)}\n\n"
        full_text = prefix + text
        try:
            repo.upsert_section(draft_id, SECTION_PROPOSED, full_text, 4, user_name)
            st.success("Section saved.")
            _advance_to_next_section(SECTION_PROPOSED)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
            logger.exception("Section save error: %s", exc)


# ---------------------------------------------------------------------------
# Section 6 — Justification
# ---------------------------------------------------------------------------

def render_section_justification(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _text_section(
        repo, draft_id, user_name,
        section_name=SECTION_JUSTIFICATION,
        section_index=5,
        title="6. Justification",
        guidance=SECTION_GUIDANCE[SECTION_JUSTIFICATION],
    )


# ---------------------------------------------------------------------------
# Section 7 — Impact Analysis
# ---------------------------------------------------------------------------

def render_section_impact(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _section_header("7. Impact Analysis", "Score each factor and capture supporting notes.")
    existing = repo.load_impact_analysis(draft_id) or {}

    score_options: list[int | None] = [None, 1, 2, 3, 4, 5]

    def _score_index(current: Any) -> int:
        try:
            cur = int(current)
            return score_options.index(cur) if cur in score_options else 0
        except Exception:
            return 0

    st.markdown("##### 1. Operational Impact")
    operational_score = st.selectbox(
        "Operational Impact",
        options=score_options,
        index=_score_index(existing.get("operational_impact_score")),
        format_func=lambda x: "Select score" if x is None else str(x),
        key=f"impact_operational_score_{draft_id}",
    )
    with st.expander("Operational Impact scoring guidance", expanded=False):
        st.markdown(
            "1 = Negligible operational effect\n\n"
            "2 = Minor operational effect\n\n"
            "3 = Moderate operational effect\n\n"
            "4 = Significant operational disruption\n\n"
            "5 = Operations stop / major production or drilling impact"
        )
    operational_note = st.text_area(
        "Operational Impact Note",
        value=str(existing.get("operational_note", "") or ""),
        height=90,
        key=f"impact_operational_note_{draft_id}",
    )

    st.markdown("##### 2. Safety / Environmental Impact")
    safety_score = st.selectbox(
        "Safety / Environmental Impact",
        options=score_options,
        index=_score_index(existing.get("safety_environment_score")),
        format_func=lambda x: "Select score" if x is None else str(x),
        key=f"impact_safety_score_{draft_id}",
    )
    with st.expander("Safety / Environmental scoring guidance", expanded=False):
        st.markdown(
            "1 = No meaningful safety or environmental impact\n\n"
            "2 = Minor concern\n\n"
            "3 = Moderate concern\n\n"
            "4 = Serious safety / compliance concern\n\n"
            "5 = Major safety or environmental risk"
        )
    safety_note = st.text_area(
        "Safety / Environmental Note",
        value=str(existing.get("safety_environment_note", "") or ""),
        height=90,
        key=f"impact_safety_note_{draft_id}",
    )

    st.markdown("##### 3. Supply Risk")
    supply_score = st.selectbox(
        "Supply Risk",
        options=score_options,
        index=_score_index(existing.get("supply_risk_score")),
        format_func=lambda x: "Select score" if x is None else str(x),
        key=f"impact_supply_score_{draft_id}",
    )
    with st.expander("Supply Risk scoring guidance", expanded=False):
        st.markdown(
            "1 = Commodity / easily available\n\n"
            "2 = Manageable supply\n\n"
            "3 = Moderate dependence\n\n"
            "4 = Limited suppliers / long lead time\n\n"
            "5 = Highly constrained / import dependent / single source"
        )
    supply_note = st.text_area(
        "Supply Risk Note",
        value=str(existing.get("supply_risk_note", "") or ""),
        height=90,
        key=f"impact_supply_note_{draft_id}",
    )

    st.markdown("##### 4. No Substitute Available")
    no_sub_default_yes = bool(int(existing.get("no_substitute_flag", 0) or 0))
    no_sub_choice = st.radio(
        "No Substitute Available",
        options=["No", "Yes"],
        index=1 if no_sub_default_yes else 0,
        horizontal=True,
        help="Select Yes only if no technically acceptable substitute exists for the intended application.",
        key=f"impact_no_substitute_{draft_id}",
    )
    no_substitute_flag = 1 if no_sub_choice == "Yes" else 0

    st.markdown("---")
    st.markdown("#### Live Impact Summary")

    if None in {operational_score, safety_score, supply_score}:
        st.info("Select all three scores to compute Total Impact Score and Grade.")
        total_score = None
        computed_grade = None
        final_grade = None
        override_applied = False
    else:
        op_weighted = float(operational_score) * 0.5
        se_weighted = float(safety_score) * 0.3
        sr_weighted = float(supply_score) * 0.2
        total_score = round(calculate_impact_score(int(operational_score), int(safety_score), int(supply_score)), 2)
        computed_grade = get_impact_grade(total_score, 0)
        final_grade = get_impact_grade(total_score, no_substitute_flag)
        override_applied = bool(
            no_substitute_flag == 1
            and computed_grade in {"LOW", "MODERATE"}
            and final_grade == "HIGH"
        )

        st.markdown(
            f"""
<div style="background:#F8FAFC;border:1px solid #CBD5E1;padding:0.85rem 1rem;border-radius:8px;">
<div><strong>Operational Impact:</strong> {operational_score} × 0.5 = {op_weighted:.2f}</div>
<div><strong>Safety / Environmental Impact:</strong> {safety_score} × 0.3 = {se_weighted:.2f}</div>
<div><strong>Supply Risk:</strong> {supply_score} × 0.2 = {sr_weighted:.2f}</div>
<div style="margin-top:.4rem;"><strong>Total Impact Score = {total_score:.2f}</strong></div>
<div><strong>Computed Grade = {computed_grade}</strong></div>
{"<div style='color:#92400E;font-weight:700;'>No Substitute Override Applied → Final Grade raised to HIGH</div>" if override_applied else ""}
<div style="margin-top:.2rem;"><strong>Final Impact Grade = {final_grade}</strong></div>
</div>
""",
            unsafe_allow_html=True,
        )

    if _save_button("Save Impact Analysis", key=f"save_impact_{draft_id}"):
        if None in {operational_score, safety_score, supply_score}:
            st.error("Please select all three impact scores before saving.")
            return
        try:
            repo.save_impact_analysis(
                draft_id=draft_id,
                operational_impact_score=int(operational_score),
                safety_environment_score=int(safety_score),
                supply_risk_score=int(supply_score),
                no_substitute_flag=no_substitute_flag,
                operational_note=operational_note,
                safety_environment_note=safety_note,
                supply_risk_note=supply_note,
                reviewed_by=user_name,
            )
            st.success("Impact Analysis saved.")
            _advance_to_next_section(SECTION_IMPACT)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
            logger.exception("Impact save error: %s", exc)


# ---------------------------------------------------------------------------
# Section 8 — Committee Recommendation
# ---------------------------------------------------------------------------

def render_section_recommendation(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _section_header(
        "8. Committee Recommendation",
        "Record the committee's recommendation and any additional remarks.",
    )

    all_sections = repo.load_sections(draft_id)
    section_map = {s["section_name"]: s.get("section_text", "") or "" for s in all_sections}

    rec_note = st.text_area(
        "Recommendation Note",
        value=section_map.get(REC_MAIN_KEY, ""),
        height=160,
        placeholder="State the committee's recommendation…",
        key=f"rec_main_{draft_id}",
    )

    remarks = st.text_area(
        "Additional Remarks",
        value=section_map.get(REC_REMARKS_KEY, ""),
        height=120,
        placeholder="Any additional remarks, conditions, or follow-up actions…",
        key=f"rec_remarks_{draft_id}",
    )

    if _save_button("Save Recommendation", key=f"save_rec_{draft_id}"):
        try:
            repo.upsert_section(draft_id, REC_MAIN_KEY,    rec_note, 70, user_name)
            repo.upsert_section(draft_id, REC_REMARKS_KEY, remarks,  71, user_name)
            st.success("Recommendation saved.")
            _advance_to_next_section(SECTION_RECOMMENDATION)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
            logger.exception("Recommendation save error: %s", exc)


# ---------------------------------------------------------------------------
# Section 9 — Preview & Export
# ---------------------------------------------------------------------------

def render_section_preview_export(
    repo: CSCRepository, draft_id: str, user_name: str
) -> None:
    _section_header("9. Preview & Export", "Review the complete draft before export.")

    draft      = repo.load_draft(draft_id)
    sections   = repo.load_sections(draft_id)
    parameters = repo.load_parameters(draft_id)
    flags      = repo.load_issue_flags(draft_id)
    impact_analysis = repo.load_impact_analysis(draft_id)

    if not draft:
        st.error("Draft not found.")
        return

    section_map = {s["section_name"]: s.get("section_text", "") or "" for s in sections}

    # ---- Document preview ---------------------------------------------------
    with st.expander("Document Preview", expanded=True):
        _render_preview(draft, section_map, parameters, flags, impact_analysis)

    # ---- Audit trail --------------------------------------------------------
    with st.expander("Audit Trail", expanded=False):
        audit_rows = repo.load_audit(draft_id, limit=30)
        if audit_rows:
            for a in audit_rows:
                st.caption(
                    f"{fmt_ist(a['action_time'])} — **{a['action']}** by {a['user_name']}"
                    + (f" | {a['remarks']}" if a.get("remarks") else "")
                )
        else:
            st.caption("No audit entries yet.")

    st.markdown("---")

    # ---- Action buttons -----------------------------------------------------
    col_save, col_export, col_delete = st.columns(3)
    is_revision = bool(draft.get("parent_draft_id")) and not bool(draft.get("is_admin_draft"))
    current_status = (draft.get("status") or "Draft").strip()

    with col_save:
        if is_revision:
            if current_status == "Pending Approval":
                st.info("This revision is pending admin approval.", icon="⏳")
            elif current_status == "Approved":
                st.success("This revision has been approved and published by admin.", icon="✅")
            else:
                if st.button(
                    "Submit for Admin Approval",
                    key=f"submit_for_approval_{draft_id}",
                    type="primary",
                    use_container_width=True,
                ):
                    try:
                        ok = repo.submit_revision_for_approval(draft_id, user_name)
                        if ok:
                            st.success("Submitted for admin approval.")
                            st.rerun()
                        else:
                            st.error("Could not submit this revision.")
                    except Exception as exc:
                        st.error(f"Failed: {exc}")
                        logger.exception("Submit for approval failed: %s", exc)
        else:
            if st.button(
                "Mark as Final Draft",
                key=f"finalize_{draft_id}",
                type="primary",
                use_container_width=True,
            ):
                try:
                    repo.update_draft_metadata(
                        draft_id,
                        spec_number    = draft["spec_number"],
                        chemical_name  = draft["chemical_name"],
                        committee_name = draft["committee_name"],
                        meeting_date   = draft.get("meeting_date", ""),
                        prepared_by    = draft.get("prepared_by", ""),
                        reviewed_by    = draft.get("reviewed_by", ""),
                        status         = "Final",
                        user_name      = user_name,
                    )
                    repo.log_audit(draft_id, "FINALIZE", user_name)
                    st.success("Draft marked as Final.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed: {exc}")

    with col_export:
        # Build the Word bytes; generate the download button
        try:
            doc_bytes = build_word_document(
                draft,
                sections,
                parameters,
                flags,
                impact_analysis=impact_analysis,
            )
            date_str  = datetime.now().strftime("%Y%m%d")
            spec_safe = "".join(
                c if c.isalnum() or c in "-_" else "_"
                for c in draft.get("spec_number", "DRAFT")
            )
            filename = f"CSC_DRAFT_{spec_safe}_{date_str}.docx"

            st.download_button(
                label="Export Word Document (.docx)",
                data=doc_bytes,
                file_name=filename,
                mime=(
                    "application/vnd.openxmlformats-officedocument"
                    ".wordprocessingml.document"
                ),
                key=f"export_word_{draft_id}",
                use_container_width=True,
            )
            # Log export on button render (Streamlit download triggers a rerun)
        except Exception as exc:
            st.error(f"Word export failed: {exc}")
            logger.exception("Word export error: %s", exc)

    with col_delete:
        if is_revision and current_status in {"Draft", "Rejected", "Pending Approval"}:
            delete_confirm = st.checkbox(
                "Confirm delete",
                key=f"del_revision_confirm_{draft_id}",
            )
            if st.button(
                "Delete This Revision Draft",
                key=f"del_revision_btn_{draft_id}",
                use_container_width=True,
                disabled=not delete_confirm,
            ):
                try:
                    ok = repo.delete_user_revision_draft(draft_id, user_name)
                    if ok:
                        st.success("Revision draft deleted.")
                        st.session_state["csc_active_draft_id"] = None
                        st.session_state["csc_active_section"] = SECTION_BACKGROUND
                        st.session_state["csc_draft_list_cache"] = None
                        st.rerun()
                    else:
                        st.error("Delete failed. You can only delete your own revision drafts.")
                except Exception as exc:
                    st.error(f"Delete failed: {exc}")
                    logger.exception("Delete revision draft failed: %s", exc)


# ---------------------------------------------------------------------------
# Document preview renderer (Streamlit markdown / tables)
# ---------------------------------------------------------------------------

def _render_preview(
    draft: dict[str, Any],
    section_map: dict[str, str],
    parameters: list[dict[str, Any]],
    flags: list[dict[str, Any]],
    impact_analysis: dict[str, Any] | None = None,
) -> None:
    """Render a styled document preview within the Streamlit UI."""

    # Metadata block
    st.markdown(
        f"""
<div style="background:#FFF7F3;border-left:4px solid #A54B23;padding:1rem 1.2rem;
            border-radius:8px;margin-bottom:1rem;">
<h3 style="margin:0 0 0.4rem;color:#A54B23;">Specification Review Committee - Draft Note</h3>
<table style="width:100%;font-size:0.9rem;border-collapse:collapse;">
  <tr>
    <td style="width:140px;font-weight:700;padding:2px 8px 2px 0;">Specification No.</td>
    <td>{draft.get('spec_number','—')}</td>
    <td style="width:140px;font-weight:700;padding:2px 8px 2px 1rem;">Prepared By</td>
    <td>{draft.get('prepared_by','—')}</td>
  </tr>
  <tr>
    <td style="font-weight:700;padding:2px 8px 2px 0;">Chemical</td>
    <td>{draft.get('chemical_name','—')}</td>
    <td style="font-weight:700;padding:2px 8px 2px 1rem;">Reviewed By</td>
    <td>{draft.get('reviewed_by','—')}</td>
  </tr>
  <tr>
    <td style="font-weight:700;padding:2px 8px 2px 0;">Committee</td>
    <td>{draft.get('committee_name','—')}</td>
    <td style="font-weight:700;padding:2px 8px 2px 1rem;">Meeting Date</td>
    <td>{draft.get('meeting_date','—')}</td>
  </tr>
  <tr>
    <td style="font-weight:700;padding:2px 8px 2px 0;">Status</td>
    <td><strong>{draft.get('status','Draft')}</strong></td>
    <td></td><td></td>
  </tr>
</table>
</div>
""",
        unsafe_allow_html=True,
    )

    # Text sections
    text_sections = [
        (SECTION_BACKGROUND,     "1. Background"),
        (SECTION_EXISTING_SPEC,  "2. Existing Specification Summary"),
        (SECTION_PROPOSED,       "5. Proposed Changes"),
        (SECTION_JUSTIFICATION,  "6. Justification"),
    ]
    for key, heading in text_sections:
        text = section_map.get(key, "").strip()
        if text:
            st.markdown(f"#### {heading}")
            st.markdown(text)

    # Issues Observed
    st.markdown("#### 3. Issues Observed")
    flag_map = {f["issue_type"]: f for f in flags}
    for key, label in ISSUE_TYPES:
        f = flag_map.get(key, {})
        present = "**Yes**" if f.get("is_present") else "No"
        note    = f" — {f['note']}" if f.get("is_present") and f.get("note") else ""
        st.markdown(f"- {label}: {present}{note}")

    # Parameter table
    if parameters:
        st.markdown("#### 4. Parameter Review")
        preview_rows: list[dict[str, Any]] = []
        for p in parameters:
            row = dict(p)
            row["test_procedure_type"] = normalize_test_procedure_type(
                _cell_text(row, "test_procedure_type").strip()
            )
            row["test_procedure_text"] = _cell_text(
                row, "test_procedure_text", _cell_text(row, "test_method")
            )
            preview_rows.append(row)
        param_df = pd.DataFrame(preview_rows)[[
            "parameter_name", "existing_value", "proposed_value",
            "parameter_type", "test_procedure_type", "test_procedure_text",
            "justification", "remarks",
        ]].rename(columns={
            "parameter_name": "Parameter",
            "existing_value": "Existing",
            "proposed_value": "Proposed",
            "parameter_type": "Type",
            "test_procedure_type": "Test Procedure Type",
            "test_procedure_text": "Test Procedure Text",
            "justification":  "Justification",
            "remarks":        "Remarks",
        })
        st.dataframe(param_df, use_container_width=True, hide_index=True)

    # Impact Analysis
    if impact_analysis:
        st.markdown("#### 7. Impact Analysis")
        st.markdown(
            f"- **Operational Impact Score:** {impact_analysis.get('operational_impact_score', '—')}"
        )
        st.markdown(
            f"- **Operational Impact Note:** {(impact_analysis.get('operational_note', '') or '—').strip() or '—'}"
        )
        st.markdown(
            f"- **Safety / Environmental Score:** {impact_analysis.get('safety_environment_score', '—')}"
        )
        st.markdown(
            f"- **Safety / Environmental Note:** {(impact_analysis.get('safety_environment_note', '') or '—').strip() or '—'}"
        )
        st.markdown(
            f"- **Supply Risk Score:** {impact_analysis.get('supply_risk_score', '—')}"
        )
        st.markdown(
            f"- **Supply Risk Note:** {(impact_analysis.get('supply_risk_note', '') or '—').strip() or '—'}"
        )
        st.markdown(
            f"- **No Substitute Available:** {'Yes' if int(impact_analysis.get('no_substitute_flag', 0) or 0) == 1 else 'No'}"
        )
        st.markdown(
            f"- **Total Impact Score:** {float(impact_analysis.get('impact_score_total', 0.0) or 0.0):.2f}"
        )
        st.markdown(
            f"- **Final Impact Grade:** {impact_analysis.get('impact_grade', '—')}"
        )
        if bool(impact_analysis.get("override_applied")):
            st.markdown("- **Note:** No Substitute Available override applied.")

    # Recommendation
    rec_note = section_map.get(REC_MAIN_KEY, "").strip()
    remarks  = section_map.get(REC_REMARKS_KEY, "").strip()
    if rec_note or remarks:
        st.markdown("#### 8. Committee Recommendation")
        if rec_note:
            st.markdown(f"**Recommendation:** {rec_note}")
        if remarks:
            st.markdown(f"**Additional Remarks:** {remarks}")
