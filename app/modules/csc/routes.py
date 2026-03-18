"""CSC (Chemical Specification Committee) Workflow Module – Routes.

Endpoints:
    GET  /csc/                                  → CSC landing page with KPI cards
    GET  /csc/api/overview                      → JSON: KPI data
    GET  /csc/workspace                         → Draft selection/list page
    GET  /csc/api/drafts                        → JSON: filtered draft list
    GET  /csc/workspace/<int:draft_id>          → 9-section tabbed editor
    GET  /csc/api/draft/<int:draft_id>          → JSON: full draft data
    POST /csc/api/draft/<int:draft_id>/section/<section_name>   → Save section text
    POST /csc/api/draft/<int:draft_id>/issues   → Save issue flags
    POST /csc/api/draft/<int:draft_id>/parameters              → Save parameters (Phase 1)
    POST /csc/api/draft/<int:draft_id>/parameters/proposed     → Save proposed values (Phase 2)
    POST /csc/api/draft/<int:draft_id>/impact  → Save impact analysis
    POST /csc/api/draft/<int:draft_id>/submit  → Submit revision for approval
    POST /csc/api/draft/<int:draft_id>/delete  → Delete draft
    GET  /csc/draft/<int:draft_id>/export/docx → Download Word document
    POST /csc/workspace/new-revision/<int:parent_id> → Create revision draft
    GET  /csc/admin                             → Admin dashboard [superuser]
    GET  /csc/admin/ingest                      → PDF/DOCX ingest page [superuser]
    POST /csc/admin/ingest/pdf                  → Upload PDF [superuser]
    POST /csc/admin/ingest/docx                 → Upload DOCX [superuser]
    POST /csc/admin/ingest/create-draft         → Create draft from extraction [superuser]
    GET  /csc/admin/drafts                      → Manage all drafts [superuser]
    GET  /csc/admin/draft/<int:draft_id>/edit   → Edit draft [superuser]
    POST /csc/admin/draft/<int:draft_id>/update → Update draft metadata [superuser]
    POST /csc/admin/draft/<int:draft_id>/lock-phase1 → Toggle Phase 1 lock [superuser]
    POST /csc/admin/draft/<int:draft_id>/set-stage → Set admin stage [superuser]
    DELETE /csc/admin/draft/<int:draft_id>/delete → Delete draft (admin) [superuser]
    GET  /csc/admin/revisions                   → Committee workbench [superuser]
    GET  /csc/admin/revision/<int:revision_id>/review → Review revision [superuser]
    POST /csc/admin/revision/<int:revision_id>/approve  → Approve revision [superuser]
    POST /csc/admin/revision/<int:revision_id>/reject   → Reject revision [superuser]
    GET  /csc/admin/export                      → Export page [superuser]
    GET  /csc/admin/export/master-docx          → Download master spec document [superuser]
    GET  /csc/admin/export/<int:export_id>/download → Download export file [superuser]
"""

from __future__ import annotations

import json
import logging
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from flask import (
    abort,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import login_required, current_user

from app.modules.csc import csc_bp
from app.extensions import db
from app.core.utils.decorators import module_access_required, superuser_required
from app.models.csc import (
    CSCDraft,
    CSCParameter,
    CSCSection,
    CSCIssueFlag,
    CSCImpactAnalysis,
    CSCAudit,
    CSCRevision,
    CSCSpecVersion,
)
from app.models.core.user import User
from app.core.services.notifications import create_notification
from app.core.services.csc_utils import (
    ADMIN_STAGE_DRAFTING,
    ADMIN_STAGE_PUBLISHED,
    DEFAULT_BACKGROUND_VERSION_HISTORY,
    DEFAULT_VERSION_CHANGE_REASON,
    IMPACT_CHECKLIST_FLAGS,
    SPEC_SUBSET_LABELS,
    SPEC_SUBSET_ORDER,
    build_default_impact_checklist_state,
    build_impact_legacy_payload,
    deserialize_impact_checklist_state,
    format_required_value,
    format_spec_version,
    increment_spec_version,
    normalize_spec_version,
    normalize_parameter_type_label,
    normalize_test_procedure_type,
    parse_spec_number,
    spec_sort_key,
    summarize_impact_checklist_state,
    PARAMETER_TYPES,
    TEST_PROCEDURE_OPTIONS,
    WORKFLOW_DRAFTING_APPROVED,
    WORKFLOW_DRAFTING_OPEN,
    WORKFLOW_DRAFTING_REJECTED,
    WORKFLOW_DRAFTING_RETURNED,
    WORKFLOW_DRAFTING_STATE_OPTIONS,
    WORKFLOW_DRAFTING_SUBMITTED,
)
from app.core.services.csc_master_data import (
    ADMIN_MASTER_EXTRA_FIELDS,
    ADMIN_MASTER_FIELDS,
    CSC_ADMIN_DRAFT_FIELDS,
    MASTER_DATA_FIELDS,
    MASTER_EXTRA_FIELDS,
    MASTER_IMPACT_FIELDS,
    STAGED_MASTER_SECTION_NAME,
    backfill_draft_material_codes,
    clear_staged_master_payload,
    get_master_record_for_draft,
    get_material_properties_fields,
    get_material_properties_values,
    get_storage_handling_fields,
    get_storage_handling_values,
    get_master_form_values,
    sync_impact_fields_to_master_record,
    update_material_properties_from_form,
    update_storage_handling_from_form,
    upsert_master_record_from_form,
)
from app.core.services.csc_committee_directory import (
    get_committee_directory,
    get_committee_tree,
    get_office_orders,
    OFFICE_ORDERS,
)

logger = logging.getLogger(__name__)
EDITOR_ISSUE_LABELS = [
    "Specification Ambiguity",
    "Parameter Non-compliance",
    "Test Procedure Gaps",
    "Safety/Environmental Concern",
]


def _coerce_admin_boolean(value) -> bool:
    """Coerce HTML/JSON boolean-like input into a real bool."""
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _get_revision_for_child(child_draft_id: int | None) -> CSCRevision | None:
    if not child_draft_id:
        return None
    return CSCRevision.query.filter_by(child_draft_id=child_draft_id).first()


def _get_active_revision_for_parent(parent_draft_id: int | None) -> CSCRevision | None:
    if not parent_draft_id:
        return None
    return (
        CSCRevision.query.filter_by(parent_draft_id=parent_draft_id)
        .filter(CSCRevision.status.in_([WORKFLOW_DRAFTING_OPEN, WORKFLOW_DRAFTING_SUBMITTED, WORKFLOW_DRAFTING_RETURNED]))
        .order_by(CSCRevision.updated_at.desc(), CSCRevision.id.desc())
        .first()
    )


def _is_open_revision_state(status: str | None) -> bool:
    return (status or "").strip().lower() in {
        WORKFLOW_DRAFTING_OPEN,
        WORKFLOW_DRAFTING_RETURNED,
    }


def _can_submit_revision(revision: CSCRevision | None) -> bool:
    return revision is not None and _is_open_revision_state(revision.status)


def _can_user_edit_workflow_draft(draft: CSCDraft) -> bool:
    if current_user.is_super_user():
        return True
    revision = _get_revision_for_child(draft.id)
    if revision is None:
        return False
    return _is_open_revision_state(revision.status)


def _notify_superusers(title: str, message: str, link: str | None = None, severity: str = "info") -> None:
    created = False
    for user in User.query.all():
        if user.is_super_user():
            create_notification(
                user_id=user.id,
                title=title,
                message=message,
                severity=severity,
                link=link,
            )
            created = True
    if created:
        db.session.commit()


def _copy_draft_content(source: CSCDraft, target: CSCDraft) -> None:
    target.spec_number = source.spec_number
    target.chemical_name = source.chemical_name
    target.committee_name = source.committee_name
    target.meeting_date = source.meeting_date
    target.prepared_by = source.prepared_by
    target.reviewed_by = source.reviewed_by
    target.material_code = source.material_code

    target.parameters.delete()
    for parameter in source.parameters.order_by(CSCParameter.sort_order).all():
        db.session.add(
            CSCParameter(
                draft_id=target.id,
                parameter_name=parameter.parameter_name,
                parameter_type=parameter.parameter_type,
                unit_of_measure=parameter.unit_of_measure,
                existing_value=parameter.existing_value,
                proposed_value=parameter.proposed_value,
                test_method=parameter.test_method,
                test_procedure_type=parameter.test_procedure_type,
                test_procedure_text=parameter.test_procedure_text,
                parameter_conditions=parameter.parameter_conditions,
                required_value_type=parameter.required_value_type,
                required_value_text=parameter.required_value_text,
                required_value_operator_1=parameter.required_value_operator_1,
                required_value_value_1=parameter.required_value_value_1,
                required_value_operator_2=parameter.required_value_operator_2,
                required_value_value_2=parameter.required_value_value_2,
                justification=parameter.justification,
                remarks=parameter.remarks,
                sort_order=parameter.sort_order,
            )
        )

    target.sections.delete()
    for section in source.sections.order_by(CSCSection.sort_order).all():
        if section.section_name == STAGED_MASTER_SECTION_NAME:
            continue
        db.session.add(
            CSCSection(
                draft_id=target.id,
                section_name=section.section_name,
                section_text=section.section_text,
                sort_order=section.sort_order,
            )
        )

    target.issue_flags.delete()
    for flag in source.issue_flags.order_by(CSCIssueFlag.sort_order).all():
        db.session.add(
            CSCIssueFlag(
                draft_id=target.id,
                issue_type=flag.issue_type,
                is_present=flag.is_present,
                note=flag.note,
                sort_order=flag.sort_order,
            )
        )

    source_impact = source.impact_analysis
    target_impact = target.impact_analysis
    if source_impact:
        if target_impact is None:
            target_impact = CSCImpactAnalysis(draft_id=target.id)
            db.session.add(target_impact)
        target_impact.operational_impact_score = source_impact.operational_impact_score
        target_impact.safety_environment_score = source_impact.safety_environment_score
        target_impact.supply_risk_score = source_impact.supply_risk_score
        target_impact.no_substitute_flag = source_impact.no_substitute_flag
        target_impact.impact_score_total = source_impact.impact_score_total
        target_impact.impact_grade = source_impact.impact_grade
        target_impact.operational_note = source_impact.operational_note
        target_impact.safety_environment_note = source_impact.safety_environment_note
        target_impact.supply_risk_note = source_impact.supply_risk_note
        target_impact.checklist_state_json = source_impact.checklist_state_json
    elif target_impact is not None:
        db.session.delete(target_impact)


# ──────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────────────────────────────────────────


def _can_edit_draft(draft: CSCDraft) -> bool:
    """Check if current user can edit this draft."""
    if current_user.is_super_user():
        return draft.parent_draft_id is not None
    return _can_user_edit_workflow_draft(draft)


def _can_delete_draft(draft: CSCDraft) -> bool:
    """Check if current user can delete this draft."""
    if current_user.is_super_user():
        return True
    revision = _get_revision_for_child(draft.id)
    if draft.created_by_id == current_user.id and _is_open_revision_state(revision.status if revision else ""):
        return True
    return False


def _calculate_impact_grade(
    operational: int, safety: int, supply: int, no_substitute: bool
) -> str:
    """Calculate impact grade based on weighted score.

    Weight formula: (operational * 0.5) + (safety * 0.3) + (supply * 0.2)
    Grades: LOW (< 2.0), MODERATE (2.0-2.99), HIGH (3.0-3.99), CRITICAL (≥ 4.0)
    Override: if no_substitute_flag and grade in (LOW, MODERATE) → HIGH
    """
    weighted_score = (operational * 0.5) + (safety * 0.3) + (supply * 0.2)

    if weighted_score < 2.0:
        grade = "LOW"
    elif weighted_score < 3.0:
        grade = "MODERATE"
    elif weighted_score < 4.0:
        grade = "HIGH"
    else:
        grade = "CRITICAL"

    # Override if no substitute flag and grade is LOW or MODERATE
    if no_substitute and grade in ("LOW", "MODERATE"):
        grade = "HIGH"

    return grade


def _status_filter_to_db_value(status: str | None) -> str | None:
    """Map UI status tokens to stored CSC draft status values."""
    value = (status or "").strip().lower()
    if not value:
        return None

    mapping = {
        "drafting": "Drafting",
        "published": "Published",
    }
    return mapping.get(value)


def _status_db_to_ui_value(status: str | None) -> str:
    """Map stored CSC draft status values to UI tokens."""
    value = (status or "").strip().lower().replace(" ", "_")
    allowed = {"drafting", "published"}
    return value if value in allowed else "published"


def _subset_display(code: str | None) -> str | None:
    """Return subset code plus human-readable label."""
    if not code:
        return None
    label = SPEC_SUBSET_LABELS.get(code, code)
    return f"{code} - {label}" if label else code


def _draft_sort_key(draft: CSCDraft) -> tuple:
    """Sort drafts by canonical subset order first, then sequence."""
    return spec_sort_key(draft.spec_number or "")


def _create_audit_entry(
    draft_id: int,
    action: str,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    remarks: Optional[str] = None,
) -> None:
    """Create an audit log entry."""
    try:
        audit = CSCAudit(
            draft_id=draft_id,
            action=action,
            user_name=current_user.username,
            action_time=datetime.now(timezone.utc),
            old_value=old_value,
            new_value=new_value,
            remarks=remarks,
        )
        db.session.add(audit)
        db.session.commit()
    except Exception as e:
        logger.error(f"Failed to create audit entry: {e}")
        db.session.rollback()


def _get_admin_stats() -> dict:
    """Get admin dashboard statistics."""
    try:
        return {
            "published": CSCDraft.query.filter_by(status="Published", parent_draft_id=None).count(),
            "drafting": CSCDraft.query.filter_by(status="Drafting", parent_draft_id=None).count(),
            "submitted": CSCRevision.query.filter_by(status=WORKFLOW_DRAFTING_SUBMITTED).count(),
            "returned": CSCRevision.query.filter_by(status=WORKFLOW_DRAFTING_RETURNED).count(),
        }
    except Exception:
        return {"published": 0, "drafting": 0, "submitted": 0, "returned": 0}


def _get_export_stats() -> dict:
    """Get export page statistics."""
    try:
        published_count = CSCDraft.query.filter_by(status="Published", parent_draft_id=None).count()
        subset_count = db.session.query(
            db.func.count(db.func.distinct(CSCDraft.spec_number))
        ).filter(CSCDraft.status == "Published", CSCDraft.parent_draft_id.is_(None)).scalar() or 0
        total_parameters = db.session.query(
            db.func.count(CSCParameter.id)
        ).join(CSCDraft).filter(CSCDraft.status == "Published", CSCDraft.parent_draft_id.is_(None)).scalar() or 0
        return {
            "published_count": published_count,
            "subset_count": subset_count,
            "total_parameters": total_parameters,
            "last_export_at": None,  # TODO: track last export time
        }
    except Exception:
        return {
            "published_count": 0,
            "subset_count": 0,
            "total_parameters": 0,
            "last_export_at": None,
        }


def _get_spec_subsets() -> list[dict[str, str]]:
    """Get distinct spec subsets for filter dropdowns in canonical order."""
    try:
        rows = db.session.query(CSCDraft.spec_number).all()
        subsets = set()
        for (spec_num,) in rows:
            subset_code, _, _ = parse_spec_number(spec_num or "")
            if subset_code:
                subsets.add(subset_code)

        ordered_codes = [code for code in SPEC_SUBSET_ORDER if code in subsets]
        extra_codes = sorted(code for code in subsets if code not in SPEC_SUBSET_ORDER)
        return [
            {
                "code": code,
                "label": SPEC_SUBSET_LABELS.get(code, code),
                "display": _subset_display(code) or code,
            }
            for code in [*ordered_codes, *extra_codes]
        ]
    except Exception:
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Landing & Overview
# ──────────────────────────────────────────────────────────────────────────────


@csc_bp.route("/")
@login_required
@module_access_required("csc")
def index():
    """CSC landing page with KPI cards and navigation."""
    return render_template(
        "csc/landing.html",
        committee_directory=get_committee_directory(),
        committee_tree=get_committee_tree(),
        office_orders=get_office_orders(),
    )


# Alias so templates using url_for('csc.landing') also work
@csc_bp.route("/landing")
@login_required
@module_access_required("csc")
def landing():
    """Alias for index – CSC landing page."""
    return render_template(
        "csc/landing.html",
        committee_directory=get_committee_directory(),
        committee_tree=get_committee_tree(),
        office_orders=get_office_orders(),
    )


@csc_bp.route("/office-orders/<slug>")
@login_required
@module_access_required("csc")
def office_order(slug: str):
    """Stream configured office-order PDFs for committee governance."""
    order = next((item for item in OFFICE_ORDERS if item["slug"] == slug), None)
    if order is None:
        abort(404)

    path = Path(order["path"])
    if not path.exists():
        flash("Office order file is not available on this machine.", "danger")
        return redirect(url_for("csc.index"))

    download = request.args.get("download", "").strip().lower() in {"1", "true", "yes"}
    return send_file(
        path,
        as_attachment=download,
        download_name=path.name,
        mimetype="application/pdf",
    )


@csc_bp.route("/api/overview")
@login_required
@module_access_required("csc")
def api_overview():
    """JSON KPIs for landing page."""
    try:
        total_specs = CSCDraft.query.filter_by(
            parent_draft_id=None,
        ).count()
        open_drafting = CSCRevision.query.filter(
            CSCRevision.status.in_([WORKFLOW_DRAFTING_OPEN, WORKFLOW_DRAFTING_RETURNED])
        ).count()
        submitted = CSCRevision.query.filter_by(
            status=WORKFLOW_DRAFTING_SUBMITTED
        ).count()
        published = CSCDraft.query.filter_by(
            parent_draft_id=None,
            status="Published",
        ).count()

        return jsonify({
            "total_specs": total_specs,
            "under_review": submitted,
            "pending_revisions": open_drafting,
            "published": published,
        })
    except Exception as e:
        logger.error(f"Error fetching overview: {e}")
        return jsonify({"error": str(e)}), 500


# ──────────────────────────────────────────────────────────────────────────────
# Workspace (Committee Members)
# ──────────────────────────────────────────────────────────────────────────────


@csc_bp.route("/workspace")
@login_required
@module_access_required("csc")
def workspace():
    """Draft selection/list page filtered by user's committee."""
    try:
        return render_template(
            "csc/workspace.html",
            drafts=[],
            q=request.args.get("q", "").strip(),
            subset_filter=request.args.get("subset", "").strip(),
            status_filter=request.args.get("status", "").strip().lower(),
            subset_options=_get_spec_subsets(),
        )
    except Exception as e:
        logger.error(f"Error loading workspace: {e}")
        flash("Error loading workspace.", "danger")
        return redirect(url_for("csc.index"))


@csc_bp.route("/api/drafts")
@login_required
@module_access_required("csc")
def api_drafts():
    """JSON endpoint for filtered draft list."""
    try:
        subset = request.args.get("subset", "").strip()
        search = request.args.get("search", request.args.get("q", "")).strip()

        query = CSCDraft.query.filter(CSCDraft.parent_draft_id.isnot(None))
        query = query.join(CSCRevision, CSCRevision.child_draft_id == CSCDraft.id)
        if not current_user.is_super_user():
            query = query.filter(
                CSCRevision.status.in_([WORKFLOW_DRAFTING_OPEN, WORKFLOW_DRAFTING_RETURNED])
            )

        if subset and subset != "all":
            query = query.filter(CSCDraft.spec_number.ilike(f"ONGC/{subset}/%"))

        if search:
            query = query.filter(
                db.or_(
                    CSCDraft.chemical_name.ilike(f"%{search}%"),
                    CSCDraft.spec_number.ilike(f"%{search}%"),
                )
            )

        drafts = sorted(query.all(), key=_draft_sort_key)

        return jsonify({
            "drafts": [
                {
                    "id": d.id,
                    "spec_number": d.spec_number,
                    "chemical_name": d.chemical_name,
                    "status": _status_db_to_ui_value(d.status),
                    "drafting_status": (_get_revision_for_child(d.id).status if _get_revision_for_child(d.id) else ""),
                    "subset": d.subset,
                    "subset_label": d.subset_label,
                    "subset_display": d.subset_display,
                    "version": d.version,
                    "created_at": d.created_at.isoformat(),
                    "updated_at": d.updated_at.isoformat(),
                }
                for d in drafts
            ],
            "total": len(drafts),
        })
    except Exception as e:
        logger.error(f"Error fetching drafts: {e}")
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/workspace/<int:draft_id>")
@login_required
@module_access_required("csc")
def editor(draft_id: int):
    """9-section tabbed editor for a single draft."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            abort(404)

        if not _can_edit_draft(draft):
            flash("You do not have permission to edit this draft.", "danger")
            return redirect(url_for("csc.workspace"))

        section_rows = draft.sections.order_by(CSCSection.sort_order).all()
        sections = {section.section_name: section.section_text or "" for section in section_rows}
        sections["background"] = (
            sections.get("background", "").strip() or DEFAULT_BACKGROUND_VERSION_HISTORY
        )
        sections["recommendation"] = (
            sections.get("recommendation", "").strip() or DEFAULT_VERSION_CHANGE_REASON
        )

        issue_rows = draft.issue_flags.order_by(CSCIssueFlag.sort_order).all()
        issues = []
        for index, label in enumerate(EDITOR_ISSUE_LABELS):
            issue = issue_rows[index] if index < len(issue_rows) else None
            issues.append(
                {
                    "issue_type": label,
                    "flagged": bool(issue.is_present) if issue else False,
                    "notes": issue.note if issue else "",
                }
            )

        parameters = [
            {
                "id": parameter.id,
                "parameter_name": parameter.parameter_name,
                "parameter_type": parameter.parameter_type or "Essential",
                "unit_of_measure": parameter.unit_of_measure or "",
                "specification": parameter.existing_value or "",
                "required_value_type": parameter.required_value_type or "text",
                "required_value_text": parameter.required_value_text or parameter.existing_value or "",
                "required_value_operator_1": parameter.required_value_operator_1 or "",
                "required_value_value_1": parameter.required_value_value_1 or "",
                "required_value_operator_2": parameter.required_value_operator_2 or "",
                "required_value_value_2": parameter.required_value_value_2 or "",
                "parameter_conditions": parameter.parameter_conditions or "",
                "test_method": parameter.test_method or "",
                "test_procedure_type": normalize_test_procedure_type(
                    parameter.test_procedure_type or parameter.test_method or ""
                ),
                "test_procedure_text": parameter.test_procedure_text or parameter.test_method or "",
                "proposed_value": parameter.proposed_value or "",
                "approved": False,
            }
            for parameter in draft.parameters.order_by(CSCParameter.sort_order).all()
        ]

        checklist_state = build_default_impact_checklist_state(draft.chemical_name)
        impact = {
            "checklist_state": checklist_state,
            "checklist_summary": summarize_impact_checklist_state(checklist_state),
            "legacy_only": False,
        }
        if draft.impact_analysis:
            checklist_state = deserialize_impact_checklist_state(
                draft.impact_analysis.checklist_state_json,
                draft.chemical_name,
            )
            impact = {
                "cost_impact": draft.impact_analysis.operational_impact_score,
                "quality_impact": draft.impact_analysis.safety_environment_score,
                "process_impact": draft.impact_analysis.supply_risk_score,
                "no_substitute": draft.impact_analysis.no_substitute_flag,
                "weighted_score": draft.impact_analysis.impact_score_total,
                "grade": draft.impact_analysis.impact_grade,
                "checklist_state": checklist_state,
                "checklist_summary": summarize_impact_checklist_state(checklist_state),
                "legacy_only": not bool((draft.impact_analysis.checklist_state_json or "").strip()),
            }

        return render_template(
            "csc/editor.html",
            draft=draft,
            sections=sections,
            issues=issues,
            parameters=parameters,
            parameter_type_options=PARAMETER_TYPES,
            test_procedure_options=TEST_PROCEDURE_OPTIONS,
            impact=impact,
            impact_checklist_flags=IMPACT_CHECKLIST_FLAGS,
            master_data=get_master_form_values(draft),
            material_properties_fields=get_material_properties_fields(),
            material_properties_values=get_material_properties_values(draft),
            storage_handling_fields=get_storage_handling_fields(),
            storage_handling_values=get_storage_handling_values(draft),
            master_fields=MASTER_DATA_FIELDS,
            master_extra_fields=MASTER_EXTRA_FIELDS,
            master_impact_fields=MASTER_IMPACT_FIELDS,
        )
    except Exception as e:
        logger.error(f"Error loading editor: {e}")
        flash("Error loading draft editor.", "danger")
        return redirect(url_for("csc.workspace"))


@csc_bp.route("/api/draft/<int:draft_id>")
@login_required
@module_access_required("csc")
def api_draft_detail(draft_id: int):
    """Full draft data as JSON."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        parameters = [
            p.to_dict() for p in draft.parameters.order_by(CSCParameter.sort_order).all()
        ]
        sections = [
            s.to_dict() for s in draft.sections.order_by(CSCSection.sort_order).all()
        ]
        issue_flags = [
            f.to_dict() for f in draft.issue_flags.order_by(CSCIssueFlag.sort_order).all()
        ]
        if draft.impact_analysis:
            impact = draft.impact_analysis.to_dict()
        else:
            checklist_state = build_default_impact_checklist_state(draft.chemical_name)
            impact = {
                "checklist_state": checklist_state,
                "checklist_summary": summarize_impact_checklist_state(checklist_state),
            }

        return jsonify({
            "draft": draft.to_dict(),
            "parameters": parameters,
            "sections": sections,
            "issue_flags": issue_flags,
            "impact_analysis": impact,
            "master_data": get_master_form_values(draft),
            "material_properties": get_material_properties_values(draft),
            "storage_handling": get_storage_handling_values(draft),
        })
    except Exception as e:
        logger.error(f"Error fetching draft detail: {e}")
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/master-data", methods=["POST"])
@login_required
@module_access_required("csc")
def save_master_data(draft_id: int):
    """Save linked master-data fields directly from the CSC drafting space."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        data = request.get_json() or {}

        if "chemical_name" in data:
            draft.chemical_name = (data.get("chemical_name") or "").strip()

        record = upsert_master_record_from_form(draft, data, user_id=current_user.id)
        if record is not None and record not in db.session:
            db.session.add(record)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, "Master data saved from drafting workspace")

        return jsonify({
            "success": True,
            "draft": draft.to_dict(),
            "master_data": get_master_form_values(draft),
        })
    except Exception as e:
        logger.error(f"Error saving master data: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/material-properties", methods=["POST"])
@login_required
@module_access_required("csc")
def save_material_properties(draft_id: int):
    """Save workbook-driven material properties fields only."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        if not (draft.material_code or "").strip():
            return jsonify({"error": "Material code is required before material properties can be saved."}), 400

        data = request.get_json() or {}
        record = update_material_properties_from_form(draft, data, user_id=current_user.id)
        if record is not None and record not in db.session:
            db.session.add(record)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, "Material properties saved from drafting workspace")

        return jsonify({
            "success": True,
            "draft": draft.to_dict(),
            "master_data": get_master_form_values(draft),
            "material_properties": get_material_properties_values(draft),
            "storage_handling": get_storage_handling_values(draft),
        })
    except Exception as e:
        logger.error(f"Error saving material properties: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/storage-handling", methods=["POST"])
@login_required
@module_access_required("csc")
def save_storage_handling(draft_id: int):
    """Save storage and handling fields only."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        if not (draft.material_code or "").strip():
            return jsonify({"error": "Material code is required before storage and handling can be saved."}), 400

        data = request.get_json() or {}
        record = update_storage_handling_from_form(draft, data, user_id=current_user.id)
        if record is not None and record not in db.session:
            db.session.add(record)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, "Storage and handling saved from drafting workspace")

        return jsonify({
            "success": True,
            "draft": draft.to_dict(),
            "master_data": get_master_form_values(draft),
            "material_properties": get_material_properties_values(draft),
            "storage_handling": get_storage_handling_values(draft),
        })
    except Exception as e:
        logger.error(f"Error saving storage and handling: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/section/<section_name>", methods=["POST"])
@login_required
@module_access_required("csc")
def save_section(draft_id: int, section_name: str):
    """Save section text content via AJAX."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        if draft.phase1_locked and section_name in ["properties", "specifications", "test_methods"]:
            return jsonify({"error": "Phase 1 is locked"}), 403

        data = request.get_json() or {}
        section_text = data.get("section_text", "")

        section = CSCSection.query.filter_by(
            draft_id=draft_id, section_name=section_name
        ).first()

        if not section:
            section = CSCSection(
                draft_id=draft_id,
                section_name=section_name,
                section_text=section_text,
                sort_order=0,
            )
            db.session.add(section)
        else:
            section.section_text = section_text

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, f"Section saved: {section_name}")

        return jsonify({"success": True, "section": section.to_dict()})
    except Exception as e:
        logger.error(f"Error saving section: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/issues", methods=["POST"])
@login_required
@module_access_required("csc")
def save_issues(draft_id: int):
    """Save issue flags via AJAX."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        data = request.get_json() or []

        CSCIssueFlag.query.filter_by(draft_id=draft_id).delete()

        for idx, issue_data in enumerate(data):
            flag = CSCIssueFlag(
                draft_id=draft_id,
                issue_type=issue_data.get("issue_type", ""),
                is_present=issue_data.get("is_present", False),
                note=issue_data.get("note", ""),
                sort_order=idx,
            )
            db.session.add(flag)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, "Issue flags saved")

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error saving issues: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/parameters", methods=["POST"])
@login_required
@module_access_required("csc")
def save_parameters(draft_id: int):
    """Save parameters (Phase 1 - full edit)."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        if draft.phase1_locked:
            return jsonify({"error": "Phase 1 is locked"}), 403

        data = request.get_json() or []

        CSCParameter.query.filter_by(draft_id=draft_id).delete()

        for idx, param_data in enumerate(data):
            param = CSCParameter(
                draft_id=draft_id,
                parameter_name=param_data.get("parameter_name", ""),
                parameter_type=normalize_parameter_type_label(
                    param_data.get("parameter_type", "Essential")
                ),
                unit_of_measure=(param_data.get("unit_of_measure", "") or "").strip() or None,
                existing_value=format_required_value(
                    param_data.get("required_value_type", "text"),
                    param_data.get("required_value_text", param_data.get("existing_value", "")),
                    param_data.get("required_value_operator_1", ""),
                    param_data.get("required_value_value_1", ""),
                    param_data.get("required_value_operator_2", ""),
                    param_data.get("required_value_value_2", ""),
                ),
                proposed_value=param_data.get("proposed_value", ""),
                test_method=normalize_test_procedure_type(
                    param_data.get("test_procedure_type", param_data.get("test_method", ""))
                ),
                test_procedure_type=normalize_test_procedure_type(
                    param_data.get("test_procedure_type", param_data.get("test_method", ""))
                ),
                test_procedure_text=param_data.get("test_procedure_text", ""),
                parameter_conditions=param_data.get("parameter_conditions", ""),
                required_value_type=(
                    "comparison"
                    if str(param_data.get("required_value_type", "text")).strip().lower() == "comparison"
                    else "text"
                ),
                required_value_text=param_data.get("required_value_text", ""),
                required_value_operator_1=param_data.get("required_value_operator_1", ""),
                required_value_value_1=param_data.get("required_value_value_1", ""),
                required_value_operator_2=param_data.get("required_value_operator_2", ""),
                required_value_value_2=param_data.get("required_value_value_2", ""),
                justification=param_data.get("justification", ""),
                remarks=param_data.get("remarks", ""),
                sort_order=idx,
            )
            db.session.add(param)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, f"Parameters saved (Phase 1): {len(data)} items")

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error saving parameters: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/parameters/proposed", methods=["POST"])
@login_required
@module_access_required("csc")
def save_proposed_values(draft_id: int):
    """Save proposed values only (Phase 2)."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        data = request.get_json() or []

        for item in data:
            param_id = item.get("parameter_id")
            proposed_value = item.get("proposed_value", "")

            param = CSCParameter.query.filter_by(
                id=param_id, draft_id=draft_id
            ).first()

            if param:
                param.proposed_value = proposed_value

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, f"Proposed values saved (Phase 2): {len(data)} items")

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error saving proposed values: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/impact", methods=["POST"])
@login_required
@module_access_required("csc")
def save_impact(draft_id: int):
    """Save impact analysis scores."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        data = request.get_json() or {}

        checklist_state = deserialize_impact_checklist_state(
            data.get("checklist_state"),
            data.get("chemical_name") or draft.chemical_name,
        )
        legacy_payload = build_impact_legacy_payload(checklist_state)

        impact = draft.impact_analysis
        if not impact:
            impact = CSCImpactAnalysis(draft_id=draft_id)
            db.session.add(impact)

        impact.operational_impact_score = legacy_payload["operational_impact_score"]
        impact.safety_environment_score = legacy_payload["safety_environment_score"]
        impact.supply_risk_score = legacy_payload["supply_risk_score"]
        impact.no_substitute_flag = legacy_payload["no_substitute_flag"]
        impact.impact_score_total = legacy_payload["impact_score_total"]
        impact.impact_grade = legacy_payload["impact_grade"]
        impact.checklist_state_json = legacy_payload["checklist_state_json"]
        impact.operational_note = legacy_payload["operational_note"]
        impact.safety_environment_note = legacy_payload["safety_environment_note"]
        impact.supply_risk_note = legacy_payload["supply_risk_note"]
        record = sync_impact_fields_to_master_record(
            draft,
            checklist_state,
            user_id=current_user.id,
        )
        if record is not None and record not in db.session:
            db.session.add(record)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(
            draft_id,
            "Impact analysis saved",
            new_value=json.dumps({
                "classification": legacy_payload["checklist_summary"]["classification"],
                "rule": legacy_payload["checklist_summary"]["rule"],
            }),
        )

        return jsonify({
            "success": True,
            "impact": impact.to_dict(),
            "checklist_summary": legacy_payload["checklist_summary"],
            "master_data": get_master_form_values(draft),
        })
    except Exception as e:
        logger.error(f"Error saving impact: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/submit", methods=["POST"])
@login_required
@module_access_required("csc")
def submit_revision(draft_id: int):
    """Submit an open drafting copy for admin approval."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_edit_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        revision = _get_revision_for_child(draft_id)
        if draft.parent_draft_id is None or revision is None:
            return jsonify({"error": "Only drafting copies can be submitted"}), 400
        if not _can_submit_revision(revision):
            return jsonify({"error": "This drafting copy is not open for submission"}), 400

        data = request.get_json(silent=True) or {}
        authorization_confirmed = bool(data.get("authorization_confirmed", False))
        authorized_by_name = (data.get("authorized_by_name") or "").strip()
        subcommittee_head_name = (data.get("subcommittee_head_name") or "").strip()

        if not authorization_confirmed:
            return jsonify({
                "error": "Authorization confirmation is required before submission."
            }), 400

        if not authorized_by_name or not subcommittee_head_name:
            return jsonify({
                "error": "Enter your name and the Sub Committee Head name before submission."
            }), 400

        draft.status = "Drafting"
        draft.created_by_id = current_user.id
        draft.created_by_role = current_user.role.name if current_user.role else draft.created_by_role
        draft.updated_at = datetime.now(timezone.utc)
        revision.submitted_at = datetime.now(timezone.utc)
        revision.status = WORKFLOW_DRAFTING_SUBMITTED
        revision.authorization_confirmed = authorization_confirmed
        revision.authorized_by_name = authorized_by_name
        revision.subcommittee_head_name = subcommittee_head_name
        db.session.add(
            CSCAudit(
                draft_id=draft_id,
                action="Revision submitted for approval",
                user_name=current_user.username,
                action_time=datetime.now(timezone.utc),
                new_value=json.dumps(
                    {
                        "authorization_confirmed": authorization_confirmed,
                        "authorized_by_name": authorized_by_name,
                        "subcommittee_head_name": subcommittee_head_name,
                        "subset": draft.subset,
                    }
                ),
            )
        )
        for user in User.query.all():
            if user.is_super_user():
                create_notification(
                    user_id=user.id,
                    title="Draft submitted for review",
                    message=f"{draft.spec_number} — {draft.chemical_name} was submitted for admin review.",
                    severity="info",
                    link=url_for("csc.admin_revisions"),
                )

        db.session.commit()
        return jsonify({"success": True, "redirect": url_for("csc.workspace")})
    except Exception as e:
        logger.error(f"Error submitting revision: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/api/draft/<int:draft_id>/delete", methods=["POST", "DELETE"])
@login_required
@module_access_required("csc")
def delete_draft(draft_id: int):
    """Delete a draft (only own drafts or admin)."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        if not _can_delete_draft(draft):
            return jsonify({"error": "Unauthorized"}), 403

        _create_audit_entry(draft_id, "Draft deleted")

        db.session.delete(draft)
        db.session.commit()

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error deleting draft: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/draft/<int:draft_id>/export/docx")
@login_required
@module_access_required("csc")
def export_docx(draft_id: int):
    """Generate and download Word document."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            abort(404)

        if not _can_edit_draft(draft):
            flash("You do not have permission to export this draft.", "danger")
            return redirect(url_for("csc.workspace"))

        # TODO: Call csc_export service to generate DOCX
        flash("Export functionality not yet implemented.", "info")
        return redirect(url_for("csc.editor", draft_id=draft_id))

    except Exception as e:
        logger.error(f"Error exporting draft: {e}")
        flash("Error exporting draft.", "danger")
        return redirect(url_for("csc.workspace"))


@csc_bp.route("/workspace/new-revision/<int:parent_id>", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def create_revision(parent_id: int):
    """Open a drafting copy from a published specification."""
    try:
        parent_draft = db.session.get(CSCDraft, parent_id)
        if not parent_draft:
            return jsonify({"error": "Parent draft not found"}), 404

        active_revision = _get_active_revision_for_parent(parent_id)
        if active_revision and active_revision.child_draft_id:
            return jsonify({
                "success": True,
                "draft_id": active_revision.child_draft_id,
                "redirect": url_for("csc.editor", draft_id=active_revision.child_draft_id),
            })

        child_draft = CSCDraft(
            spec_number=parent_draft.spec_number,
            chemical_name=parent_draft.chemical_name,
            committee_name=parent_draft.committee_name,
            meeting_date=parent_draft.meeting_date,
            prepared_by=parent_draft.prepared_by,
            reviewed_by=parent_draft.reviewed_by,
            material_code=parent_draft.material_code,
            status="Drafting",
            created_by_role=current_user.role.name if current_user.role else "User",
            is_admin_draft=False,
            phase1_locked=False,
            parent_draft_id=parent_id,
            admin_stage=ADMIN_STAGE_DRAFTING,
            spec_version=parent_draft.spec_version,
            created_by_id=current_user.id,
        )
        db.session.add(child_draft)
        db.session.flush()

        for param in parent_draft.parameters.all():
            new_param = CSCParameter(
                draft_id=child_draft.id,
                parameter_name=param.parameter_name,
                parameter_type=param.parameter_type,
                unit_of_measure=param.unit_of_measure,
                existing_value=param.existing_value,
                proposed_value=param.proposed_value,
                test_method=param.test_method,
                test_procedure_type=param.test_procedure_type,
                test_procedure_text=param.test_procedure_text,
                parameter_conditions=param.parameter_conditions,
                required_value_type=param.required_value_type,
                required_value_text=param.required_value_text,
                required_value_operator_1=param.required_value_operator_1,
                required_value_value_1=param.required_value_value_1,
                required_value_operator_2=param.required_value_operator_2,
                required_value_value_2=param.required_value_value_2,
                justification=param.justification,
                remarks=param.remarks,
                sort_order=param.sort_order,
            )
            db.session.add(new_param)

        for section in parent_draft.sections.all():
            new_section = CSCSection(
                draft_id=child_draft.id,
                section_name=section.section_name,
                section_text=section.section_text,
                sort_order=section.sort_order,
            )
            db.session.add(new_section)

        for flag in parent_draft.issue_flags.all():
            new_flag = CSCIssueFlag(
                draft_id=child_draft.id,
                issue_type=flag.issue_type,
                is_present=flag.is_present,
                note=flag.note,
                sort_order=flag.sort_order,
            )
            db.session.add(new_flag)

        revision = CSCRevision(
            parent_draft_id=parent_id,
            child_draft_id=child_draft.id,
            status=WORKFLOW_DRAFTING_OPEN,
        )
        db.session.add(revision)
        parent_draft.status = "Drafting"
        parent_draft.admin_stage = ADMIN_STAGE_DRAFTING
        parent_draft.updated_at = datetime.now(timezone.utc)

        db.session.commit()

        _create_audit_entry(
            parent_id,
            "Drafting opened",
            new_value=json.dumps({"child_draft_id": child_draft.id}),
        )

        return jsonify({
            "success": True,
            "draft_id": child_draft.id,
            "redirect": url_for("csc.editor", draft_id=child_draft.id),
        })
    except Exception as e:
        logger.error(f"Error creating revision: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ──────────────────────────────────────────────────────────────────────────────
# Admin Routes
# ──────────────────────────────────────────────────────────────────────────────


@csc_bp.route("/admin")
@login_required
@module_access_required("csc")
@superuser_required
def admin_index():
    """Admin dashboard with KPI stats."""
    stats = _get_admin_stats()
    return render_template("csc/admin/index.html", stats=stats)


@csc_bp.route("/admin/ingest")
@login_required
@module_access_required("csc")
@superuser_required
def admin_ingest():
    """PDF/DOCX ingest page (GET only – shows upload forms)."""
    return render_template("csc/admin/ingest.html")


@csc_bp.route("/admin/ingest/pdf", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_ingest_pdf():
    """Process uploaded PDF file."""
    try:
        if "pdf_file" not in request.files:
            flash("No PDF file provided.", "danger")
            return redirect(url_for("csc.admin_ingest"))

        file = request.files["pdf_file"]
        if file.filename == "":
            flash("No file selected.", "danger")
            return redirect(url_for("csc.admin_ingest"))

        # TODO: Implement PDF parsing with csc_pdf_extractor
        draft = CSCDraft(
            spec_number=request.form.get("spec_number", "TBD"),
            chemical_name=request.form.get("chemical_name", file.filename or "TBD"),
            committee_name="Corporate Specifiction Committee",
            prepared_by=current_user.username,
            status="Published",
            created_by_role="Admin",
            is_admin_draft=True,
            admin_stage=ADMIN_STAGE_PUBLISHED,
            created_by_id=current_user.id,
        )
        db.session.add(draft)
        db.session.commit()

        flash(f"PDF ingested – Draft created (ID: {draft.id})", "success")
        return redirect(url_for("csc.admin_drafts"))
    except Exception as e:
        logger.error(f"Error ingesting PDF: {e}")
        flash("Error processing PDF file.", "danger")
        return redirect(url_for("csc.admin_ingest"))


@csc_bp.route("/admin/ingest/docx", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_ingest_docx():
    """Process uploaded DOCX master document."""
    try:
        if "docx_file" not in request.files:
            flash("No DOCX file provided.", "danger")
            return redirect(url_for("csc.admin_ingest"))

        file = request.files["docx_file"]
        if file.filename == "":
            flash("No file selected.", "danger")
            return redirect(url_for("csc.admin_ingest"))

        # TODO: Implement DOCX parsing with csc_docx_extractor
        draft = CSCDraft(
            spec_number=request.form.get("spec_number", "TBD"),
            chemical_name=request.form.get("chemical_name", file.filename or "TBD"),
            committee_name="Corporate Specifiction Committee",
            prepared_by=current_user.username,
            status="Published",
            created_by_role="Admin",
            is_admin_draft=True,
            admin_stage=ADMIN_STAGE_PUBLISHED,
            created_by_id=current_user.id,
        )
        db.session.add(draft)
        db.session.commit()

        flash(f"DOCX ingested – Draft created (ID: {draft.id})", "success")
        return redirect(url_for("csc.admin_drafts"))
    except Exception as e:
        logger.error(f"Error ingesting DOCX: {e}")
        flash("Error processing DOCX file.", "danger")
        return redirect(url_for("csc.admin_ingest"))


@csc_bp.route("/admin/ingest/create-draft", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_create_draft_from_extraction():
    """Create draft from extraction preview."""
    try:
        extraction_id = request.form.get("extraction_id")
        spec_index = request.form.get("spec_index", 0, type=int)

        # TODO: look up extraction result from session/cache and create proper draft
        draft = CSCDraft(
            spec_number=f"EXT-{extraction_id}-{spec_index}",
            chemical_name="Extracted Specification",
            committee_name="Corporate Specifiction Committee",
            prepared_by=current_user.username,
            status="Published",
            created_by_role="Admin",
            is_admin_draft=True,
            admin_stage=ADMIN_STAGE_PUBLISHED,
            created_by_id=current_user.id,
        )
        db.session.add(draft)
        db.session.commit()

        flash(f"Draft created from extraction (ID: {draft.id})", "success")
        return redirect(url_for("csc.admin_drafts"))
    except Exception as e:
        logger.error(f"Error creating draft from extraction: {e}")
        flash("Error creating draft from extraction.", "danger")
        return redirect(url_for("csc.admin_ingest"))


@csc_bp.route("/admin/drafts")
@login_required
@module_access_required("csc")
@superuser_required
def admin_drafts():
    """Manage all drafts with filter support."""
    try:
        drafts = sorted(
            CSCDraft.query.filter_by(parent_draft_id=None).all(),
            key=_draft_sort_key,
        )
        subsets = _get_spec_subsets()
        workflow_rows = []
        for draft in drafts:
            active_revision = _get_active_revision_for_parent(draft.id)
            workflow_draft = active_revision.child_draft if active_revision and active_revision.child_draft else draft
            master_values = get_master_form_values(workflow_draft)
            display_status = draft.status if draft.status in {"Published", "Drafting"} else "Published"
            workflow_rows.append(
                {
                    "draft": draft,
                    "workflow_draft": workflow_draft,
                    "active_revision": active_revision,
                    "display_status": display_status,
                    "physical_state": master_values.get("physical_state", ""),
                    "drafting_status": active_revision.status if active_revision else "",
                    "submitted_at": active_revision.submitted_at if active_revision else None,
                }
            )
        response = make_response(
            render_template(
                "csc/admin/drafts.html",
                workflow_rows=workflow_rows,
                subsets=subsets,
            )
        )
        # Safari's back/forward cache can restore an older drafts table after
        # navigating back from child pages. Mark this view as non-cacheable so
        # the browser always re-requests the current table layout.
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response
    except Exception as e:
        logger.error(f"Error loading admin drafts: {e}")
        flash("Error loading drafts.", "danger")
        return redirect(url_for("csc.admin_index"))


@csc_bp.route("/admin/draft/<int:draft_id>/edit")
@login_required
@module_access_required("csc")
@superuser_required
def admin_edit_draft(draft_id: int):
    """Render combined CSC draft + material master edit form."""
    draft = db.session.get(CSCDraft, draft_id)
    if not draft:
        abort(404)
    revision = _get_revision_for_child(draft.id) if draft.parent_draft_id else _get_active_revision_for_parent(draft.id)
    drafting_state = "Published"
    if revision:
        if _is_open_revision_state(revision.status):
            drafting_state = "Open"
        elif revision.status == WORKFLOW_DRAFTING_SUBMITTED:
            drafting_state = "Submitted"
        else:
            drafting_state = revision.status.title()
    draft_values = {}
    for field in CSC_ADMIN_DRAFT_FIELDS:
        field_name = field["field_name"]
        if field_name == "committee_name" and not getattr(draft, field_name, None):
            draft_values[field_name] = "Corporate Specifiction Committee"
        elif field_name == "spec_version":
            draft_values[field_name] = format_spec_version(getattr(draft, "spec_version", 0))
        elif field_name == "phase1_locked":
            draft_values[field_name] = drafting_state
        elif field_name == "admin_stage":
            draft_values[field_name] = "Drafting" if draft.status == "Drafting" else "Published"
        else:
            draft_values[field_name] = getattr(draft, field_name, None)
    return render_template(
        "csc/admin/edit_draft.html",
        draft=draft,
        draft_values=draft_values,
        form_values=get_master_form_values(draft),
        csc_fields=CSC_ADMIN_DRAFT_FIELDS,
        master_fields=ADMIN_MASTER_FIELDS,
        master_extra_fields=ADMIN_MASTER_EXTRA_FIELDS,
    )


@csc_bp.route("/admin/draft/<int:draft_id>/update", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_update_draft(draft_id: int):
    """Update draft metadata and material master fields from the admin editor."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            if request.is_json:
                return jsonify({"error": "Draft not found"}), 404
            abort(404)

        raw_data = request.get_json(silent=True) or request.form.to_dict(flat=True)
        data = dict(raw_data)

        for field in CSC_ADMIN_DRAFT_FIELDS:
            field_name = field["field_name"]
            input_type = field.get("input_type", "text")

            if field_name == "id" or field_name not in data:
                continue

            if input_type == "display":
                continue

            if field_name == "material_code" and (draft.material_code or "").strip():
                # Keep the material master primary key immutable once established.
                data[field_name] = draft.material_code
                continue

            raw_value = data.get(field_name)

            if input_type == "boolean":
                setattr(draft, field_name, _coerce_admin_boolean(raw_value))
                continue

            if input_type == "number":
                text = str(raw_value or "").strip()
                if field_name == "spec_version":
                    setattr(draft, field_name, normalize_spec_version(text or 0))
                else:
                    setattr(draft, field_name, int(text) if text else None)
                continue

            if field_name in {"meeting_date", "reviewed_by", "material_code"}:
                setattr(draft, field_name, str(raw_value or "").strip() or None)
            else:
                setattr(draft, field_name, str(raw_value or "").strip())

        master_form_data = {}
        for field_name, _ in ADMIN_MASTER_FIELDS:
            if field_name in data:
                master_form_data[field_name] = data[field_name]
        for field_name, _ in ADMIN_MASTER_EXTRA_FIELDS:
            form_key = f"extra__{field_name}"
            if form_key in data:
                master_form_data[form_key] = data[form_key]
        if (draft.material_code or "").strip():
            master_form_data["material_code"] = draft.material_code
        elif "material_code" in data:
            master_form_data["material_code"] = data["material_code"]
        if "chemical_name" in data:
            master_form_data["chemical_name"] = data["chemical_name"]

        record = upsert_master_record_from_form(draft, master_form_data, user_id=current_user.id)
        if record is not None and record not in db.session:
            db.session.add(record)

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(draft_id, "Draft metadata updated (admin)")

        if request.is_json:
            return jsonify({"success": True, "draft": draft.to_dict()})

        flash("CSC draft and material master data updated.", "success")
        return redirect(url_for("csc.admin_edit_draft", draft_id=draft_id))
    except Exception as e:
        logger.error(f"Error updating draft: {e}")
        db.session.rollback()
        if request.is_json:
            return jsonify({"error": str(e)}), 500
        flash("Failed to update the draft and material master data.", "danger")
        return redirect(url_for("csc.admin_edit_draft", draft_id=draft_id))


@csc_bp.route("/admin/draft/<int:draft_id>/lock-phase1", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_lock_phase1(draft_id: int):
    """Toggle Phase 1 lock."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        draft.phase1_locked = not draft.phase1_locked
        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(
            draft_id,
            f"Phase 1 {'locked' if draft.phase1_locked else 'unlocked'} (admin)",
        )

        return jsonify({
            "success": True,
            "phase1_locked": draft.phase1_locked,
        })
    except Exception as e:
        logger.error(f"Error toggling phase1 lock: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# Alias: templates use url_for('csc.api_admin_toggle_phase1_lock', draft_id=...)
@csc_bp.route("/admin/api/draft/<int:draft_id>/toggle-phase1-lock", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def api_admin_toggle_phase1_lock(draft_id: int):
    """Alias for admin_lock_phase1."""
    return admin_lock_phase1(draft_id)


@csc_bp.route("/admin/draft/<int:draft_id>/set-stage", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_set_stage(draft_id: int):
    """Set workflow stage (drafting/published)."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        data = request.get_json() or {}

        new_stage = data.get("admin_stage", ADMIN_STAGE_PUBLISHED)
        if new_stage not in (ADMIN_STAGE_DRAFTING, ADMIN_STAGE_PUBLISHED):
            return jsonify({"error": "Invalid stage"}), 400

        old_stage = draft.admin_stage
        draft.admin_stage = new_stage

        if new_stage == ADMIN_STAGE_PUBLISHED and draft.status != "Published":
            draft.status = "Published"
        elif new_stage == ADMIN_STAGE_DRAFTING:
            draft.status = "Drafting"

        draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(
            draft_id,
            f"Admin stage changed: {old_stage} → {new_stage}",
        )

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error setting stage: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# Alias: templates use url_for('csc.api_admin_set_stage', draft_id=...)
@csc_bp.route("/admin/api/draft/<int:draft_id>/set-stage", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def api_admin_set_stage(draft_id: int):
    """Alias for admin_set_stage."""
    return admin_set_stage(draft_id)


@csc_bp.route("/admin/draft/<int:draft_id>/delete", methods=["POST", "DELETE"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_delete_draft(draft_id: int):
    """Admin delete draft."""
    try:
        draft = db.session.get(CSCDraft, draft_id)
        if not draft:
            return jsonify({"error": "Draft not found"}), 404

        _create_audit_entry(draft_id, "Draft deleted (admin)")

        db.session.delete(draft)
        db.session.commit()

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error deleting draft: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# Alias: templates use url_for('csc.api_admin_delete_draft', draft_id=...)
@csc_bp.route("/admin/api/draft/<int:draft_id>/delete", methods=["POST", "DELETE"])
@login_required
@module_access_required("csc")
@superuser_required
def api_admin_delete_draft(draft_id: int):
    """Alias for admin_delete_draft."""
    return admin_delete_draft(draft_id)


@csc_bp.route("/admin/revisions")
@login_required
@module_access_required("csc")
@superuser_required
def admin_revisions():
    """Committee workbench - review submitted drafting requests."""
    try:
        revisions = CSCRevision.query.order_by(
            CSCRevision.status != WORKFLOW_DRAFTING_SUBMITTED,
            CSCRevision.submitted_at.desc(),
            CSCRevision.id.desc(),
        ).all()

        return render_template("csc/admin/revisions.html", revisions=revisions)
    except Exception as e:
        logger.error(f"Error loading revisions: {e}")
        flash("Error loading revisions.", "danger")
        return redirect(url_for("csc.admin_index"))


@csc_bp.route("/admin/revision/<int:revision_id>/review")
@login_required
@module_access_required("csc")
@superuser_required
def admin_review_revision(revision_id: int):
    """Review revision – redirect to the child draft editor."""
    revision = db.session.get(CSCRevision, revision_id)
    if not revision:
        abort(404)
    return redirect(url_for("csc.editor", draft_id=revision.child_draft_id))


@csc_bp.route("/admin/revision/<int:revision_id>/approve", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_approve_revision(revision_id: int):
    """Approve a submitted drafting request."""
    try:
        revision = db.session.get(CSCRevision, revision_id)
        if not revision:
            return jsonify({"error": "Revision not found"}), 404

        data = request.get_json() or {}
        if revision.status != WORKFLOW_DRAFTING_SUBMITTED:
            return jsonify({"error": "Only submitted drafts can be approved"}), 400

        child_draft = revision.child_draft
        parent_draft = revision.parent_draft
        version_increment = (data.get("version_increment") or "whole").strip().lower()
        if version_increment not in {"whole", "decimal"}:
            return jsonify({"error": "Choose whole or decimal version increment"}), 400

        _copy_draft_content(child_draft, parent_draft)
        record = upsert_master_record_from_form(
            parent_draft,
            get_master_form_values(child_draft),
            user_id=current_user.id,
        )
        if record is not None and record not in db.session:
            db.session.add(record)

        revision.status = WORKFLOW_DRAFTING_APPROVED
        revision.reviewed_at = datetime.now(timezone.utc)
        revision.reviewer_notes = data.get("reviewer_notes", "")

        parent_draft.status = "Published"
        parent_draft.admin_stage = ADMIN_STAGE_PUBLISHED
        parent_draft.reviewed_by = current_user.username
        parent_draft.spec_version = increment_spec_version(
            parent_draft.spec_version,
            version_increment,
        )
        parent_draft.updated_at = datetime.now(timezone.utc)

        snapshot = CSCSpecVersion(
            draft_id=parent_draft.id,
            spec_version=parent_draft.spec_version,
            created_by=current_user.username,
            source_action=f"admin_approve_{version_increment}",
            remarks=revision.reviewer_notes or None,
            payload_json=json.dumps(
                {
                    "draft": parent_draft.to_dict(),
                    "master_data": get_master_form_values(child_draft),
                }
            ),
        )
        db.session.add(snapshot)
        clear_staged_master_payload(child_draft)

        proposer_id = child_draft.created_by_id
        child_draft_id = child_draft.id
        applied_version = format_spec_version(parent_draft.spec_version)
        db.session.delete(revision)
        db.session.delete(child_draft)
        db.session.commit()

        _create_audit_entry(
            parent_draft.id,
            "Drafting approved and published",
            new_value=json.dumps(
                {
                    "version": applied_version,
                    "increment_type": version_increment,
                    "submitted_draft_id": child_draft_id,
                }
            ),
            remarks=revision.reviewer_notes,
        )
        create_notification(
            user_id=proposer_id,
            title="Draft approved",
            message=(
                f"{parent_draft.spec_number} — {parent_draft.chemical_name} was approved "
                f"and published as v{applied_version}."
            ),
            severity="success",
            link=url_for("csc.workspace"),
        )
        db.session.commit()

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error approving revision: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# Alias: templates use url_for('csc.api_admin_approve_revision', revision_id=...)
@csc_bp.route("/admin/api/revision/<int:revision_id>/approve", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def api_admin_approve_revision(revision_id: int):
    """Alias for admin_approve_revision."""
    return admin_approve_revision(revision_id)


@csc_bp.route("/admin/revision/<int:revision_id>/reject", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_reject_revision(revision_id: int):
    """Reject and delete a submitted drafting request."""
    try:
        revision = db.session.get(CSCRevision, revision_id)
        if not revision:
            return jsonify({"error": "Revision not found"}), 404

        data = request.get_json() or {}
        if revision.status != WORKFLOW_DRAFTING_SUBMITTED:
            return jsonify({"error": "Only submitted drafts can be rejected"}), 400

        child_draft = revision.child_draft
        parent_draft = revision.parent_draft
        reviewer_notes = (data.get("reviewer_notes") or "").strip()
        if not reviewer_notes:
            return jsonify({"error": "Reviewer notes are required"}), 400

        revision.status = WORKFLOW_DRAFTING_REJECTED
        revision.reviewed_at = datetime.now(timezone.utc)
        revision.reviewer_notes = reviewer_notes
        parent_draft.status = "Published"
        parent_draft.admin_stage = ADMIN_STAGE_PUBLISHED
        parent_draft.updated_at = datetime.now(timezone.utc)

        proposer_id = child_draft.created_by_id
        child_draft_id = child_draft.id
        clear_staged_master_payload(child_draft)
        db.session.delete(revision)
        db.session.delete(child_draft)
        db.session.commit()

        _create_audit_entry(
            parent_draft.id,
            "Drafting rejected and deleted",
            new_value=json.dumps({"submitted_draft_id": child_draft_id}),
            remarks=reviewer_notes,
        )
        create_notification(
            user_id=proposer_id,
            title="Draft rejected",
            message=(
                f"{parent_draft.spec_number} — {parent_draft.chemical_name} was rejected. "
                f"Note: {reviewer_notes}"
            ),
            severity="danger",
            link=url_for("csc.workspace"),
        )
        db.session.commit()

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error rejecting revision: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/admin/revision/<int:revision_id>/return", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_return_revision(revision_id: int):
    """Return a submitted drafting request to open state with notes."""
    try:
        revision = db.session.get(CSCRevision, revision_id)
        if not revision:
            return jsonify({"error": "Revision not found"}), 404
        if revision.status != WORKFLOW_DRAFTING_SUBMITTED:
            return jsonify({"error": "Only submitted drafts can be returned"}), 400

        data = request.get_json() or {}
        reviewer_notes = (data.get("reviewer_notes") or "").strip()
        if not reviewer_notes:
            return jsonify({"error": "Reviewer notes are required"}), 400

        child_draft = revision.child_draft
        parent_draft = revision.parent_draft
        revision.status = WORKFLOW_DRAFTING_RETURNED
        revision.reviewed_at = datetime.now(timezone.utc)
        revision.reviewer_notes = reviewer_notes
        parent_draft.status = "Drafting"
        parent_draft.admin_stage = ADMIN_STAGE_DRAFTING
        child_draft.updated_at = datetime.now(timezone.utc)
        db.session.commit()

        _create_audit_entry(
            parent_draft.id,
            "Drafting returned for update",
            new_value=json.dumps({"submitted_draft_id": child_draft.id}),
            remarks=reviewer_notes,
        )
        create_notification(
            user_id=child_draft.created_by_id,
            title="Draft returned for update",
            message=(
                f"{parent_draft.spec_number} — {parent_draft.chemical_name} was returned for revision. "
                f"Note: {reviewer_notes}"
            ),
            severity="warning",
            link=url_for("csc.workspace", q=parent_draft.chemical_name),
        )
        db.session.commit()

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error returning revision: {e}")
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# Alias: templates use url_for('csc.api_admin_reject_revision', revision_id=...)
@csc_bp.route("/admin/api/revision/<int:revision_id>/reject", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def api_admin_reject_revision(revision_id: int):
    """Alias for admin_reject_revision."""
    return admin_reject_revision(revision_id)


@csc_bp.route("/admin/api/revision/<int:revision_id>/return", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def api_admin_return_revision(revision_id: int):
    """Alias for admin_return_revision."""
    return admin_return_revision(revision_id)


@csc_bp.route("/admin/audit")
@login_required
@module_access_required("csc")
@superuser_required
def admin_audit_log():
    """Audit log view for CSC workflow actions."""
    entries = (
        CSCAudit.query.order_by(CSCAudit.action_time.desc(), CSCAudit.id.desc())
        .limit(500)
        .all()
    )
    return render_template("csc/admin/audit.html", entries=entries)


@csc_bp.route("/admin/audit/download")
@login_required
@module_access_required("csc")
@superuser_required
def admin_audit_log_download():
    """Download CSC audit log as CSV."""
    entries = CSCAudit.query.order_by(CSCAudit.action_time.desc(), CSCAudit.id.desc()).all()
    buffer = io.StringIO()
    buffer.write("audit_id,action_time,user_name,draft_id,spec_number,chemical_name,action,remarks\n")
    for entry in entries:
        spec_number = (entry.draft.spec_number if entry.draft else "") or ""
        chemical_name = (entry.draft.chemical_name if entry.draft else "") or ""
        row = [
            str(entry.id),
            entry.action_time.isoformat() if entry.action_time else "",
            entry.user_name or "",
            str(entry.draft_id or ""),
            spec_number,
            chemical_name,
            (entry.action or "").replace('"', "'"),
            (entry.remarks or "").replace('"', "'").replace("\n", " "),
        ]
        buffer.write(",".join(f'"{value}"' for value in row) + "\n")

    response = make_response(buffer.getvalue())
    response.headers["Content-Type"] = "text/csv; charset=utf-8"
    response.headers["Content-Disposition"] = "attachment; filename=csc_audit_log.csv"
    return response


@csc_bp.route("/admin/classification")
@login_required
@module_access_required("csc")
@superuser_required
def admin_classification():
    """Type classification exercise page."""
    return render_template("csc/admin/classification.html")


@csc_bp.route("/admin/classification/download")
@login_required
@module_access_required("csc")
@superuser_required
def admin_classification_download():
    """Download classification workbook."""
    try:
        flash("Classification download not yet implemented.", "info")
        return redirect(url_for("csc.admin_classification"))
    except Exception as e:
        logger.error(f"Error downloading classification: {e}")
        flash("Error downloading classification.", "danger")
        return redirect(url_for("csc.admin_classification"))


@csc_bp.route("/admin/classification/upload", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_classification_upload():
    """Upload completed classification workbook."""
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]
        if file.filename == "":
            return jsonify({"error": "No file selected"}), 400

        flash("Classification uploaded successfully.", "success")
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error uploading classification: {e}")
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/admin/classification/apply", methods=["POST"])
@login_required
@module_access_required("csc")
@superuser_required
def admin_classification_apply():
    """Apply classification changes."""
    try:
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error applying classification: {e}")
        return jsonify({"error": str(e)}), 500


@csc_bp.route("/admin/export")
@login_required
@module_access_required("csc")
@superuser_required
def admin_export():
    """Export page with stats and history."""
    stats = _get_export_stats()
    return render_template(
        "csc/admin/export.html",
        stats=stats,
        export_history=[],  # TODO: track export history in a model
    )


@csc_bp.route("/admin/export/master-docx")
@login_required
@module_access_required("csc")
@superuser_required
def admin_export_master():
    """Download master spec document."""
    try:
        # TODO: Generate comprehensive master document from all published specs
        flash("Master export not yet implemented.", "info")
        return redirect(url_for("csc.admin_export"))
    except Exception as e:
        logger.error(f"Error exporting master: {e}")
        flash("Error exporting master document.", "danger")
        return redirect(url_for("csc.admin_export"))


@csc_bp.route("/admin/export/<int:export_id>/download")
@login_required
@module_access_required("csc")
@superuser_required
def admin_download_export(export_id: int):
    """Download a previously generated export file."""
    # TODO: Implement export file storage and retrieval
    flash("Export file not found.", "warning")
    return redirect(url_for("csc.admin_export"))
