"""Routes for the NABL AccredKit module."""

from __future__ import annotations

import io

from flask import (
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import current_user, login_required

from app.core.services.nabl import (
    LAB_ROLE_OPTIONS,
    QUESTION_TO_PATH,
    STORAGE_OPTIONS,
    SECTION_TO_QUESTION,
    WIZARD_SECTIONS,
    LAB_DRAFT_STATES,
    DOC_STATE_PUBLISHED,
    build_docx_bytes,
    bulk_push_drafts_to_lab,
    can_administer_nabl,
    compile_application_zip,
    ensure_lab_initialized,
    get_answer_text,
    get_document_list,
    get_document_payload,
    get_lab_summary,
    get_lab_workbench_payload,
    get_nabl_notifications,
    get_source_document_summary,
    get_standard_answer,
    initiate_new_draft,
    list_lab_summaries,
    load_lab_profile,
    load_lab_answers,
    progress_for_answers,
    publish_document,
    resolve_lab_for_user,
    return_document_to_lab,
    save_lab_profile,
    save_wizard_section,
    section_navigation,
    submit_document,
    update_document_draft,
    user_can_view_published_lab_document,
)
from app.core.services.notifications import mark_notification_read
from app.core.utils.decorators import module_access_required
from app.extensions import db
from app.modules.nabl import nabl_bp
from app.core.services.nabl_metadata import DOCUMENT_CATALOG, LABS, WIZARD_SECTION_MAP


def _wants_json() -> bool:
    return request.is_json or request.accept_mimetypes.best == "application/json"


def _current_lab_or_abort(lab_id: str | None = None) -> str:
    requested = lab_id or request.args.get("lab_id")
    resolved_lab, matches = resolve_lab_for_user(current_user, requested)
    if can_administer_nabl(current_user):
        if requested in LABS:
            return requested
        if resolved_lab:
            return resolved_lab
        abort(400)
    if resolved_lab:
        return resolved_lab
    abort(403)


def _module_admin_required() -> None:
    if not can_administer_nabl(current_user):
        abort(403)


def _section_current_value(section: dict, answers: dict, profile: dict):
    section_type = section["type"]
    code = section["code"]
    if section_type == "chemical_table":
        return answers["q1"]["chemicals"]
    if section_type == "lab_profile":
        return {
            "display_name": profile["display_name"],
            "location": profile["location"],
            "parent_organisation": profile["parent_organisation"],
        }
    if section_type == "approval_authority":
        return profile["approved_by"]
    if section_type == "equipment_entry":
        return {
            "entry_method": answers["q2"]["equipment"]["entry_method"],
            "equipment_list": answers["q2"]["equipment"]["equipment_list"],
        }
    if section_type == "records_table":
        return answers["q3"]["records"]
    path = QUESTION_TO_PATH[code]
    return answers[path[0]][path[1]][path[2]] if len(path) == 3 else answers[path[0]][path[1]]


def _wizard_redirect_for_lab(lab_id: str):
    answers = load_lab_answers(lab_id)
    progress = progress_for_answers(answers)
    if progress["completed"] < progress["total"]:
        remaining = [
            section["slug"]
            for section in WIZARD_SECTIONS
            if section["slug"] not in answers["wizard_progress"]["saved_sections"]
        ]
        target = remaining[0] if remaining else WIZARD_SECTIONS[0]["slug"]
        return redirect(url_for("nabl.wizard_section", lab_id=lab_id, section=target))
    return redirect(url_for("nabl.workbench", lab_id=lab_id))


@nabl_bp.route("/")
@login_required
@module_access_required("nabl")
def landing():
    requested_lab = request.args.get("lab_id")
    resolved_lab, matches = resolve_lab_for_user(current_user, requested_lab)
    recommended_href = None
    recommended_label = None
    role_mode = "admin" if can_administer_nabl(current_user) else "lab"
    if can_administer_nabl(current_user):
        recommended_href = (
            url_for("nabl.governance", lab_id=requested_lab)
            if requested_lab in LABS
            else url_for("nabl.governance")
        )
        recommended_label = "Open governance workbench"
    elif resolved_lab:
        answers = load_lab_answers(resolved_lab)
        if answers.get("wizard_complete"):
            recommended_href = url_for("nabl.workbench", lab_id=resolved_lab)
            recommended_label = "Open lab workbench"
        else:
            remaining = [
                section["slug"]
                for section in WIZARD_SECTIONS
                if section["slug"] not in answers["wizard_progress"]["saved_sections"]
            ]
            target = remaining[0] if remaining else WIZARD_SECTIONS[0]["slug"]
            recommended_href = url_for("nabl.wizard_section", lab_id=resolved_lab, section=target)
            recommended_label = "Continue wizard"
    return render_template(
        "nabl/landing.html",
        summaries=list_lab_summaries(),
        lab_matches=matches,
        lab_lookup=LABS,
        resolved_lab=resolved_lab,
        recommended_href=recommended_href,
        recommended_label=recommended_label,
        role_mode=role_mode,
        source_summary=get_source_document_summary(),
    )


@nabl_bp.route("/workbench")
@login_required
@module_access_required("nabl")
def workbench():
    lab_id = _current_lab_or_abort(request.args.get("lab_id"))
    profile, answers, _documents = ensure_lab_initialized(lab_id)
    if can_administer_nabl(current_user) and request.args.get("admin_view") != "1":
        return redirect(url_for("nabl.governance", lab_id=lab_id))
    workbench_payload = get_lab_workbench_payload(lab_id)
    return render_template(
        "nabl/workbench.html",
        lab=profile,
        answers=answers,
        progress=progress_for_answers(answers),
        workbench=workbench_payload,
        wizard_sections=WIZARD_SECTIONS,
        role_options=LAB_ROLE_OPTIONS,
        storage_options=STORAGE_OPTIONS,
    )


@nabl_bp.route("/governance")
@login_required
@module_access_required("nabl")
def governance():
    _module_admin_required()
    selected_lab = request.args.get("lab_id")
    detail = None
    if selected_lab in LABS:
        detail = {
            "summary": get_lab_summary(selected_lab),
            "documents": get_document_list(selected_lab),
        }
    return render_template(
        "nabl/governance.html",
        summaries=list_lab_summaries(),
        selected_lab=selected_lab,
        detail=detail,
    )


@nabl_bp.route("/published/<lab_id>/<doc_id>")
@login_required
@module_access_required("nabl")
def published_document_view(lab_id: str, doc_id: str):
    if not user_can_view_published_lab_document(current_user, lab_id, doc_id):
        abort(403)
    payload = get_document_payload(lab_id, doc_id)
    viewer_lab_id, _matches = resolve_lab_for_user(current_user)
    back_href = url_for("nabl.workbench", lab_id=viewer_lab_id) if viewer_lab_id else url_for("nabl.landing")
    return render_template(
        "nabl/document_review.html",
        lab=payload["lab"],
        doc=payload["doc"],
        meta=payload["meta"],
        document_docx_view=payload["document_docx_view"],
        reference_docx_view=payload["reference_docx_view"],
        is_admin_review=False,
        is_readonly=True,
        is_cross_lab_published=True,
        back_href=back_href,
    )


@nabl_bp.route("/published/<lab_id>/<doc_id>/download")
@login_required
@module_access_required("nabl")
def published_document_download(lab_id: str, doc_id: str):
    if not user_can_view_published_lab_document(current_user, lab_id, doc_id):
        abort(403)
    payload = get_document_payload(lab_id, doc_id)
    latest_version = payload["doc"]["versions"][-1]["version"] if payload["doc"]["versions"] else None
    doc_bytes = build_docx_bytes(lab_id, doc_id, published_version=latest_version)
    return send_file(
        io.BytesIO(doc_bytes),
        as_attachment=True,
        download_name=f"{lab_id}_{doc_id}_published.docx",
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        max_age=0,
    )


@nabl_bp.route("/wizard/<lab_id>")
@login_required
@module_access_required("nabl")
def wizard(lab_id: str):
    _current_lab_or_abort(lab_id)
    return _wizard_redirect_for_lab(lab_id)


@nabl_bp.route("/wizard/<lab_id>/<section>")
@login_required
@module_access_required("nabl")
def wizard_section(lab_id: str, section: str):
    _current_lab_or_abort(lab_id)
    if section not in WIZARD_SECTION_MAP:
        abort(404)
    profile, answers, _documents = ensure_lab_initialized(lab_id)
    nav = section_navigation(section)
    return render_template(
        "nabl/wizard.html",
        lab=profile,
        answers=answers,
        section=WIZARD_SECTION_MAP[section],
        section_code=SECTION_TO_QUESTION[section],
        standard_answer=get_standard_answer(SECTION_TO_QUESTION[section], profile),
        current_value=_section_current_value(WIZARD_SECTION_MAP[section], answers, profile),
        section_nav=nav,
        all_sections=WIZARD_SECTIONS,
        progress=progress_for_answers(answers),
        role_options=LAB_ROLE_OPTIONS,
        storage_options=STORAGE_OPTIONS,
    )


@nabl_bp.route("/wizard/<lab_id>/save", methods=["POST"])
@login_required
@module_access_required("nabl")
def wizard_save(lab_id: str):
    _current_lab_or_abort(lab_id)
    section = request.form.get("section") or (request.json or {}).get("section")
    if section not in WIZARD_SECTION_MAP:
        abort(400)
    payload = request.json if request.is_json else request.form.to_dict(flat=False)
    if not request.is_json:
        payload = {
            key: value[0] if len(value) == 1 else value
            for key, value in payload.items()
        }
    save_wizard_section(lab_id, section, payload, files=request.files)
    if _wants_json():
        return jsonify({"ok": True, "lab_id": lab_id, "section": section})
    nav = section_navigation(section)
    if nav["next"]:
        return redirect(url_for("nabl.wizard_section", lab_id=lab_id, section=nav["next"]))
    return redirect(url_for("nabl.workbench", lab_id=lab_id))


@nabl_bp.route("/api/lab/<lab_id>", methods=["GET", "POST"])
@login_required
@module_access_required("nabl")
def lab_profile_api(lab_id: str):
    _current_lab_or_abort(lab_id)
    if request.method == "GET":
        return jsonify(load_lab_profile(lab_id))
    payload = request.get_json(silent=True) or {}
    return jsonify(save_lab_profile(lab_id, payload))


@nabl_bp.route("/api/documents/<lab_id>")
@login_required
@module_access_required("nabl")
def documents_api(lab_id: str):
    _current_lab_or_abort(lab_id)
    return jsonify(get_document_list(lab_id))


@nabl_bp.route("/api/document/<lab_id>/<doc_id>")
@login_required
@module_access_required("nabl")
def document_api(lab_id: str, doc_id: str):
    _current_lab_or_abort(lab_id)
    if doc_id not in {row["doc_id"] for row in DOCUMENT_CATALOG}:
        abort(404)
    if request.args.get("download") == "1":
        doc_bytes = build_docx_bytes(lab_id, doc_id, published_version=request.args.get("version"))
        return send_file(
            io.BytesIO(doc_bytes),
            as_attachment=True,
            download_name=f"{doc_id}.docx",
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            max_age=0,
        )
    return jsonify(get_document_payload(lab_id, doc_id))


@nabl_bp.route("/document/<lab_id>/<doc_id>")
@login_required
@module_access_required("nabl")
def document_review(lab_id: str, doc_id: str):
    _current_lab_or_abort(lab_id)
    payload = get_document_payload(lab_id, doc_id)
    is_readonly = payload["doc"]["state"] == DOC_STATE_PUBLISHED
    return render_template(
        "nabl/document_review.html",
        lab=payload["lab"],
        doc=payload["doc"],
        meta=payload["meta"],
        document_docx_view=payload["document_docx_view"],
        reference_docx_view=payload["reference_docx_view"],
        is_admin_review=False,
        is_readonly=is_readonly,
        is_cross_lab_published=False,
        back_href=url_for("nabl.workbench", lab_id=lab_id),
    )


@nabl_bp.route("/governance/<lab_id>/<doc_id>")
@login_required
@module_access_required("nabl")
def admin_document_review(lab_id: str, doc_id: str):
    _module_admin_required()
    payload = get_document_payload(lab_id, doc_id)
    return render_template(
        "nabl/document_review.html",
        lab=payload["lab"],
        doc=payload["doc"],
        meta=payload["meta"],
        document_docx_view=payload["document_docx_view"],
        reference_docx_view=payload["reference_docx_view"],
        is_admin_review=True,
        is_readonly=False,
        is_cross_lab_published=False,
        back_href=url_for("nabl.governance", lab_id=lab_id),
    )


@nabl_bp.route("/api/document/<lab_id>/<doc_id>/save", methods=["POST"])
@login_required
@module_access_required("nabl")
def document_save(lab_id: str, doc_id: str):
    _current_lab_or_abort(lab_id)
    payload = request.get_json(silent=True)
    if payload is None:
        section_ids = request.form.getlist("section_id")
        section_bodies = request.form.getlist("section_body")
        payload = {
            "sections": [
                {"section_id": section_id, "body": section_bodies[index] if index < len(section_bodies) else ""}
                for index, section_id in enumerate(section_ids)
            ]
        }
    try:
        update_document_draft(
            lab_id,
            doc_id,
            payload,
            actor=current_user.username,
        )
    except ValueError as exc:
        if _wants_json():
            return jsonify({"ok": False, "error": str(exc)}), 400
        flash(str(exc), "danger")
        return redirect(url_for("nabl.document_review", lab_id=lab_id, doc_id=doc_id))
    if _wants_json():
        return jsonify({"ok": True})
    flash("Document draft saved.", "success")
    return redirect(url_for("nabl.document_review", lab_id=lab_id, doc_id=doc_id))


@nabl_bp.route("/api/document/<lab_id>/<doc_id>/submit", methods=["POST"])
@login_required
@module_access_required("nabl")
def document_submit(lab_id: str, doc_id: str):
    _current_lab_or_abort(lab_id)
    try:
        submit_document(lab_id, doc_id, actor_user=current_user)
    except ValueError as exc:
        if _wants_json():
            return jsonify({"ok": False, "error": str(exc)}), 400
        flash(str(exc), "danger")
        return redirect(url_for("nabl.document_review", lab_id=lab_id, doc_id=doc_id))
    if _wants_json():
        return jsonify({"ok": True})
    flash(f"{doc_id} submitted for admin review.", "success")
    return redirect(url_for("nabl.workbench", lab_id=lab_id))


@nabl_bp.route("/api/document/<lab_id>/<doc_id>/initiate", methods=["POST"])
@login_required
@module_access_required("nabl")
def document_initiate(lab_id: str, doc_id: str):
    _module_admin_required()
    initiate_new_draft(lab_id, doc_id, actor_user=current_user)
    if _wants_json():
        return jsonify({"ok": True})
    flash(f"New draft initiated for {doc_id}.", "success")
    return redirect(url_for("nabl.governance", lab_id=lab_id))


@nabl_bp.route("/api/lab/<lab_id>/push-all-drafts", methods=["POST"])
@login_required
@module_access_required("nabl")
def lab_push_all_drafts(lab_id: str):
    _module_admin_required()
    result = bulk_push_drafts_to_lab(lab_id, actor_user=current_user)
    if _wants_json():
        return jsonify({"ok": True, "count": len(result["pushed_doc_ids"])})
    flash(f"Pushed {len(result['pushed_doc_ids'])} drafts to {LABS[lab_id]['display_name']}.", "success")
    return redirect(url_for("nabl.governance", lab_id=lab_id))


@nabl_bp.route("/api/document/<lab_id>/<doc_id>/return", methods=["POST"])
@login_required
@module_access_required("nabl")
def document_return(lab_id: str, doc_id: str):
    _module_admin_required()
    comments = (request.form.get("comments") or (request.get_json(silent=True) or {}).get("comments") or "").strip()
    if not comments:
        flash("Return comments are required.", "danger")
        return redirect(url_for("nabl.admin_document_review", lab_id=lab_id, doc_id=doc_id))
    section_comments = {}
    if request.form:
        for key, value in request.form.items():
            if key.startswith("comment_") and value.strip():
                section_comments[key.removeprefix("comment_")] = value.strip()
    return_document_to_lab(
        lab_id,
        doc_id,
        actor_user=current_user,
        comments=comments,
        section_comments=section_comments,
    )
    if _wants_json():
        return jsonify({"ok": True})
    flash(f"{doc_id} returned to lab.", "success")
    return redirect(url_for("nabl.governance", lab_id=lab_id))


@nabl_bp.route("/api/document/<lab_id>/<doc_id>/publish", methods=["POST"])
@login_required
@module_access_required("nabl")
def document_publish(lab_id: str, doc_id: str):
    _module_admin_required()
    payload = request.get_json(silent=True) or request.form
    try:
        publish_document(
            lab_id,
            doc_id,
            actor_user=current_user,
            version_mode=payload.get("version_mode", "decimal"),
            version_notes=payload.get("version_notes", ""),
        )
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("nabl.admin_document_review", lab_id=lab_id, doc_id=doc_id))
    if _wants_json():
        return jsonify({"ok": True})
    flash(f"{doc_id} published.", "success")
    return redirect(url_for("nabl.governance", lab_id=lab_id))


@nabl_bp.route("/api/compile/<lab_id>", methods=["POST"])
@login_required
@module_access_required("nabl")
def compile_api(lab_id: str):
    _current_lab_or_abort(lab_id)
    stream, filename = compile_application_zip(lab_id)
    return send_file(
        stream,
        as_attachment=True,
        download_name=filename,
        mimetype="application/zip",
        max_age=0,
    )


@nabl_bp.route("/api/notifications")
@login_required
@module_access_required("nabl")
def notifications_api():
    rows = get_nabl_notifications(current_user.id, limit=50)
    return jsonify(
        [
            {
                "id": row.id,
                "title": row.title,
                "message": row.message,
                "severity": row.severity,
                "is_read": row.is_read,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "link": row.link,
            }
            for row in rows
        ]
    )


@nabl_bp.route("/api/notifications/read", methods=["POST"])
@login_required
@module_access_required("nabl")
def notifications_read_api():
    payload = request.get_json(silent=True) or request.form
    notification_ids = payload.get("notification_ids") or request.form.getlist("notification_ids")
    if isinstance(notification_ids, str):
        notification_ids = [notification_ids]
    for raw_id in notification_ids or []:
        notification = mark_notification_read(raw_id, user_id=current_user.id)
        if notification is not None:
            db.session.commit()
    return jsonify({"ok": True})
