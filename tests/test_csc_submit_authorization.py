from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.modules.csc import routes


class _DummySession:
    def __init__(self) -> None:
        self.added = []
        self.committed = False
        self.rolled_back = False

    def add(self, obj) -> None:
        self.added.append(obj)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_submit_revision_only_requires_authorization_confirmation(monkeypatch) -> None:
    app = Flask(__name__)
    draft = SimpleNamespace(
        id=101,
        parent_draft_id=11,
        status="Drafting",
        created_by_id=None,
        created_by_role=None,
        updated_at=None,
        subset="DFC",
        spec_number="ONGC/DFC/07/2026",
        chemical_name="DEOILER",
    )
    revision = SimpleNamespace(
        submitted_at=None,
        status="open",
        authorization_confirmed=None,
        authorized_by_name="legacy user",
        subcommittee_head_name="legacy head",
    )
    dummy_session = _DummySession()
    current_user = SimpleNamespace(
        id=7,
        username="committee.user",
        role=SimpleNamespace(name="committee_user"),
    )

    monkeypatch.setattr(routes, "current_user", current_user)
    monkeypatch.setattr(routes, "url_for", lambda endpoint: f"/{endpoint}")
    monkeypatch.setattr(routes, "_can_edit_draft", lambda draft_obj: True)
    monkeypatch.setattr(routes, "_json_editor_lock_conflict_response", lambda draft_obj, scope: None)
    monkeypatch.setattr(routes, "_get_revision_for_child", lambda draft_id: revision)
    monkeypatch.setattr(routes, "_can_submit_revision", lambda revision_obj: True)
    monkeypatch.setattr(routes, "_get_revision_committee_slug", lambda revision_obj: "dfc")
    monkeypatch.setattr(routes, "_get_committee_user_ids_for_committee_slug", lambda slug, role: [])
    monkeypatch.setattr(routes, "get_committee_config_payload", lambda: {"CHILD_COMMITTEES": []})
    monkeypatch.setattr(routes, "_notify_users", lambda *args, **kwargs: None)
    monkeypatch.setattr(routes.db, "session", dummy_session)
    monkeypatch.setattr(routes.db.session, "get", lambda model, draft_id: draft)

    submit_revision = routes.submit_revision.__wrapped__.__wrapped__

    with app.test_request_context(
        "/csc/api/draft/101/submit",
        method="POST",
        json={"authorization_confirmed": True},
    ):
        response = submit_revision(101)

    assert response.status_code == 200
    assert revision.authorization_confirmed is True
    assert revision.authorized_by_name is None
    assert revision.subcommittee_head_name is None
    assert draft.created_by_id == current_user.id
    assert draft.created_by_role == current_user.role.name
    assert dummy_session.committed is True


def test_revision_review_context_hides_removed_name_fields(monkeypatch) -> None:
    draft = SimpleNamespace(
        spec_number="ONGC/DFC/07/2026",
        chemical_name="DEOILER",
        subset_display="DFC",
        version="1.0",
        material_code="100000001",
        parent_draft_id=11,
    )
    parent_draft = SimpleNamespace()
    revision = SimpleNamespace(
        child_draft=draft,
        parent_draft=parent_draft,
        submitted_by="committee.user",
        submitted_at=None,
        authorization_confirmed=True,
        status="drafting_submitted",
        committee_head_user=None,
        committee_head_reviewed_at=None,
        module_admin_user=None,
        module_admin_reviewed_at=None,
    )

    monkeypatch.setattr(routes, "get_master_form_values", lambda draft_obj: {"physical_state": "Liquid"})
    monkeypatch.setattr(routes, "get_material_properties_values", lambda draft_obj: {})
    monkeypatch.setattr(routes, "get_storage_handling_values", lambda draft_obj: {})
    monkeypatch.setattr(routes, "_should_blank_legacy_supporting_baseline", lambda draft_obj: False)
    monkeypatch.setattr(routes, "_load_workflow_scope", lambda draft_obj: [])
    monkeypatch.setattr(routes, "_infer_workflow_stream_name", lambda draft_obj: "material")
    monkeypatch.setattr(routes, "_workflow_scope_display_label", lambda draft_obj: "Material Handling")
    monkeypatch.setattr(routes, "_draft_type_label", lambda draft_obj: "Revision")
    monkeypatch.setattr(routes, "_summarize_workflow_scope", lambda scope: [])
    monkeypatch.setattr(routes, "_build_section_review_rows_for_draft", lambda draft_obj, parent_draft=None: [])
    monkeypatch.setattr(routes, "_build_parameter_review_rows_for_draft", lambda draft_obj, parent_draft=None: [])
    monkeypatch.setattr(routes, "_build_impact_review_rows", lambda draft_obj, values: [])
    monkeypatch.setattr(routes, "_build_comparison_value_rows", lambda current, previous=None: current or [])
    monkeypatch.setattr(routes, "_build_labeled_value_rows", lambda fields, values: [])
    monkeypatch.setattr(routes, "_blank_comparison_source_labels", lambda rows, labels: rows)
    monkeypatch.setattr(routes, "get_material_properties_fields", lambda: [])
    monkeypatch.setattr(routes, "get_storage_handling_fields", lambda: [])

    context = routes._build_revision_review_context(revision)
    labels = [row["label"] for row in context["summary_rows"]]

    assert "Authorization Confirmed" in labels
    assert "Authorized By" not in labels
    assert "Subcommittee Head" not in labels
