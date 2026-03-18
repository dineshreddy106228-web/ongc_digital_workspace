# Prompt: Convert CSC Workflow Streamlit App to Flask Module

You are converting a **Streamlit-based CSC (Corporate Specification Committee) Workflow Automation** application into a **Flask module** that integrates into an existing ONGC Digital Workspace Flask application. The Flask app already has working modules (Inventory Intelligence, Office Management, Admin) whose patterns you must replicate exactly.

---

## 1. EXISTING FLASK APPLICATION ARCHITECTURE

### 1.1 Module Registry Pattern

The Flask app uses a central module registry (`app/core/module_registry.py`). The CSC module is already declared:

```python
{
    "key": "csc_workflow",
    "name": "CSC Workflow",
    "permission_code": "csc",
    "blueprint_import": "app.modules.csc:csc_bp",
    "url_prefix": "/csc",
    "endpoint": "csc.index",
    "feature_flag": "ENABLE_CSC",
    "nav_visible": True,
    "dashboard_visible": True,
    "roles_allowed": [ADMIN_ROLE, SUPERUSER_ROLE, USER_ROLE],
    "status": "planned",  # Change to "active"
    "description": "Corporate Specification Committee workflow for oil field chemical spec review",
    "icon": "🔬",
}
```

**Action:** Change `"status": "planned"` → `"status": "active"` and add `ENABLE_CSC=True` to config.

### 1.2 Blueprint Registration Pattern

Follow the existing inventory module pattern:

```
app/modules/csc/
    __init__.py          # Blueprint definition
    routes.py            # All route handlers
app/templates/csc/       # Jinja2 templates
app/models/csc/          # SQLAlchemy models
app/core/services/       # Add csc_*.py service files
```

**`app/modules/csc/__init__.py`:**
```python
from flask import Blueprint

csc_bp = Blueprint(
    "csc",
    __name__,
    template_folder="../templates",
)

from app.modules.csc import routes  # noqa: E402, F401
```

### 1.3 Route Guard Pattern

Every CSC route must use these decorators (matching the inventory module):

```python
from flask_login import login_required
from app.core.utils.decorators import module_access_required, superuser_required

@csc_bp.route("/")
@login_required
@module_access_required("csc")
def index():
    ...

# Admin-only routes additionally use:
@superuser_required
```

### 1.4 Template Inheritance Pattern

All templates extend `base.html` and use the `page_header` macro from `components/ui.html`:

```jinja2
{% extends "base.html" %}
{% from "components/ui.html" import page_header %}

{% block title %}CSC Workflow — {{ app_name }}{% endblock %}

{% block content %}
{% call page_header("Page Title", kicker="CSC Workflow", subtitle="Description") %}
    {# action buttons go here #}
{% endcall %}

<div class="module-content">
    {# page content #}
</div>
{% endblock %}

{% block scripts_extra %}
<script nonce="{{ csp_nonce() }}">
    // page-specific JS
</script>
{% endblock %}
```

### 1.5 UI Component Conventions

The Flask app uses a custom CSS design system (`static/css/style.css`). Key CSS classes:

- **Page layout:** `.module-content`, `.container`
- **Cards:** `.card`, `.card-header`, `.card-body`, `.card-title`, `.card-subtitle`
- **KPI cards:** `.kpi-card`, `.kpi-value`, `.kpi-label`
- **Tables:** `.table`, `.table-hover`, `.table-sm`, sortable headers
- **Buttons:** `.btn`, `.btn-primary`, `.btn-secondary`, `.btn-sm`, `.btn-outline`
- **Forms:** `.form-group`, `.form-label`, `.form-control`, `.form-select`
- **Status badges:** `.status-pill`, `.status-pill-success`, `.status-pill-warning`, `.status-pill-danger`, `.status-pill-neutral`
- **Tabs:** Use `<nav>` with `.nav-tabs` or a sub-nav bar
- **Flash messages:** Handled by base.html via `flash()` from Flask
- **Modals:** Custom `.modal-overlay`, `.modal-dialog` pattern
- **Icons:** Bootstrap Icons (`bi bi-*`)
- **Fonts:** DM Sans (body), JetBrains Mono (code/data)
- **Theme:** Light/dark mode support via `[data-theme="dark"]` CSS selectors
- **Sub-navigation within module:** Horizontal pill links for sub-pages (see inventory dashboard pattern)

### 1.6 Database Pattern

- **ORM:** SQLAlchemy via Flask-SQLAlchemy (`from app.extensions import db`)
- **Backend:** MySQL (PyMySQL driver)
- **Migrations:** Flask-Migrate (Alembic)
- **Naming:** Models in `app/models/csc/` directory
- **Audit:** Activity logging via `app/core/utils/activity.py` and `app/core/utils/audit.py`

---

## 2. STREAMLIT APP: COMPLETE FUNCTIONAL SPECIFICATION

The Streamlit app (`CSC Workflow Automation/`) implements a multi-stage corporate specification review workflow. Below is the **exact** functionality to replicate.

### 2.1 Data Model (Currently SQLite — Migrate to SQLAlchemy/MySQL)

#### Table: `csc_drafts`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | Draft ID |
| spec_number | TEXT NOT NULL | e.g. "ONGC/DFC/01/2026" |
| chemical_name | TEXT NOT NULL | Chemical name |
| test_procedure | TEXT | Test procedure reference |
| material_code | TEXT | SAP material code |
| committee | TEXT | Committee name |
| created_by | TEXT NOT NULL | Username who created |
| created_at | TEXT NOT NULL | ISO timestamp |
| updated_at | TEXT | ISO timestamp |
| status | TEXT DEFAULT 'draft' | One of: draft, structuring, reviewing, published |
| spec_subset | TEXT | One of: DFC, CCA, WCF, WS, PC, WIC, WM, UTL, LPG, API |
| version | TEXT DEFAULT '1.0' | Spec version |
| phase1_locked | INTEGER DEFAULT 0 | Whether Phase 1 editing is locked |
| spec_type | TEXT | Spec type classification |
| test_procedure_type | TEXT | Test procedure classification |
| any_more_changes | TEXT | Flag for further changes needed |

#### Table: `csc_parameters`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | Parameter ID |
| draft_id | INTEGER FK→csc_drafts.id | Parent draft |
| parameter_id | TEXT | Parameter identifier |
| parameter_name | TEXT NOT NULL | Parameter display name |
| unit_condition | TEXT | Unit or condition text |
| existing_value | TEXT | Current specification value |
| proposed_value | TEXT | Committee's proposed value |
| parameter_type | TEXT | "Essential" or "Desirable" |
| sort_order | INTEGER DEFAULT 0 | Display order |
| group_header | TEXT | Section group (e.g. "Borate Sensitivity Test") |

#### Table: `csc_sections`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | Section ID |
| draft_id | INTEGER FK→csc_drafts.id | Parent draft |
| section_name | TEXT NOT NULL | One of the 9 section names |
| content | TEXT | Section text content |
| updated_at | TEXT | ISO timestamp |

**Section names (in order):**
1. Background
2. Existing Specification Summary
3. Issues Observed
4. Parameter Review
5. Proposed Changes Summary
6. Justification
7. Impact Analysis
8. Committee Recommendation
9. Preview & Export

#### Table: `csc_issue_flags`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | |
| draft_id | INTEGER FK→csc_drafts.id | |
| issue_type | TEXT NOT NULL | "Operational", "Quality", "Supply Chain", or "Testing & Lab" |
| flagged | INTEGER DEFAULT 0 | 0/1 boolean |
| notes | TEXT | Explanatory notes for the flag |

#### Table: `csc_impact_analysis`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | |
| draft_id | INTEGER FK→csc_drafts.id UNIQUE | One per draft |
| operational_score | REAL | 1-5 scale |
| safety_env_score | REAL | 1-5 scale |
| supply_chain_score | REAL | 1-5 scale |
| no_substitute_flag | INTEGER DEFAULT 0 | Override to HIGH |
| weighted_score | REAL | Computed: (Op×0.5)+(Safety×0.3)+(Supply×0.2) |
| impact_grade | TEXT | "LOW" (≤2.3), "MODERATE" (2.3-3.5), "HIGH" (>3.5) |
| updated_at | TEXT | |

#### Table: `csc_audit`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | |
| draft_id | INTEGER | Related draft |
| user | TEXT NOT NULL | Who performed action |
| action | TEXT NOT NULL | Action description |
| detail | TEXT | Additional detail |
| timestamp | TEXT NOT NULL | ISO timestamp |

#### Table: `csc_revisions`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK AUTOINCREMENT | |
| parent_draft_id | INTEGER FK→csc_drafts.id | Published parent |
| child_draft_id | INTEGER FK→csc_drafts.id UNIQUE | Revision draft |
| submitted_at | TEXT | |
| reviewed_at | TEXT | |
| status | TEXT DEFAULT 'pending' | pending, approved, rejected |
| reviewer_notes | TEXT | |

### 2.2 Roles and Access Control

**Two roles in the Streamlit app:**
1. **Admin** — Full access: ingest specs, manage drafts, publish, approve revisions, export
2. **User (Committee Member)** — Select published specs, propose revisions through 9-section workflow

**Flask mapping:**
- `ADMIN_ROLE` / `SUPERUSER_ROLE` → Admin capabilities
- `USER_ROLE` → Committee member capabilities
- Use `@superuser_required` for admin-only routes
- Use `@module_access_required("csc")` for all routes

**Committee Users (4 committees):**
- DFC/CCA Committee
- WS Committee
- PC/WIC Committee
- WM/UTL/LPG Committee

Each user sees only specs matching their committee's subsets.

### 2.3 Spec Subsets and Ordering

```python
SPEC_SUBSETS = ["DFC", "CCA", "WCF", "WS", "PC", "WIC", "WM", "UTL", "LPG", "API"]
SUBSET_LABELS = {
    "DFC": "Drilling Fluid Chemicals",
    "CCA": "Cement & Cementing Additives",
    "WCF": "Well Completion Fluids",
    "WS": "Well Stimulation Chemicals",
    "PC": "Production Chemicals",
    "WIC": "Water Injection Chemicals",
    "WM": "Workover & Maintenance",
    "UTL": "Utility Chemicals",
    "LPG": "LPG Treatment Chemicals",
    "API": "API Grade Chemicals",
}
```

### 2.4 Page-by-Page UI Specification

---

#### PAGE 1: CSC Landing Page (`GET /csc/`)

**Equivalent to:** Inventory landing page pattern

**Layout:**
- Page header: "CSC Workflow" with kicker "Corporate Specifications"
- **KPI Cards Row (4 cards):**
  - Total Specs (count of published drafts)
  - Under Review (count of drafts in reviewing status)
  - Pending Revisions (count of revision drafts with status=pending)
  - Published (count of published drafts)
- **Module Cards Grid (matching inventory landing pattern):**
  - "Spec Drafting Workspace" — Link to committee workspace (for Users)
  - "Admin Dashboard" — Link to admin panel (Admin/Superuser only)
  - "Type Classification" — Link to classification exercise (Admin only)
  - "Master Export" — Link to export all specs (Admin only)
- Load KPIs via API: `GET /csc/api/overview`

---

#### PAGE 2: Committee Workspace — Draft Selection (`GET /csc/workspace`)

**This replaces the Streamlit sidebar + main panel draft loading.**

**Layout:**
- Page header: "Spec Drafting Workspace" with subtitle showing current user's committee
- **Filter bar:**
  - Dropdown: Spec Subset filter (DFC, CCA, etc.) — filtered to user's committee subsets
  - Dropdown: Status filter (All, Draft, Published)
  - Search input: Filter by spec number or chemical name
- **Specs Table:**
  - Columns: Spec Number, Chemical Name, Subset, Version, Status (badge), Last Updated, Actions
  - Status badges: `status-pill-success` (published), `status-pill-warning` (reviewing), `status-pill-neutral` (draft)
  - Action buttons: "Open Workspace" (links to section editor)
- **"New Revision" button** (for published specs): Creates a revision draft linked to the parent

**API:** `GET /csc/api/drafts` — Returns filtered draft list as JSON

---

#### PAGE 3: Section Editor — 9-Section Workflow (`GET /csc/workspace/<draft_id>`)

**This is the core of the app. In Streamlit it was a single page with tab-like section navigation. In Flask, implement as a single page with JavaScript tab switching (no page reload per section).**

**Layout:**
- Page header: Spec number + chemical name, with status badge
- **Sub-navigation tabs** (horizontal pill bar, like inventory dashboard sub-nav):
  1. Background
  2. Existing Spec Summary
  3. Issues Observed
  4. Parameter Review
  5. Proposed Changes
  6. Justification
  7. Impact Analysis
  8. Committee Recommendation
  9. Preview & Export
- **Content area:** Changes based on active tab (JavaScript show/hide)
- **Auto-save via AJAX** for each section

##### Section 1: Background
- Single `<textarea>` for background narrative
- Guidance text shown above: "Provide a brief background of the chemical and its usage in ONGC operations."
- Save button: `POST /csc/api/draft/<draft_id>/section/background`

##### Section 2: Existing Specification Summary
- Single `<textarea>` for summarizing current spec state
- Guidance: "Summarize the existing specification parameters and any notable aspects."
- Save: `POST /csc/api/draft/<draft_id>/section/existing_specification_summary`

##### Section 3: Issues Observed
- **4 issue flag rows** (Operational, Quality, Supply Chain, Testing & Lab):
  - Each row: Issue type label | Radio buttons (Yes/No) | Notes textarea
  - Visual indicator: green checkmark or red flag based on selection
- Save: `POST /csc/api/draft/<draft_id>/issues`

##### Section 4: Parameter Review (TWO PHASES)

**Phase 1: Verify & Correct** (shown when `phase1_locked == 0`)
- Editable HTML table of all parameters:
  - Columns: S.No | Parameter Name | Unit/Condition | Existing Value | Type (Essential/Desirable) | Group
  - All cells are editable (inline edit or input fields)
  - Row reordering: Move Up / Move Down buttons
  - Add Row / Delete Row buttons
- "Lock Phase 1" button (Admin only): Sets `phase1_locked = 1`, freezes this view
- Save: `POST /csc/api/draft/<draft_id>/parameters`

**Phase 2: Propose Changes** (shown when `phase1_locked == 1`)
- Table with parameter identity columns READ-ONLY + an editable "Proposed Value" column:
  - Columns: S.No | Parameter Name | Existing Value | Proposed Value (editable) | Type
  - Changed rows highlighted (compare `existing_value` vs `proposed_value`)
  - Change detection: identity match on (parameter_name, sort_order)
- Save: `POST /csc/api/draft/<draft_id>/parameters/proposed`

##### Section 5: Proposed Changes Summary
- **Nature of Changes checkboxes:**
  - [ ] New Parameter Added
  - [ ] Existing Parameter Modified
  - [ ] Parameter Deleted
  - [ ] Tolerance/Range Tightened
  - [ ] Tolerance/Range Relaxed
  - [ ] Test Method Changed
  - [ ] Classification Changed (Essential ↔ Desirable)
- **Summary textarea** for narrative description
- Auto-populated from parameter diff when possible
- Save: `POST /csc/api/draft/<draft_id>/section/proposed_changes_summary`

##### Section 6: Justification
- Single `<textarea>` for technical justification
- Guidance: "Provide technical and operational justification for the proposed changes."
- Save: `POST /csc/api/draft/<draft_id>/section/justification`

##### Section 7: Impact Analysis
- **Scoring form with 3 sliders/dropdowns (1-5 scale):**
  - Operational Impact (weight: 0.5)
  - Safety & Environmental Impact (weight: 0.3)
  - Supply Chain Impact (weight: 0.2)
- **No Substitute checkbox** — if checked, overrides grade to HIGH
- **Computed display (read-only):**
  - Weighted Score: `(Op×0.5) + (Safety×0.3) + (Supply×0.2)`
  - Impact Grade: LOW (≤2.3), MODERATE (2.3-3.5), HIGH (>3.5)
  - Grade badge with color coding: green/yellow/red
- JavaScript: Recalculate score in real-time as sliders change
- Save: `POST /csc/api/draft/<draft_id>/impact`

##### Section 8: Committee Recommendation
- **Recommendation textarea** — the committee's formal recommendation
- **Additional Remarks textarea** — supplementary notes
- Save: `POST /csc/api/draft/<draft_id>/section/committee_recommendation`

##### Section 9: Preview & Export
- **Read-only preview** of the complete draft:
  - Spec metadata header (number, chemical, version, status)
  - All sections rendered
  - Parameter table with change highlights
  - Impact analysis summary
  - Audit trail table
- **Action buttons:**
  - "Export as Word Document" → `GET /csc/draft/<draft_id>/export/docx` (downloads .docx)
  - "Submit Revision for Approval" (if revision draft) → `POST /csc/api/draft/<draft_id>/submit`
  - "Delete Draft" (with confirmation modal) → `POST /csc/api/draft/<draft_id>/delete`

---

#### PAGE 4: Admin Dashboard (`GET /csc/admin`)

**Superuser only. Replaces `csc_admin_pages.py`.**

**Sub-pages (implement as tabs or separate routes):**

##### 4a: PDF Ingest (`GET /csc/admin/ingest`)
- File upload area (drag-and-drop, matching inventory upload pattern)
- Accepts .pdf files of ONGC spec sheets
- On upload: Extract metadata + parameters using `csc_pdf_extractor.py` logic
- Preview extracted data in a table
- "Create Draft" or "Add to Existing Draft" buttons
- Save: `POST /csc/admin/ingest`

##### 4b: Manage All Drafts (`GET /csc/admin/drafts`)
- Filterable table of ALL drafts across all committees
- Filters: Status dropdown, Subset dropdown, Search
- Columns: ID, Spec Number, Chemical Name, Subset, Status (editable dropdown), Version, Phase 1 Locked, Created By, Actions
- Inline actions per draft:
  - Edit metadata (modal or inline)
  - Change status: Structuring → Reviewing → Published
  - Lock/Unlock Phase 1
  - Set version number
  - Delete (with confirmation)
- Bulk actions: Multi-select + status change

##### 4c: Committee Workbench (`GET /csc/admin/revisions`)
- Table of all revision drafts (from `csc_revisions` table)
- Columns: Revision ID, Parent Spec, Chemical Name, Submitted By, Submitted At, Status, Actions
- Actions per revision:
  - "Review" → Opens revision detail
  - "Approve" → `POST /csc/admin/revision/<id>/approve`
  - "Reject" → `POST /csc/admin/revision/<id>/reject` (with notes modal)
- Audit trail expandable per revision

##### 4d: Type Classification (`GET /csc/admin/classification`)
- "Generate Classification Workbook" button → Downloads Excel with one sheet per published spec
- Each sheet has columns: Parameter Name, Current Type, New Type (dropdown: Essential/Desirable), Any More Changes, Test Procedure Type
- "Upload Completed Workbook" → `POST /csc/admin/classification/upload`
- Parse uploaded Excel, show preview of changes
- "Apply Classifications" → Updates parameters with version bump

##### 4e: Master Export (`GET /csc/admin/export`)
- "Generate Master Spec Document" button
- Option: Include/exclude draft notes
- Downloads comprehensive .docx with all published specifications
- Route: `GET /csc/admin/export/master-docx`

---

## 3. API ENDPOINTS SUMMARY

### Public (Module Access Required)
| Method | Route | Description |
|--------|-------|-------------|
| GET | `/csc/` | Landing page |
| GET | `/csc/api/overview` | KPI stats JSON |
| GET | `/csc/workspace` | Draft selection page |
| GET | `/csc/api/drafts` | Filtered drafts JSON |
| GET | `/csc/workspace/<draft_id>` | Section editor page |
| GET | `/csc/api/draft/<draft_id>` | Full draft data JSON |
| POST | `/csc/api/draft/<draft_id>/section/<section_name>` | Save section text |
| POST | `/csc/api/draft/<draft_id>/issues` | Save issue flags |
| POST | `/csc/api/draft/<draft_id>/parameters` | Save parameters (Phase 1) |
| POST | `/csc/api/draft/<draft_id>/parameters/proposed` | Save proposed values (Phase 2) |
| POST | `/csc/api/draft/<draft_id>/impact` | Save impact analysis |
| POST | `/csc/api/draft/<draft_id>/submit` | Submit revision for approval |
| POST | `/csc/api/draft/<draft_id>/delete` | Delete draft |
| GET | `/csc/draft/<draft_id>/export/docx` | Download Word document |
| POST | `/csc/workspace/new-revision/<parent_id>` | Create revision draft |

### Admin Only (Superuser Required)
| Method | Route | Description |
|--------|-------|-------------|
| GET | `/csc/admin` | Admin dashboard |
| GET/POST | `/csc/admin/ingest` | PDF ingest |
| GET | `/csc/admin/drafts` | Manage all drafts |
| POST | `/csc/admin/draft/<draft_id>/update` | Update draft metadata/status |
| POST | `/csc/admin/draft/<draft_id>/lock-phase1` | Toggle Phase 1 lock |
| GET | `/csc/admin/revisions` | Committee workbench |
| POST | `/csc/admin/revision/<id>/approve` | Approve revision |
| POST | `/csc/admin/revision/<id>/reject` | Reject revision |
| GET | `/csc/admin/classification` | Type classification page |
| GET | `/csc/admin/classification/download` | Download classification workbook |
| POST | `/csc/admin/classification/upload` | Upload completed classifications |
| POST | `/csc/admin/classification/apply` | Apply classifications |
| GET | `/csc/admin/export` | Export page |
| GET | `/csc/admin/export/master-docx` | Download master spec document |

---

## 4. SERVICE LAYER FILES TO CREATE

### `app/core/services/csc_repository.py`
Port all database operations from the Streamlit `csc_repository.py` (3011 lines) to SQLAlchemy ORM:
- Draft CRUD (create, read, update, delete)
- Parameter CRUD with sort ordering
- Section text persistence
- Issue flag management
- Impact analysis scoring with weighted formula
- Audit trail logging
- Revision workflow (create, submit, approve, reject, publish)
- Type classification batch operations
- Health check query

### `app/core/services/csc_pdf_extractor.py`
Port the PDF extraction engine (461 lines):
- Uses PyMuPDF (fitz) for parsing
- Column-aware parameter extraction using x-coordinates
- Metadata extraction: chemical name, spec number, test procedure, material code
- Section group header detection
- Return structured data (not Streamlit-specific)

### `app/core/services/csc_docx_extractor.py`
Port the DOCX extraction engine (630 lines):
- Uses python-docx
- 5 parsing formats (A through E) for different spec layouts
- Extract all specs from corporate specifications document

### `app/core/services/csc_export.py`
Port the Word document export engine (1368 lines):
- Part A: ONGC Corporate Specification Sheet with logo, borders, header table, parameter table
- Part B: CSC Draft Note with sections and audit trail
- Master spec document generation
- Uses python-docx for output

---

## 5. SQLAlchemy MODELS TO CREATE

Create in `app/models/csc/`:

### `app/models/csc/__init__.py`
```python
from app.models.csc.draft import CSCDraft
from app.models.csc.parameter import CSCParameter
from app.models.csc.section import CSCSection
from app.models.csc.issue_flag import CSCIssueFlag
from app.models.csc.impact_analysis import CSCImpactAnalysis
from app.models.csc.audit import CSCAudit
from app.models.csc.revision import CSCRevision
```

### Key Model Relationships:
- `CSCDraft` has many `CSCParameter` (cascade delete)
- `CSCDraft` has many `CSCSection` (cascade delete)
- `CSCDraft` has many `CSCIssueFlag` (cascade delete)
- `CSCDraft` has one `CSCImpactAnalysis` (cascade delete)
- `CSCDraft` has many `CSCAudit`
- `CSCRevision` links `parent_draft_id` → `CSCDraft` and `child_draft_id` → `CSCDraft`
- `CSCDraft.created_by` should FK to `User.id` (the Flask app's user model)

### Migration Notes:
- The Streamlit app stored `created_by` as a TEXT username. In Flask, use `created_by_id` as INTEGER FK to `users.id`.
- Convert all TEXT timestamps to proper `DateTime` columns with `server_default=func.now()`.
- Add `updated_by_id` FK to relevant tables for audit consistency.
- Add `db.Index` on frequently queried columns: `spec_number`, `status`, `spec_subset`, `created_by_id`.

---

## 6. TEMPLATE FILE LIST

Create these Jinja2 templates in `app/templates/csc/`:

| Template | Purpose |
|----------|---------|
| `landing.html` | Module landing with KPIs and navigation cards |
| `workspace.html` | Draft selection / list page |
| `editor.html` | 9-section tabbed editor (main workflow page) |
| `_section_background.html` | Partial: Background textarea |
| `_section_existing_spec.html` | Partial: Existing spec summary textarea |
| `_section_issues.html` | Partial: Issue flags with radio buttons |
| `_section_parameters.html` | Partial: Phase 1/Phase 2 parameter tables |
| `_section_proposed_changes.html` | Partial: Checkboxes + summary |
| `_section_justification.html` | Partial: Justification textarea |
| `_section_impact.html` | Partial: Impact scoring form |
| `_section_recommendation.html` | Partial: Recommendation textareas |
| `_section_preview.html` | Partial: Read-only preview + export |
| `admin/index.html` | Admin dashboard with sub-navigation |
| `admin/ingest.html` | PDF upload and extraction preview |
| `admin/drafts.html` | Manage all drafts table |
| `admin/revisions.html` | Committee workbench |
| `admin/classification.html` | Type classification exercise |
| `admin/export.html` | Master export page |

---

## 7. CRITICAL CONVERSION RULES

1. **No Streamlit imports anywhere.** Remove all `import streamlit as st`, `st.session_state`, `st.data_editor`, `st.columns`, `st.radio`, etc.

2. **State management:** Replace `st.session_state` with:
   - Flask `session` for user preferences (active tab, filters)
   - Database queries for data state
   - JavaScript `localStorage` for client-side UI state (active section tab)
   - AJAX calls for dynamic updates

3. **Data editor → HTML tables:** Replace `st.data_editor` with:
   - HTML `<table>` with `<input>` elements for editable cells
   - JavaScript for add/delete/reorder row operations
   - AJAX POST to save changes

4. **Streamlit columns → CSS Grid/Flexbox:** Replace `st.columns([3, 1])` with CSS `.grid` or `.flex` layouts.

5. **Streamlit radio → HTML radio buttons / select:** Replace `st.radio` with `<input type="radio">` groups.

6. **Streamlit file_uploader → HTML file input:** Use the drag-and-drop pattern from inventory upload template.

7. **Streamlit toast/success/error → Flask flash():** Replace `st.toast`, `st.success`, `st.error` with `flash("message", "category")`.

8. **Streamlit rerun → AJAX response:** Replace `st.rerun()` with:
   - AJAX success callback that updates DOM
   - Or `redirect(url_for(...))` for full-page reloads

9. **Streamlit caching → Flask-Caching:** Replace `@st.cache_resource` with `@cache.cached()` or `@cache.memoize()`.

10. **Streamlit sidebar → Sub-navigation:** The sidebar selection of committees/drafts becomes the workspace page with filters.

11. **Parameter Review data_editor with dynamic columns** → Build as a `<table>` with:
    - `<input type="text">` for editable cells
    - `<select>` for Type dropdown (Essential/Desirable)
    - JavaScript handlers for Move Up/Down, Add Row, Delete Row
    - Hidden `<input>` fields for sort_order tracking

12. **Impact Analysis sliders** → Use HTML `<input type="range" min="1" max="5" step="1">` with JavaScript real-time score calculation display.

13. **CSRF Protection:** All POST forms must include `{{ csrf_token() }}` hidden field. AJAX requests must include the CSRF token header.

14. **CSP Nonce:** All inline `<script>` tags must include `nonce="{{ csp_nonce() }}"`.

15. **Word Export:** Use the existing `csc_export.py` logic server-side. Route returns `send_file()` with the generated .docx bytes.

---

## 8. FILES TO PORT (SOURCE → TARGET)

| Streamlit Source | Flask Target | Notes |
|-----------------|--------------|-------|
| `csc_app.py` (1405 lines) | `app/modules/csc/routes.py` | Rewrite as Flask routes |
| `csc_pages.py` (1581 lines) | `app/templates/csc/*.html` + JS | Convert to Jinja2 templates |
| `csc_admin_pages.py` (2625 lines) | `app/templates/csc/admin/*.html` + routes | Admin routes + templates |
| `csc_repository.py` (3011 lines) | `app/core/services/csc_repository.py` + models | Port to SQLAlchemy ORM |
| `csc_utils.py` (266 lines) | `app/core/services/csc_utils.py` | Constants and helpers |
| `csc_export.py` (1368 lines) | `app/core/services/csc_export.py` | Keep python-docx logic |
| `csc_pdf_extractor.py` (461 lines) | `app/core/services/csc_pdf_extractor.py` | Keep PyMuPDF logic |
| `csc_docx_extractor.py` (630 lines) | `app/core/services/csc_docx_extractor.py` | Keep python-docx logic |
| `occ_utils.py` (287 lines) | Already exists in Flask core utils | Reuse existing Flask auth/sanitization |
| `bulk_ingest_docx.py` (246 lines) | `app/cli/csc.py` | Flask CLI command |

---

## 9. JAVASCRIPT REQUIREMENTS

The section editor page (`editor.html`) requires significant client-side JavaScript:

1. **Tab switching** — Show/hide section panels without page reload
2. **Auto-save** — Debounced AJAX POST on textarea blur or after 3s of inactivity
3. **Parameter table editing** — Inline editable cells with add/delete/reorder
4. **Impact score calculator** — Real-time weighted score display
5. **Change detection** — Highlight parameter rows where proposed ≠ existing
6. **Confirmation modals** — For delete, submit, and status changes
7. **File upload** — Drag-and-drop with preview (for PDF ingest)
8. **Flash message dismissal** — Already handled by base.html

**All JavaScript must be vanilla JS** (no jQuery, no React). This matches the existing Flask app's approach. Use `fetch()` for AJAX calls with proper CSRF token handling.

---

## 10. DEPENDENCIES TO ADD

Add to `requirements.txt`:
```
PyMuPDF>=1.23.0    # PDF extraction (fitz)
python-docx>=0.8.11 # Word document creation and parsing
openpyxl>=3.1.0     # Excel workbook for type classification
```

The Flask app already has: Flask, Flask-SQLAlchemy, Flask-Login, Flask-WTF, Flask-Migrate, Flask-Caching, PyMySQL.

---

## 11. IMPLEMENTATION ORDER

1. **Models** — Create SQLAlchemy models + migration
2. **Service layer** — Port repository, extractors, export engine
3. **Routes** — Create blueprint with all endpoints
4. **Landing template** — KPI cards + nav cards
5. **Workspace template** — Draft list with filters
6. **Editor template** — 9-section tabbed UI with JavaScript
7. **Admin templates** — Ingest, drafts management, revisions, classification, export
8. **Integration** — Wire into module registry, add feature flag, test
9. **CLI** — Port bulk ingest as Flask CLI command

---

## 12. SPEC NUMBER FORMAT

Spec numbers follow this pattern: `ONGC/{SUBSET}/{NUMBER}/{YEAR}`

Examples: `ONGC/DFC/01/2026`, `ONGC/PC/67/2026`, `ONGC/WS/12/2026`

Parsing helper:
```python
def parse_spec_number(spec_number: str) -> dict:
    """Parse 'ONGC/DFC/01/2026' into components."""
    parts = spec_number.split("/")
    if len(parts) == 4 and parts[0] == "ONGC":
        return {"prefix": parts[0], "subset": parts[1], "number": parts[2], "year": parts[3]}
    return {"raw": spec_number}
```

---

## 13. IMPACT SCORING FORMULA

```python
def calculate_impact(operational: float, safety_env: float, supply_chain: float, no_substitute: bool) -> tuple[float, str]:
    """Returns (weighted_score, grade)."""
    score = (operational * 0.5) + (safety_env * 0.3) + (supply_chain * 0.2)
    if no_substitute:
        return (score, "HIGH")
    if score <= 2.3:
        return (score, "LOW")
    elif score <= 3.5:
        return (score, "MODERATE")
    else:
        return (score, "HIGH")
```

---

This prompt contains every detail needed to convert the Streamlit CSC Workflow app into a fully integrated Flask module matching the existing ONGC Digital Workspace architecture. Follow the patterns from the Inventory Intelligence module for UI layout, the module registry for registration, and the existing auth/permissions system for access control.
