# Repo-Wide Security & Bug Audit Prompt

Paste everything below the line into a fresh Claude Code session at the repo root.

---

You are performing a full security and correctness audit of this repository: a Flask 3
application (`app/`, ~136 Python modules / ~44k LOC) serving ONGC internal workflows —
office management, inventory intelligence, CSC workflow, QC laboratory monitoring,
manpower planning, reports, and an admin console with database backup/restore. Stack:
Flask-SQLAlchemy, Flask-Login, Flask-WTF, Flask-Migrate, Flask-Caching, PyMySQL/psycopg,
pandas/numpy, openpyxl, python-docx, python-pptx, PyMuPDF, reportlab. Deployed via
gunicorn on Railway.

Read `AGENTS.md` first and obey it — especially the `.env` and database rules. Never edit
`.env`, never print secrets, never touch `DB_*` / `DATABASE_URL` / `SQLALCHEMY_DATABASE_URI`
values, and never run destructive database commands. This audit is read-mostly: the only
writes are source-code fixes.

## Method

Work in four phases. Do not skip ahead — report findings from each phase before fixing.

### Phase 1 — Map the attack surface

Inventory, with file:line references:

1. Every route across `app/modules/*/routes.py`, `app/core/auth/routes.py`, and any
   blueprint registered in `app/core/module_registry.py`. For each, record: HTTP methods,
   whether `@login_required` is present, which permission/role gate applies, and whether
   it mutates state.
2. Every place user input reaches the filesystem, the database, a subprocess, a template,
   or an HTTP response.
3. The trust boundary for each module — who is supposed to be able to reach it, per
   `app/core/permissions/` and the `roles_allowed` / `permission_code` fields in
   `app/core/module_registry.py`.

### Phase 2 — Security review

Check each of these classes explicitly. For every one, state either a concrete finding
(file:line + how it is reached by an attacker) or "checked, no issue found" — do not
silently skip a class.

**Authentication & authorization**
- Routes that mutate state but lack `@login_required` or a permission decorator.
- Horizontal privilege escalation: does any route trust an ID from the URL/form without
  confirming the current user owns or may access that record? Check every
  `.get(id)` / `.filter_by(id=...)` that comes from `request` data.
- Vertical escalation: admin-only functionality in `app/modules/admin/routes.py`
  (19 routes) reachable by non-admins; role/permission edits that let a user raise
  their own privileges.
- Session config: cookie flags (`SESSION_COOKIE_SECURE`, `HTTPONLY`, `SAMESITE`),
  session lifetime, session fixation on login, logout invalidation.
- Password handling in `app/core/auth/` — hashing algorithm, timing, reset/change flows,
  rate limiting or lockout on login, user enumeration via differing error messages.

**Secrets & configuration** (`config.py`)
- `SECRET_KEY` falls back to `"fallback-insecure-key-change-me"` (config.py:36) and
  `BOOTSTRAP_ADMIN_PASSWORD` to `"ChangeMe@First1"` (config.py:115). Determine whether
  production can boot with those defaults, and make it fail loudly instead of silently
  running insecure.
- `DEBUG` / `TESTING` reachable in production; stack traces or SQL echoed to responses.
- Any credential, token, or connection string committed in tracked files (search the repo,
  not just `config.py`). Report location only — never print the value.

**Injection**
- SQL: ~428 `execute(` / `text(` call sites. Prioritise interpolated SQL:
  `app/core/services/backups.py:433` builds `SELECT {columns} FROM \`{table_name}\``
  and `app/cli/csc.py:590,598` build `UPDATE {table_name} SET {column_name}`. For each,
  trace whether the interpolated identifiers can originate from user input or only from a
  hardcoded allowlist. Fix by allowlisting identifiers and binding all values.
- Command injection: `subprocess.run` in `app/core/services/backups.py`
  (lines ~505, 523, 759) invoking database dump/restore tools. Verify `shell=False`,
  argument lists (never a joined string), no user-controlled binary path, and that
  DB credentials are passed via env/pgpass rather than argv.
- Template injection: any `render_template_string`, or user data reaching a template
  without escaping.
- Path traversal: the 18 `send_file` / `send_from_directory` call sites, plus every
  upload target in `app/modules/{admin,quality_control,inventory,csc}` handling
  `request.files`. Confirm filenames are sanitised (`secure_filename`) and resolved
  paths are constrained under an intended base directory.

**Deserialization & archive handling**
- `pickle.load` at `app/core/services/inventory_intelligence.py:1587,1651` — determine
  who can write those cache files. Pickle over an attacker-writable path is RCE. Prefer
  parquet/feather/JSON, or at minimum verify the file is inside an app-owned directory
  with a validated signature.
- Tar extraction in `app/core/services/backups.py`: `_safe_extract_tar` (line 342) calls
  `archive.extractall(destination)` at line 348. Verify it actually blocks absolute paths,
  `..` traversal, symlink and hardlink members, and device files *before* extracting —
  and that it does so for every member, not just the first. If the guard is incomplete,
  fix it (use `filter="data"` on Python 3.12+, plus an explicit per-member check).
- Uploaded spreadsheets/documents parsed by openpyxl, pandas, python-docx, python-pptx,
  PyMuPDF: enforce size limits (`MAX_CONTENT_LENGTH`), extension/content-type validation,
  and confirm parsing errors on hostile files are handled rather than 500-ing.

**Web-layer issues**
- CSRF: `WTF_CSRF_ENABLED` is on (config.py:109), but check every POST/PUT/DELETE
  route and every fetch/XHR in `app/static/js/` and inline template scripts for a
  missing or hardcoded `csrf_token`. Flag any `@csrf.exempt`.
- XSS: every `|safe`, `|striptags` misuse, `innerHTML` assignment, and the sanitiser in
  `app/core/services/rich_text.py` — check whether it fails open on malformed input.
- Open redirect on login `next=` parameters.
- Mass assignment: forms or JSON bodies written to models via loops or `**kwargs`.
- IDOR in export endpoints (`csc_export.py`, report/download routes).
- Security headers and CORS configuration.

**Dependencies**
- Run `pip list --outdated` and check `requirements.txt` for known-vulnerable pins.
  Note that `psycopg2-binary` and `reportlab` are listed unpinned and `reportlab`
  appears twice — flag the duplication.

### Phase 3 — Correctness bugs

Independently of security, hunt for real defects — each must come with a concrete
failure scenario (inputs/state → wrong output, crash, or corruption):

- Exception handling that swallows errors (`except Exception: pass`, bare `except`) and
  leaves partial state committed.
- Missing DB transaction boundaries / rollback on failure; `commit()` inside loops;
  operations that should be atomic across multiple tables (backup restore, imports).
- N+1 queries and unbounded `.all()` loads on large tables in inventory and QC dashboards.
- Timezone-naive vs aware datetime mixing; date arithmetic across month/year boundaries
  in forecasting and weekly QC reporting.
- Float money/quantity arithmetic where Decimal is required.
- pandas correctness: chained assignment, silent dtype coercion, `merge` producing
  duplicate rows, `fillna("")` on numeric columns, empty-DataFrame edge cases.
- Cache invalidation: `Flask-Caching` memoized functions (e.g. `_get_nav_modules_cached`
  in `app/core/module_registry.py`) whose keys omit an input they depend on, or that are
  not cleared on the right mutations.
- Jinja templates referencing attributes that can be `None`, or icon/class names that
  do not exist in the loaded icon font.
- Dead code, unreachable branches, and TODOs that mark known-broken behaviour.

### Phase 4 — Fix

After reporting, fix the findings, highest severity first.

Rules for fixes:
- Match the surrounding code's style, naming, and comment density. Comments explain *why*,
  not *what*.
- One logical change at a time. Do not bundle refactors into security fixes.
- Do not change behaviour beyond the defect. No opportunistic rewrites, no reformatting
  of untouched lines, no new dependencies without saying so.
- Never weaken a check to make a test pass.
- If a fix requires a decision that is genuinely the user's (e.g. failing startup when
  `SECRET_KEY` is unset, or dropping the pickle cache), stop and ask rather than guessing.
- If a finding is real but too large to fix safely in this pass, say so explicitly and
  describe the fix rather than half-applying it.

Then verify:
- `python -m pytest tests/ -q` — the suite in `tests/` covers backups, CSC exports,
  inventory monitoring/seeding, manpower planning, MSDS, and the QC parser. All tests
  that passed before must still pass; report any that were already failing.
- Add a regression test for each security fix where the existing suite makes that
  straightforward.
- Confirm the app still imports and starts: `python -c "from app import create_app; create_app()"`.

## Output

Produce a findings table ordered by severity (Critical / High / Medium / Low), each row:
`severity | file:line | what an attacker or user does | consequence | fix applied or proposed`.

Follow it with: what you fixed, what you deliberately did not fix and why, what needs a
human decision, and the test results verbatim.

Prefer a small number of confirmed, exploitable findings over a long list of theoretical
ones. For every finding, state how you verified it is reachable in this codebase — if you
could not verify reachability, mark it "unverified" rather than presenting it as a bug.
