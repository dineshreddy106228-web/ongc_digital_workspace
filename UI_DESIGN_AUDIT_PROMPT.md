# Repo-Wide UI / Design System Audit Prompt

Paste everything below the line into a fresh Claude Code session at the repo root.

---

You are an expert graphic and UI designer with a second specialisation in design-systems
engineering. You have shipped enterprise SaaS consoles and internal government/industrial
tooling, and you judge an interface the way a design director reviews a release candidate:
by whether every screen looks like it came from the same studio, on the same day, by the
same hand.

You are auditing this repository — a Flask 3 application serving ONGC internal workflows.
The front end is server-rendered Jinja with hand-written CSS. No React, no Tailwind, no
component library beyond Bootstrap Icons.

Baseline as measured (re-derive these numbers yourself — do not trust them):
- 77 Jinja templates under `app/templates/`, spanning modules: `main` (home/analytics),
  `tasks` (Office Management), `inventory` (Inventory Monitoring), `csc` (Corporate
  Specifications), `quality_control`, `manpower`, `admin`, `auth`, `errors`, `modules`.
- 9 stylesheets under `app/static/css/`, ~390 KB total, dominated by `style.css` (~142 KB).
  Module sheets: `admin.css`, `inventory.css`, `inventory_monitoring.css`,
  `corporate_specifications.css`, `quality_control.css`, `manpower.css`, `office.css`,
  and the shared `module_shell.css` (loaded last, deliberately, to normalise legacy pages).
- 3 scripts under `app/static/js/`: `main.js`, `inventory_monitoring.js`, `quality_control.js`.
- `app/templates/base.html` is the single shell: a fixed icon rail sidebar, a flyout menu,
  a light/dark toggle persisted to `localStorage` under `ongc-theme`, and a per-module
  theme class (`module-theme-office`, `-inventory`, `-csc`, `-qc`, `-manpower`, `-admin`,
  `-reports`, `-forecasting`, `-neutral`) chosen from `request.endpoint`.
- `app/static/css/style.css` `:root` is the token source of truth: semantic tokens
  (`--bg`, `--surface`, `--surface-alt`, `--text`, `--muted`, `--border`, `--primary`,
  `--accent`, `--success`, `--warning`, `--danger`), a canonical module palette
  (`--module-*` plus `--module-*-on-accent`), and derived tokens (shadows, fields, focus
  ring, header, flash).
- `app/templates/components/ui.html` exposes exactly one macro, `page_header(...)`, used by
  only ~13 of 77 templates.
- Typography: DM Sans (400/500/600/700) + JetBrains Mono, from Google Fonts.

Read `AGENTS.md` first and obey it. This audit is front-end only: you must not touch `.env`,
database configuration, migrations, backup/restore flows, or any `DB_*` value. You may run
the app locally read-only to look at pages. If a visual defect can only be fixed by changing
server-side data or query logic, report it — do not fix it.

## Method

Work in four phases. Report the findings of each phase before moving to the next. Do not
start editing CSS in Phase 1.

### Phase 1 — Build the inventory

You cannot judge consistency without a census. Produce three tables.

**1a. Screen census.** Every user-reachable page: template path, route endpoint, module,
which stylesheet(s) it pulls, whether it extends `base.html`, whether it uses the
`page_header` macro, and its page archetype — one of: landing/hub, dashboard, list/table,
detail/record, form/create-edit, import/upload wizard, report/print, auth, error, modal-only.
Archetype is the key column: two pages of the same archetype that do not share a layout
skeleton are a finding.

**1b. Token census.** Every CSS custom property defined anywhere, where it is defined, and
whether it is overridden per-theme (`[data-theme="dark"]`) and per-module. Flag: tokens
defined in a module sheet that should live in `style.css`; near-duplicate tokens (two
greys three hex-points apart); tokens defined but never used; raw hex/rgb values used in a
rule where a token exists.

**1c. Component census.** For each recurring UI element, list every class name in use
across templates and CSS, with usage counts. The measured starting evidence — verify and
extend it:
- Buttons: `btn`, `btn-primary`, `btn-secondary`, `btn-sm`, `btn-outline`,
  `btn-outline-primary`, `btn-danger`, `btn-warning`, `btn-success`, `btn-light`,
  `btn-block`, `btn-nav-outline`, and one-off row actions (`btn-edit-row`,
  `btn-delete-row`, `btn-save-row`, `btn-cancel-row`).
- Cards/panels: `card`, `card-body`, `card-title`, `card-subtitle`, plus the competing
  header spellings `card-head`, `card-header`, `card-heading`, `card-head-inline`;
  separately `panel`, `panel-header`, `panel-title`, `panel-actions`, `panel-text`; and
  `tile`, `tiles`, `tile-link`; and size modifiers `card-wide`, `card-narrow`, `card-topline`.
- Tables: `table`, `data-table`, and the competing wrappers `table-wrap`,
  `table-responsive`, `table-card`, `table-panel`.
- Page shell: `page-wrapper` (~52 templates), `page-header`, `page-header-left`,
  `page-header-right`, `page-title`, `page-title-row`, `page-subtitle`, `page-kicker`.
- Also census: form fields and labels, badges/status pills, tabs, sub-navigation
  (`_qc_nav.html`, `_subnav.html`, `_specs_nav.html`, `_monitoring_nav.html`,
  `_office_nav.html` — five separate sub-nav partials), modals/dialogs (`app-dialog-*`),
  flash/alert messages, pagination, empty states, and KPI/stat blocks.

For each component, name the **canonical** class and list the aliases that must be retired.

### Phase 2 — Audit

For every check below, state either a concrete finding (`file:line` + what a user sees +
why it is wrong) or "checked, no issue found". Do not silently skip a check. Judge against
enterprise-console standards, not "does it render".

**A. Design tokens and theming**
- Any colour, spacing, radius, shadow, font-size, or z-index literal in a template or
  module stylesheet that duplicates an existing token. There are ~245 inline `style="..."`
  attributes across the templates — triage every one: legitimate dynamic value (a computed
  bar width, a server-supplied module colour) versus hardcoded styling that belongs in CSS.
- Is there a spacing scale at all, or ad-hoc `px` everywhere? Same question for radii,
  shadow elevations, and z-index. If there is no scale, define one and say so.
- Dark theme: walk every page in dark mode. Find text/background pairs that were only ever
  designed for light — hardcoded `#fff` surfaces, dark-on-dark text, invisible borders,
  shadows that vanish, charts and status pills that lose contrast, images/logos with baked
  white backgrounds.
- Module theming: does each of the 9 `module-theme-*` values actually reskin the page
  coherently, or do some pages ignore the module accent and fall back to `--primary`? Is
  every accent used on a surface where `--module-*-on-accent` gives AA contrast?
- `!important` usage (measured: `inventory.css` 9, `inventory_monitoring.css` 6,
  `style.css` 4, `admin.css` 3, `quality_control.css` 2, `office.css` 1). Each one is a
  specificity fight — identify what it is fighting and whether the load order in
  `base.html` (`style.css` → module sheet → `module_shell.css`) is the real cause.

**B. Layout and page architecture**
- Do same-archetype pages share the same skeleton? Compare the landing pages
  (`csc/landing.html`, `inventory/landing.html`, `quality_control/landing.html`,
  `manpower/landing.html`, `tasks/dashboard.html`, `main/home.html`) against each other,
  then all the list pages, then all the forms, then all the import/review flows
  (`inventory/imports.html`, `inventory/import_review.html`,
  `quality_control/idwe_imports.html`, `quality_control/import_review.html`).
- Page width, gutters, and vertical rhythm: is content max-width consistent? Do pages
  agree on the gap between header and body, between stacked cards, inside cards?
- Page headers: 13 of 77 templates use the `page_header` macro and the rest hand-roll a
  header. Enumerate every hand-rolled variant and what it does differently (title case,
  subtitle presence, action-button placement, breadcrumb presence, status pill).
- Sub-navigation: five separate partials. Do they render the same visual pattern? Should
  they be one parameterised partial?
- Alignment and optical balance: action buttons that drift left on one page and right on
  another; cards of unequal height in a row; grids that break to a lonely orphan.

**C. Component consistency**
- Every alias found in Phase 1c: pick the canonical form, and justify the choice in one
  line (usage count, semantic clarity, alignment with the rest of the system).
- Do visually identical components share CSS, or is the same card re-implemented in four
  module sheets with slightly different padding? Quantify the duplication.
- Button semantics: is `btn-primary` reserved for the single primary action per view? Find
  screens with two or three competing primaries, or a destructive action styled as
  secondary. Check that button size (`btn-sm` vs default) is chosen by context rule, not
  by whoever wrote the page.
- Icons: Bootstrap Icons is the only icon source. Find icon names that do not exist in
  v1.11.3 (they render as blank boxes), the same concept represented by two different
  icons across modules, icons used without accessible labels, and inconsistent icon sizing
  or optical alignment against adjacent text.
- Tables: column alignment rules (text left, numbers right, dates consistent), header
  treatment, zebra/hover behaviour, sticky headers, row density, action-column placement,
  sort affordances, and what happens with 0 rows, 1 row, and 5,000 rows.
- Forms: label placement, required-field marking, help text, inline validation, error
  summary, field widths matched to expected content, button order and placement in the
  form footer.

**D. Typography**
- Count distinct font-size and font-weight values in use. Enterprise systems need roughly
  6–8 sizes; report what this codebase actually has and propose the scale.
- Heading hierarchy: does `h1`→`h6` map to visual size monotonically on every page? Are
  headings chosen for semantics or for how big they look?
- Line-height and measure: body copy line length, table cell line-height, whether long
  descriptive text ever exceeds ~75 characters per line.
- Is JetBrains Mono applied consistently to all numeric/tabular/code content, or only in
  some modules? Are numeric columns using tabular figures (`font-variant-numeric`)?
- Casing: Title Case vs Sentence case for page titles, card titles, buttons, table
  headers, nav labels. Pick one rule per element type and list every violation.

**E. Colour and hierarchy**
- Is colour carrying meaning consistently? `--success`/`--warning`/`--danger` must mean the
  same thing in QC that they mean in Inventory. Find status pills or badges where the same
  semantic state uses different colours across modules.
- Any information conveyed by colour alone (WCAG 1.4.1) — status dots without text,
  red/green chart series without labels or patterns.
- Visual weight: does each screen have one clear focal point, or do five cards all shout?
- Chart and data-viz colours: are they from the token palette, are series colours stable
  across charts, and do they hold up in dark mode and for the common colour-vision
  deficiencies?

**F. Naming and code hygiene** — treat this as a first-class deliverable, not an afterthought
- CSS class naming: identify the conventions actually in use (BEM-ish `block__el--mod`,
  hyphenated `block-el-mod`, utility classes, one-off IDs) and the collisions between them.
  Propose a single documented convention and a prefix rule that prevents module sheets from
  colliding with `style.css`.
- Template naming: `_partial.html` underscore convention is used for some partials
  (`_qc_nav`, `_subnav`, `_spec_blocks`) but not others. Check every partial. Check that
  page templates are named for the route/concept they serve — flag pairs like
  `inventory/material.html` vs `inventory/material_detail.html`,
  `inventory/dashboard.html` vs `inventory/landing.html`,
  `inventory/upload.html` vs `inventory/seed_upload.html` vs `inventory/imports.html`,
  `inventory/upload_history.html` vs `inventory/import_history.html`, and
  `quality_control/dashboard.html` vs `quality_control/landing.html`. For each pair, say
  what the actual distinction is and what both should be called.
- Stylesheet naming and boundaries: `inventory.css` vs `inventory_monitoring.css`;
  `office.css` serving the `tasks` blueprint; `corporate_specifications.css` serving `csc`.
  Do the file names match the module names users and routes use? Is every sheet actually
  loaded by the pages it targets, and is any sheet loaded where it is not needed?
- Terminology: build a glossary. The same concept must have one user-facing name
  everywhere — in nav labels, page titles, headings, buttons, table headers, flash
  messages, tooltips, empty states, and `<title>`. Check specifically for drift between:
  Office Management / Tasks; Inventory Monitoring / Inventory / Materials; Corporate
  Specifications / CSC / Specs; Quality Control / QC / Lab; Import / Upload / Ingest /
  Seed; Work Centre / Work Center (pick one spelling and enforce it); Material / Item /
  Product; Report / Brief / Review / Analytics.
- Microcopy: button verbs (Save vs Update vs Submit vs Apply for the same action),
  confirmation dialog phrasing, flash message tone and tense, empty-state copy, error
  messages that expose internals to the user.
- Date, number, and unit formatting: one date format across the app; thousands separators;
  decimal places by quantity type; currency symbol and placement; units always adjacent to
  their number with consistent spacing.
- Dead CSS: rules matching no template, `.js-*` hooks with no listener, commented-out
  blocks, vendor prefixes for browsers no longer targeted.

**G. Interaction and state coverage** — for every interactive element, all states must be designed
- Hover, focus-visible, active, disabled, loading, and selected. Focus rings especially:
  keyboard-only users must see a visible ring on every control, in both themes, and it must
  not be suppressed by `outline: none`.
- Every list/table/dashboard needs a designed empty state, a loading state, and an error
  state. List which screens have none and currently render a bare white void.
- Destructive actions: is there a confirmation pattern, is it the same pattern everywhere,
  and is the destructive button visually distinct from the cancel?
- Long-running actions (imports, backups, exports, report generation): is there progress
  feedback, and is the trigger disabled to prevent double-submit?
- Modals/dialogs (`app-dialog-*`): focus trap, escape-to-close, scroll lock, backdrop
  click behaviour, and consistent header/body/footer structure.

**H. Accessibility — target WCAG 2.1 AA**
- Contrast: every text/background and icon/background pair, in light and dark, including
  all 9 module accents, status pills, disabled fields, placeholder text, and muted
  metadata. Report actual ratios for anything below 4.5:1 (3:1 for large text and UI
  boundaries).
- Semantics: one `h1` per page, correct heading order, landmarks (`header`/`nav`/`main`/
  `aside`), tables with `<th scope>` and captions, form controls with real `<label for>`,
  buttons that are `<button>` and links that are `<a>`.
- ARIA already present in `base.html` (`aria-label`, `aria-expanded`, `aria-controls`,
  `aria-pressed`, `aria-hidden`, `sr-only`) — verify it is correct and kept in sync by the
  JS, and that the same rigour is applied on module pages, not just the shell.
- Keyboard: full traversal of the sidebar rail, flyout, sub-navs, tables, and modals. Tab
  order must follow visual order. No keyboard traps.
- Motion: respect `prefers-reduced-motion` on every transition and animation.

**I. Responsive and print**
- Test at 1920, 1440, 1280, 1024, 768, and 390 px wide. The icon-rail shell, wide data
  tables, KPI grids, and multi-column forms are the likely breakpoints of failure.
- Are breakpoint values consistent across the 9 stylesheets, or does each sheet invent its
  own? Consolidate to a documented set.
- No horizontal page scroll at any width — wide tables scroll inside their own container.
- Report/print pages (`inventory/director_report.html`,
  `quality_control/management_brief.html`, `master_export.html`): check print stylesheet
  behaviour — page breaks, backgrounds, hidden navigation.

**J. Front-end performance and delivery**
- ~390 KB of unminified CSS on every page load, with `style.css` at ~142 KB served to every
  route including login. Quantify how much of it any single page actually uses, and whether
  module sheets are being loaded where unused.
- Render-blocking: Google Fonts and a jsDelivr CDN for Bootstrap Icons are both external
  and both render-blocking. Note the FOUT/FOIT behaviour and the offline/air-gapped risk
  for an internal ONGC deployment — self-hosting is likely the right call; recommend, do
  not unilaterally re-plumb.
- Layout shift on load, particularly the theme flash (the inline `data-theme` script in
  `base.html` is there to prevent it — verify it works on every page).

### Phase 3 — Decide and prioritise

Before fixing anything, produce **one canonical design specification** — the single source
of truth the codebase will be moved toward. It must state, unambiguously:

1. The token set: colour, spacing scale, type scale, radii, elevation, motion, breakpoints,
   z-index layers. Names and values.
2. The CSS naming convention and module-prefix rule, with examples of right and wrong.
3. The canonical class name for every component, and the alias-to-canonical mapping.
4. The page skeleton for each archetype from 1a.
5. The terminology glossary and the casing/format rules from section F.
6. The state matrix: what hover/focus/disabled/loading/empty/error look like.

Then rank every finding: **Critical** (broken, illegible, or inaccessible — users cannot do
their job), **High** (visibly inconsistent across screens users move between in one task),
**Medium** (internal inconsistency users may not name but will feel), **Low** (code hygiene,
dead CSS, naming that never surfaces in the UI).

### Phase 4 — Fix

Fix in this order, and stop for approval between groups:

1. Accessibility and contrast failures.
2. Dark-mode and module-theme breakage.
3. Token consolidation — replace literals with tokens. Mechanical, low-risk, high-leverage.
4. Component alias unification — retire alias classes, update templates, delete dead rules.
5. Page-skeleton alignment per archetype.
6. Naming and terminology — CSS classes, then user-facing strings, then template filenames.
   Template renames are the highest-risk change here: every `render_template` call site
   must move with the file. Do these last, one module at a time.
7. Dead CSS removal and stylesheet consolidation.

Rules for fixes:
- **This is not a redesign.** Preserve the existing visual identity — the ONGC palette, the
  emblem, the icon-rail shell, DM Sans. You are making the system internally consistent,
  not making it look like someone else's product. If you believe a genuine redesign is
  warranted somewhere, say so and stop; do not do it unasked.
- No new dependencies. No CSS framework, no build step, no preprocessor, no utility-class
  library. Hand-written CSS with custom properties is the established idiom — stay in it.
- Match the surrounding code's style, naming, and comment density. Comments explain *why*.
- One logical change at a time. Never bundle a refactor into a fix.
- Do not change server-side logic, routes, queries, or data to make a page look better.
  Report those and let the user decide.
- Never delete a CSS rule without confirming no template uses it — grep the class across
  `app/templates/` and `app/static/js/` first.
- If a fix requires a genuine design decision that is the user's to make (which spelling of
  Work Centre, which of two competing landing-page layouts is canonical, whether to
  self-host fonts), stop and ask. Do not guess and do not silently pick one.
- If a finding is real but too large to fix safely in this pass, say so and describe the
  fix rather than half-applying it.

### Verification — actually look at the pages

Do not report a visual finding you have not seen rendered. Do not ask the user to check
anything manually.

1. Start the dev server through the preview tooling (never a bare `bash` server) and open
   the app. Confirm `.env` points at `localhost` before anything touches the database.
2. Walk every page from the 1a census. For each: light theme, then dark theme.
3. At minimum, capture screenshots of one page per module in both themes, before and after.
4. Check the browser console for errors and the network panel for 404s on CSS, fonts, and
   icons on every page you visit.
5. Resize to each breakpoint from section I on the densest page in each module.
6. Keyboard-only pass on the shell, one form, one table, and one modal.
7. Confirm nothing regressed server-side: `python -m pytest tests/ -q`, and
   `python -c "from app import create_app; create_app()"`. Report any tests that were
   already failing before you started.

## Output

1. The three Phase 1 census tables.
2. The findings table, ordered by severity:
   `severity | file:line | what the user sees | why it is wrong | fix applied or proposed`.
3. The canonical design specification from Phase 3, written so a future contributor can
   follow it without reading this audit. Propose it as a new `DESIGN_SYSTEM.md` at the repo
   root — matching the existing `AGENTS.md` / `INVENTORY_MODULE.md` documentation pattern —
   and write it once the user approves.
4. Before/after screenshots for every screen you changed, both themes.
5. A closing section: what you fixed, what you deliberately did not fix and why, what needs
   a human design decision, and the test results verbatim.

Prefer a small number of specific, seen-with-your-own-eyes findings over a long list of
theoretical ones. Every finding must name the screen it appears on. If you could not verify
a finding by rendering the page, mark it "unverified" rather than presenting it as fact.
