# DESIGN_SYSTEM.md

The single source of truth for tokens, component names, page skeletons, and
terminology across the ONGC Digital Workspace.

Where this document and the code disagree, this document is the target and the
code is the backlog. Read it before adding a stylesheet rule, a template, or a
user-facing string.

Stack: server-rendered Jinja + hand-written CSS. No framework, no build step, no
preprocessor. Bootstrap Icons is the only icon source.

---

## 1. Tokens

Every colour, size, and spacing value resolves to a token. A literal in a
stylesheet or a `style=""` attribute is a defect unless it is a server-computed
value (a bar width, a chart colour read from the DOM).

### 1.1 Semantic colour

Defined in `style.css :root`, redefined under `[data-theme="dark"]`. Components
reference these names only — never a hex literal, never a dark value directly.

| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg` | `#f4f5f7` | `#0f1723` | Page ground |
| `--surface` | `#ffffff` | `#182230` | Cards, panels, fields |
| `--surface-alt` | `#eef2f7` | `#1e2b3b` | Table headers, insets |
| `--text` | `#1a1d23` | `#edf2f7` | Body copy, headings |
| `--muted` | `#5f6775` | `#9aa8b9` | Secondary text, metadata |
| `--border` | `#d6dae0` | `#2b3a4d` | Rules, card edges |
| `--success` | `#15803d` | `#6fdca4` | Pass, healthy, complete |
| `--warning` | `#a16207` | `#f2ce7d` | Attention, aging, low |
| `--danger` | `#b91c1c` | `#ff9d9d` | Fail, overdue, critical |
| `--on-primary` | `#ffffff` | `#0f1723` | Text sitting on `--primary` |
| `--accent-text` | `#ad4e08` | `#f2a541` | `--accent` as text (see below) |
| `--focus-ring` | `rgba(26,86,219,.18)` | `rgba(91,140,255,.24)` | Focus shadow |

**Every token must have a value in both themes.** The one exception is
`--font-*`, `--radius*`, and `--transition`, which are theme-invariant.

`--accent` (`#e8710a`) fails AA as text on `--bg` (2.83:1). Use `--accent-text`
for any accent-coloured text; reserve `--accent` for fills and rules.

The legacy `--color-*` alias layer has been retired. Do not reintroduce it.

### 1.2 Module hues

Each module reskins the shell through `--module-accent`. Both themes are
normative — the dark values are *lighter*, which is why text on an accent must
use the token, never white.

| Module | Light | Dark |
|---|---|---|
| Office | `#2563eb` | `#60a5fa` |
| Inventory | `#8b643a` | `#d4ab79` |
| Specifications | `#665a86` | `#aaa0c7` |
| Quality Control | `#39766e` | `#7eb8b0` |
| Administration | `#4f46e5` | `#9d97f5` |

> **Normative.** Text or glyphs on `--module-accent` take their colour from
> `var(--module-on-accent)` (`#ffffff` light, `#0f1723` dark). Hardcoded `#fff`
> on an accent measures **2.54:1** against the dark office accent — a WCAG
> failure. The same applies to `--primary`: use `var(--on-primary)`.

### 1.3 Type scale

Derived from where declarations actually cluster; 81% land within 0.5px of a step.

| Token | rem | px | Role |
|---|---:|---:|---|
| `--fs-2xs` | 0.6875 | 11 | Eyebrows, table micro-labels |
| `--fs-xs` | 0.75 | 12 | Metadata, captions, chips |
| `--fs-sm` | 0.8125 | 13 | Table cells, dense UI |
| `--fs-base` | 0.875 | 14 | Default body text |
| `--fs-md` | 1 | 16 | Lead paragraphs, card titles |
| `--fs-lg` | 1.125 | 18 | Section headings (h2) |
| `--fs-xl` | 1.375 | 22 | Page titles (h1) |
| `--fs-2xl` | 1.75 | 28 | KPI figures |

**Weights: `400`, `500`, `600`, `700` only.** `base.html` loads DM Sans at those
four weights. `800`/`850`/`300` render as browser-synthesised faux-bold or
faux-light. Map `800` → `700`.

### 1.4 Spacing — 4px base

`--sp-1` 4 · `--sp-2` 8 · `--sp-3` 12 · `--sp-4` 16 · `--sp-5` 20 · `--sp-6` 24 · `--sp-8` 32

### 1.5 Radius, elevation, motion, layers, breakpoints

- **Radius** `--r-sm` 4px · `--r` 8px · `--r-lg` 14px · `--r-pill` 999px · `--r-circle` 50%
- **Elevation** `--e-1` resting · `--e-2` raised/hover · `--e-3` modal
- **Motion** `--t-fast` .12s · `--t` .18s · `--t-slow` .3s, all `ease`
- **Layers** `--z-base` 1 · `--z-sticky` 100 · `--z-rail` 200 · `--z-flyout` 300 · `--z-overlay` 400 · `--z-modal` 500 · `--z-toast` 600
- **Breakpoints** `1280` · `1024` · `768` · `560`, max-width queries only

---

## 2. CSS naming

Lowercase, hyphen-separated, `block-element--modifier`. State classes are always
`is-` or `has-` prefixed. No IDs for styling.

### 2.1 The prefix rule

A prefix declares ownership, and ownership determines which stylesheet a rule
may live in.

| Prefix | Owner | Meaning |
|---|---|---|
| `ws-` | `style.css` | Application shell — rail, flyout, header band, theme toggle |
| `mod-` | `module_shell.css` | **Shared module language.** Any component used by two or more modules lives here |
| `om- im- cs- qc- ad-` | module sheet | Genuinely module-specific |

> **A module must never style itself from another module's stylesheet.** If a
> second module needs a component, promote it to `mod-` *first*, then use it.
> Quality Control previously loaded `inventory.css` and used `inv-*` classes;
> those are now promoted to `mod-*` and `inventory.css` has been deleted.

### 2.2 Sanctioned utilities

Exactly three, all in `module_shell.css`: `.u-num` (tabular figures,
right-aligned), `.u-muted` (secondary text colour), `.sr-only`.

---

## 3. Component canon

| Primitive | Canonical | Retired aliases |
|---|---|---|
| Page shell | `page-wrapper mod-page` | `inv-page` |
| Page header | `page_header()` macro | `inv-module-header` |
| Page title | `mod-page-title` | `inv-module-title` |
| Page subtitle | `mod-page-sub` | `inv-module-subtitle` |
| Header actions | `mod-page-actions` | `inv-header-actions` |
| Card title | `mod-card-title` | `inv-card-title` |
| Card subtitle | `mod-card-sub` | `inv-card-subtitle` |
| Table wrapper | `mod-table-wrap` | `inv-audit-table-wrap`, `inv-table-wrap` |
| Table | `mod-table` | `inv-expand-table`, `inv-data-table` |
| KPI tile | `mod-kpi` / `mod-kpi-grid` | `inv-kpi-card` / `inv-kpi-grid` |
| Badge | `mod-badge` (+ `-info`/`-success`/`-muted`) | `inv-badge*` |
| Sub-nav bar | `mod-subnav-bar` | `inv-subnav` |
| Field label | `mod-field-label` | `inv-filter-label` |
| Empty state | `mod-empty` / `mod-empty-icon` | `inv-empty-state` |
| Cell metadata | `mod-cell-meta` | `inv-seed-input-meta` |

`mod-table-wrap` **owns `overflow-x: auto`.** Every table lives inside one; the
page body must never scroll sideways.

`mod-kpi` is deliberately *not* merged into `mod-stat`. `mod-stat` is a joined
bordered grid; `mod-kpi` is separate cards. Unifying them is a design decision,
not a rename — it is open (see §7).

> **Button semantics.** One `btn-primary` per view, on the single action the
> screen exists to perform. Destructive actions are `btn-danger` and never the
> visual twin of Cancel.

---

## 4. Page skeletons

```jinja
<div class="page-wrapper mod-page">
  {{ mod_nav(active) }}            {# optional #}
  {{ page_header(title, subtitle=…, kicker=…) }}
  {# archetype body #}
</div>
```

| Archetype | Body | Required states |
|---|---|---|
| Landing / hub | `mod-hero` → `mod-tiles` → feature card | — |
| Dashboard | `mod-stats` row → content cards | empty, loading |
| List / table | filter card → `card` > `mod-table-wrap` > `table` | empty, zero-results, loading |
| Detail / record | breadcrumb → hero → detail cards | not-found |
| Form | `card` > form, actions right-aligned in footer | validation error, saving |
| Import wizard | step indicator → upload → review → confirm | validating, error, progress |
| Report / print | hero → stats → cards; print rules in module sheet | no-data |

Headings wrap by measure, not by hand: use `text-wrap: balance` and a `max-width`
in `ch`. Never a literal `<br>` inside a heading.

---

## 5. Terminology

### 5.1 Glossary

| Canonical | Never | Note |
|---|---|---|
| **Task Management** | Office Management, Tasks, Office | "Office" survives only as a *scope* ("Office tasks") |
| **Inventory Monitoring** | Inventory, Materials | "Material register" is a page within it |
| **Corporate Specifications** | CSC, Specs | "CSC" stays in code and routes only |
| **QC Laboratory Monitoring** | Quality Control, Lab | "QC" permitted as a page-title prefix inside the module |
| **Work Centre** | Work Center | British spelling in all UI copy. DB columns stay `work_center_*` |
| **Import** | Upload, Ingest, Seed | The user-facing verb for bringing a workbook in |
| **Material** | Item, Product | — |
| **User Management** | Users, Admin | Registry `name` and `nav_label` must agree |

### 5.2 Casing and format

| Element | Rule | Example |
|---|---|---|
| Page title (h1) | Sentence case | Material register |
| Card / section title | Sentence case | Work centres reporting stock |
| Button | Sentence case, imperative | Import workbook |
| Table header | Sentence case | Inventory value |
| Kicker | Uppercase via CSS, sentence case in source | Data administration |
| `<title>` | `Page · Module — ONGC Digital Workspace` | Material register · Inventory Monitoring — ONGC Digital Workspace |
| Date | `%d %b %Y` | 21 Aug 2026 |
| Date + time | `%d %b %Y · %H:%M` | 21 Aug 2026 · 19:38 |
| Currency | `₹` + non-breaking space + grouped number | ₹ 5,05,91,000 |
| Unit | Number, space, unit | 12.5 MT · 4 months |

`<input type="date">` values are exempt — HTML requires ISO `%Y-%m-%d`.

### 5.3 Empty states

An empty state names what is missing and what to do. First sentence states the
fact, second offers the action. No apology.

> No materials in this health category. Import a Group 09 or Group 10 workbook
> to populate it.

---

## 6. State matrix

Every interactive element implements every applicable row.

| State | Treatment |
|---|---|
| Hover | Surface to `--surface-hover`, border to `--rule-strong`. Never underline a whole tile |
| Focus-visible | `box-shadow: 0 0 0 3px var(--focus-ring)` + border shift. Never `outline:none` without a replacement |
| Active | Elevation to `--e-1`, 1px optical nudge |
| Disabled | `--field-disabled-bg`, `--field-disabled-text`, `cursor:not-allowed`. Never opacity alone |
| Loading | Trigger disabled, label to present participle, spinner. Mandatory on imports, backups, exports |
| Selected | `--module-tint` ground, accent left border, `aria-current` |
| Empty | Icon, one-line fact, one-line action (§5.3) |
| Error | `--danger` border, message below the field naming the fix |

### Accessibility floor — WCAG 2.1 AA

- 4.5:1 body text, 3:1 large text and UI boundaries, **in both themes**.
- Every `<table>` gives header cells `scope="col"` or `scope="row"`.
- Every control has a programmatic label — `<label for>` or `aria-label`.
- Colour never carries meaning alone: status dots pair with text.
- All motion respects `prefers-reduced-motion`.

---

## 7. Open decisions

These need a human call and are deliberately not resolved in code:

1. **`mod-stat` vs `mod-kpi`.** Two stat components with different shapes.
   Merging them changes how Quality Control looks — a redesign, not a rename.
2. **Template filenames.** `material.html`, `dashboard.html` vs `landing.html`,
   and similar pairs do not describe what they render. Renaming means moving
   every `render_template` call site; deferred as high-risk, low user-visible value.
3. **Self-hosting fonts and icons.** Google Fonts and jsDelivr are render-blocking
   external hosts. On a restricted ONGC network, icons vanish and fonts fall back.
4. **Admin authorization.** `roles_required(ADMIN_ROLE)` rejects `superuser`
   accounts, while `module_access_required` grants them everything. A superuser
   gets 403 on all seven admin pages. Server-side, not a design defect.
