# Inventory Monitoring

Inventory Monitoring replaces the Inventory Intelligence user interface with a
snapshot-based monitor for material Groups 09 and 10. It does not alter the
application database connection and it never points local imports at Railway.

## Import sequence

1. As a module user, open **Inventory Monitoring → Import workbooks**.
2. Import the work-centre material mapping workbook. This creates a versioned
   mapping; material codes are stored as text so Group 09 leading zeroes remain
   intact.
3. Import Group 09 and Group 10 workbooks. Each upload is staged, validated,
   reviewed, and committed only after confirmation.
4. Select the as-on date if it cannot be derived from the file name. Both group
   imports must use the same date before the portfolio is published.

Each record retains its source workbook, sheet and row. A replacement upload is
kept in history and marks the prior batch as superseded rather than deleting it.

## Current rules

- `stock_months_warning`: 6 months (default)
- `stock_months_critical`: 12 months (default)
- high stock coverage, low stock coverage, and open PO/PR against high stock
  coverage create exceptions.
- only stock lines carrying an inventory value above zero are monitored; a nil
  or blank value is listed in the import notes and left out of every figure.

Super-users may update the stock-month thresholds in Administration. Threshold
changes apply to future imports; prior batches remain auditable snapshots.

## Monitored materials by corporate specification

The landing page groups every monitored material code by the category of its
current ONGC corporate specification, read from the specification number:
`ONGC / DFC / 01 / 2026` → DFC → Drilling Fluid Chemicals. Categories in use are
DFC (Drilling Fluid), PC (Production), WS (Well Stimulation), WIC (Water
Injection), WM (Water Maker), UTL (Utility), LPG, CCA (Cement and Cement
Additives) and WCF (Well Completion Fluids). A material code held in stock
but absent from the list is grouped as **Not in Corporate Specification List**.
Each tile opens the material register filtered to that category.

Every table that lists materials — the health register, each work-centre
register, the material register, and the register
tables in both decks — is ordered by specification category and then by
specification serial (DFC / 01, DFC / 02, then PC / 01), under a sub-heading per
category, with **Not in Corporate Specification List** last. In the decks a
category heading that runs onto the next slide is repeated as "(continued)".
Tables that exist to rank by value — the portfolio's top materials, the
concentration slides — keep their value order, as their headings say.

The specification list is **not** a second copy: it is the QC Laboratory
Monitoring testing-standards master (`qc_testing_standards`), the same rows that
carry each chemical's SLA. Maintain it in **QC Laboratory Monitoring → Testing
standards**; the inventory tiles follow immediately, and there is nothing to keep
in step by hand.

Master rows whose material code reads "Code not mapped*" or "-" cannot be matched
to stock. They are still counted in each tile's specification total, and the tile
states how many have no SAP code. Codes are matched after the same zero-padding
the imports use, so the master's `90001043` matches stock code `090001043`.

## Units of measure

The Group 09 and 10 exports carry no unit column, so every stock line was stored
without one. `_read_inventory` now accepts whatever the export calls it (`UoM`,
`Unit`, `Base Unit of Measure`, `Unit of Entry`, …), and until the export carries
one, **Administration → Fill missing units** backfills the unit from the retained
consumption and usage history (`inventory_consumption_seed_rows`,
`inventory_records`). Imports run the same backfill automatically.

That history covers 231 material codes — about 1,268 of the 2,708 current stock
lines, and 83% of inventory value. Adding a UoM column to the workbook export is
the only way to cover the rest.

## Groups 09 and 10 are one portfolio

Group 09 (oil well cement, barytes) and Group 10 (all other chemicals) are SAP
material groups, not separate reviews. Every figure on every page and in every
deck counts both together; only the per-material rows still carry the group as a
label. Nothing filters a view to one group.

A date still publishes only when both group workbooks are imported for it, which
is what guarantees the combined figure is complete.

### Superseded batches

A batch is hidden from the reviews when a *different* batch replaced it. A batch
recorded as its own successor is a defect — it once removed the whole Group 10
book from the health and work-centre views — so the readers treat it as live,
`import_workbook` repairs any such row before each import, and migration
`f8a9b0c1d2e` clears the ones already stored.

## Mapping comes from the uploaded inventory

Work-centre mapping is not declared ahead of the inventory. Every imported stock
line maps its material to the work centre reporting it, so nothing is held back
as a "held but not mapped" technical exception for a super-user to clear, and no
figure on any page is gated on a mapping decision. Materials with no current
corporate specification are already shown separately, under **Not in Corporate
Specification List**, which is where an unfamiliar code surfaces.

The mapping workbook remains the **work-centre directory**: it supplies zones,
work-centre names and the DFS / ST unit split used by the asset map and the unit
filter. It no longer declares which material may be held where.

The material register's **mapped plant / work centre** column is read straight
from the latest published Group 09 and Group 10 workbooks — wherever they report
stock, the material is mapped there — and the register is numbered so a row can
be cited by serial.

The super-user "Add to active mapping" form, the "Add a plant or work centre"
control and the per-material **Remove** action are retired with the same
reasoning: a mapping that can be declared by hand is a pre-mapping, and it would
have no effect on a register read from the workbooks.

Migration `a2b3c4d5e6f8` retires the declared pairs on an existing install:
workbook-sourced mappings are stood down, the mapping is restated from the
imported records, and the mapping exceptions they produced are deleted.

## Comparing an as-on date with an earlier one

A workbook may be imported for any as-on date; a date is published into the
review only when Group 09 and Group 10 are both imported for that same date.
Dates waiting for the second group are listed on the management review so a
half-finished upload is never silently invisible.

Comparison is explicit, never assumed:

- **Compare with** on the review picks any earlier published date. The default
  is the closest earlier one; the deck is exported on the same pair.
- The gap between the two dates is stated in days, because uploads need not be
  monthly.
- Deltas are **like-for-like**: only work centres that reported stock in both
  dates are netted. Work centres present in one date only are reported
  separately as entrants and exits with their value, so a missing submission is
  never read as a draw-down.

## Management review presentation

**Inventory Monitoring → Management review → Download presentation** builds a
PowerPoint deck for the selected reporting period from the published Group 09
and Group 10 snapshots. Nothing is stored: the deck is generated on request from
the same data the page shows.

The deck carries the executive summary, group and zone composition, coverage
bands, every work centre ranked by value, like-for-like movement against the
comparison period, and then the full registers:

- every material whose all-ONGC value crosses ₹ 1 Cr,
- critical low stock, low stock, slow-moving and excess stock lines,
- open PO / PR raised against stock that is already slow-moving or in excess,
- the source-workbook registers: non-moving, slow-moving, stock lying over one
  year, surplus items and open material-in-transit cases,
- the decisions sought from the review.

Mapping-review exceptions (held-but-not-mapped and unknown mappings) are not in
the deck; they stay in the application register. Each register is ranked by
value and capped at 250 lines per deck (`MANAGEMENT_REGISTER_ROW_LIMIT`), with
the omitted count stated on the slide.

## Work-centre review

Every work centre opens on the same review shape as the portfolio: headline
position, coverage-band mix, group split, like-for-like movement against an
earlier date for that centre, and then the four coverage registers and the
source-reported cases that were already there. **Download presentation** exports
the same content for that centre, honouring the DFS / ST unit filter and the
chosen comparison date.

Mapping is not edited by hand anywhere: there is no add form and no remove
action. A material's page lists the work centres that have reported stock of it,
and that list changes only by importing a workbook.

Every stock line the workbooks report at a centre counts, so a centre's total
reconciles to the portfolio figure without qualification. A centre's view uses the
latest **reporting date** it holds per material group from a non-superseded
batch — a back-dated import never displaces a later one. Its like-for-like
movement is netted at material level, with materials held in only one of the two
dates reported separately.

## Shared module chrome

Every page in the module renders the house shell from
`app/static/css/module_shell.css` — the pill module nav, the gradient hero with
its stat aside, the stat row, the section headings and the workbench tiles —
which Corporate Specifications, QC Laboratory Monitoring and Office Management
also use. The module supplies only its accent, through `mod-page is-inventory`
on the page wrapper; everything else is one definition, so the four modules
cannot drift apart. Inventory-specific components stay in
`inventory_monitoring.css`.

## Migration and legacy retirement

`b4d5e6f7a8b_add_inventory_monitoring_tables.py` only creates new
`inventory_monitoring_*` tables. It deliberately does **not** drop legacy
`inventory_*` tables: the former Inventory Intelligence data and shared material
master/MSDS helpers need a separately approved retirement after a verified backup.

Before a production migration or any later legacy-table retirement, take a fresh
backup and verify the configured target database is local when working locally.
Never run destructive database maintenance against Railway without explicit
approval.
