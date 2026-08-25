# Inventory Monitoring

Inventory Monitoring replaces the Inventory Intelligence user interface with a
snapshot-based monitor for material Groups 09 and 10. It does not alter the
application database connection and it never points local imports at Railway.

## Import sequence

1. As a module user, open **Inventory Monitoring → Import workbooks**.
2. Import Group 09 and Group 10 workbooks. Each upload is staged, validated,
   reviewed, and committed only after confirmation.
3. Select the as-on date if it cannot be derived from the file name. Both group
   imports must use the same date before the portfolio is published.

There is no third workbook. Work centres do not change, so the work-centre
directory is not uploaded any more — see **Unrecognised plants** below for what
happens when a workbook names one nobody claims.

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

## SAP plant codes, merged assets and unrecognised plants

SAP keeps a plant code per legacy asset, so a merger leaves two codes reporting
into one place long after the assets became one: N&H and B&S merged into NH-BS
while 12A1 and 13A1 stayed apart. Monitoring holds this on the asset rather than
duplicating the asset:

- `inventory_monitoring_work_centers.sap_plant_codes` lists every code reporting
  into an asset, comma separated (`12A1,13A1`).
- `merged_into_id` points a retired asset row at its successor. Imported stock
  lands on the successor, while the record keeps the name the workbook used, so
  a merged asset is one line in every register and history still resolves.
- **Administration → Assets and their SAP plant codes** is where both are set.
  The asset navigator lists an asset's codes beside it, and a merged asset is
  drawn as one pin, placed by the coordinates of either of the merged names.

### Unrecognised plants

Work centres are not expected to change, so a plant an import cannot place is
news rather than routine. Each import raises an open alert
(`inventory_monitoring_plant_alerts`) for any plant code that no asset claims —
or, while the export still carries no plant column, any work-centre name that is
neither in the directory nor assigned a zone. The stock is still imported and
still counted; what is outstanding is a decision.

The import review lists them before the import is confirmed, the import flashes
a warning, and Administration is where the module admin settles each one:
**attach** it to an existing asset (which is how one asset comes to carry two
codes), **add** it as a new asset, or record it as **not monitored**. Every
outcome is written to the administration audit trail.
**Administration → Read consumption and check plants** applies the same check to
stock that was imported before the check existed.

## Twelve-month consumption, and what a material is measured in

The detailed inventory sheet carries neither consumption nor a unit. The two
material summary sheets in the same workbook carry both — "09 Oil well cement -
Chemical S" / "10 Chemi incl mud chemi - Chemi" for quantity and its unit, and
the "Chemical P" / "Che 1" sheets for value in crores. They are found by their
columns, not their titles, and stored per material in
`inventory_monitoring_material_summaries`.

The unit is also written onto the material itself
(`inventory_monitoring_materials.uom`), because it belongs to the material and
not to a stock line. Two things follow:

- **Every table states the unit immediately after the material code** — the
  health registers, the material register, each work-centre register, the review
  tables, and every register in both decks.
- **A material is a liquid or a solid by its unit.** L, LT, LTR, KL, ML, M3, GAL
  and BBL are read as volume and ranked in kilolitres; G, KG, MT, TO, QTL and LB
  are read as weight and ranked in tonnes. A unit that counts pieces (NO, PAC,
  ST) is neither: those materials are counted in a footnote rather than forced
  into one of the two tables.

The management review ranks consumption by **value**: everything above ₹ 10 Cr
(`HIGH_CONSUMPTION_VALUE_FLOOR`), all-ONGC, both groups. Value is one scale, so
one table can hold every material.

Quantity is not one scale, and it is only ranked where like is compared with
like — the period movers, which are per material and split by phase. There is
deliberately no "top materials by consumption quantity" table: ranking litres
against tonnes needs a conversion the reader has to trust, and the same
information is already carried by the value table.

The twelve months are SAP's, not ours. The summary sheet states a rolling
twelve-month consumption against each material code, so the figure is a full
year read out of the workbook — it is not accumulated from the imported
snapshots, however few of those there are. The card says so, because two
snapshots a week apart otherwise make a twelve-month figure look impossible.

These figures are all-ONGC: the summary sheets carry no work centre, so they do
not narrow with an asset-scoped view, and the cards say so.

## Units of measure on a stock line

A stock line carries a copy of its material's unit so a quantity can be read on
the spot. The detailed inventory sheet still has no unit column —
`_read_inventory` accepts whatever the export calls it (`UoM`, `Unit`, `Base Unit
of Measure`, `Unit of Entry`, …) for the day it carries one — so the copy is
filled in after the fact, in this order:

1. the unit the workbook states against the material code on its material summary
   sheet, which is the material's own unit;
2. the retained consumption and usage history
   (`inventory_consumption_seed_rows`, `inventory_records`), for a code the
   summary sheets have never carried.

Imports run this automatically, and **Administration → Fill missing units**
re-runs it. The summary sheets cover every material the workbooks report, so this
is no longer a partial fill: history is now only a fallback.

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

## Reading the registers

- The **material register** leads with the material's own drill-down: latest
  source inventory first, then the mapping. A stock line at or below the
  critical threshold and one at or above the excess threshold are shaded and
  labelled, with a legend stating the boundary in force.
- **Largest movers** on the review is a table per phase — material, unit, the
  value at each of the two dates, and the movement — ranked by the size of the
  movement whichever way it went.
- On **Inventory Health**, a slow-moving row that the source workbook also names
  in its own non-moving, slow-moving, aged, surplus or transit register is
  shaded: the condition is confirmed twice over, by our reading of stock months
  and by the workbook's own registers.

## Mapping comes from the uploaded inventory

Work-centre mapping is not declared ahead of the inventory. Every imported stock
line maps its material to the work centre reporting it, so nothing is held back
as a "held but not mapped" technical exception for a super-user to clear, and no
figure on any page is gated on a mapping decision. Materials with no current
corporate specification are already shown separately, under **Not in Corporate
Specification List**, which is where an unfamiliar code surfaces.

The last imported mapping workbook remains the **work-centre directory**: it
supplies zones, work-centre names and the DFS / ST unit split used by the asset
map and the unit filter. It no longer declares which material may be held where,
and it is no longer uploaded — the directory is read from the batch already
retained, and anything new arrives through the unrecognised-plant alerts.

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

The scope is asked for rather than assumed. **Download presentation** opens a
chooser — the whole of ONGC, or a set of assets drilled down zone by zone.
Ticking any asset selects the second option, and ticking a zone ticks the assets
in it. A scoped deck counts only those assets in every figure, names its scope on
the cover, on every slide's source line and in its filename, and reports the
zone's name when the whole of one zone is chosen.

The chooser is shared, not inventory's own: the dialog comes from
`templates/components/_deck_scope.html`, its styles from the `mod-modal-*` and
`mod-scope-*` blocks of `module_shell.css`, and its behaviour from
`static/js/deck_scope.js`. QC Laboratory Monitoring's management review uses the
same component to pick laboratories, so both reviews ask the question the same
way and their action rows sit in the same place (`is-stacked-actions`).

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
`app/static/css/module_shell.css` — the pill module nav, the tinted hero with its
stat aside, the stat row, the section headings and the workbench tiles — which
Corporate Specifications, QC Laboratory Monitoring and Office Management also
use. The accent comes from the `module-theme-*` class `base.html` sets from the
request endpoint, so a page declares no colour of its own. Inventory-specific
components stay in `inventory_monitoring.css`, and they are written against the
semantic tokens (`--surface`, `--text`, `--muted`, `--border`, `--tone-*`,
`--mod-accent`) rather than fixed colours, so both themes work. The inner-page
heroes — health, work centre, management review, imports, settings — use the same
tinted panel as the landing hero rather than a dark gradient of their own.

Surfaces that are deliberately dark and carry white text in either theme — sticky
table heads, map pins — use `--mod-ink` / `--mod-ink-strong`, which mix the module
accent with near-black. Do not use the hero tokens for those: the hero is light.

## Migration and legacy retirement

`b1c2d3e4f5a6_inventory_plant_codes_and_consumption.py` adds the asset's SAP
plant codes and merge pointer, the material's unit, and the two new tables
(`inventory_monitoring_material_summaries`,
`inventory_monitoring_plant_alerts`). It adds nothing that existing rows depend
on, so an install upgrades without re-importing: **Administration → Read
consumption and check plants** fills consumption and raises the plant alerts from
the workbooks already retained.

`b4d5e6f7a8b_add_inventory_monitoring_tables.py` only creates new
`inventory_monitoring_*` tables. It deliberately does **not** drop legacy
`inventory_*` tables: the former Inventory Intelligence data and shared material
master/MSDS helpers need a separately approved retirement after a verified backup.

Before a production migration or any later legacy-table retirement, take a fresh
backup and verify the configured target database is local when working locally.
Never run destructive database maintenance against Railway without explicit
approval.
