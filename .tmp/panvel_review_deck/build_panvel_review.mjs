import fs from "node:fs/promises";
import { Presentation, PresentationFile } from "@oai/artifact-tool";

const OUT = "/Users/dineshreddy/Desktop/ongc_digital_workspace/RGL_Panvel_Performance_Deliberation_August_2026.pptx";
const LOGO = "/var/folders/gg/ltpr3kx14z9_vb1vf8pcq95h0000gn/T/codex-clipboard-9b6ca62a-c35e-4e3c-9f0a-715861144e19.png";
const ink = "#071D42", muted = "#526173", pale = "#F2F4F7", rule = "#D6DCE5";
const blue = "#1976D2", bluePale = "#EAF4FF", red = "#C53B3B", redPale = "#FCEBEC", green = "#18794E", greenPale = "#EAF7F0";
const logoBytes = await fs.readFile(LOGO);
async function saveBlob(path, blob) { await fs.writeFile(path, new Uint8Array(await blob.arrayBuffer())); }
function box(s, left, top, width, height, fill = "none", lineFill = "none") { return s.shapes.add({ geometry: "rect", position: { left, top, width, height }, fill, line: { style: "solid", fill: lineFill, width: lineFill === "none" ? 0 : 1 } }); }
function text(s, value, left, top, width, height, size, color = ink, opts = {}) { const t = s.shapes.add({ geometry: "textbox", position: { left, top, width, height }, fill: "none", line: { style: "solid", fill: "none", width: 0 } }); t.text = value; t.text.style = { fontSize: size, typeface: "Helvetica Neue", color, bold: opts.bold ?? false, alignment: opts.alignment ?? "left", verticalAlignment: opts.verticalAlignment ?? "top", autoFit: "shrinkText", insets: { top: 0, right: 0, bottom: 0, left: 0 } }; return t; }
function chrome(s, page, label) { box(s, 42, 42, 118, 7, blue, blue); text(s, label, 42, 68, 700, 25, 16, blue, { bold: true }); s.images.add({ blob: logoBytes, contentType: "image/png", alt: "ONGC Corporate Chemistry logo", fit: "contain", position: { left: 1155, top: 24, width: 72, height: 72 }, geometry: "rect" }); box(s, 42, 662, 1196, 1, rule, rule); text(s, "Source: RGL Panvel QC Laboratory Monitoring data · August 2026 · Status as at 20 Aug 2026", 42, 675, 980, 20, 11, muted); text(s, String(page).padStart(2,  "0"), 1180, 675, 58, 20, 11, muted, { alignment: "right" }); }
function metric(s, x, y, w, value, label, note, color = ink, fill = pale) { box(s, x, y, w, 178, fill, rule); text(s, value, x + 22, y + 28, w - 44, 51, 38, color, { bold: true }); text(s, label, x + 22, y + 96, w - 44, 29, 18, ink, { bold: true }); if (note) text(s, note, x + 22, y + 135, w - 44, 24, 14, muted); }
function tableHead(s, cols, y) { let x = 42; cols.forEach(([label, width]) => { box(s, x, y, width, 27, ink, ink); text(s, label, x + 5, y + 6, width - 10, 16, 12, "#FFFFFF", { bold: true }); x += width; }); }
function tableRow(s, cols, y, values, highlight = false) { let x = 42; cols.forEach(([_, width], i) => { box(s, x, y, width, 26, highlight ? "#FFF7F7" : "#FFFFFF", rule); text(s, values[i], x + 5, y + 6, width - 10, 15, 12, i === 6 && highlight ? red : ink); x += width; }); }

const deck = Presentation.create({ slideSize: { width: 1280, height: 720 } });

// 1. Neutral metric baseline only.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 1, "RGL PANVEL · AUGUST 2026 PERFORMANCE METRICS");
  text(s, "Current-month performance", 42, 112, 700, 48, 38, ink, { bold: true });
  metric(s, 42, 195, 270, "39", "Monthly intake", "samples received in August", ink, bluePale);
  metric(s, 334, 195, 270, "29", "Closed reports", "reports issued in August");
  metric(s, 626, 195, 270, "14", "Within applicable SLA", "14 of 29 assessed closures", green, greenPale);
  metric(s, 918, 195, 270, "15", "Late closures", "15 of 29 assessed closures", red, redPale);
  metric(s, 42, 407, 354, "48.3%", "Within applicable SLA", "14 / 29 closed reports", blue, bluePale);
  metric(s, 420, 407, 354, "7.4 d", "Average turnaround", "across all 29 closed reports");
  metric(s, 798, 407, 390, "14 + 15", "SLA assessment basis", "material SLA + 9-day review fallback", ink, pale);
}

// 2. Weekly workload and outcome analytics.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 2, "RGL PANVEL · CURRENT WEEK 13–19 AUGUST 2026");
  text(s, "Weekly workload and closure analytics", 42, 112, 900, 48, 38, ink, { bold: true });
  metric(s, 42, 195, 205, "37", "Samples in review", "latest workbook", ink, bluePale);
  metric(s, 265, 195, 205, "19", "Closed reports", "reports issued", ink, pale);
  metric(s, 488, 195, 205, "18", "Open workload", "under testing", ink, pale);
  metric(s, 711, 195, 205, "1", "Aged open sample", "beyond 9 days", red, redPale);
  metric(s, 934, 195, 254, "52.6%", "Weekly SLA achieved", "10 / 19 closed reports", blue, bluePale);
  text(s, "Current-week SLA exception register", 42, 424, 500, 27, 24, ink, { bold: true });
  text(s, "9", 42, 474, 90, 54, 45, red, { bold: true }); text(s, "closures later than the applicable SLA", 146, 485, 348, 28, 20, ink);
  text(s, "5", 42, 552, 55, 31, 24, blue, { bold: true }); text(s, "used the 9-day review fallback", 104, 553, 310, 25, 17, muted);
  text(s, "4", 42, 592, 55, 31, 24, blue, { bold: true }); text(s, "used a material-specific standard", 104, 593, 325, 25, 17, muted);
  box(s, 614, 424, 574, 190, "#FAFBFC", rule);
  text(s, "Weekly sample mix", 640, 447, 300, 27, 24, ink, { bold: true });
  text(s, "10", 640, 495, 52, 26, 22, blue, { bold: true }); text(s, "Superior Grade Sodium Chloride", 704, 497, 345, 24, 17, ink);
  text(s, "6", 640, 535, 52, 26, 22, blue, { bold: true }); text(s, "MDEA", 704, 537, 345, 24, 17, ink);
  text(s, "21", 640, 575, 52, 26, 22, blue, { bold: true }); text(s, "all other materials", 704, 577, 345, 24, 17, ink);
  text(s, "Product outcome mix: 17 pass / 2 fail — shown for quality context only, not laboratory performance.", 42, 631, 1040, 18, 14, muted);
}

// 3. Monthly delay analytics.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 3, "RGL PANVEL · AUGUST 2026 CLOSE-OUT ANALYTICS");
  text(s, "Late closures slightly exceed within-SLA closures", 42, 112, 1000, 48, 38, ink, { bold: true });
  text(s, "29 closed reports assessed against the applicable SLA", 42, 166, 620, 28, 19, muted);
  text(s, "14", 98, 250, 140, 55, 48, green, { bold: true }); text(s, "Within SLA", 98, 309, 160, 25, 19, ink, { bold: true });
  box(s, 318, 255, 302, 46, greenPale, "#B8E1CC"); box(s, 318, 255, 146, 46, green, green);
  text(s, "15", 98, 372, 140, 55, 48, red, { bold: true }); text(s, "Late", 98, 431, 160, 25, 19, ink, { bold: true });
  box(s, 318, 377, 302, 46, redPale, "#F2CACA"); box(s, 318, 377, 156, 46, red, red);
  box(s, 42, 504, 578, 108, pale, rule); text(s, "SLA coverage", 68, 526, 230, 23, 18, ink, { bold: true }); text(s, "14 material-specific standards · 15 closures assessed on the 9-day fallback", 68, 562, 500, 29, 17, muted);
  box(s, 674, 210, 514, 402, "#FAFBFC", rule);
  text(s, "Delay reason distribution · 15 late closures", 704, 237, 430, 28, 24, ink, { bold: true });
  const reasons = [["5", "Results awaited from outside lab / agency"], ["5", "No delay reason recorded"], ["2", "Awaiting chemicals from asset"], ["3", "Other external / inter-laboratory dependencies"]];
  reasons.forEach((r, i) => { const y = 292 + i * 67; text(s, r[0], 704, y, 35, 27, 23, i === 1 ? red : blue, { bold: true }); text(s, r[1], 757, y + 2, 390, 29, 18, ink); if (i < 3) box(s, 704, y + 45, 430, 1, rule, rule); });
}

// 4. Exception concentration and structured review prompts.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 4, "RGL PANVEL · REVIEW FOCUS");
  text(s, "Exception concentration and questions for deliberation", 42, 112, 1080, 48, 38, ink, { bold: true });
  box(s, 42, 201, 548, 411, "#FAFBFC", rule);
  text(s, "Materials with multiple August late closures", 70, 230, 450, 29, 24, ink, { bold: true });
  const materials = [["3", "Soda Ash"], ["2", "Tetrakis Phosphonium sulphate (THPS)"], ["2", "Polyelectrolyte"], ["8", "Other materials with one late closure each"]];
  materials.forEach((m, i) => { const y = 292 + i * 66; text(s, m[0], 70, y, 40, 28, 23, i === 0 ? red : blue, { bold: true }); text(s, m[1], 128, y + 2, 390, 28, 18, ink); if (i < 3) box(s, 70, y + 45, 432, 1, rule, rule); });
  text(s, "Open watch item", 70, 560, 165, 20, 16, red, { bold: true }); text(s, "Keroclay — 13 days open; reason entered as “Others”.", 70, 584, 440, 21, 15, ink);
  box(s, 642, 201, 546, 411, bluePale, "#CBE1F7");
  text(s, "Questions for the review", 672, 230, 420, 29, 24, ink, { bold: true });
  const questions = ["Which outside-lab samples need an agreed result date and escalation point?", "Why were five late closures issued without a recorded delay reason?", "Which asset-side dependencies should be pre-planned before sampling?", "What owner and closure date will be recorded for each open or late exception?"];
  questions.forEach((q, i) => { const y = 290 + i * 70; text(s, `${i + 1}`, 672, y, 25, 24, 18, blue, { bold: true }); text(s, q, 710, y, 432, 47, 18, ink); if (i < 3) box(s, 672, y + 54, 444, 1, "#BFD8EF", "#BFD8EF"); });
}

// 5. Current open samples beyond the management-review threshold.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 5, "RGL PANVEL · CURRENT OPEN SLA EXCEPTIONS");
  text(s, "Current samples above the 9-day review threshold", 42, 112, 1050, 48, 38, ink, { bold: true });
  text(s, "Latest workbook: 13–19 August 2026", 42, 166, 500, 25, 19, muted);
  metric(s, 42, 222, 260, "1", "Open SLA exception", "18 samples currently under testing", red, redPale);
  box(s, 332, 222, 856, 178, "#FAFBFC", rule);
  text(s, "Keroclay", 362, 254, 320, 34, 28, ink, { bold: true });
  text(s, "Notification: 4075027233", 362, 305, 280, 24, 17, muted);
  text(s, "Received: 07 Aug 2026", 650, 305, 230, 24, 17, muted);
  text(s, "Age: 13 days", 915, 305, 190, 24, 17, red, { bold: true });
  text(s, "Reason recorded: Others (Specify in Remarks)", 362, 350, 650, 24, 17, ink);
  text(s, "Open samples are assessed against the 9-day management-review threshold until the report is issued.", 42, 455, 1040, 28, 18, muted);
}

// 6. Completed samples above SLA.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 6, "RGL PANVEL · AUGUST COMPLETED SAMPLES ABOVE SLA");
  text(s, "15 completed samples later than the applicable SLA", 42, 112, 1030, 46, 36, ink, { bold: true });
  text(s, "Material standard used where defined; otherwise the 9-day review SLA", 42, 160, 850, 22, 17, muted);
  const cols = [["Chemical",270],["Notification",120],["Received",88],["Reported",88],["TAT",50],["SLA",50],["Variance",65],["Delay reason",447]];
  tableHead(s, cols, 205);
  const rows = [
    ["Polylactic Acid Granules","4075029064","16 Jul","14 Aug","21d","9d","+12d","Outside lab / agency"],
    ["Polylactic Acid Powder","20000035713","16 Jul","18 Aug","23d","9d","+14d","Outside lab / agency"],
    ["Tetrakis Phosphonium sulphate","20000035822","20 Jul","18 Aug","21d","9d","+12d","Asset chemicals"],
    ["Tetrakis Phosphonium sulphate","20000035823","20 Jul","18 Aug","21d","9d","+12d","Asset chemicals"],
    ["SPAN-80","20000035833","27 Jul","10 Aug","14d","9d","+5d","Cotechna result"],
    ["Intermediate Strength Proppant","20000035925","27 Jul","10 Aug","14d","9d","+5d","Vadodara result"],
    ["Potassium Lignite","20000035928","29 Jul","06 Aug","8d","5d","+3d","Not recorded"],
    ["Low Strength Proppant","20000035929","29 Jul","10 Aug","12d","9d","+3d","Vadodara result"],
    ["Sodium Hydroxide","20000035902","03 Aug","06 Aug","3d","1d","+2d","Not recorded"],
    ["Polyelectrolyte","20000035965","03 Aug","13 Aug","8d","1d","+7d","Outside lab / agency"],
    ["TWEEN-80","20000035968","03 Aug","17 Aug","10d","9d","+1d","Outside lab / agency"],
    ["Polyelectrolyte","20000036024","06 Aug","18 Aug","8d","1d","+7d","Outside lab / agency"],
    ["Soda Ash","20000036055","07 Aug","10 Aug","3d","1d","+2d","Not recorded"],
    ["Soda Ash","20000036009","10 Aug","13 Aug","3d","1d","+2d","Not recorded"],
    ["Soda Ash","20000036012","12 Aug","14 Aug","2d","1d","+1d","Not recorded"],
  ];
  rows.forEach((row, i) => tableRow(s, cols, 232 + i * 26, row, true));
}

// 7. Completed samples within SLA.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 7, "RGL PANVEL · AUGUST COMPLETED SAMPLES WITHIN SLA");
  text(s, "14 completed samples within the applicable SLA", 42, 112, 1000, 46, 36, ink, { bold: true });
  text(s, "Material standard used where defined; otherwise the 9-day review SLA", 42, 160, 850, 22, 17, muted);
  const cols = [["Chemical",350],["Notification",150],["Received",110],["Reported",110],["TAT",65],["SLA",65],["Outcome",158],["SLA basis",188]];
  tableHead(s, cols, 205);
  const rows = [
    ["SPAN-80","20000035966","03 Aug","13 Aug","8d","9d","Pass","9-day fallback"],
    ["Sodium Chlorite","20000035967","03 Aug","06 Aug","3d","9d","Fail","9-day fallback"],
    ["LTD for MH Asset","20000036005","10 Aug","12 Aug","2d","9d","Fail","9-day fallback"],
    ["LTD for MH Asset","20000036006","10 Aug","12 Aug","2d","9d","Fail","9-day fallback"],
    ["Drilling Detergent","20000036007","10 Aug","12 Aug","2d","4d","Pass","Material standard"],
    ["PPD Ankleshwar","20000036008","10 Aug","18 Aug","6d","9d","Pass","9-day fallback"],
    ["Sodium Chloride","20000036038","11 Aug","14 Aug","3d","9d","Pass","9-day fallback"],
    ["THPS","20000036039","12 Aug","19 Aug","5d","9d","Pass","9-day fallback"],
    ["Superior Grade Sodium Chloride","20000036081","13 Aug","17 Aug","2d","2d","Pass","Material standard"],
    ["Superior Grade Sodium Chloride","20000036082","13 Aug","17 Aug","2d","2d","Pass","Material standard"],
    ["Superior Grade Sodium Chloride","20000036083","13 Aug","17 Aug","2d","2d","Pass","Material standard"],
    ["Superior Grade Sodium Chloride","20000036107","13 Aug","17 Aug","2d","2d","Pass","Material standard"],
    ["Superior Grade Sodium Chloride","20000036108","13 Aug","17 Aug","2d","2d","Pass","Material standard"],
    ["Superior Grade Sodium Chloride","20000036106","13 Aug","17 Aug","2d","2d","Pass","Material standard"],
  ];
  rows.forEach((row, i) => tableRow(s, cols, 232 + i * 26, row, false));
}

// 8. Product-quality outcomes, deliberately separated from delivery performance.
{
  const s = deck.slides.add(); s.background.fill = "#FFFFFF"; chrome(s, 8, "RGL PANVEL · COMPLETED SAMPLE PRODUCT OUTCOMES");
  text(s, "Pass / fail results for August completed samples", 42, 112, 1020, 46, 36, ink, { bold: true });
  text(s, "Product-quality outcome only — it is not used to assess laboratory delivery performance.", 42, 160, 900, 22, 17, muted);
  metric(s, 42, 214, 270, "23", "Pass", "79.3% of 29 completed samples", green, greenPale);
  metric(s, 334, 214, 270, "6", "Fail", "20.7% of 29 completed samples", red, redPale);
  metric(s, 626, 214, 270, "11 / 12", "Pass within / above SLA", "product outcome by delivery timing", ink, pale);
  metric(s, 918, 214, 270, "3 / 3", "Fail within / above SLA", "product outcome by delivery timing", ink, pale);
  text(s, "Completed samples with a fail outcome", 42, 448, 570, 28, 24, ink, { bold: true });
  const cols = [["Chemical",330],["Notification",150],["Reported",110],["TAT / SLA",110],["Delivery timing",210],["Delay reason",288]];
  tableHead(s, cols, 493);
  const rows = [
    ["Polylactic Acid Granules","4075029064","14 Aug","21d / 9d","Above SLA","Outside lab / agency"],
    ["Polylactic Acid Powder","20000035713","18 Aug","23d / 9d","Above SLA","Outside lab / agency"],
    ["Potassium Lignite","20000035928","06 Aug","8d / 5d","Above SLA","Not recorded"],
    ["Sodium Chlorite","20000035967","06 Aug","3d / 9d","Within SLA","Not recorded"],
    ["LTD for MH Asset","20000036005","12 Aug","2d / 9d","Within SLA","Not recorded"],
    ["LTD for MH Asset","20000036006","12 Aug","2d / 9d","Within SLA","Not recorded"],
  ];
  rows.forEach((row, i) => tableRow(s, cols, 520 + i * 23, row, false));
}

for (const [i, slide] of deck.slides.items.entries()) { const blob = await deck.export({ slide, format: "png", scale: 1 }); await saveBlob(`/Users/dineshreddy/Desktop/ongc_digital_workspace/.tmp/panvel_review_deck/slide-${i + 1}.png`, blob); }
const pptx = await PresentationFile.exportPptx(deck); await pptx.save(OUT);
