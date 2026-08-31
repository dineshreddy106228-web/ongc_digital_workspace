/* Charts for the QC management review.
 *
 * Every series is a reshape of a table that sits on the same page, so a chart
 * is a shortcut into the data rather than the only way to reach it.  Each one
 * is paired with real links or its own table underneath, because a canvas
 * takes no keyboard focus and reads as nothing to a screen reader.
 *
 * Charts are built lazily: a canvas inside a hidden tab panel has no layout,
 * so Chart.js would size it to zero.  A panel's charts are created the first
 * time that panel is shown. */
(() => {
  const DATA = window.QC_MANAGEMENT_CHARTS;
  if (!DATA || typeof Chart === "undefined") return;

  const LAB_URLS = window.QC_LAB_DASHBOARD_URLS || {};
  const charts = new Map();
  const built = new Set();

  const themeValue = (name, fallback) =>
    getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;

  /* Tones come from the module shell's own custom properties, so the charts
     follow the light/dark palette instead of carrying a second one. */
  const tone = (key) => ({
    ok: themeValue("--tone-ok-fg", "#166534"),
    bad: themeValue("--tone-bad-fg", "#9f1239"),
    warn: themeValue("--tone-warn-fg", "#92400e"),
    accent: themeValue("--mod-accent", "#1f4fa8"),
    info: themeValue("--chart-text", "#475569"),
    muted: themeValue("--chart-grid", "#e2e8f0"),
  })[key] || themeValue("--mod-accent", "#1f4fa8");

  const base = () => {
    const text = themeValue("--chart-text", "#475569");
    const grid = themeValue("--chart-grid", "#e2e8f0");
    return {
      text,
      grid,
      border: themeValue("--chart-border", "#ffffff"),
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "bottom",
            labels: { boxWidth: 12, padding: 12, font: { size: 11 }, color: text },
          },
        },
      },
    };
  };

  const canvas = (id) => document.getElementById(id);

  const register = (id, config) => {
    const element = canvas(id);
    if (!element) return;
    const existing = charts.get(id);
    if (existing) existing.destroy();
    charts.set(id, new Chart(element, config));
  };

  /* Clicking a laboratory bar opens that laboratory's SAP dashboard. Labels are
     laboratory names, which is how the template keys the URL map. */
  const openLaboratory = (labels) => (event, elements) => {
    if (!elements.length) return;
    const url = LAB_URLS[labels[elements[0].index]];
    if (url) window.location.href = url;
  };
  const pointerOnHover = (labels) => (event, elements) => {
    const hit = elements.length && LAB_URLS[labels[elements[0].index]];
    event.native.target.style.cursor = hit ? "pointer" : "default";
  };

  const stackedBarOptions = (theme, labels, { clickable = false, percent = false } = {}) => ({
    ...theme.options,
    indexAxis: "y",
    onClick: clickable ? openLaboratory(labels) : undefined,
    onHover: clickable ? pointerOnHover(labels) : undefined,
    scales: {
      x: {
        stacked: true,
        beginAtZero: true,
        max: percent ? 100 : undefined,
        ticks: {
          color: theme.text,
          font: { size: 11 },
          callback: percent ? ((value) => `${value}%`) : undefined,
        },
        grid: { color: theme.grid },
      },
      y: { stacked: true, ticks: { color: theme.text, font: { size: 11 } }, grid: { display: false } },
    },
  });

  /* ── Position ─────────────────────────────────────────────── */
  let workloadSegment = "response";

  const drawWorkload = () => {
    const theme = base();
    const series = DATA.position.workload;
    if (!series.labels.length) return;
    register("mrWorkload", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: series[workloadSegment].map((set) => ({
          label: set.label,
          data: set.data,
          backgroundColor: tone(set.tone),
          borderRadius: 4,
          borderSkipped: false,
        })),
      },
      options: stackedBarOptions(theme, series.labels, { clickable: true }),
    });
  };

  const drawUsage = () => {
    const theme = base();
    const series = DATA.position.usage_decisions;
    if (!series.values.some((value) => value > 0)) return;
    register("mrUsage", {
      type: "doughnut",
      data: {
        labels: series.labels,
        datasets: [{
          data: series.values,
          backgroundColor: series.tones.map(tone),
          borderColor: theme.border,
          borderWidth: 2,
        }],
      },
      options: { ...theme.options, cutout: "58%" },
    });
  };

  const drawMovement = () => {
    const theme = base();
    const series = DATA.position.movement;
    if (!series.labels.length) return;
    register("mrMovement", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [
          { label: "Previous SAP-open", data: series.previous, backgroundColor: tone("muted"), borderRadius: 4 },
          { label: "Current SAP-open", data: series.current, backgroundColor: tone("accent"), borderRadius: 4 },
        ],
      },
      options: {
        ...theme.options,
        onClick: openLaboratory(series.labels),
        onHover: pointerOnHover(series.labels),
        scales: {
          x: { ticks: { color: theme.text, font: { size: 11 } }, grid: { display: false } },
          y: { beginAtZero: true, ticks: { color: theme.text, precision: 0 }, grid: { color: theme.grid } },
        },
      },
    });
  };

  /* ── Performance ──────────────────────────────────────────── */
  const drawLabStt = () => {
    const theme = base();
    const series = DATA.performance.laboratories;
    if (!series.labels.length) return;
    register("mrLabStt", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [{
          label: "Completed within STT",
          // The same bands the table uses, so the chart and the table cannot
          // disagree about which laboratory is in trouble.
          data: series.values,
          backgroundColor: series.values.map(
            (value) => tone(value >= 80 ? "ok" : value < 50 ? "bad" : "warn"),
          ),
          borderRadius: 4,
          borderSkipped: false,
        }],
      },
      options: {
        ...stackedBarOptions(theme, series.labels, { clickable: true, percent: true }),
        plugins: {
          ...theme.options.plugins,
          legend: { display: false },
          tooltip: {
            callbacks: {
              // A rate means little without the population it was taken over.
              label: (context) => {
                const index = context.dataIndex;
                const median = series.median_turnaround[index];
                return `${context.parsed.x}% of ${series.measured[index]} measured`
                  + (median === null ? "" : ` · median ${median} d`);
              },
            },
          },
        },
      },
    });
  };

  const drawMaterialLoad = () => {
    const theme = base();
    const series = DATA.performance.materials_by_load;
    if (!series.labels.length) return;
    register("mrMaterialLoad", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [
          { label: "Completed", data: series.completed, backgroundColor: tone("accent"), borderRadius: 4, borderSkipped: false },
          { label: "Open", data: series.open, backgroundColor: tone("warn"), borderRadius: 4, borderSkipped: false },
        ],
      },
      options: stackedBarOptions(theme, series.labels),
    });
  };

  const drawMaterialFailure = () => {
    const theme = base();
    const series = DATA.performance.materials_by_failure;
    if (!series.labels.length) return;
    register("mrMaterialFailure", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [{
          label: "Rejection rate",
          data: series.values,
          backgroundColor: series.values.map((value) => tone(value >= 20 ? "bad" : "warn")),
          borderRadius: 4,
          borderSkipped: false,
        }],
      },
      options: {
        ...stackedBarOptions(theme, series.labels, { percent: true }),
        plugins: {
          ...theme.options.plugins,
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => {
                const index = context.dataIndex;
                return `${context.parsed.x}% · ${series.rejected[index]} of ${series.decided[index]} decided`;
              },
            },
          },
        },
      },
    });
  };

  const drawSubgroups = () => {
    const theme = base();
    const series = DATA.performance.subgroups;
    if (!series.labels.length) return;
    const palette = ["accent", "ok", "warn", "bad", "info", "muted"];
    register("mrSubgroups", {
      type: "doughnut",
      data: {
        labels: series.labels,
        datasets: [{
          data: series.values,
          backgroundColor: series.labels.map((_, index) => tone(palette[index % palette.length])),
          borderColor: theme.border,
          borderWidth: 2,
        }],
      },
      options: { ...theme.options, cutout: "58%" },
    });
  };

  /* ── Non-SAP register ─────────────────────────────────────── */
  const drawNonSapLabs = () => {
    const theme = base();
    const series = DATA.non_sap.by_laboratory;
    if (!series.labels.length) return;
    register("mrNonSapLabs", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [
          { label: "Pending", data: series.pending, backgroundColor: tone("warn"), borderRadius: 4, borderSkipped: false },
          { label: "Closed — pass", data: series.closed_pass, backgroundColor: tone("ok"), borderRadius: 4, borderSkipped: false },
          { label: "Closed — fail", data: series.closed_fail, backgroundColor: tone("bad"), borderRadius: 4, borderSkipped: false },
        ],
      },
      options: {
        ...stackedBarOptions(theme, series.labels),
        plugins: {
          ...theme.options.plugins,
          tooltip: {
            callbacks: {
              // Overdue is a subset of pending, so it is reported here rather
              // than stacked as a fourth segment that would double-count.
              afterBody: (items) => {
                const overdue = series.overdue[items[0].dataIndex];
                return overdue ? `${overdue} past the declared completion date` : "";
              },
            },
          },
        },
      },
    });
  };

  const drawNonSapStatus = () => {
    const theme = base();
    const series = DATA.non_sap.by_status;
    if (!series.labels.length) return;
    register("mrNonSapStatus", {
      type: "doughnut",
      data: {
        labels: series.labels,
        datasets: [{
          data: series.values,
          backgroundColor: series.tones.map(tone),
          borderColor: theme.border,
          borderWidth: 2,
        }],
      },
      options: { ...theme.options, cutout: "58%" },
    });
  };

  const PANELS = {
    position: [drawWorkload, drawUsage, drawMovement],
    performance: [drawLabStt, drawMaterialLoad, drawMaterialFailure, drawSubgroups],
    "non-sap": [drawNonSapLabs, drawNonSapStatus],
  };

  const drawPanel = (name) => {
    (PANELS[name] || []).forEach((draw) => draw());
    built.add(name);
  };

  /* ── Tabs ─────────────────────────────────────────────────── */
  const tabs = [...document.querySelectorAll("[data-mr-tab]")];
  const panels = new Map(
    [...document.querySelectorAll("[data-mr-panel]")].map((panel) => [panel.dataset.mrPanel, panel]),
  );

  const show = (name, { focus = false } = {}) => {
    if (!panels.has(name)) return;
    tabs.forEach((tab) => {
      const active = tab.dataset.mrTab === name;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
      tab.tabIndex = active ? 0 : -1;
      if (active && focus) tab.focus();
    });
    panels.forEach((panel, key) => { panel.hidden = key !== name; });
    if (!built.has(name)) drawPanel(name);
    // Keep the address bar honest so a tab can be linked to and reloaded.
    if (window.history.replaceState) {
      window.history.replaceState(null, "", `#${name}`);
    }
  };

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => show(tab.dataset.mrTab));
    tab.addEventListener("keydown", (event) => {
      const step = event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
      if (!step) return;
      event.preventDefault();
      const next = (tabs.indexOf(tab) + step + tabs.length) % tabs.length;
      show(tabs[next].dataset.mrTab, { focus: true });
    });
  });

  document.querySelectorAll("[data-mr-segment]").forEach((button) => {
    button.addEventListener("click", () => {
      workloadSegment = button.dataset.mrSegment;
      document.querySelectorAll("[data-mr-segment]").forEach((other) => {
        other.classList.toggle("is-active", other === button);
      });
      drawWorkload();
    });
  });

  /* A theme swap changes every colour the charts read, so they are rebuilt —
     but only for panels that have been opened. */
  document.addEventListener("themechange", () => {
    built.forEach((name) => drawPanel(name));
  });

  const requested = window.location.hash.replace("#", "");
  show(panels.has(requested) ? requested : "position");
})();
