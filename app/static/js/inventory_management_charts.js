/* Charts for the inventory management review.
 *
 * Every series is a reshape of a table or register that sits on the same page,
 * so a chart is a shortcut into the data rather than the only way to reach it.
 * Each one is paired with real links or its own table underneath, because a
 * canvas takes no keyboard focus and reads as nothing to a screen reader.
 *
 * Charts are built lazily: a canvas inside a hidden tab panel has no layout, so
 * Chart.js would size it to zero. A panel's charts are created the first time
 * that panel is shown. */
(() => {
  const DATA = window.INVENTORY_MANAGEMENT_CHARTS;
  if (!DATA || typeof Chart === "undefined") return;

  const ASSET_URLS = window.INVENTORY_ASSET_URLS || {};
  const charts = new Map();
  const built = new Set();

  const themeValue = (name, fallback) =>
    getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;

  /* Tones come from the module shell's own custom properties, so the charts
     follow the light/dark palette instead of carrying a second one. The band
     colours are the ones the coverage strip and the month chips already use, so
     a band cannot be one colour in the chart and another in the table. */
  const tone = (key) => ({
    ok: themeValue("--tone-ok-fg", "#166534"),
    bad: themeValue("--tone-bad-fg", "#9f1239"),
    hot: themeValue("--tone-hot-fg", "#c2410c"),
    warn: themeValue("--tone-warn-fg", "#92400e"),
    alt: themeValue("--tone-alt-fg", "#7c3aed"),
    accent: themeValue("--mod-accent", "#1f4fa8"),
    info: themeValue("--tone-info-fg", "#1d4ed8"),
    muted: themeValue("--chart-text", "#475569"),
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

  const register = (id, config) => {
    const element = document.getElementById(id);
    if (!element) return;
    const existing = charts.get(id);
    if (existing) existing.destroy();
    charts.set(id, new Chart(element, config));
  };

  /* Money is plotted in crore, so every rupee figure the charts show says so.
     An axis carries the short form — a dozen ticks reading "₹ 100.00 Cr" wrap
     and collide — and the tooltip carries the figure as the tables print it. */
  const crore = (value) => `₹ ${Number(value).toLocaleString("en-IN", {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  })} Cr`;
  const croreTick = (value) => `${Number(value).toLocaleString("en-IN", {
    maximumFractionDigits: 1,
  })} Cr`;

  /* Clicking an asset bar opens that asset's status page. Labels are asset
     names, which is how the template keys the URL map. */
  const openAsset = (labels) => (event, elements) => {
    if (!elements.length) return;
    const url = ASSET_URLS[labels[elements[0].index]];
    if (url) window.location.href = url;
  };
  const pointerOnHover = (labels) => (event, elements) => {
    const hit = elements.length && ASSET_URLS[labels[elements[0].index]];
    event.native.target.style.cursor = hit ? "pointer" : "default";
  };

  const barOptions = (theme, { stacked = false, money = false, clickable = false, labels = [] } = {}) => ({
    ...theme.options,
    indexAxis: "y",
    onClick: clickable ? openAsset(labels) : undefined,
    onHover: clickable ? pointerOnHover(labels) : undefined,
    scales: {
      x: {
        stacked,
        beginAtZero: true,
        ticks: {
          color: theme.text,
          font: { size: 11 },
          callback: money ? croreTick : undefined,
        },
        grid: { color: theme.grid },
      },
      y: { stacked, ticks: { color: theme.text, font: { size: 11 } }, grid: { display: false } },
    },
  });

  /* ── Position ─────────────────────────────────────────────── */
  let mixSegment = "value";

  const drawMix = () => {
    const theme = base();
    const series = DATA.position.mix;
    if (!series.labels.length) return;
    const money = mixSegment === "value";
    register("imMix", {
      type: "doughnut",
      data: {
        labels: series.labels,
        datasets: [{
          data: series[mixSegment],
          backgroundColor: series.tones.map(tone),
          borderColor: theme.border,
          borderWidth: 2,
        }],
      },
      options: {
        ...theme.options,
        cutout: "58%",
        plugins: {
          ...theme.options.plugins,
          tooltip: {
            callbacks: {
              // The share is of value either way, so it travels with the value
              // reading only — a band's share of the lines is not its share of
              // the money, and one number must not be read as the other.
              label: (context) => {
                const index = context.dataIndex;
                return money
                  ? `${crore(series.value[index])} · ${series.share[index]}% of value`
                  : `${series.count[index]} stock lines`;
              },
            },
          },
        },
      },
    });
  };

  const drawZones = () => {
    const theme = base();
    const series = DATA.position.zones;
    if (!series.labels.length) return;
    const datasets = [];
    if (series.has_previous) {
      datasets.push({
        label: "Comparison period",
        data: series.previous,
        backgroundColor: tone("muted"),
        borderRadius: 4,
      });
    }
    datasets.push({
      label: "This period",
      data: series.value,
      backgroundColor: tone("accent"),
      borderRadius: 4,
    });
    register("imZones", {
      type: "bar",
      data: { labels: series.labels, datasets },
      options: {
        ...barOptions(theme, { money: true }),
        plugins: {
          ...theme.options.plugins,
          legend: { ...theme.options.plugins.legend, display: series.has_previous },
          tooltip: { callbacks: { label: (context) => `${context.dataset.label}: ${crore(context.parsed.x)}` } },
        },
      },
    });
  };

  const drawCentres = () => {
    const theme = base();
    const series = DATA.position.centres;
    if (!series.labels.length) return;
    register("imCentres", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [{
          label: "Inventory held",
          data: series.value,
          backgroundColor: tone("accent"),
          borderRadius: 4,
          borderSkipped: false,
        }],
      },
      options: {
        ...barOptions(theme, { money: true, clickable: true, labels: series.labels }),
        plugins: {
          ...theme.options.plugins,
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => {
                const index = context.dataIndex;
                const exceptions = series.exceptions[index];
                return `${crore(context.parsed.x)} · ${series.share[index]}% of portfolio`
                  + (exceptions ? ` · ${exceptions} open exception${exceptions === 1 ? "" : "s"}` : "");
              },
            },
          },
        },
      },
    });
  };

  const drawExceptions = () => {
    const theme = base();
    const series = DATA.position.exceptions;
    if (!series.values.some((value) => value > 0)) return;
    register("imExceptions", {
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

  /* ── Movement ─────────────────────────────────────────────── */
  const drawMovers = () => {
    const theme = base();
    const series = DATA.movement.movers;
    if (!series.labels.length) return;
    register("imMovers", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [{
          label: "Change in held value",
          data: series.delta,
          backgroundColor: series.delta.map((value) => tone(value > 0 ? "hot" : "accent")),
          borderRadius: 4,
          borderSkipped: false,
        }],
      },
      options: {
        ...barOptions(theme, { money: true }),
        scales: {
          // A signed axis must cross at zero, not at the smallest fall.
          x: {
            beginAtZero: true,
            ticks: { color: theme.text, font: { size: 11 }, callback: croreTick },
            grid: { color: theme.grid },
          },
          y: { ticks: { color: theme.text, font: { size: 11 } }, grid: { display: false } },
        },
        plugins: {
          ...theme.options.plugins,
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (context) => {
                const index = context.dataIndex;
                return `${crore(series.previous[index])} → ${crore(series.value[index])}`;
              },
            },
          },
        },
      },
    });
  };

  const drawConsumption = () => {
    const theme = base();
    const series = DATA.movement.consumption;
    if (!series.labels.length) return;
    register("imConsumption", {
      type: "bar",
      data: {
        labels: series.labels,
        datasets: [
          // Never stacked: one is a year of flow, the other is a position on a
          // date, and their sum means nothing.
          { label: "Consumed over twelve months", data: series.consumption, backgroundColor: tone("accent"), borderRadius: 4 },
          { label: "Inventory held", data: series.inventory, backgroundColor: tone("warn"), borderRadius: 4 },
        ],
      },
      options: {
        ...barOptions(theme, { money: true }),
        plugins: {
          ...theme.options.plugins,
          tooltip: {
            callbacks: {
              title: (items) => series.descriptions[items[0].dataIndex] || items[0].label,
              label: (context) => `${context.dataset.label}: ${crore(context.parsed.x)}`,
              afterBody: (items) => {
                const months = series.months[items[0].dataIndex];
                return months === null ? "" : `${months.toFixed(1)} months of cover`;
              },
            },
          },
        },
      },
    });
  };

  /* ── Health register ──────────────────────────────────────── */
  const stack = (id, series, labels) => {
    const theme = base();
    if (!labels.length) return;
    register(id, {
      type: "bar",
      data: {
        labels,
        datasets: series.map((set) => ({
          label: set.label,
          data: set.data,
          backgroundColor: tone(set.tone),
          borderRadius: 4,
          borderSkipped: false,
        })),
      },
      options: barOptions(theme, { stacked: true }),
    });
  };

  const drawExposure = () => stack("imExposure", DATA.register.exposure.series, DATA.register.exposure.labels);
  const drawSourceRegisters = () =>
    stack("imSourceRegisters", DATA.register.source_registers.series, DATA.register.source_registers.labels);

  const PANELS = {
    position: [drawMix, drawZones, drawCentres, drawExceptions],
    movement: [drawMovers, drawConsumption],
    register: [drawExposure, drawSourceRegisters],
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

  const show = (name, { focus = false, hash = true } = {}) => {
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
    if (hash && window.history.replaceState) {
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
      mixSegment = button.dataset.mrSegment;
      document.querySelectorAll("[data-mr-segment]").forEach((other) => {
        other.classList.toggle("is-active", other === button);
      });
      drawMix();
    });
  });

  /* A link from one tab into another opens the panel first, then jumps. */
  document.querySelectorAll("[data-mr-goto]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      show(link.dataset.mrGoto);
      window.scrollTo({ top: 0, behavior: "smooth" });
    });
  });

  /* A theme swap changes every colour the charts read, so they are rebuilt —
     but only for panels that have been opened. */
  document.addEventListener("themechange", () => {
    built.forEach((name) => drawPanel(name));
  });

  /* A hash is either a panel name or an anchor inside one — the register's own
     band anchors are linked to from elsewhere on the page, and an anchor in a
     hidden panel would otherwise scroll to nothing. */
  const requested = window.location.hash.replace("#", "");
  if (panels.has(requested)) {
    show(requested);
  } else {
    const target = requested && document.getElementById(requested);
    const owner = target && target.closest("[data-mr-panel]");
    show(owner ? owner.dataset.mrPanel : "position", { hash: false });
    if (owner) target.scrollIntoView();
  }
})();
