(() => {
  const normalize = (value) => (value || "").replace(/\s+/g, " ").trim().toLocaleLowerCase();

  const makeFilter = (table, index) => {
    if (table.dataset.inventoryFilterReady === "true") return;
    const body = table.tBodies[0];
    if (!body || !table.tHead || !table.tHead.rows.length) return;

    table.dataset.inventoryFilterReady = "true";
    const headers = Array.from(table.tHead.rows[0].cells);
    const workCentreColumn = table.dataset.workCentreFilter === "false" ? -1 : headers.findIndex((header) => {
      const name = normalize(header.textContent);
      return name.includes("work centre") || name.includes("workcenter");
    });
    const workCentresByRow = new Map();
    const workCentres = new Set();
    if (workCentreColumn >= 0) {
      Array.from(body.rows).forEach((row) => {
        if (row.classList.contains("im-spec-row") || row.classList.contains("im-spec-more") || row.querySelector("td[colspan]")) return;
        const cell = row.cells[workCentreColumn];
        if (!cell) return;
        const chips = Array.from(cell.querySelectorAll(".im-mapping-chips span:not(.is-more)"));
        const values = (chips.length ? chips.map((chip) => chip.textContent) : [cell.textContent])
          .map((value) => (value || "").replace(/\s+/g, " ").trim())
          .filter((value) => value && value !== "—");
        if (!values.length) return;
        workCentresByRow.set(row, values);
        values.forEach((value) => workCentres.add(value));
      });
    }
    const tableWrap = table.closest(".im-table-wrap");
    const host = tableWrap ? tableWrap.parentElement : table.parentElement;
    const control = document.createElement("div");
    control.className = "im-table-filter";
    const categoryLabel = document.createElement("label");
    categoryLabel.htmlFor = `inventory-category-filter-${index}`;
    categoryLabel.innerHTML = '<i class="bi bi-tags"></i><span>Category</span>';
    const category = document.createElement("select");
    category.id = categoryLabel.htmlFor;
    category.className = "form-select form-select-sm";
    const allCategories = document.createElement("option");
    allCategories.value = "";
    allCategories.textContent = "All categories";
    category.append(allCategories);
    const categories = new Map();
    Array.from(body.querySelectorAll(".im-spec-row")).forEach((row) => {
      const key = row.dataset.inventoryCategory || "";
      const name = row.dataset.inventoryCategoryLabel || "";
      if (key && name) categories.set(key, name);
    });
    categories.forEach((name, key) => {
      const option = document.createElement("option");
      option.value = key;
      option.textContent = key === "unspecified" ? name : `${key} — ${name}`;
      category.append(option);
    });
    const workCentreLabel = document.createElement("label");
    workCentreLabel.htmlFor = `inventory-work-centre-filter-${index}`;
    workCentreLabel.innerHTML = '<i class="bi bi-building"></i><span>Work centre</span>';
    const workCentre = document.createElement("select");
    workCentre.id = workCentreLabel.htmlFor;
    workCentre.className = "form-select form-select-sm";
    const allWorkCentres = document.createElement("option");
    allWorkCentres.value = "";
    allWorkCentres.textContent = "All work centres";
    workCentre.append(allWorkCentres);
    Array.from(workCentres).sort((left, right) => left.localeCompare(right)).forEach((name) => {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = name;
      workCentre.append(option);
    });
    const searchLabel = document.createElement("label");
    searchLabel.htmlFor = `inventory-table-filter-${index}`;
    searchLabel.innerHTML = '<i class="bi bi-funnel"></i><span>Filter this table</span>';
    const input = document.createElement("input");
    input.id = searchLabel.htmlFor;
    input.type = "search";
    input.className = "form-control form-control-sm";
    input.placeholder = "Search any column";
    input.autocomplete = "off";
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "btn btn-sm btn-secondary";
    clear.innerHTML = '<i class="bi bi-x-lg"></i><span class="sr-only">Clear table filter</span>';
    clear.hidden = true;
    if (categories.size) control.append(categoryLabel, category);
    if (workCentres.size) control.append(workCentreLabel, workCentre);
    control.append(searchLabel, input, clear);
    host.insertBefore(control, tableWrap || table);

    const colspan = Math.max(1, headers.length);
    const noMatch = document.createElement("tr");
    noMatch.className = "im-filter-empty-row";
    noMatch.hidden = true;
    const noMatchCell = document.createElement("td");
    noMatchCell.colSpan = colspan;
    noMatchCell.textContent = "No rows match this filter.";
    noMatch.append(noMatchCell);
    body.append(noMatch);

    const apply = () => {
      const query = normalize(input.value);
      const selectedCategory = category.value;
      const selectedWorkCentre = workCentre.value;
      const hasActiveFilter = Boolean(query || selectedCategory || selectedWorkCentre);
      const rows = Array.from(body.rows).filter((row) => row !== noMatch);
      let matches = 0;
      let section = null;
      const sections = [];

      rows.forEach((row) => {
        if (row.classList.contains("im-spec-row")) {
          section = { heading: row, category: row.dataset.inventoryCategory || "", rows: [], more: [] };
          sections.push(section);
          return;
        }
        if (row.classList.contains("im-spec-more")) {
          if (section) section.more.push(row);
          else row.hidden = hasActiveFilter;
          return;
        }
        if (row.querySelector("td[colspan]")) {
          row.hidden = hasActiveFilter;
          return;
        }
        const visible = (!query || normalize(row.textContent).includes(query))
          && (!selectedCategory || (section && section.category === selectedCategory))
          && (!selectedWorkCentre || (workCentresByRow.get(row) || []).includes(selectedWorkCentre));
        row.hidden = !visible;
        if (visible) matches += 1;
        if (section) section.rows.push({ row, visible });
      });

      sections.forEach(({ heading, rows: sectionRows, more }) => {
        const visible = !hasActiveFilter || sectionRows.some((item) => item.visible);
        heading.hidden = !visible;
        more.forEach((row) => { row.hidden = hasActiveFilter || !visible; });
      });
      noMatch.hidden = !hasActiveFilter || matches > 0;
      clear.hidden = !hasActiveFilter;
    };

    input.addEventListener("input", apply);
    if (categories.size) category.addEventListener("change", apply);
    if (workCentres.size) workCentre.addEventListener("change", apply);
    clear.addEventListener("click", () => {
      input.value = "";
      category.value = "";
      workCentre.value = "";
      apply();
      input.focus();
    });
  };

  const setup = () => {
    document.querySelectorAll(".im-page table").forEach(makeFilter);
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", setup);
  else setup();
})();
