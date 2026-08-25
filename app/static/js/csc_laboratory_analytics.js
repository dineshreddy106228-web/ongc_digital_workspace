(() => {
  const panel = document.querySelector("[data-lab-assignment-analytics]");
  if (!panel) return;

  const search = panel.querySelector("[data-assignment-search]");
  const gapsOnly = panel.querySelector("[data-assignment-gaps]");
  const count = panel.querySelector("[data-assignment-count]");
  const laboratories = [...panel.querySelectorAll("[data-assignment-lab]")];
  const chemicals = [...panel.querySelectorAll("[data-assignment-chemical]")];

  const normalise = (value) => (value || "").toLocaleLowerCase();
  const update = () => {
    const query = normalise(search.value.trim());
    let shownLabs = 0;
    let shownChemicals = 0;

    laboratories.forEach((item) => {
      const visible = !query || normalise(item.textContent).includes(query);
      item.hidden = !visible;
      if (visible) shownLabs += 1;
    });
    chemicals.forEach((item) => {
      const matchesSearch = !query || normalise(item.textContent).includes(query);
      const matchesGap = !gapsOnly.checked || item.dataset.assignmentUnassigned === "true";
      const visible = matchesSearch && matchesGap;
      item.hidden = !visible;
      if (visible) shownChemicals += 1;
    });
    count.textContent = `${shownLabs} laboratories · ${shownChemicals} chemicals shown`;
  };

  search.addEventListener("input", update);
  gapsOnly.addEventListener("change", update);
  update();
})();
