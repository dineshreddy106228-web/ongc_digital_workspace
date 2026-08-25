(() => {
  const form = document.querySelector('[data-dossier-form]');
  if (!form) return;

  const checks = [...form.querySelectorAll('.cs-dossier-check:not(:disabled)')];
  const counters = [...document.querySelectorAll('[data-dossier-count]')];
  const counterLabels = [...document.querySelectorAll('[data-dossier-count-label]')];
  const selectionNote = document.querySelector('[data-dossier-selection-note]');
  const groupToggles = [...form.querySelectorAll('[data-group-select-all]')];

  const update = () => {
    const selected = checks.filter((check) => check.checked).length;
    counters.forEach((counter) => { counter.textContent = selected; });
    counterLabels.forEach((label) => { label.textContent = `${selected} dossier${selected === 1 ? '' : 's'} selected`; });
    if (selectionNote) selectionNote.textContent = selected ? `${selected} dossier${selected === 1 ? '' : 's'} will be downloaded.` : 'Select one or more available chemicals to continue.';
    groupToggles.forEach((toggle) => {
      const groupChecks = checks.filter((check) => check.dataset.dossierGroupId === toggle.dataset.groupSelectAll);
      const selectedInGroup = groupChecks.filter((check) => check.checked).length;
      toggle.checked = Boolean(groupChecks.length) && selectedInGroup === groupChecks.length;
      toggle.indeterminate = selectedInGroup > 0 && selectedInGroup < groupChecks.length;
    });
  };

  checks.forEach((check) => check.addEventListener('change', update));
  groupToggles.forEach((toggle) => toggle.addEventListener('change', () => {
    checks.filter((check) => check.dataset.dossierGroupId === toggle.dataset.groupSelectAll).forEach((check) => { check.checked = toggle.checked; });
    update();
  }));

  const search = document.querySelector('[data-dossier-search]');
  if (search) search.addEventListener('input', () => {
    const needle = search.value.trim().toLowerCase();
    form.querySelectorAll('[data-dossier-item]').forEach((item) => { item.hidden = Boolean(needle) && !item.dataset.search.includes(needle); });
    form.querySelectorAll('[data-dossier-group]').forEach((group) => {
      const visible = [...group.querySelectorAll('[data-dossier-item]')].some((item) => !item.hidden);
      group.hidden = !visible;
      if (needle && visible) group.open = true;
    });
  });

  document.querySelector('[data-dossier-expand]')?.addEventListener('click', () => form.querySelectorAll('[data-dossier-group]').forEach((group) => { group.open = true; }));
  document.querySelector('[data-dossier-collapse]')?.addEventListener('click', () => form.querySelectorAll('[data-dossier-group]').forEach((group) => { group.open = false; }));
  form.addEventListener('submit', (event) => {
    if (checks.some((check) => check.checked)) return;
    event.preventDefault();
    window.alert('Select at least one available chemical before downloading dossiers.');
  });
  update();
})();
