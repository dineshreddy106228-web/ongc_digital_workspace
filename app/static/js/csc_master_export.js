(() => {
  const form = document.querySelector('[data-master-export-form]');
  if (!form) return;

  const scopes = [...form.querySelectorAll('[data-export-scope]')];
  const categoryPanel = form.querySelector('[data-category-scope]');
  const selectedPanel = form.querySelector('[data-selected-scope]');
  const checks = [...form.querySelectorAll('.cs-dossier-check')];
  const groupToggles = [...form.querySelectorAll('[data-export-select-all]')];
  const count = form.querySelector('[data-export-count]');
  const countLabel = form.querySelector('[data-export-count-label]');

  const selectedMode = () => form.querySelector('[data-export-scope][value="selected"]')?.checked;
  const update = () => {
    const total = checks.filter((check) => check.checked).length;
    if (count) count.textContent = total;
    if (countLabel) countLabel.textContent = `specification${total === 1 ? '' : 's'} selected`;
    groupToggles.forEach((toggle) => {
      const groupChecks = checks.filter((check) => check.dataset.exportGroupId === toggle.dataset.exportSelectAll);
      const groupSelected = groupChecks.filter((check) => check.checked).length;
      toggle.checked = Boolean(groupChecks.length) && groupSelected === groupChecks.length;
      toggle.indeterminate = groupSelected > 0 && groupSelected < groupChecks.length;
    });
  };
  const updateScope = () => {
    const choosing = selectedMode();
    categoryPanel.hidden = choosing;
    selectedPanel.hidden = !choosing;
  };

  scopes.forEach((scope) => scope.addEventListener('change', updateScope));
  checks.forEach((check) => check.addEventListener('change', update));
  groupToggles.forEach((toggle) => toggle.addEventListener('change', () => {
    checks.filter((check) => check.dataset.exportGroupId === toggle.dataset.exportSelectAll)
      .forEach((check) => { check.checked = toggle.checked; });
    update();
  }));

  form.querySelector('[data-export-search]')?.addEventListener('input', (event) => {
    const needle = event.target.value.trim().toLowerCase();
    form.querySelectorAll('[data-export-item]').forEach((item) => {
      item.hidden = Boolean(needle) && !item.dataset.search.includes(needle);
    });
    form.querySelectorAll('[data-export-group]').forEach((group) => {
      const visible = [...group.querySelectorAll('[data-export-item]')].some((item) => !item.hidden);
      group.hidden = !visible;
      if (needle && visible) group.open = true;
    });
  });
  form.querySelector('[data-export-expand]')?.addEventListener('click', () => form.querySelectorAll('[data-export-group]').forEach((group) => { group.open = true; }));
  form.querySelector('[data-export-collapse]')?.addEventListener('click', () => form.querySelectorAll('[data-export-group]').forEach((group) => { group.open = false; }));
  form.addEventListener('submit', (event) => {
    if (!selectedMode() || checks.some((check) => check.checked)) return;
    event.preventDefault();
    window.alert('Select at least one specification for the Word document.');
  });

  updateScope();
  update();
})();
