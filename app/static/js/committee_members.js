/**
 * Committee Member Picker — vanilla JS enhancement for <select multiple>.
 *
 * Replaces the raw <select multiple name="member_ids"> inside
 * #committeeMemberPicker with a searchable checkbox list and selected chips.
 *
 * Data attributes on #committeeMemberPicker:
 *   data-api-url   — base URL for fetching users, ending in /0  (office_id placeholder)
 *   data-office-id — initial office id to load (committee_member preset)
 *   data-selected  — comma-separated user IDs that are pre-selected
 *   data-name      — form field name (default "member_ids")
 */
(function () {
  'use strict';

  var container = document.getElementById('committeeMemberPicker');
  if (!container) return;

  var apiUrlBase = (container.dataset.apiUrl || '').replace(/\/0$/, '');
  var fieldName = container.dataset.name || 'member_ids';
  var preselected = (container.dataset.selected || '').split(',').filter(Boolean);
  var officeSelect = document.querySelector('.js-committee-office-select');

  // Remove native select
  var nativeSelect = container.querySelector('select');
  if (nativeSelect) nativeSelect.style.display = 'none';

  // Build UI
  var wrapper = document.createElement('div');
  wrapper.className = 'committee-member-picker';

  var chipsArea = document.createElement('div');
  chipsArea.className = 'committee-member-chips';

  var toolbar = document.createElement('div');
  toolbar.className = 'committee-member-toolbar';

  var searchInput = document.createElement('input');
  searchInput.type = 'text';
  searchInput.placeholder = 'Search members';
  searchInput.className = 'form-control committee-member-search';

  var countBadge = document.createElement('span');
  countBadge.className = 'committee-member-count';

  var checkboxList = document.createElement('div');
  checkboxList.className = 'committee-member-list';

  var helperText = document.createElement('div');
  helperText.className = 'committee-member-helper';

  wrapper.appendChild(chipsArea);
  toolbar.appendChild(searchInput);
  toolbar.appendChild(countBadge);
  wrapper.appendChild(toolbar);
  wrapper.appendChild(helperText);
  wrapper.appendChild(checkboxList);
  container.appendChild(wrapper);

  var selectedIds = new Set(preselected);
  var allUsers = [];

  function renderChips() {
    chipsArea.innerHTML = '';
    // Remove old hidden inputs
    container.querySelectorAll('input[type=hidden][name="' + fieldName + '"]').forEach(function (el) { el.remove(); });

    selectedIds.forEach(function (uid) {
      var user = allUsers.find(function (u) { return String(u.id) === String(uid); });
      if (!user) return;

      var chip = document.createElement('span');
      chip.className = 'committee-member-chip';
      chip.textContent = user.name;

      var removeBtn = document.createElement('button');
      removeBtn.type = 'button';
      removeBtn.textContent = '\u00D7';
      removeBtn.className = 'committee-member-chip-remove';
      removeBtn.setAttribute('aria-label', 'Remove ' + user.name);
      removeBtn.addEventListener('click', function () {
        selectedIds.delete(String(uid));
        renderChips();
        renderList();
      });
      chip.appendChild(removeBtn);
      chipsArea.appendChild(chip);

      // Hidden input for form submission
      var hidden = document.createElement('input');
      hidden.type = 'hidden';
      hidden.name = fieldName;
      hidden.value = uid;
      container.appendChild(hidden);
    });

    countBadge.textContent = selectedIds.size + ' selected';
    chipsArea.classList.toggle('is-empty', selectedIds.size === 0);
    if (!selectedIds.size) {
      chipsArea.innerHTML = '<div class="committee-member-empty">No members selected yet.</div>';
    }
  }

  function renderList(filter) {
    var lowerFilter = (filter || '').toLowerCase();
    checkboxList.innerHTML = '';

    if (!allUsers.length) {
      checkboxList.innerHTML = '<div class="committee-member-empty">No active members found for this office.</div>';
      helperText.textContent = '';
      return;
    }

    var visibleCount = 0;

    allUsers.forEach(function (user) {
      if (lowerFilter && user.name.toLowerCase().indexOf(lowerFilter) === -1 && user.username.toLowerCase().indexOf(lowerFilter) === -1) {
        return;
      }
      visibleCount += 1;
      var label = document.createElement('label');
      label.className = 'committee-member-option';

      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = user.id;
      cb.checked = selectedIds.has(String(user.id));

      cb.addEventListener('change', function () {
        if (cb.checked) {
          selectedIds.add(String(user.id));
        } else {
          selectedIds.delete(String(user.id));
        }
        renderChips();
      });

      label.appendChild(cb);
      var optionText = document.createElement('span');
      optionText.className = 'committee-member-option-text';
      optionText.textContent = user.name + ' (' + user.username + ')';
      label.appendChild(optionText);
      checkboxList.appendChild(label);
    });

    helperText.textContent = visibleCount ? 'Showing ' + visibleCount + ' available member' + (visibleCount === 1 ? '' : 's') + '.' : 'No members match the current search.';
    if (!visibleCount) {
      checkboxList.innerHTML = '<div class="committee-member-empty">No members match the current search.</div>';
    }
  }

  searchInput.addEventListener('input', function () {
    renderList(searchInput.value);
  });

  function loadMembers(officeId) {
    if (!officeId) {
      allUsers = [];
      helperText.textContent = 'Select an office to load members.';
      renderList();
      renderChips();
      return;
    }
    var url = apiUrlBase + '/' + officeId;
    fetch(url, { headers: { 'X-Requested-With': 'XMLHttpRequest' } })
      .then(function (r) { return r.ok ? r.json() : []; })
      .then(function (data) {
        allUsers = data || [];
        renderList(searchInput.value);
        renderChips();
      })
      .catch(function () {
        allUsers = [];
        helperText.textContent = 'Members could not be loaded right now.';
        renderList();
        renderChips();
      });
  }

  // Initial load
  var initialOffice = container.dataset.officeId || (officeSelect ? officeSelect.value : '');
  if (initialOffice) {
    loadMembers(initialOffice);
  }

  // Re-load on office change (admin view)
  if (officeSelect) {
    officeSelect.addEventListener('change', function () {
      selectedIds.clear();
      searchInput.value = '';
      loadMembers(officeSelect.value);
    });
  }
})();
