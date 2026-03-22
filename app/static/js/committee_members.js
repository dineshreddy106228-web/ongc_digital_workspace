/**
 * Committee user pickers for create/edit forms.
 *
 * Supports:
 * - single-select picker for committee head
 * - multi-select picker for assignees
 *
 * Expected container attributes:
 * - data-committee-user-picker
 * - data-api-url
 * - data-name
 * - data-mode="single" | "multiple"
 * - data-selected="1,2,3"
 */
(function () {
  'use strict';

  var pickerNodes = Array.prototype.slice.call(
    document.querySelectorAll('[data-committee-user-picker]')
  );
  if (!pickerNodes.length) return;

  var requestCache = {};

  function fetchUsers(apiUrl) {
    if (!requestCache[apiUrl]) {
      requestCache[apiUrl] = fetch(apiUrl, {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      }).then(function (response) {
        if (!response.ok) {
          throw new Error('Failed to load users');
        }
        return response.json();
      });
    }
    return requestCache[apiUrl];
  }

  function normalizeUsers(users) {
    return (users || [])
      .map(function (user) {
        return {
          id: String(user.id),
          name: user.name || user.username || 'Unknown user',
          username: user.username || '',
          officeId: user.office_id == null ? '' : String(user.office_id),
          officeName: user.office_name || 'Unassigned Office',
        };
      })
      .sort(function (left, right) {
        var officeCompare = left.officeName.localeCompare(right.officeName);
        if (officeCompare !== 0) return officeCompare;
        return left.name.localeCompare(right.name);
      });
  }

  function initPicker(container, index) {
    var apiUrl = container.dataset.apiUrl || '';
    if (!apiUrl) return;

    var fieldName = container.dataset.name || 'member_ids';
    var mode = (container.dataset.mode || 'multiple').toLowerCase() === 'single' ? 'single' : 'multiple';
    var preselected = (container.dataset.selected || '')
      .split(',')
      .map(function (value) { return value.trim(); })
      .filter(Boolean);
    var selectedIds = new Set(mode === 'single' ? preselected.slice(0, 1) : preselected);
    var nativeSelect = container.querySelector('select');

    if (nativeSelect) {
      nativeSelect.style.display = 'none';
    }

    var wrapper = document.createElement('div');
    wrapper.className = 'committee-member-picker';

    var chipsArea = document.createElement('div');
    chipsArea.className = 'committee-member-chips';

    var toolbar = document.createElement('div');
    toolbar.className = 'committee-member-toolbar';

    var searchInput = document.createElement('input');
    searchInput.type = 'text';
    searchInput.placeholder = mode === 'single' ? 'Search committee head' : 'Search members';
    searchInput.className = 'form-control committee-member-search';

    var officeFilter = document.createElement('select');
    officeFilter.className = 'form-control committee-member-office-filter';
    officeFilter.innerHTML = '<option value="">All offices</option>';

    var countBadge = document.createElement('span');
    countBadge.className = 'committee-member-count';

    var helperText = document.createElement('div');
    helperText.className = 'committee-member-helper';

    var optionList = document.createElement('div');
    optionList.className = 'committee-member-list';

    toolbar.appendChild(searchInput);
    toolbar.appendChild(officeFilter);
    toolbar.appendChild(countBadge);
    wrapper.appendChild(chipsArea);
    wrapper.appendChild(toolbar);
    wrapper.appendChild(helperText);
    wrapper.appendChild(optionList);
    container.appendChild(wrapper);

    var allUsers = [];
    var radioGroupName = 'committee_picker_visual_' + index;

    function syncHiddenInputs() {
      Array.prototype.slice.call(
        container.querySelectorAll('input[type="hidden"][name="' + fieldName + '"]')
      ).forEach(function (node) {
        node.remove();
      });

      Array.from(selectedIds).forEach(function (userId) {
        var hidden = document.createElement('input');
        hidden.type = 'hidden';
        hidden.name = fieldName;
        hidden.value = userId;
        container.appendChild(hidden);
      });
    }

    function getSelectedUsers() {
      return allUsers.filter(function (user) {
        return selectedIds.has(user.id);
      });
    }

    function updateOfficeFilterOptions() {
      var currentValue = officeFilter.value || '';
      var offices = [];
      var seenOffices = {};

      allUsers.forEach(function (user) {
        if (seenOffices[user.officeId]) return;
        seenOffices[user.officeId] = true;
        offices.push({
          officeId: user.officeId,
          officeName: user.officeName,
        });
      });

      offices.sort(function (left, right) {
        return left.officeName.localeCompare(right.officeName);
      });

      officeFilter.innerHTML = '<option value="">All offices</option>';
      offices.forEach(function (office) {
        var option = document.createElement('option');
        option.value = office.officeId;
        option.textContent = office.officeName;
        officeFilter.appendChild(option);
      });

      officeFilter.value = offices.some(function (office) {
        return office.officeId === currentValue;
      }) ? currentValue : '';
    }

    function renderChips() {
      var selectedUsers = getSelectedUsers();
      chipsArea.innerHTML = '';

      if (!selectedUsers.length) {
        chipsArea.innerHTML = mode === 'single'
          ? '<div class="committee-member-empty">No committee head selected yet.</div>'
          : '<div class="committee-member-empty">No members selected yet.</div>';
      } else {
        selectedUsers.forEach(function (user) {
          var chip = document.createElement('span');
          chip.className = 'committee-member-chip';
          chip.textContent = user.name + ' · ' + user.officeName;

          var removeButton = document.createElement('button');
          removeButton.type = 'button';
          removeButton.className = 'committee-member-chip-remove';
          removeButton.textContent = '\u00D7';
          removeButton.setAttribute('aria-label', 'Remove ' + user.name);
          removeButton.addEventListener('click', function () {
            selectedIds.delete(user.id);
            renderChips();
            renderList();
          });

          chip.appendChild(removeButton);
          chipsArea.appendChild(chip);
        });
      }

      countBadge.textContent = mode === 'single'
        ? (selectedUsers.length ? 'Head selected' : 'Head required')
        : (selectedUsers.length + ' selected');

      syncHiddenInputs();
    }

    function matchesFilter(user) {
      var searchValue = (searchInput.value || '').trim().toLowerCase();
      var officeValue = officeFilter.value || '';

      if (officeValue && user.officeId !== officeValue) {
        return false;
      }

      if (!searchValue) {
        return true;
      }

      return (
        user.name.toLowerCase().indexOf(searchValue) !== -1 ||
        user.username.toLowerCase().indexOf(searchValue) !== -1 ||
        user.officeName.toLowerCase().indexOf(searchValue) !== -1
      );
    }

    function renderList() {
      optionList.innerHTML = '';

      if (!allUsers.length) {
        helperText.textContent = 'Users could not be loaded right now.';
        optionList.innerHTML = '<div class="committee-member-empty">No active users available.</div>';
        return;
      }

      var visibleUsers = allUsers.filter(matchesFilter);
      if (!visibleUsers.length) {
        helperText.textContent = 'No users match the current search or office filter.';
        optionList.innerHTML = '<div class="committee-member-empty">No users match the current filter.</div>';
        return;
      }

      helperText.textContent = 'Showing ' + visibleUsers.length + ' active user' + (visibleUsers.length === 1 ? '' : 's') + '.';

      visibleUsers.forEach(function (user) {
        var label = document.createElement('label');
        label.className = 'committee-member-option';

        var input = document.createElement('input');
        input.type = mode === 'single' ? 'radio' : 'checkbox';
        input.name = mode === 'single' ? radioGroupName : (radioGroupName + '_' + user.id);
        input.value = user.id;
        input.checked = selectedIds.has(user.id);
        input.addEventListener('change', function () {
          if (mode === 'single') {
            selectedIds.clear();
            if (input.checked) {
              selectedIds.add(user.id);
            }
            renderList();
          } else if (input.checked) {
            selectedIds.add(user.id);
          } else {
            selectedIds.delete(user.id);
          }
          renderChips();
        });

        var textWrap = document.createElement('span');
        textWrap.className = 'committee-member-option-copy';

        var nameLine = document.createElement('span');
        nameLine.className = 'committee-member-option-text';
        nameLine.textContent = user.name + ' (' + user.username + ')';

        var metaLine = document.createElement('span');
        metaLine.className = 'committee-member-option-meta';
        metaLine.textContent = user.officeName;

        textWrap.appendChild(nameLine);
        textWrap.appendChild(metaLine);
        label.appendChild(input);
        label.appendChild(textWrap);
        optionList.appendChild(label);
      });
    }

    searchInput.addEventListener('input', renderList);
    officeFilter.addEventListener('change', renderList);

    fetchUsers(apiUrl)
      .then(function (data) {
        allUsers = normalizeUsers(data);
        updateOfficeFilterOptions();
        renderChips();
        renderList();
      })
      .catch(function () {
        allUsers = [];
        renderChips();
        renderList();
      });
  }

  pickerNodes.forEach(initPicker);
})();
