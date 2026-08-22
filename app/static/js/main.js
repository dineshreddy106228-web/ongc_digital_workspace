/* ONGC Digital Workspace – Client-side utilities */

document.addEventListener("DOMContentLoaded", function () {
    const storageKey = "ongc-theme";
    const sidebarStorageKey = "ongc-sidebar-flyout-open";
    const root = document.documentElement;
    const body = document.body;
    const toggle = document.querySelector(".js-theme-toggle");
    const sidebarToggles = document.querySelectorAll(".js-sidebar-toggle");
    const flyout = document.getElementById("workspace-flyout");

    function setFlyoutState(isOpen) {
        if (!flyout || !body.classList.contains("app-authenticated")) return;

        body.classList.toggle("workspace-flyout-open", isOpen);
        flyout.setAttribute("aria-hidden", String(!isOpen));

        sidebarToggles.forEach(function (btn) {
            btn.classList.toggle("is-open", isOpen);
            btn.setAttribute("aria-expanded", String(isOpen));
        });

        localStorage.setItem(sidebarStorageKey, String(isOpen));
    }

    function applyTheme(theme) {
        const nextTheme = theme === "dark" ? "dark" : "light";
        root.setAttribute("data-theme", nextTheme);
        localStorage.setItem(storageKey, nextTheme);

        if (toggle) {
            const isDark = nextTheme === "dark";
            toggle.setAttribute("aria-pressed", String(isDark));
            toggle.querySelector(".theme-toggle-icon").textContent = isDark ? "☀" : "☾";
            toggle.querySelectorAll(".theme-toggle-label").forEach(function (label) {
                label.textContent = isDark ? "Light" : "Dark";
            });
        }

        document.dispatchEvent(new CustomEvent("themechange", { detail: { theme: nextTheme } }));
    }

    applyTheme(root.getAttribute("data-theme") || localStorage.getItem(storageKey) || "light");

    if (toggle) {
        toggle.addEventListener("click", function () {
            const currentTheme = root.getAttribute("data-theme") || "light";
            applyTheme(currentTheme === "dark" ? "light" : "dark");
        });
    }

    if (flyout && body.classList.contains("app-authenticated")) {
        const initialFlyoutState = localStorage.getItem(sidebarStorageKey) === "true" && window.innerWidth > 768;
        setFlyoutState(initialFlyoutState);

        sidebarToggles.forEach(function (btn) {
            btn.addEventListener("click", function () {
                const isOpen = body.classList.contains("workspace-flyout-open");
                setFlyoutState(!isOpen);
            });
        });

        document.addEventListener("keydown", function (event) {
            if (event.key === "Escape") {
                setFlyoutState(false);
            }
        });

        window.addEventListener("resize", function () {
            if (window.innerWidth <= 768) {
                setFlyoutState(false);
            } else if (localStorage.getItem(sidebarStorageKey) === "true") {
                setFlyoutState(true);
            }
        });

        document.querySelectorAll(".workspace-flyout a").forEach(function (link) {
            link.addEventListener("click", function () {
                if (window.innerWidth <= 768) {
                    setFlyoutState(false);
                }
            });
        });
    }

    function syncRichTextField(field) {
        const editor = field.querySelector("[data-rich-text-editor]");
        const input = field.querySelector(".rich-text-input");
        if (!editor || !input) {
            return;
        }

        const isEmpty = editor.textContent.trim() === "";
        editor.classList.toggle("is-empty", isEmpty);
        input.value = isEmpty ? "" : editor.innerHTML.trim();
    }

    function updateRichTextButtons(field) {
        const editor = field.querySelector("[data-rich-text-editor]");
        if (!editor) {
            return;
        }

        const selection = document.getSelection();
        const hasFocus = document.activeElement === editor || (selection && editor.contains(selection.anchorNode));

        field.querySelectorAll("[data-rich-text-command]").forEach(function (button) {
            const command = button.getAttribute("data-rich-text-command");
            let isActive = false;

            if (hasFocus) {
                try {
                    isActive = document.queryCommandState(command);
                } catch (error) {
                    isActive = false;
                }
            }

            button.classList.toggle("is-active", Boolean(isActive));
            button.setAttribute("aria-pressed", String(Boolean(isActive)));
        });
    }

    function initializeRichTextFields() {
        document.querySelectorAll("[data-rich-text-field]").forEach(function (field) {
            const editor = field.querySelector("[data-rich-text-editor]");
            const input = field.querySelector(".rich-text-input");
            const form = field.closest("form");

            if (!editor || !input) {
                return;
            }

            try {
                document.execCommand("styleWithCSS", false, false);
            } catch (error) {
                // The editor still works when the command is unsupported.
            }

            field.querySelectorAll("[data-rich-text-command]").forEach(function (button) {
                button.addEventListener("click", function () {
                    const command = button.getAttribute("data-rich-text-command");
                    if (!command) {
                        return;
                    }

                    editor.focus();
                    document.execCommand(command, false, null);
                    syncRichTextField(field);
                    updateRichTextButtons(field);
                });
            });

            editor.addEventListener("input", function () {
                syncRichTextField(field);
                updateRichTextButtons(field);
            });

            editor.addEventListener("blur", function () {
                syncRichTextField(field);
                updateRichTextButtons(field);
            });

            editor.addEventListener("keyup", function () {
                updateRichTextButtons(field);
            });

            editor.addEventListener("mouseup", function () {
                updateRichTextButtons(field);
            });

            editor.addEventListener("paste", function (event) {
                event.preventDefault();
                const pastedText = (event.clipboardData || window.clipboardData).getData("text/plain");
                document.execCommand("insertText", false, pastedText);
            });

            if (form) {
                form.addEventListener("submit", function () {
                    syncRichTextField(field);
                });
            }

            document.addEventListener("selectionchange", function () {
                updateRichTextButtons(field);
            });

            syncRichTextField(field);
            updateRichTextButtons(field);
        });
    }

    function hasExistingSerialColumn(table) {
        const firstHeaderRow = table.tHead && table.tHead.rows.length
            ? table.tHead.rows[0]
            : null;

        if (!firstHeaderRow) {
            return false;
        }

        const firstHeaderCell = firstHeaderRow.cells[0];
        if (!firstHeaderCell) {
            return false;
        }

        const label = firstHeaderCell.textContent.trim().toLowerCase().replace(/[.\s]/g, "");
        return label === "#" || label === "no" || label === "sno" || label === "slno";
    }

    function isSpanningTableRow(row) {
        if (row.hasAttribute("data-no-serial")) {
            return true;
        }

        const cells = row.cells;
        return cells.length === 1 && cells[0].colSpan > 1;
    }

    function addSerialNumbersToTable(table) {
        if (
            table.hasAttribute("data-no-serial") ||
            !table.tHead ||
            !table.tHead.rows.length ||
            !table.tBodies.length
        ) {
            return;
        }

        if (!table.hasAttribute("data-auto-serial")) {
            if (hasExistingSerialColumn(table)) {
                table.setAttribute("data-auto-serial", "existing");
                return;
            }

            const headerRows = table.tHead.rows;
            const serialHeader = document.createElement("th");
            serialHeader.className = "table-serial-column";
            serialHeader.scope = "col";
            serialHeader.textContent = "S. No.";
            if (headerRows.length > 1) {
                serialHeader.rowSpan = headerRows.length;
            }
            headerRows[0].insertBefore(serialHeader, headerRows[0].firstElementChild);
            table.setAttribute("data-auto-serial", "added");
        }

        if (table.getAttribute("data-auto-serial") !== "added") {
            return;
        }

        let serialNumber = 0;
        Array.prototype.forEach.call(table.tBodies, function (tbody) {
            Array.prototype.forEach.call(tbody.rows, function (row) {
                if (isSpanningTableRow(row)) {
                    const spanningCell = row.cells[0];
                    if (
                        spanningCell &&
                        spanningCell.colSpan > 1 &&
                        !spanningCell.hasAttribute("data-serial-colspan-adjusted")
                    ) {
                        spanningCell.colSpan += 1;
                        spanningCell.setAttribute("data-serial-colspan-adjusted", "true");
                    }
                    return;
                }

                serialNumber += 1;
                let serialCell = row.querySelector(":scope > .table-serial-column");
                if (!serialCell) {
                    serialCell = document.createElement("td");
                    serialCell.className = "table-serial-column";
                    row.insertBefore(serialCell, row.firstElementChild);
                }
                const nextSerial = String(serialNumber);
                if (serialCell.textContent !== nextSerial) {
                    serialCell.textContent = nextSerial;
                }
            });
        });
    }

    let serialNumberPassScheduled = false;
    function addSerialNumbersToAllTables() {
        document.querySelectorAll("table").forEach(addSerialNumbersToTable);
    }

    function scheduleSerialNumberPass() {
        if (serialNumberPassScheduled) {
            return;
        }
        serialNumberPassScheduled = true;
        window.requestAnimationFrame(function () {
            serialNumberPassScheduled = false;
            addSerialNumbersToAllTables();
        });
    }

    // Manual dismiss for flash messages (replaces inline onclick handlers).
    document.querySelectorAll(".js-flash-close").forEach(function (btn) {
        btn.addEventListener("click", function () {
            const container = btn.closest(".flash");
            if (container) container.remove();
        });
    });

    // Auto-dismiss flash messages after 6 seconds
    document.querySelectorAll(".flash").forEach(function (el) {
        setTimeout(function () {
            el.style.transition = "opacity .4s ease";
            el.style.opacity = "0";
            setTimeout(function () { el.remove(); }, 400);
        }, 6000);
    });

    // Calcutta · Madras live clock (IST = Asia/Kolkata, UTC+5:30)
    var clockEl = document.querySelector(".js-ist-clock");
    if (clockEl) {
        function tickISTClock() {
            clockEl.textContent = new Date().toLocaleTimeString("en-IN", {
                timeZone: "Asia/Kolkata",
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                hour12: false
            });
        }
        tickISTClock();
        setInterval(tickISTClock, 1000);
    }

    initializeRichTextFields();
    addSerialNumbersToAllTables();

    // Inventory and reporting screens build some tables after the initial page
    // render. Keep their serial columns in sync when rows are added or replaced.
    const tableObserver = new MutationObserver(scheduleSerialNumberPass);
    tableObserver.observe(document.body, { childList: true, subtree: true });

});
