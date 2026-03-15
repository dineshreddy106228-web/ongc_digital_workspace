/* ONGC Digital Workspace – Client-side utilities */

document.addEventListener("DOMContentLoaded", function () {
    const storageKey = "ongc-theme";
    const sidebarStorageKey = "ongc-sidebar-flyout-open";
    const root = document.documentElement;
    const body = document.body;
    const toggle = document.querySelector(".js-theme-toggle");
    const sidebarToggles = document.querySelectorAll(".js-sidebar-toggle");
    const flyout = document.getElementById("workspace-flyout");
    const notificationsMenu = document.querySelector(".workspace-alerts");

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
                if (notificationsMenu && notificationsMenu.hasAttribute("open")) {
                    notificationsMenu.removeAttribute("open");
                }
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
});
