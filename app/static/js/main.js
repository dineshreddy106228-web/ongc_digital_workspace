/* ONGC Digital Workspace – Client-side utilities */

document.addEventListener("DOMContentLoaded", function () {
    const storageKey = "ongc-theme";
    const root = document.documentElement;
    const toggle = document.querySelector(".js-theme-toggle");

    function applyTheme(theme) {
        const nextTheme = theme === "dark" ? "dark" : "light";
        root.setAttribute("data-theme", nextTheme);
        localStorage.setItem(storageKey, nextTheme);

        if (toggle) {
            const isDark = nextTheme === "dark";
            toggle.setAttribute("aria-pressed", String(isDark));
            toggle.querySelector(".theme-toggle-icon").textContent = isDark ? "☀" : "☾";
            toggle.querySelector(".theme-toggle-label").textContent = isDark ? "Light" : "Dark";
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
