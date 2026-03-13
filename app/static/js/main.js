/* ONGC Digital Workspace – Client-side utilities */

document.addEventListener("DOMContentLoaded", function () {
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
