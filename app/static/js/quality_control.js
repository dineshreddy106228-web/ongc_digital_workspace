document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("[data-qc-print]").forEach((button) => {
        button.addEventListener("click", () => window.print());
    });
});
