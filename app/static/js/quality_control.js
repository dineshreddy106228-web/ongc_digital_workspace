document.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("[data-qc-print]").forEach((button) => {
        button.addEventListener("click", () => window.print());
    });

    /* A remark is free text, sometimes several hundred characters. The register
       shows an excerpt; the whole of it opens here, so one long remark cannot
       squeeze the dated columns out of the table. */
    const dialog = document.querySelector("[data-qc-remark-dialog]");
    if (!dialog) return;
    const heading = dialog.querySelector("[data-qc-remark-heading]");
    const body = dialog.querySelector("[data-qc-remark-body]");
    let opener = null;

    const close = () => {
        dialog.hidden = true;
        document.body.classList.remove("mod-modal-open");
        if (opener) opener.focus();
    };

    document.querySelectorAll("[data-qc-remark]").forEach((chip) => {
        chip.addEventListener("click", () => {
            opener = chip;
            heading.textContent = chip.dataset.qcRemarkTitle || "Sample remark";
            body.textContent = chip.dataset.qcRemark;
            dialog.hidden = false;
            document.body.classList.add("mod-modal-open");
            dialog.querySelector(".mod-modal-close").focus();
        });
    });

    dialog.querySelectorAll("[data-qc-remark-close]").forEach((node) => node.addEventListener("click", close));
    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && !dialog.hidden) close();
    });
});
