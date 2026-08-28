/* Drag-to-reorder for the task register.
 *
 * The up and down buttons beside each row do the same job without any of this,
 * so nothing here is required for the feature to work — it only makes a long
 * list quicker to arrange. Each table names the form that carries its sequence;
 * on drop we rewrite that sequence and submit, and the server decides what the
 * order means.
 */
(() => {
    "use strict";

    const tables = document.querySelectorAll("[data-reorder-table]");
    if (!tables.length) { return; }

    tables.forEach((table) => {
        const form = document.getElementById(table.dataset.reorderTable);
        const sequence = form && form.querySelector("[data-reorder-sequence]");
        const body = table.tBodies[0];
        if (!form || !sequence || !body) { return; }

        let dragged = null;

        const rows = () => Array.from(body.querySelectorAll("[data-reorder-row]"));

        const renumber = () => {
            rows().forEach((row, index) => {
                const serial = row.querySelector(".is-serial");
                if (!serial) { return; }
                // Keep the grip; replace only the number beside it.
                const text = Array.from(serial.childNodes).find(
                    (node) => node.nodeType === Node.TEXT_NODE
                );
                if (text) { text.nodeValue = String(index + 1); }
            });
        };

        body.addEventListener("dragstart", (event) => {
            const row = event.target.closest("[data-reorder-row]");
            if (!row) { return; }
            dragged = row;
            row.classList.add("is-dragging");
            // Firefox will not start a drag without data on the transfer.
            event.dataTransfer.effectAllowed = "move";
            event.dataTransfer.setData("text/plain", row.dataset.reorderRow);
        });

        body.addEventListener("dragover", (event) => {
            if (!dragged) { return; }
            const over = event.target.closest("[data-reorder-row]");
            if (!over || over === dragged) { return; }
            event.preventDefault();
            event.dataTransfer.dropEffect = "move";
            // Insert before or after depending on which half was entered, so a
            // row can be dropped at either end of the list.
            const box = over.getBoundingClientRect();
            const after = event.clientY > box.top + box.height / 2;
            body.insertBefore(dragged, after ? over.nextSibling : over);
        });

        body.addEventListener("dragend", () => {
            if (!dragged) { return; }
            dragged.classList.remove("is-dragging");
            dragged = null;

            const order = rows().map((row) => row.dataset.reorderRow);
            if (order.join(",") === sequence.value) { return; }
            renumber();
            sequence.value = order.join(",");
            form.submit();
        });
    });
})();
