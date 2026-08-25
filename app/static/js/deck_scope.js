/* The presentation-scope chooser, shared by every module that exports a deck.
   Markup comes from templates/components/_deck_scope.html.

   Two behaviours, both enhancements over a form that already works without
   them: the dialog itself, and the shorthand that ticking an item means "these
   ones" and ticking a group means "everything in it". */
(() => {
  const describeName = (box) => {
    const label = box.closest("label");
    const name = label && label.querySelector("strong");
    return name ? name.textContent.trim() : (box.value || "");
  };

  const setupScopeForm = (form) => {
    if (form.dataset.deckScopeReady === "true") return;
    form.dataset.deckScopeReady = "true";
    const all = form.querySelector("[data-deck-scope-all]");
    const set = form.querySelector("[data-deck-scope-set]");
    const items = Array.from(form.querySelectorAll("[data-deck-scope-item]"));
    const summary = form.querySelector("[data-deck-scope-summary]");
    const allLabel = summary ? summary.textContent : "";

    const describe = () => {
      if (!summary) return;
      const chosen = items.filter((box) => box.checked);
      if (set && set.checked && chosen.length) {
        const names = chosen.map(describeName);
        summary.textContent = chosen.length === 1
          ? names[0] + " only."
          : chosen.length + " selected — " + names.slice(0, 3).join(", ") + (chosen.length > 3 ? " and " + (chosen.length - 3) + " more." : ".");
      } else if (set && set.checked) {
        summary.textContent = "Nothing selected yet — tick one below.";
      } else {
        summary.textContent = allLabel;
      }
    };

    /* Whatever is ticked is what the deck covers, so ticking sets the radio and
       clearing the last tick hands it back to "all". */
    const syncRadios = () => {
      if (items.some((box) => box.checked)) {
        if (set) set.checked = true;
      } else if (all) {
        all.checked = true;
      }
    };

    items.forEach((box) => box.addEventListener("change", () => { syncRadios(); describe(); }));

    form.querySelectorAll("[data-deck-scope-group]").forEach((groupBox) => {
      // The tick lives inside <summary>; without this a click would also toggle
      // the disclosure it sits in.
      groupBox.addEventListener("click", (event) => event.stopPropagation());
      groupBox.addEventListener("change", () => {
        const group = groupBox.closest("details") || form;
        group.querySelectorAll("[data-deck-scope-item]").forEach((box) => { box.checked = groupBox.checked; });
        syncRadios();
        describe();
      });
    });

    if (all) all.addEventListener("change", () => {
      if (all.checked) {
        items.forEach((box) => { box.checked = false; });
        form.querySelectorAll("[data-deck-scope-group]").forEach((box) => { box.checked = false; });
      }
      describe();
    });
    if (set) set.addEventListener("change", describe);
  };

  const setupModal = (trigger) => {
    if (trigger.dataset.deckModalReady === "true") return;
    trigger.dataset.deckModalReady = "true";
    const modal = document.getElementById(trigger.dataset.deckOpen);
    if (!modal) return;
    const close = () => {
      modal.hidden = true;
      document.body.classList.remove("mod-modal-open");
      trigger.focus();
    };
    trigger.addEventListener("click", () => {
      modal.hidden = false;
      document.body.classList.add("mod-modal-open");
      (modal.querySelector("input, button, select") || modal).focus();
    });
    modal.querySelectorAll("[data-deck-close]").forEach((node) => node.addEventListener("click", close));
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !modal.hidden) close();
    });
  };

  const setup = () => {
    document.querySelectorAll("[data-deck-scope]").forEach(setupScopeForm);
    document.querySelectorAll("[data-deck-open]").forEach(setupModal);
  };

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", setup);
  else setup();
})();
