// Stage 6.6.4. Event bindings for the Concept renderer.
const popupConceptEventCommon = window.ChecklistPopupEventCommon;
if (!popupConceptEventCommon) {
    throw new Error('popup-event-common.js is not initialized');
}

function bindConceptRendererEvents() {
    popupConceptEventCommon.bindStandardChecklistEvents();

    document.querySelectorAll('[data-role="toggle-concept-dates"]').forEach(button => {
        button.addEventListener('click', function () {
            const groupId = Number(this.dataset.groupId || 0);
            if (groupId !== 1) return;

            conceptDateVisibility[groupId] = !conceptDateVisibility[groupId];
            renderAll();
        });
    });
}
