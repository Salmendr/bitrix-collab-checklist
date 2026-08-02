// Stage 6.6.4. Event bindings for the ID renderer.
const popupIdEventCommon = window.ChecklistPopupEventCommon;
if (!popupIdEventCommon) {
    throw new Error('popup-event-common.js is not initialized');
}

function bindIdRendererEvents() {
    popupIdEventCommon.bindStandardChecklistEvents();

    document.querySelectorAll('[data-role="toggle-id-dates"]').forEach(button => {
        button.addEventListener('click', function () {
            const groupId = Number(this.dataset.groupId || 0);
            if (![1, 2, 3].includes(groupId)) return;

            idDateVisibility[groupId] = !idDateVisibility[groupId];
            renderAll();
        });
    });
}
