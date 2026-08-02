// Stage 6.6.4. Event bindings for generic and configured BIM renderers.
const popupGenericEventCommon = window.ChecklistPopupEventCommon;
if (!popupGenericEventCommon) {
    throw new Error('popup-event-common.js is not initialized');
}

function bindGenericRendererEvents() {
    popupGenericEventCommon.bindStandardChecklistEvents();

    document.querySelectorAll('[data-role="toggle-generic-dates"]').forEach(button => {
        button.addEventListener('click', function () {
            const groupId = Number(this.dataset.groupId || 0);
            if (!groupId) return;

            genericDateVisibility[groupId] = !genericDateVisibility[groupId];
            renderAll();
        });
    });
}
