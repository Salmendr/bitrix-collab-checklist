(function (global) {
    'use strict';

    // Stage 8.4.3: date values remain in checklist data, but popup date
    // controls and columns are intentionally unavailable in the frontend.
    function getDateGridClass() {
        return 'id-grid id-grid-compact';
    }

    function getDateToggleTitle() {
        return 'Даты скрыты';
    }

    global.ChecklistPopupRendererCommon = Object.freeze({
        getDateGridClass,
        getDateToggleTitle
    });
})(window);
