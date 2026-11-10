(function (global) {
    'use strict';

    function bindDebugEvents() {
        if (debugStopYandexWarmupBtn) {
            debugStopYandexWarmupBtn.addEventListener(
                'click',
                stopCurrentYandexWarmupFromDebugPanel
            );
        }
    }

    function bindLifecycleEvents() {
        // The window is closed: the session is saved and released at once,
        // also while files are uploading (their requests stop with the
        // window; a file the server has fully received is saved with the
        // session). A slider of Bitrix24 that is only hidden on close is
        // handled by popup-frame-visibility.js.
        global.addEventListener('pagehide', function () {
            sendCloseSummaryOnce('popup_pagehide');
        });

        global.addEventListener('beforeunload', function (event) {
            if (
                typeof popupHasPendingUploads === 'function'
                && popupHasPendingUploads()
            ) {
                // A window of its own asks before leaving; if the user
                // leaves, pagehide saves the session.
                event.preventDefault();
                event.returnValue = '';
                return '';
            }

            sendCloseSummaryOnce('popup_beforeunload');
            return undefined;
        });
    }

    bindDebugEvents();
    bindLifecycleEvents();

    global.ChecklistPopupShellEvents = Object.freeze({
        bindDebugEvents,
        bindLifecycleEvents
    });
})(window);
