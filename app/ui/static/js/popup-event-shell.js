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
        document.addEventListener('visibilitychange', function () {
            if (document.visibilityState !== 'hidden') {
                return;
            }

            if (
                typeof popupHasPendingUploads === 'function'
                && popupHasPendingUploads()
            ) {
                return;
            }

            sendCloseSummaryOnce('popup_hidden');
        });

        global.addEventListener('pagehide', function () {
            if (
                typeof popupHasPendingUploads === 'function'
                && popupHasPendingUploads()
            ) {
                return;
            }

            sendCloseSummaryOnce('popup_pagehide');
        });

        global.addEventListener('beforeunload', function (event) {
            if (
                typeof popupHasPendingUploads === 'function'
                && popupHasPendingUploads()
            ) {
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
