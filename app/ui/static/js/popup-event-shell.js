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

            if (typeof registerPopupCloseHandoff === 'function') {
                registerPopupCloseHandoff({ source: 'popup_hidden' });
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

            if (typeof registerPopupCloseHandoff === 'function') {
                registerPopupCloseHandoff({ source: 'popup_pagehide' });
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

            if (typeof registerPopupCloseHandoff === 'function') {
                registerPopupCloseHandoff({ source: 'popup_beforeunload' });
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
