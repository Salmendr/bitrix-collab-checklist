(function (global) {
    'use strict';

    // A checklist opened by a link of the chat lives in a slider of Bitrix24.
    // Bitrix24 keeps such a slider after it is closed (it is cached and only
    // hidden), so the page gets no pagehide and its session would stay open.
    // The browser still tells the page that it is no longer seen: the slider
    // slides out of the screen or is hidden. Then the window does what a
    // close does: it saves and releases the session (with the chat message
    // when something changed). Shown again (the same link opened again), the
    // window reloads and starts a new session. A switch to another tab of
    // the browser or a slider opened above it is not a close.

    // A view-only window (the platform portal) has no session.
    if (global.ChecklistAuth && global.ChecklistAuth.readOnly) {
        return;
    }
    if (typeof global.IntersectionObserver !== 'function') {
        return;
    }

    const HIDDEN_CONFIRM_MS = 1500;
    const openedAt = Date.now();

    let seenVisible = false;
    let visible = false;
    let hiddenTimer = null;
    let closedByHost = false;
    let closedAt = 0;
    let reloading = false;

    function log(event, payload) {
        try {
            if (typeof global.debugLog === 'function') {
                global.debugLog(event, payload || {});
            }
        } catch (error) {
            // Diagnostics only.
        }
    }

    function sessionId() {
        try {
            return typeof global.getActiveEditSessionId === 'function'
                ? global.getActiveEditSessionId()
                : '';
        } catch (error) {
            return '';
        }
    }

    function pendingUploads() {
        try {
            return typeof global.popupHasPendingUploads === 'function'
                && global.popupHasPendingUploads();
        } catch (error) {
            return false;
        }
    }

    function windowAlreadyClosed() {
        // «Сохранить и закрыть», «Отмена» or the server closed the window:
        // its session is finalized already.
        return !!(
            (typeof global.isPopupClosedByHost === 'function'
                && global.isPopupClosedByHost())
            || (typeof popupFinalizationInProgress !== 'undefined'
                && popupFinalizationInProgress)
        );
    }

    function closeForHost() {
        hiddenTimer = null;
        if (visible || closedByHost) return;
        // A hidden tab of the browser is not a close of the slider.
        if (global.document.visibilityState !== 'visible') return;

        closedByHost = true;
        closedAt = Date.now();
        const alreadyClosed = windowAlreadyClosed();
        log('popup_frame_hidden_detected', {
            sessionId: sessionId(),
            visibleForMs: closedAt - openedAt,
            uploadsPending: pendingUploads(),
            alreadyClosed
        });

        if (alreadyClosed) {
            return;
        }

        try {
            if (typeof global.sendCloseSummaryOnce === 'function') {
                // Uploads stop, the session is saved and released by the same
                // request as on a real close of the page.
                global.sendCloseSummaryOnce('popup_frame_hidden');
            }
        } catch (error) {
            log('popup_frame_hidden_finalize_failed', {
                error: String(error && error.message || error || '')
            });
        }
        try {
            if (typeof global.markPopupWindowClosed === 'function') {
                global.markPopupWindowClosed('popup_frame_hidden');
            }
        } catch (error) {
            // The heartbeat stops with the state.
        }
    }

    function reloadWhenShownAgain() {
        if (reloading) return;
        reloading = true;
        log('popup_frame_shown_again_reload', {
            hiddenForMs: closedAt ? Date.now() - closedAt : 0
        });
        global.setTimeout(function () {
            global.location.reload();
        }, 50);
    }

    const sentinel = global.document.createElement('div');
    sentinel.setAttribute('aria-hidden', 'true');
    sentinel.style.cssText = (
        'position:fixed;left:0;top:0;width:100%;height:100%;'
        + 'pointer-events:none;opacity:0;z-index:-1;'
    );
    global.document.body.appendChild(sentinel);

    const observer = new global.IntersectionObserver(function (entries) {
        const entry = entries[entries.length - 1];
        const nowVisible = !!(
            entry
            && entry.isIntersecting
            && entry.intersectionRatio > 0
        );

        if (nowVisible) {
            visible = true;
            seenVisible = true;
            if (hiddenTimer) {
                global.clearTimeout(hiddenTimer);
                hiddenTimer = null;
            }
            if (closedByHost) {
                reloadWhenShownAgain();
            }
            return;
        }

        visible = false;
        // The slider slides in from the side: the first report may be "not
        // seen" before the window was ever shown.
        if (!seenVisible || closedByHost) return;
        if (hiddenTimer) global.clearTimeout(hiddenTimer);
        hiddenTimer = global.setTimeout(closeForHost, HIDDEN_CONFIRM_MS);
    }, { threshold: [0] });

    observer.observe(sentinel);

    global.ChecklistPopupFrameVisibility = Object.freeze({
        getState: function () {
            return {
                seenVisible,
                visible,
                closedByHost
            };
        }
    });
})(window);
