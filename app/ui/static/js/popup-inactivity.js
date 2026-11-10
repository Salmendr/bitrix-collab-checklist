(function (global) {
    'use strict';

    // A view-only window (the platform portal) has no session to save and
    // is never closed for inactivity.
    if (global.ChecklistAuth && global.ChecklistAuth.readOnly) {
        return;
    }

    const bootstrap = global.CHECKLIST_POPUP_BOOTSTRAP || {};
    const timeoutMs = Math.max(60000, Number(
        bootstrap.inactivityTimeoutSeconds || 600
    ) * 1000);
    const warningMs = Math.max(10000, Math.min(
        timeoutMs - 1000,
        Number(bootstrap.inactivityWarningSeconds || 60) * 1000
    ));
    const warningText = 'Чек-лист будет автоматически сохранён и закрыт через 1 минуту из-за отсутствия активности. Для продолжения сессии совершите любое взаимодействие с чек-листом.';

    let lastActivityAt = Date.now();
    let warningVisible = false;
    let finalizing = false;
    let checkTimer = null;

    const warning = global.document.createElement('div');
    warning.className = 'popup-inactivity-warning';
    warning.hidden = true;
    warning.setAttribute('role', 'alert');
    warning.setAttribute('aria-live', 'assertive');
    warning.textContent = warningText;
    global.document.body.appendChild(warning);

    function notifyEditSession(activityAt) {
        const api = global.ChecklistPopupEditSession;
        if (api && typeof api.noteActivity === 'function') {
            api.noteActivity(new Date(activityAt).toISOString());
        }
    }

    function noteActivity(source = 'popup', activityValue = Date.now()) {
        const parsed = new Date(activityValue);
        const activityAt = Number.isNaN(parsed.getTime())
            ? Date.now()
            : Math.min(Date.now(), parsed.getTime());
        if (activityAt < lastActivityAt) return;
        lastActivityAt = activityAt;
        warningVisible = false;
        warning.hidden = true;
        notifyEditSession(activityAt);
    }

    async function finalizeForInactivity() {
        if (finalizing) return;
        if (windowClosed()) return;
        finalizing = true;
        warning.hidden = true;
        try {
            if (typeof global.finalizePopupSession !== 'function') {
                throw new Error('Финализация popup недоступна');
            }
            const completed = await global.finalizePopupSession(true, {
                reason: 'inactivity_timeout',
                source: 'popup_inactivity'
            });
            if (!completed) {
                throw new Error('Автоматическое сохранение не завершено');
            }
        } catch (error) {
            finalizing = false;
            noteActivity('inactivity_finalize_failed', Date.now());
            try {
                setSaveState('error', 'Ошибка автоматического сохранения: ' + String(error && error.message || error || '').slice(0, 120));
            } catch (stateError) {}
        }
    }

    function windowClosed() {
        return typeof global.isPopupClosedByHost === 'function'
            && global.isPopupClosedByHost();
    }

    // An upload in progress is not inactivity: the ten minutes count from
    // the end of the last file.
    let lastUploadActivityAt = 0;
    let uploadsWerePending = false;
    function uploadsInProgress() {
        try {
            return typeof global.popupHasPendingUploads === 'function'
                && global.popupHasPendingUploads();
        } catch (error) {
            return false;
        }
    }

    function checkInactivity() {
        if (finalizing) return;
        if (windowClosed()) return;
        if (uploadsInProgress()) {
            const now = Date.now();
            uploadsWerePending = true;
            if (now - lastUploadActivityAt >= 30000 || warningVisible) {
                lastUploadActivityAt = now;
                noteActivity('upload_in_progress', now);
            }
            return;
        }
        if (uploadsWerePending) {
            // The last file has just finished: the ten minutes start now.
            uploadsWerePending = false;
            noteActivity('upload_finished', Date.now());
            return;
        }
        const idleMs = Date.now() - lastActivityAt;
        if (idleMs >= timeoutMs) {
            finalizeForInactivity();
            return;
        }
        if (idleMs >= timeoutMs - warningMs) {
            if (!warningVisible) {
                warningVisible = true;
                warning.hidden = false;
            }
        } else if (warningVisible) {
            warningVisible = false;
            warning.hidden = true;
        }
    }

    const activityEvents = [
        'pointerdown', 'keydown', 'input', 'change',
        'dragstart', 'drop', 'touchstart'
    ];
    activityEvents.forEach(eventName => {
        global.document.addEventListener(
            eventName,
            event => {
                if (event.isTrusted === false) return;
                if (warning.contains(event.target)) return;
                noteActivity(eventName);
            },
            { capture: true, passive: true }
        );
    });
    let lastScrollAt = 0;
    global.document.addEventListener('scroll', function (event) {
        if (event.isTrusted === false) return;
        const now = Date.now();
        if (now - lastScrollAt < 250) return;
        lastScrollAt = now;
        noteActivity('scroll', now);
    }, { capture: true, passive: true });

    function isCurrentSessionActivity(payload) {
        const activitySessionId = String(
            payload && payload.sessionId || ''
        ).trim();
        const editSessionApi = global.ChecklistPopupEditSession;
        const currentSessionId = String(
            editSessionApi
            && typeof editSessionApi.getSessionId === 'function'
                ? editSessionApi.getSessionId()
                : ''
        ).trim();
        return !activitySessionId
            || !currentSessionId
            || activitySessionId === currentSessionId;
    }

    global.addEventListener('checklist-folder-user-activity', event => {
        const detail = event.detail || {};
        if (!isCurrentSessionActivity(detail)) return;
        noteActivity(
            'folder:' + String(detail.eventType || 'interaction'),
            detail.activityAt || Date.now()
        );
    });
    global.addEventListener('message', event => {
        if (event.origin !== global.location.origin) return;
        const data = event.data || {};
        if (data.type !== 'checklist-user-activity') return;
        if (!isCurrentSessionActivity(data)) return;
        noteActivity(
            'folder-message:' + String(data.eventType || 'interaction'),
            data.activityAt || Date.now()
        );
    });
    global.addEventListener('checklist-edit-session-inactivity-due', function () {
        finalizeForInactivity();
    });
    // The server saved the session (ten minutes without actions or five
    // minutes without a heartbeat): the window closes and is not opened again.
    global.addEventListener('checklist-edit-session-inactivity-finalized', function (event) {
        finalizing = true;
        warning.hidden = true;
        if (checkTimer) global.clearInterval(checkTimer);
        const detail = event && event.detail || {};
        try {
            if (typeof global.markPopupWindowClosed === 'function') {
                global.markPopupWindowClosed(
                    'server_' + String(detail.closeReason || 'inactivity_timeout')
                );
            }
        } catch (error) {}
        try { closePopupWindow(); } catch (error) {}
    });

    notifyEditSession(lastActivityAt);
    checkTimer = global.setInterval(checkInactivity, 1000);

    global.ChecklistPopupInactivity = Object.freeze({
        noteActivity,
        getState: () => ({
            lastActivityAt: new Date(lastActivityAt).toISOString(),
            idleMs: Date.now() - lastActivityAt,
            warningVisible,
            finalizing,
            timeoutMs,
            warningMs
        }),
        timeoutMs,
        warningMs
    });
})(window);
