(function (global) {
    'use strict';

    let activeSessionId = '';
    let activeSessionStatus = 'idle';
    let lastSessionError = '';
    let startPromise = null;
    let lastActivityAt = new Date().toISOString();
    let heartbeatPromise = null;
    let activityHeartbeatTimer = null;
    let lastActivityHeartbeatAt = 0;

    function emitSessionState() {
        try {
            global.dispatchEvent(
                new CustomEvent(
                    'checklist-edit-session-state',
                    {
                        detail: {
                            sessionId: activeSessionId,
                            status: activeSessionStatus,
                            error: lastSessionError
                        }
                    }
                )
            );
        } catch (error) {
            // CustomEvent can be unavailable only in isolated test runtimes.
        }
    }

    function setSessionStatus(status, errorText = '') {
        activeSessionStatus = String(status || '').trim() || 'idle';
        lastSessionError = String(errorText || '').trim();
        emitSessionState();

        const handoff = global.ChecklistPopupCloseHandoff;
        if (
            activeSessionStatus === 'active'
            && activeSessionId
            && handoff
            && typeof handoff.register === 'function'
        ) {
            handoff.register({ source: 'popup_edit_session' });
        }
    }

    function actor() {
        if (typeof getCurrentEditorIdentity === 'function') {
            return getCurrentEditorIdentity();
        }

        return {
            userId: '',
            userName: 'Пользователь'
        };
    }

    async function post(path, payload) {
        const response = await fetch(appUrl(path), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload || {})
        });
        const result = await response.json().catch(() => ({}));

        if (!response.ok || !result.ok) {
            throw new Error(
                result.error
                || ('HTTP ' + response.status)
            );
        }

        return result;
    }

    async function start() {
        if (activeSessionId && activeSessionStatus === 'active') {
            return activeSessionId;
        }

        if (startPromise) {
            return startPromise;
        }

        setSessionStatus('starting');

        startPromise = (async function () {
            if (typeof ensurePopupClientSessionReady === 'function') {
                await ensurePopupClientSessionReady();
            }

            const identity = actor();

            debugLog('popup_edit_session_start_requested', {
                dialogId,
                checklistKey: currentChecklistKey,
                clientSessionId,
                userId: identity.userId
            });

            const result = await post(
                'api/checklist/session/start',
                {
                    dialogId,
                    userId: identity.userId,
                    userName: identity.userName,
                    clientSessionId,
                    metadata: {
                        source: 'popup',
                        checklistKey: currentChecklistKey,
                        href: String(global.location.href || '')
                    }
                }
            );

            const session = result.session || {};
            activeSessionId = String(
                session.sessionId || ''
            ).trim();
            setSessionStatus(
                String(session.status || '').trim()
            );

            if (!activeSessionId || activeSessionStatus !== 'active') {
                throw new Error('Не удалось создать сессию редактирования');
            }

            debugLog('popup_edit_session_started', {
                sessionId: activeSessionId,
                created: !!result.created,
                resumed: !!result.resumed,
                clientSessionId
            });

            return activeSessionId;
        })();

        try {
            return await startPromise;
        } catch (error) {
            const errorText = String(
                error && error.message || error || ''
            );
            activeSessionId = '';
            setSessionStatus('failed', errorText);
            debugLog('popup_edit_session_start_failed', {
                dialogId,
                checklistKey: currentChecklistKey,
                clientSessionId,
                error: errorText
            });
            throw error;
        } finally {
            startPromise = null;
        }
    }

    async function ensureActive() {
        if (activeSessionId && activeSessionStatus === 'active') {
            return activeSessionId;
        }
        return start();
    }

    async function heartbeat(silent = true) {
        if (!activeSessionId || activeSessionStatus !== 'active') {
            return null;
        }
        if (heartbeatPromise) {
            return heartbeatPromise;
        }

        heartbeatPromise = (async function () {
            const identity = actor();
            try {
                const result = await post(
                    'api/checklist/session/heartbeat',
                    {
                        sessionId: activeSessionId,
                        dialogId,
                        userId: identity.userId,
                        clientSessionId,
                        lastActivityAt
                    }
                );

                if (result.inactivityFinalized) {
                    setSessionStatus('committed');
                    global.dispatchEvent(new CustomEvent(
                        'checklist-edit-session-inactivity-finalized',
                        { detail: result }
                    ));
                    return result;
                }
                if (result.inactivityDue) {
                    global.dispatchEvent(new CustomEvent(
                        'checklist-edit-session-inactivity-due',
                        { detail: result }
                    ));
                    return result;
                }

                setSessionStatus(
                    String(
                        result.session
                        && result.session.status
                        || 'active'
                    ).trim()
                );
                return result;
            } catch (error) {
                const errorText = String(
                    error && error.message || error || ''
                );
                activeSessionId = '';
                setSessionStatus('failed', errorText);
                if (!silent) {
                    console.log('edit session heartbeat error:', error);
                }
                return null;
            }
        })();

        try {
            return await heartbeatPromise;
        } finally {
            heartbeatPromise = null;
        }
    }


    async function commit(reason = 'save_and_close') {
        await ensureActive();

        const identity = actor();
        setSessionStatus('committing');

        try {
            const result = await post(
                'api/checklist/session/commit',
                {
                    sessionId: activeSessionId,
                    dialogId,
                    userId: identity.userId,
                    reason
                }
            );

            setSessionStatus(
                String(
                    result.session
                    && result.session.status
                    || 'committed'
                ).trim()
            );

            debugLog('popup_edit_session_committed', {
                sessionId: activeSessionId,
                status: activeSessionStatus,
                reason
            });

            return result;
        } catch (error) {
            setSessionStatus(
                'active',
                String(error && error.message || error || '')
            );
            throw error;
        }
    }

    async function finalize(payload, reason = 'save_and_close') {
        await ensureActive();

        const identity = actor();
        const requestPayload = {
            ...(payload && typeof payload === 'object' ? payload : {}),
            sessionId: activeSessionId,
            dialogId,
            userId: identity.userId,
            reason: String(reason || 'save_and_close')
        };

        setSessionStatus('committing');

        try {
            const result = await post(
                'api/checklist/session/finalize',
                requestPayload
            );

            if (result.committed !== true) {
                throw new Error(
                    result.error
                    || 'Сессия не была зафиксирована'
                );
            }

            setSessionStatus('committed');
            if (global.ChecklistPopupCloseHandoff) {
                global.ChecklistPopupCloseHandoff.clear();
            }
            debugLog('popup_edit_session_finalized', {
                sessionId: activeSessionId,
                reason: requestPayload.reason,
                savedCount: Number(result.savedCount || 0),
                messageStatus: String(result.messageStatus || '')
            });
            return result;
        } catch (error) {
            setSessionStatus(
                'active',
                String(error && error.message || error || '')
            );
            throw error;
        }
    }

    function finalizeOnUnload(
        payload,
        reason = 'popup_unload_autosave'
    ) {
        if (
            !activeSessionId
            || activeSessionStatus !== 'active'
            || !global.navigator
            || typeof global.navigator.sendBeacon !== 'function'
        ) {
            return false;
        }

        const identity = actor();
        const requestPayload = {
            ...(payload && typeof payload === 'object' ? payload : {}),
            sessionId: activeSessionId,
            dialogId,
            userId: identity.userId,
            reason: String(reason || 'popup_unload_autosave')
        };
        const blob = new Blob(
            [JSON.stringify(requestPayload)],
            { type: 'application/json' }
        );
        const queued = global.navigator.sendBeacon(
            appUrl('api/checklist/session/finalize'),
            blob
        );

        if (queued) {
            setSessionStatus('committing');
            debugLog('popup_edit_session_unload_finalize_queued', {
                sessionId: activeSessionId,
                reason: requestPayload.reason
            });
        }

        return queued;
    }

    async function rollback(reason = 'cancel_button', confirmed = false) {
        if (reason !== 'cancel_button' || confirmed !== true) {
            throw new Error(
                'Rollback разрешён только после подтверждённой кнопки «Отменить»'
            );
        }

        await ensureActive();

        const identity = actor();
        setSessionStatus('rolling_back');

        try {
            const result = await post(
                'api/checklist/session/rollback',
                {
                    sessionId: activeSessionId,
                    dialogId,
                    userId: identity.userId,
                    reason,
                    confirmed: true
                }
            );

            setSessionStatus(
                String(
                    result.session
                    && result.session.status
                    || 'rolled_back'
                ).trim()
            );

            if (global.ChecklistPopupCloseHandoff) {
                global.ChecklistPopupCloseHandoff.clear();
            }
            debugLog('popup_edit_session_rolled_back', {
                sessionId: activeSessionId,
                status: activeSessionStatus,
                reason
            });

            return result;
        } catch (error) {
            setSessionStatus(
                'active',
                String(error && error.message || error || '')
            );
            throw error;
        }
    }

    function commitOnUnload(reason = 'popup_unload_autosave') {
        if (
            !activeSessionId
            || activeSessionStatus !== 'active'
            || !global.navigator
            || typeof global.navigator.sendBeacon !== 'function'
        ) {
            return false;
        }

        const identity = actor();
        const payload = {
            sessionId: activeSessionId,
            dialogId,
            userId: identity.userId,
            reason: String(reason || 'popup_unload_autosave')
        };
        const blob = new Blob(
            [JSON.stringify(payload)],
            { type: 'application/json' }
        );
        const queued = global.navigator.sendBeacon(
            appUrl('api/checklist/session/commit'),
            blob
        );

        if (queued) {
            setSessionStatus('committing');
            debugLog('popup_edit_session_unload_commit_queued', {
                sessionId: activeSessionId,
                reason: payload.reason
            });
        }

        return queued;
    }

    function noteActivity(activityValue = '') {
        const parsed = new Date(activityValue || Date.now());
        if (Number.isNaN(parsed.getTime())) return lastActivityAt;
        const now = Date.now();
        const clamped = new Date(Math.min(parsed.getTime(), now));
        if (clamped.getTime() > new Date(lastActivityAt).getTime()) {
            lastActivityAt = clamped.toISOString();
        }

        // Throttled immediate sync prevents a real interaction near the
        // 25-minute boundary from racing the server-side fallback.
        if (
            activeSessionId
            && activeSessionStatus === 'active'
            && now - lastActivityHeartbeatAt >= 5000
            && !activityHeartbeatTimer
        ) {
            activityHeartbeatTimer = global.setTimeout(async function () {
                activityHeartbeatTimer = null;
                lastActivityHeartbeatAt = Date.now();
                await heartbeat(true);
            }, 100);
        }
        return lastActivityAt;
    }


    function getLastActivityAt() {
        return lastActivityAt;
    }

    function invalidate(errorText = '') {
        activeSessionId = '';
        setSessionStatus(
            'failed',
            String(errorText || '').trim()
        );
    }

    function getSessionId() {
        return activeSessionId;
    }

    function getStatus() {
        return activeSessionStatus;
    }

    function isActive() {
        return (
            !!activeSessionId
            && activeSessionStatus === 'active'
        );
    }

    function getLastError() {
        return lastSessionError;
    }

    global.ChecklistPopupEditSession = Object.freeze({
        start,
        ensureActive,
        heartbeat,
        commit,
        finalize,
        finalizeOnUnload,
        rollback,
        commitOnUnload,
        invalidate,
        getSessionId,
        getStatus,
        isActive,
        getLastError,
        noteActivity,
        getLastActivityAt
    });
})(window);
