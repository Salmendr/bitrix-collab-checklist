(function (global) {
    'use strict';

    const HISTORY_STATE_KEY = '__checklistPopupClientSessionId';
    const SESSION_STORAGE_KEY = 'checklist_popup_client_session_id';
    const CHANNEL_NAME = 'checklist-popup-client-session-claim-v1';

    function createId(prefix = 'popup') {
        const randomPart = (
            global.crypto
            && typeof global.crypto.randomUUID === 'function'
        )
            ? global.crypto.randomUUID().replaceAll('-', '')
            : Math.random().toString(36).slice(2, 12);

        return prefix + '_' + Date.now() + '_' + randomPart;
    }

    function readHistoryId() {
        try {
            const state = global.history && global.history.state;
            if (!state || typeof state !== 'object') {
                return '';
            }
            return String(state[HISTORY_STATE_KEY] || '').trim();
        } catch (error) {
            return '';
        }
    }

    function writeHistoryId(value) {
        try {
            if (
                !global.history
                || typeof global.history.replaceState !== 'function'
            ) {
                return false;
            }

            const currentState = global.history.state;
            if (
                currentState !== null
                && currentState !== undefined
                && typeof currentState !== 'object'
            ) {
                return false;
            }

            const nextState = currentState
                ? Object.assign({}, currentState)
                : {};
            nextState[HISTORY_STATE_KEY] = value;
            global.history.replaceState(nextState, '');
            return true;
        } catch (error) {
            return false;
        }
    }

    function readStorageId() {
        try {
            return String(
                global.sessionStorage
                && global.sessionStorage.getItem(SESSION_STORAGE_KEY)
                || ''
            ).trim();
        } catch (error) {
            return '';
        }
    }

    function writeStorageId(value) {
        try {
            if (global.sessionStorage) {
                global.sessionStorage.setItem(
                    SESSION_STORAGE_KEY,
                    value
                );
            }
        } catch (error) {
            // sessionStorage can be unavailable in a restricted iframe.
        }
    }

    function resolveInitialId() {
        const historyId = readHistoryId();
        if (historyId) {
            writeStorageId(historyId);
            return historyId;
        }

        const createdId = createId();
        if (writeHistoryId(createdId)) {
            writeStorageId(createdId);
            return createdId;
        }

        const storageId = readStorageId();
        if (storageId) {
            return storageId;
        }

        writeStorageId(createdId);
        return createdId;
    }

    let currentId = resolveInitialId();
    const instanceId = createId('instance');
    let channel = null;
    let readyResolved = false;
    let resolveReady = null;

    const ready = new Promise(resolve => {
        resolveReady = resolve;
    });

    function finishReady() {
        if (readyResolved) return;
        readyResolved = true;
        resolveReady(currentId);
    }

    function replaceCurrentId() {
        currentId = createId();
        writeHistoryId(currentId);
        writeStorageId(currentId);
    }

    if (typeof global.BroadcastChannel !== 'function') {
        finishReady();
    } else {
        try {
            channel = new global.BroadcastChannel(CHANNEL_NAME);

            channel.onmessage = function (event) {
                const message = event && event.data || {};

                if (
                    message.type === 'probe'
                    && message.id === currentId
                    && message.instanceId !== instanceId
                ) {
                    channel.postMessage({
                        type: 'occupied',
                        id: currentId,
                        targetInstanceId: message.instanceId,
                        responderInstanceId: instanceId
                    });
                    return;
                }

                if (
                    message.type === 'occupied'
                    && message.targetInstanceId === instanceId
                    && message.id === currentId
                    && !readyResolved
                ) {
                    replaceCurrentId();
                    finishReady();
                }
            };

            channel.postMessage({
                type: 'probe',
                id: currentId,
                instanceId
            });

            global.setTimeout(finishReady, 120);
        } catch (error) {
            channel = null;
            finishReady();
        }
    }

    global.addEventListener('pagehide', function () {
        if (channel) channel.close();
    }, { once: true });

    global.ChecklistPopupClientSession = Object.freeze({
        get id() {
            return currentId;
        },
        ready,
        historyStateKey: HISTORY_STATE_KEY,
        storageKey: SESSION_STORAGE_KEY,
        channelName: CHANNEL_NAME,
        instanceId
    });
})(window);
