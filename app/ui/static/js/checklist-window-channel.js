(function (global) {
    'use strict';

    const ENVELOPE_MARKER = 'checklist-window-channel-v1';
    const CHANNEL_PREFIX = 'bitrix-checklist';
    const DEFAULT_EVENT_TTL_MS = 120000;

    function randomId(prefix) {
        const cryptoApi = global.crypto;
        let suffix = '';

        if (
            cryptoApi
            && typeof cryptoApi.randomUUID === 'function'
        ) {
            suffix = cryptoApi.randomUUID();
        } else {
            suffix = (
                Date.now().toString(36)
                + '-'
                + Math.random().toString(36).slice(2)
            );
        }

        return String(prefix || 'channel') + '-' + suffix;
    }

    function safeOrigin() {
        try {
            return String(
                global.location
                && global.location.origin
                || '*'
            );
        } catch (error) {
            return '*';
        }
    }

    function create(options = {}) {
        const dialogId = String(options.dialogId || '').trim();
        const role = String(options.role || 'window').trim();
        const sourceId = String(
            options.sourceId
            || randomId(role || 'window')
        );
        const channelName = [
            CHANNEL_PREFIX,
            encodeURIComponent(dialogId || 'unknown')
        ].join(':');
        const origin = safeOrigin();
        const listeners = new Map();
        const seenMessageIds = new Map();
        const peerWindows = new Set();
        let closed = false;

        const broadcast = (
            typeof global.BroadcastChannel === 'function'
                ? new global.BroadcastChannel(channelName)
                : null
        );

        function cleanupSeenMessages() {
            const now = Date.now();

            for (const [messageId, createdAt] of seenMessageIds) {
                if (now - createdAt > DEFAULT_EVENT_TTL_MS) {
                    seenMessageIds.delete(messageId);
                }
            }
        }

        function isEnvelope(value) {
            return Boolean(
                value
                && typeof value === 'object'
                && value.marker === ENVELOPE_MARKER
                && value.channelName === channelName
                && String(value.dialogId || '') === dialogId
                && value.messageId
                && value.type
            );
        }

        function rememberPeer(sourceWindow) {
            if (
                !sourceWindow
                || sourceWindow === global
                || typeof sourceWindow.postMessage !== 'function'
            ) {
                return;
            }

            peerWindows.add(sourceWindow);
        }

        function invokeListeners(envelope, transport) {
            const targetSourceId = String(
                envelope.targetSourceId || ''
            );

            if (
                targetSourceId
                && targetSourceId !== sourceId
            ) {
                return;
            }

            const exactListeners = listeners.get(
                String(envelope.type || '')
            );
            const wildcardListeners = listeners.get('*');
            const callbacks = [];

            if (exactListeners) {
                callbacks.push(...exactListeners);
            }

            if (wildcardListeners) {
                callbacks.push(...wildcardListeners);
            }

            callbacks.forEach(function (callback) {
                try {
                    callback(envelope, transport);
                } catch (error) {
                    console.log(
                        'checklist channel listener error:',
                        error
                    );
                }
            });
        }

        function receiveEnvelope(
            envelope,
            transport = 'unknown',
            sourceWindow = null
        ) {
            if (closed || !isEnvelope(envelope)) {
                return false;
            }

            rememberPeer(sourceWindow);
            cleanupSeenMessages();

            const messageId = String(envelope.messageId || '');

            if (seenMessageIds.has(messageId)) {
                return false;
            }

            seenMessageIds.set(messageId, Date.now());

            if (String(envelope.sourceId || '') === sourceId) {
                return false;
            }

            invokeListeners(envelope, transport);
            return true;
        }

        function postToWindow(targetWindow, envelope) {
            try {
                if (
                    !targetWindow
                    || targetWindow === global
                    || typeof targetWindow.postMessage !== 'function'
                ) {
                    return false;
                }

                targetWindow.postMessage(
                    envelope,
                    origin === 'null' ? '*' : origin
                );

                return true;
            } catch (error) {
                return false;
            }
        }

        function publish(
            type,
            payload = {},
            publishOptions = {}
        ) {
            if (closed) {
                return '';
            }

            const normalizedType = String(type || '').trim();

            if (!normalizedType) {
                throw new Error(
                    'checklist channel event type is required'
                );
            }

            const envelope = {
                marker: ENVELOPE_MARKER,
                channelName,
                dialogId,
                messageId: randomId('message'),
                sourceId,
                sourceRole: role,
                targetSourceId: String(
                    publishOptions.targetSourceId || ''
                ),
                type: normalizedType,
                createdAt: Date.now(),
                payload: (
                    payload
                    && typeof payload === 'object'
                        ? payload
                        : {}
                )
            };

            seenMessageIds.set(
                envelope.messageId,
                envelope.createdAt
            );

            if (
                publishOptions.deliverLocally === true
            ) {
                invokeListeners(envelope, 'local');
            }

            if (broadcast) {
                try {
                    broadcast.postMessage(envelope);
                } catch (error) {
                    console.log(
                        'BroadcastChannel publish error:',
                        error
                    );
                }
            }

            try {
                if (
                    global.opener
                    && global.opener !== global
                ) {
                    postToWindow(global.opener, envelope);
                }
            } catch (error) {
                void error;
            }

            for (const peerWindow of Array.from(peerWindows)) {
                if (!postToWindow(peerWindow, envelope)) {
                    peerWindows.delete(peerWindow);
                }
            }

            return envelope.messageId;
        }

        function subscribe(type, callback) {
            const normalizedType = String(type || '').trim();

            if (
                !normalizedType
                || typeof callback !== 'function'
            ) {
                return function () {};
            }

            if (!listeners.has(normalizedType)) {
                listeners.set(normalizedType, new Set());
            }

            listeners.get(normalizedType).add(callback);

            return function unsubscribe() {
                const target = listeners.get(normalizedType);

                if (!target) {
                    return;
                }

                target.delete(callback);

                if (!target.size) {
                    listeners.delete(normalizedType);
                }
            };
        }

        function handleWindowMessage(event) {
            const eventOrigin = String(
                event && event.origin || ''
            );

            if (
                origin !== '*'
                && origin !== 'null'
                && eventOrigin
                && eventOrigin !== origin
            ) {
                return;
            }

            receiveEnvelope(
                event && event.data,
                'postMessage',
                event && event.source
            );
        }

        function handleBroadcastMessage(event) {
            receiveEnvelope(
                event && event.data,
                'broadcast',
                null
            );
        }

        function close() {
            if (closed) {
                return;
            }

            closed = true;
            listeners.clear();
            peerWindows.clear();
            seenMessageIds.clear();

            global.removeEventListener(
                'message',
                handleWindowMessage
            );

            if (broadcast) {
                broadcast.removeEventListener(
                    'message',
                    handleBroadcastMessage
                );
                broadcast.close();
            }
        }

        global.addEventListener(
            'message',
            handleWindowMessage
        );

        if (broadcast) {
            broadcast.addEventListener(
                'message',
                handleBroadcastMessage
            );
        }

        return Object.freeze({
            publish,
            subscribe,
            close,
            get sourceId() {
                return sourceId;
            },
            get role() {
                return role;
            },
            get dialogId() {
                return dialogId;
            },
            get channelName() {
                return channelName;
            },
            get broadcastAvailable() {
                return Boolean(broadcast);
            }
        });
    }

    global.ChecklistWindowChannel = Object.freeze({
        create,
        marker: ENVELOPE_MARKER
    });
})(window);
