(function (global) {
    'use strict';

    const channelFactory = global.ChecklistWindowChannel;
    const bootstrap = global.CHECKLIST_POPUP_BOOTSTRAP || {};

    if (
        !channelFactory
        || typeof channelFactory.create !== 'function'
    ) {
        throw new Error(
            'ChecklistWindowChannel is not initialized'
        );
    }

    const dialogId = String(bootstrap.dialogId || '').trim();
    const channel = channelFactory.create({
        dialogId,
        role: 'popup'
    });
    const managerId = (
        'popup-upload-manager:'
        + channel.sourceId
    );
    let latestSnapshot = null;

    function cloneSnapshot(snapshot) {
        if (!snapshot || typeof snapshot !== 'object') {
            return {
                maxActive: 9,
                activeCount: 0,
                queuedCount: 0,
                runningCount: 0,
                completedCount: 0,
                failedCount: 0,
                cancelledCount: 0,
                idle: true,
                tasks: []
            };
        }

        return {
            maxActive: Number(snapshot.maxActive || 9),
            activeCount: Number(snapshot.activeCount || 0),
            queuedCount: Number(snapshot.queuedCount || 0),
            runningCount: Number(snapshot.runningCount || 0),
            completedCount: Number(
                snapshot.completedCount || 0
            ),
            failedCount: Number(snapshot.failedCount || 0),
            cancelledCount: Number(
                snapshot.cancelledCount || 0
            ),
            idle: Boolean(snapshot.idle),
            tasks: Array.isArray(snapshot.tasks)
                ? snapshot.tasks
                : []
        };
    }

    function getCurrentSnapshot() {
        const manager = global.popupUploadManager;

        if (
            manager
            && typeof manager.getSnapshot === 'function'
        ) {
            latestSnapshot = cloneSnapshot(
                manager.getSnapshot()
            );
        }

        return cloneSnapshot(latestSnapshot);
    }

    function publishUploadSnapshot(
        snapshot,
        reason = 'state-change',
        changedTask = null
    ) {
        latestSnapshot = cloneSnapshot(snapshot);

        channel.publish(
            'checklist-upload-progress',
            {
                managerId,
                snapshot: latestSnapshot,
                reason: String(reason || ''),
                changedTask: changedTask || null
            }
        );
    }

    channel.subscribe(
        'checklist-upload-snapshot-request',
        function (envelope) {
            const payload = envelope.payload || {};
            const requestId = String(
                payload.requestId || ''
            );

            channel.publish(
                'checklist-upload-snapshot-response',
                {
                    requestId,
                    managerId,
                    snapshot: getCurrentSnapshot(),
                    reason: 'snapshot-response'
                },
                {
                    targetSourceId: String(
                        envelope.sourceId || ''
                    )
                }
            );
        }
    );

    const documentEventTypes = [
        'checklist-document-removed',
        'checklist-document-uploaded',
        'checklist-document-replaced',
        'checklist-document-changed'
    ];

    documentEventTypes.forEach(function (eventType) {
        channel.subscribe(
            eventType,
            function (envelope) {
                const payload = envelope.payload || {};

                global.postMessage(
                    {
                        ...payload,
                        type: eventType,
                        __checklistChannelMessageId: String(
                            envelope.messageId || ''
                        )
                    },
                    global.location.origin
                );
            }
        );
    });

    channel.subscribe(
        'checklist-folder-refresh',
        function (envelope) {
            const payload = envelope.payload || {};

            global.postMessage(
                {
                    ...payload,
                    type: 'checklist-document-changed',
                    changeKind: String(
                        payload.changeKind
                        || 'folder-refresh'
                    ),
                    __checklistChannelMessageId: String(
                        envelope.messageId || ''
                    )
                },
                global.location.origin
            );
        }
    );

    channel.subscribe(
        'checklist-user-activity',
        function (envelope) {
            const payload = envelope.payload || {};
            global.dispatchEvent(new CustomEvent(
                'checklist-folder-user-activity',
                { detail: payload }
            ));
        }
    );

    global.addEventListener(
        'beforeunload',
        function () {
            channel.close();
        },
        {
            once: true
        }
    );

    global.ChecklistPopupWindowChannel = Object.freeze({
        channel,
        managerId,
        publishUploadSnapshot,
        getCurrentSnapshot
    });
})(window);
