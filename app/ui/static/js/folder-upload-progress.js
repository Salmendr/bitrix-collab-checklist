(function (global) {
    'use strict';

    const core = global.ChecklistFolderCore || {};
    const channel = core.channel || null;
    const region = document.getElementById(
        'folderUploadProgressRegion'
    );
    const cardsElement = document.getElementById(
        'folderUploadProgressCards'
    );
    const visibleLimit = 3;
    const snapshotsByManager = new Map();
    const dismissedIds = new Set();
    const everVisibleIds = new Set();
    const terminalTimers = new Map();
    let localSnapshotProvider = null;
    let requestSequence = 0;

    const localManagerId = (
        'folder-fallback:'
        + (
            channel
                ? channel.sourceId
                : (
                    Date.now().toString(36)
                    + '-'
                    + Math.random().toString(36).slice(2)
                )
        )
    );

    function escapeHtml(value) {
        return String(value ?? '')
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
    }

    function taskIsTerminal(task) {
        return [
            'completed',
            'failed',
            'cancelled'
        ].includes(String(task && task.state || ''));
    }

    function taskStatus(task) {
        const state = String(
            task && task.state || 'queued'
        );
        const replacement = (
            String(
                task
                && task.context
                && task.context.operationKind
                || 'upload'
            ) === 'replacement'
        );

        if (state === 'queued') {
            return replacement
                ? 'Ожидает замены'
                : 'Ожидает загрузки';
        }

        if (state === 'uploading') {
            return replacement
                ? 'Замена файла'
                : 'Загрузка в приложение';
        }

        if (state === 'completed') {
            return replacement
                ? 'Файл заменён'
                : 'Загружено в приложение';
        }

        if (state === 'cancelled') {
            return replacement
                ? 'Замена отменена'
                : 'Загрузка отменена';
        }

        if (state === 'failed') {
            return String(
                task && task.error || 'Ошибка загрузки'
            );
        }

        return 'Подготовка загрузки';
    }

    function taskPercent(task) {
        if (
            String(task && task.state || '')
            === 'completed'
        ) {
            return 100;
        }

        return Math.max(
            0,
            Math.min(
                100,
                Math.round(
                    Number(task && task.progress || 0)
                )
            )
        );
    }

    function normalizeSnapshot(snapshot) {
        const source = (
            snapshot
            && typeof snapshot === 'object'
                ? snapshot
                : {}
        );

        return {
            maxActive: Number(source.maxActive || 9),
            activeCount: Number(source.activeCount || 0),
            queuedCount: Number(source.queuedCount || 0),
            runningCount: Number(source.runningCount || 0),
            completedCount: Number(
                source.completedCount || 0
            ),
            failedCount: Number(source.failedCount || 0),
            cancelledCount: Number(
                source.cancelledCount || 0
            ),
            idle: Boolean(source.idle),
            tasks: Array.isArray(source.tasks)
                ? source.tasks
                : []
        };
    }

    function mergedSnapshot() {
        const tasksById = new Map();
        let maxActive = 9;
        let activeCount = 0;

        for (const snapshot of snapshotsByManager.values()) {
            maxActive = Math.max(
                maxActive,
                Number(snapshot.maxActive || 0)
            );
            activeCount += Number(
                snapshot.activeCount || 0
            );

            snapshot.tasks.forEach(function (task) {
                const uploadId = String(
                    task && task.uploadId || ''
                );

                if (!uploadId) return;

                const existing = tasksById.get(uploadId);
                const oldTime = Number(
                    existing
                    && (
                        existing.finishedAt
                        || existing.startedAt
                        || existing.createdAt
                    )
                    || 0
                );
                const newTime = Number(
                    task
                    && (
                        task.finishedAt
                        || task.startedAt
                        || task.createdAt
                    )
                    || 0
                );

                if (!existing || newTime >= oldTime) {
                    tasksById.set(uploadId, task);
                }
            });
        }

        const tasks = Array.from(tasksById.values())
            .sort(function (left, right) {
                const leftCreated = Number(
                    left && left.createdAt || 0
                );
                const rightCreated = Number(
                    right && right.createdAt || 0
                );

                if (leftCreated !== rightCreated) {
                    return leftCreated - rightCreated;
                }

                return Number(
                    left && left.sequence || 0
                ) - Number(
                    right && right.sequence || 0
                );
            });

        return {
            maxActive,
            activeCount,
            queuedCount: tasks.filter(
                task => task.state === 'queued'
            ).length,
            runningCount: tasks.filter(
                task => task.state === 'uploading'
            ).length,
            completedCount: tasks.filter(
                task => task.state === 'completed'
            ).length,
            failedCount: tasks.filter(
                task => task.state === 'failed'
            ).length,
            cancelledCount: tasks.filter(
                task => task.state === 'cancelled'
            ).length,
            idle: tasks.every(taskIsTerminal),
            tasks
        };
    }

    function scheduleDismiss(task) {
        if (!task || !taskIsTerminal(task)) return;

        const uploadId = String(task.uploadId || '');

        if (!uploadId || terminalTimers.has(uploadId)) {
            return;
        }

        const delay = (
            String(task.state || '') === 'completed'
                ? 900
                : 4200
        );

        terminalTimers.set(
            uploadId,
            global.setTimeout(function () {
                terminalTimers.delete(uploadId);
                dismissedIds.add(uploadId);
                render('terminal-dismissed', task);
            }, delay)
        );
    }

    function getVisibleTasks(snapshot) {
        const visible = [];

        for (const task of snapshot.tasks) {
            const uploadId = String(
                task && task.uploadId || ''
            );

            if (!uploadId || dismissedIds.has(uploadId)) {
                continue;
            }

            if (
                taskIsTerminal(task)
                && !everVisibleIds.has(uploadId)
            ) {
                dismissedIds.add(uploadId);
                continue;
            }

            visible.push(task);

            if (visible.length >= visibleLimit) {
                break;
            }
        }

        return visible;
    }

    function render(reason = '', changedTask = null) {
        if (!cardsElement || !region) return;

        const snapshot = mergedSnapshot();
        const tasks = getVisibleTasks(snapshot);

        cardsElement.innerHTML = tasks.map(
            function (task) {
                const uploadId = String(
                    task && task.uploadId || ''
                );
                const state = String(
                    task && task.state || 'queued'
                );
                const fileName = String(
                    task
                    && task.context
                    && task.context.fileName
                    || 'Файл'
                );
                const percent = taskPercent(task);
                const status = taskStatus(task);

                everVisibleIds.add(uploadId);
                scheduleDismiss(task);

                return `
                    <div
                        class="upload-progress-card is-${escapeHtml(state)}"
                        data-upload-id="${escapeHtml(uploadId)}"
                        title="${escapeHtml(fileName)}"
                    >
                        <div class="upload-progress-card-file">
                            ${escapeHtml(fileName)}
                        </div>
                        <div class="upload-progress-card-track">
                            <div
                                class="upload-progress-card-bar"
                                style="width:${percent}%;"
                            ></div>
                        </div>
                        <div class="upload-progress-card-status">
                            <span title="${escapeHtml(status)}">
                                ${escapeHtml(status)}
                            </span>
                            <b>${percent}%</b>
                        </div>
                    </div>
                `;
            }
        ).join('');

        region.hidden = tasks.length === 0;

        global.folderUploadProgressView = {
            reason: String(reason || ''),
            changedUploadId: String(
                changedTask && changedTask.uploadId || ''
            ),
            visibleUploadIds: tasks.map(
                task => String(task.uploadId || '')
            ),
            managerIds: Array.from(
                snapshotsByManager.keys()
            ),
            visibleLimit,
            snapshot
        };
    }

    function applySnapshot(
        managerId,
        snapshot,
        reason = '',
        changedTask = null
    ) {
        const normalizedId = String(managerId || '').trim();

        if (!normalizedId) return;

        snapshotsByManager.set(
            normalizedId,
            normalizeSnapshot(snapshot)
        );
        render(reason, changedTask);
    }

    function publishLocalSnapshot(
        snapshot,
        reason = 'fallback-state-change',
        changedTask = null
    ) {
        const normalized = normalizeSnapshot(snapshot);

        applySnapshot(
            localManagerId,
            normalized,
            reason,
            changedTask
        );

        if (channel) {
            channel.publish(
                'checklist-upload-progress',
                {
                    managerId: localManagerId,
                    snapshot: normalized,
                    reason: String(reason || ''),
                    changedTask: changedTask || null
                }
            );
        }
    }

    function requestSnapshot() {
        if (!channel) return '';

        requestSequence += 1;

        const requestId = [
            channel.sourceId,
            Date.now(),
            requestSequence
        ].join(':');

        channel.publish(
            'checklist-upload-snapshot-request',
            {
                requestId,
                requesterRole: 'folder'
            }
        );

        return requestId;
    }

    function setLocalSnapshotProvider(provider) {
        localSnapshotProvider = (
            typeof provider === 'function'
                ? provider
                : null
        );

        if (!localSnapshotProvider) return;

        try {
            publishLocalSnapshot(
                localSnapshotProvider(),
                'fallback-provider-ready',
                null
            );
        } catch (error) {
            console.log(
                'folder fallback snapshot error:',
                error
            );
        }
    }

    if (channel) {
        channel.subscribe(
            'checklist-upload-progress',
            function (envelope) {
                const payload = envelope.payload || {};

                applySnapshot(
                    payload.managerId,
                    payload.snapshot,
                    payload.reason || 'channel-progress',
                    payload.changedTask || null
                );
            }
        );

        channel.subscribe(
            'checklist-upload-snapshot-response',
            function (envelope) {
                const payload = envelope.payload || {};

                applySnapshot(
                    payload.managerId,
                    payload.snapshot,
                    payload.reason || 'snapshot-response',
                    null
                );
            }
        );

        channel.subscribe(
            'checklist-upload-snapshot-request',
            function (envelope) {
                if (!localSnapshotProvider) return;

                let snapshot;

                try {
                    snapshot = localSnapshotProvider();
                } catch (error) {
                    return;
                }

                const payload = envelope.payload || {};

                channel.publish(
                    'checklist-upload-snapshot-response',
                    {
                        requestId: String(
                            payload.requestId || ''
                        ),
                        managerId: localManagerId,
                        snapshot: normalizeSnapshot(snapshot),
                        reason: 'folder-fallback-response'
                    },
                    {
                        targetSourceId: String(
                            envelope.sourceId || ''
                        )
                    }
                );
            }
        );

        requestSnapshot();
        global.setTimeout(requestSnapshot, 250);
        global.addEventListener(
            'pageshow',
            requestSnapshot
        );
    }

    global.ChecklistFolderUploadProgress = Object.freeze({
        applySnapshot,
        publishLocalSnapshot,
        requestSnapshot,
        setLocalSnapshotProvider,
        getSnapshot: mergedSnapshot,
        get localManagerId() {
            return localManagerId;
        }
    });
})(window);
