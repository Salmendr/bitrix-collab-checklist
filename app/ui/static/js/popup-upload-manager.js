const POPUP_UPLOAD_MAX_ACTIVE = 9;
const POPUP_UPLOAD_PROGRESS_EMIT_MS = 140;
const POPUP_UPLOAD_MAX_ATTEMPTS = 12;

function createPopupUploadManager(options = {}) {
    const configuredMaxActive = Number(
        options.maxActive || POPUP_UPLOAD_MAX_ACTIVE
    );
    const maxActive = Math.max(
        1,
        Number.isFinite(configuredMaxActive)
            ? Math.floor(configuredMaxActive)
            : POPUP_UPLOAD_MAX_ACTIVE
    );
    const configuredMaxAttempts = Number(
        options.maxAttempts || POPUP_UPLOAD_MAX_ATTEMPTS
    );
    const maxAttempts = Math.max(
        1,
        Number.isFinite(configuredMaxAttempts)
            ? Math.floor(configuredMaxAttempts)
            : POPUP_UPLOAD_MAX_ATTEMPTS
    );
    const onStateChange = typeof options.onStateChange === 'function'
        ? options.onStateChange
        : function () {};
    const retryDelays = (
        Array.isArray(options.retryDelays)
        && options.retryDelays.length
            ? options.retryDelays.map(value => Math.max(
                0,
                Math.round(Number(value || 0))
            ))
            : [1000, 1800, 3000, 5000, 8000, 12000, 15000]
    );

    const queuedTasks = [];
    const tasksById = new Map();
    const activeQueueKeyCounts = new Map();
    const idleWaiters = [];
    let activeCount = 0;
    let sequence = 0;
    let pumpScheduled = false;
    let pumpTimer = null;
    let pauseUntil = 0;
    let deferredStateTimer = null;
    let deferredStateReason = '';
    let deferredStateTask = null;

    function queueKeyForContext(source) {
        const explicitKey = String(
            source && source.queueKey || ''
        ).trim();

        if (explicitKey) {
            return explicitKey;
        }

        return [
            String(source && source.dialogId || ''),
            String(source && source.checklistKey || ''),
            String(source && source.itemId || '')
        ].join('::');
    }

    function clonePublicContext(context) {
        const source = context && typeof context === 'object'
            ? context
            : {};

        return Object.freeze({
            uploadId: String(source.uploadId || ''),
            dialogId: String(source.dialogId || ''),
            checklistKey: String(source.checklistKey || ''),
            itemId: String(source.itemId || ''),
            itemGroup: String(source.itemGroup || ''),
            itemName: String(source.itemName || ''),
            initialStatus: String(source.initialStatus || ''),
            fileName: String(source.fileName || ''),
            fileSize: Number(source.fileSize || 0),
            fileType: String(source.fileType || ''),
            source: String(source.source || 'popup'),
            operationKind: String(
                source.operationKind || 'upload'
            ),
            documentId: String(source.documentId || ''),
            documentName: String(source.documentName || ''),
            sessionId: String(source.sessionId || '').trim(),
            queueKey: queueKeyForContext(source),
            queueLimit: Math.max(
                1,
                Math.floor(Number(
                    source.queueLimit
                    || (
                        String(
                            source.operationKind || 'upload'
                        ) === 'replacement'
                            ? 1
                            : maxActive
                    )
                ))
            )
        });
    }

    function publicTask(task) {
        return {
            uploadId: task.uploadId,
            sequence: task.sequence,
            state: task.state,
            progress: task.progress,
            createdAt: task.createdAt,
            startedAt: task.startedAt,
            finishedAt: task.finishedAt,
            retryAt: task.retryAt,
            attempt: task.attempt,
            maxAttempts: task.maxAttempts,
            error: task.error,
            context: task.context
        };
    }

    function snapshot() {
        const tasks = Array.from(tasksById.values())
            .sort((left, right) => left.sequence - right.sequence)
            .map(publicTask);

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
            paused: Date.now() < pauseUntil,
            pauseUntil,
            idle: activeCount === 0 && queuedTasks.length === 0,
            tasks
        };
    }

    function deliverStateChange(reason, task = null) {
        try {
            onStateChange(
                snapshot(),
                reason,
                task ? publicTask(task) : null
            );
        } catch (error) {
            console.log(
                'upload manager state callback error:',
                error
            );
        }
    }

    function flushDeferredStateChange() {
        if (deferredStateTimer) {
            clearTimeout(deferredStateTimer);
            deferredStateTimer = null;
        }

        if (!deferredStateReason) {
            return;
        }

        const reason = deferredStateReason;
        const task = deferredStateTask;
        deferredStateReason = '';
        deferredStateTask = null;
        deliverStateChange(reason, task);
    }

    function emitStateChange(reason, task = null) {
        const deferred = (
            reason === 'progress'
            || reason === 'queued'
        );

        if (!deferred) {
            flushDeferredStateChange();
            deliverStateChange(reason, task);
            return;
        }

        deferredStateReason = reason;
        deferredStateTask = task;

        if (deferredStateTimer) {
            return;
        }

        deferredStateTimer = setTimeout(
            flushDeferredStateChange,
            reason === 'queued'
                ? 0
                : POPUP_UPLOAD_PROGRESS_EMIT_MS
        );
    }

    function resolveIdleWaitersIfNeeded() {
        if (activeCount !== 0 || queuedTasks.length !== 0) {
            return;
        }

        const currentSnapshot = snapshot();

        while (idleWaiters.length) {
            const resolve = idleWaiters.shift();
            resolve(currentSnapshot);
        }
    }

    function releaseTaskSlot(task) {
        activeCount = Math.max(0, activeCount - 1);

        const currentCount = Number(
            activeQueueKeyCounts.get(task.queueKey) || 0
        );

        if (currentCount <= 1) {
            activeQueueKeyCounts.delete(task.queueKey);
        } else {
            activeQueueKeyCounts.set(
                task.queueKey,
                currentCount - 1
            );
        }
    }

    function finishTask(task, state, result, error) {
        task.state = state;
        task.finishedAt = Date.now();
        task.retryAt = 0;
        task.result = result;
        task.error = error
            ? String(error && error.message || error)
            : '';

        releaseTaskSlot(task);

        if (state === 'completed') {
            task.resolve(result);
        } else {
            task.reject(
                error instanceof Error
                    ? error
                    : new Error(task.error || 'upload failed')
            );
        }

        emitStateChange('finished', task);
        schedulePump();
        resolveIdleWaitersIfNeeded();
    }

    function retryDelayMs(attempt) {
        return retryDelays[Math.min(
            Math.max(0, Number(attempt || 1) - 1),
            retryDelays.length - 1
        )];
    }

    function shouldRetryTask(task, error) {
        return Boolean(
            error
            && error.retryable === true
            && task.attempt < task.maxAttempts
            && task.cancelRequested !== true
        );
    }

    function requeueTask(task, error) {
        const delay = retryDelayMs(task.attempt);
        const retryAt = Date.now() + delay;

        releaseTaskSlot(task);
        task.state = 'queued';
        task.progress = 0;
        task.startedAt = 0;
        task.finishedAt = 0;
        task.retryAt = retryAt;
        task.error = String(
            error && error.message
            || 'Связь прервана. Повторяем загрузку.'
        );

        pauseUntil = Math.max(pauseUntil, retryAt);
        queuedTasks.push(task);
        emitStateChange('retry-scheduled', task);
        schedulePump(delay);
    }

    function runTask(task) {
        task.state = 'uploading';
        task.startedAt = Date.now();
        task.retryAt = 0;
        task.attempt += 1;
        task.error = '';
        activeCount += 1;
        activeQueueKeyCounts.set(
            task.queueKey,
            Number(
                activeQueueKeyCounts.get(task.queueKey) || 0
            ) + 1
        );

        emitStateChange('started', task);

        Promise.resolve()
            .then(function () {
                return task.runner(task.context, {
                    attempt: task.attempt,
                    maxAttempts: task.maxAttempts,
                    updateProgress(percent) {
                        const safePercent = Math.max(
                            0,
                            Math.min(
                                100,
                                Math.round(Number(percent || 0))
                            )
                        );

                        task.progress = safePercent;
                        emitStateChange('progress', task);
                    }
                });
            })
            .then(function (result) {
                task.progress = 100;
                finishTask(task, 'completed', result, null);
            })
            .catch(function (error) {
                if (shouldRetryTask(task, error)) {
                    requeueTask(task, error);
                    return;
                }

                finishTask(task, 'failed', null, error);
            });
    }

    function nextRunnableTaskIndex(now) {
        let earliestRetryAt = 0;

        for (let index = 0; index < queuedTasks.length; index += 1) {
            const task = queuedTasks[index];

            if (!task || task.state !== 'queued') {
                continue;
            }

            if (task.retryAt && task.retryAt > now) {
                earliestRetryAt = (
                    !earliestRetryAt
                    || task.retryAt < earliestRetryAt
                )
                    ? task.retryAt
                    : earliestRetryAt;
                continue;
            }

            const activeForKey = Number(
                activeQueueKeyCounts.get(task.queueKey) || 0
            );

            if (activeForKey >= task.queueLimit) {
                continue;
            }

            return {
                index,
                earliestRetryAt
            };
        }

        return {
            index: -1,
            earliestRetryAt
        };
    }

    function pump() {
        pumpScheduled = false;

        if (pumpTimer) {
            clearTimeout(pumpTimer);
            pumpTimer = null;
        }

        const now = Date.now();

        if (pauseUntil > now) {
            schedulePump(pauseUntil - now);
            return;
        }

        pauseUntil = 0;
        let nextRetryAt = 0;

        while (
            activeCount < maxActive
            && queuedTasks.length > 0
        ) {
            const next = nextRunnableTaskIndex(Date.now());
            nextRetryAt = next.earliestRetryAt || nextRetryAt;

            if (next.index < 0) {
                break;
            }

            const task = queuedTasks.splice(
                next.index,
                1
            )[0];

            if (!task || task.state !== 'queued') {
                continue;
            }

            runTask(task);
        }

        if (
            activeCount < maxActive
            && queuedTasks.length > 0
            && nextRetryAt > Date.now()
        ) {
            schedulePump(nextRetryAt - Date.now());
        }

        resolveIdleWaitersIfNeeded();
    }

    function schedulePump(delayMs = 0) {
        const safeDelay = Math.max(
            0,
            Math.round(Number(delayMs || 0))
        );

        if (safeDelay > 0) {
            if (pumpTimer) {
                return;
            }

            pumpTimer = setTimeout(function () {
                pumpTimer = null;
                schedulePump();
            }, safeDelay);
            return;
        }

        if (pumpScheduled) {
            return;
        }

        pumpScheduled = true;
        Promise.resolve().then(pump);
    }

    function enqueue(context, runner) {
        if (typeof runner !== 'function') {
            return Promise.reject(
                new Error('upload runner is required')
            );
        }

        sequence += 1;

        const publicContext = clonePublicContext(context);
        const uploadId = String(
            publicContext.uploadId
            || 'upload_' + Date.now() + '_' + sequence
        );

        if (tasksById.has(uploadId)) {
            return Promise.reject(
                new Error('duplicate uploadId: ' + uploadId)
            );
        }

        let resolveTask;
        let rejectTask;

        const promise = new Promise(function (resolve, reject) {
            resolveTask = resolve;
            rejectTask = reject;
        });

        const task = {
            uploadId,
            sequence,
            queueKey: publicContext.queueKey,
            queueLimit: publicContext.queueLimit,
            state: 'queued',
            progress: 0,
            createdAt: Date.now(),
            startedAt: 0,
            finishedAt: 0,
            retryAt: 0,
            attempt: 0,
            maxAttempts,
            cancelRequested: false,
            result: null,
            error: '',
            context: Object.freeze({
                ...publicContext,
                uploadId
            }),
            runner,
            resolve: resolveTask,
            reject: rejectTask,
            promise
        };

        tasksById.set(uploadId, task);
        queuedTasks.push(task);

        emitStateChange('queued', task);
        schedulePump();

        return promise;
    }

    function cancelQueued(uploadId, reason = 'upload cancelled') {
        const targetId = String(uploadId || '');
        const task = tasksById.get(targetId);

        if (!task || task.state !== 'queued') {
            return false;
        }

        const queueIndex = queuedTasks.indexOf(task);
        if (queueIndex >= 0) {
            queuedTasks.splice(queueIndex, 1);
        }

        task.cancelRequested = true;
        task.state = 'cancelled';
        task.finishedAt = Date.now();
        task.retryAt = 0;
        task.error = String(reason || 'upload cancelled');

        task.reject(new Error(task.error));

        emitStateChange('cancelled', task);
        schedulePump();
        resolveIdleWaitersIfNeeded();

        return true;
    }

    function cancelAllQueued(reason = 'upload cancelled') {
        const queuedIds = queuedTasks
            .filter(task => task && task.state === 'queued')
            .map(task => task.uploadId);

        let cancelledCount = 0;

        queuedIds.forEach(function (uploadId) {
            if (cancelQueued(uploadId, reason)) {
                cancelledCount += 1;
            }
        });

        return cancelledCount;
    }

    function cancelAll(reason = 'upload cancelled') {
        const errorText = String(
            reason || 'upload cancelled'
        );

        for (const task of tasksById.values()) {
            if (task.state === 'uploading') {
                task.cancelRequested = true;
                task.error = errorText;
            }
        }

        return cancelAllQueued(errorText);
    }

    function updateProgress(uploadId, percent) {
        const targetId = String(uploadId || '');
        const task = tasksById.get(targetId);

        if (!task || task.state !== 'uploading') {
            return false;
        }

        task.progress = Math.max(
            0,
            Math.min(
                100,
                Math.round(Number(percent || 0))
            )
        );

        emitStateChange('progress', task);
        return true;
    }

    function getTask(uploadId) {
        const task = tasksById.get(String(uploadId || ''));
        return task ? publicTask(task) : null;
    }

    function getSnapshot() {
        return snapshot();
    }

    function hasPending() {
        return activeCount > 0 || queuedTasks.length > 0;
    }

    function waitForIdle() {
        if (!hasPending()) {
            return Promise.resolve(snapshot());
        }

        return new Promise(function (resolve) {
            idleWaiters.push(resolve);
        });
    }

    return Object.freeze({
        enqueue,
        cancelQueued,
        cancelAllQueued,
        cancelAll,
        updateProgress,
        getTask,
        getSnapshot,
        hasPending,
        waitForIdle,
        get maxActive() {
            return maxActive;
        }
    });
}

const popupUploadManager = createPopupUploadManager({
    maxActive: POPUP_UPLOAD_MAX_ACTIVE,
    maxAttempts: POPUP_UPLOAD_MAX_ATTEMPTS,
    onStateChange(snapshot, reason, task) {
        if (typeof renderUploadProgressCards === 'function') {
            renderUploadProgressCards(snapshot, reason, task);
        }

        if (
            typeof debugLog === 'function'
            && reason !== 'progress'
        ) {
            debugLog('upload_manager_state_changed', {
                reason,
                uploadId: String(task && task.uploadId || ''),
                taskState: String(task && task.state || ''),
                attempt: Number(task && task.attempt || 0),
                activeCount: snapshot.activeCount,
                queuedCount: snapshot.queuedCount,
                runningCount: snapshot.runningCount,
                completedCount: snapshot.completedCount,
                failedCount: snapshot.failedCount,
                paused: !!snapshot.paused,
                idle: snapshot.idle
            });
        }

        if (
            window.ChecklistPopupWindowChannel
            && typeof window
                .ChecklistPopupWindowChannel
                .publishUploadSnapshot
                === 'function'
        ) {
            window.ChecklistPopupWindowChannel
                .publishUploadSnapshot(
                    snapshot,
                    reason,
                    task
                );
        }
    }
});

window.popupUploadManager = popupUploadManager;

if (
    window.ChecklistPopupWindowChannel
    && typeof window
        .ChecklistPopupWindowChannel
        .publishUploadSnapshot
        === 'function'
) {
    window.ChecklistPopupWindowChannel
        .publishUploadSnapshot(
            popupUploadManager.getSnapshot(),
            'manager-ready',
            null
        );
}
