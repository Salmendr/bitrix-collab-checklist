'use strict';

const uploadProgressCardsEl = document.getElementById(
    'uploadProgressCards'
);
const POPUP_UPLOAD_VISIBLE_LIMIT = 3;
const uploadProgressDismissedIds = new Set();
const uploadProgressEverVisibleIds = new Set();
const uploadProgressTerminalTimers = new Map();
const uploadProgressSnapshotsByManager = new Map();

function popupUploadProgressLocalManagerId() {
    return String(
        window.ChecklistPopupWindowChannel
        && window.ChecklistPopupWindowChannel.managerId
        || 'popup-local'
    );
}

function normalizeUploadProgressSnapshot(snapshot) {
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

function uploadProgressTaskIsTerminal(task) {
    return [
        'completed',
        'failed',
        'cancelled'
    ].includes(String(task && task.state || ''));
}

function mergeUploadProgressSnapshots() {
    const tasksById = new Map();
    let maxActive = 9;
    let activeCount = 0;

    for (
        const snapshot
        of uploadProgressSnapshotsByManager.values()
    ) {
        maxActive = Math.max(
            maxActive,
            Number(snapshot.maxActive || 0)
        );
        activeCount += Number(snapshot.activeCount || 0);

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
        idle: tasks.every(uploadProgressTaskIsTerminal),
        tasks
    };
}

function uploadProgressTaskStatus(task) {
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
        const retryAt = Number(
            task && task.retryAt || 0
        );
        const errorText = String(
            task && task.error || ''
        ).trim();

        if (retryAt > Date.now() && errorText) {
            return 'Повторяем после восстановления связи';
        }

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

function uploadProgressTaskPercent(task) {
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

function scheduleUploadProgressDismiss(task) {
    if (
        !task
        || !uploadProgressTaskIsTerminal(task)
    ) {
        return;
    }

    const uploadId = String(task.uploadId || '');

    if (
        !uploadId
        || uploadProgressTerminalTimers.has(uploadId)
    ) {
        return;
    }

    const delay = (
        String(task.state || '') === 'completed'
            ? 900
            : 4200
    );

    uploadProgressTerminalTimers.set(
        uploadId,
        setTimeout(function () {
            uploadProgressTerminalTimers.delete(uploadId);
            uploadProgressDismissedIds.add(uploadId);

            renderUploadProgressCards(
                null,
                'terminal-dismissed',
                task
            );
        }, delay)
    );
}

function getVisibleUploadProgressTasks(snapshot) {
    const sourceTasks = Array.isArray(
        snapshot && snapshot.tasks
    )
        ? snapshot.tasks
        : [];
    const visible = [];

    for (const task of sourceTasks) {
        const uploadId = String(
            task && task.uploadId || ''
        );

        if (
            !uploadId
            || uploadProgressDismissedIds.has(uploadId)
        ) {
            continue;
        }

        if (
            uploadProgressTaskIsTerminal(task)
            && !uploadProgressEverVisibleIds.has(uploadId)
        ) {
            uploadProgressDismissedIds.add(uploadId);
            continue;
        }

        visible.push(task);

        if (
            visible.length
            >= POPUP_UPLOAD_VISIBLE_LIMIT
        ) {
            break;
        }
    }

    return visible;
}

function renderUploadProgressCards(
    snapshot,
    reason,
    changedTask
) {
    if (!uploadProgressCardsEl) return;

    if (snapshot && typeof snapshot === 'object') {
        uploadProgressSnapshotsByManager.set(
            popupUploadProgressLocalManagerId(),
            normalizeUploadProgressSnapshot(snapshot)
        );
    }

    const mergedSnapshot = mergeUploadProgressSnapshots();
    const visibleTasks = getVisibleUploadProgressTasks(
        mergedSnapshot
    );

    uploadProgressCardsEl.innerHTML = visibleTasks.map(
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
            const percent = uploadProgressTaskPercent(
                task
            );
            const statusText = uploadProgressTaskStatus(
                task
            );

            uploadProgressEverVisibleIds.add(uploadId);
            scheduleUploadProgressDismiss(task);

            return `
                <div
                    class="upload-progress-card is-${esc(state)}"
                    data-upload-id="${esc(uploadId)}"
                    title="${esc(fileName)}"
                >
                    <div class="upload-progress-card-file">
                        ${esc(fileName)}
                    </div>
                    <div class="upload-progress-card-track">
                        <div
                            class="upload-progress-card-bar"
                            style="width:${percent}%;"
                        ></div>
                    </div>
                    <div class="upload-progress-card-status">
                        <span title="${esc(statusText)}">
                            ${esc(statusText)}
                        </span>
                        <b>${percent}%</b>
                    </div>
                </div>
            `;
        }
    ).join('');

    window.popupUploadProgressView = {
        reason: String(reason || ''),
        changedUploadId: String(
            changedTask && changedTask.uploadId || ''
        ),
        visibleUploadIds: visibleTasks.map(
            task => String(task.uploadId || '')
        ),
        managerIds: Array.from(
            uploadProgressSnapshotsByManager.keys()
        ),
        visibleLimit: POPUP_UPLOAD_VISIBLE_LIMIT,
        snapshot: mergedSnapshot
    };
}

if (
    window.ChecklistPopupWindowChannel
    && window.ChecklistPopupWindowChannel.channel
) {
    window.ChecklistPopupWindowChannel.channel.subscribe(
        'checklist-upload-progress',
        function (envelope) {
            const payload = envelope.payload || {};
            const managerId = String(
                payload.managerId || ''
            );

            if (
                !managerId
                || managerId
                    === popupUploadProgressLocalManagerId()
            ) {
                return;
            }

            uploadProgressSnapshotsByManager.set(
                managerId,
                normalizeUploadProgressSnapshot(
                    payload.snapshot
                )
            );

            renderUploadProgressCards(
                null,
                payload.reason || 'channel-progress',
                payload.changedTask || null
            );
        }
    );
}

window.ChecklistPopupUploadProgress = Object.freeze({
    render: renderUploadProgressCards,
    getSnapshot: mergeUploadProgressSnapshots
});

// Совместимость со старым кодом фонового Yandex polling.
function clearUploadProgressHideTimer() {}
function setUploadProgressVisible(visible) {
    void visible;
}
function updateUploadProgress(
    fileName,
    percent,
    statusText
) {
    void fileName;
    void percent;
    void statusText;
}
function completeUploadProgress(
    fileName,
    statusText
) {
    void fileName;
    void statusText;
}
function failUploadProgress(
    fileName,
    statusText
) {
    void fileName;
    void statusText;
}
