(function (global) {
    'use strict';

    const folderRemoveApiUrl = global.folderRemoveApiUrl;
    const folderUploadApiUrl = global.folderUploadApiUrl;
    const folderReplaceApiUrl = global.folderReplaceApiUrl;
    const folderDocumentMirrorStatusApiUrl = (
        global.folderDocumentMirrorStatusApiUrl
    );
    const folderDialogId = global.folderDialogId;
    const folderChecklistKey = global.folderChecklistKey;
    const folderItemId = global.folderItemId;
    const folderItemGroup = global.folderItemGroup;
    const folderItemName = global.folderItemName;
    const folderDeleteAllowedUserIds = (
        global.folderDeleteAllowedUserIds
    );
    const getFolderDeleteActor = global.getFolderDeleteActor;
    const notifyParentChecklistDocumentChanged = (
        global.notifyParentChecklistDocumentChanged
    );
    const actionIcons = global.ChecklistActionIcons;

    let folderUploadSequence = 0;

    function createFolderFallbackUploadManager(
        maxActive = 9,
        onStateChange = function () {}
    ) {
        const queue = [];
        const tasksById = new Map();
        let activeCount = 0;
        let sequence = 0;
        let pumpScheduled = false;

        function publicTask(task) {
            return {
                uploadId: task.uploadId,
                sequence: task.sequence,
                state: task.state,
                progress: task.progress,
                createdAt: task.createdAt,
                startedAt: task.startedAt,
                finishedAt: task.finishedAt,
                error: task.error,
                context: task.context
            };
        }

        function snapshot() {
            const tasks = Array.from(
                tasksById.values()
            )
                .sort(
                    (left, right) => (
                        left.sequence - right.sequence
                    )
                )
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
                cancelledCount: 0,
                idle: (
                    activeCount === 0
                    && queue.length === 0
                ),
                tasks
            };
        }

        function emit(reason, task = null) {
            try {
                onStateChange(
                    snapshot(),
                    reason,
                    task ? publicTask(task) : null
                );
            } catch (error) {
                console.log(
                    'folder fallback state callback error:',
                    error
                );
            }
        }

        function finish(
            task,
            state,
            result,
            error
        ) {
            task.state = state;
            task.finishedAt = Date.now();
            task.error = error
                ? String(
                    error && error.message || error
                )
                : '';

            activeCount = Math.max(
                0,
                activeCount - 1
            );

            if (state === 'completed') {
                task.resolve(result);
            } else {
                task.reject(
                    error instanceof Error
                        ? error
                        : new Error(
                            task.error || 'upload failed'
                        )
                );
            }

            emit('finished', task);
            schedulePump();
        }

        function run(task) {
            task.state = 'uploading';
            task.startedAt = Date.now();
            activeCount += 1;
            emit('started', task);

            Promise.resolve()
                .then(function () {
                    return task.runner(
                        task.context,
                        {
                            updateProgress(percent) {
                                task.progress = Math.max(
                                    0,
                                    Math.min(
                                        100,
                                        Math.round(
                                            Number(
                                                percent || 0
                                            )
                                        )
                                    )
                                );
                                emit('progress', task);
                            }
                        }
                    );
                })
                .then(function (result) {
                    task.progress = 100;
                    finish(
                        task,
                        'completed',
                        result,
                        null
                    );
                })
                .catch(function (error) {
                    finish(
                        task,
                        'failed',
                        null,
                        error
                    );
                });
        }

        function pump() {
            pumpScheduled = false;

            while (
                activeCount < maxActive
                && queue.length > 0
            ) {
                const task = queue.shift();

                if (
                    !task
                    || task.state !== 'queued'
                ) {
                    continue;
                }

                run(task);
            }
        }

        function schedulePump() {
            if (pumpScheduled) return;

            pumpScheduled = true;
            Promise.resolve().then(pump);
        }

        return Object.freeze({
            enqueue(context, runner) {
                if (typeof runner !== 'function') {
                    return Promise.reject(
                        new Error(
                            'upload runner is required'
                        )
                    );
                }

                sequence += 1;

                const uploadId = String(
                    context
                    && context.uploadId
                    || (
                        'folder-fallback-'
                        + Date.now()
                        + '-'
                        + sequence
                    )
                );

                let resolveTask;
                let rejectTask;

                const promise = new Promise(
                    function (resolve, reject) {
                        resolveTask = resolve;
                        rejectTask = reject;
                    }
                );

                const task = {
                    uploadId,
                    sequence,
                    state: 'queued',
                    progress: 0,
                    createdAt: Date.now(),
                    startedAt: 0,
                    finishedAt: 0,
                    error: '',
                    context: Object.freeze({
                        ...(context || {}),
                        uploadId
                    }),
                    runner,
                    resolve: resolveTask,
                    reject: rejectTask
                };

                tasksById.set(uploadId, task);
                queue.push(task);
                emit('queued', task);
                schedulePump();

                return promise;
            },
            getSnapshot() {
                return snapshot();
            }
        });
    }

    const folderFallbackUploadManager = (
        createFolderFallbackUploadManager(
            1,
            function (
                snapshot,
                reason,
                task
            ) {
                if (
                    global.ChecklistFolderUploadProgress
                    && typeof global
                        .ChecklistFolderUploadProgress
                        .publishLocalSnapshot
                        === 'function'
                ) {
                    global.ChecklistFolderUploadProgress
                        .publishLocalSnapshot(
                            snapshot,
                            reason,
                            task
                        );
                }
            }
        )
    );

    function getFolderOpenerUploadManager() {
        try {
            const manager = (
                window.opener
                && window.opener.popupUploadManager
            );

            if (
                manager
                && typeof manager.enqueue === 'function'
            ) {
                return manager;
            }
        } catch (e) {
            console.log(
                'opener upload manager unavailable:',
                e
            );
        }

        return null;
    }

    function buildFolderUploadTaskContext(
        file,
        operationKind = 'upload',
        extra = {}
    ) {
        folderUploadSequence += 1;

        return Object.freeze({
            uploadId: (
                'folder_'
                + Date.now()
                + '_'
                + folderUploadSequence
            ),
            dialogId: folderDialogId,
            checklistKey: folderChecklistKey,
            itemId: folderItemId,
            itemGroup: folderItemGroup,
            itemName: folderItemName,
            fileName: String(
                file && file.name || 'Файл'
            ),
            fileSize: Number(
                file && file.size || 0
            ),
            fileType: String(
                file && file.type || ''
            ),
            source: 'folder',
            operationKind: String(
                operationKind || 'upload'
            ),
            documentId: String(
                extra.documentId || ''
            ),
            documentName: String(
                extra.documentName || ''
            ),
            sessionId: String(
                extra.sessionId || ''
            ).trim()
        });
    }

    function sendFolderMultipartRequest(
        url,
        formData,
        progressControl
    ) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();

            xhr.open('POST', url, true);
            xhr.timeout = 10 * 60 * 1000;

            xhr.upload.onprogress = function (event) {
                if (
                    !progressControl
                    || typeof progressControl.updateProgress
                        !== 'function'
                ) {
                    return;
                }

                if (!event.lengthComputable) {
                    progressControl.updateProgress(1);
                    return;
                }

                progressControl.updateProgress(
                    Math.max(
                        0,
                        Math.min(
                            100,
                            Math.round(
                                event.loaded
                                / event.total
                                * 100
                            )
                        )
                    )
                );
            };

            xhr.onload = function () {
                let result = {};

                try {
                    result = JSON.parse(
                        String(xhr.responseText || '{}')
                    );
                } catch (e) {
                    reject(
                        new Error(
                            xhr.status === 413
                                ? 'Файл слишком большой для сервера'
                                : 'Некорректный ответ сервера'
                        )
                    );
                    return;
                }

                resolve({
                    status: Number(xhr.status || 0),
                    responseOk: (
                        xhr.status >= 200
                        && xhr.status < 300
                    ),
                    result
                });
            };

            xhr.onerror = function () {
                const error = new Error(
                    'Ошибка сети при загрузке файла'
                );
                error.retryable = true;
                error.httpStatus = Number(xhr.status || 0);
                reject(error);
            };

            xhr.onabort = function () {
                reject(
                    new Error('Загрузка отменена')
                );
            };

            xhr.ontimeout = function () {
                const error = new Error(
                    'Истекло время загрузки'
                );
                error.retryable = true;
                error.httpStatus = 408;
                reject(error);
            };

            xhr.send(formData);
        });
    }

    async function sendFolderUploadRequest(
        file,
        context,
        progressControl
    ) {
        const actor = getFolderDeleteActor();
        const formData = new FormData();

        formData.append('dialogId', folderDialogId);
        formData.append('itemId', folderItemId);
        formData.append('file', file);
        formData.append(
            'checklistKey',
            folderChecklistKey
        );
        formData.append('itemGroup', folderItemGroup);
        formData.append('actingUserId', actor.id);
        formData.append('actingUserName', actor.name);
        let editSessionId = '';

        try {
            editSessionId = String(
                requireFolderEditSession(
                    'загрузка файлов'
                )
                || context
                && context.sessionId
                || ''
            ).trim();
        } catch (sessionError) {
            sessionError.retryable = true;
            throw sessionError;
        }

        if (!editSessionId) {
            const error = new Error(
                'Загрузка ожидает восстановления '
                + 'сессии редактирования'
            );
            error.retryable = true;
            throw error;
        }

        formData.append(
            'sessionId',
            editSessionId
        );
        formData.append(
            'requireEditSession',
            '1'
        );

        const response = await sendFolderMultipartRequest(
            folderUploadApiUrl,
            formData,
            progressControl
        );

        if (
            !response.responseOk
            || !response.result.ok
        ) {
            const error = new Error(
                response.result.error
                || response.result.details
                || 'upload document failed'
            );
            const status = Number(response.status || 0);
            error.retryable = (
                [408, 425, 429, 500, 502, 503, 504]
                    .includes(status)
                || (
                    response.result.editSessionError === true
                    && [404, 409].includes(status)
                )
            );
            error.httpStatus = status;

            if (
                error.retryable
                && response.result.editSessionError === true
            ) {
                try {
                    const openerApi = (
                        window.opener
                        && window.opener
                            .ChecklistPopupEditSession
                    );
                    if (
                        openerApi
                        && typeof openerApi.invalidate
                            === 'function'
                    ) {
                        openerApi.invalidate(error.message);
                    }
                } catch (sessionError) {
                    void sessionError;
                }
            }

            throw error;
        }

        return response.result;
    }

    window.folderUploadQueueDebug = Object.freeze({
        get openerConnected() {
            return !!getFolderOpenerUploadManager();
        },
        get fallbackSnapshot() {
            return folderFallbackUploadManager
                .getSnapshot();
        }
    });

    // Stage 6.5.5: replacement logic moved to folder-replacement.js.



    document.querySelectorAll('[data-role="folder-remove-file"]').forEach(btn => {
        btn.addEventListener('click', async function () {
            const documentName = this.dataset.documentName || 'файл';
            const actor = getFolderDeleteActor();
            let editSessionId = '';

            try {
                editSessionId = requireFolderEditSession(
                    'удаление файла'
                );
            } catch (error) {
                alert(
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            if (!folderDeleteAllowedUserIds.has(String(actor.id || '').trim())) {
                alert('У вас недостаточно прав на удаление файлов');
                return;
            }

            if (!window.confirm('Удалить файл "' + documentName + '"?')) {
                return;
            }

            this.disabled = true;

            try {
                const response = await fetch(folderRemoveApiUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        dialogId: this.dataset.dialogId,
                        checklistKey: this.dataset.checklistKey,
                        itemId: this.dataset.itemId,
                        documentId: this.dataset.documentId,
                        actingUserId: actor.id,
                        actingUserName: actor.name,
                        sessionId: editSessionId,
                        requireEditSession: true
                    })
                });

                const result = await response.json();
                if (!response.ok || !result.ok) {
                    throw new Error(result.error || 'remove document failed');
                }

                notifyParentChecklistDocumentChanged('checklist-document-removed', {
                    documentName: documentName
                });
                window.location.reload();
            } catch (e) {
                console.log('folder remove error:', e);
                alert(e && e.message ? e.message : 'Ошибка удаления файла');
            } finally {
                this.disabled = false;
                if (
                    global.ChecklistFolderCore
                    && typeof global.ChecklistFolderCore
                        .applySessionState === 'function'
                ) {
                    global.ChecklistFolderCore
                        .applySessionState();
                }
            }
        });
    });

    const folderUploadBtn = document.getElementById('folderUploadBtn');
    const folderUploadInput = document.getElementById('folderUploadInput');

    if (folderUploadBtn && folderUploadInput) {
        folderUploadBtn.addEventListener('click', function () {
            try {
                requireFolderEditSession(
                    'загрузка файлов'
                );
            } catch (error) {
                alert(
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            folderUploadInput.click();
        });

        folderUploadInput.addEventListener('change', async function () {
            const files = Array.from(this.files || []);

            if (!files.length) {
                return;
            }

            let editSessionId = '';

            try {
                editSessionId = requireFolderEditSession(
                    'загрузка файлов'
                );
            } catch (error) {
                this.value = '';
                alert(
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            const openerManager = (
                getFolderOpenerUploadManager()
            );

            const uploadManager = (
                openerManager
                || folderFallbackUploadManager
            );

            const managedByUploadManager = !!openerManager;

            folderUploadBtn.disabled = true;
            if (
                actionIcons
                && typeof actionIcons.setBusy === 'function'
            ) {
                actionIcons.setBusy(folderUploadBtn, true);
            }

            const uploadPlans = files.map(file => {
                const context = buildFolderUploadTaskContext(
                    file,
                    'upload',
                    {
                        sessionId: editSessionId
                    }
                );

                return {
                    file,
                    context,
                    promise: uploadManager.enqueue(
                        context,
                        function (
                            immutableContext,
                            progressControl
                        ) {
                            return sendFolderUploadRequest(
                                file,
                                immutableContext,
                                progressControl
                            );
                        }
                    )
                };
            });

            try {
                const settled = await Promise.allSettled(
                    uploadPlans.map(plan => (
                        plan.promise.then(result => ({
                            file: plan.file,
                            context: plan.context,
                            result
                        }))
                    ))
                );

                const successful = [];
                const failed = [];

                settled.forEach((entry, index) => {
                    const plan = uploadPlans[index];

                    if (entry.status === 'fulfilled') {
                        successful.push({
                            ...entry.value,
                            fileName: String(
                                plan.file.name || 'Файл'
                            )
                        });
                    } else {
                        failed.push({
                            fileName: String(
                                plan.file.name || 'Файл'
                            ),
                            error: String(
                                entry.reason
                                && entry.reason.message
                                || entry.reason
                                || 'Ошибка загрузки'
                            )
                        });
                    }
                });

                if (successful.length) {
                    notifyParentChecklistDocumentChanged(
                        'checklist-document-uploaded',
                        {
                            itemName: folderItemName,
                            fileNames: successful.map(
                                entry => entry.fileName
                            ),
                            managedByUploadManager
                        }
                    );
                }

                if (failed.length) {
                    const failedText = failed
                        .map(entry => (
                            entry.fileName
                            + ': '
                            + entry.error
                        ))
                        .join('\\n');

                    alert(
                        'Не удалось загрузить часть файлов:\\n\\n'
                        + failedText
                    );
                }

                if (successful.length) {
                    window.location.reload();
                }

            } catch (e) {
                console.log('folder upload error:', e);

                alert(
                    e && e.message
                        ? e.message
                        : 'Ошибка загрузки файлов'
                );

            } finally {
                this.value = '';
                folderUploadBtn.disabled = false;
                if (
                    actionIcons
                    && typeof actionIcons.setBusy === 'function'
                ) {
                    actionIcons.setBusy(folderUploadBtn, false);
                }
                if (
                    global.ChecklistFolderCore
                    && typeof global.ChecklistFolderCore
                        .applySessionState === 'function'
                ) {
                    global.ChecklistFolderCore
                        .applySessionState();
                }
            }
        });
    }
    if (
        global.ChecklistFolderUploadProgress
        && typeof global
            .ChecklistFolderUploadProgress
            .setLocalSnapshotProvider
            === 'function'
    ) {
        global.ChecklistFolderUploadProgress
            .setLocalSnapshotProvider(
                function () {
                    return folderFallbackUploadManager
                        .getSnapshot();
                }
            );
    }

    window.ChecklistFolderUploads = Object.freeze({
        get openerConnected() {
            return !!getFolderOpenerUploadManager();
        },
        get fallbackSnapshot() {
            return folderFallbackUploadManager.getSnapshot();
        },
        getManager() {
            return (
                getFolderOpenerUploadManager()
                || folderFallbackUploadManager
            );
        },
        buildTaskContext: buildFolderUploadTaskContext,
        sendMultipartRequest: sendFolderMultipartRequest,
        notifyParent: notifyParentChecklistDocumentChanged
    });

})(window);
