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
        maxActive = 10,
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

    // Up to ten uploads at once, the next starts as soon as one finishes.
    const folderFallbackUploadManager = (
        createFolderFallbackUploadManager(
            10,
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
            itemId: String(extra.itemId || folderItemId),
            itemGroup: folderItemGroup,
            itemName: folderItemName,
            relativeFolder: String(extra.relativeFolder || ''),
            folderUploadRoot: String(extra.folderUploadRoot || ''),
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
        formData.append(
            'itemId',
            String(context && context.itemId || folderItemId)
        );
        formData.append('file', file);
        if (context && context.relativeFolder) {
            formData.append('relativeFolder', context.relativeFolder);
        }
        if (context && context.folderUploadRoot) {
            formData.append('folderUploadRoot', context.folderUploadRoot);
        }
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
    const folderUploadStagingMount = document.getElementById(
        'folderUploadStagingMount'
    );

    const folderBootstrap = (
        global.ChecklistFolderCore
        && global.ChecklistFolderCore.bootstrap
    ) || {};

    async function postFolderStructureApi(url, body) {
        const actor = getFolderDeleteActor();
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dialogId: folderDialogId,
                checklistKey: folderChecklistKey,
                sessionId: requireFolderEditSession('загрузка папки'),
                requireEditSession: true,
                actingUserId: actor.id,
                actingUserName: actor.name,
                ...body
            })
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.ok) {
            throw new Error(result && result.error || 'Не удалось создать папку');
        }
        return result;
    }

    async function planFolderPageUpload(files, emptyDirs) {
        const planner = global.ChecklistFolderUploadPlanner;
        const currentFolder = String(folderBootstrap.relativeFolder || '');
        if (!planner || !planner.hasFolders(files, emptyDirs)) {
            return {
                placements: files.map(file => ({
                    file,
                    itemId: folderItemId,
                    relativeFolder: currentFolder,
                    folderUploadRoot: ''
                })),
                failures: [],
                structureChanged: false
            };
        }
        let structureChanged = false;
        const result = await planner.plan({
            files,
            emptyDirs,
            target: {
                itemId: folderItemId,
                topLevel: folderBootstrap.itemIsTopLevel === true,
                relativeFolder: currentFolder,
                subitems: Array.isArray(folderBootstrap.subitems)
                    ? folderBootstrap.subitems
                    : []
            },
            async ensureSubitem(name) {
                const created = await postFolderStructureApi(
                    String(folderBootstrap.addItemApiUrl || ''),
                    {
                        groupId: Number(folderItemGroup || 0),
                        parentItemId: folderItemId,
                        name
                    }
                );
                if (!created.item) {
                    throw new Error('Не удалось создать подпункт «' + name + '»');
                }
                structureChanged = true;
                return {
                    id: String(created.item.id || ''),
                    name: String(created.item.name || name)
                };
            },
            async createFolder(folder) {
                const parts = String(folder.path || '').split('/').filter(Boolean);
                const name = parts.pop();
                const created = await postFolderStructureApi(
                    String(folderBootstrap.folderCreateApiUrl || ''),
                    {
                        itemId: folder.itemId,
                        parentFolder: parts.join('/'),
                        name,
                        folderUploadRoot: folder.root || ''
                    }
                );
                if (created.created) structureChanged = true;
            }
        });
        return { ...result, structureChanged };
    }

    async function uploadFolderStagedFiles(
        files,
        stagingController,
        extras = {}
    ) {
        const selectedFiles = Array.from(files || []);
        const selectedEmptyDirs = Array.isArray(extras && extras.emptyDirs)
            ? extras.emptyDirs.slice()
            : [];
        if (!selectedFiles.length && !selectedEmptyDirs.length) {
            return { keepState: true };
        }

        const editSessionId = requireFolderEditSession(
            'загрузка файлов'
        );
        const openerManager = getFolderOpenerUploadManager();
        const uploadManager = (
            openerManager
            || folderFallbackUploadManager
        );
        const managedByUploadManager = !!openerManager;

        if (
            actionIcons
            && typeof actionIcons.setBusy === 'function'
        ) {
            actionIcons.setBusy(folderUploadBtn, true);
        }

        let planned;
        try {
            planned = await planFolderPageUpload(
                selectedFiles,
                selectedEmptyDirs
            );
        } catch (error) {
            if (
                actionIcons
                && typeof actionIcons.setBusy === 'function'
            ) {
                actionIcons.setBusy(folderUploadBtn, false);
            }
            throw error;
        }

        const uploadPlans = planned.placements.map(placement => {
            const file = placement.file;
            const context = buildFolderUploadTaskContext(
                file,
                'upload',
                {
                    sessionId: editSessionId,
                    itemId: placement.itemId,
                    relativeFolder: placement.relativeFolder,
                    folderUploadRoot: placement.folderUploadRoot
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
            const failedFiles = [];

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
                    failedFiles.push(plan.file);
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

            planned.failures.forEach(failure => {
                if (failure.file) failedFiles.push(failure.file);
                failed.push({
                    fileName: String(
                        failure.file && failure.file.name
                        || failure.emptyDir
                        || 'Папка'
                    ),
                    error: String(
                        failure.error && failure.error.message
                        || failure.error
                        || 'Ошибка загрузки'
                    )
                });
            });
            const failedEmptyDirs = planned.failures
                .map(failure => failure.emptyDir)
                .filter(Boolean);

            stagingController.replaceFiles(
                failedFiles,
                {
                    expanded: failedFiles.length > 0 || failedEmptyDirs.length > 0,
                    emptyDirs: failedEmptyDirs
                }
            );

            if (!successful.length && planned.structureChanged) {
                notifyParentChecklistDocumentChanged(
                    'checklist-document-changed',
                    {
                        folderChange: {
                            oldValue: '',
                            newValue: selectedEmptyDirs.join(', ')
                        }
                    }
                );
            }

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
                    .join('\n');

                alert(
                    'Не удалось загрузить часть файлов:\n\n'
                    + failedText
                );
            }

            if ((successful.length || planned.structureChanged) && !failed.length) {
                stagingController.clear({ collapse: true });
                window.location.reload();
            }

            return { keepState: true };
        } finally {
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
                global.ChecklistFolderCore.applySessionState();
            }
        }
    }

    if (
        folderUploadBtn
        && folderUploadInput
        && folderUploadStagingMount
        && global.ChecklistUploadStaging
        && typeof global.ChecklistUploadStaging.create === 'function'
    ) {
        global.ChecklistUploadStaging.create({
            trigger: folderUploadBtn,
            input: folderUploadInput,
            mount: folderUploadStagingMount,
            scrollOnExpand: true,
            stateKey: [
                'folder',
                String(folderDialogId || ''),
                String(folderChecklistKey || 'id'),
                String(folderItemId || '')
            ].join(':'),
            canInteract() {
                requireFolderEditSession('загрузка файлов');
                return true;
            },
            onBlocked(error) {
                alert(
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
            },
            onConfirm(files, controller, extras) {
                return uploadFolderStagedFiles(
                    files,
                    controller,
                    extras
                );
            },
            onError(error) {
                console.log('folder upload error:', error);
                alert(
                    error && error.message
                        ? error.message
                        : 'Ошибка загрузки файлов'
                );
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
