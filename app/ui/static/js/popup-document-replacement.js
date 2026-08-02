(function (global) {
    'use strict';

    let replacementInput = null;
    let pendingTarget = null;
    let pollingTimer = null;
    let pollingInProgress = false;

    function normalizeStatus(value) {
        return String(value || '').trim().toLowerCase();
    }

    function isEditingAvailable() {
        return (
            typeof isEditingAllowed !== 'function'
            || isEditingAllowed()
        );
    }

    function getReplaceButtons() {
        return Array.from(
            document.querySelectorAll(
                '[data-role="replace-document"]'
            )
        );
    }

    function statusFromJob(result) {
        const status = normalizeStatus(
            result && result.status
        );
        const stage = normalizeStatus(
            result && result.stage
        );

        if (
            status === 'skipped'
            && stage === 'yandex_disabled'
        ) {
            return 'disabled';
        }

        return status;
    }

    function statusTitle(status, errorText = '') {
        const normalized = normalizeStatus(status);
        const error = String(errorText || '').trim();

        if (normalized === 'queued') {
            return 'Файл ожидает синхронизации с Яндекс.Диском';
        }
        if (normalized === 'running') {
            return 'Файл загружается на Яндекс.Диск';
        }
        if (normalized === 'error') {
            return error || (
                'Ошибка синхронизации. '
                + 'Для замены потребуется подтверждение'
            );
        }
        if (normalized === 'disabled') {
            return 'Заменить локальный файл';
        }

        return 'Заменить файл';
    }

    function applyButtonStatus(button, status, errorText = '') {
        if (!button) return;

        const normalized = normalizeStatus(status);
        const blocked = ['queued', 'running'].includes(
            normalized
        );
        const title = statusTitle(normalized, errorText);

        button.dataset.mirrorStatus = normalized;
        button.dataset.mirrorError = String(
            errorText || ''
        ).trim();
        button.disabled = blocked || !isEditingAvailable();
        button.title = title;
        button.setAttribute('aria-label', title);
        button.classList.toggle(
            'is-mirror-pending',
            blocked
        );
        button.classList.toggle(
            'is-mirror-error',
            normalized === 'error'
        );
    }

    async function refreshButtonStatus(button) {
        const documentId = String(
            button && button.dataset.documentId || ''
        ).trim();
        const itemId = String(
            button && button.dataset.itemId || ''
        ).trim();

        if (!documentId || !itemId) {
            return normalizeStatus(
                button && button.dataset.mirrorStatus
            );
        }

        const query = new URLSearchParams({
            dialogId: String(dialogId || ''),
            checklistKey: String(
                currentChecklistKey || 'id'
            ),
            itemId,
            documentId
        });

        try {
            const response = await fetch(
                appUrl(
                    'api/checklist/document-mirror-status'
                ) + '?' + query.toString(),
                { cache: 'no-store' }
            );
            const result = await response
                .json()
                .catch(() => ({}));

            if (!response.ok || !result.ok) {
                const failures = Number(
                    button.dataset.mirrorPollingFailures || 0
                ) + 1;
                button.dataset.mirrorPollingFailures = String(
                    failures
                );

                if (failures >= 3) {
                    applyButtonStatus(
                        button,
                        'error',
                        'Связанная задача синхронизации не найдена'
                    );
                    return 'error';
                }

                return normalizeStatus(
                    button.dataset.mirrorStatus
                );
            }

            button.dataset.mirrorPollingFailures = '0';
            const resolvedStatus = statusFromJob(result);
            applyButtonStatus(
                button,
                resolvedStatus,
                String(result.error || '')
            );
            return resolvedStatus;
        } catch (error) {
            console.log(
                'popup replacement mirror status error:',
                error
            );
            return normalizeStatus(
                button.dataset.mirrorStatus
            );
        }
    }

    async function pollPendingButtons() {
        if (pollingInProgress) return;

        const pendingButtons = getReplaceButtons().filter(
            button => ['queued', 'running'].includes(
                normalizeStatus(
                    button.dataset.mirrorStatus
                )
            )
        );

        if (!pendingButtons.length) {
            stopPolling();
            return;
        }

        pollingInProgress = true;

        try {
            await Promise.all(
                pendingButtons.map(refreshButtonStatus)
            );
        } finally {
            pollingInProgress = false;
        }
    }

    function startPollingIfNeeded() {
        const hasPending = getReplaceButtons().some(
            button => ['queued', 'running'].includes(
                normalizeStatus(
                    button.dataset.mirrorStatus
                )
            )
        );

        if (!hasPending || pollingTimer) return;

        void pollPendingButtons();
        pollingTimer = global.setInterval(
            pollPendingButtons,
            1200
        );
    }

    function stopPolling() {
        if (!pollingTimer) return;
        global.clearInterval(pollingTimer);
        pollingTimer = null;
    }

    function ensureReplacementInput() {
        if (
            replacementInput
            && replacementInput.isConnected
        ) {
            return replacementInput;
        }

        replacementInput = document.getElementById(
            'popupDocumentReplaceInput'
        );

        if (!replacementInput) {
            replacementInput = document.createElement('input');
            replacementInput.id = 'popupDocumentReplaceInput';
            replacementInput.type = 'file';
            replacementInput.hidden = true;
            replacementInput.setAttribute(
                'aria-label',
                'Выбрать новую версию файла'
            );
            document.body.appendChild(replacementInput);
        }

        if (replacementInput.dataset.bound !== '1') {
            replacementInput.dataset.bound = '1';
            replacementInput.addEventListener(
                'change',
                handleReplacementFileSelected
            );
        }

        return replacementInput;
    }

    function currentActor() {
        if (
            typeof getFileDeleteActor === 'function'
        ) {
            return getFileDeleteActor();
        }

        return {
            id: '',
            name: 'Пользователь'
        };
    }

    function buildTaskContext(file, target) {
        const targetItem = Array.isArray(items)
            ? items.find(item => (
                String(item && item.id || '')
                === String(target.itemId || '')
            ))
            : null;

        return Object.freeze({
            uploadId: (
                'popup_replace_'
                + Date.now()
                + '_'
                + Math.random().toString(36).slice(2, 8)
            ),
            dialogId: String(dialogId || ''),
            checklistKey: String(
                currentChecklistKey || 'id'
            ),
            itemId: String(target.itemId || ''),
            itemGroup: String(
                targetItem && targetItem.group || ''
            ),
            itemName: String(
                targetItem && targetItem.name || ''
            ),
            fileName: String(file && file.name || 'Файл'),
            fileSize: Number(file && file.size || 0),
            fileType: String(file && file.type || ''),
            source: 'popup',
            operationKind: 'replacement',
            documentId: String(target.documentId || ''),
            documentName: String(target.documentName || ''),
            sessionId: String(target.sessionId || '').trim()
        });
    }

    function sendMultipartRequest(
        formData,
        progressControl
    ) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();

            xhr.open(
                'POST',
                appUrl(
                    'api/checklist/replace-document'
                ),
                true
            );

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
                                event.loaded / event.total * 100
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
                } catch (error) {
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
                reject(
                    new Error(
                        'Ошибка сети при замене файла'
                    )
                );
            };

            xhr.onabort = function () {
                reject(new Error('Замена файла отменена'));
            };

            xhr.ontimeout = function () {
                reject(
                    new Error(
                        'Истекло время замены файла'
                    )
                );
            };

            xhr.send(formData);
        });
    }

    async function sendReplacement(
        file,
        target,
        forceReplace,
        progressControl
    ) {
        if (
            typeof fetchCurrentUserIfPossible
            === 'function'
        ) {
            await fetchCurrentUserIfPossible();
        }

        const actor = currentActor();
        const formData = new FormData();

        formData.append(
            'dialogId',
            String(dialogId || '')
        );
        formData.append(
            'checklistKey',
            String(currentChecklistKey || 'id')
        );
        formData.append('itemId', target.itemId);
        formData.append('documentId', target.documentId);
        formData.append('file', file);
        formData.append('actingUserId', actor.id || '');
        formData.append(
            'actingUserName',
            actor.name || 'Пользователь'
        );
        const editSessionId = String(
            target && target.sessionId || ''
        ).trim();

        if (!editSessionId) {
            throw new Error(
                'Замена заблокирована: '
                + 'сессия редактирования не готова'
            );
        }

        formData.append(
            'sessionId',
            editSessionId
        );
        formData.append(
            'requireEditSession',
            '1'
        );
        formData.append(
            'forceReplace',
            forceReplace ? '1' : '0'
        );

        const response = await sendMultipartRequest(
            formData,
            progressControl
        );
        const result = response.result || {};

        if (
            response.status === 409
            && result.requiresForceReplace
            && !forceReplace
        ) {
            const confirmed = global.confirm(
                'У текущего файла есть ошибка '
                + 'синхронизации с Яндекс.Диском.\n\n'
                + 'Заменить его принудительно?'
            );

            if (!confirmed) {
                return {
                    ok: false,
                    cancelled: true
                };
            }

            return sendReplacement(
                file,
                target,
                true,
                progressControl
            );
        }

        if (!response.responseOk || !result.ok) {
            throw new Error(
                result.error
                || result.details
                || 'Не удалось заменить файл'
            );
        }

        return result;
    }

    function buildReplacementEntry(result, target, file) {
        const replacement = (
            result
            && result.replacement
            && typeof result.replacement === 'object'
                ? result.replacement
                : {}
        );
        const archiveVersion = (
            replacement.archiveVersion
            && typeof replacement.archiveVersion === 'object'
                ? replacement.archiveVersion
                : {}
        );
        const archiveNumber = Number(
            archiveVersion.version || 0
        );

        return {
            operationId: String(
                replacement.operationId
                || result.operationId
                || ''
            ).trim(),
            oldDocumentId: String(
                replacement.oldDocumentId
                || target.documentId
                || ''
            ).trim(),
            newDocumentId: String(
                replacement.newDocumentId || ''
            ).trim(),
            oldDocumentName: String(
                replacement.oldFileName
                || target.documentName
                || 'Файл'
            ).trim(),
            newDocumentName: String(
                replacement.newFileName
                || file.name
                || 'Файл'
            ).trim(),
            archiveVersion: archiveNumber,
            archiveVersionLabel: String(
                archiveVersion.versionLabel
                || (archiveNumber > 0
                    ? 'v' + archiveNumber
                    : '')
            ).trim(),
            archiveVersionId: String(
                archiveVersion.id || ''
            ).trim(),
            seriesId: String(
                replacement.seriesId
                || target.seriesId
                || ''
            ).trim()
        };
    }

    async function applyReplacementResult(
        result,
        target,
        file
    ) {
        const entry = buildReplacementEntry(
            result,
            target,
            file
        );
        const item = Array.isArray(items)
            ? items.find(candidate => (
                String(candidate && candidate.id || '')
                === target.itemId
            ))
            : null;

        const syncApi = global.ChecklistPopupWindowSyncEvents;

        if (
            syncApi
            && typeof syncApi.handleMessage === 'function'
        ) {
            await syncApi.handleMessage({
                source: global,
                data: {
                    type: 'checklist-document-replaced',
                    dialogId: String(dialogId || ''),
                    checklistKey: String(
                        currentChecklistKey || 'id'
                    ),
                    itemId: target.itemId,
                    itemName: String(
                        item && item.name || ''
                    ),
                    documentName: entry.newDocumentName,
                    replacement: entry,
                    managedByUploadManager: true,
                    source: 'popup'
                }
            });
        } else {
            const response = await fetch(
                appUrl('api/checklist')
                + '?dialogId=' + encodeURIComponent(
                    String(dialogId || '')
                )
                + '&checklistKey=' + encodeURIComponent(
                    String(currentChecklistKey || 'id')
                )
            );
            const snapshot = await response.json();

            if (!response.ok) {
                throw new Error(
                    snapshot.error
                    || 'Не удалось обновить чек-лист после замены'
                );
            }

            if (
                typeof applyChecklistData === 'function'
            ) {
                applyChecklistData(snapshot);
            }
            if (typeof renderAll === 'function') {
                renderAll();
            }
        }

        return entry;
    }

    async function runReplacement(file, target) {
        const manager = popupUploadManager;

        if (
            !manager
            || typeof manager.enqueue !== 'function'
        ) {
            throw new Error(
                'Общий менеджер загрузок не инициализирован'
            );
        }

        const context = buildTaskContext(file, target);

        if (typeof setSaveState === 'function') {
            setSaveState(
                'saving',
                'Заменяем файл...'
            );
        }

        const result = await manager.enqueue(
            context,
            function (
                immutableContext,
                progressControl
            ) {
                void immutableContext;
                return sendReplacement(
                    file,
                    target,
                    false,
                    progressControl
                );
            }
        );

        if (result && result.cancelled) {
            if (
                typeof updateSaveStateBySession
                === 'function'
            ) {
                updateSaveStateBySession();
            }
            return null;
        }

        const entry = await applyReplacementResult(
            result,
            target,
            file
        );

        if (
            typeof updateSaveStateBySession
            === 'function'
        ) {
            updateSaveStateBySession();
        } else if (
            typeof setSaveState === 'function'
        ) {
            setSaveState('', 'Сохранено');
        }

        if (typeof debugLog === 'function') {
            debugLog(
                'popup_document_replacement_completed',
                {
                    itemId: target.itemId,
                    oldDocumentId: target.documentId,
                    newDocumentId: entry.newDocumentId,
                    oldDocumentName: entry.oldDocumentName,
                    newDocumentName: entry.newDocumentName,
                    operationId: entry.operationId
                }
            );
        }

        return entry;
    }

    async function handleReplacementFileSelected() {
        const file = (
            replacementInput
            && replacementInput.files
            && replacementInput.files[0]
                ? replacementInput.files[0]
                : null
        );
        const target = pendingTarget;

        if (replacementInput) {
            replacementInput.value = '';
        }
        pendingTarget = null;

        if (!file || !target) return;

        const button = getReplaceButtons().find(candidate => (
            String(
                candidate
                && candidate.dataset
                && candidate.dataset.documentId
                || ''
            ).trim() === target.documentId
        )) || null;

        if (button) {
            button.disabled = true;
            button.classList.add('is-uploading');
        }

        try {
            await runReplacement(file, target);
        } catch (error) {
            console.log(
                'popup document replacement error:',
                error
            );
            if (typeof setSaveState === 'function') {
                setSaveState(
                    'error',
                    'Ошибка замены файла'
                );
            }
            global.alert(
                error && error.message
                    ? error.message
                    : 'Ошибка замены файла'
            );
        } finally {
            if (button && button.isConnected) {
                button.classList.remove('is-uploading');
                applyButtonStatus(
                    button,
                    button.dataset.mirrorStatus,
                    button.dataset.mirrorError
                );
            }
        }
    }

    function bind() {
        const input = ensureReplacementInput();

        getReplaceButtons().forEach(button => {
            if (button.dataset.replacementBound === '1') {
                return;
            }

            button.dataset.replacementBound = '1';
            button.addEventListener(
                'click',
                async function () {
                    if (!isEditingAvailable()) return;

                    let editSessionId = '';

                    try {
                        editSessionId = await requireEditingSession(
                            'замена файла'
                        );
                    } catch (error) {
                        global.alert(
                            error && error.message
                                ? error.message
                                : 'Сессия редактирования не готова'
                        );
                        return;
                    }

                    const resolvedStatus = await refreshButtonStatus(
                        button
                    );

                    if (
                        ['queued', 'running'].includes(
                            normalizeStatus(resolvedStatus)
                        )
                    ) {
                        global.alert(
                            'Файл ещё синхронизируется '
                            + 'с Яндекс.Диском. Дождитесь '
                            + 'завершения синхронизации.'
                        );
                        return;
                    }

                    pendingTarget = {
                        itemId: String(
                            button.dataset.itemId || ''
                        ).trim(),
                        documentId: String(
                            button.dataset.documentId || ''
                        ).trim(),
                        documentName: String(
                            button.dataset.documentName || 'Файл'
                        ).trim(),
                        seriesId: String(
                            button.dataset.seriesId || ''
                        ).trim(),
                        sessionId: editSessionId
                    };

                    if (
                        !pendingTarget.itemId
                        || !pendingTarget.documentId
                    ) {
                        pendingTarget = null;
                        return;
                    }

                    input.value = '';
                    input.click();
                }
            );
        });

        startPollingIfNeeded();
    }

    global.addEventListener(
        'pagehide',
        stopPolling
    );

    global.ChecklistPopupDocumentReplacement = Object.freeze({
        bind,
        refreshButtonStatus,
        statusFromJob,
        buildReplacementEntry,
        runReplacement
    });
})(window);
