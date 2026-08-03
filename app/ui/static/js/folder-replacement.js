(function (global) {
    'use strict';

    const core = global.ChecklistFolderCore;
    const uploads = global.ChecklistFolderUploads;
    const actionIcons = global.ChecklistActionIcons;

    if (!core) {
        throw new Error(
            'ChecklistFolderCore is not initialized'
        );
    }

    if (
        !uploads
        || typeof uploads.getManager !== 'function'
        || typeof uploads.buildTaskContext !== 'function'
        || typeof uploads.sendMultipartRequest !== 'function'
    ) {
        throw new Error(
            'ChecklistFolderUploads replacement API is not initialized'
        );
    }

    const bootstrap = core.bootstrap || {};
    const replaceApiUrl = String(
        bootstrap.replaceApiUrl || ''
    );
    const mirrorStatusApiUrl = String(
        bootstrap.documentMirrorStatusApiUrl || ''
    );
    const dialogId = String(bootstrap.dialogId || '');
    const checklistKey = String(
        bootstrap.checklistKey || 'id'
    );
    const itemId = String(bootstrap.itemId || '');
    const itemName = String(
        bootstrap.itemName || 'Пункт'
    );

    const replaceInput = document.getElementById(
        'folderReplaceInput'
    );
    const replaceButtons = Array.from(
        document.querySelectorAll(
            '[data-role="folder-replace-upload"]'
        )
    );
    const mirrorBadges = Array.from(
        document.querySelectorAll(
            '[data-role="folder-mirror-status"]'
        )
    );

    let pendingTarget = null;
    let pollingTimer = null;
    let destroyed = false;

    function normalizeStatus(value) {
        return String(value || '').trim().toLowerCase();
    }

    function findByDocumentId(collection, documentId) {
        const targetId = String(documentId || '').trim();

        return collection.find(element => (
            String(
                element
                && element.dataset
                && element.dataset.documentId
                || ''
            ).trim() === targetId
        )) || null;
    }

    function presentation(status, errorText = '') {
        const normalized = normalizeStatus(status);
        const error = String(errorText || '').trim();

        if (normalized === 'error') {
            return {
                text: 'Ошибка Яндекса',
                className: 'error',
                title: error || (
                    'При синхронизации произошла ошибка. ' +
                    'Замена доступна после подтверждения'
                )
            };
        }

        return {
            text: '',
            className: normalized || 'unknown',
            title: ''
        };
    }

    function setMirrorStatus(
        documentId,
        status,
        errorText = ''
    ) {
        const normalized = normalizeStatus(status);
        const sessionReady = !!(
            typeof core.getSessionId === 'function'
            && core.getSessionId()
        );
        const blocked = ['queued', 'running'].includes(
            normalized
        );
        const view = presentation(normalized, errorText);
        const button = findByDocumentId(
            replaceButtons,
            documentId
        );
        const badge = findByDocumentId(
            mirrorBadges,
            documentId
        );

        if (button) {
            button.dataset.mirrorStatus = normalized;
            button.disabled = blocked || !sessionReady;
            button.title = !sessionReady
                ? (
                    'Недоступно без активной '
                    + 'сессии редактирования'
                )
                : blocked
                    ? 'Заменить файл'
                    : 'Заменить файл';
            button.setAttribute(
                'aria-label',
                !sessionReady
                    ? (
                        'Недоступно без активной '
                        + 'сессии редактирования'
                    )
                    : blocked
                        ? 'Заменить файл'
                        : 'Заменить файл'
            );
            button.classList.toggle(
                'is-pending',
                blocked
            );
            button.classList.toggle(
                'is-error',
                normalized === 'error'
            );
        }

        if (badge) {
            const previous = normalizeStatus(
                badge.dataset.mirrorStatus
            );

            if (previous) {
                badge.classList.remove(
                    'folder-mirror-status--' + previous
                );
            }

            badge.dataset.mirrorStatus = normalized;
            badge.textContent = view.text;
            if (view.title) {
                badge.title = view.title;
            } else {
                badge.removeAttribute('title');
            }
            badge.hidden = !view.text;
            badge.classList.add(
                'folder-mirror-status--' + view.className
            );
        }
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

    async function pollButton(button) {
        const documentId = String(
            button.dataset.documentId || ''
        ).trim();
        const currentStatus = normalizeStatus(
            button.dataset.mirrorStatus
        );

        if (
            !documentId
            || !['queued', 'running'].includes(
                currentStatus
            )
            || button.dataset.mirrorPollingStopped === '1'
        ) {
            return;
        }

        const query = new URLSearchParams({
            dialogId,
            checklistKey,
            itemId,
            documentId
        });

        try {
            const response = await fetch(
                mirrorStatusApiUrl + '?' + query.toString(),
                { cache: 'no-store' }
            );
            const result = await response
                .json()
                .catch(() => ({}));

            if (!response.ok || !result.ok) {
                const failures = (
                    Number(
                        button.dataset.mirrorPollingFailures
                        || 0
                    ) + 1
                );

                button.dataset.mirrorPollingFailures = String(
                    failures
                );

                if (failures >= 3) {
                    button.dataset.mirrorPollingStopped = '1';
                    setMirrorStatus(
                        documentId,
                        'error',
                        'Связанная задача синхронизации не найдена'
                    );
                }

                return;
            }

            button.dataset.mirrorPollingFailures = '0';
            setMirrorStatus(
                documentId,
                statusFromJob(result),
                String(result.error || '')
            );
        } catch (error) {
            console.log(
                'folder replacement mirror polling error:',
                error
            );
        }
    }

    async function pollAll() {
        if (destroyed) return;

        const active = replaceButtons.filter(button => (
            ['queued', 'running'].includes(
                normalizeStatus(
                    button.dataset.mirrorStatus
                )
            )
            && button.dataset.mirrorPollingStopped !== '1'
        ));

        if (!active.length) {
            stopPolling();
            return;
        }

        await Promise.all(active.map(pollButton));
    }

    function startPolling() {
        stopPolling();
        void pollAll();
        pollingTimer = global.setInterval(
            pollAll,
            1200
        );
    }

    function stopPolling() {
        if (!pollingTimer) return;
        global.clearInterval(pollingTimer);
        pollingTimer = null;
    }

    function actor() {
        return typeof core.getActor === 'function'
            ? core.getActor()
            : { id: '', name: 'Пользователь' };
    }

    function buildReplacementEntry(
        result,
        target,
        file
    ) {
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
                replacement.seriesId || ''
            ).trim()
        };
    }

    async function sendReplacement(
        file,
        target,
        forceReplace,
        progressControl
    ) {
        const currentActor = actor();
        const formData = new FormData();

        formData.append('dialogId', dialogId);
        formData.append('checklistKey', checklistKey);
        formData.append('itemId', itemId);
        formData.append('documentId', target.documentId);
        formData.append('file', file);
        formData.append('actingUserId', currentActor.id);
        formData.append('actingUserName', currentActor.name);
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

        const response = await uploads.sendMultipartRequest(
            replaceApiUrl,
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
                'У текущего файла есть ошибка ' +
                'синхронизации с Яндекс.Диском.\n\n' +
                'Заменить его принудительно?'
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
                || 'replace document failed'
            );
        }

        return result;
    }

    async function runReplacement(file, target) {
        const button = findByDocumentId(
            replaceButtons,
            target.documentId
        );
        const manager = uploads.getManager();
        const context = uploads.buildTaskContext(
            file,
            'replacement',
            {
                documentId: target.documentId,
                documentName: target.documentName,
                sessionId: target.sessionId
            }
        );

        if (button) {
            button.disabled = true;
            button.classList.add('is-uploading');
            if (
                actionIcons
                && typeof actionIcons.setBusy === 'function'
            ) {
                actionIcons.setBusy(button, true);
            } else {
                button.setAttribute('aria-busy', 'true');
            }
        }

        try {
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
                return;
            }

            const entry = buildReplacementEntry(
                result,
                target,
                file
            );

            core.notifyParent(
                'checklist-document-replaced',
                {
                    itemName,
                    replacement: entry,
                    documentName: entry.newDocumentName,
                    managedByUploadManager: true
                }
            );

            if (typeof core.requestRefresh === 'function') {
                core.requestRefresh({
                    changeKind: 'replacement',
                    replacement: entry
                });
            }

            global.setTimeout(function () {
                global.location.reload();
            }, 120);
        } catch (error) {
            console.log(
                'folder replacement error:',
                error
            );
            global.alert(
                error && error.message
                    ? error.message
                    : 'Ошибка замены файла'
            );
        } finally {
            pendingTarget = null;

            if (button && !destroyed) {
                button.disabled = false;
                button.classList.remove('is-uploading');
                if (
                    actionIcons
                    && typeof actionIcons.setBusy === 'function'
                ) {
                    actionIcons.setBusy(button, false);
                } else {
                    button.setAttribute('aria-busy', 'false');
                }
                setMirrorStatus(
                    button.dataset.documentId,
                    button.dataset.mirrorStatus,
                    button.dataset.mirrorError || ''
                );
            }
        }
    }

    replaceButtons.forEach(function (button) {
        button.addEventListener('click', function () {
            let editSessionId = '';

            try {
                editSessionId = core.requireSession(
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

            const mirrorStatus = normalizeStatus(
                button.dataset.mirrorStatus
            );

            if (['queued', 'running'].includes(mirrorStatus)) {
                global.alert(
                    'Файл ещё синхронизируется ' +
                    'с Яндекс.Диском. Дождитесь завершения ' +
                    'синхронизации.'
                );
                return;
            }

            pendingTarget = {
                documentId: String(
                    button.dataset.documentId || ''
                ).trim(),
                documentName: String(
                    button.dataset.documentName || 'Файл'
                ).trim(),
                sessionId: editSessionId
            };

            if (!pendingTarget.documentId || !replaceInput) {
                pendingTarget = null;
                return;
            }

            replaceInput.value = '';
            replaceInput.click();
        });
    });

    if (replaceInput) {
        replaceInput.addEventListener(
            'change',
            async function () {
                const file = (
                    replaceInput.files
                    && replaceInput.files[0]
                        ? replaceInput.files[0]
                        : null
                );
                const target = pendingTarget;
                replaceInput.value = '';

                if (!file || !target) {
                    pendingTarget = null;
                    return;
                }

                await runReplacement(file, target);
            }
        );
    }

    startPolling();

    global.addEventListener(
        'pagehide',
        function () {
            destroyed = true;
            stopPolling();
        },
        { once: true }
    );

    global.ChecklistFolderReplacement = Object.freeze({
        replace(file, target) {
            return runReplacement(file, target);
        },
        setMirrorStatus,
        get pendingTarget() {
            return pendingTarget
                ? { ...pendingTarget }
                : null;
        },
        get buttonsCount() {
            return replaceButtons.length;
        }
    });
})(window);
