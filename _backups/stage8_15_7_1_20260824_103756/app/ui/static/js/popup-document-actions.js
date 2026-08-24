'use strict';

const yandexStructurePollers = new Map();

function getLocalChecklistItem(itemId) {
    const targetId = String(itemId || '').trim();
    return (Array.isArray(items) ? items : []).find(item => (
        String(item && item.id || '').trim() === targetId
    )) || null;
}

function applyYandexStructureJobToLocalItem(itemId, publicJob) {
    const item = getLocalChecklistItem(itemId);
    if (!item || !publicJob) return false;

    const folderState = publicJob.folderState || {};
    item.yandexFolderStatus = String(
        folderState.yandexFolderStatus || ''
    );
    item.yandexFolderError = String(
        folderState.yandexFolderError || ''
    );
    item.yandexFolderPath = String(
        folderState.yandexFolderPath || ''
    );
    item.yandexFolderUrl = String(
        folderState.yandexFolderUrl || ''
    );
    item.yandexFolderTargetPath = String(
        folderState.yandexFolderTargetPath || ''
    );
    item.yandexStructureJobId = String(
        publicJob.jobId || folderState.yandexStructureJobId || ''
    );
    item.yandexStructureAction = String(
        publicJob.action || folderState.yandexStructureAction || ''
    );
    item.yandexStructureUpdatedAt = String(
        publicJob.updatedAt || folderState.yandexStructureUpdatedAt || ''
    );
    return true;
}

function stopYandexStructurePolling(itemId) {
    const targetId = String(itemId || '').trim();
    const timer = yandexStructurePollers.get(targetId);
    if (timer) window.clearTimeout(timer);
    yandexStructurePollers.delete(targetId);
}

function scheduleYandexStructurePolling(itemId, jobId, delayMs = 1800) {
    const targetId = String(itemId || '').trim();
    const targetJobId = String(jobId || '').trim();
    if (!targetId || !targetJobId || yandexStructurePollers.has(targetId)) {
        return;
    }

    const timer = window.setTimeout(async () => {
        yandexStructurePollers.delete(targetId);
        try {
            const query = new URLSearchParams({ jobId: targetJobId });
            const response = await fetch(
                appUrl('api/checklist/yandex-structure-job')
                + '?' + query.toString(),
                { cache: 'no-store' }
            );
            const result = await response.json().catch(() => ({}));
            if (!response.ok || !result.ok || !result.job) return;

            applyYandexStructureJobToLocalItem(targetId, result.job);
            const status = String(
                result.job.folderState
                && result.job.folderState.yandexFolderStatus
                || ''
            );
            renderAll();

            if (['queued', 'running'].includes(status)) {
                scheduleYandexStructurePolling(
                    targetId,
                    targetJobId,
                    2200
                );
            }
        } catch (error) {
            console.log('Yandex structure polling error:', error);
            scheduleYandexStructurePolling(targetId, targetJobId, 4000);
        }
    }, Math.max(500, Number(delayMs) || 1800));

    yandexStructurePollers.set(targetId, timer);
}

function startVisibleYandexStructurePolling() {
    document.querySelectorAll(
        '[data-structure-job-id]'
    ).forEach(element => {
        const status = String(
            element.dataset.yandexStatus
            || element.dataset.status
            || ''
        );
        const jobId = String(
            element.dataset.structureJobId || ''
        );
        if (['queued', 'running'].includes(status) && jobId) {
            scheduleYandexStructurePolling(
                element.dataset.itemId || '',
                jobId
            );
        }
    });
}

async function retryYandexStructureJob(itemId, knownJobId) {
    let jobId = String(knownJobId || '').trim();
    if (!jobId) {
        const query = new URLSearchParams({
            dialogId: String(dialogId || ''),
            checklistKey: String(currentChecklistKey || 'id'),
            itemId: String(itemId || '')
        });
        const response = await fetch(
            appUrl('api/checklist/yandex-structure-job')
            + '?' + query.toString(),
            { cache: 'no-store' }
        );
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.ok || !result.job) {
            throw new Error(result.error || 'Structure job не найден');
        }
        jobId = String(result.job.jobId || '');
    }

    const response = await fetch(
        appUrl('api/checklist/yandex-structure-job/retry'),
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ jobId })
        }
    );
    const result = await response.json().catch(() => ({}));
    if (!response.ok || !result.ok || !result.job) {
        throw new Error(result.error || 'Не удалось повторить создание папки');
    }

    applyYandexStructureJobToLocalItem(itemId, result.job);
    renderAll();
    scheduleYandexStructurePolling(itemId, jobId, 800);
}

async function getYandexStructureJobForItem(itemId, knownJobId = '') {
    const query = new URLSearchParams();
    if (String(knownJobId || '').trim()) {
        query.set('jobId', String(knownJobId || '').trim());
    } else {
        query.set('dialogId', String(dialogId || ''));
        query.set('checklistKey', String(currentChecklistKey || 'id'));
        query.set('itemId', String(itemId || ''));
    }
    const response = await fetch(
        appUrl('api/checklist/yandex-structure-job') + '?' + query.toString(),
        { cache: 'no-store' }
    );
    const result = await response.json().catch(() => ({}));
    if (!response.ok || !result.ok || !result.job) {
        throw new Error(result.error || 'Structure job не найден');
    }
    return result.job;
}

async function showYandexFolderConflict(itemId, knownJobId = '', knownJob = null) {
    const job = knownJob || await getYandexStructureJobForItem(
        itemId,
        knownJobId
    );
    const candidates = Array.isArray(job && job.result && job.result.conflictCandidates)
        ? job.result.conflictCandidates
        : [];

    const previous = document.getElementById('yandexFolderConflictOverlay');
    if (previous) previous.remove();

    const overlay = document.createElement('div');
    overlay.id = 'yandexFolderConflictOverlay';
    overlay.className = 'yandex-conflict-overlay';
    overlay.innerHTML = `
        <div class="yandex-conflict-dialog" role="dialog" aria-modal="true">
            <h3>Конфликт папок Яндекс.Диска</h3>
            <div>
                Приложение не объединяет и не удаляет папки автоматически.
                Откройте ссылки, оставьте одну правильную папку, затем нажмите
                «Проверить снова».
            </div>
            <div class="yandex-conflict-links">
                ${candidates.map((candidate, index) => `
                    <a
                        href="${esc(String(candidate.url || candidate.clientUrl || ''))}"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        ${esc(String(candidate.name || ('Папка ' + (index + 1))))}<br>
                        <small>${esc(String(candidate.path || ''))}</small>
                    </a>
                `).join('') || '<span>Ссылки на папки не получены.</span>'}
            </div>
            <div class="yandex-conflict-actions">
                <button type="button" data-role="yandex-conflict-close">Закрыть</button>
                <button type="button" data-role="yandex-conflict-recheck">Проверить снова</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    overlay.querySelector('[data-role="yandex-conflict-close"]')
        .addEventListener('click', () => overlay.remove());
    overlay.querySelector('[data-role="yandex-conflict-recheck"]')
        .addEventListener('click', async event => {
            const button = event.currentTarget;
            button.disabled = true;
            try {
                const response = await fetch(
                    appUrl('api/checklist/yandex-recovery/recheck'),
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            dialogId: String(dialogId || ''),
                            checklistKey: String(currentChecklistKey || 'id'),
                            itemId: String(itemId || '')
                        })
                    }
                );
                const result = await response.json().catch(() => ({}));
                if (result.job) {
                    applyYandexStructureJobToLocalItem(itemId, result.job);
                }
                if (result.conflict) {
                    overlay.remove();
                    await showYandexFolderConflict(
                        itemId,
                        result.job && result.job.jobId || '',
                        result.job
                    );
                    renderAll();
                    return;
                }
                if (!response.ok || !result.ok) {
                    throw new Error(result.error || 'Проверка папок не выполнена');
                }
                overlay.remove();
                renderAll();
                if (result.job && ['queued', 'running'].includes(result.job.status)) {
                    scheduleYandexStructurePolling(
                        itemId,
                        result.job.jobId,
                        700
                    );
                }
                setSaveState('', 'Конфликт устранён, восстановление запущено');
            } catch (error) {
                window.alert(error && error.message || 'Ошибка проверки папок');
                button.disabled = false;
            }
        });
}

async function retryYandexRecovery(itemId) {
    const response = await fetch(
        appUrl('api/checklist/yandex-recovery/retry'),
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dialogId: String(dialogId || ''),
                checklistKey: String(currentChecklistKey || 'id'),
                itemId: String(itemId || '')
            })
        }
    );
    const result = await response.json().catch(() => ({}));
    if (result.conflict) {
        if (result.job) applyYandexStructureJobToLocalItem(itemId, result.job);
        renderAll();
        await showYandexFolderConflict(
            itemId,
            result.job && result.job.jobId || '',
            result.job || null
        );
        return result;
    }
    if (!response.ok || !result.ok) {
        throw new Error(result.error || 'Не удалось повторить синхронизацию');
    }
    if (result.job && result.job.jobId) {
        applyYandexStructureJobToLocalItem(itemId, result.job);
        const status = String(result.job.status || '').toLowerCase();
        if (['queued', 'running'].includes(status)) {
            scheduleYandexStructurePolling(itemId, result.job.jobId, 700);
        }
    }
    renderAll();
    if (
        result.files
        && Number(result.files.requeued || 0) > 0
        && typeof loadChecklistByKey === 'function'
    ) {
        window.setTimeout(() => {
            loadChecklistByKey(currentChecklistKey).catch(() => {});
        }, 1200);
    }
    return result;
}

// Stage 7.1.1: document DOM actions and unified item toolbar.
function bindDocumentActions() {
    document.querySelectorAll('[data-role="upload"]').forEach(btn => {
        btn.addEventListener('click', function() {
            if (
                this.disabled
                || (
                    typeof isEditingAllowed === 'function'
                    && !isEditingAllowed()
                )
            ) {
                return;
            }

            const input = document.querySelector('[data-role="file-input"][data-item-id="' + this.dataset.itemId + '"]');
            if (input) input.click();
        });
    });

    document.querySelectorAll('[data-role="view-folder"]').forEach(btn => {
        btn.addEventListener('click', function () {
            if (
                this.disabled
                || (
                    typeof isEditingAllowed === 'function'
                    && !isEditingAllowed()
                )
            ) {
                return;
            }

            const folderUrl = this.dataset.folderUrl || '';
            if (!folderUrl) return;

            try {
                const targetUrl = new URL(
                    folderUrl,
                    window.location.href
                );
                const editSessionId = (
                    typeof getActiveEditSessionId === 'function'
                        ? getActiveEditSessionId()
                        : ''
                );
                if (editSessionId) {
                    targetUrl.searchParams.set(
                        'sessionId',
                        editSessionId
                    );
                }
                const actor = (
                    typeof getCurrentEditorIdentity === 'function'
                        ? getCurrentEditorIdentity()
                        : { userId: '', userName: '' }
                );
                if (actor.userId) {
                    targetUrl.searchParams.set('userId', actor.userId);
                }
                if (actor.userName) {
                    targetUrl.searchParams.set('userName', actor.userName);
                }
                window.open(targetUrl.href, '_blank');
            } catch (e) {
                console.log('open folder error:', e);
                setSaveState('error', 'Ошибка открытия папки');
            }
        });
    });

    document.querySelectorAll('[data-role="view-yandex-folder"]').forEach(btn => {
        btn.addEventListener('click', async function () {
            if (this.dataset.loading === '1' || this.disabled) return;

            const itemId = String(this.dataset.itemId || '').trim();
            const yandexStatus = String(
                this.dataset.yandexStatus || ''
            ).trim().toLowerCase();
            const hasMirrorErrors = this.dataset.hasMirrorErrors === '1';
            if (!itemId) return;

            this.dataset.loading = '1';
            this.classList.add('is-loading');

            try {
                if (yandexStatus === 'conflict') {
                    await showYandexFolderConflict(
                        itemId,
                        this.dataset.structureJobId || ''
                    );
                    return;
                }

                if (yandexStatus === 'error' || hasMirrorErrors) {
                    const recovery = await retryYandexRecovery(itemId);
                    if (!recovery.conflict) {
                        setSaveState('', 'Повтор неуспешной синхронизации запущен');
                    }
                    return;
                }

                const storedUrl = String(
                    this.dataset.yandexFolderUrl || ''
                ).trim();
                const storedPath = String(
                    this.dataset.yandexFolderPath || ''
                ).trim();

                if (storedUrl) {
                    window.open(storedUrl, '_blank', 'noopener,noreferrer');
                    return;
                }

                const query = new URLSearchParams({
                    dialogId: String(dialogId || ''),
                    checklistKey: String(currentChecklistKey || 'id'),
                    itemId
                });
                const response = await fetch(
                    appUrl('api/checklist/item-yandex-folder')
                    + '?' + query.toString(),
                    { cache: 'no-store' }
                );
                const result = await response.json().catch(() => ({}));

                if (!response.ok || !result.ok) {
                    throw new Error(
                        result.error
                        || 'Не удалось получить папку Яндекс.Диска'
                    );
                }

                const folderUrl = String(result.url || '').trim();
                const folderPath = String(result.path || storedPath).trim();

                if (folderUrl) {
                    window.open(folderUrl, '_blank', 'noopener,noreferrer');
                    return;
                }

                window.alert(
                    folderPath
                        ? (
                            'Папка Яндекс.Диска найдена, '
                            + 'но публичная ссылка ещё не готова.\n\n'
                            + folderPath
                        )
                        : 'Папка пункта на Яндекс.Диске пока недоступна.'
                );
            } catch (error) {
                console.log('open or retry item Yandex folder error:', error);
                window.alert(
                    error && error.message
                        ? error.message
                        : 'Ошибка операции с папкой Яндекс.Диска'
                );
            } finally {
                this.dataset.loading = '0';
                this.classList.remove('is-loading');
            }
        });
    });

    startVisibleYandexStructurePolling();

    document.querySelectorAll('[data-role="view-file"]').forEach(link => {
        link.addEventListener('click', function (event) {
            event.preventDefault();
            event.stopPropagation();

            const openUrl = this.dataset.openUrl || '';
            if (!openUrl) return;

            try {
                const absoluteUrl = new URL(openUrl, window.location.href).href;
                window.open(absoluteUrl, '_blank', 'noopener,noreferrer');
            } catch (e) {
                console.log('open file error:', e);
                setSaveState('error', 'Ошибка открытия файла');
            }
        });
    });

    document.querySelectorAll('[data-role="file-input"]').forEach(input => {
        input.addEventListener('change', async function() {
            const inputElement = this;
            const itemId = String(
                inputElement.dataset.itemId || ''
            );
            const files = Array.from(
                inputElement.files || []
            );

            if (!files.length) {
                debugLog(
                    'upload_frontend_input_empty',
                    {
                        dialogId,
                        checklistKey: currentChecklistKey,
                        itemId
                    }
                );
                return;
            }

            let editSessionId = '';

            try {
                editSessionId = await requireEditingSession(
                    'загрузка файлов'
                );
            } catch (error) {
                inputElement.value = '';
                setSaveState(
                    'error',
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            const selectedChecklistKey = String(
                currentChecklistKey || 'id'
            ).trim() || 'id';

            const selectedItem = items.find(item => (
                String(item && item.id || '')
                === itemId
            ));

            const batchContext = Object.freeze({
                dialogId: String(dialogId || ''),
                checklistKey: selectedChecklistKey,
                itemId,
                itemName: String(
                    selectedItem
                    && selectedItem.name
                    || ''
                ),
                initialStatus: normalizeStatus(
                    selectedItem
                    && selectedItem.status
                )
            });

            const uploadPlans = files.map(file => {
                const context = captureUploadContext(
                    itemId,
                    file,
                    selectedChecklistKey,
                    editSessionId
                );

                return {
                    context,
                    file,
                    promise: uploadDocument(
                        itemId,
                        file,
                        context
                    )
                };
            });

            debugLog(
                'upload_frontend_batch_enqueued',
                {
                    dialogId: batchContext.dialogId,
                    checklistKey: (
                        batchContext.checklistKey
                    ),
                    itemId: batchContext.itemId,
                    itemName: batchContext.itemName,
                    filesCount: uploadPlans.length,
                    uploadIds: uploadPlans.map(
                        plan => plan.context.uploadId
                    ),
                    files: uploadPlans.map(plan => ({
                        name: plan.context.fileName,
                        size: plan.context.fileSize,
                        type: plan.context.fileType
                    }))
                }
            );

            try {
                const settled = await Promise.allSettled(
                    uploadPlans.map(plan => (
                        plan.promise.then(result => ({
                            context: plan.context,
                            file: plan.file,
                            result
                        }))
                    ))
                );

                const successfulUploads = settled
                    .filter(entry => (
                        entry.status === 'fulfilled'
                    ))
                    .map(entry => entry.value);

                const failedUploads = settled.filter(
                    entry => entry.status === 'rejected'
                );

                if (successfulUploads.length) {
                    const refreshedChecklist = (
                        await loadChecklistSnapshotForUpload(
                            batchContext
                        )
                    );

                    recordCompletedUploadBatch(
                        batchContext,
                        successfulUploads,
                        refreshedChecklist
                    );
                }

                debugLog(
                    'upload_frontend_batch_completed',
                    {
                        dialogId: batchContext.dialogId,
                        checklistKey: (
                            batchContext.checklistKey
                        ),
                        itemId: batchContext.itemId,
                        filesCount: uploadPlans.length,
                        successfulCount: (
                            successfulUploads.length
                        ),
                        failedCount: failedUploads.length,
                        currentChecklistKey
                    }
                );

                if (failedUploads.length) {
                    const firstError = (
                        failedUploads[0].reason
                    );

                    setSaveState(
                        'error',
                        (
                            failedUploads.length === 1
                                ? 'Ошибка загрузки файла'
                                : (
                                    'Не загружено файлов: '
                                    + failedUploads.length
                                )
                        )
                    );

                    console.log(
                        'upload batch error:',
                        firstError
                    );
                } else if (
                    typeof updateSaveStateBySession
                    === 'function'
                ) {
                    updateSaveStateBySession();
                } else {
                    setSaveState('', 'Сохранено');
                }

            } catch (error) {
                console.log(error);

                debugLog(
                    'upload_frontend_batch_exception',
                    {
                        dialogId: batchContext.dialogId,
                        checklistKey: (
                            batchContext.checklistKey
                        ),
                        itemId: batchContext.itemId,
                        filesCount: uploadPlans.length,
                        error: String(
                            error
                            && error.message
                            || error
                        )
                    }
                );

                setSaveState(
                    'error',
                    'Ошибка загрузки файлов'
                );

            } finally {
                inputElement.value = '';
            }
        });
    });

    if (
        window.ChecklistPopupDocumentReplacement
        && typeof window
            .ChecklistPopupDocumentReplacement
            .bind === 'function'
    ) {
        window.ChecklistPopupDocumentReplacement.bind();
    }
}

window.ChecklistPopupDocumentActions = Object.freeze({
    bind: bindDocumentActions
});
