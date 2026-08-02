            const popupBootstrap = window.CHECKLIST_POPUP_BOOTSTRAP;
            const popupApi = window.ChecklistPopupApi;
            const popupBitrix = window.ChecklistPopupBitrix;

            if (!popupBootstrap || !popupApi || !popupBitrix) {
                throw new Error('Popup JavaScript infrastructure is not initialized');
            }

            const dialogId = String(popupBootstrap.dialogId || '');
            const projectRootYandexPath = String(
                popupBootstrap.projectRootYandexPath || ''
            );
            let projectRootYandexUrl = String(
                popupBootstrap.projectRootYandexUrl || ''
            );

            let projectRootYandexPrepared = Boolean(
                popupBootstrap.projectRootYandexPrepared
            );
            let projectRootYandexPreparing = false;
            let stageYandexFoldersByKey = (
                popupBootstrap.stageYandexFoldersByKey
                && typeof popupBootstrap.stageYandexFoldersByKey === 'object'
            )
                ? popupBootstrap.stageYandexFoldersByKey
                : {};
            const checklistLayoutMetaByKey = (
                popupBootstrap.checklistLayoutMetaByKey
                && typeof popupBootstrap.checklistLayoutMetaByKey === 'object'
            )
                ? popupBootstrap.checklistLayoutMetaByKey
                : {};

            let rawGroups = popupBootstrap.groups;
            let rawProjectChecklists = popupBootstrap.projectChecklists;
            let rawItems = popupBootstrap.items;
            let collabTitle = String(popupBootstrap.collabTitle || '');

            let groups = Array.isArray(rawGroups) ? rawGroups : [];
            let projectChecklists = Array.isArray(rawProjectChecklists) ? rawProjectChecklists : [];
            let items = Array.isArray(rawItems) ? rawItems : [];
            let currentOrderVersion = Number(
                popupBootstrap.orderVersion || 0
            );

            let currentChecklistKey = String(
                popupBootstrap.checklistKey || 'id'
            );
            let checklistTitle = String(
                popupBootstrap.checklistTitle || 'Чек-лист'
            );
            let checklistCache = {};
            let sessionChanges = [];
            let currentEditor = {
                id: "",
                name: ""
            };
            window.currentEditor = currentEditor;
            let currentEditorReady = false;
            let currentEditorReadyPromise = null;
                        const saveStateEl = document.getElementById('saveState');
                        let uploadJobPollTimer = null;
            const leftTableBodyEl = document.getElementById('leftTableBody');
            const middleTableBodyEl = document.getElementById('middleTableBody');
            const rightTableBodyEl = document.getElementById('rightTableBody');
            const progressValueEl = document.getElementById('progressValue');
            const progressBarEl = document.getElementById('progressBar');
            const progressBoxEl = document.querySelector('.progress-box');
            const popupTitleEl = document.getElementById('popupTitle');
            const projectRootFolderBoxEl = document.getElementById('projectRootFolderBox');
            const stageFolderBoxEl = document.getElementById('stageFolderBox');
            const projectChecklistListEl = document.getElementById('projectChecklistList');
            const tablePanels = document.querySelectorAll('.table-panel');
            const tablesGridEl = document.querySelector('.tables-grid');
            const leftTableEl = tablePanels[0] ? tablePanels[0].querySelector('.table') : null;
            const middleTableEl = tablePanels[1] ? tablePanels[1].querySelector('.table') : null;
            const rightTableEl = tablePanels[2] ? tablePanels[2].querySelector('.table') : null;
            const idTableShellHtml = leftTableEl ? leftTableEl.innerHTML : '';
            const idDateVisibility = { 1: false, 2: false, 3: false };
            const oprDateVisibility = { 1: false };
            const conceptDateVisibility = { 1: false };
            const debugLastEventEl = document.getElementById('debugLastEvent');
            const debugPanelEl = document.getElementById('debugPanel');
            const debugLogsLinkEl = document.getElementById('debugLogsLink');
            const adminPanelLinkEl = document.getElementById('adminPanelLink');
            const debugStopYandexWarmupBtn = document.getElementById('debugStopYandexWarmupBtn');
            const debugYandexWarmupStopStateEl = document.getElementById('debugYandexWarmupStopState');
            const allowedDebugUserIds = new Set(['138', '18']);
            const fileDeleteAllowedUserIds = new Set(
                Array.isArray(popupBootstrap.fileDeleteAllowedUserIds)
                    ? popupBootstrap.fileDeleteAllowedUserIds.map(value => String(value))
                    : []
            );

            function getFileDeleteActor() {
                return {
                    id: String(currentEditor && currentEditor.id || '').trim(),
                    name: String(currentEditor && currentEditor.name || '').trim() || 'Пользователь'
                };
            }

            function canCurrentUserDeleteFiles() {
                const actor = getFileDeleteActor();
                return fileDeleteAllowedUserIds.has(String(actor.id || '').trim());
            }

            function showFileDeleteForbiddenAlert() {
                alert('У вас недостаточно прав на удаление файлов');
            }
            function updateDebugPanelAccess() {
                const currentUserId = String(currentEditor.id || '');
                if (debugPanelEl) {
                    debugPanelEl.style.display = allowedDebugUserIds.has(currentUserId) ? '' : 'none';
                }
                if (debugLogsLinkEl) {
                    debugLogsLinkEl.href = 'debug/logs?userId=' + encodeURIComponent(currentUserId);
                }
                if (adminPanelLinkEl) {
                    adminPanelLinkEl.href = appUrl('admin') + '?userId=' + encodeURIComponent(currentUserId);
                }
            }
            const APP_BASE_PATH = popupApi.basePath;
            const APP_BASE_URL = popupApi.baseUrl;
            const appUrl = popupApi.url;
            let closeSummarySent = false;
            let sessionDirty = false;
            let inlineItemRenameState = null;
            let pendingInlineItemRename = null;

            function setSaveState(mode, text) {
                saveStateEl.classList.remove('saving', 'error');
                if (mode === 'saving') saveStateEl.classList.add('saving');
                if (mode === 'error') saveStateEl.classList.add('error');
                saveStateEl.textContent = text;
            }
            function esc(v) {
                if (v === null || v === undefined) return '';
                return String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
            }
            function toInputDate(value) {
                if (!value) return '';
                const parts = value.split('.');
                if (parts.length !== 3) return '';
                return `${parts[2]}-${parts[1]}-${parts[0]}`;
            }
            function fromInputDate(value) {
                if (!value) return '';
                const parts = value.split('-');
                if (parts.length !== 3) return '';
                return `${parts[2]}.${parts[1]}.${parts[0]}`;
            }
            function normalizeStatus(status) {
                const s = String(status || '').trim();
                if (s === 'Есть') return 'Есть';
                if (s === 'Нет') return 'Нет';
                if (s === 'Не требуется') return 'Не требуется';
                return '';
            }
            function indicatorClass(status) {
                const s = normalizeStatus(status);
                if (s === 'Есть') return 'status-indicator green';
                if (s === 'Не требуется') return 'status-indicator gray';
                return 'status-indicator';
            }

            function getStatusCircleTitle(status) {
                const s = normalizeStatus(status);
                if (s === 'Есть') {
                    return 'Статус «Есть». Нажмите, чтобы отметить «Не требуется»';
                }
                if (s === 'Не требуется') {
                    return 'Статус «Не требуется». Нажмите, чтобы вернуть пункт';
                }
                return 'Статус «Нет». Нажмите, чтобы отметить «Есть»';
            }

            function getItemDocuments(item) {
                const docs = Array.isArray(item && item.documents) ? item.documents : [];
                if (docs.length) {
                    return docs;
                }

                const legacyUrl = String(item && item.documentUrl || '').trim();
                const legacyName = String(item && item.documentName || '').trim();

                if (legacyUrl || legacyName) {
                    return [{
                        id: 'legacy_' + String(item && item.id || ''),
                        name: legacyName || 'Файл',
                        path: legacyUrl,
                        fileUrl: legacyUrl,
                        previewUrl: legacyUrl,
                        size: 0,
                        modifiedAt: '',
                        source: 'local'
                    }];
                }

                return [];
            }

            function stopUploadJobPolling() {
                if (uploadJobPollTimer) {
                    clearInterval(uploadJobPollTimer);
                    uploadJobPollTimer = null;
                }
            }

            function getUploadJobStageText(job) {
                const status = String(job && job.status || '');
                const stage = String(job && job.stage || '');

                if (status === 'queued') return 'Файл сохранён. Ожидает синхронизации...';
                if (status === 'running' && stage === 'folder_prepare') return 'Готовим папку Яндекс.Диска...';
                if (status === 'running' && stage === 'yandex_upload') return 'Загружаем копию на Яндекс.Диск...';
                if (status === 'synced') return 'Файл загружен и синхронизирован';
                if (status === 'skipped' && stage === 'yandex_disabled') return 'Файл сохранён';
                if (status === 'error') return 'Файл сохранён, ошибка синхронизации';
                if (status === 'cancelled') return 'Загрузка отменена';
                return 'Обрабатываем файл...';
            }

            function pollUploadJobStatus(jobId, fileName) {
                stopUploadJobPolling();

                if (!jobId) {
                    completeUploadProgress(fileName, 'Файл сохранён');
                    return;
                }

                uploadJobPollTimer = setInterval(async function () {
                    try {
                        const response = await fetch(
                            appUrl('api/checklist/upload-job-status') +
                            '?jobId=' + encodeURIComponent(jobId)
                        );

                        const result = await response.json().catch(() => ({}));

                        if (!response.ok || !result.ok) {
                            return;
                        }

                        const status = String(result.status || '');
                        const rawJobPercent = Number(result.progressPercent || 0);
                        const displayPercent = status === 'queued'
                            ? 82
                            : status === 'running'
                                ? Math.max(84, Math.min(98, 75 + Math.round(rawJobPercent * 0.23)))
                                : 100;

                        updateUploadProgress(fileName, displayPercent, getUploadJobStageText(result));

                        if (['synced', 'skipped', 'error', 'cancelled', 'deleted'].includes(status)) {
                            stopUploadJobPolling();

                            if (status === 'error') {
                                failUploadProgress(fileName, result.error || 'Ошибка синхронизации');
                            } else {
                                completeUploadProgress(fileName, getUploadJobStageText(result));
                            }
                        }

                    } catch (e) {
                        console.log('upload job polling error:', e);
                    }
                }, 900);
            }

            function formatFileSize(size) {
                const value = Number(size || 0);
                if (!value || value <= 0) return '';

                const units = ['Б', 'КБ', 'МБ', 'ГБ'];
                let current = value;
                let unitIndex = 0;

                while (current >= 1024 && unitIndex < units.length - 1) {
                    current /= 1024;
                    unitIndex += 1;
                }

                if (unitIndex === 0) {
                    return Math.round(current) + ' ' + units[unitIndex];
                }

                if (current >= 100) return current.toFixed(0) + ' ' + units[unitIndex];
                if (current >= 10) return current.toFixed(1) + ' ' + units[unitIndex];
                return current.toFixed(2) + ' ' + units[unitIndex];
            }

            function confirmStatusNoWithFiles(itemName, documents) {
                const docs = Array.isArray(documents) ? documents : [];
                if (!docs.length) {
                    return true;
                }

                if (!canCurrentUserDeleteFiles()) {
                    showFileDeleteForbiddenAlert();
                    return false;
                }

                const safeItemName = String(itemName || 'пункт').trim() || 'пункт';

                return window.confirm(
                    'В пункте "' + safeItemName + '" уже загружены файлы.\n\n' +
                    'При выборе статуса "Нет" эти файлы будут удалены.\n\n' +
                    'Продолжить?'
                );
            }

            function renderTitle() {
                if (collabTitle) {
                    popupTitleEl.innerHTML = esc(checklistTitle) + ' <small>— ' + esc(collabTitle) + '</small>';
                } else {
                    popupTitleEl.textContent = checklistTitle;
                }
            }
            function fetchCurrentUserIfPossible() {
                if (currentEditorReadyPromise) {
                    return currentEditorReadyPromise;
                }

                currentEditorReadyPromise = (async function () {
                    try {
                        const data = await popupBitrix.getCurrentUser();

                        if (data && typeof data === 'object') {
                            const fullName = [data.NAME, data.LAST_NAME]
                                .filter(Boolean)
                                .join(' ')
                                .trim();

                            currentEditor = {
                                id: String(data.ID || ''),
                                name: fullName || String(data.NAME || '') || ''
                            };
                            window.currentEditor = currentEditor;
                        }
                    } catch (error) {
                        console.log('fetchCurrentUserIfPossible skipped:', error);
                    } finally {
                        currentEditorReady = true;
                        updateDebugPanelAccess();
                    }

                    return currentEditor;
                })();

                return currentEditorReadyPromise;
            }
            function setDebugText(text) {
                if (debugLastEventEl) {
                    debugLastEventEl.textContent = text;
                }
            }
            function debugLog(event, payload = {}, useBeacon = false) {
                const body = JSON.stringify({
                    event,
                    dialogId,
                    checklistKey: currentChecklistKey,
                    payload,
                    href: window.location.href,
                    ts: new Date().toISOString()
                });

                setDebugText(event);

                try {
                    const url = APP_BASE_URL + '/api/debug/event';

                    if (useBeacon && navigator.sendBeacon) {
                        const blob = new Blob([body], { type: 'application/json' });
                        const ok = navigator.sendBeacon(url, blob);
                        setDebugText(event + ' | beacon=' + ok);
                        return;
                    }

                    fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body
                    })
                    .then(r => {
                        setDebugText(event + ' | http=' + r.status);
                    })
                    .catch(err => {
                        console.log('debugLog fetch error:', err);
                        setDebugText(event + ' | fetch error');
                    });
                } catch (e) {
                    console.log('debugLog error:', e);
                    setDebugText(event + ' | js error');
                }
            }

            async function stopCurrentYandexWarmupFromDebugPanel() {
                await fetchCurrentUserIfPossible();

                const actor = getFileDeleteActor();

                if (!allowedDebugUserIds.has(String(actor.id || '').trim())) {
                    alert('Остановка warmup доступна только техническим пользователям');
                    return;
                }

                if (!window.confirm('Остановить создание папок Яндекс.Диска для текущей коллабы? Уже созданные папки останутся на месте.')) {
                    return;
                }

                if (debugStopYandexWarmupBtn) {
                    debugStopYandexWarmupBtn.disabled = true;
                    debugStopYandexWarmupBtn.style.opacity = '0.65';
                }

                if (debugYandexWarmupStopStateEl) {
                    debugYandexWarmupStopStateEl.textContent = 'Отправляем команду остановки...';
                }

                try {
                    const response = await fetch(appUrl('api/project-yandex-warmup/stop'), {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            dialogId,
                            checklistKey: currentChecklistKey,
                            userId: actor.id,
                            userName: actor.name
                        })
                    });

                    const result = await response.json().catch(() => ({}));

                    if (!response.ok || !result.ok) {
                        throw new Error(result.error || 'warmup stop failed');
                    }

                    debugLog('debug_yandex_warmup_stop_requested', result);

                    if (debugYandexWarmupStopStateEl) {
                        const stateText = result.wasRunning
                            ? 'Остановка запрошена. Текущая папка завершится, следующая уже не начнётся.'
                            : result.wasQueued
                                ? 'Проект убран из очереди.'
                                : 'Команда остановки принята.';

                        debugYandexWarmupStopStateEl.textContent = stateText;
                    }

                } catch (e) {
                    console.log('stop warmup error:', e);

                    if (debugYandexWarmupStopStateEl) {
                        debugYandexWarmupStopStateEl.textContent = 'Ошибка остановки: ' + String(e && e.message || e);
                    }

                    debugLog('debug_yandex_warmup_stop_failed', {
                        message: String(e && e.message || e)
                    });

                } finally {
                    if (debugStopYandexWarmupBtn) {
                        debugStopYandexWarmupBtn.disabled = false;
                        debugStopYandexWarmupBtn.style.opacity = '1';
                    }
                }
            }

            function logRenderState(stage) {
                debugLog('render_state', {
                    stage,
                    groupsType: typeof rawGroups,
                    itemsType: typeof rawItems,
                    projectChecklistsType: typeof rawProjectChecklists,
                    groupsIsArray: Array.isArray(groups),
                    itemsIsArray: Array.isArray(items),
                    projectChecklistsIsArray: Array.isArray(projectChecklists),
                    groupsLength: groups.length,
                    itemsLength: items.length,
                    projectChecklistsLength: projectChecklists.length,
                    leftTableExists: !!leftTableBodyEl,
                    rightTableExists: !!rightTableBodyEl,
                    titleExists: !!popupTitleEl,
                    sidePanelExists: !!projectChecklistListEl
                });
            }

            function logRenderError(stage, error) {
                const message = (error && error.message) ? error.message : String(error || 'unknown error');
                const stack = (error && error.stack) ? error.stack : '';

                setDebugText(stage + ' | ERROR: ' + message);

                debugLog('render_error', {
                    stage,
                    message,
                    stack
                });
            }
            function deepClone(value) {
                return JSON.parse(JSON.stringify(value));
            }

            function mergeDocumentRefreshSnapshot(
                localSnapshot,
                refreshedSnapshot,
                itemId,
                pendingChanges
            ) {
                const refreshApi = window.ChecklistPopupDocumentRefresh;

                if (
                    !refreshApi
                    || typeof refreshApi.mergeSnapshot !== 'function'
                ) {
                    throw new Error(
                        'ChecklistPopupDocumentRefresh is not initialized'
                    );
                }

                return refreshApi.mergeSnapshot(
                    localSnapshot,
                    refreshedSnapshot,
                    itemId,
                    pendingChanges
                );
            }

            function mergeUploadResponseStatusSnapshot(
                refreshedSnapshot,
                itemId,
                successfulUploads
            ) {
                const refreshApi = window.ChecklistPopupDocumentRefresh;

                if (
                    !refreshApi
                    || typeof refreshApi.mergeUploadResponseStatus
                    !== 'function'
                ) {
                    throw new Error(
                        'ChecklistPopupDocumentRefresh upload status '
                        + 'merge is not initialized'
                    );
                }

                return refreshApi.mergeUploadResponseStatus(
                    refreshedSnapshot,
                    itemId,
                    successfulUploads
                );
            }

            function buildChecklistSnapshot() {
                return {
                    checklistKey: currentChecklistKey,
                    title: checklistTitle,
                    collabTitle,
                    groups: deepClone(groups),
                    projectChecklists: deepClone(projectChecklists),
                    items: deepClone(items),
                    orderVersion: Number(currentOrderVersion || 0)
                };
            }

            function syncChecklistCache() {
                checklistCache[currentChecklistKey] = buildChecklistSnapshot();
            }

            function applyChecklistData(data) {
                const nextData = data || {};
                const nextKey = String(nextData.checklistKey || currentChecklistKey || 'id').trim() || 'id';

                currentChecklistKey = nextKey;
                checklistTitle = String(nextData.title || getChecklistDefaultTitle(nextKey));
                collabTitle = String(nextData.collabTitle || collabTitle || '');

                rawGroups = Array.isArray(nextData.groups) ? nextData.groups : [];
                rawProjectChecklists = Array.isArray(nextData.projectChecklists)
                    ? nextData.projectChecklists
                    : rawProjectChecklists;
                rawItems = Array.isArray(nextData.items) ? nextData.items : [];

                groups = Array.isArray(rawGroups) ? rawGroups : [];
                projectChecklists = Array.isArray(rawProjectChecklists) ? rawProjectChecklists : [];
                items = Array.isArray(rawItems) ? rawItems : [];
                currentOrderVersion = Number(
                    nextData.orderVersion || 0
                );

                document.title = collabTitle
                    ? checklistTitle + ' — ' + collabTitle
                    : checklistTitle;
            }

            async function flushCurrentChecklistSummary(reason = 'checklist_switch') {
                syncChecklistCache();
                debugLog('close_summary_switch_skipped', {
                    checklistKey: currentChecklistKey,
                    reason,
                    changesCount: sessionChanges.length,
                    dirty: !!sessionDirty
                });
            }

            async function reloadCurrentChecklistFromServer() {
                const response = await fetch(
                    appUrl('api/checklist') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey)
                );
                const result = await response.json();

                if (!response.ok) {
                    throw new Error(result.error || 'reload checklist failed');
                }

                applyChecklistData(result);
                return result;
            }

            async function fetchChatTitleIfMissing() {
                if (collabTitle) {
                    renderTitle();
                    return;
                }

                try {
                    const data = await popupBitrix.getDialog(dialogId);

                    if (!data || typeof data !== 'object') {
                        renderTitle();
                        return;
                    }

                    let title = (
                        data.title
                        || data.name
                        || (data.dialog && (data.dialog.title || data.dialog.name))
                        || (data.chat && (data.chat.title || data.chat.name))
                        || ''
                    );
                    title = String(title || '').trim();

                    if (!title) {
                        renderTitle();
                        return;
                    }

                    collabTitle = title;
                    renderTitle();
                    debugLog('chat_title_loaded', {
                        title
                    });

                    try {
                        const editSessionId = (
                            typeof requireEditingSession === 'function'
                                ? await requireEditingSession(
                                    'обновление названия проекта'
                                )
                                : ''
                        );

                        if (!editSessionId) {
                            throw new Error(
                                'Сессия редактирования не готова'
                            );
                        }

                        await fetch(appUrl('api/checklist/update-meta'), {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({
                                dialogId,
                                checklistKey: currentChecklistKey,
                                field: 'collabTitle',
                                value: title,
                                sessionId: editSessionId,
                                requireEditSession: true,
                                actingUserId: (
                                    typeof getCurrentEditorIdentity === 'function'
                                        ? getCurrentEditorIdentity().userId
                                        : ''
                                ),
                                actingUserName: (
                                    typeof getCurrentEditorIdentity === 'function'
                                        ? getCurrentEditorIdentity().userName
                                        : 'Пользователь'
                                )
                            })
                        });
                    } catch (error) {
                        console.log('save collabTitle error:', error);
                    }
                } catch (error) {
                    console.log('Bitrix dialog title skipped:', error);
                    renderTitle();
                }
            }
            function calculateProgress() {
                if (!progressValueEl || !progressBarEl) {
                    return;
                }

                const activeItems = items.filter(x => normalizeStatus(x.status) !== 'Не требуется');
                const completedItems = activeItems.filter(x => normalizeStatus(x.status) === 'Есть');
                const activeCount = activeItems.length;
                const completedCount = completedItems.length;
                const percent = activeCount ? Math.round((completedCount / activeCount) * 100) : 0;

                progressValueEl.textContent = percent + '%';
                progressBarEl.style.width = percent + '%';
            }
            function buildItemNameCell(item, indicatorClassName) {
                const itemName = String(item && item.name || '');
                return `
                    <div class="cell-name">
                        <button
                            type="button"
                            class="item-drag-handle"
                            data-role="item-drag-handle"
                            data-item-id="${esc(item && item.id || '')}"
                            title="Перетащить пункт"
                            aria-label="Перетащить пункт ${esc(itemName)}"
                            ${disabledAttr()}
                        >⋮⋮</button>
                        <button
                            type="button"
                            class="${esc(indicatorClassName || 'status-indicator')}"
                            data-role="cycle-item-status"
                            data-item-id="${esc(item && item.id || '')}"
                            data-item-status="${esc(normalizeStatus(item && item.status))}"
                            title="${esc(getStatusCircleTitle(item && item.status))}"
                            aria-label="${esc(getStatusCircleTitle(item && item.status))}"
                            ${disabledAttr()}
                        ></button>
                        <div class="item-name-wrap">
                            <button
                                type="button"
                                class="item-name item-name-edit-trigger"
                                data-role="inline-rename-item"
                                data-item-id="${esc(item && item.id || '')}"
                                title="Нажмите на название, чтобы переименовать"
                                aria-label="Переименовать пункт ${esc(itemName)}"
                                ${disabledAttr()}
                            >${esc(itemName)}</button>
                        </div>
                    </div>
                `;
            }

            async function renameItem(itemId, name, checklistKey = currentChecklistKey) {
                setSaveState('saving', 'Переименовываем...');
                const editSessionId = await requireEditingSession(
                    'переименование пункта'
                );
                const identity = typeof getCurrentEditorIdentity === 'function'
                    ? getCurrentEditorIdentity()
                    : { userId: '', userName: 'Пользователь' };
                const response = await fetch(appUrl('api/checklist/rename-item'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        dialogId,
                        checklistKey,
                        itemId,
                        name,
                        sessionId: editSessionId,
                        requireEditSession: true,
                        actingUserId: identity.userId || '',
                        actingUserName: identity.userName || 'Пользователь'
                    })
                });
                const result = await response.json();
                if (!response.ok || !result.ok) {
                    throw new Error(result.error || 'rename item failed');
                }
                currentOrderVersion = Number(
                    result.orderVersion || currentOrderVersion || 0
                );
                setSaveState('', result.nameAdjusted
                    ? `Сохранено как «${result.finalName}»`
                    : 'Сохранено');
                return result;
            }

            function findInlineRenameTrigger(itemId) {
                const normalizedId = String(itemId || '');
                return Array.from(
                    document.querySelectorAll('[data-role="inline-rename-item"]')
                ).find(element => String(element.dataset.itemId || '') === normalizedId) || null;
            }

            function replaceCachedChecklistItem(checklistKey, updatedItem) {
                const key = String(checklistKey || '');
                const cached = checklistCache[key];
                if (!cached || !Array.isArray(cached.items) || !updatedItem) {
                    return;
                }
                const index = cached.items.findIndex(item => item.id === updatedItem.id);
                if (index >= 0) {
                    cached.items[index] = Object.assign({}, cached.items[index], updatedItem);
                }
            }

            function restoreInlineItemNameLabel(state, displayName) {
                if (!state || !state.input || !document.body.contains(state.input)) {
                    return;
                }
                const itemName = String(displayName || state.originalName || '');
                const trigger = document.createElement('button');
                trigger.type = 'button';
                trigger.className = 'item-name item-name-edit-trigger';
                trigger.dataset.role = 'inline-rename-item';
                trigger.dataset.itemId = String(state.itemId || '');
                trigger.title = 'Нажмите на название, чтобы переименовать';
                trigger.setAttribute(
                    'aria-label',
                    'Переименовать пункт ' + itemName
                );
                trigger.disabled = !isEditingAllowed();
                trigger.textContent = itemName;
                state.input.replaceWith(trigger);

                const eventApi = window.ChecklistPopupEventCommon;
                if (
                    eventApi
                    && typeof eventApi.bindItemRenameEvents === 'function'
                ) {
                    eventApi.bindItemRenameEvents();
                }
            }

            function resumePendingInlineItemRename() {
                const pending = pendingInlineItemRename;
                pendingInlineItemRename = null;
                if (!pending || pending.checklistKey !== currentChecklistKey) {
                    return;
                }

                window.setTimeout(function () {
                    const item = items.find(entry => entry.id === pending.itemId);
                    const trigger = findInlineRenameTrigger(pending.itemId);
                    if (item && trigger && !trigger.disabled) {
                        startInlineItemRename(item, trigger);
                    }
                }, 0);
            }

            function renderAfterInlineItemRename() {
                renderAll();
                resumePendingInlineItemRename();
            }

            function focusInlineRenameInput(input, selectAll = false) {
                if (!input || !document.body.contains(input)) {
                    return;
                }
                window.setTimeout(function () {
                    if (!document.body.contains(input)) {
                        return;
                    }
                    input.focus();
                    if (selectAll && typeof input.select === 'function') {
                        input.select();
                    }
                }, 0);
            }

            function startInlineItemRename(item, trigger) {
                if (!item || !trigger || !isEditingAllowed()) {
                    return false;
                }

                if (inlineItemRenameState) {
                    if (inlineItemRenameState.itemId === item.id) {
                        focusInlineRenameInput(inlineItemRenameState.input, false);
                        return true;
                    }

                    pendingInlineItemRename = {
                        itemId: item.id,
                        checklistKey: currentChecklistKey
                    };
                    if (
                        inlineItemRenameState.input
                        && document.body.contains(inlineItemRenameState.input)
                        && !inlineItemRenameState.saving
                    ) {
                        inlineItemRenameState.input.blur();
                    }
                    return true;
                }

                const input = document.createElement('input');
                input.type = 'text';
                input.className = 'item-rename-input';
                input.value = String(item.name || '');
                input.maxLength = 160;
                input.dataset.itemId = String(item.id || '');
                input.setAttribute('aria-label', 'Новое название пункта');
                input.setAttribute('autocomplete', 'off');

                const state = {
                    itemId: String(item.id || ''),
                    checklistKey: currentChecklistKey,
                    originalName: String(item.name || ''),
                    input,
                    saving: false,
                    cancelled: false,
                    promise: null
                };
                inlineItemRenameState = state;
                trigger.replaceWith(input);

                input.addEventListener('keydown', function (event) {
                    if (event.key === 'Enter') {
                        event.preventDefault();
                        input.blur();
                        return;
                    }
                    if (event.key === 'Escape') {
                        event.preventDefault();
                        state.cancelled = true;
                        input.blur();
                    }
                });

                input.addEventListener('blur', function () {
                    finishInlineItemRename(state).catch(function (error) {
                        console.log('inline item rename error:', error);
                    });
                });

                focusInlineRenameInput(input, true);
                return true;
            }

            function finishInlineItemRename(state, options = {}) {
                if (!state || state !== inlineItemRenameState) {
                    return Promise.resolve(null);
                }

                if (options.discard) {
                    state.cancelled = true;
                }
                if (state.promise) {
                    return state.promise;
                }
                if (state.cancelled) {
                    inlineItemRenameState = null;
                    restoreInlineItemNameLabel(state, state.originalName);
                    resumePendingInlineItemRename();
                    return Promise.resolve(null);
                }

                const requestedName = String(state.input && state.input.value || '');
                if (!requestedName.trim()) {
                    const error = new Error('Название пункта не может быть пустым');
                    setSaveState('error', error.message);
                    focusInlineRenameInput(state.input, false);
                    return Promise.reject(error);
                }

                if (requestedName === state.originalName) {
                    inlineItemRenameState = null;
                    restoreInlineItemNameLabel(state, state.originalName);
                    resumePendingInlineItemRename();
                    return Promise.resolve(null);
                }

                state.saving = true;
                state.input.disabled = true;
                state.input.classList.add('is-saving');

                state.promise = (async function () {
                    try {
                        const result = await renameItem(
                            state.itemId,
                            requestedName,
                            state.checklistKey
                        );
                        if (!result || !result.item) {
                            throw new Error('rename item result is incomplete');
                        }

                        if (state.checklistKey === currentChecklistKey) {
                            replaceItem(result.item);
                        } else {
                            replaceCachedChecklistItem(state.checklistKey, result.item);
                        }
                        pushSessionChange(
                            result.item.id,
                            result.item.name,
                            'name',
                            state.originalName,
                            result.item.name
                        );
                        debugLog('item_renamed_inline', {
                            itemId: result.item.id,
                            oldName: state.originalName,
                            requestedName,
                            finalName: result.item.name,
                            nameAdjusted: !!result.nameAdjusted
                        });

                        inlineItemRenameState = null;
                        renderAfterInlineItemRename();
                        return result;
                    } catch (error) {
                        state.saving = false;
                        state.promise = null;
                        if (state === inlineItemRenameState) {
                            state.input.disabled = false;
                            state.input.classList.remove('is-saving');
                            setSaveState(
                                'error',
                                error && error.message
                                    ? error.message
                                    : 'Ошибка переименования пункта'
                            );
                            focusInlineRenameInput(state.input, false);
                        }
                        throw error;
                    }
                })();

                return state.promise;
            }

            async function settleInlineItemRename(options = {}) {
                const state = inlineItemRenameState;
                if (!state) {
                    return null;
                }

                if (options.discard) {
                    if (state.saving && state.promise) {
                        try {
                            await state.promise;
                        } catch (error) {
                            // Rollback may continue even when the rename request failed.
                        }
                        return null;
                    }
                    return finishInlineItemRename(state, { discard: true });
                }

                return finishInlineItemRename(state);
            }

            async function updateItem(itemId, field, value, checklistKey = currentChecklistKey) {
                setSaveState('saving', 'Сохраняем...');

                const editSessionId = await requireEditingSession(
                    'изменение пункта'
                );

                if (typeof fetchCurrentUserIfPossible === 'function') {
                    await fetchCurrentUserIfPossible();
                }

                const actor = getFileDeleteActor();

                const response = await fetch(appUrl('api/checklist/update-item'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        dialogId,
                        checklistKey,
                        itemId,
                        field,
                        value,
                        actingUserId: actor.id,
                        actingUserName: actor.name,
                        sessionId: editSessionId,
                        requireEditSession: true
                    })
                });

                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'save failed');
                currentOrderVersion = Number(
                    result.orderVersion || currentOrderVersion || 0
                );
                setSaveState('', 'Сохранено');
                return result;
            }
            async function addItem(groupId, name, checklistKey = currentChecklistKey) {
                setSaveState('saving', 'Сохраняем...');

                const editSessionId = await requireEditingSession(
                    'добавление пункта'
                );

                const response = await fetch(appUrl('api/checklist/add-item'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        dialogId,
                        checklistKey,
                        groupId,
                        name,
                        sessionId: editSessionId,
                        requireEditSession: true,
                        actingUserId: (
                            typeof getCurrentEditorIdentity === 'function'
                                ? getCurrentEditorIdentity().userId
                                : ''
                        ),
                        actingUserName: (
                            typeof getCurrentEditorIdentity === 'function'
                                ? getCurrentEditorIdentity().userName
                                : 'Пользователь'
                        )
                    })
                });
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'add item failed');
                currentOrderVersion = Number(
                    result.orderVersion || currentOrderVersion || 0
                );
                setSaveState('', 'Сохранено');
                return result;
            }

            async function removeDocument(itemId, documentId = '') {
                setSaveState('saving', 'Сохраняем...');

                const editSessionId = await requireEditingSession(
                    'удаление файла'
                );

                if (typeof fetchCurrentUserIfPossible === 'function') {
                    await fetchCurrentUserIfPossible();
                }

                if (!canCurrentUserDeleteFiles()) {
                    showFileDeleteForbiddenAlert();
                    setSaveState('', 'Сохранено');
                    throw new Error('У вас недостаточно прав на удаление файлов');
                }

                const actor = getFileDeleteActor();

                const response = await fetch(appUrl('api/checklist/remove-document'), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        dialogId,
                        checklistKey: currentChecklistKey,
                        itemId,
                        documentId,
                        actingUserId: actor.id,
                        actingUserName: actor.name,
                        sessionId: editSessionId,
                        requireEditSession: true
                    })
                });

                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'remove document failed');
                setSaveState('', 'Сохранено');
                return result;
            }

            function captureUploadContext(
                itemId,
                file,
                checklistKey = currentChecklistKey,
                editSessionId = ''
            ) {
                const targetKey = String(
                    checklistKey
                    || currentChecklistKey
                    || 'id'
                ).trim() || 'id';

                const targetSnapshot = targetKey === currentChecklistKey
                    ? buildChecklistSnapshot()
                    : deepClone(checklistCache[targetKey] || {});

                const targetItems = Array.isArray(targetSnapshot.items)
                    ? targetSnapshot.items
                    : [];

                const targetItem = targetItems.find(item => (
                    String(item && item.id || '')
                    === String(itemId || '')
                ));

                const uploadId = (
                    'front_'
                    + Date.now()
                    + '_'
                    + Math.random().toString(36).slice(2, 8)
                );

                return Object.freeze({
                    uploadId,
                    dialogId: String(dialogId || ''),
                    checklistKey: targetKey,
                    itemId: String(itemId || ''),
                    itemGroup: String(
                        targetItem && targetItem.group || ''
                    ),
                    itemName: String(
                        targetItem && targetItem.name || ''
                    ),
                    initialStatus: normalizeStatus(
                        targetItem && targetItem.status
                    ),
                    fileName: String(file && file.name || ''),
                    fileSize: Number(file && file.size || 0),
                    fileType: String(file && file.type || ''),
                    source: 'popup',
                    sessionId: String(editSessionId || '').trim()
                });
            }

            function buildUploadRequestError(
                message,
                options = {}
            ) {
                const error = new Error(
                    String(message || 'Ошибка загрузки файла')
                );
                error.retryable = options.retryable === true;
                error.httpStatus = Number(
                    options.httpStatus || 0
                );
                error.editSessionError = (
                    options.editSessionError === true
                );
                return error;
            }

            function invalidateUploadEditSession(
                errorText = ''
            ) {
                const editSessionApi = (
                    window.ChecklistPopupEditSession
                );

                if (
                    editSessionApi
                    && typeof editSessionApi.invalidate
                        === 'function'
                ) {
                    editSessionApi.invalidate(errorText);
                }
            }

            function uploadResponseIsRetryable(
                status,
                result = {}
            ) {
                const normalizedStatus = Number(status || 0);

                if (
                    result
                    && result.editSessionError === true
                    && [404, 409].includes(normalizedStatus)
                ) {
                    return true;
                }

                return [
                    0,
                    408,
                    425,
                    429,
                    500,
                    502,
                    503,
                    504
                ].includes(normalizedStatus);
            }

            async function executeUploadRequest(
                context,
                file,
                progressControl
            ) {
                const editSessionId = String(
                    context && context.sessionId || ''
                ).trim();

                if (!editSessionId) {
                    throw buildUploadRequestError(
                        'Загрузка ожидает восстановления '
                        + 'сессии редактирования',
                        {
                            retryable: true,
                            editSessionError: true
                        }
                    );
                }

                const fileName = String(context.fileName || '');
                const fileSize = Number(context.fileSize || 0);
                const fileType = String(context.fileType || '');

                debugLog('upload_frontend_started', {
                    uploadId: context.uploadId,
                    dialogId: context.dialogId,
                    checklistKey: context.checklistKey,
                    itemId: context.itemId,
                    itemGroup: context.itemGroup,
                    itemName: context.itemName,
                    fileName,
                    fileSize,
                    fileType
                });

                updateUploadProgress(
                    fileName,
                    0,
                    'Ожидает передачи в приложение...'
                );

                if (
                    typeof fetchCurrentUserIfPossible
                    === 'function'
                ) {
                    await fetchCurrentUserIfPossible();
                }

                const actor = getFileDeleteActor();
                const formData = new FormData();

                formData.append(
                    'dialogId',
                    context.dialogId
                );
                formData.append(
                    'itemId',
                    context.itemId
                );
                formData.append(
                    'file',
                    file
                );
                formData.append(
                    'checklistKey',
                    context.checklistKey
                );
                formData.append(
                    'itemGroup',
                    context.itemGroup
                );
                formData.append(
                    'actingUserId',
                    actor.id
                );
                formData.append(
                    'actingUserName',
                    actor.name
                );
                formData.append(
                    'sessionId',
                    editSessionId
                );
                formData.append(
                    'requireEditSession',
                    '1'
                );

                return await new Promise(function (resolve, reject) {
                    const xhr = new XMLHttpRequest();

                    xhr.open(
                        'POST',
                        appUrl('api/checklist/upload-document'),
                        true
                    );
                    xhr.timeout = 10 * 60 * 1000;

                    xhr.upload.onprogress = function (event) {
                        if (!event.lengthComputable) {
                            progressControl.updateProgress(1);
                            updateUploadProgress(
                                fileName,
                                15,
                                'Передаём файл в приложение...'
                            );
                            return;
                        }

                        const rawPercent = Math.max(
                            0,
                            Math.min(
                                100,
                                Math.round(
                                    (event.loaded / event.total) * 100
                                )
                            )
                        );

                        progressControl.updateProgress(rawPercent);

                        const displayPercent = Math.max(
                            1,
                            Math.min(
                                70,
                                Math.round(rawPercent * 0.70)
                            )
                        );

                        updateUploadProgress(
                            fileName,
                            displayPercent,
                            (
                                'Передаём файл в приложение... '
                                + rawPercent
                                + '%'
                            )
                        );
                    };

                    xhr.onload = function () {
                        const responseText = String(
                            xhr.responseText || ''
                        );

                        debugLog(
                            'upload_frontend_response_received',
                            {
                                uploadId: context.uploadId,
                                dialogId: context.dialogId,
                                checklistKey: context.checklistKey,
                                itemId: context.itemId,
                                itemGroup: context.itemGroup,
                                fileName,
                                fileSize,
                                fileType,
                                status: xhr.status,
                                ok: (
                                    xhr.status >= 200
                                    && xhr.status < 300
                                ),
                                responseTextStart: (
                                    responseText.slice(0, 1600)
                                )
                            }
                        );

                        let result = {};

                        try {
                            result = JSON.parse(
                                responseText || '{}'
                            );
                        } catch (parseError) {
                            const retryable = (
                                uploadResponseIsRetryable(
                                    xhr.status,
                                    {}
                                )
                            );
                            const errorText = (
                                xhr.status === 413
                                    ? (
                                        'Файл слишком большой '
                                        + 'для сервера'
                                    )
                                    : (
                                        retryable
                                            ? (
                                                'Сервер временно '
                                                + 'недоступен. '
                                                + 'Загрузка будет повторена.'
                                            )
                                            : (
                                                'Некорректный ответ '
                                                + 'сервера при загрузке файла'
                                            )
                                    )
                            );

                            debugLog(
                                'upload_frontend_json_parse_failed',
                                {
                                    uploadId: context.uploadId,
                                    dialogId: context.dialogId,
                                    checklistKey: (
                                        context.checklistKey
                                    ),
                                    itemId: context.itemId,
                                    itemGroup: context.itemGroup,
                                    fileName,
                                    fileSize,
                                    status: xhr.status,
                                    retryable,
                                    responseTextStart: (
                                        responseText.slice(0, 1600)
                                    ),
                                    error: String(
                                        parseError
                                        && parseError.message
                                        || parseError
                                    )
                                }
                            );

                            if (retryable) {
                                updateUploadProgress(
                                    fileName,
                                    0,
                                    errorText
                                );
                            } else {
                                failUploadProgress(
                                    fileName,
                                    errorText
                                );
                            }

                            reject(
                                buildUploadRequestError(
                                    errorText,
                                    {
                                        retryable,
                                        httpStatus: xhr.status
                                    }
                                )
                            );
                            return;
                        }

                        if (
                            xhr.status < 200
                            || xhr.status >= 300
                            || !result.ok
                        ) {
                            const retryable = (
                                uploadResponseIsRetryable(
                                    xhr.status,
                                    result
                                )
                            );
                            const errorText = String(
                                result.error
                                || result.details
                                || 'Ошибка загрузки файла'
                            );

                            if (
                                retryable
                                && result.editSessionError === true
                            ) {
                                invalidateUploadEditSession(
                                    errorText
                                );
                            }

                            debugLog(
                                'upload_frontend_failed_response',
                                {
                                    uploadId: context.uploadId,
                                    dialogId: context.dialogId,
                                    checklistKey: (
                                        context.checklistKey
                                    ),
                                    itemId: context.itemId,
                                    itemGroup: context.itemGroup,
                                    fileName,
                                    fileSize,
                                    status: xhr.status,
                                    retryable,
                                    result
                                }
                            );

                            if (retryable) {
                                updateUploadProgress(
                                    fileName,
                                    0,
                                    (
                                        'Связь с приложением прервана. '
                                        + 'Загрузка будет повторена.'
                                    )
                                );
                            } else {
                                failUploadProgress(
                                    fileName,
                                    errorText
                                );
                            }

                            reject(
                                buildUploadRequestError(
                                    errorText,
                                    {
                                        retryable,
                                        httpStatus: xhr.status,
                                        editSessionError: (
                                            result.editSessionError
                                            === true
                                        )
                                    }
                                )
                            );
                            return;
                        }

                        debugLog(
                            'upload_frontend_completed',
                            {
                                uploadId: context.uploadId,
                                dialogId: context.dialogId,
                                checklistKey: context.checklistKey,
                                itemId: context.itemId,
                                itemGroup: context.itemGroup,
                                fileName,
                                fileSize,
                                uploadJobId: (
                                    result.uploadJobId || ''
                                ),
                                yandexMirrorQueued: (
                                    !!result.yandexMirrorQueued
                                )
                            }
                        );

                        completeUploadProgress(
                            fileName,
                            result.yandexMirrorQueued
                                ? (
                                    'Файл сохранён. '
                                    + 'Синхронизация с '
                                    + 'Яндекс.Диском идёт в фоне.'
                                )
                                : 'Файл сохранён'
                        );

                        resolve(result);
                    };

                    xhr.onerror = function () {
                        debugLog('upload_frontend_xhr_error', {
                            uploadId: context.uploadId,
                            dialogId: context.dialogId,
                            checklistKey: context.checklistKey,
                            itemId: context.itemId,
                            itemGroup: context.itemGroup,
                            fileName,
                            fileSize,
                            fileType,
                            status: xhr.status || '',
                            retryable: true,
                            responseTextStart: String(
                                xhr.responseText || ''
                            ).slice(0, 1600)
                        });

                        updateUploadProgress(
                            fileName,
                            0,
                            (
                                'Ошибка сети. '
                                + 'Загрузка будет повторена.'
                            )
                        );
                        reject(
                            buildUploadRequestError(
                                'Ошибка сети при загрузке файла',
                                {
                                    retryable: true,
                                    httpStatus: xhr.status || 0
                                }
                            )
                        );
                    };

                    xhr.onabort = function () {
                        debugLog('upload_frontend_xhr_abort', {
                            uploadId: context.uploadId,
                            dialogId: context.dialogId,
                            checklistKey: context.checklistKey,
                            itemId: context.itemId,
                            itemGroup: context.itemGroup,
                            fileName,
                            fileSize,
                            fileType
                        });

                        failUploadProgress(
                            fileName,
                            'Загрузка отменена'
                        );
                        reject(new Error('upload aborted'));
                    };

                    xhr.ontimeout = function () {
                        debugLog('upload_frontend_xhr_timeout', {
                            uploadId: context.uploadId,
                            dialogId: context.dialogId,
                            checklistKey: context.checklistKey,
                            itemId: context.itemId,
                            itemGroup: context.itemGroup,
                            fileName,
                            fileSize,
                            fileType,
                            retryable: true
                        });

                        updateUploadProgress(
                            fileName,
                            0,
                            (
                                'Истекло время ожидания. '
                                + 'Загрузка будет повторена.'
                            )
                        );
                        reject(
                            buildUploadRequestError(
                                'Истекло время загрузки',
                                {
                                    retryable: true,
                                    httpStatus: 408
                                }
                            )
                        );
                    };

                    xhr.send(formData);
                });
            }

            async function uploadDocument(
                itemId,
                file,
                contextOverride = null
            ) {
                const capturedContext = contextOverride
                    || captureUploadContext(
                        itemId,
                        file,
                        currentChecklistKey
                    );

                const capturedFile = file;

                return popupUploadManager.enqueue(
                    capturedContext,
                    async function (
                        immutableContext,
                        progressControl
                    ) {
                        const currentSessionId = (
                            typeof requireEditingSession
                            === 'function'
                                ? await requireEditingSession(
                                    'загрузка файлов'
                                )
                                : String(
                                    immutableContext.sessionId || ''
                                ).trim()
                        );
                        const requestContext = Object.freeze({
                            ...immutableContext,
                            sessionId: currentSessionId
                        });

                        return executeUploadRequest(
                            requestContext,
                            capturedFile,
                            progressControl
                        );
                    }
                );
            }

            async function loadChecklistSnapshotForUpload(
                uploadContext
            ) {
                const response = await fetch(
                    appUrl('api/checklist')
                    + '?dialogId='
                    + encodeURIComponent(
                        uploadContext.dialogId
                    )
                    + '&checklistKey='
                    + encodeURIComponent(
                        uploadContext.checklistKey
                    )
                );

                const result = await response.json();

                if (!response.ok) {
                    throw new Error(
                        result.error
                        || 'reload checklist after upload failed'
                    );
                }

                return result;
            }

            function recordCompletedUploadBatch(
                batchContext,
                successfulUploads,
                refreshedChecklist
            ) {
                const targetKey = String(
                    batchContext.checklistKey || 'id'
                ).trim() || 'id';

                const effectiveRefreshedChecklist = (
                    mergeUploadResponseStatusSnapshot(
                        refreshedChecklist,
                        batchContext.itemId,
                        successfulUploads
                    )
                );

                const targetItems = Array.isArray(
                    effectiveRefreshedChecklist
                    && effectiveRefreshedChecklist.items
                )
                    ? effectiveRefreshedChecklist.items
                    : [];

                const refreshedItem = targetItems.find(item => (
                    String(item && item.id || '')
                    === String(batchContext.itemId || '')
                ));

                const existingState = targetKey === currentChecklistKey
                    ? {
                        changes: deepClone(sessionChanges),
                        dirty: !!sessionDirty
                    }
                    : deepClone(
                        getChecklistState(targetKey)
                    );

                const pendingChangesBeforeUpload = Array.isArray(
                    existingState.changes
                )
                    ? deepClone(existingState.changes)
                    : [];

                const nextChanges = deepClone(
                    pendingChangesBeforeUpload
                );

                successfulUploads.forEach(entry => {
                    nextChanges.push({
                        field: 'document',
                        itemId: String(
                            refreshedItem && refreshedItem.id
                            || batchContext.itemId
                        ),
                        itemName: String(
                            refreshedItem && refreshedItem.name
                            || batchContext.itemName
                            || ''
                        ),
                        oldValue: '',
                        newValue: String(
                            entry.context.fileName
                            || 'uploaded'
                        )
                    });
                });

                const finalStatus = normalizeStatus(
                    refreshedItem && refreshedItem.status
                );

                if (
                    finalStatus
                    && finalStatus !== batchContext.initialStatus
                ) {
                    nextChanges.push({
                        field: 'status',
                        itemId: String(
                            refreshedItem && refreshedItem.id
                            || batchContext.itemId
                        ),
                        itemName: String(
                            refreshedItem && refreshedItem.name
                            || batchContext.itemName
                            || ''
                        ),
                        oldValue: String(
                            batchContext.initialStatus || ''
                        ),
                        newValue: finalStatus,
                        source: 'upload-auto',
                        automatic: true
                    });
                }

                const localSnapshot = (
                    targetKey === currentChecklistKey
                        ? buildChecklistSnapshot()
                        : deepClone(
                            checklistCache[targetKey]
                            || effectiveRefreshedChecklist
                        )
                );

                const mergedChecklist = (
                    mergeDocumentRefreshSnapshot(
                        localSnapshot,
                        effectiveRefreshedChecklist,
                        batchContext.itemId,
                        pendingChangesBeforeUpload
                    )
                );

                checklistCache[targetKey] = deepClone(
                    mergedChecklist
                );

                checklistSessionState[targetKey] = {
                    changes: nextChanges,
                    dirty: true
                };

                if (targetKey === currentChecklistKey) {
                    applyChecklistData(
                        deepClone(mergedChecklist)
                    );
                    renderAll();
                }

                debugLog(
                    'upload_frontend_batch_applied',
                    {
                        dialogId: batchContext.dialogId,
                        checklistKey: targetKey,
                        itemId: batchContext.itemId,
                        filesCount: successfulUploads.length,
                        currentChecklistKey,
                        rendered: (
                            targetKey === currentChecklistKey
                        ),
                        refreshedStatus: finalStatus,
                        mergedStatus: normalizeStatus(
                            (
                                Array.isArray(mergedChecklist.items)
                                    ? mergedChecklist.items
                                    : []
                            ).find(item => (
                                String(item && item.id || '')
                                === String(batchContext.itemId || '')
                            ))?.status
                        )
                    }
                );
            }

            function getItemsByGroup(groupId) {
                const targetGroupId = Number(groupId);
                const notRequiredGroupId = typeof getCurrentNotRequiredGroupId === 'function'
                    ? Number(getCurrentNotRequiredGroupId() || 0)
                    : 0;

                return items
                    .filter(item => {
                        const itemGroupId = Number(item && item.group || 0);
                        const itemIsNotRequired = normalizeStatus(item && item.status) === 'Не требуется';

                        if (notRequiredGroupId && targetGroupId === notRequiredGroupId) {
                            return itemGroupId === targetGroupId || itemIsNotRequired;
                        }

                        if (notRequiredGroupId && itemIsNotRequired) {
                            return false;
                        }

                        return itemGroupId === targetGroupId;
                    })
                    .sort((a, b) => Number(a.order || 0) - Number(b.order || 0));
            }
            function hasItemsInGroup(groupId) {
                return getItemsByGroup(groupId).length > 0;
            }
            function getProjectChecklistMetaForKey(key) {
                const targetKey = String(key || currentChecklistKey || '').trim() || 'id';

                const savedMeta = (Array.isArray(projectChecklists) ? projectChecklists : []).find(item =>
                    String(item && item.key || '').trim() === targetKey
                ) || {};

                const configMeta = checklistLayoutMetaByKey[targetKey] || {};

                return {
                    key: targetKey,
                    title: String(configMeta.title || savedMeta.title || 'Чек-лист'),
                    notRequiredGroupId: Number(configMeta.notRequiredGroupId || savedMeta.notRequiredGroupId || 0),
                    defaultGroupId: Number(configMeta.defaultGroupId || savedMeta.defaultGroupId || 0),
                    allowCustomItemGroupIds: Array.isArray(configMeta.allowCustomItemGroupIds)
                        ? configMeta.allowCustomItemGroupIds
                        : Array.isArray(savedMeta.allowCustomItemGroupIds)
                            ? savedMeta.allowCustomItemGroupIds
                            : [],
                    stageYandexFolderAlias: String(configMeta.stageYandexFolderAlias || savedMeta.stageYandexFolderAlias || ''),
                    layoutMode: String(configMeta.layoutMode || savedMeta.layoutMode || 'generic'),
                    bimGroupId: Number(configMeta.bimGroupId || savedMeta.bimGroupId || 0),
                    bimPlacement: String(configMeta.bimPlacement || savedMeta.bimPlacement || '')
                };
            }

            function getCurrentChecklistLayoutMeta() {
                return getProjectChecklistMetaForKey(currentChecklistKey);
            }

            function getCurrentStageFolderInfo() {
                const key = String(currentChecklistKey || '').trim() || 'id';
                return stageYandexFoldersByKey[key] || {};
            }

            function getCurrentStageYandexAvailability() {
                const folderInfo = getCurrentStageFolderInfo();
                return {
                    disabled: !!(folderInfo && folderInfo.yandexDisabled),
                    reason: String(folderInfo && folderInfo.reason || '').trim(),
                    path: String(folderInfo && folderInfo.path || '').trim(),
                    url: String(folderInfo && folderInfo.url || '').trim()
                };
            }

            function getItemYandexButtonTitle(
                status,
                stageUnavailable,
                action = ''
            ) {
                if (stageUnavailable) {
                    return 'Открыть папку пункта на Яндекс.Диске';
                }
                const normalizedAction = String(action || '').trim();
                if (status === 'error') {
                    return normalizedAction === 'move_item_folder'
                        ? 'Повторить перемещение папки на Яндекс.Диске'
                        : normalizedAction === 'rename_item_folder'
                            ? 'Повторить переименование папки на Яндекс.Диске'
                            : 'Повторить создание папки на Яндекс.Диске';
                }
                if (status === 'queued') {
                    return normalizedAction === 'move_item_folder'
                        ? 'Папка Яндекс.Диска ожидает перемещения'
                        : normalizedAction === 'rename_item_folder'
                            ? 'Папка Яндекс.Диска ожидает переименования'
                            : 'Папка Яндекс.Диска ожидает создания';
                }
                if (status === 'running') {
                    return normalizedAction === 'move_item_folder'
                        ? 'Папка Яндекс.Диска перемещается…'
                        : normalizedAction === 'rename_item_folder'
                            ? 'Папка Яндекс.Диска переименовывается…'
                            : 'Папка Яндекс.Диска создаётся…';
                }
                if (status === 'disabled') {
                    return 'Открыть папку пункта на Яндекс.Диске';
                }
                return 'Открыть папку пункта на Яндекс.Диске';
            }

            function updateCurrentStageYandexItemButtons() {
                const availability = getCurrentStageYandexAvailability();

                document.querySelectorAll(
                    '[data-role="view-yandex-folder"]'
                ).forEach(button => {
                    const status = String(
                        button.dataset.yandexStatus || ''
                    ).trim().toLowerCase();
                    const itemBlocked = [
                        'queued',
                        'running',
                        'disabled'
                    ].includes(status);
                    const disabled = availability.disabled || itemBlocked;
                    const title = getItemYandexButtonTitle(
                        status,
                        availability.disabled,
                        button.dataset.yandexAction || ''
                    );

                    button.disabled = disabled;
                    button.setAttribute(
                        'aria-disabled',
                        disabled ? 'true' : 'false'
                    );
                    button.setAttribute('title', title);
                    button.setAttribute('aria-label', title);
                    button.dataset.stageYandexDisabled = (
                        availability.disabled ? '1' : '0'
                    );
                });
            }

            const stageYandexFolderRefreshInProgress = new Set();
            const stageYandexFolderRefreshRequested = new Set();

            async function refreshCurrentStageYandexFolderInfo(options = {}) {
                const requestedKey = String(
                    options && options.checklistKey
                    || currentChecklistKey
                    || ''
                ).trim();
                const key = requestedKey || 'id';
                const force = !!(options && options.force);

                if (stageYandexFolderRefreshInProgress.has(key)) {
                    return null;
                }

                if (
                    !force
                    && stageYandexFolderRefreshRequested.has(key)
                ) {
                    return stageYandexFoldersByKey[key] || null;
                }

                stageYandexFolderRefreshRequested.add(key);
                stageYandexFolderRefreshInProgress.add(key);

                try {
                    const response = await fetch(
                        appUrl('api/checklist/stage-yandex-folder') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(key) +
                        '&prepare=1'
                    );

                    const result = await response.json().catch(() => ({}));

                    if (!response.ok || !result || !result.ok) {
                        const previous = stageYandexFoldersByKey[key] || {};
                        const previousUrl = String(
                            previous.url || ''
                        ).trim();

                        stageYandexFoldersByKey[key] = {
                            alias: String(
                                result && result.alias
                                || previous.alias
                                || ''
                            ),
                            path: String(
                                result && result.path
                                || previous.path
                                || ''
                            ),
                            url: previousUrl,
                            yandexDisabled: true,
                            reason: String(
                                result && result.error
                                || 'Не удалось подключиться к Яндекс.Диску'
                            )
                        };

                        debugLog('stage_yandex_folder_prepare_failed', {
                            checklistKey: key,
                            result
                        });

                        if (key === currentChecklistKey) {
                            renderStageFolderButton();
                            updateCurrentStageYandexItemButtons();
                        }
                        return stageYandexFoldersByKey[key];
                    }

                    stageYandexFoldersByKey[key] = {
                        alias: String(result.alias || ''),
                        path: String(result.path || ''),
                        url: String(result.url || ''),
                        yandexDisabled: !!result.yandexDisabled,
                        reason: String(result.reason || '')
                    };

                    debugLog('stage_yandex_folder_prepared', {
                        checklistKey: key,
                        alias: result.alias || '',
                        pathExists: !!String(result.path || '').trim(),
                        urlExists: !!String(result.url || '').trim(),
                        preparedNow: !!result.preparedNow,
                        yandexDisabled: !!result.yandexDisabled
                    });

                    if (key === currentChecklistKey) {
                        renderStageFolderButton();
                        updateCurrentStageYandexItemButtons();
                    }

                    return stageYandexFoldersByKey[key];

                } catch (e) {
                    const previous = stageYandexFoldersByKey[key] || {};
                    const previousUrl = String(
                        previous.url || ''
                    ).trim();

                    stageYandexFoldersByKey[key] = {
                        alias: String(previous.alias || ''),
                        path: String(previous.path || ''),
                        url: previousUrl,
                        yandexDisabled: true,
                        reason: String(
                            e && e.message
                            || 'Не удалось подключиться к Яндекс.Диску'
                        )
                    };

                    console.log('stage yandex folder refresh error:', e);
                    debugLog('stage_yandex_folder_prepare_exception', {
                        checklistKey: key,
                        message: String(e && e.message || e)
                    });

                    if (key === currentChecklistKey) {
                        renderStageFolderButton();
                        updateCurrentStageYandexItemButtons();
                    }
                    return stageYandexFoldersByKey[key];
                } finally {
                    stageYandexFolderRefreshInProgress.delete(key);
                }
            }

            function renderStageFolderButton() {
                if (!stageFolderBoxEl) {
                    return;
                }

                const key = String(currentChecklistKey || '').trim() || 'id';
                const hasFolderInfo = Object.prototype.hasOwnProperty.call(
                    stageYandexFoldersByKey,
                    key
                );
                const folderInfo = stageYandexFoldersByKey[key] || {};
                const yandexDisabled = !!(
                    folderInfo
                    && folderInfo.yandexDisabled
                );
                const folderPath = String(
                    folderInfo && folderInfo.path || ''
                ).trim();
                const folderUrl = String(
                    folderInfo && folderInfo.url || ''
                ).trim();

                if (
                    !hasFolderInfo
                    && !stageYandexFolderRefreshRequested.has(key)
                    && !stageYandexFolderRefreshInProgress.has(key)
                ) {
                    void refreshCurrentStageYandexFolderInfo({
                        checklistKey: key
                    });
                }

                stageFolderBoxEl.style.display = 'flex';

                const stageFolderEnabled = !!folderUrl && !yandexDisabled;
                const buttonText = 'Открыть Стадию в Яндекс Диске';

                stageFolderBoxEl.innerHTML = `
                    <button
                        class="doc-btn"
                        type="button"
                        data-role="view-stage-folder"
                        data-folder-url="${esc(stageFolderEnabled ? folderUrl : '')}"
                        data-yandex-disabled="${yandexDisabled ? '1' : '0'}"
                        title="Открыть Стадию в Яндекс Диске"
                        aria-disabled="${stageFolderEnabled ? 'false' : 'true'}"
                        ${stageFolderEnabled ? '' : 'disabled'}
                    >
                        ${esc(buttonText)}
                    </button>
                `;

                const btn = stageFolderBoxEl.querySelector(
                    '[data-role="view-stage-folder"]'
                );
                if (!btn) {
                    return;
                }

                updateCurrentStageYandexItemButtons();

                if (!stageFolderEnabled) {
                    btn.disabled = true;
                    btn.style.opacity = '0.65';
                    btn.style.cursor = 'default';
                    return;
                }

                btn.addEventListener('click', function () {
                    const url = String(
                        this.dataset.folderUrl || ''
                    ).trim();
                    if (url) {
                        window.open(url, '_blank', 'noopener');
                    }
                });
            }


            function renderProjectRootFolderButton() {
                if (!projectRootFolderBoxEl) {
                    return;
                }

                const folderUrl = String(projectRootYandexUrl || '').trim();
                const folderPath = String(projectRootYandexPath || '').trim();
                const isReady = !!folderUrl;

                if (!folderPath) {
                    projectRootFolderBoxEl.style.display = 'none';
                    projectRootFolderBoxEl.innerHTML = '';
                    return;
                }

                projectRootFolderBoxEl.style.display = 'flex';

                const buttonText = isReady
                    ? 'Открыть Проект на Яндекс Диске'
                    : 'Готовим структуру Яндекс.Диска...';

                projectRootFolderBoxEl.innerHTML = `
                    <button
                        class="doc-btn"
                        type="button"
                        data-role="view-project-root-folder"
                        data-folder-url="${esc(folderUrl)}"
                        title="${esc(folderPath || 'Корневая папка проекта')}"
                        style="min-width:260px; width:260px; height:32px; white-space:nowrap;"
                        ${isReady ? '' : 'disabled'}
                    >
                        ${esc(buttonText)}
                    </button>
                `;

                const btn = projectRootFolderBoxEl.querySelector('[data-role="view-project-root-folder"]');
                if (!btn) {
                    return;
                }

                if (!isReady) {
                    btn.disabled = true;
                    btn.style.opacity = '0.65';
                    btn.style.cursor = 'default';
                    return;
                }

                btn.addEventListener('click', function () {
                    const url = String(this.dataset.folderUrl || '').trim();
                    if (url) {
                        window.open(url, '_blank', 'noopener');
                    }
                });
            }

            function renderProjectChecklistList() {
                if (!projectChecklistListEl) {
                    return;
                }

                const normalizedList = (Array.isArray(projectChecklists) ? projectChecklists : []).map(item => {
                    const key = String(item && item.key || '').trim();
                    const title = String(item && item.title || '').trim() || getChecklistDefaultTitle(key);

                    return {
                        key,
                        title
                    };
                });

                projectChecklistListEl.innerHTML = normalizedList.map(item => {
                    const active = item.key === currentChecklistKey ? 'side-link active' : 'side-link';
                    return `<button type="button" class="${active}" data-checklist-key="${esc(item.key)}">${esc(item.title)}</button>`;
                }).join('');

                projectChecklistListEl.querySelectorAll('[data-checklist-key]').forEach(btn => {
                    btn.addEventListener('click', async function () {
                        const key = this.dataset.checklistKey;
                        await loadChecklistByKey(key);
                    });
                });
            }
            // Stage 6.5.1: buildDocumentCell is provided by popup-document-list.js.
            function pushSessionChange(itemId, itemName, field, oldValue, newValue) {
                if (String(oldValue || '') === String(newValue || '')) {
                    return;
                }

                sessionChanges.push({
                    field,
                    itemId: itemId || '',
                    itemName: itemName || '',
                    oldValue: oldValue || '',
                    newValue: newValue || ''
                });
                sessionDirty = true;
                closeSummarySent = false;
                setSaveState('saving', 'Есть несохраненные изменения');
            }
            function replaceItem(updatedItem) {
                if (!updatedItem) return;

                const normalizedItem = Object.assign({
                    folderKey: '',
                    folderPath: '',
                    folderUrl: '',
                    yandexFolderStatus: '',
                    yandexFolderError: '',
                    yandexFolderPath: '',
                    yandexFolderUrl: '',
                    yandexFolderTargetPath: '',
                    yandexStructureJobId: '',
                    yandexStructureAction: '',
                    yandexStructureUpdatedAt: '',
                    documents: [],
                    documentUrl: '',
                    documentName: ''
                }, updatedItem || {});

                normalizedItem.documents = Array.isArray(normalizedItem.documents) ? normalizedItem.documents : [];

                if (!Object.prototype.hasOwnProperty.call(normalizedItem, 'documentUrl')) {
                    normalizedItem.documentUrl = '';
                }
                if (!Object.prototype.hasOwnProperty.call(normalizedItem, 'documentName')) {
                    normalizedItem.documentName = '';
                }

                const idx = items.findIndex(x => x.id === normalizedItem.id);
                if (idx >= 0) {
                    items[idx] = Object.assign({}, items[idx], normalizedItem, {
                        folderKey: normalizedItem.folderKey || '',
                        folderPath: normalizedItem.folderPath || '',
                        folderUrl: normalizedItem.folderUrl || '',
                        yandexFolderStatus: normalizedItem.yandexFolderStatus || '',
                        yandexFolderError: normalizedItem.yandexFolderError || '',
                        yandexFolderPath: normalizedItem.yandexFolderPath || '',
                        yandexFolderUrl: normalizedItem.yandexFolderUrl || '',
                        yandexFolderTargetPath: normalizedItem.yandexFolderTargetPath || '',
                        yandexStructureJobId: normalizedItem.yandexStructureJobId || '',
                        yandexStructureAction: normalizedItem.yandexStructureAction || '',
                        yandexStructureUpdatedAt: normalizedItem.yandexStructureUpdatedAt || '',
                        documents: normalizedItem.documents || [],
                        documentUrl: normalizedItem.documentUrl || '',
                        documentName: normalizedItem.documentName || ''
                    });
                } else {
                    items.push(normalizedItem);
                }
            }
