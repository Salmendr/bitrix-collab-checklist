import json
import html

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.storage import (
    get_checklist,
    get_project_storage_context,
    get_project_root_yandex_folder_info,
)

from app.checklists.permissions import FILE_DELETE_ALLOWED_USER_IDS

def popup_html(dialogId: str = "", checklistKey: str = "id") -> str:
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    data = get_checklist(dialog_id, checklist_key)
    project_context = get_project_storage_context(dialog_id) or {}

    title_raw = clean_cell_value(data.get("title")) or "Чек-лист"
    title = html.escape(title_raw)

    project_name_raw = clean_cell_value(project_context.get("projectName"))
    collab_title_raw = clean_cell_value(data.get("collabTitle")) or project_name_raw

    collab_title = html.escape(collab_title_raw)
    full_title = f"{title} — {collab_title}" if collab_title_raw else title
    progress_percent = int(data.get("progressPercent", 0) or 0)

    project_root_folder_info = get_project_root_yandex_folder_info(dialog_id)
    project_root_yandex_path_json = json.dumps(
        clean_cell_value(project_root_folder_info.get("path")),
        ensure_ascii=False
    )
    project_root_yandex_url_json = json.dumps(
        clean_cell_value(project_root_folder_info.get("url")),
        ensure_ascii=False
    )

    project_root_yandex_prepared_json = json.dumps(
        bool(project_root_folder_info.get("standardFoldersPrepared")),
        ensure_ascii=False
    )

    items_json = json.dumps(data.get("items", []), ensure_ascii=False)
    groups_json = json.dumps(data.get("groups", []), ensure_ascii=False)
    project_checklists_json = json.dumps(data.get("projectChecklists", []), ensure_ascii=False)
    dialog_id_json = json.dumps(dialog_id, ensure_ascii=False)
    collab_title_json = json.dumps(collab_title_raw, ensure_ascii=False)
    checklist_key_json = json.dumps(checklist_key, ensure_ascii=False)
    checklist_title_json = json.dumps(title_raw, ensure_ascii=False)
    file_delete_allowed_user_ids_json = json.dumps(
        sorted(FILE_DELETE_ALLOWED_USER_IDS),
        ensure_ascii=False
    )
    popup_session_enhancements_js = """
            const clientSessionId = 'popup_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
            let checklistSessionState = {};
            let currentChecklistLock = {
                owned: false,
                lockedByOther: false,
                lockId: '',
                userId: '',
                userName: '',
                checklistKey: ''
            };
            let lockHeartbeatTimer = null;
            let suppressAutoCloseSave = false;
            let activeInlineEditor = { role: '', itemId: '' };

            const headerRightEl = document.querySelector('.header-right');
            if (headerRightEl && !document.getElementById('lockNotice')) {
                const lockNode = document.createElement('div');
                lockNode.id = 'lockNotice';
                lockNode.style.fontSize = '12px';
                lockNode.style.fontWeight = '700';
                lockNode.style.padding = '7px 10px';
                lockNode.style.borderRadius = '999px';
                lockNode.style.background = '#fff4e5';
                lockNode.style.color = '#b26a00';
                lockNode.style.whiteSpace = 'nowrap';
                lockNode.style.display = 'none';
                headerRightEl.insertBefore(lockNode, saveStateEl);
            }
            const lockNoticeEl = document.getElementById('lockNotice');

            const contentEl = document.querySelector('.content');
            if (contentEl && !document.getElementById('footerActions')) {
                const footerNode = document.createElement('div');
                footerNode.id = 'footerActions';
                footerNode.style.position = 'sticky';
                footerNode.style.bottom = '0';
                footerNode.style.zIndex = '30';
                footerNode.style.marginTop = '14px';
                footerNode.style.padding = '10px 12px';
                footerNode.style.display = 'flex';
                footerNode.style.gap = '10px';
                footerNode.style.justifyContent = 'flex-end';
                footerNode.style.alignItems = 'center';
                footerNode.style.border = '1px solid #e5e7eb';
                footerNode.style.borderRadius = '12px';
                footerNode.style.background = '#fafbfc';
                footerNode.style.boxShadow = '0 -4px 18px rgba(15,23,42,.06)';
                footerNode.innerHTML = `
                    <button id="saveCloseBtn" type="button" style="min-width:150px;height:34px;border:none;border-radius:8px;background:#16a34a;color:#fff;font-size:12px;font-weight:700;cursor:pointer;">
                        Сохранить и закрыть
                    </button>
                    <button id="cancelBtn" type="button" style="min-width:100px;height:34px;border:none;border-radius:8px;background:#dc2626;color:#fff;font-size:12px;font-weight:700;cursor:pointer;">
                        Отмена
                    </button>
                `;
                contentEl.appendChild(footerNode);
            }
            const saveCloseBtn = document.getElementById('saveCloseBtn');
            const cancelBtn = document.getElementById('cancelBtn');

            function normalizeFrontendChecklistKey(key) {
                return String(key || '').trim() || 'id';
            }

            function getChecklistMeta(key = currentChecklistKey) {
                const targetKey = normalizeFrontendChecklistKey(key);

                const found = (Array.isArray(projectChecklists) ? projectChecklists : []).find(item =>
                    String(item && item.key || '').trim() === targetKey
                );

                return found || {
                    key: targetKey,
                    title: 'Чек-лист',
                    notRequiredGroupId: 0,
                    defaultGroupId: 0,
                    allowCustomItemGroupIds: []
                };
            }

            function getChecklistDefaultTitle(key) {
                const meta = getChecklistMeta(key);
                const title = String(meta && meta.title || '').trim();

                return title || 'Чек-лист';
            }
            function getCurrentNotRequiredGroupId() {
                const meta = getChecklistMeta(currentChecklistKey);
                return Number(meta && meta.notRequiredGroupId || 0);
            }

            function currentAllowsCustomItemsForGroup(groupId) {
                const meta = getChecklistMeta(currentChecklistKey);
                const allowed = Array.isArray(meta && meta.allowCustomItemGroupIds)
                    ? meta.allowCustomItemGroupIds.map(Number)
                    : [];

                return allowed.includes(Number(groupId));
            }

            function isCurrentNotRequiredGroup(groupId) {
                const notRequiredGroupId = getCurrentNotRequiredGroupId();
                return !!notRequiredGroupId && Number(groupId) === notRequiredGroupId;
            }

            function getChecklistState(key) {
                const stateKey = String(key || '').trim() || 'id';
                if (!checklistSessionState[stateKey]) {
                    checklistSessionState[stateKey] = { changes: [], dirty: false };
                }
                return checklistSessionState[stateKey];
            }

            function getCurrentEditorIdentity() {
                return {
                    userId: String(currentEditor.id || clientSessionId),
                    userName: String(currentEditor.name || 'Пользователь')
                };
            }

            async function ensureCurrentEditorReady() {
                await fetchCurrentUserIfPossible();
                return getCurrentEditorIdentity();
            }

            function isChecklistLockedByOther() {
                return !!(currentChecklistLock.lockedByOther && currentChecklistLock.checklistKey === currentChecklistKey);
            }

            function isEditingAllowed() {
                return !isChecklistLockedByOther();
            }

            function disabledAttr() {
                return isEditingAllowed() ? '' : 'disabled';
            }

            function setActionButtonState() {
                if (saveCloseBtn) {
                    saveCloseBtn.disabled = !isEditingAllowed();
                    saveCloseBtn.style.opacity = saveCloseBtn.disabled ? '0.55' : '1';
                    saveCloseBtn.style.cursor = saveCloseBtn.disabled ? 'not-allowed' : 'pointer';
                }
                if (cancelBtn) {
                    cancelBtn.disabled = false;
                }
            }

            function updateSaveStateBySession() {
                if (isChecklistLockedByOther()) {
                    setSaveState('error', 'Только просмотр');
                    return;
                }
                if (sessionDirty) {
                    setSaveState('saving', 'Есть несохраненные изменения');
                    return;
                }
                setSaveState('', 'Сохранено');
            }

            function isDocumentDeletionChange(change) {
                if (!change || String(change.field || '') !== 'document') {
                    return false;
                }

                const newValue = String(change.newValue || '').trim().toLowerCase();
                return newValue === 'удален' || newValue === 'удалён' || newValue === 'removed';
            }

            function sessionStateHasDocumentDeletion(state) {
                const changes = Array.isArray(state && state.changes) ? state.changes : [];
                return changes.some(isDocumentDeletionChange);
            }

            function hasDocumentDeletionInAnyDirtyChecklist() {
                syncChecklistCache();
                const dirtyKeys = getDirtyChecklistKeys();
                return dirtyKeys.some(key => sessionStateHasDocumentDeletion(checklistSessionState[key]));
            }

            function updateLockNotice() {
                if (!lockNoticeEl) return;
                if (isChecklistLockedByOther()) {
                    const ownerName = currentChecklistLock.userName || 'другой сотрудник';
                    lockNoticeEl.textContent = 'Сейчас с этим чек-листом работает ' + ownerName + ', дождитесь завершения сессии';
                    lockNoticeEl.style.display = '';
                } else {
                    lockNoticeEl.textContent = '';
                    lockNoticeEl.style.display = 'none';
                }
                setActionButtonState();
                updateSaveStateBySession();
            }

            const previousSyncChecklistCache = syncChecklistCache;
            syncChecklistCache = function () {
                checklistCache[currentChecklistKey] = buildChecklistSnapshot();
                checklistSessionState[currentChecklistKey] = {
                    changes: deepClone(sessionChanges),
                    dirty: !!sessionDirty
                };
                if (typeof previousSyncChecklistCache === 'function') {
                    previousSyncChecklistCache();
                }
            };

            const previousApplyChecklistData = applyChecklistData;
            applyChecklistData = function (data) {
                previousApplyChecklistData(data);
                checklistTitle = String((data && data.title) || checklistTitle || getChecklistDefaultTitle(currentChecklistKey));
                const cachedState = getChecklistState(currentChecklistKey);
                sessionChanges = deepClone(cachedState.changes || []);
                sessionDirty = !!cachedState.dirty;
                closeSummarySent = false;
                activeInlineEditor = { role: '', itemId: '' };
                currentChecklistLock.checklistKey = currentChecklistKey;
                updateLockNotice();
            };

            function clearChecklistSessionState(checklistKey) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                checklistSessionState[targetKey] = { changes: [], dirty: false };
                if (targetKey === currentChecklistKey) {
                    sessionChanges = [];
                    sessionDirty = false;
                    closeSummarySent = false;
                    updateLockNotice();
                }
            }

            function getDirtyChecklistKeys() {
                syncChecklistCache();
                return Object.keys(checklistSessionState).filter(key => {
                    const state = checklistSessionState[key];
                    return !!(state && state.dirty && Array.isArray(state.changes) && state.changes.length && checklistCache[key]);
                });
            }

            function buildClosePayloadForKey(checklistKey, closeEvent) {
                const key = String(checklistKey || '').trim() || 'id';
                const snapshot = key === currentChecklistKey
                    ? buildChecklistSnapshot()
                    : deepClone(checklistCache[key] || {});
                const state = key === currentChecklistKey
                    ? { changes: deepClone(sessionChanges), dirty: !!sessionDirty }
                    : deepClone(checklistSessionState[key] || { changes: [], dirty: false });

                return {
                    dialogId,
                    checklistKey: key,
                    editor: currentEditor,
                    data: snapshot,
                    changes: state.changes || [],
                    closeEvent,
                    ts: new Date().toISOString()
                };
            }

            function buildCloseBatchPayload(closeEvent) {
                const dirtyKeys = getDirtyChecklistKeys();
                return {
                    dialogId,
                    editor: currentEditor,
                    closeEvent,
                    ts: new Date().toISOString(),
                    sessions: dirtyKeys.map(key => buildClosePayloadForKey(key, closeEvent))
                };
            }

            async function persistDirtyChecklists(closeEvent, useBeacon = false) {
                const dirtyKeys = getDirtyChecklistKeys();
                if (!dirtyKeys.length) {
                    return {
                        savedCount: 0,
                        messageOk: true,
                        messageSkipped: true,
                        result: {}
                    };
                }

                const payload = buildCloseBatchPayload(closeEvent);

                if (useBeacon && navigator.sendBeacon) {
                    const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
                    navigator.sendBeacon(APP_BASE_URL + '/api/checklist/close-session', blob);
                    dirtyKeys.forEach(clearChecklistSessionState);
                    return {
                        savedCount: dirtyKeys.length,
                        messageOk: true,
                        messageSkipped: false,
                        result: {}
                    };
                }

                const response = await fetch(appUrl('api/checklist/close-session'), {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(payload),
                    keepalive: true
                });
                const result = await response.json().catch(() => ({}));
                if (!response.ok) {
                    throw new Error(result.error || 'close-session failed');
                }

                if (result && result.messageOk === false) {
                    debugLog('close_session_message_warning', {
                        closeEvent,
                        dialogId,
                        checklistKeys: dirtyKeys,
                        result
                    });
                } else {
                    debugLog('close_session_completed', {
                        closeEvent,
                        dialogId,
                        checklistKeys: dirtyKeys,
                        result
                    });
                }

                dirtyKeys.forEach(clearChecklistSessionState);
                return {
                    savedCount: dirtyKeys.length,
                    messageOk: result.messageOk !== false,
                    messageSkipped: !!result.messageSkipped,
                    result
                };
            }

            function stopLockHeartbeat() {
                if (lockHeartbeatTimer) {
                    clearInterval(lockHeartbeatTimer);
                    lockHeartbeatTimer = null;
                }
            }

            async function acquireChecklistLock(checklistKey = currentChecklistKey, silent = false) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                const editorIdentity = getCurrentEditorIdentity();
                const payload = {
                    dialogId,
                    checklistKey: targetKey,
                    userId: editorIdentity.userId,
                    userName: editorIdentity.userName,
                    lockId: (currentChecklistLock.checklistKey === targetKey && currentChecklistLock.owned)
                        ? currentChecklistLock.lockId
                        : ''
                };

                try {
                    const response = await fetch(appUrl('api/checklist/lock/acquire'), {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify(payload),
                        keepalive: true
                    });
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'lock acquire failed');
                    }

                    if (targetKey !== currentChecklistKey) {
                        return result;
                    }

                    const prevOwned = currentChecklistLock.owned;
                    const prevBlocked = currentChecklistLock.lockedByOther;
                    const prevLockId = currentChecklistLock.lockId;
                    const nextOwned = !!result.owned;

                    currentChecklistLock = {
                        owned: nextOwned,
                        lockedByOther: !!result.lockedByOther,
                        lockId: nextOwned ? String(result.lockId || '') : '',
                        userId: String(result.userId || ''),
                        userName: String(result.userName || ''),
                        checklistKey: targetKey
                    };
                    updateLockNotice();

                    if (prevOwned !== currentChecklistLock.owned || prevBlocked !== currentChecklistLock.lockedByOther || prevLockId !== currentChecklistLock.lockId) {
                        renderAll();
                    }
                    return result;
                } catch (e) {
                    if (!silent) {
                        console.log('acquireChecklistLock error:', e);
                    }
                    return null;
                }
            }

            async function heartbeatChecklistLock(silent = true) {
                if (!currentChecklistLock.owned || !currentChecklistLock.lockId || currentChecklistLock.checklistKey !== currentChecklistKey) {
                    return null;
                }

                const editorIdentity = getCurrentEditorIdentity();
                try {
                    const response = await fetch(appUrl('api/checklist/lock/heartbeat'), {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            dialogId,
                            checklistKey: currentChecklistKey,
                            userId: editorIdentity.userId,
                            userName: editorIdentity.userName,
                            lockId: currentChecklistLock.lockId
                        }),
                        keepalive: true
                    });
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'lock heartbeat failed');
                    }

                    const nextOwned = !!result.owned;
                    currentChecklistLock = {
                        owned: nextOwned,
                        lockedByOther: !!result.lockedByOther,
                        lockId: nextOwned ? String(result.lockId || '') : '',
                        userId: String(result.userId || ''),
                        userName: String(result.userName || ''),
                        checklistKey: currentChecklistKey
                    };
                    updateLockNotice();
                    return result;
                } catch (e) {
                    if (!silent) {
                        console.log('heartbeatChecklistLock error:', e);
                    }
                    return null;
                }
            }

            function startLockHeartbeat() {
                stopLockHeartbeat();
                lockHeartbeatTimer = setInterval(async function () {
                    if (!currentEditorReady) {
                        return;
                    }

                    if (currentChecklistLock.owned) {
                        await heartbeatChecklistLock(true);
                    } else {
                        await acquireChecklistLock(currentChecklistKey, true);
                    }
                }, 15000);
            }

            async function releaseChecklistLock(checklistKey = currentChecklistKey, useBeacon = false) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                const isOwner = currentChecklistLock.checklistKey === targetKey && currentChecklistLock.owned;
                const lockId = isOwner ? currentChecklistLock.lockId : '';
                const editorIdentity = getCurrentEditorIdentity();
                stopLockHeartbeat();

                if (!isOwner || !lockId) {
                    if (currentChecklistLock.checklistKey === targetKey) {
                        currentChecklistLock = {
                            owned: false,
                            lockedByOther: false,
                            lockId: '',
                            userId: '',
                            userName: '',
                            checklistKey: targetKey
                        };
                        updateLockNotice();
                    }
                    return;
                }

                const payload = {
                    dialogId,
                    checklistKey: targetKey,
                    lockId,
                    userId: editorIdentity.userId
                };

                if (useBeacon && navigator.sendBeacon) {
                    const blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
                    navigator.sendBeacon(APP_BASE_URL + '/api/checklist/lock/release', blob);
                } else {
                    try {
                        await fetch(appUrl('api/checklist/lock/release'), {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify(payload),
                            keepalive: true
                        });
                    } catch (e) {
                        console.log('releaseChecklistLock error:', e);
                    }
                }

                if (currentChecklistLock.checklistKey === targetKey) {
                    currentChecklistLock = {
                        owned: false,
                        lockedByOther: false,
                        lockId: '',
                        userId: '',
                        userName: '',
                        checklistKey: targetKey
                    };
                    updateLockNotice();
                }
            }

            function closePopupWindow() {
                try {
                    if (window.BX24 && typeof window.BX24.closeApplication === 'function') {
                        window.BX24.closeApplication();
                        return;
                    }
                } catch (e) {
                    console.log('BX24.closeApplication error:', e);
                }

                try {
                    window.close();
                } catch (e) {
                    console.log('window.close error:', e);
                }
            }

            async function finalizePopupSession(saveChanges) {
                suppressAutoCloseSave = true;
                closeSummarySent = true;
                syncChecklistCache();

                let persistResult = {
                    savedCount: 0,
                    messageOk: true,
                    messageSkipped: true,
                    result: {}
                };

                try {
                    if (saveChanges) {
                        persistResult = await persistDirtyChecklists('save_and_close', false);
                    } else if (hasDocumentDeletionInAnyDirtyChecklist()) {
                        persistResult = await persistDirtyChecklists('cancel_with_document_deletions', false);
                    }
                } catch (e) {
                    console.log('finalizePopupSession error:', e);
                    setSaveState('error', saveChanges ? 'Ошибка сохранения' : 'Ошибка отмены');
                    return;
                }

                if (saveChanges) {
                    if (persistResult.messageOk === false) {
                        setSaveState('saving', 'Сохранено, сообщение не отправлено');
                    } else if (persistResult.savedCount > 0) {
                        setSaveState('', 'Сохранено');
                    }
                }

                await releaseChecklistLock(currentChecklistKey, false);
                closePopupWindow();
            }

            if (saveCloseBtn) {
                saveCloseBtn.addEventListener('click', async function () {
                    await finalizePopupSession(true);
                });
            }

            if (cancelBtn) {
                cancelBtn.addEventListener('click', async function () {
                    await finalizePopupSession(false);
                });
            }

            sendCloseSummaryOnce = function (eventName) {
                if (eventName === 'popup_hidden' || suppressAutoCloseSave || closeSummarySent) {
                    return;
                }

                closeSummarySent = true;
                persistDirtyChecklists(eventName, true);
                releaseChecklistLock(currentChecklistKey, true);
            };

            loadChecklistByKey = async function (checklistKey) {
                const targetKey = String(checklistKey || '').trim() || 'id';
                if (targetKey === currentChecklistKey) {
                    return;
                }

                await ensureCurrentEditorReady();
                syncChecklistCache();
                await releaseChecklistLock(currentChecklistKey, false);
                setSaveState('saving', 'Загружаем...');

                try {
                    const cachedData = checklistCache[targetKey];
                    if (cachedData) {
                        applyChecklistData(deepClone(cachedData));
                        renderAll();
                        await acquireChecklistLock(targetKey, true);
                        startLockHeartbeat();
                        return;
                    }

                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(targetKey)
                    );
                    const result = await response.json();
                    if (!response.ok) {
                        throw new Error(result.error || 'load checklist failed');
                    }

                    applyChecklistData(result);
                    renderAll();
                    await acquireChecklistLock(targetKey, true);
                    startLockHeartbeat();
                } catch (e) {
                    console.log('loadChecklistByKey enhanced error:', e);
                    setSaveState('error', 'Ошибка загрузки чек-листа');
                }
            };

            buildDocumentCell = function (item) {
                if (normalizeStatus(item && item.status) === 'Не требуется') {
                    return '';
                }
                const documents = getItemDocuments(item);
                const itemId = String(item && item.id || '');
                const folderViewUrl = String(item.folderUrl || '').trim() || (documents.length ? (
                    appUrl('api/checklist/folder') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                    '&itemId=' + encodeURIComponent(itemId)
                ) : '');
                const showViewFolder = documents.length > 0 && !!folderViewUrl;

                const filesHtml = documents.map(doc => {
                    const docId = String(doc.id || '');
                    const docName = String(doc.name || 'Файл');
                    const openUrl = appUrl('api/checklist/file') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                        '&itemId=' + encodeURIComponent(itemId) +
                        '&documentId=' + encodeURIComponent(docId);
                    const sizeText = formatFileSize(doc.size || 0);

                    return `
                        <div class="doc-file-row">
                            <a
                                href="javascript:void(0)"
                                class="doc-file-link"
                                data-role="view-file"
                                data-item-id="${esc(itemId)}"
                                data-document-id="${esc(docId)}"
                                data-open-url="${esc(openUrl)}"
                                title="${esc(docName)}"
                            >
                                ${esc(docName)}
                            </a>
                            ${sizeText ? `<span class="doc-file-meta">${esc(sizeText)}</span>` : ''}
                        </div>
                    `;
                }).join('');

                return `
                    <div class="doc-cell">
                        <div class="doc-actions">
                            <button
                                class="upload-btn"
                                type="button"
                                data-role="upload"
                                data-item-id="${esc(itemId)}"
                                ${typeof disabledAttr === 'function' ? disabledAttr() : ''}
                            >
                                Загрузить
                            </button>

                            ${showViewFolder ? `
                                <button
                                    class="doc-btn"
                                    type="button"
                                    data-role="view-folder"
                                    data-item-id="${esc(itemId)}"
                                    data-folder-url="${esc(folderViewUrl)}"
                                >
                                    Посмотреть
                                </button>
                            ` : ''}
                        </div>

                        ${documents.length ? `
                            <div class="doc-files">
                                ${filesHtml}
                            </div>
                        ` : ''}

                        <input
                            type="file"
                            data-role="file-input"
                            data-item-id="${esc(itemId)}"
                            style="display:none;"
                            multiple
                            ${typeof disabledAttr === 'function' ? disabledAttr() : ''}
                        >
                    </div>
                `;
            };

            renderGroup = function (group) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 4;
                const rows = groupItems.map(item => {
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';
                    return `
                        <div class="${rowClass}" data-item-id="${esc(item.id)}">
                            <div class="td"><div class="cell-name"><div class="${indicatorClass(item.status)}"></div><div class="item-name">${esc(item.name)}</div></div></div>
                            <div class="td">${buildDocumentCell(item)}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                                    <option value="" ${normalizeStatus(item.status) === '' ? 'selected' : ''}></option>
                                    <option value="Есть" ${normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}>Есть</option>
                                    <option value="Нет" ${normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}>Нет</option>
                                    <option value="Не требуется" ${normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                                </select>
                            </div>
                            <div class="td"><input class="date-input" type="date" data-role="plan" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.plan))}" ${disabledAttr()}></div>
                            <div class="td"><input class="date-input" type="date" data-role="fact" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.fact))}" ${disabledAttr()}></div>
                        </div>
                    `;
                }).join('');
                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>` : '';
                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            };

            function getInlineEditorDomId(role, itemId) {
                return 'inline_' + String(role || '').replace(/[^a-z0-9_-]/ig, '_') + '_' + String(itemId || '').replace(/[^a-z0-9_-]/ig, '_');
            }

            function isInlineEditorActive(role, itemId) {
                return activeInlineEditor.role === role && String(activeInlineEditor.itemId || '') === String(itemId || '');
            }

            function startInlineEditor(role, itemId) {
                if (!isEditingAllowed()) {
                    return;
                }
                activeInlineEditor = {
                    role,
                    itemId: String(itemId || '')
                };
                renderAll();
            }

            function stopInlineEditor() {
                activeInlineEditor = { role: '', itemId: '' };
            }

            function focusActiveInlineEditor() {
                if (!activeInlineEditor.role || !activeInlineEditor.itemId) {
                    return;
                }
                const el = document.getElementById(getInlineEditorDomId(activeInlineEditor.role, activeInlineEditor.itemId));
                if (!el) {
                    return;
                }
                if (typeof el.focus === 'function') {
                    el.focus();
                }
                if (typeof el.select === 'function') {
                    el.select();
                }
                if (el.tagName === 'TEXTAREA') {
                    el.dataset.initialValue = String(el.value || '');
                    el.dataset.baseHeight = '32';
                    autoGrowTextarea(el);
                }
            }

            function buildConceptInlineCell(role, item, value, placeholder, extraStyle = '') {
                const itemId = String(item.id || '');
                const isActive = isInlineEditorActive(role, itemId) && isEditingAllowed();
                const displayValue = String(value || '').trim();
                const content = displayValue || String(placeholder || '').trim() || '';
                const displayClass = 'concept-inline-display' + (displayValue ? '' : ' empty') + (isEditingAllowed() ? '' : ' disabled');
                const styleAttr = extraStyle ? ` style="${extraStyle}"` : '';

                if (isActive) {
                    return `
                        <textarea
                            id="${esc(getInlineEditorDomId(role, itemId))}"
                            class="concept-inline-input"
                            data-role="${esc(role + '-edit')}"
                            data-item-id="${esc(itemId)}"
                            data-initial-value="${esc(value || '')}"
                            placeholder="${esc(placeholder || '')}"
                            ${disabledAttr()}
                        >${esc(value || '')}</textarea>
                    `;
                }

                return `
                    <div
                        class="${displayClass}"
                        data-role="${esc(role + '-display')}"
                        data-item-id="${esc(itemId)}"
                        tabindex="${isEditingAllowed() ? '0' : '-1'}"
                        ${styleAttr}
                    >${esc(content) || '&nbsp;'}</div>
                `;
            }

            function buildConceptNameCell(item) {
                const textStyle = item.status === 'Не требуется' ? 'text-decoration:line-through;color:#98a2b3;' : '';
                return buildConceptInlineCell('concept-name', item, item.name || '', 'Название пункта', textStyle);
            }

            function buildConceptSourceCell(item) {
                return buildConceptInlineCell('concept-source', item, item.source || '', 'Нормативы');
            }

            buildConceptStatusCell = function (item) {
                if (item.statusKind === 'bool') {
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                            <option value="" ${item.status === '' ? 'selected' : ''}></option>
                            <option value="Да" ${item.status === 'Да' ? 'selected' : ''}>Да</option>
                            <option value="Нет" ${item.status === 'Нет' ? 'selected' : ''}>Нет</option>
                            <option value="Не требуется" ${item.status === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                        </select>
                    `;
                }
                if (item.statusKind === 'select') {
                    const options = [''].concat(item.statusOptions || [], ['Не требуется']);
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                            ${options.map(option => `<option value="${esc(option)}" ${item.status === option ? 'selected' : ''}>${esc(option)}</option>`).join('')}
                        </select>
                    `;
                }
                return `<input class="status-select" type="text" data-role="concept-status" data-item-id="${esc(item.id)}" placeholder="${esc(item.statusPlaceholder || '')}" value="${esc(item.status || '')}" ${disabledAttr()}>`;
            };

            buildConceptExtraCell = function (item) {
                return `<textarea class="concept-extra-textarea" data-role="concept-extra" data-item-id="${esc(item.id)}" placeholder="${esc(item.extraInfoPlaceholder || '')}" ${disabledAttr()}>${esc(item.extraInfo || '')}</textarea>`;
            };

            renderConceptGroup = function (group) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const rows = groupItems.map(item => `
                    <div class="row" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;" data-item-id="${esc(item.id)}">
                        <div class="td"><div class="cell-name"><div class="${conceptIndicatorClass(item)}"></div>${buildConceptNameCell(item)}</div></div>
                        <div class="td">${buildConceptSourceCell(item)}</div>
                        <div class="td">${buildDocumentCell(item)}</div>
                        <div class="td">${buildConceptStatusCell(item)}</div>
                        <div class="td">${buildConceptExtraCell(item)}</div>
                    </div>
                `).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="conceptAddItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="concept-add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            };

            function buildConceptTableHtmlEnhanced(conceptGroups) {
                return `
                    <div class="thead">
                        <div class="thead-top" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;">
                            <div class="th">Пункт</div>
                            <div class="th">Нормативы</div>
                            <div class="th">Документ</div>
                            <div class="th">Статус</div>
                            <div class="th">Доп информация</div>
                        </div>
                    </div>
                    <div>${conceptGroups.map(renderConceptGroup).join('')}</div>
                `;
            }

            renderConceptTable = function () {
                if (!leftTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('concept table containers not found');
                }

                tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                if (tablePanels[1]) {
                    tablePanels[1].style.display = '';
                }

                const visibleGroups = groups.filter(group => {
                    if (Number(group.id) !== 10) return true;
                    return items.some(x => Number(x.group) === 10);
                });

                const leftGroups = visibleGroups.filter(group => [1, 3, 5, 7, 9].includes(Number(group.id)));
                const rightGroups = visibleGroups.filter(group => [2, 4, 6, 8, 10].includes(Number(group.id)));

                leftTableEl.innerHTML = buildConceptTableHtmlEnhanced(leftGroups);
                rightTableEl.innerHTML = buildConceptTableHtmlEnhanced(rightGroups);
            };

            renderProjectChecklistList = function () {
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
            };

            oprIndicatorClass = function (item) {
                const status = normalizeStatus(item && item.status);
                if (status === 'Есть') return 'status-indicator green';
                if (status === 'Нет' || status === 'Не требуется') return 'status-indicator gray';
                return 'status-indicator';
            };

            function resolveOprGroupIdByItemIdOrName(item) {
                const itemId = String(item && item.id || '');
                if (itemId.startsWith('opr_g')) {
                    const match = itemId.match(/^opr_g(\\d+)_/);
                    if (match) {
                        const groupId = Number(match[1]);
                        if (groupId && groupId !== 2) {
                            return groupId;
                        }
                    }
                }

                const name = String(item && item.name || '').trim();
                const matchedGroup = (Array.isArray(groups) ? groups : []).find(group => {
                    const gid = Number(group && group.id);
                    if (gid === 2) return false;

                    return Array.isArray(items) && items.some(existing =>
                        existing !== item &&
                        Number(existing.group) === gid &&
                        String(existing.name || '').trim() === name
                    );
                });

                return matchedGroup ? Number(matchedGroup.id) : 1;
            }

            function buildOprDatesToggle() {
                const active = !!oprDateVisibility[1];
                return `
                    <button
                        type="button"
                        class="id-dates-toggle"
                        data-role="opr-toggle-dates"
                        title="${active ? 'Скрыть даты' : 'Показать даты'}"
                        aria-label="${active ? 'Скрыть даты' : 'Показать даты'}"
                        ${disabledAttr()}
                    >
                        📅
                    </button>
                `;
            }

            function buildOprStatusCellUi(item) {
                return `
                    <select class="status-select" data-role="opr-status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                        <option value="" ${normalizeStatus(item.status) === '' ? 'selected' : ''}></option>
                        <option value="Есть" ${normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}>Есть</option>
                        <option value="Нет" ${normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}>Нет</option>
                        <option value="Не требуется" ${normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                    </select>
                `;
            }

            function renderOprGroupUi(group) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 2;
                const showDates = !!oprDateVisibility[1];
                const gridClass = showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';

                const rows = groupItems.map(item => {
                    const rowClass = normalizeStatus(item.status) === 'Не требуется'
                        ? `row not-required ${gridClass}`
                        : `row ${gridClass}`;

                    return `
                        <div class="${rowClass}" data-item-id="${esc(item.id)}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${oprIndicatorClass(item)}"></div>
                                    <div class="item-name">${esc(item.name)}</div>
                                </div>
                            </div>
                            <div class="td">${buildDocumentCell(item)}</div>
                            <div class="td">${buildOprStatusCellUi(item)}</div>
                            ${showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="opr-plan" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.plan || ''))}" ${disabledAttr()}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="opr-fact" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.fact || ''))}" ${disabledAttr()}>
                                </div>
                            ` : ''}
                        </div>
                    `;
                }).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="oprAddItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="opr-add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            }

            renderOprTables = function () {
                if (!leftTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('opr table containers not found');
                }

                const showDates = !!oprDateVisibility[1];
                const gridClass = showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';

                tablesGridEl.style.gridTemplateColumns = 'clamp(620px, 37vw, 760px)';
                tablesGridEl.style.justifyContent = 'start';

                if (tablePanels[0]) {
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '0 0 auto';
                    tablePanels[0].style.width = 'clamp(620px, 37vw, 760px)';
                    tablePanels[0].style.maxWidth = 'clamp(620px, 37vw, 760px)';
                }
                if (tablePanels[1]) {
                    tablePanels[1].style.display = 'none';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }
                if (tablePanels[2]) {
                    tablePanels[2].style.display = 'none';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }

                leftTableEl.style.width = '100%';
                leftTableEl.style.maxWidth = '100%';

                const visibleGroups = groups.filter(group => {
                    if (Number(group.id) !== 2) return true;
                    return items.some(x => Number(x.group) === 2);
                });

                leftTableEl.classList.add('id-table');
                leftTableEl.innerHTML = `
                    <div class="thead">
                        <div class="thead-top ${gridClass}">
                            <div class="th">ОПР</div>
                            <div class="th">Документ</div>
                            <div class="th">
                                <div class="th-status-with-toggle">
                                    <span>Статус</span>
                                    ${buildOprDatesToggle()}
                                </div>
                            </div>
                            ${showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Даты</div>` : ''}
                        </div>
                        ${showDates ? `
                            <div class="thead-bottom ${gridClass}">
                                <div class="th"></div>
                                <div class="th"></div>
                                <div class="th"></div>
                                <div class="th">План</div>
                                <div class="th">Факт</div>
                            </div>
                        ` : ''}
                    </div>
                    <div>
                        ${visibleGroups.map(renderOprGroupUi).join('')}
                    </div>
                `;

                if (middleTableEl) middleTableEl.innerHTML = '';
                if (rightTableEl) rightTableEl.innerHTML = '';
            };

            let genericDateVisibility = {};

            function isGenericDatesVisible(groupId) {
                return !!genericDateVisibility[Number(groupId)];
            }

            function getGenericGridClass(showDates) {
                return showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';
            }

            function buildGenericTableHeader(group, showDates) {
                const groupId = Number(group.id);
                const toggleTitle = showDates ? 'Скрыть даты' : 'Показать даты';

                return `
                    <div class="thead-top ${getGenericGridClass(showDates)}">
                        <div class="th">${esc(group.title)}</div>
                        <div class="th">Документ</div>
                        <div class="th th-status-with-toggle">
                            <span>Статус</span>
                            <button
                                type="button"
                                class="id-dates-toggle"
                                data-role="toggle-generic-dates"
                                data-group-id="${esc(groupId)}"
                                title="${esc(toggleTitle)}"
                                aria-label="${esc(toggleTitle)}"
                                ${disabledAttr()}
                            >
                                📅
                            </button>
                        </div>
                        ${showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>` : ''}
                    </div>
                    ${showDates ? `
                        <div class="thead-bottom ${getGenericGridClass(showDates)}">
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th">План</div>
                            <div class="th">Факт</div>
                        </div>
                    ` : ''}
                `;
            }

            function renderGenericGroup(group, showDates) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = currentAllowsCustomItemsForGroup(group.id);
                const gridClass = getGenericGridClass(showDates);

                const rows = groupItems.map(item => {
                    const rowClass = normalizeStatus(item.status) === 'Не требуется'
                        ? 'row not-required'
                        : 'row';

                    return `
                        <div class="${rowClass} ${gridClass}" data-item-id="${esc(item.id)}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${indicatorClass(item.status)}"></div>
                                    <div class="item-name">${esc(item.name)}</div>
                                </div>
                            </div>
                            <div class="td">${buildDocumentCell(item)}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${esc(item.id)}" ${disabledAttr()}>
                                    <option value="" ${normalizeStatus(item.status) === '' ? 'selected' : ''}></option>
                                    <option value="Есть" ${normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}>Есть</option>
                                    <option value="Нет" ${normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}>Нет</option>
                                    <option value="Не требуется" ${normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}>Не требуется</option>
                                </select>
                            </div>
                            ${showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="plan" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.plan))}" ${disabledAttr()}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="fact" data-item-id="${esc(item.id)}" value="${esc(toInputDate(item.fact))}" ${disabledAttr()}>
                                </div>
                            ` : ''}
                        </div>
                    `;
                }).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${esc(group.title)}</div>${rows}${addBlock}</div>`;
            }

            function splitGroupsIntoPanels(sourceGroups, panelCount) {
                const result = Array.from({ length: panelCount }, () => []);
                const safeGroups = Array.isArray(sourceGroups) ? sourceGroups : [];

                if (!safeGroups.length) {
                    return result;
                }

                safeGroups.forEach((group, index) => {
                    const panelIndex = Math.min(
                        panelCount - 1,
                        Math.floor(index * panelCount / safeGroups.length)
                    );

                    result[panelIndex].push(group);
                });

                return result;
            }

            function renderGenericPanel(panelGroups, appendNotRequired = false) {
                const safePanelGroups = Array.isArray(panelGroups) ? panelGroups : [];
                const notRequiredGroupId = getCurrentNotRequiredGroupId();
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === Number(notRequiredGroupId))
                    : null;

                const groupBlocks = safePanelGroups.map(group => {
                    const showDates = isGenericDatesVisible(group.id);

                    return `
                        <div class="thead">
                            ${buildGenericTableHeader(group, showDates)}
                        </div>
                        <div>
                            ${renderGenericGroup(group, showDates)}
                        </div>
                    `;
                }).join('');

                const notRequiredBlock = appendNotRequired && notRequiredGroup && hasItemsInGroup(notRequiredGroupId)
                    ? `
                        <div>
                            ${renderGenericGroup(notRequiredGroup, false)}
                        </div>
                    `
                    : '';

                return groupBlocks + notRequiredBlock;
            }

            function resetTablePanelsForGeneric(panelCount) {
                if (tablesGridEl) {
                    tablesGridEl.classList.toggle('id-three-cols', panelCount >= 3);
                    tablesGridEl.style.gridTemplateColumns = panelCount >= 3
                        ? 'minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)'
                        : panelCount === 2
                            ? 'minmax(0, 1fr) minmax(0, 1fr)'
                            : 'clamp(620px, 37vw, 760px)';
                    tablesGridEl.style.justifyContent = panelCount === 1 ? 'start' : '';
                }

                tablePanels.forEach((panel, index) => {
                    const visible = index < panelCount;
                    panel.style.display = visible ? '' : 'none';
                    panel.style.flex = panelCount === 1 && visible ? '0 0 auto' : '';
                    panel.style.width = panelCount === 1 && visible ? 'clamp(620px, 37vw, 760px)' : '';
                    panel.style.maxWidth = panelCount === 1 && visible ? 'clamp(620px, 37vw, 760px)' : '';
                });

                [leftTableEl, middleTableEl, rightTableEl].forEach((table, index) => {
                    if (!table) return;
                    table.style.width = panelCount === 1 && index === 0 ? '100%' : '';
                    table.style.maxWidth = panelCount === 1 && index === 0 ? '100%' : '';
                    table.innerHTML = '';
                });
            }

            function renderGenericTables() {
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('generic table containers not found');
                }

                const notRequiredGroupId = getCurrentNotRequiredGroupId();

                const activeGroups = (Array.isArray(groups) ? groups : []).filter(group =>
                    Number(group.id) !== Number(notRequiredGroupId)
                );

                const visibleGroups = activeGroups.length ? activeGroups : (Array.isArray(groups) ? groups : []).slice(0, 1);
                const panelCount = Math.min(Math.max(visibleGroups.length, 1), 3);
                const targetTables = [leftTableEl, middleTableEl, rightTableEl];
                const groupedPanels = splitGroupsIntoPanels(visibleGroups, panelCount);

                resetTablePanelsForGeneric(panelCount);

                groupedPanels.forEach((panelGroups, index) => {
                    const appendNotRequired = index === panelCount - 1;
                    targetTables[index].innerHTML = renderGenericPanel(panelGroups, appendNotRequired);
                });
            }

            renderTables = function () {
                renderGenericTables();
            };

            const previousRenderAll = renderAll;
            renderAll = function () {
                previousRenderAll();
                updateLockNotice();
                focusActiveInlineEditor();
            };

            const previousBindEventsEnhanced = bindEvents;
            bindEvents = function () {
                previousBindEventsEnhanced();

                document.querySelectorAll('[data-role="toggle-generic-dates"]').forEach(btn => {
                    btn.addEventListener('click', function () {
                        const groupId = Number(this.dataset.groupId || 0);
                        if (!groupId) return;

                        genericDateVisibility[groupId] = !genericDateVisibility[groupId];
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="concept-extra"], [data-role="concept-name-edit"], [data-role="concept-source-edit"]').forEach(el => {
                    el.dataset.initialValue = String(el.value || '');
                    el.dataset.baseHeight = '32';
                    el.style.height = '32px';
                    el.addEventListener('input', function () {
                        autoGrowTextarea(this);
                    });
                });

                document.querySelectorAll('[data-role="concept-name-display"]').forEach(el => {
                    const openEditor = function () {
                        startInlineEditor('concept-name', this.dataset.itemId);
                    };
                    el.addEventListener('click', openEditor);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            openEditor.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="concept-source-display"]').forEach(el => {
                    const openEditor = function () {
                        startInlineEditor('concept-source', this.dataset.itemId);
                    };
                    el.addEventListener('click', openEditor);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            openEditor.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="concept-name-edit"]').forEach(el => {
                    const commit = function () {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.name || '';
                        const newValue = String(this.value || '').trim();
                        item.name = newValue;
                        pushSessionChange(item.id, newValue || oldValue || item.id, 'name', oldValue, newValue);
                        stopInlineEditor();
                        renderAll();
                    };
                    const cancel = function () {
                        stopInlineEditor();
                        renderAll();
                    };
                    el.addEventListener('blur', commit);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Escape') {
                            e.preventDefault();
                            cancel();
                            return;
                        }
                        if (e.key === 'Enter' && !e.shiftKey) {
                            e.preventDefault();
                            commit.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="concept-source-edit"]').forEach(el => {
                    const commit = function () {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.source || '';
                        const newValue = String(this.value || '').trim();
                        item.source = newValue;
                        pushSessionChange(item.id, item.name || item.id, 'source', oldValue, newValue);
                        stopInlineEditor();
                        renderAll();
                    };
                    const cancel = function () {
                        stopInlineEditor();
                        renderAll();
                    };
                    el.addEventListener('blur', commit);
                    el.addEventListener('keydown', function (e) {
                        if (e.key === 'Escape') {
                            e.preventDefault();
                            cancel();
                            return;
                        }
                        if (e.key === 'Enter' && !e.shiftKey) {
                            e.preventDefault();
                            commit.call(this);
                        }
                    });
                });

                document.querySelectorAll('[data-role="opr-toggle-dates"]').forEach(btn => {
                    btn.addEventListener('click', function () {
                        oprDateVisibility[1] = !oprDateVisibility[1];
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-status"]').forEach(el => {
                    el.addEventListener('change', async function() {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldItem = JSON.parse(JSON.stringify(item));
                        const oldValue = item.status || '';
                        const newValue = this.value;
                        const oldDocuments = getItemDocuments(oldItem);

                        if (newValue === 'Нет' && oldDocuments.length) {
                            if (typeof fetchCurrentUserIfPossible === 'function') {
                                await fetchCurrentUserIfPossible();
                            }

                            if (!confirmStatusNoWithFiles(item.name, oldDocuments)) {
                                this.value = normalizeStatus(oldItem.status);
                                return;
                            }
                        }

                        if (newValue === 'Нет') {
                            try {
                                const result = await updateItem(item.id, 'status', newValue, 'opr');
                                if (!result || !result.item) {
                                    throw new Error('opr status save failed');
                                }

                                replaceItem(result.item);
                                pushSessionChange(item.id, item.name, 'status', oldValue, newValue);

                                if (oldDocuments.length) {
                                    const removedNames = oldDocuments.map(x => x.name || 'uploaded').join(', ');
                                    pushSessionChange(item.id, item.name, 'document', removedNames, 'Удален');
                                }

                                renderAll();
                            } catch (e) {
                                console.log('opr status save error:', e);
                                setSaveState('error', 'Ошибка сохранения статуса');
                                this.value = normalizeStatus(oldItem.status);
                            }
                            return;
                        }

                        item.status = newValue;
                        if (newValue === 'Не требуется') {
                            item.group = 2;
                        } else if (Number(item.group) === 2) {
                            item.group = resolveOprGroupIdByItemIdOrName ? resolveOprGroupIdByItemIdOrName(item) : 1;
                            if (Number(item.group) === 2) {
                                item.group = 1;
                            }
                        }

                        pushSessionChange(item.id, item.name, 'status', oldValue, newValue);
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-plan"]').forEach(el => {
                    el.addEventListener('change', function() {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.plan || '';
                        const newValue = fromInputDate(this.value);
                        item.plan = newValue;
                        pushSessionChange(item.id, item.name, 'plan', oldValue, newValue);
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-fact"]').forEach(el => {
                    el.addEventListener('change', function() {
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldValue = item.fact || '';
                        const newValue = fromInputDate(this.value);
                        item.fact = newValue;
                        pushSessionChange(item.id, item.name, 'fact', oldValue, newValue);
                        renderAll();
                    });
                });

                document.querySelectorAll('[data-role="opr-add-item"]').forEach(btn => {
                    btn.addEventListener('click', async function() {
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('oprAddItemInput_' + groupId);
                        if (!input) return;

                        const name = (input.value || '').trim();
                        if (!name) return;

                        this.disabled = true;

                        try {
                            const result = await addItem(groupId, name, 'opr');
                            if (!result || !result.item) {
                                throw new Error('add opr item failed');
                            }

                            replaceItem(result.item);
                            pushSessionChange(result.item.id, result.item.name, 'add-item', '', result.item.name);

                            debugLog('opr_item_added', {
                                itemId: result.item.id,
                                itemName: result.item.name,
                                groupId: groupId
                            });

                            input.value = '';
                            renderAll();
                        } catch (e) {
                            console.log('opr add item error:', e);
                            setSaveState('error', 'Ошибка добавления пункта');
                        } finally {
                            this.disabled = false;
                        }
                    });
                });
            };

            setTimeout(async function () {
                await ensureCurrentEditorReady();
                await acquireChecklistLock(currentChecklistKey, true);
                startLockHeartbeat();
                updateLockNotice();

                if (String(projectRootYandexPath || '').trim() && (!String(projectRootYandexUrl || '').trim() || !projectRootYandexPrepared)) {
                    projectRootYandexPreparing = true;
                    renderProjectRootFolderButton();

                    try {
                        const response = await fetch(
                            appUrl('api/project-root-folder') +
                            '?dialogId=' + encodeURIComponent(dialogId)
                        );
                        const result = await response.json();

                        if (response.ok && result && result.ok) {
                            projectRootYandexUrl = String(result.url || '').trim();
                            projectRootYandexPrepared = !!result.standardFoldersPrepared;
                            debugLog('project_yandex_structure_prepared', result);
                        } else {
                            debugLog('project_yandex_structure_prepare_failed', result || {});
                        }
                    } catch (e) {
                        console.log('project root folder background load error:', e);
                        debugLog('project_yandex_structure_prepare_exception', {
                            message: String(e && e.message || e)
                        });
                    } finally {
                        projectRootYandexPreparing = false;
                        renderProjectRootFolderButton();
                    }
                }
            }, 0);
    """

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{full_title}</title>
        <script src="https://api.bitrix24.com/api/v1/"></script>
        <style>
            * {{ box-sizing: border-box; }}
            body {{ margin:0; font-family:Arial,sans-serif; background:#f3f6fb; color:#1f2328; }}
            .shell {{ padding:14px; }}
            .modal {{ background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden; box-shadow:0 16px 40px rgba(0,0,0,.12); }}
            .header {{ padding:14px 16px 12px; border-bottom:1px solid #edf0f2; display:flex; justify-content:space-between; align-items:flex-start; gap:14px; }}
            .title {{ font-size:22px; font-weight:700; line-height:1.2; }}
            .title small {{ font-size:20px; font-weight:600; color:#344054; }}
            .header-main {{ display:flex; align-items:flex-start; gap:18px; flex:1 1 auto; min-width:0; }}
            .header-right {{ display:flex; align-items:center; gap:14px; flex:0 0 auto; }}
            .progress-box {{ min-width:150px; }}
            .progress-label {{ font-size:12px; color:#667085; margin-bottom:4px; }}
            .progress-value {{ font-size:21px; font-weight:700; margin-bottom:5px; }}
            .progress-track {{ width:100%; height:8px; background:#edf2f7; border-radius:999px; overflow:hidden; }}
            .progress-bar {{ height:100%; width:0%; background:#22c55e; transition:width .2s ease; }}
            .save-state {{ font-size:12px; font-weight:700; padding:7px 10px; border-radius:999px; background:#eef2ff; color:#3730a3; white-space:nowrap; }}
            .progress-box.id-accent {{ min-width:172px; }}
            .progress-box.id-accent .progress-label {{ font-size:13px; }}
            .progress-box.id-accent .progress-value {{ font-size:24px; }}
            .progress-box.id-accent .progress-track {{ height:9px; }}
            .save-state.saving {{ background:#fff4e5; color:#b26a00; }}
            .save-state.error {{ background:#fdecec; color:#b42318; }}
            .upload-progress-box {{
                width: 310px;
                padding: 9px 10px;
                border: 1px solid #fed7aa;
                border-radius: 12px;
                background: #fff7ed;
                color: #9a3412;
                box-shadow: 0 6px 18px rgba(154,52,18,.10);
            }}

            .upload-progress-title {{
                font-size: 12px;
                font-weight: 800;
                margin-bottom: 3px;
            }}

            .upload-progress-file {{
                font-size: 12px;
                font-weight: 700;
                color: #1f2328;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                margin-bottom: 3px;
            }}

            .upload-progress-warning {{
                font-size: 11px;
                font-weight: 700;
                color: #b45309;
                margin-bottom: 6px;
            }}

            .upload-progress-track {{
                width: 100%;
                height: 8px;
                border-radius: 999px;
                background: #ffedd5;
                overflow: hidden;
            }}

            .upload-progress-bar {{
                height: 100%;
                width: 0%;
                border-radius: 999px;
                background: #f97316;
                transition: width .18s ease;
            }}

            .upload-progress-status {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 8px;
                margin-top: 5px;
                font-size: 11px;
            }}

            .upload-progress-status b {{
                font-size: 12px;
            }}
            .content {{ padding:14px 16px 16px; max-height:82vh; overflow:auto; }}
            .layout {{ display:flex; flex-direction:column; gap:12px; align-items:stretch; }}
            .tables-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; align-items:start; }}
            .table-panel {{ min-width:0; display:flex; }}
            .table-panel .table {{ flex:1 1 auto; }}
            .side-panel {{ order:-1; border:1px solid #e5e7eb; border-radius:12px; background:#fff; overflow:hidden; position:static; }}
            .side-panel-title {{ padding:12px 14px; background:#fafbfc; border-bottom:1px solid #e5e7eb; font-size:13px; font-weight:700; color:#344054; }}
            .side-panel-list {{ padding:10px; display:flex; flex-wrap:wrap; gap:8px; }}
            .side-link {{ display:inline-flex; width:auto; text-align:left; border:1px solid #d0d7de; border-radius:8px; background:#fff; padding:9px 12px; font-size:13px; cursor:pointer; align-items:center; }}
            .side-link.active {{ background:#eef2ff; border-color:#c7d2fe; font-weight:700; }}
            .table {{ width:100%; border:1px solid #e5e7eb; border-radius:12px; overflow:hidden; background:#fff; }}
            .thead {{ position:sticky; top:0; z-index:10; background:#f8fafc; border-bottom:1px solid #e5e7eb; }}
            .thead-top,.thead-bottom {{ min-height:38px; }}
            .thead-top,.thead-bottom,.row {{ display:grid; grid-template-columns:190px 190px 100px 136px 136px; gap:0; align-items:stretch; justify-content:start; }}
            .th,.td {{ padding:8px 9px; border-right:1px solid #edf0f2; }}
            .th:last-child,.td:last-child {{ border-right:none; }}
            .th {{ font-size:12px; font-weight:700; color:#475467; min-height:38px; display:flex; align-items:center; }}
            .thead-top .th,.thead-bottom .th {{ min-height:38px; }}
            .th.center {{ text-align:center; justify-content:center; }}
            .group-block {{ border-top:8px solid #f8fafc; }}
            .group-title {{ padding:9px 12px; min-height:40px; background:#fafbfc; border-top:1px solid #e5e7eb; border-bottom:1px solid #e5e7eb; font-size:13px; font-weight:700; color:#344054; display:flex; align-items:center; }}
            .row {{ border-top:1px solid #edf0f2; background:#fff; }}
            .row.not-required {{ background:#fafafa; }}
            .row.not-required .item-name {{ text-decoration:line-through; color:#98a2b3; }}
            .cell-name {{ display:flex; align-items:center; gap:8px; min-width:0; }}
            .status-indicator {{ width:15px; height:15px; border-radius:999px; border:1px solid #d0d7de; flex:0 0 15px; background:#fff; }}
            .status-indicator.green {{ background:#22c55e; border-color:#22c55e; }}
            .status-indicator.gray {{ background:#9ca3af; border-color:#9ca3af; }}
            .item-name {{ font-size:13px; font-weight:700; color:#1f2328; line-height:1.15; min-width:0; word-break:break-word; max-width:165px; }}
            .status-select,.date-input {{ width:100%; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; font-size:12px; background:#fff; }}
            .concept-extra-textarea {{ width:100%; height:32px; min-height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; font-size:12px; background:#fff; resize:vertical; overflow:hidden; line-height:1.35; }}
            .concept-inline-display {{ min-height:32px; padding:6px 8px; border:1px solid transparent; border-radius:8px; font-size:12px; line-height:1.35; white-space:pre-wrap; word-break:break-word; cursor:text; }}
            .concept-inline-display:hover {{ background:#f8fafc; border-color:#e5e7eb; }}
            .concept-inline-display.empty {{ color:#98a2b3; }}
            .concept-inline-display.disabled {{ cursor:default; background:#f8fafc; border-color:transparent; }}
            .concept-inline-input {{ width:100%; min-height:32px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; font-size:12px; background:#fff; resize:vertical; overflow:hidden; line-height:1.35; }}
            .doc-btn,.upload-btn,.add-item-btn {{ display:inline-block; width:100%; text-align:center; padding:6px 8px; border:1px solid #d0d7de; border-radius:8px; background:#f8fafc; color:#1f2328; font-size:12px; text-decoration:none; cursor:pointer; }}
            .doc-btn:hover,.upload-btn:hover,.side-link:hover,.add-item-btn:hover {{ background:#f1f5f9; }}
            .doc-cell {{
                display: flex;
                flex-direction: column;
                gap: 6px;
            }}

            .doc-actions {{
                display: flex;
                gap: 6px;
            }}

            .doc-actions .upload-btn,
            .doc-actions .doc-btn {{
                flex: 1 1 0;
                width: auto;
            }}

            .doc-files {{
                display: flex;
                flex-direction: column;
                gap: 4px;
            }}

            .doc-file-row {{
                display: flex;
                align-items: center;
                gap: 6px;
                min-width: 0;
            }}

            .doc-file-link {{
                flex: 1 1 auto;
                min-width: 0;
                font-size: 12px;
                color: #175cd3;
                text-decoration: none;
                cursor: pointer;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}

            .doc-file-link:hover {{
                text-decoration: underline;
            }}

            .doc-file-remove {{
                flex: 0 0 auto;
                border: none;
                background: transparent;
                color: #b42318;
                cursor: pointer;
                font-size: 14px;
                line-height: 1;
                padding: 0 2px;
                position: relative;
                z-index: 2;
                pointer-events: auto;
            }}

            .doc-file-meta {{
                flex: 0 0 auto;
                font-size: 11px;
                color: #667085;
                white-space: nowrap;
            }}
            
            .doc-file-remove:hover {{
                opacity: .8;
            }}
            .add-item-row {{ padding:9px 12px 10px; min-height:52px; border-top:1px solid #edf0f2; background:#fcfcfd; display:flex; gap:8px; align-items:center; justify-content:flex-start; }}
            .add-item-row::after {{ content:''; flex:1 1 auto; }}
            .add-item-input {{ flex:0 0 190px; width:190px; min-width:190px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:7px 9px; font-size:12px; }}
            .add-item-btn {{ flex:0 0 100px; width:100px; min-width:100px; height:32px; border:1px solid #d0d7de; border-radius:8px; padding:6px 8px; background:#f8fafc; cursor:pointer; font-size:12px; white-space:nowrap; }}
            @media (max-width:1320px) {{ .layout {{ grid-template-columns:1fr; }} .side-panel {{ position:static; }} }}
            @media (max-width:1120px) {{ .tables-grid {{ grid-template-columns:1fr; }} }}
            .tables-grid.id-three-cols {{
                grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr);
            }}

            .id-table .thead {{
                position: sticky;
                top: 0;
                z-index: 10;
                background: #f8fafc;
                border-bottom: 1px solid #e5e7eb;
            }}

            .id-grid {{
                display: grid;
                gap: 0;
                align-items: stretch;
                justify-content: start;
            }}

            .id-grid.id-grid-compact {{
                grid-template-columns: minmax(0, 1.18fr) minmax(0, 1.02fr) 108px;
            }}

            .id-grid.id-grid-expanded {{
                grid-template-columns: minmax(0, 1.08fr) minmax(0, 0.96fr) 108px 112px 112px;
            }}

            .id-table .thead-top,
            .id-table .thead-bottom,
            .id-table .row {{
                min-height: 38px;
            }}

            .id-table .thead-bottom {{
                border-top: 1px solid #edf0f2;
            }}

            .th-status-with-toggle {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 6px;
            }}

            .id-dates-toggle {{
                width: 22px;
                min-width: 22px;
                height: 22px;
                border: 1px solid #d0d7de;
                background: #fff;
                color: #344054;
                border-radius: 6px;
                padding: 0;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                font-size: 12px;
                line-height: 1;
                cursor: pointer;
            }}

            .id-dates-toggle:hover {{
                background: #f8fafc;
            }}

            .id-table .item-name {{
                max-width: none;
            }}

            @media (max-width:980px) {{
                .thead-top,.thead-bottom,.row {{ grid-template-columns:1fr; }}
                .th,.td {{ border-right:none; border-bottom:1px solid #edf0f2; }}
                .th:last-child,.td:last-child {{ border-bottom:none; }}
            }}
        </style>
    </head>
    <body>
        <div class="shell">
            <div class="modal">
                <div class="header">
                    <div class="header-main">
                        <div class="title" id="popupTitle">{full_title}</div>
                        <div style="display:flex; align-items:flex-end; gap:10px; flex-wrap:wrap;">
                            <div class="progress-box">
                                <div class="progress-label">Прогресс</div>
                                <div class="progress-value" id="progressValue">{progress_percent}%</div>
                                <div class="progress-track"><div class="progress-bar" id="progressBar"></div></div>
                            </div>
                            <div id="projectRootFolderBox" style="display:none; align-self:flex-end; min-width:260px;"></div>
                        </div>
                    </div>
                    <div class="header-right">
                        <div id="uploadProgressBox" class="upload-progress-box" style="display:none;">
                            <div class="upload-progress-title">Загрузка файла</div>
                            <div class="upload-progress-file" id="uploadProgressFile">Файл</div>
                            <div class="upload-progress-warning">Не закрывайте приложение до завершения загрузки</div>
                            <div class="upload-progress-track">
                                <div class="upload-progress-bar" id="uploadProgressBar"></div>
                            </div>
                            <div class="upload-progress-status">
                                <span id="uploadProgressStatus">Подготовка...</span>
                                <b id="uploadProgressPercent">0%</b>
                            </div>
                        </div>
                        <div id="saveState" class="save-state">Сохранено</div>
                    </div>
                </div>
                <div class="content">
                    <div id="debugPanel" style="
                        display:none;
                        margin-bottom:12px;
                        padding:10px 12px;
                        border:1px solid #e5e7eb;
                        border-radius:10px;
                        background:#fafbfc;
                        font-size:12px;
                        color:#344054;
                    ">
                        <div><b>Debug:</b> <span id="debugLastEvent">popup init</span></div>
                        <div style="margin-top:4px;">
                            <a id="debugLogsLink" href="debug/logs" target="_blank">Открыть /debug/logs</a>
                        </div>
                        <div style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                        <button
                            id="debugStopYandexWarmupBtn"
                            type="button"
                            style="height:28px;border:1px solid #fca5a5;border-radius:8px;background:#fef2f2;color:#b42318;font-size:12px;font-weight:700;cursor:pointer;padding:0 10px;"
                        >
                            Остановить создание папок Яндекс.Диска
                        </button>
                        <span id="debugYandexWarmupStopState" style="font-size:12px;color:#667085;"></span>
                        <a
                            id="adminPanelLink"
                            href="admin"
                            target="_blank"
                            style="margin-left:auto;height:28px;display:inline-flex;align-items:center;border:1px solid #bfdbfe;border-radius:8px;background:#eff6ff;color:#175cd3;font-size:12px;font-weight:700;text-decoration:none;padding:0 10px;"
                        >
                            Админ-панель
                        </a>
                    </div>
                    </div>
                    <div class="layout">
                        <div class="tables-grid">
                            <div class="table-panel">
                                <div class="table">
                                    <div class="thead">
                                        <div class="thead-top">
                                            <div class="th">ИД</div>
                                            <div class="th">Документ</div>
                                            <div class="th">Статус</div>
                                            <div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>
                                        </div>
                                        <div class="thead-bottom">
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th">План</div>
                                            <div class="th">Факт</div>
                                        </div>
                                    </div>
                                    <div id="leftTableBody"></div>
                                </div>
                            </div>

                            <div class="table-panel">
                                <div class="table">
                                    <div class="thead">
                                        <div class="thead-top">
                                            <div class="th">ТУ</div>
                                            <div class="th">Документ</div>
                                            <div class="th">Статус</div>
                                            <div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>
                                        </div>
                                        <div class="thead-bottom">
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th">План</div>
                                            <div class="th">Факт</div>
                                        </div>
                                    </div>
                                    <div id="middleTableBody"></div>
                                </div>
                            </div>

                            <div class="table-panel">
                                <div class="table">
                                    <div class="thead">
                                        <div class="thead-top">
                                            <div class="th">Прочее</div>
                                            <div class="th">Документ</div>
                                            <div class="th">Статус</div>
                                            <div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>
                                        </div>
                                        <div class="thead-bottom">
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th"></div>
                                            <div class="th">План</div>
                                            <div class="th">Факт</div>
                                        </div>
                                    </div>
                                    <div id="rightTableBody"></div>
                                </div>
                            </div>
                        </div>
                        <div class="side-panel">
                            <div class="side-panel-title">Список чек-листов по проекту</div>
                            <div class="side-panel-list" id="projectChecklistList"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <script>
            const dialogId = {dialog_id_json};
            const projectRootYandexPath = {project_root_yandex_path_json};
            let projectRootYandexUrl = {project_root_yandex_url_json};

            let projectRootYandexPrepared = {project_root_yandex_prepared_json};
            let projectRootYandexPreparing = false;

            let rawGroups = {groups_json};
            let rawProjectChecklists = {project_checklists_json};
            let rawItems = {items_json};
            let collabTitle = {collab_title_json};

            let groups = Array.isArray(rawGroups) ? rawGroups : [];
            let projectChecklists = Array.isArray(rawProjectChecklists) ? rawProjectChecklists : [];
            let items = Array.isArray(rawItems) ? rawItems : [];

            let currentChecklistKey = {checklist_key_json};
            let checklistTitle = {checklist_title_json};
            let checklistCache = {{}};
            let sessionChanges = [];
            let currentEditor = {{
                id: "",
                name: ""
            }};
            window.currentEditor = currentEditor;
            let currentEditorReady = false;
            let currentEditorReadyPromise = null;
                        const saveStateEl = document.getElementById('saveState');
                        const uploadProgressBoxEl = document.getElementById('uploadProgressBox');
                        const uploadProgressFileEl = document.getElementById('uploadProgressFile');
                        const uploadProgressBarEl = document.getElementById('uploadProgressBar');
                        const uploadProgressStatusEl = document.getElementById('uploadProgressStatus');
                        const uploadProgressPercentEl = document.getElementById('uploadProgressPercent');
                        let uploadProgressHideTimer = null;
                        let uploadJobPollTimer = null;
            const leftTableBodyEl = document.getElementById('leftTableBody');
            const middleTableBodyEl = document.getElementById('middleTableBody');
            const rightTableBodyEl = document.getElementById('rightTableBody');
            const progressValueEl = document.getElementById('progressValue');
            const progressBarEl = document.getElementById('progressBar');
            const progressBoxEl = document.querySelector('.progress-box');
            const popupTitleEl = document.getElementById('popupTitle');
            const projectRootFolderBoxEl = document.getElementById('projectRootFolderBox');
            const projectChecklistListEl = document.getElementById('projectChecklistList');
            const tablePanels = document.querySelectorAll('.table-panel');
            const tablesGridEl = document.querySelector('.tables-grid');
            const leftTableEl = tablePanels[0] ? tablePanels[0].querySelector('.table') : null;
            const middleTableEl = tablePanels[1] ? tablePanels[1].querySelector('.table') : null;
            const rightTableEl = tablePanels[2] ? tablePanels[2].querySelector('.table') : null;
            const idTableShellHtml = leftTableEl ? leftTableEl.innerHTML : '';
            const idDateVisibility = {{ 1: false, 2: false, 3: false }};
            const oprDateVisibility = {{ 1: false }};
            const conceptDateVisibility = {{ 1: false }};
            const debugLastEventEl = document.getElementById('debugLastEvent');
            const debugPanelEl = document.getElementById('debugPanel');
            const debugLogsLinkEl = document.getElementById('debugLogsLink');
            const adminPanelLinkEl = document.getElementById('adminPanelLink');
            const debugStopYandexWarmupBtn = document.getElementById('debugStopYandexWarmupBtn');
            const debugYandexWarmupStopStateEl = document.getElementById('debugYandexWarmupStopState');
            const allowedDebugUserIds = new Set(['138', '18']);
            const fileDeleteAllowedUserIds = new Set({file_delete_allowed_user_ids_json});

            function getFileDeleteActor() {{
                return {{
                    id: String(currentEditor && currentEditor.id || '').trim(),
                    name: String(currentEditor && currentEditor.name || '').trim() || 'Пользователь'
                }};
            }}

            function canCurrentUserDeleteFiles() {{
                const actor = getFileDeleteActor();
                return fileDeleteAllowedUserIds.has(String(actor.id || '').trim());
            }}

            function showFileDeleteForbiddenAlert() {{
                alert('У вас недостаточно прав на удаление файлов');
            }}
            function updateDebugPanelAccess() {{
                const currentUserId = String(currentEditor.id || '');
                if (debugPanelEl) {{
                    debugPanelEl.style.display = allowedDebugUserIds.has(currentUserId) ? '' : 'none';
                }}
                if (debugLogsLinkEl) {{
                    debugLogsLinkEl.href = 'debug/logs?userId=' + encodeURIComponent(currentUserId);
                }}
                if (adminPanelLinkEl) {{
                    adminPanelLinkEl.href = appUrl('admin') + '?userId=' + encodeURIComponent(currentUserId);
                }}
            }}
            function detectAppBasePath() {{
                const path = String(window.location.pathname || '/').replace(/\\/+$/, '');
                const suffixes = ['/popup', '/launch', '/textarea', '/install', '/health', '/debug/logs', '/admin', '/admin/upload'];

                for (const suffix of suffixes) {{
                    if (path === suffix) return '';
                    if (path.endsWith(suffix)) {{
                        return path.slice(0, -suffix.length) || '';
                    }}
                }}

                return '';
            }}

            const APP_BASE_PATH = detectAppBasePath();
            const APP_BASE_URL = window.location.origin + (APP_BASE_PATH || '');

            function appUrl(path) {{
                return APP_BASE_URL + '/' + String(path || '').replace(/^\\/+/, '');
            }}
            let closeSummarySent = false;
            let sessionDirty = false;

            function setSaveState(mode, text) {{
                saveStateEl.classList.remove('saving', 'error');
                if (mode === 'saving') saveStateEl.classList.add('saving');
                if (mode === 'error') saveStateEl.classList.add('error');
                saveStateEl.textContent = text;
            }}
            function esc(v) {{
                if (v === null || v === undefined) return '';
                return String(v).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
            }}
            function toInputDate(value) {{
                if (!value) return '';
                const parts = value.split('.');
                if (parts.length !== 3) return '';
                return `${{parts[2]}}-${{parts[1]}}-${{parts[0]}}`;
            }}
            function fromInputDate(value) {{
                if (!value) return '';
                const parts = value.split('-');
                if (parts.length !== 3) return '';
                return `${{parts[2]}}.${{parts[1]}}.${{parts[0]}}`;
            }}
            function normalizeStatus(status) {{
                const s = String(status || '').trim();
                if (s === 'Есть') return 'Есть';
                if (s === 'Нет') return 'Нет';
                if (s === 'Не требуется') return 'Не требуется';
                return '';
            }}
            function indicatorClass(status) {{
                const s = normalizeStatus(status);
                if (s === 'Есть') return 'status-indicator green';
                if (s === 'Нет' || s === 'Не требуется') return 'status-indicator gray';
                return 'status-indicator';
            }}

            function getItemDocuments(item) {{
                const docs = Array.isArray(item && item.documents) ? item.documents : [];
                if (docs.length) {{
                    return docs;
                }}

                const legacyUrl = String(item && item.documentUrl || '').trim();
                const legacyName = String(item && item.documentName || '').trim();

                if (legacyUrl || legacyName) {{
                    return [{{
                        id: 'legacy_' + String(item && item.id || ''),
                        name: legacyName || 'Файл',
                        path: legacyUrl,
                        fileUrl: legacyUrl,
                        previewUrl: legacyUrl,
                        size: 0,
                        modifiedAt: '',
                        source: 'local'
                    }}];
                }}

                return [];
            }}

            function clearUploadProgressHideTimer() {{
                if (uploadProgressHideTimer) {{
                    clearTimeout(uploadProgressHideTimer);
                    uploadProgressHideTimer = null;
                }}
            }}

            function stopUploadJobPolling() {{
                if (uploadJobPollTimer) {{
                    clearInterval(uploadJobPollTimer);
                    uploadJobPollTimer = null;
                }}
            }}

            function setUploadProgressVisible(visible) {{
                if (!uploadProgressBoxEl) return;
                uploadProgressBoxEl.style.display = visible ? '' : 'none';
            }}

            function updateUploadProgress(fileName, percent, statusText) {{
                clearUploadProgressHideTimer();

                const safePercent = Math.max(0, Math.min(100, Math.round(Number(percent || 0))));

                setUploadProgressVisible(true);

                if (uploadProgressFileEl) {{
                    uploadProgressFileEl.textContent = String(fileName || 'Файл');
                    uploadProgressFileEl.title = String(fileName || 'Файл');
                }}

                if (uploadProgressBarEl) {{
                    uploadProgressBarEl.style.width = safePercent + '%';
                }}

                if (uploadProgressPercentEl) {{
                    uploadProgressPercentEl.textContent = safePercent + '%';
                }}

                if (uploadProgressStatusEl) {{
                    uploadProgressStatusEl.textContent = String(statusText || 'Загрузка...');
                }}
            }}

            function completeUploadProgress(fileName, statusText) {{
                updateUploadProgress(fileName, 100, statusText || 'Готово');

                uploadProgressHideTimer = setTimeout(function () {{
                    setUploadProgressVisible(false);
                }}, 2600);
            }}

            function failUploadProgress(fileName, statusText) {{
                updateUploadProgress(fileName, 100, statusText || 'Ошибка загрузки');

                if (uploadProgressBoxEl) {{
                    uploadProgressBoxEl.style.background = '#fef2f2';
                    uploadProgressBoxEl.style.borderColor = '#fecaca';
                    uploadProgressBoxEl.style.color = '#b42318';
                }}

                uploadProgressHideTimer = setTimeout(function () {{
                    if (uploadProgressBoxEl) {{
                        uploadProgressBoxEl.style.background = '#fff7ed';
                        uploadProgressBoxEl.style.borderColor = '#fed7aa';
                        uploadProgressBoxEl.style.color = '#9a3412';
                    }}
                    setUploadProgressVisible(false);
                }}, 5000);
            }}

            function getUploadJobStageText(job) {{
                const status = String(job && job.status || '');
                const stage = String(job && job.stage || '');

                if (status === 'queued') return 'Файл сохранён. Ожидает синхронизации...';
                if (status === 'running' && stage === 'folder_prepare') return 'Готовим папку Яндекс.Диска...';
                if (status === 'running' && stage === 'yandex_upload') return 'Загружаем копию на Яндекс.Диск...';
                if (status === 'synced') return 'Файл загружен и синхронизирован';
                if (status === 'skipped' && stage === 'yandex_disabled') return 'Файл сохранён локально. Яндекс отключён';
                if (status === 'error') return 'Файл сохранён, ошибка синхронизации';
                if (status === 'cancelled') return 'Загрузка отменена';
                return 'Обрабатываем файл...';
            }}

            function pollUploadJobStatus(jobId, fileName) {{
                stopUploadJobPolling();

                if (!jobId) {{
                    completeUploadProgress(fileName, 'Файл сохранён');
                    return;
                }}

                uploadJobPollTimer = setInterval(async function () {{
                    try {{
                        const response = await fetch(
                            appUrl('api/checklist/upload-job-status') +
                            '?jobId=' + encodeURIComponent(jobId)
                        );

                        const result = await response.json().catch(() => ({{}}));

                        if (!response.ok || !result.ok) {{
                            return;
                        }}

                        const status = String(result.status || '');
                        const rawJobPercent = Number(result.progressPercent || 0);
                        const displayPercent = status === 'queued'
                            ? 82
                            : status === 'running'
                                ? Math.max(84, Math.min(98, 75 + Math.round(rawJobPercent * 0.23)))
                                : 100;

                        updateUploadProgress(fileName, displayPercent, getUploadJobStageText(result));

                        if (['synced', 'skipped', 'error', 'cancelled', 'deleted'].includes(status)) {{
                            stopUploadJobPolling();

                            if (status === 'error') {{
                                failUploadProgress(fileName, result.error || 'Ошибка синхронизации');
                            }} else {{
                                completeUploadProgress(fileName, getUploadJobStageText(result));
                            }}
                        }}

                    }} catch (e) {{
                        console.log('upload job polling error:', e);
                    }}
                }}, 900);
            }}

            function formatFileSize(size) {{
                const value = Number(size || 0);
                if (!value || value <= 0) return '';

                const units = ['Б', 'КБ', 'МБ', 'ГБ'];
                let current = value;
                let unitIndex = 0;

                while (current >= 1024 && unitIndex < units.length - 1) {{
                    current /= 1024;
                    unitIndex += 1;
                }}

                if (unitIndex === 0) {{
                    return Math.round(current) + ' ' + units[unitIndex];
                }}

                if (current >= 100) return current.toFixed(0) + ' ' + units[unitIndex];
                if (current >= 10) return current.toFixed(1) + ' ' + units[unitIndex];
                return current.toFixed(2) + ' ' + units[unitIndex];
            }}

            function confirmStatusNoWithFiles(itemName, documents) {{
                const docs = Array.isArray(documents) ? documents : [];
                if (!docs.length) {{
                    return true;
                }}

                if (!canCurrentUserDeleteFiles()) {{
                    showFileDeleteForbiddenAlert();
                    return false;
                }}

                const safeItemName = String(itemName || 'пункт').trim() || 'пункт';

                return window.confirm(
                    'В пункте "' + safeItemName + '" уже загружены файлы.\\n\\n' +
                    'При выборе статуса "Нет" эти файлы будут удалены.\\n\\n' +
                    'Продолжить?'
                );
            }}

            function renderTitle() {{
                if (collabTitle) {{
                    popupTitleEl.innerHTML = esc(checklistTitle) + ' <small>— ' + esc(collabTitle) + '</small>';
                }} else {{
                    popupTitleEl.textContent = checklistTitle;
                }}
            }}
            function fetchCurrentUserIfPossible() {{
                if (currentEditorReadyPromise) {{
                    return currentEditorReadyPromise;
                }}

                currentEditorReadyPromise = new Promise(function(resolve) {{
                    try {{
                        if (!(window.BX24 && typeof window.BX24.init === 'function')) {{
                            currentEditorReady = true;
                            updateDebugPanelAccess();
                            resolve(currentEditor);
                            return;
                        }}

                        let resolved = false;

                        function finish() {{
                            if (resolved) return;
                            resolved = true;
                            currentEditorReady = true;
                            updateDebugPanelAccess();
                            resolve(currentEditor);
                        }}

                        window.BX24.init(function () {{
                            try {{
                                window.BX24.callMethod('user.current', {{}}, function(result) {{
                                    try {{
                                        if (!result.error()) {{
                                            const data = result.data() || {{}};
                                            const fullName = [data.NAME, data.LAST_NAME].filter(Boolean).join(' ').trim();

                                            currentEditor = {{
                                                id: String(data.ID || ''),
                                                name: fullName || String(data.NAME || '') || ''
                                            }};
                                            window.currentEditor = currentEditor;
                                        }}
                                    }} catch (e) {{
                                        console.log('user.current parse error:', e);
                                    }} finally {{
                                        finish();
                                    }}
                                }});
                            }} catch (e) {{
                                console.log('user.current call error:', e);
                                finish();
                            }}
                        }});
                    }} catch (e) {{
                        console.log('fetchCurrentUserIfPossible skipped:', e);
                        currentEditorReady = true;
                        updateDebugPanelAccess();
                        resolve(currentEditor);
                    }}
                }});

                return currentEditorReadyPromise;
            }}
            function setDebugText(text) {{
                if (debugLastEventEl) {{
                    debugLastEventEl.textContent = text;
                }}
            }}
            function debugLog(event, payload = {{}}, useBeacon = false) {{
                const body = JSON.stringify({{
                    event,
                    dialogId,
                    checklistKey: currentChecklistKey,
                    payload,
                    href: window.location.href,
                    ts: new Date().toISOString()
                }});

                setDebugText(event);

                try {{
                    const url = APP_BASE_URL + '/api/debug/event';

                    if (useBeacon && navigator.sendBeacon) {{
                        const blob = new Blob([body], {{ type: 'application/json' }});
                        const ok = navigator.sendBeacon(url, blob);
                        setDebugText(event + ' | beacon=' + ok);
                        return;
                    }}

                    fetch(url, {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json'
                        }},
                        body
                    }})
                    .then(r => {{
                        setDebugText(event + ' | http=' + r.status);
                    }})
                    .catch(err => {{
                        console.log('debugLog fetch error:', err);
                        setDebugText(event + ' | fetch error');
                    }});
                }} catch (e) {{
                    console.log('debugLog error:', e);
                    setDebugText(event + ' | js error');
                }}
            }}

            async function stopCurrentYandexWarmupFromDebugPanel() {{
                await fetchCurrentUserIfPossible();

                const actor = getFileDeleteActor();

                if (!allowedDebugUserIds.has(String(actor.id || '').trim())) {{
                    alert('Остановка warmup доступна только техническим пользователям');
                    return;
                }}

                if (!window.confirm('Остановить создание папок Яндекс.Диска для текущей коллабы? Уже созданные папки останутся на месте.')) {{
                    return;
                }}

                if (debugStopYandexWarmupBtn) {{
                    debugStopYandexWarmupBtn.disabled = true;
                    debugStopYandexWarmupBtn.style.opacity = '0.65';
                }}

                if (debugYandexWarmupStopStateEl) {{
                    debugYandexWarmupStopStateEl.textContent = 'Отправляем команду остановки...';
                }}

                try {{
                    const response = await fetch(appUrl('api/project-yandex-warmup/stop'), {{
                        method: 'POST',
                        headers: {{
                            'Content-Type': 'application/json'
                        }},
                        body: JSON.stringify({{
                            dialogId,
                            checklistKey: currentChecklistKey,
                            userId: actor.id,
                            userName: actor.name
                        }})
                    }});

                    const result = await response.json().catch(() => ({{}}));

                    if (!response.ok || !result.ok) {{
                        throw new Error(result.error || 'warmup stop failed');
                    }}

                    debugLog('debug_yandex_warmup_stop_requested', result);

                    if (debugYandexWarmupStopStateEl) {{
                        const stateText = result.wasRunning
                            ? 'Остановка запрошена. Текущая папка завершится, следующая уже не начнётся.'
                            : result.wasQueued
                                ? 'Проект убран из очереди.'
                                : 'Команда остановки принята.';

                        debugYandexWarmupStopStateEl.textContent = stateText;
                    }}

                }} catch (e) {{
                    console.log('stop warmup error:', e);

                    if (debugYandexWarmupStopStateEl) {{
                        debugYandexWarmupStopStateEl.textContent = 'Ошибка остановки: ' + String(e && e.message || e);
                    }}

                    debugLog('debug_yandex_warmup_stop_failed', {{
                        message: String(e && e.message || e)
                    }});

                }} finally {{
                    if (debugStopYandexWarmupBtn) {{
                        debugStopYandexWarmupBtn.disabled = false;
                        debugStopYandexWarmupBtn.style.opacity = '1';
                    }}
                }}
            }}

            if (debugStopYandexWarmupBtn) {{
                debugStopYandexWarmupBtn.addEventListener('click', stopCurrentYandexWarmupFromDebugPanel);
            }}

            function logRenderState(stage) {{
                debugLog('render_state', {{
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
                }});
            }}

            function logRenderError(stage, error) {{
                const message = (error && error.message) ? error.message : String(error || 'unknown error');
                const stack = (error && error.stack) ? error.stack : '';

                setDebugText(stage + ' | ERROR: ' + message);

                debugLog('render_error', {{
                    stage,
                    message,
                    stack
                }});
            }}
            function deepClone(value) {{
                return JSON.parse(JSON.stringify(value));
            }}

            function buildChecklistSnapshot() {{
                return {{
                    checklistKey: currentChecklistKey,
                    title: checklistTitle,
                    collabTitle,
                    groups: deepClone(groups),
                    projectChecklists: deepClone(projectChecklists),
                    items: deepClone(items)
                }};
            }}

            function syncChecklistCache() {{
                checklistCache[currentChecklistKey] = buildChecklistSnapshot();
            }}

            function applyChecklistData(data) {{
                const nextData = data || {{}};
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

                document.title = collabTitle
                    ? checklistTitle + ' — ' + collabTitle
                    : checklistTitle;
            }}

            async function flushCurrentChecklistSummary(reason = 'checklist_switch') {{
                syncChecklistCache();
                debugLog('close_summary_switch_skipped', {{
                    checklistKey: currentChecklistKey,
                    reason,
                    changesCount: sessionChanges.length,
                    dirty: !!sessionDirty
                }});
            }}

            async function loadChecklistByKey(checklistKey) {{
                const targetKey = String(checklistKey || '').trim() || 'id';
                if (targetKey === currentChecklistKey) {{
                    return;
                }}

                syncChecklistCache();
                await releaseChecklistLock(currentChecklistKey, false);
                setSaveState('saving', 'Загружаем...');

                try {{
                    const cachedData = checklistCache[targetKey];
                    if (cachedData) {{
                        applyChecklistData(deepClone(cachedData));
                        renderAll();
                        debugLog('checklist_switched_cached', {{
                            checklistKey: targetKey
                        }});
                        await acquireChecklistLock(targetKey, true);
                        startLockHeartbeat();
                        setSaveState('', 'Сохранено');
                        return;
                    }}

                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(targetKey)
                    );
                    const result = await response.json();

                    if (!response.ok) {{
                        throw new Error(result.error || 'load checklist failed');
                    }}

                    applyChecklistData(result);
                    renderAll();
                    debugLog('checklist_switched', {{
                        checklistKey: targetKey
                    }});
                    await acquireChecklistLock(targetKey, true);
                    startLockHeartbeat();
                    setSaveState('', 'Сохранено');
                }} catch (e) {{
                    console.log('loadChecklistByKey error:', e);
                    setSaveState('error', 'Ошибка загрузки чек-листа');
                }}
            }}

            async function reloadCurrentChecklistFromServer() {{
                const response = await fetch(
                    appUrl('api/checklist') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey)
                );
                const result = await response.json();

                if (!response.ok) {{
                    throw new Error(result.error || 'reload checklist failed');
                }}

                applyChecklistData(result);
                return result;
            }}

            function sendCloseSummaryOnce(eventName) {{
                if (eventName === 'popup_hidden' || suppressAutoCloseSave || closeSummarySent) {{
                    return;
                }}

                closeSummarySent = true;
                persistDirtyChecklists(eventName, true);
                releaseChecklistLock(currentChecklistKey, true);
            }}

            window.addEventListener('message', async function (event) {{
                const data = event && event.data ? event.data : {{}};
                const messageType = String(data && data.type || '');

                if (!['checklist-document-removed', 'checklist-document-uploaded', 'checklist-document-changed'].includes(messageType)) return;
                if (String(data.dialogId || '') !== String(dialogId || '')) return;
                if (String(data.checklistKey || '') !== String(currentChecklistKey || '')) return;

                if (messageType === 'checklist-document-removed') {{
                    const itemId = String(data.itemId || '').trim();
                    const documentName = String(data.documentName || '').trim() || 'Файл';
                    const item = items.find(x => String(x.id || '') === itemId);

                    pushSessionChange(
                        itemId,
                        item ? item.name : '',
                        'document',
                        documentName,
                        'Удален'
                    );

                    checklistSessionState[currentChecklistKey] = {{
                        changes: deepClone(sessionChanges),
                        dirty: !!sessionDirty
                    }};
                }}

                try {{
                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(currentChecklistKey)
                    );
                    const result = await response.json();
                    if (!response.ok) throw new Error(result.error || 'reload after folder sync failed');

                    applyChecklistData(result);
                    renderAll();
                }} catch (e) {{
                    console.log('folder sync error:', e);
                }}
            }});

            document.addEventListener('visibilitychange', function () {{
                if (document.visibilityState === 'hidden') {{
                    sendCloseSummaryOnce('popup_hidden');
                }}
            }});

            window.addEventListener('pagehide', function () {{
                sendCloseSummaryOnce('popup_pagehide');
            }});

            window.addEventListener('beforeunload', function () {{
                sendCloseSummaryOnce('popup_beforeunload');
            }});
            async function fetchChatTitleIfMissing() {{
                if (collabTitle) {{ renderTitle(); return; }}
                try {{
                    if (!(window.BX24 && typeof window.BX24.init === 'function')) {{ renderTitle(); return; }}
                    window.BX24.init(function () {{
                        try {{
                            window.BX24.callMethod('im.dialog.get', {{ dialog_id: dialogId }}, async function(result) {{
                                try {{
                                    if (result.error()) {{ renderTitle(); return; }}
                                    const data = result.data() || {{}};
                                    let title = data.title || data.name || (data.dialog && (data.dialog.title || data.dialog.name)) || (data.chat && (data.chat.title || data.chat.name)) || '';
                                    title = String(title || '').trim();
                                    if (!title) {{ renderTitle(); return; }}
                                    collabTitle = title;
                                    renderTitle();
                                    debugLog('chat_title_loaded', {{
                                        title: title
                                    }});
                                    try {{
                                        await fetch(appUrl('api/checklist/update-meta'), {{
                                            method: 'POST',
                                            headers: {{ 'Content-Type': 'application/json' }},
                                            body: JSON.stringify({{ dialogId, checklistKey: currentChecklistKey, field: 'collabTitle', value: title }})
                                        }});
                                    }} catch (e) {{
                                        console.log('save collabTitle error:', e);
                                    }}
                                }} catch (e) {{
                                    console.log('im.dialog.get parse error:', e);
                                    renderTitle();
                                }}
                            }});
                        }} catch (e) {{
                            console.log('im.dialog.get call error:', e);
                            renderTitle();
                        }}
                    }});
                }} catch (e) {{
                    console.log('BX24 init for title skipped:', e);
                    renderTitle();
                }}
            }}
            function calculateProgress() {{
                if (!progressValueEl || !progressBarEl) {{
                    return;
                }}

                const activeItems = items.filter(x => normalizeStatus(x.status) !== 'Не требуется');
                const completedItems = activeItems.filter(x => normalizeStatus(x.status) === 'Есть');
                const activeCount = activeItems.length;
                const completedCount = completedItems.length;
                const percent = activeCount ? Math.round((completedCount / activeCount) * 100) : 0;

                progressValueEl.textContent = percent + '%';
                progressBarEl.style.width = percent + '%';
            }}
            async function updateItem(itemId, field, value, checklistKey = currentChecklistKey) {{
                setSaveState('saving', 'Сохраняем...');

                if (typeof fetchCurrentUserIfPossible === 'function') {{
                    await fetchCurrentUserIfPossible();
                }}

                const actor = getFileDeleteActor();

                const response = await fetch(appUrl('api/checklist/update-item'), {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{
                        dialogId,
                        checklistKey,
                        itemId,
                        field,
                        value,
                        actingUserId: actor.id,
                        actingUserName: actor.name
                    }})
                }});

                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'save failed');
                setSaveState('', 'Сохранено');
                return result;
            }}
            async function addItem(groupId, name, checklistKey = currentChecklistKey) {{
                setSaveState('saving', 'Сохраняем...');
                const response = await fetch(appUrl('api/checklist/add-item'), {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ dialogId, checklistKey, groupId, name }})
                }});
                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'add item failed');
                setSaveState('', 'Сохранено');
                return result;
            }}

            async function removeDocument(itemId, documentId = '') {{
                setSaveState('saving', 'Сохраняем...');

                if (typeof fetchCurrentUserIfPossible === 'function') {{
                    await fetchCurrentUserIfPossible();
                }}

                if (!canCurrentUserDeleteFiles()) {{
                    showFileDeleteForbiddenAlert();
                    setSaveState('', 'Сохранено');
                    throw new Error('У вас недостаточно прав на удаление файлов');
                }}

                const actor = getFileDeleteActor();

                const response = await fetch(appUrl('api/checklist/remove-document'), {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{
                        dialogId,
                        checklistKey: currentChecklistKey,
                        itemId,
                        documentId,
                        actingUserId: actor.id,
                        actingUserName: actor.name
                    }})
                }});

                const result = await response.json();
                if (!response.ok || !result.ok) throw new Error(result.error || 'remove document failed');
                setSaveState('', 'Сохранено');
                return result;
            }}

            async function uploadDocument(itemId, file) {{
                const uploadId = 'front_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
                const item = items.find(x => x.id === itemId);
                const itemGroup = String(item && item.group ? item.group : '');
                const fileName = String(file && file.name || '');
                const fileSize = Number(file && file.size || 0);
                const fileType = String(file && file.type || '');

                debugLog('upload_frontend_started', {{
                    uploadId,
                    dialogId,
                    checklistKey: currentChecklistKey,
                    itemId,
                    itemGroup,
                    itemName: item ? String(item.name || '') : '',
                    fileName,
                    fileSize,
                    fileType
                }});

                setSaveState('saving', 'Загружаем файл...');
                updateUploadProgress(fileName, 0, 'Начинаем загрузку...');

                const formData = new FormData();
                formData.append('dialogId', dialogId);
                formData.append('itemId', itemId);
                formData.append('file', file);
                formData.append('checklistKey', currentChecklistKey);
                formData.append('itemGroup', itemGroup);

                return await new Promise(function (resolve, reject) {{
                    const xhr = new XMLHttpRequest();

                    xhr.open('POST', appUrl('api/checklist/upload-document'), true);

                    xhr.upload.onprogress = function (event) {{
                        if (!event.lengthComputable) {{
                            updateUploadProgress(fileName, 15, 'Передаём файл в приложение...');
                            return;
                        }}

                        const rawPercent = Math.round((event.loaded / event.total) * 100);
                        const displayPercent = Math.max(1, Math.min(70, Math.round(rawPercent * 0.70)));

                        updateUploadProgress(
                            fileName,
                            displayPercent,
                            'Передаём файл в приложение... ' + rawPercent + '%'
                        );
                    }};

                    xhr.onload = function () {{
                        const responseText = String(xhr.responseText || '');

                        debugLog('upload_frontend_response_received', {{
                            uploadId,
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId,
                            itemGroup,
                            fileName,
                            fileSize,
                            fileType,
                            status: xhr.status,
                            ok: xhr.status >= 200 && xhr.status < 300,
                            responseTextStart: responseText.slice(0, 1600)
                        }});

                        let result = {{}};

                        try {{
                            result = JSON.parse(responseText || '{{}}');
                        }} catch (parseError) {{
                            debugLog('upload_frontend_json_parse_failed', {{
                                uploadId,
                                dialogId,
                                checklistKey: currentChecklistKey,
                                itemId,
                                itemGroup,
                                fileName,
                                fileSize,
                                status: xhr.status,
                                responseTextStart: responseText.slice(0, 1600),
                                error: String(parseError && parseError.message || parseError)
                            }});

                            failUploadProgress(fileName, xhr.status === 413
                                ? 'Файл слишком большой для сервера'
                                : 'Некорректный ответ сервера'
                            );

                            reject(new Error(xhr.status === 413
                                ? 'Файл слишком большой для сервера'
                                : 'Некорректный ответ сервера при загрузке файла'
                            ));
                            return;
                        }}

                        if (xhr.status < 200 || xhr.status >= 300 || !result.ok) {{
                            debugLog('upload_frontend_failed_response', {{
                                uploadId,
                                dialogId,
                                checklistKey: currentChecklistKey,
                                itemId,
                                itemGroup,
                                fileName,
                                fileSize,
                                status: xhr.status,
                                result
                            }});

                            failUploadProgress(fileName, result.error || result.details || 'Ошибка загрузки файла');
                            reject(new Error(result.error || result.details || 'upload document failed'));
                            return;
                        }}

                        debugLog('upload_frontend_completed', {{
                            uploadId,
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId,
                            itemGroup,
                            fileName,
                            fileSize,
                            uploadJobId: result.uploadJobId || '',
                            yandexMirrorQueued: !!result.yandexMirrorQueued
                        }});

                        updateUploadProgress(fileName, 80, 'Файл сохранён. Запускаем синхронизацию...');
                        pollUploadJobStatus(result.uploadJobId || '', fileName);

                        setSaveState('', 'Сохранено');
                        resolve(result);
                    }};

                    xhr.onerror = function () {{
                        debugLog('upload_frontend_xhr_error', {{
                            uploadId,
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId,
                            itemGroup,
                            fileName,
                            fileSize,
                            fileType,
                            status: xhr.status || '',
                            responseTextStart: String(xhr.responseText || '').slice(0, 1600)
                        }});

                        failUploadProgress(fileName, 'Ошибка сети при загрузке файла');
                        reject(new Error('network upload error'));
                    }};

                    xhr.onabort = function () {{
                        debugLog('upload_frontend_xhr_abort', {{
                            uploadId,
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId,
                            itemGroup,
                            fileName,
                            fileSize,
                            fileType
                        }});

                        failUploadProgress(fileName, 'Загрузка отменена');
                        reject(new Error('upload aborted'));
                    }};

                    xhr.ontimeout = function () {{
                        debugLog('upload_frontend_xhr_timeout', {{
                            uploadId,
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId,
                            itemGroup,
                            fileName,
                            fileSize,
                            fileType
                        }});

                        failUploadProgress(fileName, 'Истекло время загрузки');
                        reject(new Error('upload timeout'));
                    }};

                    xhr.send(formData);
                }});
            }}
            function getItemsByGroup(groupId) {{
                return items
                    .filter(item => Number(item.group) === Number(groupId))
                    .sort((a, b) => Number(a.order || 0) - Number(b.order || 0));
            }}
            function hasItemsInGroup(groupId) {{
                return getItemsByGroup(groupId).length > 0;
            }}
            function renderProjectRootFolderButton() {{
                if (!projectRootFolderBoxEl) {{
                    return;
                }}

                const folderUrl = String(projectRootYandexUrl || '').trim();
                const folderPath = String(projectRootYandexPath || '').trim();
                const isReady = !!folderUrl;

                if (!folderPath) {{
                    projectRootFolderBoxEl.style.display = 'none';
                    projectRootFolderBoxEl.innerHTML = '';
                    return;
                }}

                projectRootFolderBoxEl.style.display = 'flex';

                const buttonText = isReady
                    ? 'Открыть папку в Яндекс Диске'
                    : 'Готовим структуру Яндекс.Диска...';

                projectRootFolderBoxEl.innerHTML = `
                    <button
                        class="doc-btn"
                        type="button"
                        data-role="view-project-root-folder"
                        data-folder-url="${{esc(folderUrl)}}"
                        title="${{esc(folderPath || 'Корневая папка проекта')}}"
                        style="min-width:260px; width:260px; height:32px; white-space:nowrap;"
                        ${{isReady ? '' : 'disabled'}}
                    >
                        ${{esc(buttonText)}}
                    </button>
                `;

                const btn = projectRootFolderBoxEl.querySelector('[data-role="view-project-root-folder"]');
                if (!btn) {{
                    return;
                }}

                if (!isReady) {{
                    btn.disabled = true;
                    btn.style.opacity = '0.65';
                    btn.style.cursor = 'default';
                    return;
                }}

                btn.addEventListener('click', function () {{
                    const url = String(this.dataset.folderUrl || '').trim();
                    if (url) {{
                        window.open(url, '_blank', 'noopener');
                    }}
                }});
            }}

            function renderProjectChecklistList() {{
                if (!projectChecklistListEl) {{
                    return;
                }}

                if (!Array.isArray(projectChecklists) || !projectChecklists.length) {{
                    projectChecklistListEl.innerHTML = '';
                    return;
                }}

                projectChecklistListEl.innerHTML = projectChecklists.map(item => {{
                    const active = item.key === currentChecklistKey ? 'side-link active' : 'side-link';
                    return `<button type="button" class="${{active}}" data-checklist-key="${{esc(item.key)}}">${{esc(item.title)}}</button>`;
                }}).join('');
                projectChecklistListEl.querySelectorAll('[data-checklist-key]').forEach(btn => {{
                    btn.addEventListener('click', async function () {{
                        const key = this.dataset.checklistKey;
                        await loadChecklistByKey(key);
                    }});
                }});
            }}
            function buildDocumentCell(item) {{
                if (normalizeStatus(item && item.status) === 'Не требуется') {{
                    return '';
                }}

                const documents = getItemDocuments(item);
                const itemId = String(item && item.id || '');
                const folderViewUrl = String(item.folderUrl || '').trim() || (documents.length ? (
                    appUrl('api/checklist/folder') +
                    '?dialogId=' + encodeURIComponent(dialogId) +
                    '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                    '&itemId=' + encodeURIComponent(itemId)
                ) : '');
                const showViewFolder = documents.length > 0 && !!folderViewUrl;

                const filesHtml = documents.map(doc => {{
                    const docId = String(doc.id || '');
                    const docName = String(doc.name || 'Файл');
                    const openUrl = appUrl('api/checklist/file') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(currentChecklistKey) +
                        '&itemId=' + encodeURIComponent(itemId) +
                        '&documentId=' + encodeURIComponent(docId);
                    const sizeText = formatFileSize(doc.size || 0);

                    return `
                        <div class="doc-file-row">
                            <a
                                href="javascript:void(0)"
                                class="doc-file-link"
                                data-role="view-file"
                                data-item-id="${{esc(itemId)}}"
                                data-document-id="${{esc(docId)}}"
                                data-open-url="${{esc(openUrl)}}"
                                title="${{esc(docName)}}"
                            >
                                ${{esc(docName)}}
                            </a>
                            <button
                                type="button"
                                class="doc-file-remove"
                                data-role="remove-file"
                                data-item-id="${{esc(itemId)}}"
                                data-document-id="${{esc(docId)}}"
                                data-document-name="${{esc(docName)}}"
                                title="Удалить файл"
                                ${{typeof disabledAttr === 'function' ? disabledAttr() : ''}}
                            >
                                ×
                            </button>
                            ${{sizeText ? `<span class="doc-file-meta">${{esc(sizeText)}}</span>` : ''}}
                        </div>
                    `;
                }}).join('');

                return `
                    <div class="doc-cell">
                        <div class="doc-actions">
                            <button
                                class="upload-btn"
                                type="button"
                                data-role="upload"
                                data-item-id="${{esc(itemId)}}"
                                ${{typeof disabledAttr === 'function' ? disabledAttr() : ''}}
                            >
                                Загрузить
                            </button>

                            ${{showViewFolder ? `
                                <button
                                    class="doc-btn"
                                    type="button"
                                    data-role="view-folder"
                                    data-item-id="${{esc(itemId)}}"
                                    data-folder-url="${{esc(folderViewUrl)}}"
                                >
                                    Посмотреть
                                </button>
                            ` : ''}}
                        </div>

                        ${{documents.length ? `
                            <div class="doc-files">
                                ${{filesHtml}}
                            </div>
                        ` : ''}}

                        <input
                            type="file"
                            data-role="file-input"
                            data-item-id="${{esc(itemId)}}"
                            style="display:none;"
                            multiple
                            ${{typeof disabledAttr === 'function' ? disabledAttr() : ''}}
                        >
                    </div>
                `;
            }}
            function pushSessionChange(itemId, itemName, field, oldValue, newValue) {{
                if (String(oldValue || '') === String(newValue || '')) {{
                    return;
                }}

                sessionChanges.push({{
                    field,
                    itemId: itemId || '',
                    itemName: itemName || '',
                    oldValue: oldValue || '',
                    newValue: newValue || ''
                }});
                sessionDirty = true;
                closeSummarySent = false;
                setSaveState('saving', 'Есть несохраненные изменения');
            }}
            function autoGrowTextarea(el) {{
                if (!el) return;

                const baseHeight = Number(el.dataset.baseHeight || 0) || 32;
                const initialValue = String(el.dataset.initialValue || '');
                el.dataset.baseHeight = String(baseHeight);
                el.style.height = baseHeight + 'px';

                if (String(el.value || '').length <= initialValue.length) {{
                    return;
                }}

                let nextHeight = baseHeight;
                while (el.scrollHeight > el.clientHeight && nextHeight < 1600) {{
                    nextHeight *= 2;
                    el.style.height = nextHeight + 'px';
                }}
            }}
            function extractConceptUnit(placeholder) {{
                const raw = String(placeholder || '').trim();
                const match = raw.match(new RegExp('^_+\\s*(.+)$'));
                return match ? match[1].trim() : '';
            }}
            function formatConceptTextStatus(value, placeholder) {{
                const rawValue = String(value || '').trim();
                if (!rawValue) {{
                    return '';
                }}

                if (rawValue === 'Не требуется') {{
                    return rawValue;
                }}

                const unit = extractConceptUnit(placeholder);
                if (!unit) {{
                    return rawValue;
                }}

                if (rawValue.toLowerCase().endsWith(unit.toLowerCase())) {{
                    return rawValue;
                }}

                if (!/^[0-9]+([.,][0-9]+)?$/.test(rawValue)) {{
                    return rawValue;
                }}

                return unit.startsWith('%') ? rawValue + unit : rawValue + ' ' + unit;
            }}
            function buildClientItemId(prefix) {{
                return prefix + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
            }}
            function createLocalItem(groupId, name, checklistKey) {{
                const groupItems = getItemsByGroup(groupId);
                const nextOrder = groupItems.length + 1;

                if (checklistKey === 'concept') {{
                    return {{
                        id: buildClientItemId('concept_g' + groupId + '_custom'),
                        group: groupId,
                        order: nextOrder,
                        name,
                        source: '',
                        statusKind: 'text',
                        statusOptions: [],
                        statusPlaceholder: '',
                        status: '',
                        extraInfo: '',
                        extraInfoPlaceholder: '',
                        documentUrl: '',
                        documentName: '',
                        isCustom: true,
                        priority: 'white'
                    }};
                }}

                return {{
                    id: buildClientItemId('item_g' + groupId + '_custom'),
                    group: groupId,
                    order: nextOrder,
                    name,
                    priority: 'white',
                    status: '',
                    plan: '',
                    fact: '',
                    documentUrl: '',
                    documentName: '',
                    isCustom: true
                }};
            }}
            function resolveConceptGroupId(item) {{
                const itemId = String(item && item.id || '');
                const name = String(item && item.name || '').trim();

                if (itemId.startsWith('concept_g')) {{
                    const match = itemId.match(new RegExp('^concept_g(\\d+)_'));
                    if (match) {{
                        const groupId = Number(match[1]);
                        if (groupId && groupId !== 10) {{
                            return groupId;
                        }}
                    }}
                }}

                const byName = groups.find(group => Number(group.id) !== 10 && Array.isArray(items) && items.some(existing =>
                    existing !== item &&
                    Number(existing.group) === Number(group.id) &&
                    String(existing.name || '').trim() === name
                ));
                if (byName) {{
                    return Number(byName.id);
                }}

                return 1;
            }}
            function conceptIndicatorClass(item) {{
                const status = String(item.status || '').trim();
                const kind = String(item.statusKind || '').trim();

                if (status === 'Не требуется') {{
                    return 'status-indicator gray';
                }}

                if (kind === 'bool') {{
                    if (status === 'Да') return 'status-indicator green';
                    if (status === 'Нет') return 'status-indicator gray';
                    return 'status-indicator';
                }}

                return status ? 'status-indicator green' : 'status-indicator';
            }}
            function buildConceptStatusCell(item) {{
                if (item.statusKind === 'bool') {{
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${{esc(item.id)}}">
                            <option value="" ${{item.status === '' ? 'selected' : ''}}></option>
                            <option value="Да" ${{item.status === 'Да' ? 'selected' : ''}}>Да</option>
                            <option value="Нет" ${{item.status === 'Нет' ? 'selected' : ''}}>Нет</option>
                            <option value="Не требуется" ${{item.status === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                        </select>
                    `;
                }}
                if (item.statusKind === 'select') {{
                    const options = [''].concat(item.statusOptions || [], ['Не требуется']);
                    return `
                        <select class="status-select" data-role="concept-status" data-item-id="${{esc(item.id)}}">
                            ${{options.map(option => `<option value="${{esc(option)}}" ${{item.status === option ? 'selected' : ''}}>${{esc(option)}}</option>`).join('')}}
                        </select>
                    `;
                }}
                return `<input class="status-select" type="text" data-role="concept-status" data-item-id="${{esc(item.id)}}" placeholder="${{esc(item.statusPlaceholder || '')}}" value="${{esc(item.status || '')}}">`;
            }}
            function buildConceptExtraCell(item) {{
                return `<textarea class="concept-extra-textarea" data-role="concept-extra" data-item-id="${{esc(item.id)}}" placeholder="${{esc(item.extraInfoPlaceholder || '')}}">${{esc(item.extraInfo || '')}}</textarea>`;
            }}
            function renderConceptGroup(group) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const rows = groupItems.map(item => `
                    <div class="row" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;" data-item-id="${{esc(item.id)}}">
                        <div class="td">
                            <div class="cell-name">
                                <div class="${{conceptIndicatorClass(item)}}"></div>
                                <div class="item-name" style="${{item.status === 'Не требуется' ? 'text-decoration:line-through;color:#98a2b3;' : ''}}">
                                    ${{esc(item.name)}}
                                </div>
                            </div>
                        </div>
                        <div class="td">${{esc(item.source || '')}}</div>
                        <div class="td">${{buildDocumentCell(item)}}</div>
                        <div class="td">${{buildConceptStatusCell(item)}}</div>
                        <div class="td">${{buildConceptExtraCell(item)}}</div>
                    </div>
                `).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="conceptAddItemInput_${{group.id}}" type="text" placeholder="Новый пункт">
                        <button class="add-item-btn" type="button" data-role="concept-add-item" data-group-id="${{group.id}}">Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}
            function buildConceptTableHtml(conceptGroups) {{
                return `
                    <div class="thead">
                        <div class="thead-top" style="grid-template-columns: 1.05fr 0.66fr 190px 150px 1.15fr;">
                            <div class="th">Пункт</div>
                            <div class="th">Нормативы</div>
                            <div class="th">Документ</div>
                            <div class="th">Статус</div>
                            <div class="th">Доп информация</div>
                        </div>
                    </div>
                    <div>${{conceptGroups.map(renderConceptGroup).join('')}}</div>
                `;
            }}
            function renderConceptTable() {{
                if (!leftTableEl || !rightTableEl || !tablesGridEl) {{
                    throw new Error('concept table containers not found');
                }}

                tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                tablesGridEl.style.justifyContent = '';

                if (tablePanels[0]) {{
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '';
                    tablePanels[0].style.width = '';
                    tablePanels[0].style.maxWidth = '';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = '';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }}
                if (tablePanels[2]) {{
                    tablePanels[2].style.display = 'none';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }}

                leftTableEl.style.width = '';
                leftTableEl.style.maxWidth = '';
                rightTableEl.style.width = '';
                rightTableEl.style.maxWidth = '';

                const visibleGroups = groups.filter(group => {{
                    if (Number(group.id) !== 10) return true;
                    return items.some(x => Number(x.group) === 10);
                }});

                const leftGroups = visibleGroups.filter(group => [1, 3, 5, 7, 9].includes(Number(group.id)));
                const rightGroups = visibleGroups.filter(group => [2, 4, 6, 8, 10].includes(Number(group.id)));

                leftTableEl.innerHTML = buildConceptTableHtml(leftGroups);
                rightTableEl.innerHTML = buildConceptTableHtml(rightGroups);
            }}

            function isIdChecklist() {{
                return currentChecklistKey === 'id';
            }}

            function isIdDatesVisible(groupId) {{
                return !!idDateVisibility[Number(groupId)];
            }}

            function getIdGridClass(showDates) {{
                return showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';
            }}

            function buildIdHeader(group, showDates) {{
                const groupId = Number(group.id);
                const toggleTitle = showDates ? 'Скрыть даты' : 'Показать даты';
                const toggleIcon = '📅';

                return `
                    <div class="thead-top ${{getIdGridClass(showDates)}}">
                        <div class="th">${{esc(group.title)}}</div>
                        <div class="th">Документ</div>
                        <div class="th th-status-with-toggle">
                            <span>Статус</span>
                            <button
                                type="button"
                                class="id-dates-toggle"
                                data-role="toggle-id-dates"
                                data-group-id="${{esc(groupId)}}"
                                title="${{esc(toggleTitle)}}"
                                aria-label="${{esc(toggleTitle)}}"
                            >
                                ${{toggleIcon}}
                            </button>
                        </div>
                        ${{showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>` : ''}}
                    </div>
                    ${{showDates ? `
                        <div class="thead-bottom ${{getIdGridClass(showDates)}}">
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th">План</div>
                            <div class="th">Факт</div>
                        </div>
                    ` : ''}}
                `;
            }}

            function renderIdGroup(group, showDates) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 4;
                const gridClass = getIdGridClass(showDates);

                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';

                    return `
                        <div class="${{rowClass}} ${{gridClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${{indicatorClass(item.status)}}"></div>
                                    <div class="item-name">${{esc(item.name)}}</div>
                                </div>
                            </div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}" ${{disabledAttr()}}>
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            ${{showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}" ${{disabledAttr()}}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}" ${{disabledAttr()}}>
                                </div>
                            ` : ''}}
                        </div>
                    `;
                }}).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт" ${{disabledAttr()}}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}" ${{disabledAttr()}}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}

            function renderIdPanel(mainGroup, appendNotRequired = false) {{
                const showDates = isIdDatesVisible(mainGroup.id);
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === 4)
                    : null;

                const panelHtml = `
                    <div class="table id-table">
                        <div class="thead">
                            ${{buildIdHeader(mainGroup, showDates)}}
                        </div>
                        <div>
                            ${{renderIdGroup(mainGroup, showDates)}}
                            ${{appendNotRequired && notRequiredGroup && hasItemsInGroup(4) ? renderIdGroup(notRequiredGroup, false) : ''}}
                        </div>
                    </div>
                `;

                return panelHtml;
            }}

            function renderIdTables() {{
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {{
                    throw new Error('id table containers not found');
                }}

                const idGroup = groups.find(g => Number(g.id) === 1) || {{ id: 1, title: 'ИД' }};
                const tuGroup = groups.find(g => Number(g.id) === 2) || {{ id: 2, title: 'ТУ' }};
                const otherGroup = groups.find(g => Number(g.id) === 3) || {{ id: 3, title: 'Прочее' }};

                tablesGridEl.classList.add('id-three-cols');
                tablesGridEl.style.gridTemplateColumns = 'minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)';
                tablesGridEl.style.justifyContent = '';

                if (tablePanels[0]) {{
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '';
                    tablePanels[0].style.width = '';
                    tablePanels[0].style.maxWidth = '';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = '';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }}
                if (tablePanels[2]) {{
                    tablePanels[2].style.display = '';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }}

                leftTableEl.style.width = '';
                leftTableEl.style.maxWidth = '';
                middleTableEl.style.width = '';
                middleTableEl.style.maxWidth = '';
                rightTableEl.style.width = '';
                rightTableEl.style.maxWidth = '';

                leftTableEl.innerHTML = renderIdPanel(idGroup, false);
                middleTableEl.innerHTML = renderIdPanel(tuGroup, false);
                rightTableEl.innerHTML = renderIdPanel(otherGroup, true);
            }}

            function renderGroup(group) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = group.id !== 4;
                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';
                    return `
                        <div class="${{rowClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td"><div class="cell-name"><div class="${{indicatorClass(item.status)}}"></div><div class="item-name">${{esc(item.name)}}</div></div></div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}" ${{disabledAttr()}}>
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            <div class="td"><input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}" ${{disabledAttr()}}></div>
                            <div class="td"><input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}" ${{disabledAttr()}}></div>
                        </div>
                    `;
                }}).join('');
                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт" ${{disabledAttr()}}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}" ${{disabledAttr()}}>Добавить пункт</button>
                    </div>` : '';
                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}

            function isConceptDatesVisible(groupId) {{
                return !!conceptDateVisibility[Number(groupId)];
            }}

            function getConceptGridClass(showDates) {{
                return showDates ? 'id-grid id-grid-expanded' : 'id-grid id-grid-compact';
            }}

            function buildConceptTableHeader(group, showDates) {{
                const groupId = Number(group.id);
                const toggleTitle = showDates ? 'Скрыть даты' : 'Показать даты';

                return `
                    <div class="thead-top ${{getConceptGridClass(showDates)}}">
                        <div class="th">${{esc(group.title)}}</div>
                        <div class="th">Документ</div>
                        <div class="th th-status-with-toggle">
                            <span>Статус</span>
                            <button
                                type="button"
                                class="id-dates-toggle"
                                data-role="toggle-concept-dates"
                                data-group-id="${{esc(groupId)}}"
                                title="${{esc(toggleTitle)}}"
                                aria-label="${{esc(toggleTitle)}}"
                            >
                                📅
                            </button>
                        </div>
                        ${{showDates ? `<div class="th center" style="grid-column: 4 / span 2;">Дата получения</div>` : ''}}
                    </div>
                    ${{showDates ? `
                        <div class="thead-bottom ${{getConceptGridClass(showDates)}}">
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th"></div>
                            <div class="th">План</div>
                            <div class="th">Факт</div>
                        </div>
                    ` : ''}}
                `;
            }}

            function renderConceptTableGroup(group, showDates) {{
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const gridClass = getConceptGridClass(showDates);

                const rows = groupItems.map(item => {{
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';

                    return `
                        <div class="${{rowClass}} ${{gridClass}}" data-item-id="${{esc(item.id)}}">
                            <div class="td">
                                <div class="cell-name">
                                    <div class="${{indicatorClass(item.status)}}"></div>
                                    <div class="item-name">${{esc(item.name)}}</div>
                                </div>
                            </div>
                            <div class="td">${{buildDocumentCell(item)}}</div>
                            <div class="td">
                                <select class="status-select" data-role="status" data-item-id="${{esc(item.id)}}" ${{disabledAttr()}}>
                                    <option value="" ${{normalizeStatus(item.status) === '' ? 'selected' : ''}}></option>
                                    <option value="Есть" ${{normalizeStatus(item.status) === 'Есть' ? 'selected' : ''}}>Есть</option>
                                    <option value="Нет" ${{normalizeStatus(item.status) === 'Нет' ? 'selected' : ''}}>Нет</option>
                                    <option value="Не требуется" ${{normalizeStatus(item.status) === 'Не требуется' ? 'selected' : ''}}>Не требуется</option>
                                </select>
                            </div>
                            ${{showDates ? `
                                <div class="td">
                                    <input class="date-input" type="date" data-role="plan" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.plan))}}" ${{disabledAttr()}}>
                                </div>
                                <div class="td">
                                    <input class="date-input" type="date" data-role="fact" data-item-id="${{esc(item.id)}}" value="${{esc(toInputDate(item.fact))}}" ${{disabledAttr()}}>
                                </div>
                            ` : ''}}
                        </div>
                    `;
                }}).join('');

                const addBlock = allowAdd ? `
                    <div class="add-item-row">
                        <input class="add-item-input" id="addItemInput_${{group.id}}" type="text" placeholder="Новый пункт" ${{disabledAttr()}}>
                        <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${{group.id}}" ${{disabledAttr()}}>Добавить пункт</button>
                    </div>
                ` : '';

                return `<div class="group-block"><div class="group-title">${{esc(group.title)}}</div>${{rows}}${{addBlock}}</div>`;
            }}

            function renderConceptPanel(mainGroup, appendNotRequired = false) {{
                const showDates = isConceptDatesVisible(mainGroup.id);
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === 10)
                    : null;

                return `
                    <div class="table id-table">
                        <div class="thead">
                            ${{buildConceptTableHeader(mainGroup, showDates)}}
                        </div>
                        <div>
                            ${{renderConceptTableGroup(mainGroup, showDates)}}
                            ${{appendNotRequired && notRequiredGroup && hasItemsInGroup(10) ? renderConceptTableGroup(notRequiredGroup, false) : ''}}
                        </div>
                    </div>
                `;
            }}

            function renderConceptTables() {{
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {{
                    throw new Error('concept table containers not found');
                }}

                const conceptGroup = groups.find(g => Number(g.id) === 1) || {{ id: 1, title: 'Концепция' }};

                tablesGridEl.classList.remove('id-three-cols');
                tablesGridEl.style.gridTemplateColumns = 'clamp(620px, 37vw, 760px)';
                tablesGridEl.style.justifyContent = 'start';

                if (tablePanels[0]) {{
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '0 0 auto';
                    tablePanels[0].style.width = 'clamp(620px, 37vw, 760px)';
                    tablePanels[0].style.maxWidth = 'clamp(620px, 37vw, 760px)';
                }}
                if (tablePanels[1]) {{
                    tablePanels[1].style.display = 'none';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }}
                if (tablePanels[2]) {{
                    tablePanels[2].style.display = 'none';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }}

                leftTableEl.style.width = '100%';
                leftTableEl.style.maxWidth = '100%';

                leftTableEl.innerHTML = renderConceptPanel(conceptGroup, true);
                middleTableEl.innerHTML = '';
                rightTableEl.innerHTML = '';
            }}

            function renderTables() {{
                if (currentChecklistKey === 'concept') {{
                    renderConceptTables();
                    return;
                }}

                if (currentChecklistKey === 'id') {{
                    renderIdTables();
                    return;
                }}

                if (currentChecklistKey === 'opr') {{
                    renderOprTables();
                    return;
                }}

                if (tablesGridEl) {{
                    tablesGridEl.classList.remove('id-three-cols');
                    tablesGridEl.style.gridTemplateColumns = '1fr 1fr';
                }}

                if (tablePanels[0]) tablePanels[0].style.display = '';
                if (tablePanels[1]) tablePanels[1].style.display = '';
                if (tablePanels[2]) tablePanels[2].style.display = 'none';

                if (leftTableEl) leftTableEl.innerHTML = idTableShellHtml;
                if (middleTableEl) middleTableEl.innerHTML = idTableShellHtml;

                const leftBody = document.getElementById('leftTableBody');
                const middleBody = document.getElementById('middleTableBody');

                if (!leftBody || !middleBody) {{
                    throw new Error('leftTableBody or middleTableBody not found');
                }}

                const leftGroups = groups.filter(g => Number(g.id) === 1 || Number(g.id) === 3);
                const rightGroups = groups.filter(g => Number(g.id) === 2);

                if (hasItemsInGroup(4)) {{
                    const notRequiredGroup = groups.find(g => Number(g.id) === 4);
                    if (notRequiredGroup) {{
                        rightGroups.push(notRequiredGroup);
                    }}
                }}

                leftBody.innerHTML = leftGroups.map(renderGroup).join('');
                middleBody.innerHTML = rightGroups.map(renderGroup).join('');
            }}
            function renderAll() {{
                renderTables();
                bindEvents();
                calculateProgress();
                renderTitle();
                renderProjectChecklistList();
                renderProjectRootFolderButton();

                if (progressBoxEl) {{
                    progressBoxEl.classList.toggle('id-accent', false);
                }}

                updateDebugPanelAccess();
                syncChecklistCache();
            }}
            function replaceItem(updatedItem) {{
                if (!updatedItem) return;

                const normalizedItem = Object.assign({{
                    folderKey: '',
                    folderPath: '',
                    folderUrl: '',
                    documents: [],
                    documentUrl: '',
                    documentName: ''
                }}, updatedItem || {{}});

                normalizedItem.documents = Array.isArray(normalizedItem.documents) ? normalizedItem.documents : [];

                if (!Object.prototype.hasOwnProperty.call(normalizedItem, 'documentUrl')) {{
                    normalizedItem.documentUrl = '';
                }}
                if (!Object.prototype.hasOwnProperty.call(normalizedItem, 'documentName')) {{
                    normalizedItem.documentName = '';
                }}

                const idx = items.findIndex(x => x.id === normalizedItem.id);
                if (idx >= 0) {{
                    items[idx] = Object.assign({{}}, items[idx], normalizedItem, {{
                        folderKey: normalizedItem.folderKey || '',
                        folderPath: normalizedItem.folderPath || '',
                        folderUrl: normalizedItem.folderUrl || '',
                        documents: normalizedItem.documents || [],
                        documentUrl: normalizedItem.documentUrl || '',
                        documentName: normalizedItem.documentName || ''
                    }});
                }} else {{
                    items.push(normalizedItem);
                }}
            }}
            function bindEvents() {{
                document.querySelectorAll('[data-role="concept-status"]').forEach(el => {{
                    const commitConceptStatus = function () {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldValue = item.status || '';
                        let newValue = this.value;

                        if (String(item.statusKind || '') === 'text') {{
                            newValue = formatConceptTextStatus(newValue, item.statusPlaceholder || '');
                            this.value = newValue;
                        }}

                        if (newValue === 'Не требуется') {{
                            item.group = 10;
                        }} else if (Number(item.group) === 10) {{
                            item.group = resolveConceptGroupId(item);
                        }}

                        item.status = newValue;
                        pushSessionChange(item.id, item.name, 'status', oldValue, newValue);
                        renderAll();
                    }};

                    const item = items.find(x => x.id === el.dataset.itemId);
                    if (!item) return;

                    if (String(item.statusKind || '') === 'text') {{
                        el.addEventListener('blur', commitConceptStatus);
                        el.addEventListener('keydown', function (e) {{
                            if (e.key === 'Enter') {{
                                e.preventDefault();
                                commitConceptStatus.call(this);
                            }}
                        }});
                    }} else {{
                        el.addEventListener('change', commitConceptStatus);
                    }}
                }});

                document.querySelectorAll('[data-role="concept-extra"]').forEach(el => {{
                    el.dataset.initialValue = String(el.value || '');
                    el.dataset.baseHeight = '32';
                    el.style.height = '32px';
                    el.addEventListener('input', function () {{
                        autoGrowTextarea(this);
                    }});

                    const handler = function () {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldValue = item.extraInfo || '';
                        const newValue = this.value;
                        item.extraInfo = newValue;
                        pushSessionChange(item.id, item.name, 'extraInfo', oldValue, newValue);
                        renderAll();
                    }};

                    el.addEventListener('change', handler);
                    el.addEventListener('blur', handler);
                }});
                document.querySelectorAll('[data-role="status"]').forEach(el => {{
                    el.addEventListener('change', async function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;

                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = this.value;
                        const oldDocuments = getItemDocuments(oldItem);

                        if (newValue === 'Нет' && oldDocuments.length) {{
                            if (typeof fetchCurrentUserIfPossible === 'function') {{
                                await fetchCurrentUserIfPossible();
                            }}

                            if (!confirmStatusNoWithFiles(item.name, oldDocuments)) {{
                                this.value = normalizeStatus(oldItem.status);
                                return;
                            }}
                        }}

                        if (newValue === 'Нет') {{
                            try {{
                                const result = await updateItem(item.id, 'status', newValue);
                                if (!result || !result.item) {{
                                    throw new Error('status save failed');
                                }}

                                replaceItem(result.item);

                                pushSessionChange(item.id, item.name, 'status', oldItem.status || '', newValue || '');
                                debugLog('status_changed', {{
                                    itemId: item.id,
                                    itemName: item.name,
                                    oldValue: oldItem.status || '',
                                    newValue: newValue || ''
                                }});

                                if (oldDocuments.length) {{
                                    const removedNames = oldDocuments.map(x => x.name || 'uploaded').join(', ');
                                    pushSessionChange(item.id, item.name, 'document', removedNames, 'Удален');
                                    debugLog('document_removed_by_status', {{
                                        itemId: item.id,
                                        itemName: item.name,
                                        oldValue: removedNames,
                                        newValue: 'Удален'
                                    }});
                                }}

                                renderAll();
                            }} catch (e) {{
                                console.log('status save error:', e);
                                setSaveState('error', 'Ошибка сохранения статуса');
                                this.value = normalizeStatus(oldItem.status);
                            }}
                            return;
                        }}

                        pushSessionChange(item.id, item.name, 'status', oldItem.status || '', newValue || '');
                        debugLog('status_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.status || '',
                            newValue: newValue || ''
                        }});

                        item.status = newValue;
                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="plan"]').forEach(el => {{
                    el.addEventListener('change', function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = fromInputDate(this.value);

                        item.plan = newValue;
                        pushSessionChange(item.id, item.name, 'plan', oldItem.plan || '', newValue || '');
                        debugLog('plan_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.plan || '',
                            newValue: newValue || ''
                        }});

                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="fact"]').forEach(el => {{
                    el.addEventListener('change', function() {{
                        const item = items.find(x => x.id === this.dataset.itemId);
                        if (!item) return;
                        const oldItem = JSON.parse(JSON.stringify(item));
                        const newValue = fromInputDate(this.value);

                        item.fact = newValue;
                        pushSessionChange(item.id, item.name, 'fact', oldItem.fact || '', newValue || '');
                        debugLog('fact_changed', {{
                            itemId: item.id,
                            itemName: item.name,
                            oldValue: oldItem.fact || '',
                            newValue: newValue || ''
                        }});

                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="add-item"]').forEach(btn => {{
                    btn.addEventListener('click', async function() {{
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('addItemInput_' + groupId);
                        if (!input) return;

                        const name = (input.value || '').trim();
                        if (!name) return;

                        this.disabled = true;

                        try {{
                            const result = await addItem(groupId, name, currentChecklistKey);
                            if (!result || !result.item) {{
                                throw new Error('add item failed');
                            }}

                            replaceItem(result.item);
                            pushSessionChange(result.item.id, result.item.name, 'add-item', '', result.item.name);

                            debugLog('item_added', {{
                                itemId: result.item.id,
                                itemName: result.item.name,
                                groupId: groupId
                            }});

                            input.value = '';
                            renderAll();
                        }} catch (e) {{
                            console.log('add item error:', e);
                            setSaveState('error', 'Ошибка добавления пункта');
                        }} finally {{
                            this.disabled = false;
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="toggle-id-dates"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const groupId = Number(this.dataset.groupId || 0);
                        if (![1, 2, 3].includes(groupId)) return;

                        idDateVisibility[groupId] = !idDateVisibility[groupId];
                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="toggle-concept-dates"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const groupId = Number(this.dataset.groupId || 0);
                        if (groupId !== 1) return;

                        conceptDateVisibility[groupId] = !conceptDateVisibility[groupId];
                        renderAll();
                    }});
                }});

                document.querySelectorAll('[data-role="concept-add-item"]').forEach(btn => {{
                    btn.addEventListener('click', async function() {{
                        const groupId = Number(this.dataset.groupId);
                        const input = document.getElementById('conceptAddItemInput_' + groupId);
                        if (!input) return;

                        const name = (input.value || '').trim();
                        if (!name) return;

                        this.disabled = true;

                        try {{
                            const result = await addItem(groupId, name, 'concept');
                            if (!result || !result.item) {{
                                throw new Error('add concept item failed');
                            }}

                            replaceItem(result.item);
                            pushSessionChange(result.item.id, result.item.name, 'add-item', '', result.item.name);

                            debugLog('item_added', {{
                                itemId: result.item.id,
                                itemName: result.item.name,
                                groupId: groupId
                            }});

                            input.value = '';
                            renderAll();
                        }} catch (e) {{
                            console.log('concept add item error:', e);
                            setSaveState('error', 'Ошибка добавления пункта');
                        }} finally {{
                            this.disabled = false;
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="upload"]').forEach(btn => {{
                    btn.addEventListener('click', function() {{
                        const input = document.querySelector('[data-role="file-input"][data-item-id="' + this.dataset.itemId + '"]');
                        if (input) input.click();
                    }});
                }});

                document.querySelectorAll('[data-role="view-folder"]').forEach(btn => {{
                    btn.addEventListener('click', function () {{
                        const folderUrl = this.dataset.folderUrl || '';
                        if (!folderUrl) return;

                        try {{
                            window.open(folderUrl, '_blank');
                        }} catch (e) {{
                            console.log('open folder error:', e);
                            setSaveState('error', 'Ошибка открытия папки');
                        }}
                    }});
                }});

                document.querySelectorAll('[data-role="view-file"]').forEach(link => {{
                    link.addEventListener('click', function (event) {{
                        event.preventDefault();
                        event.stopPropagation();

                        const openUrl = this.dataset.openUrl || '';
                        if (!openUrl) return;

                        try {{
                            const absoluteUrl = new URL(openUrl, window.location.href).href;
                            window.open(absoluteUrl, '_blank', 'noopener,noreferrer');
                        }} catch (e) {{
                            console.log('open file error:', e);
                            setSaveState('error', 'Ошибка открытия файла');
                        }}
                    }});
                }});
                document.querySelectorAll('[data-role="file-input"]').forEach(input => {{
                    input.addEventListener('change', async function() {{
                        const itemId = this.dataset.itemId;
                        const files = Array.from(this.files || []);

                        if (!files.length) {{
                            debugLog('upload_frontend_input_empty', {{
                                dialogId,
                                checklistKey: currentChecklistKey,
                                itemId
                            }});
                            return;
                        }}

                        const item = items.find(x => x.id === itemId);
                        const initialStatus = item ? normalizeStatus(item.status) : '';
                        let currentStatus = initialStatus;

                        debugLog('upload_frontend_input_selected', {{
                            dialogId,
                            checklistKey: currentChecklistKey,
                            itemId,
                            itemName: item ? String(item.name || '') : '',
                            filesCount: files.length,
                            files: files.map(file => ({{
                                name: String(file && file.name || ''),
                                size: Number(file && file.size || 0),
                                type: String(file && file.type || '')
                            }}))
                        }});

                        try {{
                            for (const file of files) {{
                                debugLog('upload_frontend_file_loop_started', {{
                                    dialogId,
                                    checklistKey: currentChecklistKey,
                                    itemId,
                                    itemName: item ? String(item.name || '') : '',
                                    fileName: String(file && file.name || ''),
                                    fileSize: Number(file && file.size || 0),
                                    fileType: String(file && file.type || '')
                                }});

                                const result = await uploadDocument(itemId, file);

                                debugLog('upload_frontend_file_loop_result', {{
                                    dialogId,
                                    checklistKey: currentChecklistKey,
                                    itemId,
                                    fileName: String(file && file.name || ''),
                                    fileSize: Number(file && file.size || 0),
                                    uploadJobId: result && result.uploadJobId || '',
                                    resultOk: !!(result && result.ok),
                                    hasItem: !!(result && result.item)
                                }});

                                replaceItem(result.item);

                                const uploadedDocs = getItemDocuments(result.item);
                                let uploadedDoc = uploadedDocs.find(x => String(x.name || '') === String(file.name || ''));

                                if (!uploadedDoc && uploadedDocs.length) {{
                                    uploadedDoc = uploadedDocs[uploadedDocs.length - 1];
                                }}

                                sessionChanges.push({{
                                    field: 'document',
                                    itemId: result.item ? result.item.id : itemId,
                                    itemName: result.item ? result.item.name : (item ? item.name : ''),
                                    oldValue: '',
                                    newValue: uploadedDoc ? (uploadedDoc.name || file.name || 'uploaded') : (file.name || 'uploaded')
                                }});
                                sessionDirty = true;

                                const newStatus = result.item ? normalizeStatus(result.item.status) : currentStatus;
                                if (newStatus !== currentStatus) {{
                                    sessionChanges.push({{
                                        field: 'status',
                                        itemId: result.item ? result.item.id : itemId,
                                        itemName: result.item ? result.item.name : (item ? item.name : ''),
                                        oldValue: currentStatus || '',
                                        newValue: newStatus || ''
                                    }});
                                    sessionDirty = true;
                                    currentStatus = newStatus;
                                }}

                                debugLog('upload_frontend_file_loop_completed', {{
                                    dialogId,
                                    checklistKey: currentChecklistKey,
                                    itemId,
                                    fileName: String(file && file.name || ''),
                                    fileSize: Number(file && file.size || 0),
                                    currentStatus
                                }});
                            }}

                            renderAll();

                            debugLog('upload_frontend_batch_completed', {{
                                dialogId,
                                checklistKey: currentChecklistKey,
                                itemId,
                                filesCount: files.length
                            }});

                        }} catch (e) {{
                            console.log(e);

                            debugLog('upload_frontend_batch_exception', {{
                                dialogId,
                                checklistKey: currentChecklistKey,
                                itemId,
                                filesCount: files.length,
                                error: String(e && e.message || e)
                            }});

                            setSaveState('error', 'Ошибка загрузки файлов');

                        }} finally {{
                            this.value = '';
                        }}
                    }});
                }});
            }}

            const baseLoadChecklistByKey = loadChecklistByKey;
            loadChecklistByKey = async function (checklistKey) {{
                const targetKey = String(checklistKey || '').trim() || 'id';
                if (targetKey !== 'opr') {{
                    return baseLoadChecklistByKey(targetKey);
                }}
                if (targetKey === currentChecklistKey) {{
                    return;
                }}

                await flushCurrentChecklistSummary();
                setSaveState('saving', 'Загружаем...');

                try {{
                    const cachedData = checklistCache[targetKey];
                    if (cachedData) {{
                        applyChecklistData(deepClone(cachedData));
                        renderAll();
                        debugLog('checklist_switched_cached', {{
                            checklistKey: targetKey
                        }});
                        setSaveState('', 'Сохранено');
                        return;
                    }}

                    const response = await fetch(
                        appUrl('api/checklist') +
                        '?dialogId=' + encodeURIComponent(dialogId) +
                        '&checklistKey=' + encodeURIComponent(targetKey)
                    );
                    const result = await response.json();

                    if (!response.ok) {{
                        throw new Error(result.error || 'load checklist failed');
                    }}

                    applyChecklistData(result);
                    renderAll();
                    debugLog('checklist_switched', {{
                        checklistKey: targetKey
                    }});
                    setSaveState('', 'Сохранено');
                }} catch (e) {{
                    console.log('loadChecklistByKey error:', e);
                    setSaveState('error', 'Ошибка загрузки чек-листа');
                }}
            }};
            {popup_session_enhancements_js}

            function safeInitBx24ForPopup() {{
                function applyPopupWindowSize() {{
                    try {{
                        if (typeof window.BX24.resizeWindow === 'function') {{
                            window.BX24.resizeWindow(1180, 720);
                        }}
                        if (typeof window.BX24.fitWindow === 'function') {{
                            window.BX24.fitWindow();
                        }}
                    }} catch (e) {{
                        console.log('BX24 popup sizing error:', e);
                    }}
                }}

                try {{
                    if (window.BX24 && typeof window.BX24.init === 'function') {{
                        window.BX24.init(function () {{
                            applyPopupWindowSize();
                            setTimeout(applyPopupWindowSize, 80);
                            setTimeout(applyPopupWindowSize, 220);
                        }});
                    }}
                }} catch (e) {{
                    console.log('BX24.init skipped:', e);
                }}
            }}
            try {{
                logRenderState('before_renderAll');
                renderAll();
                logRenderState('after_renderAll');

                debugLog('popup_loaded', {{
                    href: window.location.href,
                    hasDialogId: !!dialogId
                }});
            }} catch (e) {{
                logRenderError('renderAll', e);
            }}

            try {{
                fetchChatTitleIfMissing();
            }} catch (e) {{
                logRenderError('fetchChatTitleIfMissing', e);
            }}

            try {{
                fetchCurrentUserIfPossible();
            }} catch (e) {{
                logRenderError('fetchCurrentUserIfPossible', e);
            }}

            try {{
                safeInitBx24ForPopup();
            }} catch (e) {{
                logRenderError('safeInitBx24ForPopup', e);
            }}
        </script>
    </body>
    </html>
    """
