const popupClientSession = window.ChecklistPopupClientSession;
if (!popupClientSession || !popupClientSession.id) {
    throw new Error('ChecklistPopupClientSession is not initialized');
}
let clientSessionId = String(popupClientSession.id);

async function ensurePopupClientSessionReady() {
    if (
        popupClientSession
        && popupClientSession.ready
        && typeof popupClientSession.ready.then === 'function'
    ) {
        await popupClientSession.ready;
    }

    clientSessionId = String(
        popupClientSession && popupClientSession.id || ''
    ).trim();

    return clientSessionId;
}

function getActiveEditSessionId() {
    const api = window.ChecklistPopupEditSession;
    return String(
        api && typeof api.getSessionId === 'function'
            ? api.getSessionId()
            : ''
    ).trim();
}
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
let lockHeartbeatInFlight = false;
let sessionRecoveryPromise = null;
let suppressAutoCloseSave = false;
let popupFinalizationInProgress = false;
let popupUploadFinalizeWaitPromise = null;
let popupFinalizeRequestInProgress = false;

const POPUP_CLOSE_HANDOFF_PREFIX = 'checklist_popup_close_handoff_v1:';
const popupCloseToken = String(
    new URLSearchParams(window.location.search || '').get('closeToken') || ''
).trim();

function getPopupCloseHandoffKey() {
    return popupCloseToken
        ? POPUP_CLOSE_HANDOFF_PREFIX + popupCloseToken
        : '';
}

function registerPopupCloseHandoff(extra = {}) {
    const storageKey = getPopupCloseHandoffKey();
    const sessionId = getActiveEditSessionId();

    if (!storageKey || !sessionId) {
        return false;
    }

    const identity = getCurrentEditorIdentity();
    const handoff = {
        version: 1,
        closeToken: popupCloseToken,
        sessionId,
        dialogId: String(dialogId || '').trim(),
        checklistKey: String(currentChecklistKey || 'id').trim() || 'id',
        userId: String(identity.userId || '').trim(),
        userName: String(identity.userName || '').trim(),
        clientSessionId: String(clientSessionId || '').trim(),
        updatedAt: Date.now(),
        ...(
            extra && typeof extra === 'object'
                ? extra
                : {}
        )
    };

    try {
        localStorage.setItem(storageKey, JSON.stringify(handoff));
        return true;
    } catch (error) {
        debugLog('popup_close_handoff_write_failed', {
            closeToken: popupCloseToken,
            sessionId,
            error: String(error && error.message || error || '')
        });
        return false;
    }
}

function clearPopupCloseHandoff() {
    const storageKey = getPopupCloseHandoffKey();
    if (!storageKey) return;

    try {
        localStorage.removeItem(storageKey);
    } catch (error) {
        // The server finalization remains authoritative even if storage cleanup fails.
    }
}

window.ChecklistPopupCloseHandoff = Object.freeze({
    token: popupCloseToken,
    register: registerPopupCloseHandoff,
    clear: clearPopupCloseHandoff
});

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
    const meta = typeof getProjectChecklistMetaForKey === 'function'
        ? getProjectChecklistMetaForKey(currentChecklistKey)
        : getChecklistMeta(currentChecklistKey);

    const allowed = Array.isArray(meta && meta.allowCustomItemGroupIds)
        ? meta.allowCustomItemGroupIds.map(Number)
        : [];

    return allowed.includes(Number(groupId));
}

function isCurrentNotRequiredGroup(groupId) {
    const notRequiredGroupId = getCurrentNotRequiredGroupId();
    return !!notRequiredGroupId && Number(groupId) === notRequiredGroupId;
}

function resolveRequiredGroupForItem(item, previousGroupId = 0) {
    const meta = typeof getProjectChecklistMetaForKey === 'function'
        ? getProjectChecklistMetaForKey(currentChecklistKey)
        : getChecklistMeta(currentChecklistKey);

    const notRequiredGroupId = Number(meta && meta.notRequiredGroupId || 0);
    const defaultGroupId = Number(meta && meta.defaultGroupId || 0);
    const itemId = String(item && item.id || '');
    const key = String(currentChecklistKey || '').trim();
    const prefix = key ? key + '_g' : '';

    if (prefix && itemId.startsWith(prefix)) {
        const rest = itemId.slice(prefix.length);
        const rawGroupId = String(rest || '').split('_')[0];
        const groupId = Number(rawGroupId);

        if (groupId && groupId !== notRequiredGroupId) {
            return groupId;
        }
    }

    if (previousGroupId && previousGroupId !== notRequiredGroupId) {
        return previousGroupId;
    }

    const itemName = String(item && item.name || '').trim();

    const matchedGroup = (Array.isArray(groups) ? groups : []).find(group => {
        const groupId = Number(group && group.id || 0);

        if (!groupId || groupId === notRequiredGroupId) {
            return false;
        }

        return (Array.isArray(items) ? items : []).some(existing =>
            existing !== item &&
            Number(existing && existing.group || 0) === groupId &&
            String(existing && existing.name || '').trim() === itemName
        );
    });

    return matchedGroup
        ? Number(matchedGroup.id || 0)
        : defaultGroupId || previousGroupId || 1;
}

function applyGenericStatusGroupMove(item, newStatus, oldItem = null) {
    const notRequiredGroupId = getCurrentNotRequiredGroupId();

    if (!item || !notRequiredGroupId) {
        return;
    }

    if (newStatus === 'Не требуется') {
        item.group = notRequiredGroupId;
        return;
    }

    if (Number(item.group || 0) === Number(notRequiredGroupId)) {
        item.group = resolveRequiredGroupForItem(
            item,
            Number(oldItem && oldItem.group || 0)
        );
    }
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
    return !!(
        currentChecklistLock.lockedByOther
        && currentChecklistLock.checklistKey === currentChecklistKey
    );
}

function getEditSessionRuntimeState() {
    const api = window.ChecklistPopupEditSession;
    const sessionId = String(
        api && typeof api.getSessionId === 'function'
            ? api.getSessionId()
            : ''
    ).trim();
    const status = String(
        api && typeof api.getStatus === 'function'
            ? api.getStatus()
            : 'idle'
    ).trim() || 'idle';
    const active = (
        api
        && typeof api.isActive === 'function'
            ? api.isActive()
            : (
                !!sessionId
                && status === 'active'
            )
    );
    const error = String(
        api && typeof api.getLastError === 'function'
            ? api.getLastError()
            : ''
    ).trim();

    return {
        available: !!api,
        sessionId,
        status,
        active: !!active,
        error
    };
}

function ownsCurrentChecklistLock() {
    return !!(
        currentChecklistLock.owned
        && !currentChecklistLock.lockedByOther
        && currentChecklistLock.checklistKey === currentChecklistKey
        && currentChecklistLock.lockId
    );
}

function isEditingAllowed() {
    const sessionState = getEditSessionRuntimeState();

    return !!(
        !popupFinalizationInProgress
        && sessionState.active
        && ownsCurrentChecklistLock()
    );
}

function getEditingBlockedReason() {
    if (popupFinalizationInProgress) {
        return 'Сессия уже завершается';
    }

    const sessionState = getEditSessionRuntimeState();

    if (!sessionState.available) {
        return 'Модуль сессии редактирования не загружен';
    }

    if (sessionState.status === 'failed') {
        return (
            sessionState.error
            || 'Не удалось запустить сессию редактирования'
        );
    }

    if (!sessionState.active) {
        return 'Сессия редактирования ещё не готова';
    }

    if (isChecklistLockedByOther()) {
        const ownerName = (
            currentChecklistLock.userName
            || 'другой сотрудник'
        );
        return (
            'С этим чек-листом сейчас работает '
            + ownerName
        );
    }

    if (!ownsCurrentChecklistLock()) {
        return 'Не получена блокировка текущего чек-листа';
    }

    return '';
}

async function requireEditingSession(
    actionName = 'изменение',
    options = {}
) {
    const requireCurrentLock = (
        options.requireCurrentLock !== false
    );

    if (popupFinalizationInProgress) {
        throw new Error('Сессия уже завершается');
    }

    let sessionState = getEditSessionRuntimeState();

    if (!sessionState.active) {
        const sessionEvents = (
            window.ChecklistPopupSessionEvents
        );

        if (
            sessionEvents
            && typeof sessionEvents.ensureStarted === 'function'
        ) {
            await sessionEvents.ensureStarted();
        }

        sessionState = getEditSessionRuntimeState();
    }

    if (!sessionState.active || !sessionState.sessionId) {
        const reason = getEditingBlockedReason();
        throw new Error(
            reason
            || (
                'Нельзя выполнить '
                + actionName
                + ': сессия редактирования не готова'
            )
        );
    }

    if (requireCurrentLock && !ownsCurrentChecklistLock()) {
        await acquireChecklistLock(
            currentChecklistKey,
            false
        );
    }

    if (
        requireCurrentLock
        && !ownsCurrentChecklistLock()
    ) {
        const reason = getEditingBlockedReason();
        throw new Error(
            reason
            || (
                'Нельзя выполнить '
                + actionName
                + ': чек-лист открыт только для просмотра'
            )
        );
    }

    return sessionState.sessionId;
}

function disabledAttr() {
    return isEditingAllowed() ? '' : 'disabled';
}

function setActionButtonState() {
    const editingAllowed = isEditingAllowed();

    if (saveCloseBtn) {
        saveCloseBtn.disabled = !editingAllowed;
        saveCloseBtn.style.opacity = saveCloseBtn.disabled ? '0.55' : '1';
        saveCloseBtn.style.cursor = saveCloseBtn.disabled ? 'not-allowed' : 'pointer';
    }
    if (cancelBtn) {
        cancelBtn.disabled = popupFinalizationInProgress;
    }

    document.querySelectorAll('[data-role="view-folder"]').forEach(button => {
        button.disabled = !editingAllowed;
        button.setAttribute(
            'aria-disabled',
            button.disabled ? 'true' : 'false'
        );
    });

    document.querySelectorAll(
        '[data-role="item-drag-handle"],'
        + '[data-role="cycle-item-status"]'
    ).forEach(handle => {
        handle.disabled = !editingAllowed;
        handle.setAttribute(
            'aria-disabled',
            handle.disabled ? 'true' : 'false'
        );
    });

    if (!editingAllowed) {
        const mutationSelector = [
            '[data-role="status"]',
            '[data-role="plan"]',
            '[data-role="fact"]',
            '[data-role="add-item"]',
            '[data-role="opr-status"]',
            '[data-role="opr-plan"]',
            '[data-role="opr-fact"]',
            '[data-role="opr-add-item"]',
            '[data-role="upload"]',
            '[data-role="file-input"]',
            '[data-role="replace-document"]',
            '[data-role="remove-document"]',
            '[data-role="item-drag-handle"]',
            '[data-role="cycle-item-status"]'
        ].join(',');

        document.querySelectorAll(
            mutationSelector
        ).forEach(element => {
            element.disabled = true;
            element.setAttribute(
                'aria-disabled',
                'true'
            );
        });
    }
}

function updateSaveStateBySession() {
    const sessionState = getEditSessionRuntimeState();

    if (popupFinalizationInProgress) {
        return;
    }

    if (sessionState.status === 'failed') {
        setSaveState(
            'error',
            'Ошибка запуска сессии'
        );
        return;
    }

    if (!sessionState.active) {
        setSaveState(
            'saving',
            'Подключаем сессию...'
        );
        return;
    }

    if (!ownsCurrentChecklistLock()) {
        setSaveState(
            isChecklistLockedByOther()
                ? 'error'
                : 'saving',
            isChecklistLockedByOther()
                ? 'Только просмотр'
                : 'Получаем доступ...'
        );
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
    const sessionState = getEditSessionRuntimeState();

    if (lockNoticeEl) {
        if (popupFinalizationInProgress) {
            lockNoticeEl.textContent = 'Завершаем сессию...';
            lockNoticeEl.style.display = '';
        } else if (sessionState.status === 'failed') {
            lockNoticeEl.textContent = (
                'Сессия редактирования не запущена'
                + (
                    sessionState.error
                        ? ': ' + sessionState.error.slice(0, 120)
                        : ''
                )
            );
            lockNoticeEl.style.display = '';
        } else if (!sessionState.active) {
            lockNoticeEl.textContent = 'Подключаем защищённую сессию...';
            lockNoticeEl.style.display = '';
        } else if (isChecklistLockedByOther()) {
            const ownerName = currentChecklistLock.userName || 'другой сотрудник';
            lockNoticeEl.textContent = 'Сейчас с этим чек-листом работает ' + ownerName + ', дождитесь завершения сессии';
            lockNoticeEl.style.display = '';
        } else if (!ownsCurrentChecklistLock()) {
            lockNoticeEl.textContent = 'Получаем доступ к чек-листу...';
            lockNoticeEl.style.display = '';
        } else {
            lockNoticeEl.textContent = '';
            lockNoticeEl.style.display = 'none';
        }
    }

    setActionButtonState();
    updateSaveStateBySession();
}

window.addEventListener(
    'checklist-edit-session-state',
    function (event) {
        updateLockNotice();

        const detail = event && event.detail || {};
        if (
            String(detail.status || '').trim() === 'active'
            && String(detail.sessionId || '').trim()
        ) {
            registerPopupCloseHandoff({
                source: 'edit_session_state'
            });
        }
    }
);

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

function buildSessionFinalizePayload(closeEvent) {
    const identity = getCurrentEditorIdentity();
    return {
        ...buildCloseBatchPayload(closeEvent),
        sessionId: getActiveEditSessionId(),
        userId: identity.userId,
        clientSessionId: String(clientSessionId || '').trim(),
        reason: String(closeEvent || 'save_and_close')
    };
}

function buildCompactSessionFinalizePayload(closeEvent) {
    const identity = getCurrentEditorIdentity();
    return {
        sessionId: getActiveEditSessionId(),
        dialogId,
        userId: identity.userId,
        userName: identity.userName,
        clientSessionId: String(clientSessionId || '').trim(),
        editor: {
            id: identity.userId,
            name: identity.userName
        },
        sessions: [],
        closeEvent: String(closeEvent || 'popup_unload_autosave'),
        reason: String(closeEvent || 'popup_unload_autosave')
    };
}

async function finalizeDirtyChecklists(
    closeEvent,
    useBeacon = false
) {
    const editSessionApi = window.ChecklistPopupEditSession;
    if (!editSessionApi) {
        throw new Error('Сессия редактирования не инициализирована');
    }

    const dirtyKeys = getDirtyChecklistKeys();
    const payload = useBeacon
        ? buildCompactSessionFinalizePayload(closeEvent)
        : buildSessionFinalizePayload(closeEvent);

    if (useBeacon) {
        const queued = !!(
            typeof editSessionApi.finalizeOnUnload === 'function'
            && editSessionApi.finalizeOnUnload(
                payload,
                closeEvent
            )
        );
        return {
            queued,
            savedCount: dirtyKeys.length,
            messageOk: true,
            messageSkipped: !dirtyKeys.length,
            result: {}
        };
    }

    await ensureCurrentEditorReady();

    if (typeof editSessionApi.finalize !== 'function') {
        throw new Error('Единое завершение edit-session недоступно');
    }

    const result = await editSessionApi.finalize(
        payload,
        closeEvent
    );

    dirtyKeys.forEach(clearChecklistSessionState);
    return {
        savedCount: Number(
            result.savedCount
            || dirtyKeys.length
            || 0
        ),
        messageOk: result.messageOk !== false,
        messageSkipped: !!result.messageSkipped,
        result
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

    const requestBody = JSON.stringify(payload);
    const requestBodyBytes = (
        typeof TextEncoder === 'function'
    )
        ? new TextEncoder().encode(requestBody).length
        : requestBody.length;

    debugLog('close_session_request_started', {
        closeEvent,
        dialogId,
        checklistKeys: dirtyKeys,
        payloadCharacters: requestBody.length,
        payloadBytes: requestBodyBytes,
        transport: 'standard_fetch'
    });

    let response;
    let responseText = '';
    let result = {};

    try {
        response = await fetch(appUrl('api/checklist/close-session'), {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: requestBody
        });

        responseText = await response.text();

        try {
            result = responseText ? JSON.parse(responseText) : {};
        } catch (parseError) {
            result = {};
        }
    } catch (requestError) {
        debugLog('close_session_request_failed', {
            closeEvent,
            dialogId,
            checklistKeys: dirtyKeys,
            payloadBytes: requestBodyBytes,
            transport: 'standard_fetch',
            error: String(
                requestError
                && requestError.message
                || requestError
            )
        });
        throw requestError;
    }

    if (!response.ok || result.ok === false) {
        const responseError = String(
            result.error
            || result.messageError
            || responseText
            || ('HTTP ' + response.status)
        ).trim();

        debugLog('close_session_response_failed', {
            closeEvent,
            dialogId,
            checklistKeys: dirtyKeys,
            status: response.status,
            responseText: responseText.slice(0, 1000)
        });

        throw new Error(
            responseError
            || 'close-session failed'
        );
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
            : '',
        sessionId: getActiveEditSessionId(),
        clientSessionId
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
                lockId: currentChecklistLock.lockId,
                sessionId: getActiveEditSessionId(),
                clientSessionId
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

function syncCurrentLockFromSessionHeartbeat(result) {
    const locks = (
        result
        && result.session
        && Array.isArray(result.session.locks)
            ? result.session.locks
            : []
    );
    const targetKey = String(currentChecklistKey || '').trim();
    const matching = locks.find(lock => (
        String(lock && lock.checklistKey || '').trim() === targetKey
        && String(lock && lock.status || '').trim() === 'locked'
        && String(lock && lock.lockId || '').trim()
    ));

    if (!matching) {
        if (
            currentChecklistLock.checklistKey === targetKey
            && currentChecklistLock.owned
        ) {
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
        return false;
    }

    currentChecklistLock = {
        owned: true,
        lockedByOther: false,
        lockId: String(matching.lockId || ''),
        userId: String(matching.userId || ''),
        userName: String(matching.userName || ''),
        checklistKey: targetKey
    };
    updateLockNotice();
    return true;
}

async function recoverPopupSessionAndCurrentLock() {
    if (popupFinalizationInProgress) {
        return null;
    }

    if (sessionRecoveryPromise) {
        return sessionRecoveryPromise;
    }

    const sessionEvents = window.ChecklistPopupSessionEvents;
    if (
        !sessionEvents
        || typeof sessionEvents.ensureStarted !== 'function'
    ) {
        return null;
    }

    currentChecklistLock = {
        owned: false,
        lockedByOther: false,
        lockId: '',
        userId: '',
        userName: '',
        checklistKey: currentChecklistKey
    };
    updateLockNotice();

    sessionRecoveryPromise = sessionEvents.ensureStarted()
        .then(function () {
            if (!ownsCurrentChecklistLock()) {
                return acquireChecklistLock(
                    currentChecklistKey,
                    true
                );
            }
            return currentChecklistLock;
        })
        .then(function (result) {
            if (ownsCurrentChecklistLock()) {
                renderAll();
            }
            return result;
        })
        .catch(function (error) {
            console.log(
                'edit session reconnect error:',
                error
            );
            updateLockNotice();
            return null;
        })
        .finally(function () {
            sessionRecoveryPromise = null;
        });

    return sessionRecoveryPromise;
}

function startLockHeartbeat() {
    stopLockHeartbeat();
    lockHeartbeatTimer = setInterval(async function () {
        if (
            !currentEditorReady
            || popupFinalizationInProgress
            || lockHeartbeatInFlight
        ) {
            return;
        }

        const editSessionApi = window.ChecklistPopupEditSession;
        if (
            !editSessionApi
            || typeof editSessionApi.heartbeat !== 'function'
        ) {
            return;
        }

        lockHeartbeatInFlight = true;

        try {
            // /session/heartbeat already renews every lock owned by the
            // edit-session on the backend. A second /lock/heartbeat request
            // is not required.
            const heartbeatResult = await editSessionApi.heartbeat(true);

            if (!heartbeatResult) {
                await recoverPopupSessionAndCurrentLock();
                return;
            }

            const hasCurrentLock = syncCurrentLockFromSessionHeartbeat(
                heartbeatResult
            );

            if (!hasCurrentLock) {
                await acquireChecklistLock(
                    currentChecklistKey,
                    true
                );
            }
        } finally {
            lockHeartbeatInFlight = false;
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
        userId: editorIdentity.userId,
        sessionId: getActiveEditSessionId(),
        clientSessionId
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

function isEmbeddedBitrixPopup() {
    try {
        return window.self !== window.top;
    } catch (error) {
        // Cross-origin access means the page is embedded.
        return true;
    }
}

function closePopupWindow() {
    const embeddedInBitrix = isEmbeddedBitrixPopup();

    if (
        embeddedInBitrix
        && window.BX24
        && typeof window.BX24.closeApplication === 'function'
    ) {
        try {
            window.BX24.closeApplication();
            return;
        } catch (error) {
            console.log('BX24.closeApplication error:', error);
        }
    }

    const hasUsableOpener = Boolean(
        window.opener
        && !window.opener.closed
    );

    if (hasUsableOpener) {
        try {
            if (typeof window.opener.focus === 'function') {
                window.opener.focus();
            }
            window.close();
        } catch (error) {
            console.log('window.close error:', error);
        }
    }

    window.setTimeout(function () {
        if (!window.closed) {
            window.location.reload();
        }
    }, hasUsableOpener ? 180 : 60);
}

function clearLocalPopupSessionState() {
    checklistSessionState = {};
    sessionChanges = [];
    sessionDirty = false;
    checklistCache = {};
}

async function waitForPopupUploadsBeforeFinalize(
    saveChanges
) {
    const manager = window.popupUploadManager;

    if (
        !manager
        || typeof manager.hasPending !== 'function'
        || !manager.hasPending()
    ) {
        return null;
    }

    if (!saveChanges) {
        if (typeof manager.cancelAll === 'function') {
            manager.cancelAll(
                'Отменено пользователем'
            );
        } else if (
            typeof manager.cancelAllQueued === 'function'
        ) {
            manager.cancelAllQueued(
                'Отменено пользователем'
            );
        }
        setSaveState(
            'saving',
            'Завершаем активные загрузки перед отменой...'
        );
    } else {
        setSaveState(
            'saving',
            'Ожидаем завершения загрузки файлов...'
        );
    }

    if (!popupUploadFinalizeWaitPromise) {
        popupUploadFinalizeWaitPromise = (
            typeof manager.waitForIdle === 'function'
                ? manager.waitForIdle()
                : Promise.resolve(null)
        ).finally(function () {
            popupUploadFinalizeWaitPromise = null;
        });
    }

    return popupUploadFinalizeWaitPromise;
}

function popupHasPendingUploads() {
    const manager = window.popupUploadManager;

    return Boolean(
        manager
        && typeof manager.hasPending === 'function'
        && manager.hasPending()
    );
}

async function finalizePopupSession(saveChanges) {
    if (
        popupFinalizationInProgress
        || popupFinalizeRequestInProgress
    ) {
        return false;
    }

    popupFinalizeRequestInProgress = true;

    if (saveCloseBtn) saveCloseBtn.disabled = true;
    if (cancelBtn) cancelBtn.disabled = true;

    try {
        if (typeof settleInlineItemRename === 'function') {
            await settleInlineItemRename({
                discard: !saveChanges,
                reason: saveChanges ? 'save_and_close' : 'cancel_button'
            });
        }
    } catch (renameError) {
        popupFinalizeRequestInProgress = false;
        if (saveCloseBtn) saveCloseBtn.disabled = false;
        if (cancelBtn) cancelBtn.disabled = false;
        setSaveState(
            'error',
            renameError && renameError.message
                ? renameError.message
                : 'Не удалось сохранить название пункта'
        );
        return false;
    }

    try {
        await waitForPopupUploadsBeforeFinalize(
            !!saveChanges
        );
    } catch (uploadWaitError) {
        popupFinalizeRequestInProgress = false;
        if (saveCloseBtn) saveCloseBtn.disabled = false;
        if (cancelBtn) cancelBtn.disabled = false;
        setSaveState(
            'error',
            'Не удалось завершить очередь загрузки'
        );
        return false;
    }

    popupFinalizeRequestInProgress = false;
    suppressAutoCloseSave = true;
    closeSummarySent = true;
    popupFinalizationInProgress = true;
    syncChecklistCache();
    updateLockNotice();

    let persistResult = {
        savedCount: 0,
        messageOk: true,
        messageSkipped: true,
        result: {}
    };

    debugLog('finalize_popup_session_started', {
        saveChanges: !!saveChanges,
        dialogId,
        checklistKey: currentChecklistKey,
        editSessionId: getActiveEditSessionId()
    });

    try {
        const sessionEvents = (
            window.ChecklistPopupSessionEvents
        );
        if (
            sessionEvents
            && typeof sessionEvents.ensureStarted === 'function'
        ) {
            await sessionEvents.ensureStarted();
        }

        if (saveChanges) {
            persistResult = await finalizeDirtyChecklists(
                'save_and_close',
                false
            );
        } else {
            const editSessionApi = window.ChecklistPopupEditSession;
            if (
                !editSessionApi
                || typeof editSessionApi.rollback !== 'function'
            ) {
                throw new Error('Сессия редактирования не инициализирована');
            }

            await editSessionApi.rollback('cancel_button', true);
            clearLocalPopupSessionState();
        }
    } catch (e) {
        console.log('finalizePopupSession error:', e);

        const errorText = String(
            e && e.message || e || ''
        ).trim();

        debugLog('finalize_popup_session_failed', {
            saveChanges: !!saveChanges,
            dialogId,
            checklistKey: currentChecklistKey,
            editSessionId: getActiveEditSessionId(),
            error: errorText
        });

        const stateText = saveChanges
            ? 'Ошибка сохранения'
            : 'Ошибка отмены';

        setSaveState(
            'error',
            errorText
                ? stateText + ': ' + errorText.slice(0, 140)
                : stateText
        );
        popupFinalizationInProgress = false;
        popupFinalizeRequestInProgress = false;
        suppressAutoCloseSave = false;
        closeSummarySent = false;
        if (saveCloseBtn) saveCloseBtn.disabled = false;
        if (cancelBtn) cancelBtn.disabled = false;
        updateLockNotice();
        return false;
    }

    if (saveChanges) {
        if (persistResult.messageOk === false) {
            setSaveState('saving', 'Сохранено, сообщение не отправлено');
        } else if (persistResult.savedCount > 0) {
            setSaveState('', 'Сохранено');
        }
    } else {
        setSaveState('', 'Изменения отменены');
    }

    debugLog('finalize_popup_session_completed', {
        saveChanges: !!saveChanges,
        dialogId,
        checklistKey: currentChecklistKey,
        editSessionId: getActiveEditSessionId()
    });

    stopLockHeartbeat();

    currentChecklistLock = {
        owned: false,
        lockedByOther: false,
        lockId: '',
        userId: '',
        userName: '',
        checklistKey: currentChecklistKey
    };
    updateLockNotice();

    // The launcher callback is reserved for the Bitrix host cross. A close
    // initiated after a successful Save/Cancel must not finalize a second time.
    clearPopupCloseHandoff();

    // Stage 7.1.1.1: both successful save and rollback use the safe local
    // fallback. Bitrix closes through BX24.closeApplication(); a direct local
    // tab reloads instead of being redirected to a blank page.
    closePopupWindow();
    return true;
}

function sendCloseSummaryOnce(eventName) {
    registerPopupCloseHandoff({
        source: String(eventName || 'popup_lifecycle')
    });

    // A tab/iframe can become hidden without being closed. The reliable Bitrix
    // host-cross signal is handled by the launcher close callback; pagehide and
    // beforeunload stay as compact-beacon fallbacks.
    if (eventName === 'popup_hidden' || suppressAutoCloseSave || closeSummarySent) {
        return;
    }

    if (getActiveEditSessionId()) {
        const finalizeResult = finalizeDirtyChecklists(
            eventName,
            true
        );

        Promise.resolve(finalizeResult).then(function (result) {
            const queued = !!(result && result.queued);
            if (queued) {
                closeSummarySent = true;
            }
            debugLog('popup_unload_finalize_requested', {
                eventName,
                sessionId: getActiveEditSessionId(),
                dialogId,
                checklistKey: currentChecklistKey,
                beaconQueued: queued
            });
        }).catch(function (error) {
            closeSummarySent = false;
            debugLog('popup_unload_finalize_failed', {
                eventName,
                sessionId: getActiveEditSessionId(),
                error: String(error && error.message || error || '')
            });
        });
        return;
    }

    closeSummarySent = true;
    persistDirtyChecklists(eventName, true);
    releaseChecklistLock(currentChecklistKey, true);
}
