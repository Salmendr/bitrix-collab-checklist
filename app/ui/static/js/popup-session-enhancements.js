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
// The session the last pagehide/beforeunload finalized by a beacon.
let unloadFinalizedSessionId = '';
let popupUploadFinalizeWaitPromise = null;
let popupFinalizeRequestInProgress = false;

// The window is gone for its host (the slider of Bitrix24 was closed and
// only hidden, see popup-frame-visibility.js): the session is finalized and
// nothing starts a new one until the window is shown and reloaded.
let popupClosedByHost = false;

function isPopupClosedByHost() {
    return popupClosedByHost;
}

// The window is closed for good (by its host, by the server after ten
// minutes without actions or five minutes without a heartbeat): no
// heartbeat, no lock checks and no new session from it any more.
function markPopupWindowClosed(reason) {
    if (!popupClosedByHost) {
        debugLog('popup_window_closed_state', {
            dialogId,
            checklistKey: currentChecklistKey,
            sessionId: getActiveEditSessionId(),
            reason: String(reason || '')
        });
    }
    popupClosedByHost = true;
    try {
        stopLockHeartbeat();
    } catch (error) {
        console.log('stop heartbeat error:', error);
    }
}

// What the window had seen when it read each checklist it shows (a marker of
// the server): a window that only views a checklist holds no lock, so
// another window may save it meanwhile; the server says whether it did (the
// sessions of this window do not count).
const checklistReadMarker = {};
let lockStatusInFlight = false;
const pendingViewRefresh = {};

function rememberChecklistReadMarker(checklistKey, readMarker) {
    const key = String(checklistKey || '').trim();
    const value = String(readMarker || '').trim();
    if (key && value) {
        checklistReadMarker[key] = value;
    }
}

(function rememberBootstrapReadMarker() {
    const bootstrap = window.CHECKLIST_POPUP_BOOTSTRAP || {};
    rememberChecklistReadMarker(bootstrap.checklistKey, bootstrap.checklistReadMarker);
})();

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
    // The bar of the buttons is fixed to the bottom of the window and is
    // transparent: the checklist shows around it and stays clickable. Only
    // the white strip under the buttons hides the text, with no gaps.
    const footerNode = document.createElement('div');
    footerNode.id = 'footerActions';
    footerNode.className = 'popup-footer-actions';
    footerNode.innerHTML = `
        <div class="popup-footer-buttons">
            <button id="saveCloseBtn" type="button" class="popup-footer-btn is-save-close">
                Сохранить и закрыть
            </button>
            <button id="saveContinueBtn" type="button" class="popup-footer-btn is-save" title="Сохранить изменения и продолжить работу: чек-лист остаётся за вами">
                Сохранить
            </button>
            <button id="cancelBtn" type="button" class="popup-footer-btn is-cancel">
                Отмена
            </button>
        </div>
    `;
    contentEl.appendChild(footerNode);
    contentEl.classList.add('has-fixed-footer');
}
const saveCloseBtn = document.getElementById('saveCloseBtn');
const saveContinueBtn = document.getElementById('saveContinueBtn');
const cancelBtn = document.getElementById('cancelBtn');

// A window of the platform portal is open for viewing only: no edit session
// and no lock; the server refuses changes from it anyway.
function isReadOnlyWindow() {
    return !!(window.ChecklistAuth && window.ChecklistAuth.readOnly);
}

if (isReadOnlyWindow()) {
    if (saveCloseBtn) saveCloseBtn.style.display = 'none';
    if (saveContinueBtn) saveContinueBtn.style.display = 'none';
    if (cancelBtn) {
        cancelBtn.textContent = 'Закрыть';
        cancelBtn.style.background = '#475467';
    }
}

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

// The lock of a checklist is taken by its first change, not by opening it
// or its tab: viewing holds nothing and does not keep others from editing.
// The window asks who holds the lock (api/checklist/lock/status) to show it.
// Граданализ has its own lock, taken by its first change too (api/ga).
function defersCurrentChecklistLock() {
    return !isReadOnlyWindow();
}

// The window can save Граданализ while another employee holds OPR.
function gradAnalysisEditable() {
    return !!(window.ChecklistGa && window.ChecklistGa.canEditNow());
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
        && (
            ownsCurrentChecklistLock()
            || (
                defersCurrentChecklistLock()
                && !isChecklistLockedByOther()
            )
        )
    );
}

function getEditingBlockedReason() {
    if (isReadOnlyWindow()) {
        return 'Чек-лист открыт в режиме просмотра';
    }

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

    if (!ownsCurrentChecklistLock() && !defersCurrentChecklistLock()) {
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

    if (isReadOnlyWindow()) {
        throw new Error('Чек-лист открыт в режиме просмотра');
    }

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
        // The first change takes the lock.
        const lockResult = await acquireChecklistLock(
            currentChecklistKey,
            false
        );
        if (ownsCurrentChecklistLock()) {
            await ensureCurrentChecklistIsFresh(lockResult, actionName);
        }
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
        saveCloseBtn.disabled = !(editingAllowed || gradAnalysisEditable());
        saveCloseBtn.style.opacity = saveCloseBtn.disabled ? '0.55' : '1';
        saveCloseBtn.style.cursor = saveCloseBtn.disabled ? 'not-allowed' : 'pointer';
    }
    if (saveContinueBtn) {
        saveContinueBtn.disabled = !!(
            popupFinalizeRequestInProgress
            || !(editingAllowed || gradAnalysisEditable())
        );
        saveContinueBtn.style.opacity = saveContinueBtn.disabled ? '0.55' : '1';
        saveContinueBtn.style.cursor = saveContinueBtn.disabled ? 'not-allowed' : 'pointer';
    }
    if (cancelBtn) {
        // While «Отмена» (or a save) runs the button stays unavailable.
        cancelBtn.disabled = !!(
            popupFinalizationInProgress
            || popupFinalizeRequestInProgress
        );
    }

    // Folders open in the view mode too (read only, see view-folder).
    document.querySelectorAll('[data-role="view-folder"]').forEach(button => {
        button.disabled = !!popupFinalizationInProgress;
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

    if (isReadOnlyWindow()) {
        setSaveState('error', 'Только просмотр');
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

    if (
        isChecklistLockedByOther()
        && !ownsCurrentChecklistLock()
        && !gradAnalysisEditable()
    ) {
        setSaveState('error', 'Только просмотр');
        return;
    }

    if (
        !ownsCurrentChecklistLock()
        && !defersCurrentChecklistLock()
        && !gradAnalysisEditable()
    ) {
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

    if (
        sessionDirty
        || (window.ChecklistGa && window.ChecklistGa.hasDraft())
    ) {
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

// The checklist is drawn with its controls on or off. The lock is taken by
// the first change, so no lock change redraws it when the session gets ready
// or another employee takes or gives back the checklist: it is redrawn here.
let renderedEditingAllowed = false;
let editingRerenderScheduled = false;

function noteRenderedEditingState() {
    renderedEditingAllowed = !isReadOnlyWindow() && isEditingAllowed();
}

function scheduleEditingRerender() {
    if (editingRerenderScheduled || isReadOnlyWindow()) {
        return;
    }
    editingRerenderScheduled = true;
    setTimeout(function () {
        editingRerenderScheduled = false;
        if (
            popupClosedByHost
            || typeof renderAll !== 'function'
            || isEditingAllowed() === renderedEditingAllowed
        ) {
            return;
        }
        try {
            renderAll();
        } catch (error) {
            console.log('editing state rerender error:', error);
        }
    }, 0);
}

function updateLockNotice() {
    const sessionState = getEditSessionRuntimeState();

    if (lockNoticeEl) {
        if (isReadOnlyWindow()) {
            lockNoticeEl.textContent = 'Режим просмотра';
            lockNoticeEl.style.display = '';
        } else if (popupFinalizationInProgress) {
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
        } else if (!ownsCurrentChecklistLock() && !defersCurrentChecklistLock()) {
            lockNoticeEl.textContent = 'Получаем доступ к чек-листу...';
            lockNoticeEl.style.display = '';
        } else {
            lockNoticeEl.textContent = '';
            lockNoticeEl.style.display = 'none';
        }
    }

    setActionButtonState();
    updateSaveStateBySession();
    scheduleEditingRerender();
}

window.addEventListener(
    'checklist-edit-session-state',
    function (event) {
        updateLockNotice();

        const detail = event && event.detail || {};
        const activeSessionId = String(detail.sessionId || '').trim();
        if (
            String(detail.status || '').trim() === 'active'
            && activeSessionId
            && closeSummarySent
            && !suppressAutoCloseSave
            && !popupFinalizationInProgress
            && activeSessionId !== unloadFinalizedSessionId
        ) {
            // beforeunload came but the window stayed (a download, a slow
            // or cancelled navigation, a late close of the slider), and the
            // heartbeat started a new session with the lock. The close of
            // the window must finish this session too, or its lock stays
            // and the same user finds the checklist locked by himself.
            closeSummarySent = false;
            debugLog('popup_unload_finalize_rearmed', {
                sessionId: activeSessionId,
                finalizedSessionId: unloadFinalizedSessionId
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
    if (isReadOnlyWindow()) {
        return null;
    }
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
        clientSessionId,
        // The server says whether another window saved it after this.
        since: checklistReadMarker[targetKey] || ''
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
    if (popupFinalizationInProgress || popupClosedByHost) {
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
            if (
                !ownsCurrentChecklistLock()
                && !defersCurrentChecklistLock()
            ) {
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

// Read the current checklist again for a window that does not hold its
// lock: another employee saved or cancelled changes meanwhile.
async function refreshCurrentChecklistView(reason) {
    const targetKey = currentChecklistKey;
    if (
        !targetKey
        || targetKey === 'ga'
        || ownsCurrentChecklistLock()
        || sessionDirty
        || popupFinalizationInProgress
        || popupFinalizeRequestInProgress
        || popupClosedByHost
    ) {
        return false;
    }
    const response = await fetch(
        appUrl('api/checklist')
        + '?dialogId=' + encodeURIComponent(dialogId)
        + '&checklistKey=' + encodeURIComponent(targetKey),
        { cache: 'no-store' }
    );
    const result = await response.json();
    if (!response.ok) {
        throw new Error(result.error || 'reload checklist failed');
    }
    if (
        targetKey !== currentChecklistKey
        || ownsCurrentChecklistLock()
        || sessionDirty
    ) {
        return false;
    }
    applyChecklistData(result);
    rememberChecklistReadMarker(
        targetKey,
        response.headers.get('X-Checklist-Read-Marker')
    );
    syncChecklistCache();
    renderAll();
    pendingViewRefresh[targetKey] = false;
    debugLog('checklist_view_refreshed', {
        dialogId,
        checklistKey: targetKey,
        reason: String(reason || ''),
        readMarker: checklistReadMarker[targetKey] || ''
    });
    return true;
}

// The user edits something in an input, a menu or a window: the view is
// read again on the next check.
function popupViewIsBusy() {
    const active = document.activeElement;
    if (
        active
        && active !== document.body
        && /^(INPUT|TEXTAREA|SELECT)$/.test(String(active.tagName || ''))
    ) {
        return true;
    }
    if (popupHasPendingUploads()) {
        return true;
    }
    return !!document.querySelector(
        '.item-status-menu, .checklist-link-menu, .project-object-menu,'
        + ' .ga-status-menu, .ga-modal-backdrop, .notification-draft-backdrop,'
        + ' .oa-popover, .public-share-overlay'
    );
}

// Who holds the lock of the current checklist, without taking it.
async function refreshCurrentChecklistLockStatus(reason = 'heartbeat') {
    if (
        isReadOnlyWindow()
        || popupClosedByHost
        || popupFinalizationInProgress
        || lockStatusInFlight
    ) {
        return null;
    }
    const targetKey = String(currentChecklistKey || '').trim();
    if (!targetKey || targetKey === 'ga') {
        return null;
    }
    lockStatusInFlight = true;
    try {
        const response = await fetch(
            appUrl('api/checklist/lock/status')
            + '?dialogId=' + encodeURIComponent(dialogId)
            + '&checklistKey=' + encodeURIComponent(targetKey)
            + '&sessionId=' + encodeURIComponent(getActiveEditSessionId())
            + '&since=' + encodeURIComponent(checklistReadMarker[targetKey] || ''),
            { cache: 'no-store' }
        );
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result || result.ok === false) {
            return null;
        }
        if (targetKey !== currentChecklistKey) {
            return null;
        }

        const prevOwned = ownsCurrentChecklistLock();
        const prevBlocked = isChecklistLockedByOther();
        const prevOwner = String(currentChecklistLock.userName || '');
        const owned = !!result.owned;
        currentChecklistLock = {
            owned,
            lockedByOther: !!result.lockedByOther,
            lockId: owned ? String(result.lockId || '') : '',
            userId: String(result.userId || ''),
            userName: String(result.userName || ''),
            checklistKey: targetKey
        };
        const readMarker = String(result.readMarker || '');
        if (owned || !checklistReadMarker[targetKey]) {
            // Nobody else saves it while this window holds it.
            rememberChecklistReadMarker(targetKey, readMarker);
        } else if (result.changedSince) {
            pendingViewRefresh[targetKey] = true;
        }
        if (prevBlocked && !currentChecklistLock.lockedByOther && !owned) {
            // The other employee saved or cancelled: show it as it is now.
            pendingViewRefresh[targetKey] = true;
        }

        const changed = (
            prevOwned !== ownsCurrentChecklistLock()
            || prevBlocked !== isChecklistLockedByOther()
            || prevOwner !== String(currentChecklistLock.userName || '')
        );
        if (changed) {
            debugLog('checklist_lock_status_changed', {
                dialogId,
                checklistKey: targetKey,
                reason: String(reason || ''),
                owned,
                lockedByOther: !!result.lockedByOther,
                userName: String(result.userName || '')
            });
            // The notice follows the lock at once, before the data is read.
            updateLockNotice();
        }

        let refreshed = false;
        if (
            pendingViewRefresh[targetKey]
            && !owned
            && !currentChecklistLock.lockedByOther
            && !popupViewIsBusy()
        ) {
            try {
                refreshed = await refreshCurrentChecklistView(
                    prevBlocked ? 'lock_released' : 'saved_by_other'
                );
            } catch (refreshError) {
                console.log('checklist view refresh error:', refreshError);
            }
        }

        if (changed && !refreshed) {
            renderAll();
        }
        return result;
    } catch (error) {
        console.log('lock status error:', error);
        return null;
    } finally {
        lockStatusInFlight = false;
    }
}

// The first change took the lock: if another employee saved the checklist
// after this window read it, the window reads it again and the action is
// repeated on the current data (the lock is given back meanwhile).
async function ensureCurrentChecklistIsFresh(lockResult, actionName) {
    const targetKey = String(currentChecklistKey || '').trim();
    const readMarker = String(lockResult && lockResult.readMarker || '');
    if (!(lockResult && lockResult.changedSince) || targetKey === 'ga') {
        rememberChecklistReadMarker(targetKey, readMarker);
        return;
    }
    debugLog('checklist_changed_before_first_change', {
        dialogId,
        checklistKey: targetKey,
        actionName: String(actionName || ''),
        readMarker: checklistReadMarker[targetKey] || ''
    });
    try {
        await releaseChecklistLock(targetKey, false);
    } catch (releaseError) {
        console.log('lock release after stale view error:', releaseError);
    }
    try {
        await refreshCurrentChecklistView('changed_before_first_change');
    } catch (refreshError) {
        console.log('checklist reload after stale view error:', refreshError);
    }
    throw new Error(
        'Пока чек-лист был открыт, его сохранил другой сотрудник. '
        + 'Данные обновлены — повторите действие.'
    );
}

function startLockHeartbeat() {
    stopLockHeartbeat();
    if (isReadOnlyWindow() || popupClosedByHost) {
        return;
    }
    refreshCurrentChecklistLockStatus('start');
    lockHeartbeatTimer = setInterval(async function () {
        if (
            !currentEditorReady
            || popupFinalizationInProgress
            || lockHeartbeatInFlight
            || popupClosedByHost
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

            if (heartbeatResult.inactivityFinalized) {
                stopLockHeartbeat();
                window.dispatchEvent(new CustomEvent(
                    'checklist-edit-session-inactivity-finalized',
                    { detail: heartbeatResult }
                ));
                return;
            }

            const hasCurrentLock = syncCurrentLockFromSessionHeartbeat(
                heartbeatResult
            );

            if (!hasCurrentLock && !defersCurrentChecklistLock()) {
                await acquireChecklistLock(
                    currentChecklistKey,
                    true
                );
            }
            // Who edits the checklist now (the window holds no lock until
            // its first change).
            await refreshCurrentChecklistLockStatus('heartbeat');
        } finally {
            lockHeartbeatInFlight = false;
        }
    }, 15000);
}

async function releaseChecklistLock(checklistKey = currentChecklistKey, useBeacon = false) {
    if (isReadOnlyWindow()) {
        return null;
    }
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
    const fromPortal = Boolean(
        window.ChecklistAuth
        && window.ChecklistAuth.source === 'portal'
    );
    const embeddedInBitrix = !fromPortal && isEmbeddedBitrixPopup();
    const hostDiagnostics = window.ChecklistHostDiagnostics;

    function recordPopupCloseDiagnostic(event, payload) {
        try {
            if (
                hostDiagnostics
                && typeof hostDiagnostics.record === 'function'
            ) {
                hostDiagnostics.record(event, payload || {}, true);
            }
        } catch (error) {
            console.log('popup close diagnostics skipped:', error);
        }
    }

    recordPopupCloseDiagnostic('popup_diag_close_requested', {
        embeddedInBitrix,
        hasBx24: Boolean(window.BX24),
        hasCloseApplication: Boolean(
            window.BX24
            && typeof window.BX24.closeApplication === 'function'
        )
    });

    if (
        embeddedInBitrix
        && window.BX24
        && typeof window.BX24.closeApplication === 'function'
    ) {
        try {
            recordPopupCloseDiagnostic(
                'popup_diag_close_application_before',
                {}
            );
            window.BX24.closeApplication();
            recordPopupCloseDiagnostic(
                'popup_diag_close_application_returned',
                {}
            );
            return;
        } catch (error) {
            console.log('BX24.closeApplication error:', error);
            recordPopupCloseDiagnostic(
                'popup_diag_close_application_error',
                { error: String(error) }
            );
        }
    }

    // The page of the platform portal decides itself how to close the frame
    // it shows the checklist in; it is told so by a message.
    if (fromPortal && isEmbeddedBitrixPopup()) {
        try {
            window.parent.postMessage({ source: 'checklist', event: 'close' }, '*');
            recordPopupCloseDiagnostic('popup_diag_close_portal_message', {});
        } catch (error) {
            console.log('portal close message error:', error);
        }
    }

    const hasUsableOpener = Boolean(
        window.opener
        && !window.opener.closed
    );

    if (hasUsableOpener) {
        try {
            recordPopupCloseDiagnostic(
                'popup_diag_close_window_fallback_before',
                { hasUsableOpener }
            );
            if (typeof window.opener.focus === 'function') {
                window.opener.focus();
            }
            window.close();
            recordPopupCloseDiagnostic(
                'popup_diag_close_window_fallback_returned',
                { closed: Boolean(window.closed) }
            );
        } catch (error) {
            console.log('window.close error:', error);
            recordPopupCloseDiagnostic(
                'popup_diag_close_window_fallback_error',
                { error: String(error) }
            );
        }
    }

    window.setTimeout(function () {
        if (!window.closed) {
            recordPopupCloseDiagnostic(
                'popup_diag_close_reload_fallback',
                { hasUsableOpener }
            );
            (window.ChecklistAuth ? window.ChecklistAuth.reload() : window.location.reload());
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
    const staging = window.ChecklistUploadStaging;
    const hasStagedFiles = Boolean(
        staging
        && typeof staging.hasPendingFiles === 'function'
        && staging.hasPendingFiles()
    );

    if (hasStagedFiles) {
        if (!saveChanges) {
            if (typeof staging.clearAll === 'function') {
                staging.clearAll();
            }
        } else {
            throw new Error(
                'В области загрузки остались неподтверждённые файлы. '
                + 'Нажмите «Загрузить» или удалите их из списка.'
            );
        }
    }

    if (
        !manager
        || typeof manager.hasPending !== 'function'
        || !manager.hasPending()
    ) {
        return null;
    }

    if (!saveChanges) {
        // «Отмена» stops the uploads at once; the server rolls back with the
        // session also a file it has already received.
        if (typeof manager.abortAll === 'function') {
            manager.abortAll(
                'Отменено пользователем'
            );
        } else if (typeof manager.cancelAll === 'function') {
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
            'Отменяем изменения...'
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
    const staging = window.ChecklistUploadStaging;

    return Boolean(
        (
            manager
            && typeof manager.hasPending === 'function'
            && manager.hasPending()
        )
        || (
            staging
            && typeof staging.hasPendingFiles === 'function'
            && staging.hasPendingFiles()
        )
    );
}

async function finalizePopupSession(saveChanges, options = {}) {
    const finalizeOptions = options && typeof options === 'object' ? options : {};
    const finalizeReason = saveChanges
        ? String(finalizeOptions.reason || 'save_and_close')
        : 'cancel_button';
    if (
        popupFinalizationInProgress
        || popupFinalizeRequestInProgress
    ) {
        return false;
    }

    popupFinalizeRequestInProgress = true;

    if (saveCloseBtn) saveCloseBtn.disabled = true;
    if (saveContinueBtn) saveContinueBtn.disabled = true;
    if (cancelBtn) cancelBtn.disabled = true;

    // Граданализ: «Нет» needs a note; Save stops until it is filled. The
    // autosave of an idle window is not stopped.
    if (
        saveChanges
        && finalizeReason !== 'inactivity_timeout'
        && window.ChecklistGa
    ) {
        let gradAnalysisReady = true;
        try {
            gradAnalysisReady = await window.ChecklistGa.checkBeforeSave();
        } catch (gradAnalysisError) {
            console.log('Граданализ check skipped:', gradAnalysisError);
        }
        if (!gradAnalysisReady) {
            // The message of the check stays in the save state.
            popupFinalizeRequestInProgress = false;
            if (saveCloseBtn) saveCloseBtn.disabled = false;
            if (saveContinueBtn) saveContinueBtn.disabled = false;
            if (cancelBtn) cancelBtn.disabled = false;
            return false;
        }
    }

    try {
        if (typeof settleInlineItemRename === 'function') {
            await settleInlineItemRename({
                discard: !saveChanges,
                reason: finalizeReason
            });
        }
    } catch (renameError) {
        popupFinalizeRequestInProgress = false;
        if (saveCloseBtn) saveCloseBtn.disabled = false;
        if (saveContinueBtn) saveContinueBtn.disabled = false;
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
        if (saveContinueBtn) saveContinueBtn.disabled = false;
        if (cancelBtn) cancelBtn.disabled = false;
        setSaveState(
            'error',
            uploadWaitError && uploadWaitError.message
                ? uploadWaitError.message
                : 'Не удалось завершить очередь загрузки'
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
                finalizeReason,
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
        if (saveContinueBtn) saveContinueBtn.disabled = false;
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

    // Switching or adding a project stage continues in this window.
    if (typeof finalizeOptions.afterFinalize === 'function') {
        await finalizeOptions.afterFinalize(persistResult);
        return true;
    }

    // Stage 7.1.1.1: both successful save and rollback use the safe local
    // fallback. Bitrix closes through BX24.closeApplication(); a direct local
    // tab reloads instead of being redirected to a blank page.
    closePopupWindow();
    return true;
}

window.finalizePopupSession = finalizePopupSession;

// «Сохранить»: everything «Сохранить и закрыть» does (the changes, the files
// to Yandex, the message to the chat), but the window stays open and keeps
// the checklist: the server gives the locks to a new session of this window
// in the same transaction, so nobody else takes it in between.
async function savePopupSessionAndContinue() {
    if (
        isReadOnlyWindow()
        || popupFinalizationInProgress
        || popupFinalizeRequestInProgress
    ) {
        return false;
    }
    popupFinalizeRequestInProgress = true;
    const buttons = [saveCloseBtn, saveContinueBtn, cancelBtn].filter(Boolean);
    const release = () => {
        popupFinalizeRequestInProgress = false;
        buttons.forEach(button => { button.disabled = false; });
        setActionButtonState();
        updateLockNotice();
    };
    buttons.forEach(button => { button.disabled = true; });

    if (window.ChecklistGa) {
        let gradAnalysisReady = true;
        try {
            gradAnalysisReady = await window.ChecklistGa.checkBeforeSave();
        } catch (gradAnalysisError) {
            console.log('Граданализ check skipped:', gradAnalysisError);
        }
        if (!gradAnalysisReady) {
            release();
            return false;
        }
    }

    setSaveState('saving', 'Сохраняем...');
    try {
        await waitForPopupUploadsBeforeFinalize(true);
    } catch (uploadWaitError) {
        release();
        setSaveState(
            'error',
            uploadWaitError && uploadWaitError.message
                ? uploadWaitError.message
                : 'Не удалось завершить очередь загрузки'
        );
        return false;
    }

    // Edits and the session recovery of the heartbeat wait for the answer.
    popupFinalizationInProgress = true;
    syncChecklistCache();
    updateLockNotice();
    debugLog('save_continue_started', {
        dialogId,
        checklistKey: currentChecklistKey,
        editSessionId: getActiveEditSessionId()
    });

    const editSessionApi = window.ChecklistPopupEditSession;
    let result = null;
    try {
        const sessionEvents = window.ChecklistPopupSessionEvents;
        if (sessionEvents && typeof sessionEvents.ensureStarted === 'function') {
            await sessionEvents.ensureStarted();
        }
        await ensureCurrentEditorReady();
        const dirtyKeys = getDirtyChecklistKeys();
        const payload = {
            ...buildSessionFinalizePayload('save_continue'),
            continueEditing: true
        };
        result = await editSessionApi.finalize(payload, 'save_continue');
        dirtyKeys.forEach(clearChecklistSessionState);
    } catch (error) {
        popupFinalizationInProgress = false;
        const errorText = String(error && error.message || error || '').trim();
        debugLog('save_continue_failed', {
            dialogId,
            checklistKey: currentChecklistKey,
            error: errorText
        });
        release();
        setSaveState(
            'error',
            errorText ? 'Ошибка сохранения: ' + errorText.slice(0, 140) : 'Ошибка сохранения'
        );
        return false;
    }

    const adopted = !!(
        result
        && result.continuedSession
        && typeof editSessionApi.adopt === 'function'
        && editSessionApi.adopt(result.continuedSession)
    );
    popupFinalizationInProgress = false;
    if (!adopted) {
        // A repeated request gets no new session: the window starts one.
        await recoverPopupSessionAndCurrentLock();
    }
    try {
        await reloadCurrentChecklistFromServer();
        renderAll();
    } catch (reloadError) {
        console.log('reload after save skipped:', reloadError);
    }
    // «Анализ состояния объекта» shows ИД, ТУ and Граданализ as saved.
    if (
        window.ChecklistPopupObjectAnalysis
        && typeof window.ChecklistPopupObjectAnalysis.refresh === 'function'
    ) {
        window.ChecklistPopupObjectAnalysis.refresh();
    }
    startLockHeartbeat();
    release();
    setSaveState(
        '',
        result && result.messageOk === false ? 'Сохранено, сообщение не отправлено' : 'Сохранено'
    );
    debugLog('save_continue_completed', {
        dialogId,
        checklistKey: currentChecklistKey,
        editSessionId: getActiveEditSessionId(),
        adopted
    });
    return true;
}

window.savePopupSessionAndContinue = savePopupSessionAndContinue;

// The window closes: its uploads stop at once. A file the server has fully
// received is still written there and saved with the session; the others
// have to be uploaded again.
function abortPopupUploadsForClose(eventName) {
    const manager = window.popupUploadManager;
    const staging = window.ChecklistUploadStaging;
    let result = { aborted: 0, cancelled: 0 };
    try {
        if (manager && typeof manager.abortAll === 'function') {
            result = manager.abortAll('Окно закрыто') || result;
        }
    } catch (error) {
        console.log('close upload abort error:', error);
    }
    try {
        if (
            staging
            && typeof staging.hasPendingFiles === 'function'
            && staging.hasPendingFiles()
            && typeof staging.clearAll === 'function'
        ) {
            staging.clearAll();
        }
    } catch (error) {
        console.log('close staging clear error:', error);
    }
    if (result.aborted || result.cancelled) {
        debugLog('popup_close_uploads_aborted', {
            eventName,
            sessionId: getActiveEditSessionId(),
            aborted: Number(result.aborted || 0),
            cancelledQueued: Number(result.cancelled || 0)
        });
    }
    return result;
}

function sendCloseSummaryOnce(eventName) {
    // A switch to another tab of the browser only hides the window: it is
    // not closed (the host close of a slider is popup_frame_hidden).
    if (eventName === 'popup_hidden' || suppressAutoCloseSave || closeSummarySent) {
        return;
    }

    abortPopupUploadsForClose(eventName);

    if (getActiveEditSessionId()) {
        const finalizingSessionId = getActiveEditSessionId();
        const finalizeResult = finalizeDirtyChecklists(
            eventName,
            true
        );

        Promise.resolve(finalizeResult).then(function (result) {
            const queued = !!(result && result.queued);
            if (queued) {
                closeSummarySent = true;
                unloadFinalizedSessionId = finalizingSessionId;
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
