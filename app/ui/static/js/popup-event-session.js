(function (global) {
    'use strict';

    let eventsBound = false;
    let startupPromise = null;

    function bindSessionActionEvents() {
        if (eventsBound) {
            return;
        }
        eventsBound = true;

        if (saveCloseBtn) {
            saveCloseBtn.addEventListener('click', async function () {
                await finalizePopupSession(true);
            });
        }

        if (cancelBtn) {
            cancelBtn.addEventListener('click', async function () {
                debugLog('popup_cancel_clicked', {
                    dialogId,
                    checklistKey: currentChecklistKey,
                    editSessionId: getActiveEditSessionId()
                });

                const confirmed = global.confirm(
                    'Отменить все изменения текущей сессии?\n\n' +
                    'Загруженные файлы и новые версии будут удалены, ' +
                    'удалённые файлы и прежние версии будут восстановлены.\n\n' +
                    'Подтверждаете?'
                );

                if (!confirmed) {
                    debugLog('popup_cancel_rejected', {
                        dialogId,
                        checklistKey: currentChecklistKey
                    });
                    return;
                }

                cancelBtn.disabled = true;
                if (saveCloseBtn) saveCloseBtn.disabled = true;
                setSaveState('saving', 'Отменяем изменения...');

                const completed = await finalizePopupSession(false);

                if (!completed && !global.closed) {
                    cancelBtn.disabled = false;
                    if (saveCloseBtn) saveCloseBtn.disabled = false;
                }
            });
        }
    }

    async function startPopupSession() {
        await ensureCurrentEditorReady();

        const editSessionApi = global.ChecklistPopupEditSession;
        if (
            !editSessionApi
            || typeof editSessionApi.start !== 'function'
        ) {
            throw new Error('popup-edit-session.js is not initialized');
        }

        await editSessionApi.start();
        await acquireChecklistLock(currentChecklistKey, true);
        startLockHeartbeat();
        updateLockNotice();

        if (
            String(projectRootYandexPath || '').trim()
            && (
                !String(projectRootYandexUrl || '').trim()
                || !projectRootYandexPrepared
            )
        ) {
            projectRootYandexPreparing = true;
            renderProjectRootFolderButton();

            try {
                const response = await fetch(
                    appUrl('api/project-root-folder')
                    + '?dialogId=' + encodeURIComponent(dialogId)
                );
                const result = await response.json();

                if (response.ok && result && result.ok) {
                    projectRootYandexUrl = String(result.url || '').trim();
                    projectRootYandexPrepared = !!result.standardFoldersPrepared;
                    debugLog('project_yandex_structure_prepared', result);
                } else {
                    debugLog(
                        'project_yandex_structure_prepare_failed',
                        result || {}
                    );
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

        await refreshCurrentStageYandexFolderInfo();

        debugLog('popup_session_startup_completed', {
            dialogId,
            checklistKey: currentChecklistKey,
            editSessionId: getActiveEditSessionId(),
            clientSessionId
        });

        return getActiveEditSessionId();
    }

    function isPopupSessionReady() {
        const editSessionApi = global.ChecklistPopupEditSession;
        const sessionActive = !!(
            editSessionApi
            && typeof editSessionApi.isActive === 'function'
            && editSessionApi.isActive()
        );
        const lockOwned = (
            typeof ownsCurrentChecklistLock === 'function'
                ? ownsCurrentChecklistLock()
                : false
        );

        return sessionActive && lockOwned;
    }

    function ensureStarted() {
        if (isPopupSessionReady()) {
            return Promise.resolve(getActiveEditSessionId());
        }

        if (!startupPromise) {
            startupPromise = startPopupSession().finally(function () {
                startupPromise = null;
            });
        }

        return startupPromise;
    }

    bindSessionActionEvents();
    global.setTimeout(function () {
        ensureStarted().catch(function (error) {
            console.log('startPopupSession error:', error);
            setSaveState(
                'error',
                'Ошибка запуска сессии: ' + String(
                    error && error.message || error || ''
                ).slice(0, 120)
            );
        });
    }, 0);

    global.ChecklistPopupSessionEvents = Object.freeze({
        bindSessionActionEvents,
        startPopupSession,
        ensureStarted
    });
})(window);
