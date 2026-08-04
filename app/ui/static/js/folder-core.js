(function (global) {
    'use strict';

    const bootstrap = (
        global.CHECKLIST_FOLDER_BOOTSTRAP
        || {}
    );

    const actionIcons = global.ChecklistActionIcons;
    if (
        actionIcons
        && typeof actionIcons.hydrate === 'function'
    ) {
        actionIcons.hydrate(global.document);
    }

    const folderRemoveApiUrl = String(
        bootstrap.removeApiUrl || ''
    );
    const folderUploadApiUrl = String(
        bootstrap.uploadApiUrl || ''
    );
    const folderReplaceApiUrl = String(
        bootstrap.replaceApiUrl || ''
    );
    const folderDocumentMirrorStatusApiUrl = String(
        bootstrap.documentMirrorStatusApiUrl || ''
    );
    const folderArchiveDeleteApiUrl = String(
        bootstrap.archiveDeleteApiUrl || ''
    );
    const folderDialogId = String(
        bootstrap.dialogId || ''
    );
    const folderChecklistKey = String(
        bootstrap.checklistKey || ''
    );
    const folderItemId = String(
        bootstrap.itemId || ''
    );
    const folderItemGroup = String(
        bootstrap.itemGroup || ''
    );
    const folderItemName = String(
        bootstrap.itemName || 'Пункт'
    );
    const folderBootstrapSessionId = String(
        bootstrap.sessionId || ''
    ).trim();
    const folderPopupUrl = String(
        bootstrap.popupUrl || ''
    ).trim();

    let folderReturnInProgress = false;

    function focusChecklistPopupWindow(openerWindow) {
        try {
            if (
                openerWindow
                && typeof openerWindow.focus === 'function'
            ) {
                openerWindow.focus();
            }
        } catch (error) {
            console.log('folder return opener focus skipped:', error);
        }

        try {
            if (
                openerWindow
                && openerWindow.top
                && typeof openerWindow.top.focus === 'function'
            ) {
                openerWindow.top.focus();
            }
        } catch (error) {
            // Bitrix and the application can have different origins. Focusing
            // the top window is best-effort and the direct popup focus above
            // remains the primary path.
            console.log('folder return top focus skipped:', error);
        }
    }

    function replaceFolderWindowWithPopup() {
        if (!folderPopupUrl) return false;

        try {
            global.location.replace(folderPopupUrl);
        } catch (error) {
            global.location.assign(folderPopupUrl);
        }

        return true;
    }

    function scheduleFolderReturnFallback() {
        if (!folderPopupUrl) return;

        // Do not inspect window.closed here. Chromium can mark the browsing
        // context as closing even when the host shell keeps the tab visible.
        // If close() really succeeds this timer disappears with the window;
        // otherwise the folder tab deterministically becomes the target popup.
        global.setTimeout(function () {
            replaceFolderWindowWithPopup();
        }, 120);
    }

    function returnToChecklistPopup(event) {
        if (event && typeof event.preventDefault === 'function') {
            event.preventDefault();
        }
        if (folderReturnInProgress) return;

        folderReturnInProgress = true;
        if (folderBackButton) {
            folderBackButton.disabled = true;
            folderBackButton.setAttribute('aria-busy', 'true');
        }

        try {
            const openerWindow = (
                global.opener
                && !global.opener.closed
                    ? global.opener
                    : null
            );

            if (openerWindow) {
                const navigationApi = openerWindow.ChecklistPopupNavigation;
                if (
                    navigationApi
                    && typeof navigationApi.returnToItem === 'function'
                ) {
                    // Start navigation in the opener, but do not await it here.
                    // Waiting for checklist rendering consumes the click's user
                    // activation and Bitrix/Chromium can then reject close().
                    Promise.resolve(
                        navigationApi.returnToItem({
                            dialogId: folderDialogId,
                            checklistKey: folderChecklistKey,
                            itemId: folderItemId,
                            source: 'folder_back_button'
                        })
                    ).catch(function (error) {
                        console.log(
                            'folder return opener navigation error:',
                            error
                        );
                    });

                    focusChecklistPopupWindow(openerWindow);
                    scheduleFolderReturnFallback();

                    // Must remain synchronous inside the trusted click handler.
                    global.close();
                    return;
                }

                try {
                    openerWindow.postMessage({
                        type: 'checklist-return-to-item',
                        dialogId: folderDialogId,
                        checklistKey: folderChecklistKey,
                        itemId: folderItemId,
                        source: 'folder_back_button'
                    }, global.location.origin);

                    focusChecklistPopupWindow(openerWindow);
                    scheduleFolderReturnFallback();
                    global.close();
                    return;
                } catch (error) {
                    console.log('folder return postMessage error:', error);
                }
            }
        } catch (error) {
            console.log('folder return navigation error:', error);
        }

        if (replaceFolderWindowWithPopup()) {
            return;
        }

        if (global.history && global.history.length > 1) {
            global.history.back();
            return;
        }

        folderReturnInProgress = false;
        if (folderBackButton) {
            folderBackButton.disabled = false;
            folderBackButton.removeAttribute('aria-busy');
        }
    }

    const folderBackButton = (
        global.document
        && global.document.getElementById('folderBackBtn')
    );

    if (folderBackButton) {
        folderBackButton.addEventListener(
            'click',
            returnToChecklistPopup
        );
    }

    function getFolderEditSessionId() {
        try {
            const openerApi = (
                global.opener
                && global.opener.ChecklistPopupEditSession
            );
            if (
                openerApi
                && typeof openerApi.getSessionId === 'function'
            ) {
                const openerSessionId = String(
                    openerApi.getSessionId() || ''
                ).trim();
                const openerSessionActive = (
                    typeof openerApi.isActive !== 'function'
                    || openerApi.isActive()
                );

                if (
                    openerSessionId
                    && openerSessionActive
                ) {
                    return openerSessionId;
                }

                return '';
            }
        } catch (error) {
            // opener can be unavailable after popup close.
        }

        return folderBootstrapSessionId;
    }

    function requireFolderEditSession(
        actionName = 'изменение'
    ) {
        const sessionId = getFolderEditSessionId();

        if (!sessionId) {
            throw new Error(
                'Нельзя выполнить '
                + actionName
                + ': окно папки открыто без активной '
                + 'сессии редактирования'
            );
        }

        return sessionId;
    }

    function applyFolderEditSessionState() {
        const sessionReady = !!getFolderEditSessionId();
        const selector = [
            '#folderUploadBtn',
            '#folderUploadInput',
            '#folderReplaceInput',
            '#folderNotificationBtn',
            '[data-role="folder-remove-file"]',
            '[data-role="folder-replace-upload"]',
            '[data-role="folder-delete-archive-version"]'
        ].join(',');

        global.document.querySelectorAll(
            selector
        ).forEach(element => {
            if (!sessionReady) {
                if (
                    element.dataset.sessionDisabled
                    !== '1'
                ) {
                    element.dataset.sessionOriginalTitle = (
                        element.title || ''
                    );
                }

                element.dataset.sessionDisabled = '1';
                element.disabled = true;
                element.setAttribute(
                    'aria-disabled',
                    'true'
                );
                element.title = (
                    'Недоступно без активной '
                    + 'сессии редактирования'
                );
                return;
            }

            if (
                element.dataset.sessionDisabled
                !== '1'
            ) {
                return;
            }

            element.disabled = false;
            element.removeAttribute('aria-disabled');
            element.title = (
                element.dataset.sessionOriginalTitle
                || ''
            );
            delete element.dataset.sessionDisabled;
            delete element.dataset.sessionOriginalTitle;
        });

        return sessionReady;
    }

    const folderDeleteAllowedUserIds = new Set(
        Array.isArray(bootstrap.deleteAllowedUserIds)
            ? bootstrap.deleteAllowedUserIds.map(
                value => String(value || '').trim()
            )
            : []
    );

    const folderArchiveDeleteAdminUserIds = new Set(
        Array.isArray(bootstrap.archiveDeleteAdminUserIds)
            ? bootstrap.archiveDeleteAdminUserIds.map(
                value => String(value || '').trim()
            )
            : []
    );

    const channelFactory = global.ChecklistWindowChannel;
    const folderWindowChannel = (
        channelFactory
        && typeof channelFactory.create === 'function'
            ? channelFactory.create({
                dialogId: folderDialogId,
                role: 'folder'
            })
            : null
    );

    let folderActivityLastPublishedAt = 0;
    function publishFolderUserActivity(eventType = 'interaction') {
        const now = Date.now();
        if (now - folderActivityLastPublishedAt < 750) return;
        folderActivityLastPublishedAt = now;
        const payload = {
            activityAt: new Date(now).toISOString(),
            eventType: String(eventType || 'interaction'),
            sessionId: getFolderEditSessionId(),
            checklistKey: folderChecklistKey,
            itemId: folderItemId
        };
        if (folderWindowChannel) {
            folderWindowChannel.publish('checklist-user-activity', payload);
        }
        try {
            if (global.opener && !global.opener.closed) {
                global.opener.postMessage({
                    type: 'checklist-user-activity',
                    ...payload
                }, global.location.origin);
            }
        } catch (error) {
            // BroadcastChannel remains the primary transport.
        }
    }

    ['pointerdown', 'keydown', 'input', 'change', 'dragstart', 'drop', 'touchstart'].forEach(
        eventName => global.document.addEventListener(
            eventName,
            event => {
                if (event.isTrusted === false) return;
                publishFolderUserActivity(eventName);
            },
            { capture: true, passive: true }
        )
    );
    global.document.addEventListener(
        'scroll',
        event => {
            if (event.isTrusted === false) return;
            publishFolderUserActivity('scroll');
        },
        { capture: true, passive: true }
    );

    global.ChecklistFolderArchiveBootstrap = Object.freeze({
        deleteApiUrl: folderArchiveDeleteApiUrl,
        dialogId: folderDialogId,
        checklistKey: folderChecklistKey,
        itemId: folderItemId,
        adminUserIds: Array.from(
            folderArchiveDeleteAdminUserIds
        )
    });

    function getFolderDeleteActor() {
        try {
            const openerEditor = (
                global.opener
                && global.opener.currentEditor
                    ? global.opener.currentEditor
                    : null
            );

            const openerId = String(
                openerEditor
                && openerEditor.id
                || ''
            ).trim();
            const openerName = String(
                openerEditor
                && openerEditor.name
                || ''
            ).trim();

            return {
                id: openerId || String(bootstrap.userId || '').trim(),
                name: openerName || String(bootstrap.userName || '').trim() || 'Пользователь'
            };
        } catch (error) {
            // opener can be inaccessible; bootstrap identity remains available.
        }

        return {
            id: String(bootstrap.userId || '').trim(),
            name: String(bootstrap.userName || '').trim() || 'Пользователь'
        };
    }

    function postDirectlyToOpener(payload) {
        try {
            if (
                global.opener
                && typeof global.opener.postMessage
                    === 'function'
            ) {
                global.opener.postMessage(
                    payload,
                    global.location.origin
                );
                return true;
            }
        } catch (error) {
            console.log(
                'opener sync error:',
                error
            );
        }

        return false;
    }

    function notifyParentChecklistDocumentChanged(
        messageType = 'checklist-document-changed',
        extraPayload = {}
    ) {
        const payload = {
            dialogId: folderDialogId,
            checklistKey: folderChecklistKey,
            itemId: folderItemId,
            itemName: folderItemName,
            ...extraPayload
        };

        if (
            folderWindowChannel
            && messageType
                !== 'checklist-document-replacement-summary'
        ) {
            folderWindowChannel.publish(
                messageType,
                payload
            );
            return;
        }

        postDirectlyToOpener({
            type: messageType,
            ...payload
        });
    }

    function requestFolderRefresh(
        extraPayload = {}
    ) {
        if (!folderWindowChannel) {
            return false;
        }

        folderWindowChannel.publish(
            'checklist-folder-refresh',
            {
                dialogId: folderDialogId,
                checklistKey: folderChecklistKey,
                itemId: folderItemId,
                itemName: folderItemName,
                ...extraPayload
            }
        );

        return true;
    }

    if (folderWindowChannel) {
        folderWindowChannel.subscribe(
            'checklist-folder-refresh',
            function (envelope) {
                const payload = envelope.payload || {};
                const targetChecklistKey = String(
                    payload.checklistKey || ''
                );
                const targetItemId = String(
                    payload.itemId || ''
                );

                if (
                    targetChecklistKey
                    && targetChecklistKey
                        !== folderChecklistKey
                ) {
                    return;
                }

                if (
                    targetItemId
                    && targetItemId !== folderItemId
                ) {
                    return;
                }

                global.location.reload();
            }
        );

        global.addEventListener(
            'beforeunload',
            function () {
                folderWindowChannel.close();
            },
            {
                once: true
            }
        );
    }

    global.ChecklistFolderCore = Object.freeze({
        bootstrap,
        channel: folderWindowChannel,
        getActor: getFolderDeleteActor,
        getSessionId: getFolderEditSessionId,
        requireSession: requireFolderEditSession,
        applySessionState: applyFolderEditSessionState,
        notifyParent: notifyParentChecklistDocumentChanged,
        requestRefresh: requestFolderRefresh,
        returnToPopup: returnToChecklistPopup
    });

    Object.assign(global, {
        folderRemoveApiUrl,
        folderUploadApiUrl,
        folderReplaceApiUrl,
        folderDocumentMirrorStatusApiUrl,
        folderArchiveDeleteApiUrl,
        folderDialogId,
        folderChecklistKey,
        folderItemId,
        folderItemGroup,
        folderItemName,
        folderPopupUrl,
        folderDeleteAllowedUserIds,
        folderArchiveDeleteAdminUserIds,
        folderWindowChannel,
        getFolderDeleteActor,
        getFolderEditSessionId,
        requireFolderEditSession,
        notifyParentChecklistDocumentChanged
    });

    if (global.document.readyState === 'loading') {
        global.document.addEventListener(
            'DOMContentLoaded',
            applyFolderEditSessionState,
            { once: true }
        );
    } else {
        applyFolderEditSessionState();
    }
})(window);
