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

    function returnToChecklistPopup() {
        let openerFocused = false;

        try {
            if (
                global.opener
                && !global.opener.closed
            ) {
                if (typeof global.opener.focus === 'function') {
                    global.opener.focus();
                }
                openerFocused = true;
                global.close();
            }
        } catch (error) {
            console.log('folder return opener error:', error);
        }

        if (openerFocused) {
            global.setTimeout(function () {
                if (!global.closed && folderPopupUrl) {
                    global.location.assign(folderPopupUrl);
                }
            }, 160);
            return;
        }

        if (folderPopupUrl) {
            global.location.assign(folderPopupUrl);
            return;
        }

        if (global.history && global.history.length > 1) {
            global.history.back();
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
