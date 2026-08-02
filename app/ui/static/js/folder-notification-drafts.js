(function (global) {
    'use strict';

    function collectFiles() {
        return Array.from(global.document.querySelectorAll('.folder-document-row')).map(function (row) {
            const link = row.querySelector('.folder-document-name');
            const documentId = String(row.dataset.documentRowId || '').trim();
            return {
                documentId,
                fileName: String(link && link.textContent || 'Файл').trim()
            };
        }).filter(function (file) { return !!file.documentId; });
    }

    async function openFolderDrafts() {
        const core = global.ChecklistFolderCore;
        const bootstrap = core && core.bootstrap || {};
        const sessionId = core && typeof core.requireSession === 'function'
            ? core.requireSession('создание оповещения')
            : '';
        const actor = core && typeof core.getActor === 'function'
            ? core.getActor()
            : { id: '', name: 'Пользователь' };

        await global.ChecklistNotificationDraftUI.open({
            source: 'folder',
            apiUrl: String(bootstrap.notificationDraftsApiUrl || ''),
            sessionId,
            dialogId: String(bootstrap.dialogId || ''),
            checklistKey: String(bootstrap.checklistKey || ''),
            itemId: String(bootstrap.itemId || ''),
            itemName: String(bootstrap.itemName || 'Пункт'),
            actor,
            files: collectFiles(),
            onChanged: function (detail) {
                if (core && typeof core.notifyParent === 'function') {
                    core.notifyParent('checklist-notification-drafts-changed', {
                        draftCount: detail.count || 0
                    });
                }
            }
        });
    }

    const button = global.document.getElementById('folderNotificationBtn');
    if (button) {
        button.addEventListener('click', function () {
            if (button.disabled) return;
            openFolderDrafts().catch(function (error) {
                console.log('folder notification drafts error:', error);
                global.alert(error && error.message ? error.message : 'Ошибка открытия оповещения');
            });
        });
    }

    global.ChecklistFolderNotificationDrafts = Object.freeze({
        open: openFolderDrafts,
        collectFiles
    });
})(window);
