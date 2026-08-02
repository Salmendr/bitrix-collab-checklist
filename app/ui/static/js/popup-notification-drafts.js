(function (global) {
    'use strict';

    async function openForButton(button) {
        if (!button || button.disabled) return;
        const itemId = String(button.dataset.itemId || '').trim();
        const list = typeof items !== 'undefined' && Array.isArray(items) ? items : [];
        const item = list.find(function (entry) {
            return String(entry && entry.id || '').trim() === itemId;
        });
        if (!item) throw new Error('Пункт не найден');

        const sessionId = await requireEditingSession('создание оповещения');
        const identity = typeof getCurrentEditorIdentity === 'function'
            ? getCurrentEditorIdentity()
            : { userId: '', userName: 'Пользователь' };
        const documents = typeof getItemDocuments === 'function'
            ? getItemDocuments(item)
            : [];
        const apiUrl = typeof appUrl === 'function'
            ? appUrl('api/checklist/notification-drafts')
            : '';

        await global.ChecklistNotificationDraftUI.open({
            source: 'popup',
            apiUrl,
            sessionId,
            dialogId: typeof dialogId !== 'undefined' ? dialogId : '',
            checklistKey: typeof currentChecklistKey !== 'undefined' ? currentChecklistKey : '',
            itemId,
            itemName: String(item.name || 'Пункт'),
            actor: {
                id: identity.userId || '',
                name: identity.userName || 'Пользователь'
            },
            files: documents
        });
    }

    global.document.addEventListener('click', function (event) {
        const button = event.target.closest('[data-role="notify-documents"]');
        if (!button) return;
        event.preventDefault();
        event.stopPropagation();
        openForButton(button).catch(function (error) {
            console.log('notification draft popup error:', error);
            if (typeof setSaveState === 'function') {
                setSaveState('error', error && error.message ? error.message : 'Ошибка открытия оповещения');
            } else {
                global.alert(error && error.message ? error.message : 'Ошибка открытия оповещения');
            }
        });
    });

    global.ChecklistPopupNotificationDrafts = Object.freeze({
        openForButton
    });
})(window);
