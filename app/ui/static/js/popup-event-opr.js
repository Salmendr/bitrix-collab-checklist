// Stage 6.6.4. Event bindings for the OPR renderer.
const popupOprEventCommon = window.ChecklistPopupEventCommon;
if (!popupOprEventCommon) {
    throw new Error('popup-event-common.js is not initialized');
}

function bindOprRendererEvents() {
    popupOprEventCommon.bindDocumentEvents();
    popupOprEventCommon.bindItemRenameEvents();

    document.querySelectorAll('[data-role="opr-toggle-dates"]').forEach(button => {
        button.addEventListener('click', function () {
            oprDateVisibility[1] = !oprDateVisibility[1];
            renderAll();
        });
    });

    popupOprEventCommon.bindStandardStatusEvents();

    document.querySelectorAll('[data-role="opr-plan"]').forEach(element => {
        element.addEventListener('change', async function () {
            const item = items.find(candidate => candidate.id === this.dataset.itemId);
            if (!item) return;

            const oldValue = item.plan || '';
            const newValue = fromInputDate(this.value);

            try {
                await requireEditingSession(
                    'изменение плановой даты'
                );
            } catch (error) {
                this.value = toInputDate(oldValue);
                setSaveState(
                    'error',
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            item.plan = newValue;
            pushSessionChange(item.id, item.name, 'plan', oldValue, newValue);
            renderAll();
        });
    });

    document.querySelectorAll('[data-role="opr-fact"]').forEach(element => {
        element.addEventListener('change', async function () {
            const item = items.find(candidate => candidate.id === this.dataset.itemId);
            if (!item) return;

            const oldValue = item.fact || '';
            const newValue = fromInputDate(this.value);

            try {
                await requireEditingSession(
                    'изменение фактической даты'
                );
            } catch (error) {
                this.value = toInputDate(oldValue);
                setSaveState(
                    'error',
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            item.fact = newValue;
            pushSessionChange(item.id, item.name, 'fact', oldValue, newValue);
            renderAll();
        });
    });

    document.querySelectorAll('[data-role="opr-add-item"]').forEach(button => {
        button.addEventListener('click', async function () {
            const groupId = Number(this.dataset.groupId);
            const input = document.getElementById('oprAddItemInput_' + groupId);
            if (!input) return;

            const name = String(input.value || '').trim();
            if (!name) return;

            try {
                await requireEditingSession(
                    'добавление пункта'
                );
            } catch (error) {
                setSaveState(
                    'error',
                    error && error.message
                        ? error.message
                        : 'Сессия редактирования не готова'
                );
                return;
            }

            this.disabled = true;

            try {
                const result = await addItem(groupId, name, 'opr');
                if (!result || !result.item) {
                    throw new Error('add opr item failed');
                }

                replaceItem(result.item);
                pushSessionChange(
                    result.item.id,
                    result.item.name,
                    'add-item',
                    '',
                    result.item.name
                );
                debugLog('opr_item_added', {
                    itemId: result.item.id,
                    itemName: result.item.name,
                    groupId
                });

                input.value = '';
                renderAll();
            } catch (error) {
                console.log('opr add item error:', error);
                setSaveState('error', 'Ошибка добавления пункта');
            } finally {
                this.disabled = false;
            }
        });
    });
}
