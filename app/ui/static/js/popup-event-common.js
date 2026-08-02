// Stage 6.6.4. Shared event handlers for standard checklist renderers.
(function (global) {
    'use strict';

    function cloneItem(item) {
        return JSON.parse(JSON.stringify(item));
    }

    function findItemByElement(element) {
        const itemId = element && element.dataset ? element.dataset.itemId : '';
        return items.find(item => item && item.id === itemId) || null;
    }

    function bindStandardStatusEvents() {
        const statusApi = global.ChecklistPopupItemStatus;
        if (!statusApi || typeof statusApi.bind !== 'function') {
            throw new Error('popup-item-status.js is not initialized');
        }
        statusApi.bind();
    }

    function bindStandardPlanEvents() {
        document.querySelectorAll('[data-role="plan"]').forEach(element => {
            element.addEventListener('change', async function () {
                const item = findItemByElement(this);
                if (!item) return;

                const oldItem = cloneItem(item);
                const newValue = fromInputDate(this.value);

                try {
                    await requireEditingSession(
                        'изменение плановой даты'
                    );
                } catch (error) {
                    this.value = toInputDate(oldItem.plan);
                    setSaveState(
                        'error',
                        error && error.message
                            ? error.message
                            : 'Сессия редактирования не готова'
                    );
                    return;
                }

                item.plan = newValue;
                pushSessionChange(
                    item.id,
                    item.name,
                    'plan',
                    oldItem.plan || '',
                    newValue || ''
                );
                debugLog('plan_changed', {
                    itemId: item.id,
                    itemName: item.name,
                    oldValue: oldItem.plan || '',
                    newValue: newValue || ''
                });
                renderAll();
            });
        });
    }

    function bindStandardFactEvents() {
        document.querySelectorAll('[data-role="fact"]').forEach(element => {
            element.addEventListener('change', async function () {
                const item = findItemByElement(this);
                if (!item) return;

                const oldItem = cloneItem(item);
                const newValue = fromInputDate(this.value);

                try {
                    await requireEditingSession(
                        'изменение фактической даты'
                    );
                } catch (error) {
                    this.value = toInputDate(oldItem.fact);
                    setSaveState(
                        'error',
                        error && error.message
                            ? error.message
                            : 'Сессия редактирования не готова'
                    );
                    return;
                }

                item.fact = newValue;
                pushSessionChange(
                    item.id,
                    item.name,
                    'fact',
                    oldItem.fact || '',
                    newValue || ''
                );
                debugLog('fact_changed', {
                    itemId: item.id,
                    itemName: item.name,
                    oldValue: oldItem.fact || '',
                    newValue: newValue || ''
                });
                renderAll();
            });
        });
    }

    function bindStandardAddItemEvents() {
        document.querySelectorAll('[data-role="add-item"]').forEach(button => {
            button.addEventListener('click', async function () {
                const groupId = Number(this.dataset.groupId);
                const input = document.getElementById('addItemInput_' + groupId);
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
                    const result = await addItem(groupId, name, currentChecklistKey);
                    if (!result || !result.item) {
                        throw new Error('add item failed');
                    }

                    replaceItem(result.item);
                    pushSessionChange(
                        result.item.id,
                        result.item.name,
                        'add-item',
                        '',
                        result.item.name
                    );
                    debugLog('item_added', {
                        itemId: result.item.id,
                        itemName: result.item.name,
                        groupId
                    });

                    input.value = '';
                    renderAll();
                } catch (error) {
                    console.log('add item error:', error);
                    setSaveState('error', 'Ошибка добавления пункта');
                } finally {
                    this.disabled = false;
                }
            });
        });
    }

    function bindItemRenameEvents() {
        document.querySelectorAll('[data-role="inline-rename-item"]').forEach(trigger => {
            if (trigger.dataset.inlineRenameBound === '1') {
                return;
            }
            trigger.dataset.inlineRenameBound = '1';
            trigger.addEventListener('click', function () {
                const item = findItemByElement(this);
                if (!item) return;
                startInlineItemRename(item, this);
            });
        });
    }

    function bindDocumentEvents() {
        if (typeof bindDocumentActions !== 'function') {
            throw new Error('popup-document-actions.js is not initialized');
        }
        bindDocumentActions();
    }

    function bindStandardChecklistEvents() {
        bindStandardStatusEvents();
        bindStandardPlanEvents();
        bindStandardFactEvents();
        bindStandardAddItemEvents();
        bindItemRenameEvents();
        bindDocumentEvents();
    }

    global.ChecklistPopupEventCommon = Object.freeze({
        bindStandardChecklistEvents,
        bindStandardStatusEvents,
        bindItemRenameEvents,
        bindDocumentEvents
    });
})(window);
