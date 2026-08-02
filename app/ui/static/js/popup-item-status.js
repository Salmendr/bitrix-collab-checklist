(function (global) {
    'use strict';

    const inFlightItems = new Set();

    function normalizeItemStatus(item) {
        return normalizeStatus(item && item.status);
    }

    function findItem(itemId) {
        const targetId = String(itemId || '');
        return (Array.isArray(items) ? items : []).find(
            item => String(item && item.id || '') === targetId
        ) || null;
    }

    function getDefaultRequiredGroupId() {
        const meta = (
            typeof getCurrentChecklistLayoutMeta === 'function'
                ? getCurrentChecklistLayoutMeta()
                : {}
        ) || {};
        const notRequiredGroupId = Number(
            typeof getCurrentNotRequiredGroupId === 'function'
                ? getCurrentNotRequiredGroupId()
                : 0
        );
        const configured = Number(meta.defaultGroupId || 0);
        if (configured && configured !== notRequiredGroupId) {
            return configured;
        }
        const group = (Array.isArray(groups) ? groups : []).find(
            entry => Number(entry && entry.id || 0) !== notRequiredGroupId
        );
        return Number(group && group.id || 0);
    }

    function countItemsInGroup(groupId, excludedItemId = '') {
        const targetGroupId = Number(groupId || 0);
        const excluded = String(excludedItemId || '');
        return (Array.isArray(items) ? items : []).filter(item => (
            Number(item && item.group || 0) === targetGroupId
            && String(item && item.id || '') !== excluded
        )).length;
    }

    function statusLabel(status) {
        const normalized = normalizeStatus(status);
        if (normalized === 'Есть') return 'Есть';
        if (normalized === 'Не требуется') return 'Не требуется';
        return 'Нет';
    }

    function pushStatusChange(item, oldStatus, newStatus) {
        if (normalizeStatus(oldStatus) === normalizeStatus(newStatus)) {
            return;
        }
        pushSessionChange(
            item.id,
            item.name,
            'status',
            statusLabel(oldStatus),
            statusLabel(newStatus)
        );
        debugLog('status_circle_changed', {
            itemId: item.id,
            itemName: item.name,
            oldValue: statusLabel(oldStatus),
            newValue: statusLabel(newStatus),
        });
    }

    async function setStatusExists(item) {
        const oldStatus = item.status || '';
        const result = await updateItem(
            item.id,
            'status',
            'Есть',
            currentChecklistKey
        );
        if (!result || !result.item) {
            throw new Error('Не удалось изменить статус пункта');
        }
        replaceItem(result.item);
        pushStatusChange(result.item, oldStatus, result.item.status);
        renderAll();
        return result;
    }

    async function moveToNotRequired(item) {
        const ordering = global.ChecklistPopupItemOrdering;
        if (!ordering || typeof ordering.submitReorder !== 'function') {
            throw new Error('Модуль изменения порядка не готов');
        }
        const notRequiredGroupId = Number(
            typeof getCurrentNotRequiredGroupId === 'function'
                ? getCurrentNotRequiredGroupId()
                : 0
        );
        if (!notRequiredGroupId) {
            throw new Error('Раздел «Не требуется» не настроен');
        }
        const oldStatus = item.status || '';
        const result = await ordering.submitReorder(
            item.id,
            notRequiredGroupId,
            countItemsInGroup(notRequiredGroupId, item.id) + 1,
            {
                source: 'status-circle',
            }
        );
        const updated = result && result.item
            ? result.item
            : findItem(item.id);
        if (updated) {
            pushStatusChange(updated, oldStatus, updated.status);
        }
        return result;
    }

    async function restoreFromNotRequired(item) {
        const ordering = global.ChecklistPopupItemOrdering;
        if (!ordering || typeof ordering.submitReorder !== 'function') {
            throw new Error('Модуль изменения порядка не готов');
        }

        if (typeof fetchCurrentUserIfPossible === 'function') {
            await fetchCurrentUserIfPossible();
        }

        const documents = getItemDocuments(item);
        const canDelete = (
            typeof canCurrentUserDeleteFiles === 'function'
            && canCurrentUserDeleteFiles()
        );
        let deleteDocuments = false;

        if (documents.length && canDelete) {
            deleteDocuments = global.confirm(
                'В пункте "' + String(item.name || 'Пункт') + '" есть файлы.\n\n'
                + 'Нажмите «ОК», чтобы удалить текущие файлы и вернуть статус «Нет».\n'
                + 'Нажмите «Отмена», чтобы сохранить файлы и вернуть статус «Есть».'
            );
        }

        const targetGroupId = Number(
            item.notRequiredReturnGroupId
            || getDefaultRequiredGroupId()
            || 0
        );
        if (!targetGroupId) {
            throw new Error('Не удалось определить исходный раздел пункта');
        }
        const rememberedPosition = Number(
            item.notRequiredReturnPosition || 0
        );
        const targetPosition = rememberedPosition > 0
            ? rememberedPosition
            : countItemsInGroup(targetGroupId, item.id) + 1;
        const oldStatus = item.status || '';
        const removedNames = deleteDocuments
            ? documents.map(documentItem => (
                documentItem.name || 'Файл'
            )).join(', ')
            : '';

        const result = await ordering.submitReorder(
            item.id,
            targetGroupId,
            targetPosition,
            {
                source: 'status-circle',
                restoreFromNotRequired: true,
                deleteDocumentsOnRestore: deleteDocuments,
            }
        );
        const updated = result && result.item
            ? result.item
            : findItem(item.id);
        if (updated) {
            pushStatusChange(updated, oldStatus, updated.status);
        }
        if (deleteDocuments && removedNames) {
            pushSessionChange(
                item.id,
                item.name,
                'document',
                removedNames,
                'Удален'
            );
            debugLog('document_removed_by_status_circle', {
                itemId: item.id,
                itemName: item.name,
                documents: removedNames,
            });
        }
        return result;
    }

    async function cycleItemStatus(item) {
        const current = normalizeItemStatus(item);
        if (current === 'Есть') {
            return moveToNotRequired(item);
        }
        if (current === 'Не требуется') {
            return restoreFromNotRequired(item);
        }
        return setStatusExists(item);
    }

    function bind() {
        document.querySelectorAll(
            '[data-role="cycle-item-status"]'
        ).forEach(button => {
            if (button.dataset.statusCircleBound === '1') {
                return;
            }
            button.dataset.statusCircleBound = '1';
            button.addEventListener('click', async function (event) {
                event.preventDefault();
                event.stopPropagation();

                const itemId = String(this.dataset.itemId || '');
                if (
                    !itemId
                    || this.disabled
                    || inFlightItems.has(itemId)
                    || (
                        typeof isEditingAllowed === 'function'
                        && !isEditingAllowed()
                    )
                ) {
                    return;
                }

                const item = findItem(itemId);
                if (!item) return;

                inFlightItems.add(itemId);
                this.disabled = true;
                this.setAttribute('aria-busy', 'true');
                setSaveState('saving', 'Меняем статус...');

                try {
                    await requireEditingSession('изменение статуса');
                    await cycleItemStatus(item);
                    setSaveState('', 'Сохранено');
                } catch (error) {
                    console.error('status circle change failed:', error);
                    setSaveState(
                        'error',
                        error && error.message
                            ? error.message
                            : 'Ошибка изменения статуса'
                    );
                    if (typeof renderAll === 'function') {
                        renderAll();
                    }
                } finally {
                    inFlightItems.delete(itemId);
                }
            });
        });
    }

    global.ChecklistPopupItemStatus = Object.freeze({
        bind,
        cycleItemStatus,
    });
})(window);
