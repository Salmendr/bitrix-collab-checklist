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
        if (normalized === 'В работе') return 'В работе';
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

    async function setStatus(item, status) {
        const oldStatus = item.status || '';
        const result = await updateItem(
            item.id,
            'status',
            status,
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

    async function restoreFromNotRequired(item, chosenStatus) {
        const ordering = global.ChecklistPopupItemOrdering;
        if (!ordering || typeof ordering.submitReorder !== 'function') {
            throw new Error('Модуль изменения порядка не готов');
        }

        if (typeof fetchCurrentUserIfPossible === 'function') {
            await fetchCurrentUserIfPossible();
        }

        const documents = getItemDocuments(item);
        // «Нет» returns the item without its files; another status keeps them.
        let deleteDocuments = false;
        if (chosenStatus === 'Нет' && documents.length) {
            if (!confirmStatusNoWithFiles(item.name, documents)) {
                return null;
            }
            deleteDocuments = true;
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
        let updated = result && result.item
            ? result.item
            : findItem(item.id);
        if (
            updated
            && chosenStatus
            && chosenStatus !== 'Нет'
            && normalizeStatus(updated.status) !== chosenStatus
        ) {
            const changed = await updateItem(item.id, 'status', chosenStatus, currentChecklistKey);
            if (changed && changed.item) {
                replaceItem(changed.item);
                updated = changed.item;
            }
            renderAll();
        }
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

    // «Нет» of an item with subitems: the files of the item and of its
    // subitems are deleted (one warning, the right to delete files), the
    // subitems become «Нет» too.
    async function setNo(item) {
        const subitemsApi = global.ChecklistPopupSubitems;
        const children = subitemsApi && subitemsApi.hasActiveSubitems(item)
            ? subitemsApi.getSubitems(item.id).filter(child => normalizeStatus(child.status) !== 'Не требуется')
            : [];
        const withFiles = [item].concat(children).filter(entry => getItemDocuments(entry).length);
        if (withFiles.length) {
            const documents = withFiles.reduce((all, entry) => all.concat(getItemDocuments(entry)), []);
            const name = children.length
                ? String(item.name || 'Пункт') + '" и его подпунктах "' + withFiles.filter(entry => entry !== item).map(entry => entry.name).join('", "')
                : item.name;
            if (!confirmStatusNoWithFiles(name, documents)) {
                return null;
            }
        }
        for (const child of children) {
            if (normalizeStatus(child.status) !== 'Нет' || getItemDocuments(child).length) {
                await setStatus(child, 'Нет');
            }
        }
        return setStatus(findItem(item.id) || item, 'Нет');
    }

    async function chooseItemStatus(item, status) {
        const current = normalizeItemStatus(item);
        const target = normalizeStatus(status);
        if (!target || target === current) {
            return null;
        }
        const subitemsApi = global.ChecklistPopupSubitems;
        if (
            target === 'Есть'
            && current !== 'Не требуется'
            && subitemsApi
            && subitemsApi.hasActiveSubitems(item)
        ) {
            throw new Error(
                'Статус пункта станет «Есть» сам, когда все подпункты будут «Есть»'
            );
        }
        if (current === 'Не требуется') {
            return restoreFromNotRequired(item, target);
        }
        if (target === 'Не требуется') {
            return moveToNotRequired(item);
        }
        if (target === 'Нет') {
            return setNo(item);
        }
        return setStatus(item, target);
    }

    // ---- the mini-menu of the status, as in Граданализ ----

    const MENU_ENTRIES = [
        { status: 'Есть', cls: 'green' },
        { status: 'В работе', cls: 'yellow' },
        { status: 'Нет', cls: '' },
        { status: 'Не требуется', cls: 'gray' }
    ];
    let menuItemId = '';

    function closeMenu() {
        const menu = document.querySelector('.item-status-menu');
        if (menu) menu.remove();
        menuItemId = '';
    }

    function openMenu(button, item) {
        closeMenu();
        const subitemsApi = global.ChecklistPopupSubitems;
        const isParent = !!(subitemsApi && subitemsApi.hasActiveSubitems(item));
        const current = normalizeItemStatus(item) || 'Нет';
        const menu = document.createElement('div');
        menu.className = 'item-status-menu';
        menu.setAttribute('role', 'menu');
        menu.innerHTML = MENU_ENTRIES.map(entry => {
            const parentYes = isParent && entry.status === 'Есть' && current !== 'Есть';
            const hint = parentYes
                ? '«Есть» ставится само, когда все подпункты «Есть»'
                : (entry.status === 'Нет' ? 'Файлы пункта будут удалены' : (entry.status === 'Не требуется' ? 'Пункт уйдёт в «Не требуется»' : ''));
            return '<button type="button" role="menuitem" data-item-status-set="' + entry.status + '"'
                + (parentYes ? ' disabled' : '')
                + (entry.status === current ? ' class="is-current"' : '')
                + (hint ? ' title="' + hint + '"' : '') + '>'
                + '<span class="status-indicator ' + entry.cls + '" aria-hidden="true"></span>'
                + '<span>' + entry.status + '</span></button>';
        }).join('');
        document.body.appendChild(menu);
        const box = button.getBoundingClientRect();
        const viewportHeight = document.documentElement.clientHeight;
        const below = box.bottom + 6;
        const top = below + menu.offsetHeight > viewportHeight - 8 ? box.top - menu.offsetHeight - 6 : below;
        const left = Math.min(box.left, document.documentElement.clientWidth - menu.offsetWidth - 8);
        menu.style.top = Math.max(8, top) + 'px';
        menu.style.left = Math.max(8, left) + 'px';
        menuItemId = String(item.id || '');
        const first = menu.querySelector('button.is-current:not(:disabled)') || menu.querySelector('button:not(:disabled)');
        if (first) first.focus();
    }

    async function applyFromMenu(itemId, status) {
        closeMenu();
        const item = findItem(itemId);
        if (!item || inFlightItems.has(itemId)) return;
        inFlightItems.add(itemId);
        setSaveState('saving', 'Меняем статус...');
        try {
            await requireEditingSession('изменение статуса');
            const result = await chooseItemStatus(item, status);
            setSaveState('', result === null ? 'Статус не изменён' : 'Сохранено');
        } catch (error) {
            console.error('status change failed:', error);
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
    }

    document.addEventListener('click', function (event) {
        const choice = event.target.closest && event.target.closest('[data-item-status-set]');
        if (choice) {
            event.preventDefault();
            event.stopPropagation();
            if (!choice.disabled) {
                void applyFromMenu(menuItemId, choice.dataset.itemStatusSet);
            }
            return;
        }
        if (menuItemId && !(event.target.closest && event.target.closest('.item-status-menu'))
            && !(event.target.closest && event.target.closest('[data-role="cycle-item-status"]'))) {
            closeMenu();
        }
    }, true);
    document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && menuItemId) closeMenu();
    });
    global.addEventListener('resize', closeMenu);
    document.addEventListener('scroll', function (event) {
        if (menuItemId && !(event.target.closest && event.target.closest('.item-status-menu'))) closeMenu();
    }, true);

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

                // The status is chosen in the mini-menu; a second click closes it.
                if (menuItemId === itemId && document.querySelector('.item-status-menu')) {
                    closeMenu();
                    return;
                }
                openMenu(this, item);
            });
        });
    }

    global.ChecklistPopupItemStatus = Object.freeze({
        bind,
        chooseItemStatus,
        closeMenu,
    });
})(window);
