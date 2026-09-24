// Subitems: one level of items inside a checklist item (Todoist-like).
// A subitem is an ordinary item with parentItemId; this module renders the
// nested rows, the "add subitem" controls and the collapse chevron.
(function (global) {
    'use strict';

    const STORAGE_PREFIX = 'checklist_subitems_open:';
    const NOT_REQUIRED = 'Не требуется';

    let openParents = null;
    let openParentsKey = '';
    let addOpenFor = '';
    let listenersBound = false;
    let submitting = false;

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function parentIdOf(item) {
        return text(item && item.parentItemId);
    }

    function isNotRequired(item) {
        return normalizeStatus(item && item.status) === NOT_REQUIRED;
    }

    function currentItems() {
        return Array.isArray(items) ? items : [];
    }

    function findItem(itemId) {
        const target = text(itemId);
        return currentItems().find(item => text(item && item.id) === target) || null;
    }

    function getSubitems(parentId) {
        const target = text(parentId);
        if (!target) return [];
        return currentItems()
            .filter(item => parentIdOf(item) === target)
            .sort((a, b) => Number(a.order || 0) - Number(b.order || 0));
    }

    function activeSubitems(parentId) {
        return getSubitems(parentId).filter(item => !isNotRequired(item));
    }

    function notRequiredGroupId() {
        return Number(
            typeof getCurrentNotRequiredGroupId === 'function'
                ? getCurrentNotRequiredGroupId()
                : 0
        );
    }

    // ---- collapsed / expanded state (per checklist, per browser) ----

    function storageKey() {
        return STORAGE_PREFIX
            + text(typeof dialogId !== 'undefined' ? dialogId : '')
            + ':'
            + text(typeof currentChecklistKey !== 'undefined' ? currentChecklistKey : '');
    }

    function openSet() {
        const key = storageKey();
        if (openParents && openParentsKey === key) {
            return openParents;
        }
        openParents = new Set();
        openParentsKey = key;
        try {
            const raw = global.localStorage.getItem(key);
            const parsed = raw ? JSON.parse(raw) : [];
            (Array.isArray(parsed) ? parsed : []).forEach(id => openParents.add(text(id)));
        } catch (error) {
            // Storage may be unavailable inside the Bitrix frame.
        }
        return openParents;
    }

    function saveOpenSet() {
        try {
            global.localStorage.setItem(
                storageKey(),
                JSON.stringify(Array.from(openSet()))
            );
        } catch (error) {
            // Not critical: subitems are simply collapsed next time.
        }
    }

    function isExpanded(parentId) {
        return openSet().has(text(parentId));
    }

    function setExpanded(parentId, expanded) {
        const set = openSet();
        if (expanded) {
            set.add(text(parentId));
        } else {
            set.delete(text(parentId));
        }
        saveOpenSet();
    }

    // ---- derived state ----

    // Mirror of the backend rule: a parent with active subitems is "Есть"
    // only when all of them are "Есть".
    function applyDerivedStatuses() {
        currentItems().forEach(item => {
            if (parentIdOf(item) || isNotRequired(item)) return;
            const children = activeSubitems(item.id);
            if (!children.length) return;
            const allDone = children.every(
                child => normalizeStatus(child.status) === 'Есть'
            );
            const current = normalizeStatus(item.status);
            let derived = current;
            if (allDone) {
                derived = 'Есть';
            } else if (current === 'Есть') {
                derived = 'Нет';
            }
            if (derived !== current) {
                item.status = derived;
                item.priority = derived === 'Есть' ? 'green' : 'gray';
            }
        });
    }

    function progressItems(sourceItems) {
        const list = Array.isArray(sourceItems) ? sourceItems : [];
        const notRequiredIds = new Set(
            list.filter(isNotRequired).map(item => text(item.id))
        );
        return list.filter(item => !notRequiredIds.has(parentIdOf(item)));
    }

    function hasSubitemDocuments(item) {
        return getSubitems(item && item.id).some(child => (
            typeof getItemDocuments === 'function'
            && getItemDocuments(child).length > 0
        ));
    }

    function canAddSubitem(item) {
        return !!item
            && !parentIdOf(item)
            && !isNotRequired(item)
            && Number(item.group || 0) !== notRequiredGroupId();
    }

    function hasActiveSubitems(item) {
        return !!item && !parentIdOf(item) && activeSubitems(item.id).length > 0;
    }

    function statusTitle(item) {
        if (!hasActiveSubitems(item) || isNotRequired(item)) return '';
        if (normalizeStatus(item.status) === 'Есть') {
            return 'Все подпункты выполнены. Нажмите, чтобы отметить «Не требуется»';
        }
        return 'Статус пункта станет «Есть», когда будут выполнены все подпункты';
    }

    // ---- markup ----

    function buildCounter(item) {
        if (!item || parentIdOf(item) || isNotRequired(item)) return '';
        const children = activeSubitems(item.id);
        if (!children.length) return '';
        const done = children.filter(
            child => normalizeStatus(child.status) === 'Есть'
        ).length;
        return `
            <span class="subitem-counter${done === children.length ? ' is-complete' : ''}"
                  title="Выполнено подпунктов: ${done} из ${children.length}">
                <svg class="subitem-counter-icon" viewBox="0 0 16 16" aria-hidden="true">
                    <circle cx="4" cy="3.5" r="1.8"></circle>
                    <circle cx="12" cy="12.5" r="1.8"></circle>
                    <path d="M4 5.3v3.2a4 4 0 0 0 4 4h2.2"></path>
                </svg>
                <span>${done}/${children.length}</span>
            </span>
        `;
    }

    function buildAddTrigger(item) {
        if (!canAddSubitem(item)) return '';
        return `
            <button
                type="button"
                class="subitem-add-trigger"
                data-role="open-add-subitem"
                data-item-id="${esc(item.id)}"
                title="Добавить подпункт"
                aria-label="Добавить подпункт к пункту ${esc(item.name || '')}"
                ${disabledAttr()}
            >
                <span class="subitem-add-plus" aria-hidden="true">+</span>
                <span class="subitem-add-label">Добавить подпункт</span>
            </button>
        `;
    }

    function buildToggle(item) {
        if (!item || parentIdOf(item) || !getSubitems(item.id).length) return '';
        const expanded = isExpanded(item.id);
        const title = expanded ? 'Скрыть подпункты' : 'Показать подпункты';
        return `
            <button
                type="button"
                class="subitem-toggle${expanded ? ' is-expanded' : ''}"
                data-role="toggle-subitems"
                data-item-id="${esc(item.id)}"
                aria-expanded="${expanded ? 'true' : 'false'}"
                title="${title}"
                aria-label="${title}"
            >
                <svg viewBox="0 0 16 16" aria-hidden="true">
                    <path d="M6 3.5 10.5 8 6 12.5"></path>
                </svg>
            </button>
        `;
    }

    function buildBlock(item, options = {}) {
        if (!item || parentIdOf(item) || isNotRequired(item)) return '';
        const parentId = text(item.id);
        const children = getSubitems(parentId);
        const addOpen = addOpenFor === parentId && canAddSubitem(item);
        if (!children.length && !addOpen) return '';

        const gridClass = text(options.gridClass);
        const indicator = typeof options.indicator === 'function'
            ? options.indicator
            : entry => indicatorClass(entry.status);
        const expanded = isExpanded(parentId) || addOpen;

        const rows = children.map(child => `
            <div class="row subitem-row ${gridClass}" data-item-id="${esc(child.id)}" data-parent-item-id="${esc(parentId)}">
                <div class="td">
                    ${buildItemNameCell(child, indicator(child))}
                </div>
                <div class="td">${buildDocumentCell(child)}</div>
            </div>
        `).join('');

        const addRow = addOpen ? `
            <div class="add-item-row subitem-add-row">
                <input
                    class="add-item-input subitem-add-input"
                    id="addSubitemInput_${esc(parentId)}"
                    data-role="add-subitem-input"
                    data-parent-item-id="${esc(parentId)}"
                    type="text"
                    placeholder="Название подпункта"
                    maxlength="160"
                    ${disabledAttr()}
                >
                <button
                    class="add-item-btn"
                    type="button"
                    data-role="add-subitem"
                    data-parent-item-id="${esc(parentId)}"
                    ${disabledAttr()}
                >Добавить подпункт</button>
                <button
                    class="subitem-add-cancel"
                    type="button"
                    data-role="cancel-add-subitem"
                    title="Отмена"
                    aria-label="Отмена"
                >×</button>
            </div>
        ` : '';

        return `
            <div class="subitem-block" data-subitem-parent-id="${esc(parentId)}">
                ${children.length ? `
                    <div
                        class="subitem-list"
                        data-order-group-id="${esc(item.group)}"
                        data-order-parent-id="${esc(parentId)}"
                        data-order-drop-zone="true"
                        ${expanded ? '' : 'hidden'}
                    >${rows}</div>
                ` : ''}
                ${addRow}
            </div>
        `;
    }

    // ---- actions ----

    function applyServerItems(result) {
        if (Array.isArray(result && result.items)) {
            items = result.items;
            if (typeof rawItems !== 'undefined') {
                rawItems = result.items;
            }
        } else if (result && result.item && typeof replaceItem === 'function') {
            replaceItem(result.item);
        }
        if (result && result.orderVersion) {
            currentOrderVersion = Number(result.orderVersion) || currentOrderVersion;
        }
    }

    function focusAddInput(parentId) {
        const input = document.getElementById('addSubitemInput_' + parentId);
        if (input) {
            input.focus();
        }
    }

    function openAddForm(parentId) {
        addOpenFor = text(parentId);
        setExpanded(parentId, true);
        renderAll();
        focusAddInput(parentId);
    }

    function closeAddForm() {
        if (!addOpenFor) return;
        addOpenFor = '';
        renderAll();
    }

    async function submitSubitem(parentId) {
        if (submitting) return;
        const parent = findItem(parentId);
        const input = document.getElementById('addSubitemInput_' + parentId);
        if (!parent || !input) return;
        const name = text(input.value);
        if (!name) {
            input.focus();
            return;
        }

        submitting = true;
        setSaveState('saving', 'Добавляем подпункт...');
        try {
            const editSessionId = await requireEditingSession('добавление подпункта');
            const identity = (
                typeof getCurrentEditorIdentity === 'function'
                    ? getCurrentEditorIdentity()
                    : { userId: '', userName: 'Пользователь' }
            );
            const response = await fetch(appUrl('api/checklist/add-item'), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    dialogId,
                    checklistKey: currentChecklistKey,
                    groupId: Number(parent.group || 0),
                    parentItemId: text(parent.id),
                    name,
                    sessionId: editSessionId,
                    requireEditSession: true,
                    actingUserId: identity.userId || '',
                    actingUserName: identity.userName || 'Пользователь'
                })
            });
            const result = await response.json();
            if (!response.ok || !result.ok || !result.item) {
                throw new Error(result && result.error || 'Не удалось добавить подпункт');
            }

            applyServerItems(result);
            pushSessionChange(
                result.item.id,
                `${parent.name} › ${result.item.name}`,
                'add-item',
                '',
                result.item.name
            );
            debugLog('subitem_added', {
                itemId: result.item.id,
                itemName: result.item.name,
                parentItemId: text(parent.id),
            });

            addOpenFor = '';
            setExpanded(parent.id, true);
            setSaveState('', result.nameAdjusted
                ? `Сохранено как «${result.finalName}»`
                : 'Сохранено');
            renderAll();
        } catch (error) {
            console.log('add subitem error:', error);
            setSaveState(
                'error',
                error && error.message ? error.message : 'Ошибка добавления подпункта'
            );
        } finally {
            submitting = false;
        }
    }

    function toggleSubitems(button) {
        const parentId = text(button.dataset.itemId);
        const expanded = !isExpanded(parentId);
        setExpanded(parentId, expanded);
        const list = document.querySelector(
            `.subitem-list[data-order-parent-id="${CSS.escape(parentId)}"]`
        );
        if (list) {
            list.hidden = !expanded;
        }
        button.classList.toggle('is-expanded', expanded);
        button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        const title = expanded ? 'Скрыть подпункты' : 'Показать подпункты';
        button.title = title;
        button.setAttribute('aria-label', title);
    }

    function bindOnce() {
        if (listenersBound) return;
        listenersBound = true;

        document.addEventListener('click', function (event) {
            const target = event.target && event.target.closest
                ? event.target.closest('[data-role]')
                : null;
            if (!target) return;
            const role = target.dataset.role;

            if (role === 'toggle-subitems') {
                event.preventDefault();
                event.stopPropagation();
                toggleSubitems(target);
                return;
            }
            if (role === 'open-add-subitem') {
                event.preventDefault();
                event.stopPropagation();
                if (target.disabled) return;
                openAddForm(target.dataset.itemId);
                return;
            }
            if (role === 'add-subitem') {
                event.preventDefault();
                if (target.disabled) return;
                submitSubitem(text(target.dataset.parentItemId));
                return;
            }
            if (role === 'cancel-add-subitem') {
                event.preventDefault();
                closeAddForm();
            }
        });

        document.addEventListener('keydown', function (event) {
            const input = event.target;
            if (
                !input
                || !input.dataset
                || input.dataset.role !== 'add-subitem-input'
            ) {
                return;
            }
            if (event.key === 'Enter') {
                event.preventDefault();
                submitSubitem(text(input.dataset.parentItemId));
            } else if (event.key === 'Escape') {
                event.preventDefault();
                closeAddForm();
            }
        });
    }

    function beforeRender() {
        bindOnce();
        applyDerivedStatuses();
        if (addOpenFor && !canAddSubitem(findItem(addOpenFor))) {
            addOpenFor = '';
        }
    }

    global.ChecklistPopupSubitems = Object.freeze({
        beforeRender,
        buildAddTrigger,
        buildBlock,
        buildCounter,
        buildToggle,
        getSubitems,
        hasActiveSubitems,
        hasSubitemDocuments,
        isExpanded,
        parentIdOf,
        progressItems,
        setExpanded,
        statusTitle,
    });
})(window);
