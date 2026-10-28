(function (global) {
    'use strict';

    const DRAG_THRESHOLD_PX = 6;
    const RETURN_TO_ORIGIN_PX = 18;
    const GHOST_OFFSET_X_PX = 16;
    const GHOST_OFFSET_Y_PX = -20;
    const AUTO_SCROLL_EDGE_PX = 72;
    const AUTO_SCROLL_STEP_PX = 22;
    const DROP_HYSTERESIS_PX = 10;
    const ROW_MOVE_ANIMATION_MS = 150;
    const DROP_SETTLE_MS = 90;

    let pointerState = null;
    let placeholder = null;
    let virtualNotRequiredZone = null;
    let currentDropTarget = null;
    let dragGhost = null;
    let listenersBound = false;
    let dragFrameId = 0;
    let pendingDragPoint = null;
    let currentDropSignature = '';
    let dropTargetLockedUntil = 0;

    function getItem(itemId) {
        const normalized = String(itemId || '');
        return (Array.isArray(items) ? items : []).find(
            item => String(item && item.id || '') === normalized
        ) || null;
    }

    function getGroupTitle(groupId) {
        const targetId = Number(groupId || 0);
        const group = (Array.isArray(groups) ? groups : []).find(
            entry => Number(entry && entry.id || 0) === targetId
        );
        return String(group && group.title || `Раздел ${targetId}`);
    }

    function getRowChildren(groupBlock) {
        if (!groupBlock) return [];
        return Array.from(groupBlock.children).filter(element =>
            element
            && element.matches
            && element.matches('.row[data-item-id], .row[data-item-id]')
        );
    }

    function isInlineRenameActive() {
        return (
            typeof inlineItemRenameState !== 'undefined'
            && !!inlineItemRenameState
        );
    }

    function isDragAllowed() {
        return (
            typeof isEditingAllowed === 'function'
            && isEditingAllowed()
            && !isInlineRenameActive()
            && !(
                typeof popupFinalizationInProgress !== 'undefined'
                && popupFinalizationInProgress
            )
        );
    }

    function removePlaceholder() {
        if (placeholder && placeholder.parentNode) {
            placeholder.parentNode.removeChild(placeholder);
        }
        placeholder = null;
    }

    function clearDropHighlights(options = {}) {
        document.querySelectorAll(
            '.item-order-drop-before,'
            + '.item-order-drop-after,'
            + '.item-order-drop-group,'
            + '.item-order-drop-into,'
            + '.item-order-drop-folder'
        ).forEach(element => {
            element.classList.remove(
                'item-order-drop-before',
                'item-order-drop-after',
                'item-order-drop-group',
                'item-order-drop-into',
                'item-order-drop-folder'
            );
        });
        setGhostHint('');
        currentDropTarget = null;
        currentDropSignature = '';
        if (!options.keepPlaceholder) {
            removePlaceholder();
        }
    }

    function captureVisibleRowRects() {
        const result = new Map();
        document.querySelectorAll(
            '.row[data-item-id]:not(.item-order-drag-source)'
        ).forEach(row => {
            result.set(
                String(row.dataset.itemId || ''),
                row.getBoundingClientRect()
            );
        });
        return result;
    }

    function animateRowsFromRects(beforeRects) {
        if (
            !beforeRects
            || typeof Element === 'undefined'
            || !Element.prototype.animate
            || (
                typeof global.matchMedia === 'function'
                && global.matchMedia(
                    '(prefers-reduced-motion: reduce)'
                ).matches
            )
        ) {
            return;
        }

        document.querySelectorAll(
            '.row[data-item-id]:not(.item-order-drag-source)'
        ).forEach(row => {
            const itemId = String(row.dataset.itemId || '');
            const before = beforeRects.get(itemId);
            if (!before) return;
            const after = row.getBoundingClientRect();
            const deltaY = before.top - after.top;
            if (Math.abs(deltaY) < 1) return;
            row.animate(
                [
                    {
                        transform: `translate3d(0, ${deltaY}px, 0)`,
                    },
                    {
                        transform: 'translate3d(0, 0, 0)',
                    },
                ],
                {
                    duration: ROW_MOVE_ANIMATION_MS,
                    easing: 'cubic-bezier(.2,.75,.25,1)',
                }
            );
        });
    }

    function ensureVirtualNotRequiredZone() {
        const notRequiredGroupId = Number(
            typeof getCurrentNotRequiredGroupId === 'function'
                ? getCurrentNotRequiredGroupId()
                : 0
        );
        if (!notRequiredGroupId) {
            return null;
        }

        const existing = Array.from(
            document.querySelectorAll(
                `[data-order-group-id="${notRequiredGroupId}"]`
            )
        ).find(element => (
            !element.classList.contains(
                'item-order-virtual-not-required'
            )
        )) || null;
        if (existing) {
            if (virtualNotRequiredZone && virtualNotRequiredZone.parentNode) {
                virtualNotRequiredZone.remove();
            }
            virtualNotRequiredZone = null;
            return existing;
        }

        if (!virtualNotRequiredZone) {
            virtualNotRequiredZone = document.createElement('div');
            virtualNotRequiredZone.className =
                'item-order-virtual-not-required';
            virtualNotRequiredZone.dataset.orderGroupId =
                String(notRequiredGroupId);
            virtualNotRequiredZone.dataset.orderDropZone = 'true';
            virtualNotRequiredZone.innerHTML = `
                <span class="item-order-virtual-icon">⋮⋮</span>
                <span>Переместить в «Не требуется»</span>
            `;
            document.body.appendChild(virtualNotRequiredZone);
        }

        return virtualNotRequiredZone;
    }

    function createPlaceholder() {
        if (placeholder) return placeholder;
        placeholder = document.createElement('div');
        placeholder.className = 'item-order-placeholder';
        placeholder.setAttribute('aria-hidden', 'true');
        const sourceHeight = Number(
            pointerState && pointerState.sourceHeight || 0
        );
        if (sourceHeight > 0) {
            placeholder.style.height = `${Math.max(38, sourceHeight)}px`;
        }
        return placeholder;
    }

    function removeDragGhost() {
        if (dragGhost && dragGhost.parentNode) {
            dragGhost.parentNode.removeChild(dragGhost);
        }
        dragGhost = null;
    }

    function createDragGhost() {
        removeDragGhost();
        if (!pointerState) return null;

        const item = getItem(pointerState.itemId);
        const itemName = String(item && item.name || 'Пункт');
        const sourceNameWidth = Number(
            pointerState.sourceNameWidth || 0
        );

        dragGhost = document.createElement('div');
        dragGhost.className = 'item-order-drag-ghost';
        dragGhost.setAttribute('aria-hidden', 'true');
        const nameLine = document.createElement('span');
        nameLine.textContent = itemName;
        const hintLine = document.createElement('span');
        hintLine.className = 'item-order-drag-ghost-hint';
        hintLine.hidden = true;
        dragGhost.append(nameLine, hintLine);
        dragGhost.style.width = `${Math.min(
            420,
            Math.max(180, sourceNameWidth)
        )}px`;
        document.body.appendChild(dragGhost);
        return dragGhost;
    }

    // A drop "into" a row keeps the gap of the placeholder, only unseen:
    // rows that jumped back would put another target under the pointer.
    function keepLayoutForInto() {
        clearDropHighlights({ keepPlaceholder: true });
        if (placeholder) {
            placeholder.style.visibility = 'hidden';
        }
    }

    // What the drop will do, under the name of the dragged item.
    function setGhostHint(text) {
        const hint = dragGhost && dragGhost.querySelector('.item-order-drag-ghost-hint');
        if (!hint) return;
        hint.textContent = String(text || '');
        hint.hidden = !text;
    }

    function updateDragGhost(clientX, clientY) {
        if (!dragGhost) return;
        const width = Number(dragGhost.offsetWidth || 220);
        const height = Number(dragGhost.offsetHeight || 42);
        const left = Math.min(
            Math.max(10, clientX + GHOST_OFFSET_X_PX),
            Math.max(10, window.innerWidth - width - 10)
        );
        const top = Math.min(
            Math.max(10, clientY + GHOST_OFFSET_Y_PX),
            Math.max(10, window.innerHeight - height - 10)
        );
        dragGhost.style.transform = `translate3d(${left}px, ${top}px, 0)`;
    }

    function insertPlaceholder(groupBlock, referenceRow, after) {
        const marker = createPlaceholder();
        marker.style.visibility = '';
        if (!groupBlock) return;

        if (referenceRow && referenceRow.parentNode === groupBlock) {
            if (after) {
                // Keep a parent row together with its block of subitems.
                const next = referenceRow.nextElementSibling;
                const anchor = (
                    next
                    && next.classList
                    && next.classList.contains('subitem-block')
                ) ? next : referenceRow;
                anchor.insertAdjacentElement('afterend', marker);
            } else {
                referenceRow.insertAdjacentElement('beforebegin', marker);
            }
            return;
        }

        const addRow = Array.from(groupBlock.children).find(
            element => element.classList
                && element.classList.contains('add-item-row')
        );
        groupBlock.insertBefore(marker, addRow || null);
    }

    function resolveGroupBlock(element) {
        if (!element || !element.closest) return null;
        return element.closest('[data-order-group-id]');
    }

    function parentIdOf(item) {
        return String(item && item.parentItemId || '').trim();
    }

    function currentNotRequiredGroupId() {
        return Number(
            typeof getCurrentNotRequiredGroupId === 'function'
                ? getCurrentNotRequiredGroupId()
                : 0
        );
    }

    function directRow(zone, itemId) {
        if (!zone) return null;
        return Array.from(zone.children).find(child => (
            child.matches
            && child.matches('.row[data-item-id]')
            && String(child.dataset.itemId || '') === String(itemId || '')
        )) || null;
    }

    // The middle of a row means "inside it", its edges "before / after".
    const INTO_EDGE_SHARE = 0.25;

    function inMiddleBand(row, clientY) {
        const rect = row.getBoundingClientRect();
        const edge = rect.height * INTO_EDGE_SHARE;
        return clientY > rect.top + edge && clientY < rect.bottom - edge;
    }

    function isNotRequiredItem(item) {
        return String(item && item.status || '').trim() === 'Не требуется'
            || Number(item && item.group || 0) === currentNotRequiredGroupId();
    }

    function activeChildrenOf(itemId) {
        return (Array.isArray(items) ? items : []).filter(entry => (
            parentIdOf(entry) === String(itemId || '')
            && String(entry && entry.status || '').trim() !== 'Не требуется'
        ));
    }

    // Another item row the dragged item may become a subitem of.
    function canHostSubitem(row) {
        const dragged = getItem(pointerState && pointerState.itemId);
        const host = getItem(row && row.dataset.itemId);
        return !!(
            dragged
            && host
            && String(host.id) !== String(dragged.id)
            && !parentIdOf(host)
            && !row.classList.contains('not-required')
            && !isNotRequiredItem(host)
            && !isNotRequiredItem(dragged)
            // «Изыскания» of a new ИД checklist stays an item of its section.
            && !dragged.systemItem
        );
    }

    // A subitem row the dragged item may become a folder of (items are two
    // levels deep: deeper they are folders, see item_folders.py).
    function folderHostRow(element, clientY) {
        const row = element.closest('.row.subitem-row[data-item-id]');
        if (!row || !pointerState) return null;
        const dragged = getItem(pointerState.itemId);
        const host = getItem(row.dataset.itemId);
        if (
            !dragged
            || !host
            || String(host.id) === String(dragged.id)
            || parentIdOf(host) === String(dragged.id)
            || isNotRequiredItem(dragged)
            || isNotRequiredItem(host)
            // A standard item of the template never becomes a folder.
            || (!parentIdOf(dragged) && !dragged.isCustom)
            || dragged.systemItem
        ) {
            return null;
        }
        return inMiddleBand(row, clientY) ? row : null;
    }

    // Top-level items drop between top-level rows or into the middle of
    // another item row (becomes its subitem); subitems drop into a list of
    // subitems, onto another item row (becomes its last subitem) or into
    // "Не требуется". The middle of a subitem row makes either a folder of
    // that subitem.
    function resolveDropContext(element, clientY) {
        const empty = { zone: null, row: null, intoParentRow: null, intoSubitemRow: null };
        if (!element || !element.closest || !pointerState) return empty;
        const folderHost = folderHostRow(element, clientY);
        if (folderHost) {
            return { zone: null, row: null, intoParentRow: null, intoSubitemRow: folderHost };
        }
        let zone = resolveGroupBlock(element);
        let row = element.closest('.row[data-item-id]');

        if (!pointerState.isSubitem) {
            while (zone && zone.dataset.orderParentId) {
                const parentId = zone.dataset.orderParentId;
                const outer = zone.parentElement
                    ? resolveGroupBlock(zone.parentElement)
                    : null;
                row = directRow(outer, parentId);
                zone = outer;
            }
            const block = element.closest('.subitem-block');
            if (zone && block && (!row || row.parentNode !== zone)) {
                row = directRow(zone, block.dataset.subitemParentId);
            }
            if (row && zone && row.parentNode !== zone) {
                row = null;
            }
            if (
                row
                && zone
                && element.closest('.row[data-item-id]') === row
                && inMiddleBand(row, clientY)
                && canHostSubitem(row)
            ) {
                return { zone: null, row: null, intoParentRow: row, intoSubitemRow: null };
            }
            return { zone, row, intoParentRow: null, intoSubitemRow: null };
        }

        if (zone && zone.dataset.orderParentId) {
            return {
                zone,
                row: row && row.parentNode === zone ? row : null,
                intoParentRow: null,
            };
        }
        if (
            zone
            && Number(zone.dataset.orderGroupId || 0) === currentNotRequiredGroupId()
        ) {
            return {
                zone,
                row: row && row.parentNode === zone ? row : null,
                intoParentRow: null,
            };
        }
        if (
            row
            && zone
            && row.parentNode === zone
            && !row.classList.contains('not-required')
            && String(row.dataset.itemId || '') !== String(pointerState.itemId || '')
        ) {
            return { zone: null, row: null, intoParentRow: row };
        }
        return empty;
    }

    function buildDropSignature(groupBlock, row, after) {
        const groupId = String(
            groupBlock && groupBlock.dataset.orderGroupId || ''
        );
        const rowId = String(row && row.dataset.itemId || '');
        return `${groupId}:${rowId}:${after ? 'after' : 'before'}`;
    }

    function updateDropTarget(clientX, clientY) {
        if (!pointerState || !pointerState.started) return;

        const element = document.elementFromPoint(clientX, clientY);
        const resolved = resolveDropContext(element, clientY);
        const row = resolved.row;
        const groupBlock = resolved.zone;

        if (resolved.intoSubitemRow) {
            const hostRow = resolved.intoSubitemRow;
            const signature = `folder:${hostRow.dataset.itemId || ''}`;
            if (signature === currentDropSignature) {
                return;
            }
            keepLayoutForInto();
            hostRow.classList.add('item-order-drop-folder');
            const host = getItem(hostRow.dataset.itemId);
            setGhostHint('станет папкой в «' + String(host && host.name || '') + '»');
            currentDropTarget = {
                groupBlock: null,
                row: null,
                after: true,
                intoParentRow: null,
                intoSubitemRow: hostRow,
            };
            currentDropSignature = signature;
            return;
        }

        if (resolved.intoParentRow) {
            const parentRow = resolved.intoParentRow;
            const signature = `into:${parentRow.dataset.itemId || ''}`;
            if (signature === currentDropSignature) {
                return;
            }
            keepLayoutForInto();
            parentRow.classList.add('item-order-drop-into');
            const parent = getItem(parentRow.dataset.itemId);
            setGhostHint('внутрь «' + String(parent && parent.name || '') + '»');
            currentDropTarget = {
                groupBlock: null,
                row: null,
                after: true,
                intoParentRow: parentRow,
            };
            currentDropSignature = signature;
            return;
        }

        if (row && groupBlock) {
            const rect = row.getBoundingClientRect();
            const midpoint = rect.top + rect.height / 2;
            let after = clientY > midpoint;

            if (
                currentDropTarget
                && currentDropTarget.row === row
                && currentDropTarget.after !== after
                && Math.abs(clientY - midpoint) < DROP_HYSTERESIS_PX
            ) {
                after = currentDropTarget.after;
            }

            const signature = buildDropSignature(
                groupBlock,
                row,
                after
            );
            if (signature === currentDropSignature) {
                return;
            }
            if (
                currentDropSignature
                && Date.now() < dropTargetLockedUntil
            ) {
                return;
            }

            const beforeRects = captureVisibleRowRects();
            clearDropHighlights({ keepPlaceholder: true });
            row.classList.add(
                after
                    ? 'item-order-drop-after'
                    : 'item-order-drop-before'
            );
            insertPlaceholder(groupBlock, row, after);
            currentDropTarget = {
                groupBlock,
                row,
                after,
            };
            currentDropSignature = signature;
            dropTargetLockedUntil = Date.now() + DROP_SETTLE_MS;
            animateRowsFromRects(beforeRects);
            return;
        }

        if (groupBlock) {
            if (
                currentDropTarget
                && currentDropTarget.row
                && currentDropTarget.groupBlock === groupBlock
            ) {
                const currentRect = (
                    currentDropTarget.row.getBoundingClientRect()
                );
                if (
                    clientY >= currentRect.top - DROP_HYSTERESIS_PX
                    && clientY <= currentRect.bottom + DROP_HYSTERESIS_PX
                ) {
                    return;
                }
            }

            const signature = buildDropSignature(
                groupBlock,
                null,
                true
            );
            if (signature === currentDropSignature) {
                return;
            }
            if (
                currentDropSignature
                && Date.now() < dropTargetLockedUntil
            ) {
                return;
            }

            const beforeRects = captureVisibleRowRects();
            clearDropHighlights({ keepPlaceholder: true });
            groupBlock.classList.add('item-order-drop-group');
            insertPlaceholder(groupBlock, null, true);
            currentDropTarget = {
                groupBlock,
                row: null,
                after: true,
            };
            currentDropSignature = signature;
            dropTargetLockedUntil = Date.now() + DROP_SETTLE_MS;
            animateRowsFromRects(beforeRects);
            return;
        }

        if (currentDropTarget) {
            const currentGroup = currentDropTarget.groupBlock;
            const rect = currentGroup
                ? currentGroup.getBoundingClientRect()
                : null;
            if (
                rect
                && clientX >= rect.left - DROP_HYSTERESIS_PX
                && clientX <= rect.right + DROP_HYSTERESIS_PX
                && clientY >= rect.top - DROP_HYSTERESIS_PX
                && clientY <= rect.bottom + DROP_HYSTERESIS_PX
            ) {
                return;
            }
            clearDropHighlights();
        }
    }

    function updateAutoScroll(clientY) {
        if (!pointerState || !pointerState.started) return;
        if (clientY < AUTO_SCROLL_EDGE_PX) {
            window.scrollBy(0, -AUTO_SCROLL_STEP_PX);
        } else if (
            clientY > window.innerHeight - AUTO_SCROLL_EDGE_PX
        ) {
            window.scrollBy(0, AUTO_SCROLL_STEP_PX);
        }
    }

    function startDragging(clientX, clientY) {
        if (!pointerState || pointerState.started) return;
        pointerState.started = true;
        pointerState.row = document.querySelector(
            `.row[data-item-id="${CSS.escape(pointerState.itemId)}"]`
        );
        const draggedItem = getItem(pointerState.itemId);
        pointerState.isSubitem = !!parentIdOf(draggedItem);
        pointerState.subitemBlock = pointerState.isSubitem
            ? null
            : document.querySelector(
                `.subitem-block[data-subitem-parent-id="${CSS.escape(pointerState.itemId)}"]`
            );
        if (pointerState.subitemBlock) {
            pointerState.subitemBlock.classList.add('item-order-drag-source');
        }
        if (pointerState.row) {
            const sourceRect = pointerState.row.getBoundingClientRect();
            const nameElement = pointerState.row.querySelector('.item-name');
            const nameRect = nameElement && nameElement.getBoundingClientRect
                ? nameElement.getBoundingClientRect()
                : null;
            pointerState.sourceHeight = Math.round(sourceRect.height || 0);
            pointerState.sourceNameWidth = Math.round(
                Number(nameRect && nameRect.width || 0)
            );
            pointerState.row.classList.add('item-order-drag-source');
        }
        createDragGhost();
        updateDragGhost(clientX, clientY);
        document.body.classList.add('item-order-dragging');
        ensureVirtualNotRequiredZone();
    }

    function releasePointerCapture(state) {
        if (
            state
            && state.handle
            && state.handle.hasPointerCapture
            && state.handle.hasPointerCapture(state.pointerId)
        ) {
            try {
                state.handle.releasePointerCapture(state.pointerId);
            } catch (error) {
                // Capture can already be released by the browser.
            }
        }
    }

    function cleanupPointerState(options = {}) {
        const state = pointerState;
        const restoreSource = options.restoreSource !== false;
        clearDropHighlights();
        document.body.classList.remove('item-order-dragging');
        removeDragGhost();
        if (restoreSource && state && state.row) {
            state.row.classList.remove('item-order-drag-source');
        }
        if (state && state.subitemBlock) {
            state.subitemBlock.classList.remove('item-order-drag-source');
        }
        releasePointerCapture(state);
        if (dragFrameId) {
            global.cancelAnimationFrame(dragFrameId);
            dragFrameId = 0;
        }
        pendingDragPoint = null;
        dropTargetLockedUntil = 0;
        pointerState = null;
    }

    function placeSourceAtPlaceholder(state) {
        if (!state || !state.row || !placeholder || !placeholder.parentNode) {
            return false;
        }
        placeholder.parentNode.insertBefore(state.row, placeholder);
        state.row.classList.remove('item-order-drag-source');
        removePlaceholder();
        return true;
    }

    function calculateTargetPosition(groupBlock, itemId) {
        if (!groupBlock || !placeholder) return 0;
        let position = 1;
        for (const child of Array.from(groupBlock.children)) {
            if (child === placeholder) {
                return position;
            }
            if (
                child.matches
                && child.matches('.row[data-item-id]')
                && String(child.dataset.itemId || '') !== String(itemId || '')
            ) {
                position += 1;
            }
        }
        return position;
    }

    async function submitReorder(itemId, targetGroupId, targetPosition, options = {}) {
        const item = getItem(itemId);
        if (!item) return null;

        setSaveState('saving', 'Сохраняем порядок...');
        const editSessionId = await requireEditingSession(
            'изменение порядка пунктов'
        );
        const identity = (
            typeof getCurrentEditorIdentity === 'function'
                ? getCurrentEditorIdentity()
                : { userId: '', userName: 'Пользователь' }
        );

        const response = await fetch(
            appUrl('api/checklist/reorder-items'),
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    dialogId,
                    checklistKey: currentChecklistKey,
                    itemId,
                    targetGroupId,
                    targetPosition,
                    targetParentId: (
                        options
                        && options.targetParentId !== undefined
                    )
                        ? String(options.targetParentId)
                        : undefined,
                    orderVersion: Number(currentOrderVersion || 0),
                    itemsState: (Array.isArray(items) ? items : []).map(
                        currentItem => ({
                            id: currentItem && currentItem.id || '',
                            group: Number(
                                currentItem && currentItem.group || 0
                            ),
                            order: Number(
                                currentItem && currentItem.order || 0
                            ),
                            name: currentItem && currentItem.name || '',
                            nameOverride:
                                currentItem
                                && currentItem.nameOverride
                                || '',
                            priority:
                                currentItem
                                && currentItem.priority
                                || '',
                            status:
                                currentItem
                                && currentItem.status
                                || '',
                            plan:
                                currentItem
                                && currentItem.plan
                                || '',
                            fact:
                                currentItem
                                && currentItem.fact
                                || '',
                            notRequiredReturnGroupId: Number(
                                currentItem
                                && currentItem.notRequiredReturnGroupId
                                || 0
                            ),
                            notRequiredReturnPosition: Number(
                                currentItem
                                && currentItem.notRequiredReturnPosition
                                || 0
                            ),
                            notRequiredReturnStatus:
                                currentItem
                                && currentItem.notRequiredReturnStatus
                                || '',
                            notRequiredReturnPriority:
                                currentItem
                                && currentItem.notRequiredReturnPriority
                                || '',
                            notRequiredReturnPlan:
                                currentItem
                                && currentItem.notRequiredReturnPlan
                                || '',
                            notRequiredReturnFact:
                                currentItem
                                && currentItem.notRequiredReturnFact
                                || '',
                        })
                    ),
                    restoreFromNotRequired: !!(
                        options && options.restoreFromNotRequired
                    ),
                    deleteDocumentsOnRestore: !!(
                        options && options.deleteDocumentsOnRestore
                    ),
                    sessionId: editSessionId,
                    requireEditSession: true,
                    actingUserId: identity.userId || '',
                    actingUserName:
                        identity.userName || 'Пользователь',
                }),
            }
        );
        const result = await response.json();

        if (!response.ok || !result.ok) {
            if (result && result.orderConflict) {
                await reloadCurrentChecklistFromServer();
                renderAll();
            }
            throw new Error(
                result.error || 'Не удалось сохранить порядок пунктов'
            );
        }

        const oldGroupId = Number(item.group || 0);
        const oldPosition = Number(item.order || 0);

        if (Array.isArray(result.items)) {
            items = result.items;
            rawItems = result.items;
        }
        currentOrderVersion = Number(
            result.orderVersion || currentOrderVersion || 0
        );

        if (!result.unchanged) {
            const updatedItem = result.item || getItem(itemId) || item;
            const newGroupId = Number(
                updatedItem.group || targetGroupId || 0
            );
            const newPosition = Number(
                updatedItem.order || targetPosition || 0
            );
            // move-name-suffix-v1
            if (result.nameAdjusted && result.previousName) {
                pushSessionChange(
                    itemId,
                    updatedItem.name || '',
                    'name',
                    result.previousName,
                    updatedItem.name || ''
                );
            }
            pushSessionChange(
                itemId,
                updatedItem.name || item.name || '',
                'order',
                `${getGroupTitle(oldGroupId)} / ${oldPosition}`,
                `${getGroupTitle(newGroupId)} / ${newPosition}`
            );
            (Array.isArray(result.convertedItems) ? result.convertedItems : []).forEach(converted => {
                pushSessionChange(
                    itemId,
                    updatedItem.name || item.name || '',
                    'item-to-folder',
                    String(converted && converted.name || ''),
                    'папка «' + String(converted && converted.folder || '') + '»'
                );
            });
            debugLog('checklist_item_reordered', {
                itemId,
                sourceGroupId: oldGroupId,
                sourcePosition: oldPosition,
                targetGroupId: newGroupId,
                targetPosition: newPosition,
                orderVersion: currentOrderVersion,
                source: String(options && options.source || 'drag'),
            });
        }

        setSaveState('', result.nameAdjusted && result.finalName
            ? `Сохранено как «${result.finalName}»`
            : 'Сохранено');
        renderAll();
        return result;
    }

    function quoteNames(list) {
        return list.map(entry => '«' + String(entry && entry.name || '') + '»').join(', ');
    }

    // An item with subitems dropped into another item: its subitems become
    // its folders and lose their status, plan and fact.
    function confirmSubitemsBecomeFolders(item, parent, children) {
        return global.confirm(
            '«' + String(item.name || '') + '» станет подпунктом «'
            + String(parent && parent.name || '') + '».\n\n'
            + 'Его подпункты ' + quoteNames(children)
            + ' станут папками внутри «' + String(item.name || '') + '»: '
            + 'их статусы, план и факт будут удалены, файлы и их версии сохранятся.\n\n'
            + 'До сохранения перенос можно отменить кнопкой «Отмена».'
        );
    }

    async function submitItemToFolder(itemId, host) {
        const item = getItem(itemId);
        if (!item || !host) return null;
        const children = activeChildrenOf(itemId);
        const confirmed = global.confirm(
            '«' + String(item.name || '') + '» станет папкой внутри подпункта «'
            + String(host.name || '') + '».\n\n'
            + (children.length
                ? 'Его подпункты ' + quoteNames(children) + ' станут вложенными папками.\n\n'
                : '')
            + 'Статус, план и факт будут удалены, файлы и их версии сохранятся.\n\n'
            + 'До сохранения перенос можно отменить кнопкой «Отмена».'
        );
        if (!confirmed) {
            renderAll();
            return null;
        }

        setSaveState('saving', 'Переносим...');
        const editSessionId = await requireEditingSession('перенос пункта');
        const identity = (
            typeof getCurrentEditorIdentity === 'function'
                ? getCurrentEditorIdentity()
                : { userId: '', userName: 'Пользователь' }
        );
        const response = await fetch(appUrl('api/checklist/item-to-folder'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                dialogId,
                checklistKey: currentChecklistKey,
                itemId,
                hostItemId: String(host.id || ''),
                orderVersion: Number(currentOrderVersion || 0),
                sessionId: editSessionId,
                requireEditSession: true,
                actingUserId: identity.userId || '',
                actingUserName: identity.userName || 'Пользователь',
            }),
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.ok) {
            if (result && result.orderConflict) {
                await reloadCurrentChecklistFromServer();
                renderAll();
            }
            throw new Error(result.error || 'Не удалось перенести пункт');
        }

        if (Array.isArray(result.items)) {
            items = result.items;
            rawItems = result.items;
        }
        currentOrderVersion = Number(result.orderVersion || currentOrderVersion || 0);
        (Array.isArray(result.convertedItems) ? result.convertedItems : []).forEach(converted => {
            pushSessionChange(
                String(host.id || ''),
                String(host.name || ''),
                'item-to-folder',
                String(converted && converted.name || ''),
                'папка «' + String(converted && converted.folder || '') + '»'
            );
        });
        debugLog('checklist_item_to_folder', {
            itemId,
            hostItemId: String(host.id || ''),
            relativeFolder: String(result.relativeFolder || ''),
            convertedCount: Array.isArray(result.convertedItems) ? result.convertedItems.length : 0,
            orderVersion: currentOrderVersion,
        });
        setSaveState('', 'Сохранено');
        renderAll();
        return result;
    }

    function handlePointerDown(event) {
        const handle = event.target.closest(
            '[data-role="item-drag-handle"]'
        );
        if (!handle) return;

        event.stopPropagation();
        event.preventDefault();

        if (
            event.button !== undefined
            && event.button !== 0
            && event.pointerType !== 'touch'
        ) {
            return;
        }
        if (handle.disabled || !isDragAllowed()) {
            return;
        }

        pointerState = {
            pointerId: event.pointerId,
            pointerType: event.pointerType || 'mouse',
            handle,
            itemId: String(handle.dataset.itemId || ''),
            startX: event.clientX,
            startY: event.clientY,
            started: false,
            row: null,
        };
        try {
            handle.setPointerCapture(event.pointerId);
        } catch (error) {
            // Pointer capture is optional.
        }
    }

    function handlePointerMove(event) {
        if (
            !pointerState
            || pointerState.pointerId !== event.pointerId
        ) {
            return;
        }

        const distance = Math.hypot(
            event.clientX - pointerState.startX,
            event.clientY - pointerState.startY
        );
        if (!pointerState.started && distance >= DRAG_THRESHOLD_PX) {
            startDragging(event.clientX, event.clientY);
        }
        if (!pointerState.started) return;

        event.preventDefault();
        updateDragGhost(event.clientX, event.clientY);
        pendingDragPoint = {
            clientX: event.clientX,
            clientY: event.clientY,
        };
        if (!dragFrameId) {
            dragFrameId = global.requestAnimationFrame(function () {
                dragFrameId = 0;
                const point = pendingDragPoint;
                pendingDragPoint = null;
                if (!point || !pointerState || !pointerState.started) {
                    return;
                }
                updateAutoScroll(point.clientY);
                updateDropTarget(point.clientX, point.clientY);
            });
        }
    }

    async function handlePointerUp(event) {
        if (
            !pointerState
            || pointerState.pointerId !== event.pointerId
        ) {
            return;
        }

        const state = pointerState;
        const wasStarted = state.started;
        const dropTarget = currentDropTarget;
        const releaseDistance = Math.hypot(
            event.clientX - state.startX,
            event.clientY - state.startY
        );
        let targetGroupId = 0;
        let targetPosition = 0;
        let targetParentId;

        if (wasStarted && dropTarget && dropTarget.intoSubitemRow) {
            const host = getItem(dropTarget.intoSubitemRow.dataset.itemId);
            cleanupPointerState();
            if (!host) return;
            try {
                await submitItemToFolder(state.itemId, host);
            } catch (error) {
                console.error('item to folder failed:', error);
                if (typeof renderAll === 'function') {
                    renderAll();
                }
                setSaveState(
                    'error',
                    error && error.message
                        ? error.message
                        : 'Не удалось перенести пункт'
                );
            }
            return;
        }

        if (wasStarted && dropTarget && dropTarget.intoParentRow) {
            const parentItem = getItem(dropTarget.intoParentRow.dataset.itemId);
            if (parentItem) {
                targetGroupId = Number(parentItem.group || 0);
                targetParentId = String(parentItem.id || '');
                targetPosition = (Array.isArray(items) ? items : []).filter(entry => (
                    parentIdOf(entry) === targetParentId
                    && String(entry.id || '') !== String(state.itemId || '')
                )).length + 1;
            }
        } else if (wasStarted && dropTarget && dropTarget.groupBlock) {
            targetGroupId = Number(
                dropTarget.groupBlock.dataset.orderGroupId || 0
            );
            targetPosition = calculateTargetPosition(
                dropTarget.groupBlock,
                state.itemId
            );
            if (dropTarget.groupBlock.dataset.orderParentId) {
                targetParentId = String(dropTarget.groupBlock.dataset.orderParentId);
            }
        }

        const sourceItem = getItem(state.itemId);
        const unchangedTarget = !!(
            sourceItem
            && Number(sourceItem.group || 0) === targetGroupId
            && Number(sourceItem.order || 0) === targetPosition
            && parentIdOf(sourceItem) === String(targetParentId || '')
        );
        const shouldReturnToOrigin = (
            !wasStarted
            || releaseDistance <= RETURN_TO_ORIGIN_PX
            || !targetGroupId
            || !targetPosition
            || unchangedTarget
        );

        if (shouldReturnToOrigin) {
            cleanupPointerState();
            return;
        }

        if (targetParentId && sourceItem && !parentIdOf(sourceItem)) {
            const children = activeChildrenOf(sourceItem.id);
            if (
                children.length
                && !confirmSubitemsBecomeFolders(sourceItem, getItem(targetParentId), children)
            ) {
                cleanupPointerState();
                return;
            }
        }

        if (dropTarget && dropTarget.intoParentRow) {
            // Its unseen gap is not where the item goes.
            removePlaceholder();
        }
        const visuallyPlaced = placeSourceAtPlaceholder(state);
        clearDropHighlights();
        document.body.classList.remove('item-order-dragging');
        removeDragGhost();
        releasePointerCapture(state);
        if (dragFrameId) {
            global.cancelAnimationFrame(dragFrameId);
            dragFrameId = 0;
        }
        pendingDragPoint = null;
        pointerState = null;

        if (!visuallyPlaced && state.row) {
            state.row.classList.remove('item-order-drag-source');
        }

        if (state.subitemBlock) {
            state.subitemBlock.classList.remove('item-order-drag-source');
        }

        try {
            await submitReorder(
                state.itemId,
                targetGroupId,
                targetPosition,
                targetParentId === undefined
                    ? {}
                    : { targetParentId }
            );
        } catch (error) {
            console.error('item reorder failed:', error);
            if (typeof renderAll === 'function') {
                renderAll();
            }
            setSaveState(
                'error',
                error && error.message
                    ? error.message
                    : 'Ошибка изменения порядка пунктов'
            );
        }
    }

    function handlePointerCancel(event) {
        if (
            pointerState
            && pointerState.pointerId === event.pointerId
        ) {
            cleanupPointerState();
        }
    }

    function bindGlobalListeners() {
        if (listenersBound) return;
        listenersBound = true;
        document.addEventListener('pointerdown', handlePointerDown);
        document.addEventListener('pointermove', handlePointerMove, {
            passive: false,
        });
        document.addEventListener('pointerup', handlePointerUp);
        document.addEventListener('pointercancel', handlePointerCancel);
    }

    function afterRender() {
        bindGlobalListeners();
        ensureVirtualNotRequiredZone();

        document.querySelectorAll(
            '[data-role="item-drag-handle"]'
        ).forEach(handle => {
            const disabled = !isDragAllowed();
            handle.disabled = disabled;
            handle.setAttribute(
                'aria-disabled',
                disabled ? 'true' : 'false'
            );
        });
    }

    global.ChecklistPopupItemOrdering = Object.freeze({
        afterRender,
        submitReorder,
        isDragAllowed,
        getOrderVersion: function () {
            return Number(currentOrderVersion || 0);
        },
    });
})(window);
