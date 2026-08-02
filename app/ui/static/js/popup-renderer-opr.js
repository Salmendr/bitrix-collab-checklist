// Stage 6.6.3. Active OPR renderer extracted verbatim from popup-session-enhancements.js.
// This file intentionally keeps the original global function names.

const popupOprRendererCommon = window.ChecklistPopupRendererCommon;
if (!popupOprRendererCommon) {
    throw new Error('popup-renderer-common.js is not initialized');
}

oprIndicatorClass = function (item) {
    return indicatorClass(item && item.status);
};

function resolveOprGroupIdByItemIdOrName(item) {
    const itemId = String(item && item.id || '');
    if (itemId.startsWith('opr_g')) {
        const match = itemId.match(/^opr_g(\\d+)_/);
        if (match) {
            const groupId = Number(match[1]);
            if (groupId && groupId !== 2) {
                return groupId;
            }
        }
    }

    const name = String(item && item.name || '').trim();
    const matchedGroup = (Array.isArray(groups) ? groups : []).find(group => {
        const gid = Number(group && group.id);
        if (gid === 2) return false;

        return Array.isArray(items) && items.some(existing =>
            existing !== item &&
            Number(existing.group) === gid &&
            String(existing.name || '').trim() === name
        );
    });

    return matchedGroup ? Number(matchedGroup.id) : 1;
}

function buildOprDatesToggle() {
    return '';
}

function renderOprGroupUi(group) {
    const groupItems = getItemsByGroup(group.id);
    const allowAdd = Number(group.id) !== 2;
    const showDates = false;
    const gridClass = popupOprRendererCommon.getDateGridClass(showDates);

    const rows = groupItems.map(item => {
        const rowClass = normalizeStatus(item.status) === 'Не требуется'
            ? `row not-required ${gridClass}`
            : `row ${gridClass}`;

        return `
            <div class="${rowClass}" data-item-id="${esc(item.id)}">
                <div class="td">
                    ${buildItemNameCell(item, oprIndicatorClass(item))}
                </div>
                <div class="td">${buildDocumentCell(item)}</div>
            </div>
        `;
    }).join('');

    const addBlock = allowAdd ? `
        <div class="add-item-row">
            <input class="add-item-input" id="oprAddItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
            <button class="add-item-btn" type="button" data-role="opr-add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
        </div>
    ` : '';

    return `<div class="group-block" data-order-group-id="${esc(group.id)}" data-order-drop-zone="true">${rows}${addBlock}</div>`;
}

renderOprTables = function () {
    if (!leftTableEl || !rightTableEl || !tablesGridEl) {
        throw new Error('opr table containers not found');
    }

    const showDates = false;
    const gridClass = popupOprRendererCommon.getDateGridClass(showDates);

    tablesGridEl.style.gridTemplateColumns = 'clamp(620px, 37vw, 760px)';
    tablesGridEl.style.justifyContent = 'start';

    if (tablePanels[0]) {
        tablePanels[0].style.display = '';
        tablePanels[0].style.flex = '0 0 auto';
        tablePanels[0].style.width = 'clamp(620px, 37vw, 760px)';
        tablePanels[0].style.maxWidth = 'clamp(620px, 37vw, 760px)';
    }
    if (tablePanels[1]) {
        tablePanels[1].style.display = 'none';
        tablePanels[1].style.flex = '';
        tablePanels[1].style.maxWidth = '';
    }
    if (tablePanels[2]) {
        tablePanels[2].style.display = 'none';
        tablePanels[2].style.flex = '';
        tablePanels[2].style.maxWidth = '';
    }

    leftTableEl.style.width = '100%';
    leftTableEl.style.maxWidth = '100%';

    const visibleGroups = groups.filter(group => {
        if (Number(group.id) !== 2) return true;
        return items.some(x => Number(x.group) === 2);
    });

    leftTableEl.classList.add('id-table');
    leftTableEl.innerHTML = `
        <div class="thead">
            <div class="thead-top ${gridClass}">
                <div class="th">
                    <span>ОПР</span>
                </div>
                <div class="th">Документ</div>
            </div>
        </div>
        <div>
            ${visibleGroups.map(renderOprGroupUi).join('')}
        </div>
    `;

    if (middleTableEl) middleTableEl.innerHTML = '';
    if (rightTableEl) rightTableEl.innerHTML = '';
};
