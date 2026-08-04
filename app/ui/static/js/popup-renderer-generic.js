// Stage 6.6.3. Active generic renderer extracted verbatim from popup-session-enhancements.js.
// This file intentionally keeps the original global function names.

const popupGenericRendererCommon = window.ChecklistPopupRendererCommon;
if (!popupGenericRendererCommon) {
    throw new Error('popup-renderer-common.js is not initialized');
}

let genericDateVisibility = {};

function isGenericDatesVisible(groupId) {
    void groupId;
    return false;
}

function getGenericGridClass(showDates) {
    return popupGenericRendererCommon.getDateGridClass(showDates);
}

function buildGenericTableHeader(group, showDates) {
    void showDates;

    return `
        <div class="group-title group-title-generic-head">${esc(group.title)}</div>
        <div class="thead-top ${getGenericGridClass(false)}">
            <div class="th">
                <span>Пункты раздела</span>
            </div>
            <div class="th">Документ</div>
        </div>
    `;
}

function renderGenericGroup(group, showDates) {
    const groupItems = getItemsByGroup(group.id);
    const allowAdd = currentAllowsCustomItemsForGroup(group.id);
    const gridClass = getGenericGridClass(showDates);

    const rows = groupItems.map(item => {
        const rowClass = normalizeStatus(item.status) === 'Не требуется'
            ? 'row not-required'
            : 'row';

        return `
            <div class="${rowClass} ${gridClass}" data-item-id="${esc(item.id)}">
                <div class="td">
                    ${buildItemNameCell(item, indicatorClass(item.status))}
                </div>
                <div class="td">${buildDocumentCell(item)}</div>
            </div>
        `;
    }).join('');

    const addBlock = allowAdd ? `
        <div class="add-item-row">
            <input class="add-item-input" id="addItemInput_${group.id}" type="text" placeholder="Новый пункт" ${disabledAttr()}>
            <button class="add-item-btn" type="button" data-role="add-item" data-group-id="${group.id}" ${disabledAttr()}>Добавить пункт</button>
        </div>
    ` : '';

    return `<div class="group-block" data-order-group-id="${esc(group.id)}" data-order-drop-zone="true">${rows}${addBlock}</div>`;
}

function splitGroupsIntoPanels(sourceGroups, panelCount) {
    const result = Array.from({ length: panelCount }, () => []);
    const safeGroups = Array.isArray(sourceGroups) ? sourceGroups : [];

    if (!safeGroups.length) {
        return result;
    }

    safeGroups.forEach((group, index) => {
        const panelIndex = Math.min(
            panelCount - 1,
            Math.floor(index * panelCount / safeGroups.length)
        );

        result[panelIndex].push(group);
    });

    return result;
}

function renderGenericPanel(panelGroups, appendNotRequired = false) {
    const safePanelGroups = Array.isArray(panelGroups) ? panelGroups : [];
    const notRequiredGroupId = getCurrentNotRequiredGroupId();
    const notRequiredGroup = appendNotRequired
        ? groups.find(g => Number(g.id) === Number(notRequiredGroupId))
        : null;

    const groupBlocks = safePanelGroups.map(group => {
        const showDates = false;

        return `
            <div class="generic-subtable">
                <div class="thead">
                    ${buildGenericTableHeader(group, showDates)}
                </div>
                <div>
                    ${renderGenericGroup(group, showDates)}
                </div>
            </div>
        `;
    }).join('');

    const notRequiredBlock = appendNotRequired && notRequiredGroup && hasItemsInGroup(notRequiredGroupId)
        ? `
            <div class="generic-subtable">
                <div class="thead">
                    ${buildGenericTableHeader(notRequiredGroup, false)}
                </div>
                <div>
                    ${renderGenericGroup(notRequiredGroup, false)}
                </div>
            </div>
        `
        : '';

    return groupBlocks + notRequiredBlock;
}

function setGenericSplitTableMode(enabled) {
    [leftTableEl, middleTableEl, rightTableEl].forEach(table => {
        if (!table) return;
        table.classList.toggle('generic-split-table', !!enabled);
        if (enabled) table.classList.remove('id-split-table');
    });
}

function resetTablePanelsForGeneric(panelCount) {
    if (typeof setGenericSplitTableMode === 'function') {
        setGenericSplitTableMode(true);
    }

    if (tablesGridEl) {
        tablesGridEl.classList.toggle('id-three-cols', panelCount >= 3);
        tablesGridEl.style.gridTemplateColumns = panelCount >= 3
            ? 'minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)'
            : panelCount === 2
                ? 'minmax(0, 1fr) minmax(0, 1fr)'
                : 'clamp(620px, 37vw, 760px)';
        tablesGridEl.style.justifyContent = panelCount === 1 ? 'start' : '';
    }

    tablePanels.forEach((panel, index) => {
        const visible = index < panelCount;
        panel.style.display = visible ? '' : 'none';
        panel.style.flex = panelCount === 1 && visible ? '0 0 auto' : '';
        panel.style.width = panelCount === 1 && visible ? 'clamp(620px, 37vw, 760px)' : '';
        panel.style.maxWidth = panelCount === 1 && visible ? 'clamp(620px, 37vw, 760px)' : '';
    });

    [leftTableEl, middleTableEl, rightTableEl].forEach((table, index) => {
        if (!table) return;
        table.style.width = panelCount === 1 && index === 0 ? '100%' : '';
        table.style.maxWidth = panelCount === 1 && index === 0 ? '100%' : '';
        table.innerHTML = '';
    });
}

function renderGenericTables() {
    if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {
        throw new Error('generic table containers not found');
    }

    const notRequiredGroupId = getCurrentNotRequiredGroupId();

    const activeGroups = (Array.isArray(groups) ? groups : []).filter(group =>
        Number(group.id) !== Number(notRequiredGroupId)
    );

    const visibleGroups = activeGroups.length ? activeGroups : (Array.isArray(groups) ? groups : []).slice(0, 1);
    const panelCount = Math.min(Math.max(visibleGroups.length, 1), 3);
    const targetTables = [leftTableEl, middleTableEl, rightTableEl];
    const groupedPanels = splitGroupsIntoPanels(visibleGroups, panelCount);

    resetTablePanelsForGeneric(panelCount);

    groupedPanels.forEach((panelGroups, index) => {
        const appendNotRequired = index === panelCount - 1;
        targetTables[index].innerHTML = renderGenericPanel(panelGroups, appendNotRequired);
    });
}
