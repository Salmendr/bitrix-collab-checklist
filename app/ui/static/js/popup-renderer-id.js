// Stage 6.6.3. Active ID renderer extracted verbatim from popup-core.js.
// This file intentionally keeps the original global function names.

const popupIdRendererCommon = window.ChecklistPopupRendererCommon;
if (!popupIdRendererCommon) {
    throw new Error('popup-renderer-common.js is not initialized');
}

            function isIdChecklist() {
                return currentChecklistKey === 'id';
            }

            function isIdDatesVisible(groupId) {
                void groupId;
                return false;
            }

            function getIdGridClass(showDates) {
                return popupIdRendererCommon.getDateGridClass(showDates);
            }

            function buildIdHeader(group, showDates) {
                void showDates;

                return `
                    <div class="thead-top ${getIdGridClass(false)}">
                        <div class="th">
                            <span>${esc(group.title)}</span>
                        </div>
                        <div class="th">Документ</div>
                    </div>
                `;
            }

            function renderIdGroup(group, showDates) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 4;
                const gridClass = getIdGridClass(showDates);

                const rows = groupItems.map(item => {
                    const rowClass = normalizeStatus(item.status) === 'Не требуется' ? 'row not-required' : 'row';

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

            function renderIdPanel(mainGroup, appendNotRequired = false) {
                const showDates = false;
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === 4)
                    : null;

                const mainPanel = `
                    <div class="id-subtable">
                        <div class="thead">
                            ${buildIdHeader(mainGroup, showDates)}
                        </div>
                        <div>
                            ${renderIdGroup(mainGroup, showDates)}
                        </div>
                    </div>
                `;

                const notRequiredPanel = (
                    appendNotRequired
                    && notRequiredGroup
                    && hasItemsInGroup(4)
                ) ? `
                    <div class="id-subtable">
                        <div class="thead">
                            ${buildIdHeader(notRequiredGroup, false)}
                        </div>
                        <div>
                            ${renderIdGroup(notRequiredGroup, false)}
                        </div>
                    </div>
                ` : '';

                return mainPanel + notRequiredPanel;
            }

            function renderIdTables() {
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('id table containers not found');
                }

                const idGroup = groups.find(g => Number(g.id) === 1) || { id: 1, title: 'ИД' };
                const tuGroup = groups.find(g => Number(g.id) === 2) || { id: 2, title: 'ТУ' };
                const otherGroup = groups.find(g => Number(g.id) === 3) || { id: 3, title: 'Прочее' };

                tablesGridEl.classList.add('id-three-cols');
                [leftTableEl, middleTableEl, rightTableEl].forEach(table => {
                    if (!table) return;
                    table.classList.remove('generic-split-table');
                    table.classList.add('id-split-table');
                });
                tablesGridEl.style.gridTemplateColumns = 'minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)';
                tablesGridEl.style.justifyContent = '';

                if (tablePanels[0]) {
                    tablePanels[0].style.display = '';
                    tablePanels[0].style.flex = '';
                    tablePanels[0].style.width = '';
                    tablePanels[0].style.maxWidth = '';
                }
                if (tablePanels[1]) {
                    tablePanels[1].style.display = '';
                    tablePanels[1].style.flex = '';
                    tablePanels[1].style.maxWidth = '';
                }
                if (tablePanels[2]) {
                    tablePanels[2].style.display = '';
                    tablePanels[2].style.flex = '';
                    tablePanels[2].style.maxWidth = '';
                }

                leftTableEl.style.width = '';
                leftTableEl.style.maxWidth = '';
                middleTableEl.style.width = '';
                middleTableEl.style.maxWidth = '';
                rightTableEl.style.width = '';
                rightTableEl.style.maxWidth = '';

                leftTableEl.innerHTML = renderIdPanel(idGroup, false);
                middleTableEl.innerHTML = renderIdPanel(tuGroup, false);
                rightTableEl.innerHTML = renderIdPanel(otherGroup, true);
            }
