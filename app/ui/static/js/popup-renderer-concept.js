// Stage 6.6.3. Active Concept renderer extracted verbatim from popup-core.js.
// This file intentionally keeps the original global function names.

const popupConceptRendererCommon = window.ChecklistPopupRendererCommon;
if (!popupConceptRendererCommon) {
    throw new Error('popup-renderer-common.js is not initialized');
}

            function isConceptDatesVisible(groupId) {
                void groupId;
                return false;
            }

            function getConceptGridClass(showDates) {
                return popupConceptRendererCommon.getDateGridClass(showDates);
            }

            function buildConceptTableHeader(group, showDates) {
                void showDates;

                return `
                    <div class="thead-top ${getConceptGridClass(false)}">
                        <div class="th">
                            <span>${esc(group.title)}</span>
                        </div>
                        <div class="th">Документ</div>
                    </div>
                `;
            }

            function renderConceptTableGroup(group, showDates) {
                const groupItems = getItemsByGroup(group.id);
                const allowAdd = Number(group.id) !== 10;
                const gridClass = getConceptGridClass(showDates);

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

            function renderConceptPanel(mainGroup, appendNotRequired = false) {
                const showDates = false;
                const notRequiredGroup = appendNotRequired
                    ? groups.find(g => Number(g.id) === 10)
                    : null;

                return `
                    <div class="table id-table">
                        <div class="thead">
                            ${buildConceptTableHeader(mainGroup, showDates)}
                        </div>
                        <div>
                            ${renderConceptTableGroup(mainGroup, showDates)}
                            ${appendNotRequired && notRequiredGroup && hasItemsInGroup(10) ? renderConceptTableGroup(notRequiredGroup, false) : ''}
                        </div>
                    </div>
                `;
            }

            function renderConceptTables() {
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('concept table containers not found');
                }

                const conceptGroup = groups.find(g => Number(g.id) === 1) || { id: 1, title: 'Концепция' };

                tablesGridEl.classList.remove('id-three-cols');
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

                leftTableEl.innerHTML = renderConceptPanel(conceptGroup, true);
                middleTableEl.innerHTML = '';
                rightTableEl.innerHTML = '';
            }
