// Stage 6.6.2. Active configured BIM renderer extracted verbatim from popup-core.js.
// This file intentionally keeps the original global function names.

            function getConfiguredBimGroup(meta = getCurrentChecklistLayoutMeta()) {
                const bimGroupId = Number(meta && meta.bimGroupId || 0);
                if (!bimGroupId) {
                    return null;
                }

                return (Array.isArray(groups) ? groups : []).find(group =>
                    Number(group && group.id) === bimGroupId
                ) || null;
            }

            function getConfiguredActiveGroupsWithoutBim(meta = getCurrentChecklistLayoutMeta()) {
                const notRequiredGroupId = Number(meta && meta.notRequiredGroupId || 0);
                const bimGroupId = Number(meta && meta.bimGroupId || 0);

                return (Array.isArray(groups) ? groups : []).filter(group => {
                    const groupId = Number(group && group.id || 0);
                    return groupId !== notRequiredGroupId && groupId !== bimGroupId;
                });
            }

            function renderConfiguredBimTables() {
                if (!leftTableEl || !middleTableEl || !rightTableEl || !tablesGridEl) {
                    throw new Error('configured BIM table containers not found');
                }

                const meta = getCurrentChecklistLayoutMeta();
                const bimGroup = getConfiguredBimGroup(meta);
                const placement = String(meta && meta.bimPlacement || '').trim();
                const activeGroups = getConfiguredActiveGroupsWithoutBim(meta);

                if (!bimGroup) {
                    renderGenericTables();
                    return;
                }

                if (placement === 'right') {
                    resetTablePanelsForGeneric(3);

                    if (tablePanels[2]) {
                        tablePanels[2].style.display = 'none';
                    }
                    if (rightTableEl) {
                        rightTableEl.innerHTML = '';
                    }

                    const mainGroups = activeGroups.length
                        ? activeGroups
                        : (Array.isArray(groups) ? groups.slice(0, 1) : []);

                    leftTableEl.innerHTML = renderGenericPanel(mainGroups, true);
                    middleTableEl.innerHTML = renderGenericPanel([bimGroup], false);
                    return;
                }

                if (placement === 'below_first_group') {
                    resetTablePanelsForGeneric(3);

                    const firstGroup = activeGroups[0] ? [activeGroups[0]] : [];
                    const secondGroup = activeGroups[1] ? [activeGroups[1]] : [];
                    const rightGroups = activeGroups.length > 2 ? activeGroups.slice(2) : [];

                    leftTableEl.innerHTML = renderGenericPanel(firstGroup.concat([bimGroup]), false);
                    middleTableEl.innerHTML = renderGenericPanel(secondGroup, false);
                    rightTableEl.innerHTML = renderGenericPanel(rightGroups, true);
                    return;
                }

                renderGenericTables();
            }
