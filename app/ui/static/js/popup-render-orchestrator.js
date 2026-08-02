const popupRendererRegistry = window.ChecklistPopupRendererRegistry;

if (!popupRendererRegistry) {
    throw new Error('popup-renderer-registry.js is not initialized');
}

function getPopupRendererContext() {
    const meta = typeof getCurrentChecklistLayoutMeta === 'function'
        ? getCurrentChecklistLayoutMeta()
        : {};

    return {
        checklistKey: String(currentChecklistKey || 'id').trim() || 'id',
        layoutMode: String(meta && meta.layoutMode || '').trim(),
        meta: meta || {}
    };
}

function registerPopupRendererStrategies() {
    popupRendererRegistry.register('configured-bim', {
        priority: 500,
        matches: function (context) {
            return [
                'concept_with_bim',
                'opr_with_bim',
                'stage_with_bim'
            ].includes(String(context && context.layoutMode || ''));
        },
        render: function () {
            renderConfiguredBimTables();
        },
        bindEvents: function () {
            bindGenericRendererEvents();
        }
    });

    popupRendererRegistry.register('id', {
        priority: 400,
        matches: function (context) {
            return String(context && context.checklistKey || '') === 'id';
        },
        render: function () {
            renderIdTables();
        },
        bindEvents: function () {
            bindIdRendererEvents();
        }
    });

    popupRendererRegistry.register('opr', {
        priority: 300,
        matches: function (context) {
            return String(context && context.checklistKey || '') === 'opr';
        },
        render: function () {
            renderOprTables();
        },
        bindEvents: function () {
            bindOprRendererEvents();
        }
    });

    popupRendererRegistry.register('concept', {
        priority: 200,
        matches: function (context) {
            return String(context && context.checklistKey || '') === 'concept';
        },
        render: function () {
            renderConceptTables();
        },
        bindEvents: function () {
            bindConceptRendererEvents();
        }
    });

    popupRendererRegistry.register('generic', {
        priority: -100,
        matches: function () {
            return true;
        },
        render: function () {
            renderGenericTables();
        },
        bindEvents: function () {
            bindGenericRendererEvents();
        }
    });
}

registerPopupRendererStrategies();

function resolvePopupRenderer() {
    const context = getPopupRendererContext();
    const renderer = popupRendererRegistry.resolve(context);

    if (!renderer) {
        throw new Error(
            'popup renderer is not registered for checklist: '
            + String(context.checklistKey || '')
        );
    }

    return {
        context,
        renderer
    };
}

function bindEvents(rendererEntry) {
    const renderer = rendererEntry && rendererEntry.renderer
        ? rendererEntry.renderer
        : null;

    if (!renderer || typeof renderer.bindEvents !== 'function') {
        throw new Error('popup renderer event binder is not registered');
    }

    renderer.bindEvents(rendererEntry.context || {});
}

function renderAll() {
    const rendererEntry = resolvePopupRenderer();

    rendererEntry.renderer.render(rendererEntry.context);
    bindEvents(rendererEntry);
    calculateProgress();
    renderTitle();
    renderProjectChecklistList();
    renderProjectRootFolderButton();
    renderStageFolderButton();

    if (
        window.ChecklistPopupItemOrdering
        && typeof window.ChecklistPopupItemOrdering.afterRender === 'function'
    ) {
        window.ChecklistPopupItemOrdering.afterRender();
    }

    if (progressBoxEl) {
        progressBoxEl.classList.toggle('id-accent', false);
    }

    updateDebugPanelAccess();
    syncChecklistCache();
    updateLockNotice();
}

async function loadChecklistByKey(checklistKey) {
    const targetKey = String(checklistKey || '').trim() || 'id';
    if (targetKey === currentChecklistKey) {
        return;
    }

    if (typeof settleInlineItemRename === 'function') {
        try {
            await settleInlineItemRename({ reason: 'checklist_switch' });
        } catch (error) {
            setSaveState(
                'error',
                error && error.message
                    ? error.message
                    : 'Сначала исправьте название пункта'
            );
            return;
        }
    }

    await ensureCurrentEditorReady();
    syncChecklistCache();
    await releaseChecklistLock(currentChecklistKey, false);
    setSaveState('saving', 'Загружаем...');

    try {
        const cachedData = checklistCache[targetKey];
        if (cachedData) {
            applyChecklistData(deepClone(cachedData));
            renderAll();
            await acquireChecklistLock(targetKey, true);
            startLockHeartbeat();
            return;
        }

        const response = await fetch(
            appUrl('api/checklist')
            + '?dialogId=' + encodeURIComponent(dialogId)
            + '&checklistKey=' + encodeURIComponent(targetKey)
        );
        const result = await response.json();

        if (!response.ok) {
            throw new Error(result.error || 'load checklist failed');
        }

        applyChecklistData(result);
        renderAll();
        await acquireChecklistLock(targetKey, true);
        startLockHeartbeat();
    } catch (error) {
        console.log('loadChecklistByKey orchestrator error:', error);
        setSaveState('error', 'Ошибка загрузки чек-листа');
    }
}

window.ChecklistPopupRenderOrchestrator = Object.freeze({
    getRendererContext: getPopupRendererContext,
    resolveRenderer: function () {
        const entry = resolvePopupRenderer();
        return {
            name: entry.renderer.name,
            context: entry.context
        };
    },
    listRenderers: popupRendererRegistry.list
});
