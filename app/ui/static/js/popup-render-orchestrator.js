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

function findRenderedChecklistItem(itemId) {
    const targetItemId = String(itemId || '').trim();
    if (!targetItemId) return null;

    const candidates = document.querySelectorAll('[data-item-id]');
    for (let index = 0; index < candidates.length; index += 1) {
        const candidate = candidates[index];
        if (
            String(candidate && candidate.dataset && candidate.dataset.itemId || '')
                .trim()
            === targetItemId
        ) {
            return candidate;
        }
    }

    return null;
}

function waitForChecklistRender() {
    return new Promise(function (resolve) {
        window.requestAnimationFrame(function () {
            window.requestAnimationFrame(resolve);
        });
    });
}

function focusPopupHostWindow() {
    try {
        window.focus();
    } catch (error) {
        console.log('popup return focus skipped:', error);
    }

    try {
        if (
            window.top
            && typeof window.top.focus === 'function'
        ) {
            window.top.focus();
        }
    } catch (error) {
        console.log('popup host focus skipped:', error);
    }
}

async function returnToChecklistItem(options) {
    const payload = (
        options
        && typeof options === 'object'
            ? options
            : {}
    );
    const targetDialogId = String(payload.dialogId || '').trim();
    const targetChecklistKey = String(
        payload.checklistKey || currentChecklistKey || 'id'
    ).trim() || 'id';
    const targetItemId = String(payload.itemId || '').trim();

    if (
        targetDialogId
        && String(dialogId || '').trim()
        && targetDialogId !== String(dialogId || '').trim()
    ) {
        return {
            ok: false,
            reason: 'dialog_mismatch',
            checklistKey: targetChecklistKey,
            itemId: targetItemId
        };
    }

    await loadChecklistByKey(targetChecklistKey);
    await waitForChecklistRender();

    const itemElement = findRenderedChecklistItem(targetItemId);
    if (itemElement) {
        try {
            itemElement.scrollIntoView({
                behavior: 'smooth',
                block: 'center',
                inline: 'nearest'
            });
        } catch (error) {
            itemElement.scrollIntoView();
        }

        itemElement.classList.add('checklist-return-target');
        window.setTimeout(function () {
            itemElement.classList.remove('checklist-return-target');
        }, 2400);

        const focusTarget = itemElement.querySelector(
            'button, a, input, select, textarea, [tabindex]'
        );
        if (focusTarget && typeof focusTarget.focus === 'function') {
            try {
                focusTarget.focus({ preventScroll: true });
            } catch (error) {
                focusTarget.focus();
            }
        }
    }

    focusPopupHostWindow();

    if (typeof debugLog === 'function') {
        debugLog('popup_returned_from_item_folder', {
            checklistKey: targetChecklistKey,
            itemId: targetItemId,
            itemFound: !!itemElement,
            source: String(payload.source || '')
        });
    }

    return {
        ok: true,
        checklistKey: targetChecklistKey,
        itemId: targetItemId,
        itemFound: !!itemElement
    };
}

window.addEventListener('message', function (event) {
    if (
        event
        && event.origin
        && event.origin !== window.location.origin
    ) {
        return;
    }

    const data = event && event.data ? event.data : {};
    if (String(data.type || '') !== 'checklist-return-to-item') return;

    returnToChecklistItem(data).then(function (result) {
        try {
            if (
                event.source
                && typeof event.source.postMessage === 'function'
            ) {
                event.source.postMessage({
                    type: 'checklist-return-to-item-result',
                    ...result
                }, '*');
            }
        } catch (error) {
            console.log('popup return acknowledgement skipped:', error);
        }
    }).catch(function (error) {
        console.log('popup return-to-item error:', error);
    });
});

window.ChecklistPopupNavigation = Object.freeze({
    returnToItem: returnToChecklistItem,
    focusItem: function (itemId) {
        return returnToChecklistItem({
            dialogId,
            checklistKey: currentChecklistKey,
            itemId,
            source: 'popup_navigation_api'
        });
    }
});

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
