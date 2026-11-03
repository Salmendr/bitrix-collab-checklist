(function (global) {
    'use strict';

    // The panel «Анализ состояния объекта» under the checklist header. The
    // server shows it to the leaders, administrators and GIPs of the
    // project; its data belong to the project, so every checklist window of
    // the project shows the same panel. The arrow opens the slice of all
    // objects, «Внешние риски» the risk tables. The panel opens folded; the
    // chevron of the right corner unfolds it, like the subitems of an item.

    const doc = global.document;
    const panelEl = doc.getElementById('objectAnalysisPanel');
    const UI = global.ObjectAnalysisUI;
    const popupApi = global.ChecklistPopupApi;
    if (!panelEl || !UI || !popupApi) return;

    const REFRESH_PAUSE_MS = 5000;
    const state = { data: null, loadedAt: 0, loading: null, collapsed: true };

    function actor() {
        const editor = global.currentEditor || {};
        return { userId: String(editor.id || ''), userName: String(editor.name || '') };
    }

    const api = UI.createApi({ baseUrl: popupApi.baseUrl, actor });
    const dialogId = String((global.CHECKLIST_POPUP_BOOTSTRAP || {}).dialogId || '');

    function pageUrl(path) {
        const who = actor();
        const url = new URL(popupApi.url(path));
        url.searchParams.set('dialogId', dialogId);
        if (who.userId) url.searchParams.set('userId', who.userId);
        if (who.userName) url.searchParams.set('userName', who.userName);
        return url.href;
    }

    const ctx = {
        api,
        editable: true,
        get options() { return state.data ? state.data.options : {}; },
        get rights() { return state.data ? state.data.rights : {}; },
        get contractStatuses() { return state.data ? state.data.contractStatuses : []; },
        get riskLevels() { return state.data ? state.data.riskLevels : []; },
        get limits() { return state.data ? state.data.limits : {}; },
        project(id) {
            const project = state.data && state.data.project;
            return project && project.dialogId === id ? project : null;
        },
        onProject(project) {
            if (!state.data || !project) return;
            state.data.project = project;
            render();
        },
        onOptions(options) {
            if (!state.data || !options) return;
            state.data.options = options;
            render();
        },
        openRisks() {
            global.open(pageUrl('object-analysis/risks'), '_blank');
        },
        findAnchor(id, key, half) {
            return UI.findAnchor(panelEl, id, key, half);
        }
    };

    function render() {
        const data = state.data;
        if (!data || !data.visible || !data.project) {
            panelEl.hidden = true;
            panelEl.innerHTML = '';
            return;
        }
        const collapsed = !!state.collapsed;
        const toggleTitle = collapsed ? 'Развернуть панель' : 'Свернуть панель';
        panelEl.classList.toggle('is-collapsed', collapsed);
        panelEl.innerHTML = '<div class="oa-panel-head">'
            + '<span class="oa-panel-title" data-oa-toggle>Анализ состояния объекта</span>'
            + '<span class="oa-panel-actions">'
            + '<button type="button" class="oa-expand" data-oa-slice title="Общий срез по всем объектам" aria-label="Общий срез по всем объектам">'
            + '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M9 2h5v5M14 2 8 8M7 3H3v10h10V9" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>'
            + '</button>'
            + '<button type="button" class="oa-panel-toggle' + (collapsed ? '' : ' is-expanded') + '" data-oa-toggle'
            + ' aria-expanded="' + (collapsed ? 'false' : 'true') + '" title="' + toggleTitle + '" aria-label="' + toggleTitle + '">'
            + '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M6 3.5 10.5 8 6 12.5"></path></svg>'
            + '</button></span></div>'
            + (collapsed ? '' : '<div class="oa-grid-scroll"><div class="oa-grid" style="grid-template-columns:' + UI.gridTemplate(false) + '">'
                + UI.headerHtml(false)
                + UI.rowHtml(data.project, ctx, false)
                + '</div></div>');
        panelEl.hidden = false;
    }

    async function load(force) {
        if (global.ChecklistAuth && global.ChecklistAuth.readOnly) return;
        if (!force && Date.now() - state.loadedAt < REFRESH_PAUSE_MS) return;
        if (state.loading) return state.loading;
        // An open editor is not torn down under the hands.
        if (doc.querySelector('.oa-popover')) return;
        state.loading = (async function () {
            try {
                const result = await api.panel(dialogId);
                state.data = result;
                state.loadedAt = Date.now();
                render();
            } catch (error) {
                console.log('object analysis load skipped:', error);
            } finally {
                state.loading = null;
            }
        })();
        return state.loading;
    }

    panelEl.addEventListener('click', function (event) {
        if (event.target.closest('[data-oa-slice]')) {
            event.preventDefault();
            global.open(pageUrl('object-analysis'), '_blank');
            return;
        }
        if (event.target.closest('[data-oa-toggle]')) {
            event.preventDefault();
            if (!state.collapsed && typeof UI.closeEditor === 'function') UI.closeEditor();
            state.collapsed = !state.collapsed;
            render();
        }
    });
    UI.bindGrid(panelEl, ctx);

    // After «Сохранить»: ИД, ТУ and Граданализ as saved, folded or not. A
    // load already running may be older than the save; an open editor of
    // the panel is not torn down, the panel is read once it is closed.
    let refreshTimer = 0;
    function refresh() {
        global.clearTimeout(refreshTimer);
        if (doc.querySelector('.oa-popover')) {
            refreshTimer = global.setTimeout(refresh, 1000);
            return Promise.resolve(null);
        }
        return Promise.resolve(state.loading).catch(() => null).then(function () {
            return load(true);
        });
    }

    // The risk window and the slice change the same data: read it again
    // when the checklist gets the focus back.
    global.addEventListener('focus', function () { load(false); });
    doc.addEventListener('visibilitychange', function () {
        if (doc.visibilityState === 'visible') load(false);
    });

    Promise.resolve(
        typeof fetchCurrentUserIfPossible === 'function' ? fetchCurrentUserIfPossible() : null
    ).catch(() => null).then(function () { load(true); });

    global.ChecklistPopupObjectAnalysis = Object.freeze({
        load,
        refresh,
        render,
        state
    });
})(window);
