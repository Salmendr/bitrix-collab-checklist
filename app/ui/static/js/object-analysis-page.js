(function (global) {
    'use strict';

    // The slice of all objects: the project the window was opened from goes
    // first with a green glow, the others are grouped by the contract
    // status. Everyone who sees it sees all the projects; a project is
    // edited by its leaders and GIPs.

    const doc = global.document;
    const UI = global.ObjectAnalysisUI;
    const bootstrap = global.OBJECT_ANALYSIS_BOOTSTRAP || {};
    const sliceEl = doc.getElementById('oaSlice');
    const noteEl = doc.getElementById('oaPageNote');
    if (!UI || !sliceEl) return;

    const REFRESH_PAUSE_MS = 5000;
    const state = { data: null, byId: {}, loadedAt: 0, loading: null };

    function actor() {
        const checked = global.ChecklistAuth && global.ChecklistAuth.user;
        if (checked && checked.id) return { userId: String(checked.id), userName: String(checked.name || '') };
        return { userId: String(bootstrap.userId || ''), userName: String(bootstrap.userName || '') };
    }

    const appBase = global.location.origin + String(bootstrap.appBase || '');
    const api = UI.createApi({ baseUrl: appBase, actor });

    function pageUrl(path, focus) {
        const who = actor();
        const url = new URL(appBase + '/' + path);
        url.searchParams.set('dialogId', bootstrap.dialogId);
        if (focus && focus !== bootstrap.dialogId) url.searchParams.set('focus', focus);
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
        project(id) { return state.byId[id] || null; },
        onProject(project) {
            if (!state.data || !project) return;
            const index = state.data.projects.findIndex(entry => entry.dialogId === project.dialogId);
            if (index < 0) return;
            const old = state.data.projects[index];
            // The place in the list stays until the next reading.
            state.data.projects[index] = Object.assign({}, project, { current: old.current, group: old.group });
            render();
        },
        onOptions(options) {
            if (!state.data || !options) return;
            state.data.options = options;
            render();
        },
        openRisks(project) {
            // The risk window opens at the table of the clicked project.
            global.open(pageUrl('object-analysis/risks', project.dialogId), '_blank');
        },
        findAnchor(id, key, half) {
            return UI.findAnchor(sliceEl, id, key, half);
        }
    };

    function render() {
        const data = state.data;
        if (!data) return;
        state.byId = {};
        data.projects.forEach(project => { state.byId[project.dialogId] = project; });
        const parts = [UI.headerHtml(true)];
        let group = null;
        data.projects.forEach(project => {
            if (!project.current && project.group !== group) {
                group = project.group;
                const count = data.projects.filter(entry => !entry.current && entry.group === group).length;
                parts.push('<div class="oa-group-row"><span class="oa-group-label">' + UI.esc(group)
                    + ' <span class="oa-group-count">' + count + '</span></span></div>');
            }
            parts.push(UI.rowHtml(project, ctx, true));
        });
        sliceEl.style.gridTemplateColumns = UI.gridTemplate(true);
        sliceEl.innerHTML = parts.join('');
        sliceEl.setAttribute('aria-busy', 'false');
        if (noteEl) noteEl.textContent = 'Объектов: ' + data.projects.length;
    }

    async function load(force) {
        if (!force && Date.now() - state.loadedAt < REFRESH_PAUSE_MS) return;
        if (state.loading || doc.querySelector('.oa-popover')) return;
        state.loading = (async function () {
            try {
                state.data = await api.slice(bootstrap.dialogId);
                state.loadedAt = Date.now();
                render();
            } catch (error) {
                sliceEl.innerHTML = '<div class="oa-loading is-error">' + UI.esc(error.message || 'Не удалось загрузить объекты') + '</div>';
            } finally {
                state.loading = null;
            }
        })();
        return state.loading;
    }

    UI.bindGrid(sliceEl, ctx);
    const risksButton = doc.getElementById('oaOpenRisks');
    if (risksButton) {
        risksButton.addEventListener('click', () => global.open(pageUrl('object-analysis/risks', ''), '_blank'));
    }
    global.addEventListener('focus', () => load(false));
    doc.addEventListener('visibilitychange', () => {
        if (doc.visibilityState === 'visible') load(false);
    });
    load(true);
})(window);
