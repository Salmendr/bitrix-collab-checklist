(function (global) {
    'use strict';

    // Project stages (tabs above the checklist list) and the GIP field.
    // Stage 1 is the chat dialog itself; stage N ≥ 2 has its own dialog id.
    // Adding stages: administrators and the project GIPs. Editing the GIP
    // list: administrators only. The server checks both again.
    // When the project has a Bitrix24 object, the field shows the people of
    // the object of this checklist («Главный Концептолог» / «Главный
    // Дизайнер» in Концепция and Дизайн) and is not edited here.

    const doc = global.document;
    const phaseBarEl = doc.getElementById('projectPhaseBar');
    const gipControlEl = doc.getElementById('projectGipControl');
    if (!phaseBarEl && !gipControlEl) return;

    const bootstrapPhases = (
        popupBootstrap.phases && typeof popupBootstrap.phases === 'object'
    ) ? popupBootstrap.phases : {};

    const state = {
        baseDialogId: String(bootstrapPhases.baseDialogId || dialogId),
        current: Number(bootstrapPhases.current || 1),
        phases: Array.isArray(bootstrapPhases.phases) ? bootstrapPhases.phases : [],
        gips: Array.isArray(bootstrapPhases.gips) ? bootstrapPhases.gips : [],
        adminUserIds: Array.isArray(bootstrapPhases.adminUserIds)
            ? bootstrapPhases.adminUserIds.map(String)
            : [],
        managerUserIds: Array.isArray(bootstrapPhases.managerUserIds)
            ? bootstrapPhases.managerUserIds.map(String)
            : null,
        object: (
            popupBootstrap.projectObject
            && popupBootstrap.projectObject.byKey
            && typeof popupBootstrap.projectObject.byKey === 'object'
        ) ? (popupBootstrap.projectObject.byKey[String(currentChecklistKey || 'id')] || null) : null,
        busy: false,
        gipPickerOpen: false
    };
    let gipPicker = null;

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function escHtml(value) {
        return text(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function currentUserId() {
        return text(currentEditor && currentEditor.id);
    }

    function isAdmin() {
        return state.adminUserIds.includes(currentUserId());
    }

    function canManagePhases() {
        const userId = currentUserId();
        if (!userId) return false;
        if (isAdmin()) return true;
        if (state.managerUserIds) return state.managerUserIds.includes(userId);
        return state.gips.some(gip => text(gip.userId) === userId);
    }

    function objectDriven() {
        return !!(state.object && state.object.objectDriven);
    }

    function phaseUrl(phaseDialogId) {
        const url = new URL(global.location.href);
        url.searchParams.set('dialogId', phaseDialogId);
        url.searchParams.set('checklistKey', String(currentChecklistKey || 'id'));
        url.searchParams.delete('focusItemId');
        return url.href;
    }

    async function postJson(path, body) {
        const response = await fetch(appUrl(path), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || !result.ok) {
            throw new Error(result && result.error || 'Операция не выполнена');
        }
        return result;
    }

    function applyServerState(result) {
        if (!result) return;
        if (Array.isArray(result.phases)) state.phases = result.phases;
        if (Array.isArray(result.gips)) state.gips = result.gips;
        if (Array.isArray(result.managerUserIds)) state.managerUserIds = result.managerUserIds.map(String);
        renderPhaseBar();
        renderGipControl();
    }

    // Saves the current changes (and sends the chat summary) like the Save
    // button, then continues in this window instead of closing it.
    async function saveThen(reason, continuation) {
        const hasSession = (
            typeof getActiveEditSessionId === 'function'
            && !!getActiveEditSessionId()
        );
        if (hasSession && typeof finalizePopupSession === 'function') {
            const finished = await finalizePopupSession(true, {
                reason,
                afterFinalize: continuation
            });
            return finished;
        }
        await continuation();
        return true;
    }

    function showOverlay(message) {
        let overlay = doc.getElementById('projectPhaseOverlay');
        if (!overlay) {
            overlay = doc.createElement('div');
            overlay.id = 'projectPhaseOverlay';
            overlay.className = 'project-phase-overlay';
            overlay.innerHTML = '<div class="project-phase-overlay-box" role="status" aria-live="polite"></div>';
            doc.body.appendChild(overlay);
        }
        overlay.querySelector('.project-phase-overlay-box').textContent = message;
        overlay.hidden = false;
    }

    async function switchPhase(phase) {
        if (state.busy || !phase || Number(phase.no) === state.current) return;
        state.busy = true;
        renderPhaseBar();
        const target = phaseUrl(text(phase.dialogId));
        try {
            const finished = await saveThen('phase_switch', async function () {
                showOverlay('Открываем ' + text(phase.name) + '…');
                global.location.assign(target);
            });
            if (!finished) {
                state.busy = false;
                renderPhaseBar();
            }
        } catch (error) {
            state.busy = false;
            renderPhaseBar();
            setSaveState('error', error && error.message ? error.message : 'Не удалось открыть этап');
        }
    }

    async function addPhase() {
        if (state.busy || !canManagePhases()) return;
        const nextNo = state.phases.length
            ? Math.max(...state.phases.map(phase => Number(phase.no) || 0)) + 1
            : 2;
        const firstTime = !state.phases.length;
        const message = firstTime
            ? (
                'Будет создан «Этап 2» — новый пустой набор чек-листов проекта.\n\n'
                + 'Текущие чек-листы станут «Этапом 1»: папки 00_Исходные данные, '
                + '02_Выдача документации и 03_Архив на Яндекс.Диске будут перенесены '
                + 'в папку «Этап 1». Это действие нельзя отменить.\n\n'
                + 'Текущие изменения будут сохранены. Продолжить?'
            )
            : (
                'Будет создан «Этап ' + nextNo + '» — новый пустой набор чек-листов проекта.\n\n'
                + 'Текущие изменения будут сохранены. Продолжить?'
            );
        if (!global.confirm(message)) return;

        state.busy = true;
        renderPhaseBar();
        try {
            const finished = await saveThen('phase_add', async function () {
                showOverlay(
                    firstTime
                        ? 'Создаём этапы и переносим папки Этапа 1 на Яндекс.Диске… Не закрывайте окно.'
                        : 'Создаём Этап ' + nextNo + '… Не закрывайте окно.'
                );
                const identity = getCurrentEditorIdentity();
                let result = null;
                try {
                    result = await postJson('api/checklist/project-phases/add', {
                        dialogId,
                        actingUserId: identity.userId,
                        actingUserName: identity.userName
                    });
                } catch (error) {
                    global.alert(
                        (error && error.message ? error.message : 'Не удалось добавить этап')
                    );
                    // The edit session was saved: start a fresh one here.
                    global.location.reload();
                    return;
                }
                const phase = result.phase || {};
                global.location.assign(phaseUrl(text(phase.dialogId) || dialogId));
            });
            if (!finished) {
                state.busy = false;
                renderPhaseBar();
            }
        } catch (error) {
            state.busy = false;
            renderPhaseBar();
            setSaveState('error', error && error.message ? error.message : 'Не удалось добавить этап');
        }
    }

    function renderPhaseBar() {
        if (!phaseBarEl) return;
        const manage = canManagePhases();
        if (!state.phases.length && !manage) {
            phaseBarEl.hidden = true;
            phaseBarEl.innerHTML = '';
            return;
        }
        const tabs = state.phases.map(phase => {
            const active = Number(phase.no) === state.current;
            return `<button type="button" class="project-phase-tab${active ? ' active' : ''}"`
                + ` data-phase-no="${escHtml(phase.no)}"`
                + (active ? ' aria-current="page"' : '')
                + (state.busy ? ' disabled' : '')
                + `>${escHtml(phase.name)}</button>`;
        }).join('');
        const addButton = manage
            ? `<button type="button" class="project-phase-add" data-role="add-project-phase"${state.busy ? ' disabled' : ''}>+ Добавить этап</button>`
            : '';
        phaseBarEl.innerHTML = `<div class="project-phase-tabs" role="tablist" aria-label="Этапы проекта">${tabs}${addButton}</div>`;
        phaseBarEl.hidden = false;
        phaseBarEl.querySelectorAll('[data-phase-no]').forEach(button => {
            button.addEventListener('click', () => {
                const phase = state.phases.find(entry => String(entry.no) === button.dataset.phaseNo);
                switchPhase(phase);
            });
        });
        const add = phaseBarEl.querySelector('[data-role="add-project-phase"]');
        if (add) add.addEventListener('click', addPhase);
    }

    async function removeGip(userId) {
        const gip = state.gips.find(entry => text(entry.userId) === text(userId));
        if (!gip || !global.confirm('Убрать ' + (gip.name || ('ID ' + gip.userId)) + ' из ГИП проекта?')) return;
        try {
            const identity = getCurrentEditorIdentity();
            applyServerState(await postJson('api/checklist/project-gips/remove', {
                dialogId,
                userId: gip.userId,
                actingUserId: identity.userId
            }));
        } catch (error) {
            global.alert(error && error.message ? error.message : 'Не удалось изменить ГИП');
        }
    }

    async function addGip(user) {
        if (!user || !text(user.userId)) return;
        try {
            const identity = getCurrentEditorIdentity();
            applyServerState(await postJson('api/checklist/project-gips/add', {
                dialogId,
                userId: user.userId,
                userName: user.name,
                actingUserId: identity.userId
            }));
            state.gipPickerOpen = false;
            renderGipControl();
        } catch (error) {
            global.alert(error && error.message ? error.message : 'Не удалось назначить ГИП');
        }
    }

    function renderObjectPeople() {
        const object = state.object || {};
        const people = Array.isArray(object.people) ? object.people : [];
        const chips = people.map(person => {
            const roles = Array.isArray(person.roles) ? person.roles.join(', ') : '';
            const title = roles + (person.objectId ? ' · объект #' + person.objectId : '');
            return `<span class="project-gip-chip" title="${escHtml(title)}">`
                + `<span>${escHtml(person.name || ('ID ' + person.userId))}</span>`
                + '</span>';
        }).join('');
        const empty = people.length ? '' : '<span class="project-gip-empty">не назначен</span>';
        gipControlEl.innerHTML = `
            <span class="project-gip-label">${escHtml(object.peopleLabel || 'ГИП')}:</span>
            <span class="project-gip-list">${chips}${empty}</span>
        `;
        gipControlEl.hidden = false;
    }

    function renderGipControl() {
        if (!gipControlEl) return;
        if (objectDriven()) {
            if (gipPicker && typeof gipPicker.destroy === 'function') {
                gipPicker.destroy();
                gipPicker = null;
            }
            state.gipPickerOpen = false;
            renderObjectPeople();
            return;
        }
        const admin = isAdmin();
        const chips = state.gips.map(gip => (
            `<span class="project-gip-chip" title="ID ${escHtml(gip.userId)}">`
            + `<span>${escHtml(gip.name || ('ID ' + gip.userId))}</span>`
            + (admin ? `<button type="button" class="project-gip-remove" data-gip-remove="${escHtml(gip.userId)}" title="Убрать из ГИП" aria-label="Убрать ${escHtml(gip.name)} из ГИП">×</button>` : '')
            + '</span>'
        )).join('');
        const empty = state.gips.length ? '' : '<span class="project-gip-empty">не назначен</span>';
        const addButton = admin && !state.gipPickerOpen
            ? '<button type="button" class="project-gip-add" data-role="gip-add">+ Добавить</button>'
            : '';
        const picker = admin && state.gipPickerOpen ? `
            <div class="project-gip-picker bitrix-user-picker">
                <div class="bitrix-user-picker-search-row">
                    <input type="text" id="projectGipLookup" placeholder="Начните вводить ФИО сотрудника" autocomplete="off">
                    <button type="button" class="project-gip-cancel" data-role="gip-cancel" title="Закрыть">×</button>
                </div>
                <div id="projectGipResults" class="bitrix-user-picker-results" hidden></div>
                <div id="projectGipHint" class="bitrix-user-picker-hint"></div>
                <input type="hidden" id="projectGipUserId">
                <input type="hidden" id="projectGipUserName">
            </div>
        ` : '';
        gipControlEl.innerHTML = `
            <span class="project-gip-label">ГИП:</span>
            <span class="project-gip-list">${chips}${empty}</span>
            ${addButton}
            ${picker}
        `;
        gipControlEl.hidden = false;
        gipControlEl.querySelectorAll('[data-gip-remove]').forEach(button => {
            button.addEventListener('click', () => removeGip(button.dataset.gipRemove));
        });
        const add = gipControlEl.querySelector('[data-role="gip-add"]');
        if (add) {
            add.addEventListener('click', () => {
                state.gipPickerOpen = true;
                renderGipControl();
            });
        }
        const cancel = gipControlEl.querySelector('[data-role="gip-cancel"]');
        if (cancel) {
            cancel.addEventListener('click', () => {
                state.gipPickerOpen = false;
                renderGipControl();
            });
        }
        if (gipPicker && typeof gipPicker.destroy === 'function') {
            gipPicker.destroy();
            gipPicker = null;
        }
        if (admin && state.gipPickerOpen && global.ChecklistBitrixUserPicker) {
            const input = gipControlEl.querySelector('#projectGipLookup');
            gipPicker = global.ChecklistBitrixUserPicker.create({
                input,
                results: gipControlEl.querySelector('#projectGipResults'),
                hint: gipControlEl.querySelector('#projectGipHint'),
                idInput: gipControlEl.querySelector('#projectGipUserId'),
                nameInput: gipControlEl.querySelector('#projectGipUserName'),
                apiUrl: appUrl('api/checklist/bitrix-users'),
                onSelect: addGip
            });
            input.focus();
        }
    }

    function renderAllPhaseControls() {
        renderPhaseBar();
        renderGipControl();
    }

    renderAllPhaseControls();
    // The Bitrix user arrives asynchronously; rights depend on it.
    Promise.resolve(
        typeof fetchCurrentUserIfPossible === 'function'
            ? fetchCurrentUserIfPossible()
            : null
    ).catch(() => null).then(renderAllPhaseControls);

    // The Bitrix24 object data arrived (popup-project-object.js).
    function applyObject(view, managerUserIds) {
        state.object = view && typeof view === 'object' ? view : null;
        if (Array.isArray(managerUserIds)) state.managerUserIds = managerUserIds.map(String);
        renderAllPhaseControls();
    }

    global.ChecklistPopupProjectPhases = Object.freeze({
        render: renderAllPhaseControls,
        applyObject,
        canManagePhases,
        phaseUrl,
        state
    });
})(window);
