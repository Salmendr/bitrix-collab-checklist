// Исполнители пунктов и разделов (assignments.py on the server).
//
// The checklist window: stickers of the assignees under the name of an item
// (three, then «…»), a yellow button after «Загрузить» for those who assign
// and for the assignee himself («Передать»), a button and stickers in the
// header of a section. The folder page of an item: all its stickers in the
// head of the page, the same button and the journal of assignments and
// execution. A change goes to the draft of the edit session: «Сохранить»
// keeps it and makes the tasks in Bitrix, «Отмена» drops it.
(function (global) {
    'use strict';

    const doc = global.document;
    const ROW_STICKERS = 3;
    const STALE_MS = 30000;
    const PERSON_ICON = '<svg class="checklist-action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"'
        + ' stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        + '<circle cx="12" cy="8" r="3.6"></circle><path d="M5 20.2c.7-3.7 3.5-5.8 7-5.8s6.3 2.1 7 5.8"></path></svg>';

    const folderBoot = global.CHECKLIST_FOLDER_BOOTSTRAP || null;
    const onFolderPage = !!folderBoot;

    const state = {
        data: null,
        key: '',
        loadedAt: 0,
        loading: null,
        modal: null,
        renderPending: false
    };

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function esc(value) {
        return text(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    // ---- the page this script runs in ----

    const env = onFolderPage ? {
        dialogId: () => text(folderBoot.dialogId),
        key: () => text(folderBoot.checklistKey),
        api: path => {
            const base = text(folderBoot.assignmentsApiUrl) || '';
            return base + path;
        },
        usersApi: () => text(folderBoot.bitrixUsersApiUrl),
        identity: () => {
            const actor = global.ChecklistFolderCore ? global.ChecklistFolderCore.getActor() : {};
            return { userId: text(actor && actor.id), userName: text(actor && actor.name) || 'Пользователь' };
        },
        sessionId: () => (global.ChecklistFolderCore ? text(global.ChecklistFolderCore.getSessionId()) : ''),
        requireSession: async () => global.ChecklistFolderCore.requireSession('назначение исполнителя'),
        readOnly: () => !!(global.ChecklistFolderCore && global.ChecklistFolderCore.readOnly)
            || !!(global.ChecklistAuth && global.ChecklistAuth.readOnly),
        canEdit: () => !!(global.ChecklistFolderCore && global.ChecklistFolderCore.getSessionId()),
        items: () => [],
        rerender: () => renderFolder(),
        afterChange: () => notifyOpener()
    } : {
        dialogId: () => text(typeof dialogId !== 'undefined' ? dialogId : ''),
        key: () => text(typeof currentChecklistKey !== 'undefined' ? currentChecklistKey : ''),
        api: path => (typeof appUrl === 'function' ? appUrl(path.replace(/^\//, '')) : path.replace(/^\//, '')),
        usersApi: () => (typeof popupApi !== 'undefined' && popupApi && typeof popupApi.url === 'function'
            ? popupApi.url('api/checklist/bitrix-users')
            : (typeof appUrl === 'function' ? appUrl('api/checklist/bitrix-users') : 'api/checklist/bitrix-users')),
        identity: () => (typeof getCurrentEditorIdentity === 'function'
            ? getCurrentEditorIdentity() : { userId: '', userName: 'Пользователь' }),
        sessionId: () => {
            const api = global.ChecklistPopupEditSession;
            return text(api && typeof api.getSessionId === 'function' ? api.getSessionId() : '');
        },
        requireSession: async () => requireEditingSession('назначение исполнителя'),
        readOnly: () => (typeof isReadOnlyWindow === 'function' ? isReadOnlyWindow() : false),
        canEdit: () => (typeof isEditingAllowed === 'function' ? isEditingAllowed() : false),
        items: () => (typeof items !== 'undefined' && Array.isArray(items) ? items : []),
        rerender: () => rerenderPopup(),
        afterChange: () => {
            if (typeof updateSaveStateBySession === 'function') {
                try { updateSaveStateBySession(); } catch (error) { /* the window is closing */ }
            }
        }
    };

    function me() {
        return text(env.identity().userId);
    }

    function supported() {
        return !!(state.data && state.data.supported && state.key === env.key());
    }

    function rights() {
        return (state.data && state.data.rights) || {};
    }

    function targetUsers(kind, id) {
        const target = ((state.data && state.data.targets) || []).find(t => t.kind === kind && t.id === text(id));
        return target ? target.users : null;
    }

    function itemOf(itemId) {
        return env.items().find(item => text(item && item.id) === text(itemId)) || null;
    }

    function parentOf(itemId) {
        if (onFolderPage) {
            return text(itemId) === text(folderBoot.itemId) ? text(folderBoot.parentItemId) : '';
        }
        return text((itemOf(itemId) || {}).parentItemId);
    }

    // The assignees of an item: its own list, or those of its item.
    function effective(itemId) {
        const own = targetUsers('item', itemId);
        if (own) return { users: own, inherited: false, own: true };
        const parent = parentOf(itemId);
        const inherited = parent ? targetUsers('item', parent) : null;
        return { users: inherited || [], inherited: !!(inherited && inherited.length), own: false };
    }

    function isNotRequired(itemId) {
        const item = itemOf(itemId);
        const status = item && typeof normalizeStatus === 'function' ? normalizeStatus(item.status) : text(item && item.status);
        return status === 'Не требуется';
    }

    function chatUrl(userId) {
        const portal = text(state.data && state.data.portalUrl);
        return portal ? portal + '/online/?IM_DIALOG=' + encodeURIComponent(text(userId)) : '';
    }

    function shortDate(value) {
        const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(text(value));
        return match ? match[3] + '.' + match[2] + '.' + match[1] : '';
    }

    function moment(value) {
        const date = new Date(text(value));
        if (Number.isNaN(date.getTime())) return '';
        return date.toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
    }

    function stickerTitle(user, inherited) {
        const parts = [text(user.name) + ' — открыть чат'];
        if (user.assignedByName) parts.push('назначил ' + user.assignedByName + (user.assignedAt ? ' ' + moment(user.assignedAt) : ''));
        if (user.deadline) parts.push('срок до ' + shortDate(user.deadline));
        if (inherited) parts.push('исполнитель пункта');
        return parts.join(' · ');
    }

    function stickerHtml(user, inherited) {
        const url = chatUrl(user.userId);
        const cls = 'assignee-sticker' + (inherited ? ' is-inherited' : '');
        const label = esc(user.name || ('ID ' + user.userId));
        return url
            ? '<a class="' + cls + '" href="' + esc(url) + '" target="_blank" rel="noopener" data-assignee-chat="' + esc(user.userId) + '"'
                + ' title="' + esc(stickerTitle(user, inherited)) + '">' + label + '</a>'
            : '<span class="' + cls + '" title="' + esc(stickerTitle(user, inherited)) + '">' + label + '</span>';
    }

    function stickersHtml(users, inherited, limit, openAttrs) {
        if (!users || !users.length) return '';
        const shown = limit ? users.slice(0, limit) : users;
        const rest = limit ? users.length - shown.length : 0;
        return shown.map(user => stickerHtml(user, inherited)).join('')
            + (rest > 0 ? '<button type="button" class="assignee-more" ' + openAttrs
                + ' title="' + esc('Ещё: ' + users.slice(limit).map(u => u.name).join(', ')) + '">…</button>' : '');
    }

    // ---- the checklist window ----

    function canAssign() {
        return !!rights().assign && !env.readOnly();
    }

    function mayOpen(kind, id) {
        if (!supported() || env.readOnly()) return false;
        if (canAssign()) return kind === 'group' || !isNotRequired(id);
        const users = kind === 'group' ? (targetUsers('group', id) || []) : effective(id).users;
        return users.some(user => user.userId === me());
    }

    function openAttrs(kind, id) {
        return 'data-assign-open="' + esc(kind) + '" data-assign-target="' + esc(id) + '"';
    }

    function itemStickers(item) {
        if (!supported() || !item) return '';
        const info = effective(item.id);
        if (!info.users.length) return '';
        return '<div class="assignee-stickers" data-assignees-item="' + esc(item.id) + '">'
            + stickersHtml(info.users, info.inherited, ROW_STICKERS, openAttrs('item', item.id)) + '</div>';
    }

    function itemButton(item) {
        if (!item || !mayOpen('item', item.id)) return '';
        const title = canAssign() ? 'Назначить исполнителя' : 'Передать пункт другому сотруднику';
        return '<button class="doc-icon-btn checklist-action-button checklist-action-button-assign" type="button"'
            + ' data-role="assign-open" ' + openAttrs('item', item.id)
            + ' title="' + esc(title) + '" aria-label="' + esc(title) + '"' + (env.canEdit() ? '' : ' disabled') + '>'
            + PERSON_ICON + '</button>';
    }

    function notRequiredGroup() {
        return typeof getCurrentNotRequiredGroupId === 'function' ? Number(getCurrentNotRequiredGroupId() || 0) : 0;
    }

    function sectionHtml(group) {
        if (!supported() || !group || Number(group.id) === notRequiredGroup()) return '';
        const users = targetUsers('group', group.id) || [];
        const button = mayOpen('group', group.id)
            ? '<button class="assignee-section-button" type="button" data-role="assign-open" ' + openAttrs('group', group.id)
                + ' title="' + esc(canAssign() ? 'Назначить исполнителя на раздел' : 'Передать раздел') + '"'
                + (env.canEdit() ? '' : ' disabled') + '>' + PERSON_ICON + '</button>'
            : '';
        if (!button && !users.length) return '';
        return '<span class="assignee-section" data-assignees-group="' + esc(group.id) + '">' + button
            + stickersHtml(users, false, ROW_STICKERS, openAttrs('group', group.id)) + '</span>';
    }

    function typing() {
        const active = doc.activeElement;
        if (!active || active === doc.body) return false;
        if (active.closest && active.closest('.assign-modal')) return false;
        return /^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName) || active.isContentEditable;
    }

    function rerenderPopup() {
        if (typing()) {
            state.renderPending = true;
            return;
        }
        state.renderPending = false;
        if (typeof renderAll === 'function') renderAll();
    }

    if (!onFolderPage) {
        doc.addEventListener('focusout', function () {
            if (state.renderPending) global.setTimeout(function () { if (state.renderPending) rerenderPopup(); }, 50);
        });
    }

    // ---- loading ----

    function snapshot(data) {
        return JSON.stringify([data && data.targets, data && data.rights, data && data.hasDraft, data && data.tasks]);
    }

    async function load() {
        const key = env.key();
        const dialog = env.dialogId();
        if (!dialog || !key) return null;
        if (key === 'ga') {
            // Граданализ has its architects instead: nothing to ask.
            state.data = { ok: true, supported: false, targets: [], rights: {} };
            state.key = key;
            state.loadedAt = Date.now();
            return state.data;
        }
        if (state.loading && state.loadingKey === key) return state.loading;
        const params = new URLSearchParams({ dialogId: dialog, checklistKey: key, userId: me(), sessionId: env.sessionId() });
        state.loadingKey = key;
        state.loading = (async function () {
            try {
                const response = await global.fetch(env.api('api/assignments') + '?' + params.toString(), { cache: 'no-store' });
                const data = await response.json();
                if (!response.ok || data.ok === false) throw new Error(data.error || ('HTTP ' + response.status));
                if (key !== env.key()) return null;
                const changed = snapshot(data) !== snapshot(state.data) || state.key !== key;
                state.data = data;
                state.key = key;
                state.loadedAt = Date.now();
                if (changed) env.rerender();
                if (state.modal) renderModal();
                env.afterChange();
                return data;
            } catch (error) {
                console.log('assignments load failed:', error);
                return null;
            } finally {
                state.loading = null;
            }
        })();
        return state.loading;
    }

    // Called by every render of the window: a new checklist or an old state
    // is read again (the render follows when something changed).
    function refresh() {
        if (env.key() !== state.key || Date.now() - state.loadedAt > STALE_MS) load();
    }

    function hasDraft() {
        return !!(state.data && state.data.hasDraft && state.key === env.key());
    }

    // ---- changes ----

    async function send(body) {
        const sessionId = await env.requireSession();
        const who = env.identity();
        const response = await global.fetch(env.api('api/assignments/change'), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(Object.assign({
                dialogId: env.dialogId(),
                checklistKey: env.key(),
                sessionId,
                actingUserId: who.userId,
                actingUserName: who.userName
            }, body))
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || data.ok === false) throw new Error(data.error || 'Не удалось изменить исполнителей');
        state.data = data;
        state.key = env.key();
        state.loadedAt = Date.now();
        env.rerender();
        env.afterChange();
        if (state.modal) renderModal();
        return data;
    }

    function currentList(kind, id) {
        return kind === 'group' ? (targetUsers('group', id) || []) : effective(id).users;
    }

    async function guarded(action) {
        if (!state.modal) return;
        state.modal.busy = true;
        state.modal.error = '';
        renderModal();
        try {
            await action();
        } catch (error) {
            if (state.modal) state.modal.error = error && error.message ? error.message : 'Ошибка';
        } finally {
            if (state.modal) {
                state.modal.busy = false;
                renderModal();
            }
        }
    }

    function addUser(user) {
        const modal = state.modal;
        if (!modal || !user || !text(user.userId)) return;
        if (modal.mode === 'delegate') {
            if (!modal.receivers.some(r => r.userId === text(user.userId)) && text(user.userId) !== modal.from) {
                modal.receivers.push({ userId: text(user.userId), name: text(user.name) });
            }
            renderModal();
            return;
        }
        const list = currentList(modal.kind, modal.id);
        if (list.some(u => u.userId === text(user.userId))) {
            modal.error = 'Этот сотрудник уже назначен';
            renderModal();
            return;
        }
        guarded(() => send({
            op: 'set', kind: modal.kind, targetId: modal.id, deadline: modal.deadline,
            users: list.map(u => ({ userId: u.userId, name: u.name })).concat([{ userId: text(user.userId), name: text(user.name) }])
        }));
    }

    function removeUser(userId) {
        const modal = state.modal;
        const list = currentList(modal.kind, modal.id);
        const user = list.find(u => u.userId === userId);
        if (!user || !global.confirm('Снять ' + user.name + ' с ' + (modal.kind === 'group' ? 'раздела' : 'пункта') + '? Его задача завершится после сохранения.')) return;
        guarded(() => send({
            op: 'set', kind: modal.kind, targetId: modal.id,
            users: list.filter(u => u.userId !== userId).map(u => ({ userId: u.userId, name: u.name }))
        }));
    }

    function setDeadline(userId, value) {
        const modal = state.modal;
        const list = currentList(modal.kind, modal.id);
        guarded(() => send({
            op: 'set', kind: modal.kind, targetId: modal.id,
            users: list.map(u => (u.userId === userId
                ? { userId: u.userId, name: u.name, deadline: text(value) }
                : { userId: u.userId, name: u.name }))
        }));
    }

    function delegate() {
        const modal = state.modal;
        if (!modal.receivers.length) {
            modal.error = 'Выберите, кому передать';
            renderModal();
            return;
        }
        guarded(async () => {
            await send({ op: 'delegate', kind: modal.kind, targetId: modal.id, fromUserId: modal.from,
                         users: modal.receivers, deadline: modal.deadline });
            if (state.modal) {
                state.modal.mode = 'list';
                state.modal.receivers = [];
                state.modal.from = '';
            }
        });
    }

    function inherit() {
        const modal = state.modal;
        guarded(() => send({ op: 'inherit', kind: modal.kind, targetId: modal.id }));
    }

    // ---- the window of the assignees ----

    function targetName(kind, id) {
        if (kind === 'group') {
            const list = typeof groups !== 'undefined' && Array.isArray(groups) ? groups : [];
            const group = list.find(g => String(g.id) === String(id));
            return 'Раздел «' + text(group && (group.title || group.name) || 'Раздел') + '»';
        }
        if (onFolderPage) return 'Пункт «' + text(folderBoot.itemName) + '»';
        const item = itemOf(id);
        return (parentOf(id) ? 'Подпункт «' : 'Пункт «') + text(item && item.name) + '»';
    }

    function open(kind, id) {
        closeModal();
        state.modal = {
            kind, id: text(id), mode: 'list', from: '', receivers: [], deadline: '', error: '', busy: false,
            editDeadline: ''
        };
        renderModal();
        if (canAssign() && env.canEdit()) mountPicker();
    }

    function closeModal() {
        if (state.modal && state.modal.picker) state.modal.picker.destroy();
        const node = doc.querySelector('[data-assign-modal]');
        if (node) node.remove();
        state.modal = null;
    }

    function chipHtml(user, modal, editable) {
        const mine = user.userId === me();
        const canDelegate = editable && (canAssign() || mine);
        const deadline = user.deadline ? 'срок до ' + shortDate(user.deadline) : 'без срока';
        const url = chatUrl(user.userId);
        const name = url
            ? '<a href="' + esc(url) + '" target="_blank" rel="noopener" title="Открыть чат">' + esc(user.name) + '</a>'
            : '<span>' + esc(user.name) + '</span>';
        const meta = editable && canAssign()
            ? (modal.editDeadline === user.userId
                ? '<input type="date" class="assign-chip-date" data-assign-date="' + esc(user.userId) + '" value="' + esc(user.deadline) + '">'
                : '<button type="button" class="assign-chip-meta" data-assign-edit-date="' + esc(user.userId) + '" title="Изменить крайний срок">' + esc(deadline) + '</button>')
            : '<span class="assign-chip-meta">' + esc(deadline) + '</span>';
        return '<span class="assign-chip' + (mine ? ' is-me' : '') + '">' + name + meta
            + (canDelegate ? '<button type="button" class="assign-chip-action" data-assign-delegate="' + esc(user.userId) + '" title="Передать другому сотруднику">⇄</button>' : '')
            + (editable && canAssign() ? '<button type="button" class="assign-chip-action" data-assign-remove="' + esc(user.userId) + '" title="Снять с ' + (modal.kind === 'group' ? 'раздела' : 'пункта') + '">×</button>' : '')
            + '</span>';
    }

    function renderModal() {
        const modal = state.modal;
        if (!modal) return;
        let node = doc.querySelector('[data-assign-modal]');
        const pickerNode = node && node.querySelector('[data-assign-picker]');
        const editable = env.canEdit() && !env.readOnly() && !modal.busy;
        const info = modal.kind === 'group'
            ? { users: targetUsers('group', modal.id) || [], inherited: false, own: true }
            : effective(modal.id);
        const parent = modal.kind === 'item' ? parentOf(modal.id) : '';
        let note = '';
        if (parent && info.inherited) {
            note = '<div class="assign-note">Это исполнители пункта: по умолчанию они отвечают и за подпункт. Любое изменение здесь — свой список подпункта.</div>';
        } else if (parent && info.own && canAssign() && editable) {
            note = '<div class="assign-note">У подпункта свой список исполнителей. <button type="button" class="assign-link" data-assign-inherit>Сделать как у пункта</button></div>';
        }
        if (modal.kind === 'group') {
            note = '<div class="assign-note">Каждый получит одну задачу по разделу со списком его пунктов на момент назначения; пункты, добавленные позже, в неё не попадут.</div>';
        }
        const chips = info.users.length
            ? info.users.map(user => chipHtml(user, modal, editable)).join('')
            : '<span class="assign-empty">Исполнитель не назначен</span>';
        let panel = '';
        if (modal.mode === 'delegate') {
            const from = info.users.find(u => u.userId === modal.from) || { name: '' };
            panel = '<div class="assign-panel">'
                + '<div class="assign-panel-title">Передать ' + (modal.kind === 'group' ? 'раздел' : 'пункт') + ' от ' + esc(from.name)
                + ' — первый выбранный получит его задачу, остальные свои</div>'
                + '<div class="assign-receivers">' + (modal.receivers.map(r => '<span class="assign-chip">' + esc(r.name)
                    + '<button type="button" class="assign-chip-action" data-assign-unpick="' + esc(r.userId) + '">×</button></span>').join('')
                    || '<span class="assign-empty">Выберите сотрудников ниже</span>') + '</div>'
                + '<div class="assign-panel-actions"><button type="button" class="assign-primary" data-assign-delegate-go'
                + (modal.receivers.length && editable ? '' : ' disabled') + '>Передать</button>'
                + '<button type="button" class="assign-secondary" data-assign-delegate-cancel>Отмена</button></div>'
                + '</div>';
        }
        const showPicker = editable && (canAssign() || modal.mode === 'delegate');
        const head = '<div class="assign-head"><div><div class="assign-title">Исполнители</div>'
            + '<div class="assign-subtitle">' + esc(targetName(modal.kind, modal.id)) + '</div></div>'
            + '<button type="button" class="assign-close" data-assign-close title="Закрыть">×</button></div>';
        const body = '<div class="assign-chips">' + chips + '</div>' + note + panel
            + (showPicker ? '' : (env.canEdit() || env.readOnly() ? '' : '<div class="assign-note">Изменить исполнителей можно, когда чек-лист открыт для редактирования.</div>'))
            + (modal.error ? '<div class="assign-error">' + esc(modal.error) + '</div>' : '')
            + (modal.busy ? '<div class="assign-note">Сохраняем…</div>' : '');
        const pickerHtml = '<div class="assign-picker bitrix-user-picker" data-assign-picker>'
            + '<label class="assign-label">' + (modal.mode === 'delegate' ? 'Кому передать' : 'Добавить исполнителя') + '</label>'
            + '<div class="bitrix-user-picker-search-row"><input type="text" data-assign-lookup placeholder="Начните вводить ФИО сотрудника" autocomplete="off"></div>'
            + '<div class="bitrix-user-picker-results" data-assign-results hidden></div>'
            + '<div class="bitrix-user-picker-hint" data-assign-hint></div>'
            + '<input type="hidden" data-assign-user-id><input type="hidden" data-assign-user-name>'
            + '<label class="assign-label">Крайний срок <span class="assign-muted">(необязательно, для назначаемых сейчас)</span></label>'
            + '<input type="date" class="assign-deadline" data-assign-deadline value="' + esc(modal.deadline) + '">'
            + '</div>';
        if (!node) {
            node = doc.createElement('div');
            node.className = 'assign-backdrop';
            node.setAttribute('data-assign-modal', '');
            node.innerHTML = '<div class="assign-modal" role="dialog" aria-modal="true">' + head
                + '<div class="assign-body" data-assign-body></div>' + pickerHtml
                + '<div class="assign-foot"><button type="button" class="assign-primary" data-assign-close>Готово</button></div></div>';
            doc.body.appendChild(node);
        } else {
            node.querySelector('.assign-head').outerHTML = head;
        }
        node.querySelector('[data-assign-body]').innerHTML = body;
        const picker = node.querySelector('[data-assign-picker]') || pickerNode;
        if (picker) picker.hidden = !showPicker;
        const label = node.querySelector('[data-assign-picker] .assign-label');
        if (label) label.textContent = modal.mode === 'delegate' ? 'Кому передать' : 'Добавить исполнителя';
    }

    function mountPicker() {
        const modal = state.modal;
        const node = doc.querySelector('[data-assign-modal]');
        if (!modal || !node || modal.picker || !global.ChecklistBitrixUserPicker) return;
        const input = node.querySelector('[data-assign-lookup]');
        modal.picker = global.ChecklistBitrixUserPicker.create({
            input,
            results: node.querySelector('[data-assign-results]'),
            hint: node.querySelector('[data-assign-hint]'),
            idInput: node.querySelector('[data-assign-user-id]'),
            nameInput: node.querySelector('[data-assign-user-name]'),
            apiUrl: env.usersApi(),
            onSelect: function (user) {
                if (!user) return;
                addUser(user);
                global.setTimeout(function () {
                    if (state.modal && state.modal.picker) state.modal.picker.clear();
                }, 0);
            }
        });
        input.focus();
    }

    doc.addEventListener('click', function (event) {
        const target = event.target && event.target.closest ? event.target : null;
        if (!target) return;
        const opener = target.closest('[data-assign-open]');
        if (opener && !opener.closest('.assign-modal')) {
            event.preventDefault();
            event.stopPropagation();
            open(opener.getAttribute('data-assign-open'), opener.getAttribute('data-assign-target'));
            return;
        }
        if (!state.modal) return;
        if (target.closest('[data-assign-close]') || target.matches('[data-assign-modal]')) {
            closeModal();
            return;
        }
        const remove = target.closest('[data-assign-remove]');
        if (remove) return removeUser(remove.getAttribute('data-assign-remove'));
        const give = target.closest('[data-assign-delegate]');
        if (give) {
            state.modal.mode = 'delegate';
            state.modal.from = give.getAttribute('data-assign-delegate');
            state.modal.receivers = [];
            renderModal();
            mountPicker();
            const input = doc.querySelector('[data-assign-lookup]');
            if (input) input.focus();
            return;
        }
        const unpick = target.closest('[data-assign-unpick]');
        if (unpick) {
            state.modal.receivers = state.modal.receivers.filter(r => r.userId !== unpick.getAttribute('data-assign-unpick'));
            renderModal();
            return;
        }
        if (target.closest('[data-assign-delegate-go]')) return delegate();
        if (target.closest('[data-assign-delegate-cancel]')) {
            state.modal.mode = 'list';
            state.modal.receivers = [];
            renderModal();
            return;
        }
        const editDate = target.closest('[data-assign-edit-date]');
        if (editDate) {
            state.modal.editDeadline = editDate.getAttribute('data-assign-edit-date');
            renderModal();
            const field = doc.querySelector('[data-assign-date]');
            if (field) field.focus();
            return;
        }
        if (target.closest('[data-assign-inherit]')) inherit();
    }, true);

    doc.addEventListener('change', function (event) {
        const target = event.target;
        if (!state.modal || !target || !target.matches) return;
        if (target.matches('[data-assign-deadline]')) {
            state.modal.deadline = text(target.value);
        } else if (target.matches('[data-assign-date]')) {
            const userId = target.getAttribute('data-assign-date');
            state.modal.editDeadline = '';
            setDeadline(userId, target.value);
        }
    });

    doc.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && state.modal) closeModal();
    });

    // ---- the folder page of an item ----

    const JOURNAL_TASK_ACTIONS = {
        created: 'создана задача',
        progress: 'задача в работе («Выполняется»)',
        control: 'задача на проверке («Ждёт контроля»)',
        pending: 'задача ждёт выполнения',
        closed: 'задача завершена',
        responsible: 'задача передана'
    };
    const CLOSE_REASONS = {
        unassigned: 'исполнитель снят с пункта',
        not_required: 'пункт в «Не требуется»',
        deleted: 'пункт удалён',
        covered: 'подпункт вошёл в задачу пункта',
        section_empty: 'в разделе не осталось пунктов задачи'
    };

    function names(list) {
        return (list || []).map(user => text(user.name)).filter(Boolean).join(', ');
    }

    function answering(current) {
        const list = (current || []).filter(Boolean);
        if (list.length > 1) {
            return 'в данный момент за исполнение отвечают ' + list.slice(0, -1).join(', ') + ' и ' + list[list.length - 1];
        }
        return list.length ? list[0] : '—';
    }

    function journalWhat(row) {
        const data = row.data || {};
        const where = row.section ? ' (раздел «' + row.section + '»)' : '';
        switch (row.kind) {
        case 'assign':
            return 'Назначил: ' + names(data.users) + (data.deadline ? ', срок до ' + shortDate(data.deadline) : '') + where;
        case 'unassign':
            return 'Снял: ' + names(data.users) + where;
        case 'delegate':
            return text((data.from || {}).name) + ' передал: ' + names(data.users) + where;
        case 'inherit':
            return 'Исполнители подпункта — как у пункта';
        case 'own':
            return 'Свой список исполнителей подпункта: ' + names(data.users);
        case 'deadline':
            return 'Крайний срок: ' + (data.users || []).map(u => u.name + ' — ' + (u.deadline ? 'до ' + shortDate(u.deadline) : 'без срока')).join(', ');
        case 'status': {
            const by = data.byAssignee ? ' (отметил исполнитель)' : '';
            return 'Статус «' + text(data.from) + '» → «' + text(data.to) + '»' + by;
        }
        case 'task': {
            const action = JOURNAL_TASK_ACTIONS[data.action] || data.action;
            const task = data.url ? '<a href="' + esc(data.url) + '" target="_blank" rel="noopener">№' + esc(data.taskId) + '</a>' : '№' + esc(data.taskId);
            let tail = '';
            if (data.action === 'closed' && CLOSE_REASONS[data.reason]) tail = ': ' + CLOSE_REASONS[data.reason];
            if (data.reopened) tail = ' (возобновлена)';
            // esc() trims: the parts are joined first.
            return { html: esc(action.charAt(0).toUpperCase() + action.slice(1)) + ' ' + task + ' — '
                + esc(text(data.userName) + tail + where) };
        }
        default:
            return row.label || row.kind;
        }
    }

    function journalHtml(rows) {
        if (!rows.length) {
            return '<div class="assign-journal-empty">Назначений по этому пункту ещё не было.</div>';
        }
        return '<div class="assign-journal-scroll"><table class="assign-journal-table"><thead><tr>'
            + '<th>Дата и время</th><th>Кто</th><th>Что произошло</th><th>Отвечают</th></tr></thead><tbody>'
            + rows.map(row => {
                const what = journalWhat(row);
                const current = row.kind === 'task' ? '' : answering((row.data || {}).current);
                return '<tr data-assign-journal-kind="' + esc(row.kind) + '"><td>' + esc(moment(row.at)) + '</td>'
                    + '<td>' + esc(row.actorName || (row.kind === 'task' ? 'Приложение' : '')) + '</td>'
                    + '<td>' + (typeof what === 'object' ? what.html : esc(what)) + '</td>'
                    + '<td>' + esc(current) + '</td></tr>';
            }).join('')
            + '</tbody></table></div>';
    }

    async function loadJournal() {
        const params = new URLSearchParams({ dialogId: env.dialogId(), checklistKey: env.key(), itemId: text(folderBoot.itemId) });
        try {
            const response = await global.fetch(env.api('api/assignments/journal') + '?' + params.toString(), { cache: 'no-store' });
            const data = await response.json();
            return Array.isArray(data.journal) ? data.journal : [];
        } catch (error) {
            console.log('assignments journal failed:', error);
            return [];
        }
    }

    function renderFolder() {
        if (!onFolderPage || text(folderBoot.relativeFolder)) return;
        if (!supported()) return;
        const itemId = text(folderBoot.itemId);
        let head = doc.getElementById('folderAssignees');
        if (!head) {
            head = doc.createElement('div');
            head.id = 'folderAssignees';
            head.className = 'folder-assignees';
            const anchor = doc.querySelector('.folder-heading');
            if (!anchor) return;
            anchor.appendChild(head);
        }
        const info = effective(itemId);
        const button = mayOpen('item', itemId)
            ? '<button class="checklist-action-button checklist-action-button-assign" type="button" data-role="assign-open" '
                + openAttrs('item', itemId) + ' title="' + esc(canAssign() ? 'Назначить исполнителя' : 'Передать пункт') + '"'
                + (env.canEdit() ? '' : ' disabled') + '>' + PERSON_ICON + '</button>'
            : '';
        head.innerHTML = '<span class="folder-assignees-label">Исполнители:</span>'
            + (info.users.length ? stickersHtml(info.users, info.inherited, 0, '') : '<span class="assign-muted">не назначены</span>')
            + button;
        renderJournal();
    }

    async function renderJournal() {
        let section = doc.getElementById('folderAssignJournal');
        if (!section) {
            section = doc.createElement('section');
            section.id = 'folderAssignJournal';
            section.className = 'assign-journal';
            const anchor = doc.querySelector('.folder-content') || doc.body;
            anchor.appendChild(section);
        }
        const rows = await loadJournal();
        section.innerHTML = '<h2 class="assign-journal-title">Журнал назначений и исполнения</h2>' + journalHtml(rows);
    }

    function notifyOpener() {
        try {
            const opener = global.opener;
            if (opener && opener.ChecklistAssignments && typeof opener.ChecklistAssignments.load === 'function') {
                opener.ChecklistAssignments.load();
            }
        } catch (error) {
            // Another origin: the checklist reads it on its focus.
        }
    }

    // ---- wiring ----

    global.addEventListener('checklist-edit-session-state', function (event) {
        const detail = event && event.detail || {};
        if (detail.status === 'active' && detail.sessionId) load();
    });
    global.addEventListener('focus', function () {
        if (env.key()) load();
    });

    if (onFolderPage) {
        const start = function () {
            // The first read draws the stickers and the journal.
            load();
        };
        if (doc.readyState === 'loading') doc.addEventListener('DOMContentLoaded', start);
        else start();
    }

    global.ChecklistAssignments = Object.freeze({
        state,
        load,
        refresh,
        hasDraft,
        itemStickers,
        itemButton,
        sectionHtml,
        open,
        close: closeModal
    });
})(window);
