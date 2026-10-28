(function (global) {
    'use strict';

    // Граданализ (ГА): the right column of the OPR checklist window with the
    // checklists to КОПР Генплан and КОПР АР, ПБ, ОДИ. The server shows it to
    // leaders, administrators, architects and GIPs of the project; GIPs only
    // read it. A change goes to the draft of the edit session of the window
    // and is kept by «Сохранить и закрыть»; «Отмена» drops it. ГА has its own
    // lock: the architect and the GIP work on ГА and OPR at the same time.
    // Its items that are «Не требуется» are shown in the common
    // «Не требуется» block of OPR.

    const doc = global.document;
    const popupApi = global.ChecklistPopupApi;
    const bootstrap = global.CHECKLIST_POPUP_BOOTSTRAP || {};
    const dialogId = String(bootstrap.dialogId || '');
    if (!popupApi || !dialogId) return;

    const REFRESH_MS = 30000;
    const STALE_MS = 10000;
    const FIRST_LOAD_WAIT_MS = 4000;
    const STATUS_TITLES = { yes: 'Да', no: 'Нет', '': 'Нет', nr: 'Не требуется' };
    const ICONS = {
        pencil: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 20h4L19 9l-4-4L4 16v4Z"/><path d="m13.5 6.5 4 4"/></svg>',
        link: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M10 14a4.5 4.5 0 0 0 6.4 0l3-3a4.5 4.5 0 0 0-6.4-6.4l-1.2 1.2"/><path d="M14 10a4.5 4.5 0 0 0-6.4 0l-3 3a4.5 4.5 0 0 0 6.4 6.4l1.2-1.2"/></svg>',
        eye: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12Z"/><circle cx="12" cy="12" r="3"/></svg>',
        folder: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3.5 7.5v9.25A2.25 2.25 0 0 0 5.75 19h12.5a2.25 2.25 0 0 0 2.25-2.25v-7A2.25 2.25 0 0 0 18.25 7.5H11l-2-2H5.75A2.25 2.25 0 0 0 3.5 7.75"/></svg>',
        upload: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 4h14"/><path d="M12 20V4"/><path d="m7 9 5-5 5 5"/></svg>',
        archive: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3.5 5h17v4h-17z"/><path d="M5 9v10h14V9"/><path d="M10 13h4"/></svg>',
        file: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 3H6.5A1.5 1.5 0 0 0 5 4.5v15A1.5 1.5 0 0 0 6.5 21h11a1.5 1.5 0 0 0 1.5-1.5V8Z"/><path d="M14 3v5h5"/></svg>',
        remove: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 7l10 10"/><path d="M17 7 7 17"/></svg>',
        plus: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5v14"/><path d="M5 12h14"/></svg>'
    };

    const state = {
        data: null,
        loading: null,
        busy: 0,
        menuItemId: '',
        linkEditor: {},
        nameEditor: '',
        addingUnder: '',
        pending: {},
        stopped: false,
        architectPicker: false,
        renderPending: false,
        focus: null,
        loadedAt: 0
    };
    let picker = null;
    let resolveFirstLoad = null;
    const firstLoad = new Promise(resolve => { resolveFirstLoad = resolve; });

    // ---- environment ----

    function text(value) {
        return String(value == null ? '' : value).trim();
    }

    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function identity() {
        if (typeof getCurrentEditorIdentity === 'function') return getCurrentEditorIdentity();
        const editor = global.currentEditor || {};
        return { userId: String(editor.id || ''), userName: String(editor.name || '') };
    }

    function sessionId() {
        return typeof getActiveEditSessionId === 'function' ? text(getActiveEditSessionId()) : '';
    }

    function readOnlyWindow() {
        return typeof isReadOnlyWindow === 'function' && isReadOnlyWindow();
    }

    function sessionActive() {
        const api = global.ChecklistPopupEditSession;
        return !!(api && typeof api.isActive === 'function' && api.isActive());
    }

    function finalizing() {
        return typeof popupFinalizationInProgress !== 'undefined' && !!popupFinalizationInProgress;
    }

    function isOpr() {
        return typeof currentChecklistKey !== 'undefined' && currentChecklistKey === 'opr';
    }

    function rights() {
        return (state.data && state.data.rights) || {};
    }

    function visible() {
        return !!(state.data && state.data.visible && !readOnlyWindow());
    }

    function canEditNow() {
        return !!(visible() && rights().edit && !state.data.lockedBy && sessionActive() && !finalizing());
    }

    function say(mode, message) {
        if (typeof setSaveState === 'function') setSaveState(mode, message);
    }

    function url(path, params) {
        const target = new URL(popupApi.url(path));
        Object.keys(params || {}).forEach(key => {
            if (params[key] !== undefined && params[key] !== null && params[key] !== '') {
                target.searchParams.set(key, params[key]);
            }
        });
        return target.href;
    }

    async function getJson(path, params) {
        const response = await global.fetch(url(path, params), { cache: 'no-store' });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.ok === false) {
            const error = new Error(payload.error || ('HTTP ' + response.status));
            error.status = response.status;
            throw error;
        }
        return payload;
    }

    async function sendJson(path, body, init) {
        const response = await global.fetch(popupApi.url(path), Object.assign({
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        }, init || {}));
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.ok === false) {
            const error = new Error(payload.error || ('HTTP ' + response.status));
            error.status = response.status;
            throw error;
        }
        return payload;
    }

    // ---- data ----

    function items() {
        return (state.data && Array.isArray(state.data.items)) ? state.data.items : [];
    }

    function itemById(itemId) {
        return items().find(item => item.id === itemId) || null;
    }

    function childrenOf(itemId) {
        return items().filter(item => item.parentId === itemId);
    }

    function load() {
        if (state.loading) return state.loading;
        const who = identity();
        const sid = sessionId();
        state.loading = (async function () {
            try {
                const result = await getJson('api/ga', { dialogId, userId: who.userId, sessionId: sid });
                state.loadedAt = Date.now();
                apply(result);
            } catch (error) {
                console.log('ГА load skipped:', error);
            } finally {
                state.loading = null;
                resolveFirstLoad();
            }
            return state.data;
        })();
        return state.loading;
    }

    function apply(result, force) {
        const before = JSON.stringify(state.data);
        state.data = result;
        syncDeferredOprLock();
        if (force) redraw();
        else if (JSON.stringify(result) !== before) rerender(false);
    }

    function editingInTables() {
        const active = doc.activeElement;
        return !!(active && active.closest && active.closest('.tables-grid')
            && /^(INPUT|TEXTAREA)$/.test(active.tagName));
    }

    function rerender(force) {
        if (!isOpr()) {
            if (typeof calculateProgress === 'function') calculateProgress();
            return;
        }
        // A background refresh does not tear down a field being typed in.
        if (editingInTables() && !force) {
            state.renderPending = true;
            if (typeof calculateProgress === 'function') calculateProgress();
            return;
        }
        state.renderPending = false;
        if (typeof renderAll === 'function') renderAll();
    }

    function redraw() {
        rerender(true);
    }

    // The OPR lock of an architect who only checks ГА is taken by the first
    // change of OPR; until then the window shows who holds it.
    function syncDeferredOprLock() {
        if (!defersOprLock() || !isOpr() || typeof currentChecklistLock === 'undefined') return;
        if (currentChecklistLock.owned) return;
        const holder = state.data && state.data.oprLockedBy;
        const blocked = !!holder;
        if (blocked === !!currentChecklistLock.lockedByOther
            && (!blocked || currentChecklistLock.userName === holder.userName)) return;
        currentChecklistLock = {
            owned: false,
            lockedByOther: blocked,
            lockId: '',
            userId: blocked ? String(holder.userId || '') : '',
            userName: blocked ? String(holder.userName || '') : '',
            checklistKey: 'opr'
        };
        if (typeof updateLockNotice === 'function') updateLockNotice();
        redraw();
    }

    async function change(op, body) {
        if (!canEditNow()) {
            say('error', blockedReason());
            return null;
        }
        let sid = '';
        try {
            sid = await requireEditingSession('изменение Граданализа', { requireCurrentLock: false });
        } catch (error) {
            say('error', error && error.message ? error.message : 'Сессия редактирования не готова');
            return null;
        }
        const who = identity();
        state.busy += 1;
        say('saving', 'Сохраняем Граданализ...');
        try {
            const result = await sendJson('api/ga/change', Object.assign({
                dialogId,
                sessionId: sid,
                op,
                actingUserId: who.userId,
                actingUserName: who.userName
            }, body));
            apply(result, true);
            if (typeof updateLockNotice === 'function') updateLockNotice();
            return result;
        } catch (error) {
            say('error', error && error.message ? error.message : 'Не удалось изменить Граданализ');
            if (error && error.status === 409) load();
            return null;
        } finally {
            state.busy -= 1;
        }
    }

    async function upload(itemId, files, replaceFileId) {
        if (!canEditNow() || !files || !files.length) return null;
        let sid = '';
        try {
            sid = await requireEditingSession('загрузка файла', { requireCurrentLock: false });
        } catch (error) {
            say('error', error && error.message ? error.message : 'Сессия редактирования не готова');
            return null;
        }
        const who = identity();
        let result = null;
        state.busy += 1;
        try {
            for (let index = 0; index < files.length; index += 1) {
                say('saving', 'Загружаем ' + files[index].name + '...');
                const form = new FormData();
                form.append('dialogId', dialogId);
                form.append('sessionId', sid);
                form.append('itemId', itemId);
                form.append('replaceFileId', index === 0 ? (replaceFileId || '') : '');
                form.append('actingUserId', who.userId);
                form.append('actingUserName', who.userName);
                form.append('file', files[index], files[index].name);
                const response = await global.fetch(popupApi.url('api/ga/upload'), { method: 'POST', body: form });
                const payload = await response.json().catch(() => ({}));
                if (!response.ok || payload.ok === false) {
                    throw new Error(payload.error || ('HTTP ' + response.status));
                }
                result = payload;
            }
            apply(result, true);
            if (typeof updateLockNotice === 'function') updateLockNotice();
        } catch (error) {
            say('error', 'Файл не загружен: ' + (error && error.message ? error.message : ''));
        } finally {
            state.busy -= 1;
        }
        return result;
    }

    function blockedReason() {
        if (!visible()) return 'Граданализ недоступен';
        if (!rights().edit) return 'Граданализ заполняют руководители, администраторы и архитекторы проекта';
        if (state.data.lockedBy) return 'Граданализ сейчас заполняет ' + state.data.lockedBy.userName;
        if (finalizing()) return 'Сессия уже завершается';
        return 'Сессия редактирования ещё не готова';
    }

    // ---- formatting ----

    function formatMoment(value) {
        const moment = new Date(value);
        if (!value || Number.isNaN(moment.getTime())) return '';
        const pad = number => String(number).padStart(2, '0');
        return pad(moment.getDate()) + '.' + pad(moment.getMonth() + 1) + '.' + moment.getFullYear()
            + ' ' + pad(moment.getHours()) + ':' + pad(moment.getMinutes());
    }

    function statusClass(status) {
        if (status === 'yes') return 'status-indicator green';
        if (status === 'nr') return 'status-indicator gray';
        return 'status-indicator';
    }

    function statusTitle(item) {
        const label = STATUS_TITLES[item.status] || 'Нет';
        const base = item.status === '' ? 'Не проверен' : 'Статус «' + label + '»';
        return base + (canEditNow() ? '. Нажмите, чтобы выбрать статус' : '');
    }

    function fileHref(file) {
        if (file.kind === 'upload') {
            // A file of the draft opens for the session that uploaded it.
            return url('api/ga/file', { dialogId, fileId: file.id, userId: identity().userId, sessionId: sessionId() });
        }
        return file.url ? popupApi.url(file.url) : '';
    }

    // ---- rendering: the panel ----

    function showsPanel() {
        return isOpr() && visible();
    }

    function headerHtml() {
        const data = state.data;
        const progress = data.progress || {};
        const percent = progress.percent === null || progress.percent === undefined ? '—' : progress.percent + '%';
        const manage = !!rights().manageArchitects && !readOnlyWindow();
        const chips = (data.architects || []).map(user => (
            '<span class="project-gip-chip ga-architect-chip" title="ID ' + esc(user.userId) + '">'
            + '<span>' + esc(user.name || ('ID ' + user.userId)) + '</span>'
            + (manage ? '<button type="button" class="project-gip-remove" data-ga-architect-remove="' + esc(user.userId) + '"'
                + ' title="Убрать из архитекторов" aria-label="Убрать ' + esc(user.name) + ' из архитекторов">×</button>' : '')
            + '</span>'
        )).join('');
        const add = manage && !state.architectPicker
            ? '<button type="button" class="project-gip-add" data-ga-architect-add>+ Добавить</button>' : '';
        const pickerHtml = manage && state.architectPicker ? (
            '<div class="project-gip-picker bitrix-user-picker ga-architect-picker">'
            + '<div class="bitrix-user-picker-search-row">'
            + '<input type="text" id="gaArchitectLookup" placeholder="Начните вводить ФИО сотрудника" autocomplete="off">'
            + '<button type="button" class="project-gip-cancel" data-ga-architect-cancel title="Закрыть">×</button>'
            + '</div>'
            + '<div id="gaArchitectResults" class="bitrix-user-picker-results" hidden></div>'
            + '<div id="gaArchitectHint" class="bitrix-user-picker-hint"></div>'
            + '<input type="hidden" id="gaArchitectUserId"><input type="hidden" id="gaArchitectUserName">'
            + '</div>'
        ) : '';
        let notice = '';
        if (data.lockedBy) {
            notice = '<div class="ga-notice">Сейчас Граданализ заполняет ' + esc(data.lockedBy.userName)
                + ' — у вас он открыт для просмотра</div>';
        } else if (!rights().edit) {
            notice = '<div class="ga-notice ga-notice-muted">Граданализ открыт для просмотра</div>';
        }
        if (state.stopped && (data.missingNotes || []).length) {
            notice += '<div class="ga-notice ga-notice-error">Сохранение остановлено: у пунктов со статусом «Нет» '
                + 'заполните примечание — они подсвечены</div>';
        }
        return '<div class="ga-head">'
            + '<div class="ga-head-row">'
            + '<span class="ga-title">Граданализ</span>'
            + '<span class="ga-percent" title="Выполнено ' + esc(progress.done || 0) + ' из ' + esc(progress.total || 0) + '">' + esc(percent) + '</span>'
            + '<span class="ga-architects"><span class="project-gip-label">Архитектор:</span>'
            + '<span class="project-gip-list">' + (chips || '<span class="project-gip-empty">не назначен</span>') + '</span>'
            + add + '</span>'
            + '</div>'
            + pickerHtml
            + notice
            + '</div>';
    }

    function columnsHtml() {
        return '<div class="ga-row ga-row-head">'
            + '<div class="ga-cell ga-cell-status" title="Статус"></div>'
            + '<div class="ga-cell">Описание требования</div>'
            + '<div class="ga-cell">Ссылка на документ</div>'
            + '<div class="ga-cell">Проверяющий</div>'
            + '<div class="ga-cell">Примечание</div>'
            + '</div>';
    }

    function nameCellHtml(item, editable) {
        const isSub = !!item.parentId;
        let name;
        if (editable && item.isCustom && state.nameEditor === item.id) {
            const value = state.pending[item.id + ':name'] !== undefined ? state.pending[item.id + ':name'] : item.name;
            name = '<textarea class="ga-input ga-autogrow" rows="1" data-ga-name-input="' + esc(item.id) + '">' + esc(value) + '</textarea>';
        } else {
            name = '<span class="ga-name">' + esc(item.name) + '</span>';
        }
        const tools = [];
        if (editable && item.isCustom && state.nameEditor !== item.id) {
            tools.push('<button type="button" class="ga-icon-btn" data-ga-rename="' + esc(item.id) + '" title="Изменить описание" aria-label="Изменить описание">' + ICONS.pencil + '</button>');
        }
        if (editable && item.isCustom) {
            tools.push('<button type="button" class="ga-icon-btn ga-danger" data-ga-delete="' + esc(item.id) + '" title="Удалить пункт" aria-label="Удалить пункт">' + ICONS.remove + '</button>');
        }
        if (editable && !isSub && item.status !== 'nr') {
            tools.push('<button type="button" class="ga-icon-btn" data-ga-add-sub="' + esc(item.id) + '" title="Добавить подпункт" aria-label="Добавить подпункт">' + ICONS.plus + '</button>');
        }
        const counter = item.childCount && !isSub
            ? '<span class="ga-sub-counter">' + childrenOf(item.id).filter(child => child.status === 'yes').length + ' / ' + childrenOf(item.id).filter(child => child.status !== 'nr').length + '</span>'
            : '';
        return '<div class="ga-cell ga-cell-name">'
            + (isSub ? '<span class="ga-sub-mark" aria-hidden="true">↳</span>' : '')
            + '<div class="ga-name-wrap">' + name + counter + '</div>'
            + (tools.length ? '<span class="ga-tools">' + tools.join('') + '</span>' : '')
            + '</div>';
    }

    function filesHtml(item) {
        return (item.files || []).map(file => {
            const href = fileHref(file);
            const icon = file.kind === 'link' ? ICONS.link : ICONS.file;
            const label = esc(file.name);
            const name = href
                ? '<a class="ga-file-name" href="' + esc(href) + '" target="_blank" rel="noopener" data-ga-open-file>' + label + '</a>'
                : '<span class="ga-file-name is-gone">' + label + '</span>';
            const mark = file.mark
                ? '<span class="ga-file-mark' + (file.state === 'deleted' ? ' is-deleted' : '') + '">' + esc(file.mark) + '</span>'
                : '';
            return '<div class="ga-file" title="' + esc((file.kind === 'link' ? 'Ссылка на файл ИД' : 'Загружен') + (file.byName ? ' · ' + file.byName : '')) + '">'
                + '<span class="ga-file-icon">' + icon + '</span>' + name + mark + '</div>';
        }).join('');
    }

    function linkCellHtml(item, editable) {
        const files = item.files || [];
        const hasContent = !!(text(item.linkText) || files.length);
        const folder = files.length || item.archiveCount
            ? '<button type="button" class="ga-icon-btn" data-ga-folder="' + esc(item.id) + '" title="Посмотреть папку" aria-label="Посмотреть папку">' + ICONS.folder + '</button>'
            : '';
        if (editable && state.linkEditor[item.id]) {
            const value = state.pending[item.id + ':linkText'] !== undefined ? state.pending[item.id + ':linkText'] : item.linkText;
            return '<div class="ga-cell ga-cell-link is-editing">'
                + '<textarea class="ga-input ga-autogrow" rows="1" placeholder="Нормативный документ, письмо, раздел" data-ga-link-text="' + esc(item.id) + '">' + esc(value) + '</textarea>'
                + filesHtml(item)
                + '<div class="ga-link-actions">'
                + '<button type="button" class="ga-btn" data-ga-upload="' + esc(item.id) + '" title="Загрузить файл">' + ICONS.upload + '<span>Загрузить</span></button>'
                + '<button type="button" class="ga-btn" data-ga-link-pick="' + esc(item.id) + '" title="Сослаться на файл ИД">' + ICONS.link + '<span>Сослаться на файл</span></button>'
                + folder
                + '<button type="button" class="ga-btn ga-btn-primary" data-ga-link-done="' + esc(item.id) + '">Готово</button>'
                + '</div>'
                + '</div>';
        }
        if (!hasContent) {
            return '<div class="ga-cell ga-cell-link">'
                + (editable
                    ? '<button type="button" class="ga-btn ga-edit-empty" data-ga-link-edit="' + esc(item.id) + '" title="Редактировать пункт">' + ICONS.pencil + '<span>Редактировать пункт</span></button>'
                    : '<span class="ga-placeholder">—</span>')
                + folder
                + '</div>';
        }
        return '<div class="ga-cell ga-cell-link">'
            + (text(item.linkText) ? '<div class="ga-link-text">' + esc(item.linkText) + '</div>' : '')
            + filesHtml(item)
            + '<div class="ga-link-tools">'
            + folder
            + (editable ? '<button type="button" class="ga-icon-btn" data-ga-link-edit="' + esc(item.id) + '" title="Редактировать пункт" aria-label="Редактировать пункт">' + ICONS.pencil + '</button>' : '')
            + '</div>'
            + '</div>';
    }

    function reviewCellHtml(item) {
        const review = item.review;
        if (!review) {
            return '<div class="ga-cell ga-cell-review"><span class="ga-placeholder">—</span></div>';
        }
        const eye = item.reviewCount > 1
            ? '<button type="button" class="ga-icon-btn" data-ga-history="' + esc(item.id) + '" title="Посмотреть все изменения" aria-label="Посмотреть все изменения">' + ICONS.eye + '</button>'
            : '';
        return '<div class="ga-cell ga-cell-review">'
            + '<div class="ga-review-name">' + esc(review.userName || ('ID ' + review.userId)) + '</div>'
            + '<div class="ga-review-meta"><span>' + esc(formatMoment(review.at)) + '</span>'
            + '<span class="ga-review-status ga-status-' + esc(review.status || 'none') + '">' + esc(STATUS_TITLES[review.status] || 'Нет') + '</span>'
            + eye + '</div>'
            + '</div>';
    }

    function noteCellHtml(item, editable) {
        if (!editable) {
            return '<div class="ga-cell ga-cell-note">'
                + (text(item.note) ? '<div class="ga-note-text">' + esc(item.note) + '</div>' : '<span class="ga-placeholder">—</span>')
                + '</div>';
        }
        const value = state.pending[item.id + ':note'] !== undefined ? state.pending[item.id + ':note'] : item.note;
        return '<div class="ga-cell ga-cell-note">'
            + '<textarea class="ga-input ga-autogrow ga-note" rows="1" placeholder="' + (item.status === 'no' ? 'Обоснование «Нет» — обязательно' : 'Примечание') + '"'
            + ' data-ga-note="' + esc(item.id) + '">' + esc(value) + '</textarea>'
            + '</div>';
    }

    function rowHtml(item, editable) {
        const classes = ['ga-row'];
        if (item.parentId) classes.push('ga-subitem');
        if (item.noteMissing) classes.push('is-note-missing');
        if (item.noteMissing && state.stopped) classes.push('is-stopped');
        const row = '<div class="' + classes.join(' ') + '" data-ga-row="' + esc(item.id) + '">'
            + '<div class="ga-cell ga-cell-status"><button type="button" class="' + statusClass(item.status) + ' ga-status"'
            + ' data-ga-status="' + esc(item.id) + '" title="' + esc(statusTitle(item)) + '" aria-label="' + esc(statusTitle(item)) + '"'
            + (editable ? '' : ' disabled') + '></button></div>'
            + nameCellHtml(item, editable)
            + linkCellHtml(item, editable)
            + reviewCellHtml(item)
            + noteCellHtml(item, editable)
            + '</div>';
        const adding = editable && state.addingUnder === item.id
            ? '<div class="ga-add-row ga-add-sub"><input type="text" class="ga-input" data-ga-new-sub="' + esc(item.id) + '" placeholder="Новый подпункт">'
                + '<button type="button" class="ga-btn" data-ga-new-sub-save="' + esc(item.id) + '">Добавить</button>'
                + '<button type="button" class="ga-btn" data-ga-new-sub-cancel>Отмена</button></div>'
            : '';
        return row + adding;
    }

    function groupHtml(group, editable) {
        const shown = items().filter(item => item.group === group.id && item.status !== 'nr' && !item.hidden);
        const rows = shown.map(item => rowHtml(item, editable)).join('');
        const add = editable
            ? '<div class="ga-add-row"><input type="text" class="ga-input" data-ga-new-item="' + esc(group.id) + '" placeholder="Новый пункт">'
                + '<button type="button" class="ga-btn" data-ga-new-item-save="' + esc(group.id) + '">Добавить пункт</button></div>'
            : '';
        return '<div class="ga-group" data-ga-group="' + esc(group.id) + '">'
            + '<div class="ga-group-title">' + esc(group.title) + '</div>'
            + (rows || '<div class="ga-empty">Все пункты раздела в «Не требуется»</div>')
            + add
            + '</div>';
    }

    function panelHtml() {
        if (!showsPanel()) return '';
        // Back to OPR from another checklist: the column is read again.
        if (Date.now() - state.loadedAt > STALE_MS) global.setTimeout(load, 0);
        const editable = canEditNow();
        const blocks = (state.data.blocks || []).map(block => {
            const groups = (state.data.groups || []).filter(group => group.block === block.id);
            return '<section class="ga-block" data-ga-block="' + esc(block.id) + '">'
                + '<div class="ga-block-title">' + esc(block.title) + '</div>'
                + '<div class="ga-grid-scroll"><div class="ga-grid">'
                + columnsHtml()
                + groups.map(group => groupHtml(group, editable)).join('')
                + '</div></div>'
                + '</section>';
        }).join('');
        return '<div class="ga-panel' + (editable ? ' is-editable' : '') + '">'
            + headerHtml()
            + blocks
            + '<input type="file" multiple hidden data-ga-file-input>'
            + '</div>';
    }

    // Items of ГА in «Не требуется» of OPR (rows of the left column).
    function notRequiredItems() {
        if (!showsPanel()) return [];
        return items().filter(item => item.status === 'nr' && !item.hidden);
    }

    function notRequiredRowsHtml(gridClass) {
        const list = notRequiredItems();
        if (!list.length) return '';
        const editable = canEditNow();
        const groupTitle = id => ((state.data.groups || []).find(group => group.id === id) || {}).title || '';
        return '<div class="ga-nr-block">' + list.map(item => {
            const parent = item.parentId ? itemById(item.parentId) : null;
            const children = childrenOf(item.id);
            const where = 'Граданализ · ' + groupTitle(item.group) + (parent ? ' · подпункт пункта «' + parent.name.trim() + '»' : '');
            return '<div class="row not-required ga-nr-row ' + esc(gridClass || '') + '" data-ga-row="' + esc(item.id) + '">'
                + '<div class="td"><div class="cell-name">'
                + '<button type="button" class="status-indicator gray ga-status" data-ga-status="' + esc(item.id) + '"'
                + ' title="' + esc(editable ? 'Не требуется. Нажмите, чтобы вернуть пункт в Граданализ' : 'Не требуется') + '"' + (editable ? '' : ' disabled') + '></button>'
                + '<div class="item-name-wrap"><span class="item-name">' + esc(item.name) + '</span>'
                + (children.length ? '<span class="ga-sub-counter">+ ' + children.length + ' подп.</span>' : '') + '</div>'
                + '</div></div>'
                + '<div class="td"><span class="ga-nr-where" title="' + esc(where) + '"><span class="ga-badge">ГА</span>' + esc(groupTitle(item.group)) + '</span></div>'
                + '</div>';
        }).join('') + '</div>';
    }

    function afterRender(root) {
        const scope = root || doc;
        scope.querySelectorAll('.ga-autogrow').forEach(grow);
        if (state.focus) {
            const target = doc.querySelector(state.focus.selector);
            if (target) {
                target.focus();
                if (typeof target.setSelectionRange === 'function' && state.focus.caret !== undefined) {
                    target.setSelectionRange(state.focus.caret, state.focus.caret);
                }
            }
            state.focus = null;
        }
        if (picker && typeof picker.destroy === 'function') {
            picker.destroy();
            picker = null;
        }
        const lookup = doc.getElementById('gaArchitectLookup');
        if (lookup && global.ChecklistBitrixUserPicker) {
            picker = global.ChecklistBitrixUserPicker.create({
                input: lookup,
                results: doc.getElementById('gaArchitectResults'),
                hint: doc.getElementById('gaArchitectHint'),
                idInput: doc.getElementById('gaArchitectUserId'),
                nameInput: doc.getElementById('gaArchitectUserName'),
                apiUrl: popupApi.url('api/checklist/bitrix-users'),
                onSelect: addArchitect
            });
            lookup.focus();
        }
    }

    function grow(textarea) {
        textarea.style.height = 'auto';
        textarea.style.height = Math.max(30, textarea.scrollHeight + 2) + 'px';
    }

    function focusAfterRender(selector, caret) {
        state.focus = { selector, caret };
    }

    // ---- status menu ----

    function closeMenu() {
        const menu = doc.querySelector('.ga-status-menu');
        if (menu) menu.remove();
        state.menuItemId = '';
    }

    function openMenu(button, item) {
        closeMenu();
        const derivedYes = item.derived;
        const entries = [
            { status: 'yes', label: 'Да', cls: 'green', disabled: derivedYes && item.status !== 'yes',
              hint: derivedYes ? '«Да» ставится само, когда все подпункты отмечены «Да»' : '' },
            { status: 'no', label: 'Нет', cls: '', disabled: false, hint: 'Нужно примечание с обоснованием' },
            { status: 'nr', label: 'Не требуется', cls: 'gray', disabled: false, hint: 'Пункт уйдёт в «Не требуется»' }
        ];
        const menu = doc.createElement('div');
        menu.className = 'ga-status-menu';
        menu.setAttribute('role', 'menu');
        menu.innerHTML = entries.map(entry => (
            '<button type="button" role="menuitem" data-ga-set-status="' + entry.status + '"'
            + (entry.disabled ? ' disabled' : '')
            + (entry.status === item.status ? ' class="is-current"' : '')
            + (entry.hint ? ' title="' + esc(entry.hint) + '"' : '') + '>'
            + '<span class="status-indicator ' + entry.cls + '" aria-hidden="true"></span>'
            + '<span>' + entry.label + '</span></button>'
        )).join('');
        doc.body.appendChild(menu);
        const box = button.getBoundingClientRect();
        const viewportHeight = doc.documentElement.clientHeight;
        const below = box.bottom + 6;
        const top = below + menu.offsetHeight > viewportHeight - 8 ? box.top - menu.offsetHeight - 6 : below;
        const left = Math.min(box.left, doc.documentElement.clientWidth - menu.offsetWidth - 8);
        menu.style.top = Math.max(8, top) + 'px';
        menu.style.left = Math.max(8, left) + 'px';
        state.menuItemId = item.id;
        const first = menu.querySelector('button:not(:disabled)');
        if (first) first.focus();
    }

    // ---- modals ----

    function closeModal() {
        const modal = doc.querySelector('.ga-modal-backdrop');
        if (modal) modal.remove();
    }

    function openModal(title, bodyHtml, extraClass) {
        closeModal();
        const backdrop = doc.createElement('div');
        backdrop.className = 'ga-modal-backdrop';
        backdrop.innerHTML = '<div class="ga-modal ' + (extraClass || '') + '" role="dialog" aria-modal="true">'
            + '<div class="ga-modal-head"><div class="ga-modal-title">' + esc(title) + '</div>'
            + '<button type="button" class="ga-icon-btn" data-ga-modal-close title="Закрыть" aria-label="Закрыть">' + ICONS.remove + '</button></div>'
            + '<div class="ga-modal-body">' + bodyHtml + '</div></div>';
        doc.body.appendChild(backdrop);
        return backdrop;
    }

    async function itemDetails(itemId) {
        const who = identity();
        return (await getJson('api/ga/item', { dialogId, itemId, userId: who.userId, sessionId: sessionId() })).item;
    }

    function fileRowHtml(file, archived, editable, itemId) {
        const href = fileHref(file);
        const name = href
            ? '<a href="' + esc(href) + '" target="_blank" rel="noopener" data-ga-open-file>' + esc(file.name) + '</a>'
            : '<span class="is-gone">' + esc(file.name) + '</span>';
        const kind = file.kind === 'link'
            ? 'Ссылка на ИД' + (file.sourceItemName ? ' · ' + esc(file.sourceItemName) : '')
            : 'Загружен';
        const mark = file.mark ? '<div class="ga-file-mark' + (file.state === 'deleted' ? ' is-deleted' : '') + '">' + esc(file.mark) + '</div>' : '';
        const archivedInfo = archived
            ? '<div class="ga-muted">' + (file.reason === 'replaced' ? 'Заменён' : 'Перенесён в архив') + ' '
                + esc(formatMoment(file.archivedAt)) + (file.archivedByName ? ' · ' + esc(file.archivedByName) : '') + '</div>'
            : '';
        const actions = !archived && editable
            ? '<button type="button" class="ga-icon-btn" data-ga-replace="' + esc(file.id) + '" data-ga-item="' + esc(itemId) + '" title="Заменить файл" aria-label="Заменить файл">' + ICONS.upload + '</button>'
                + '<button type="button" class="ga-icon-btn" data-ga-archive="' + esc(file.id) + '" data-ga-item="' + esc(itemId) + '" title="Перенести в архив" aria-label="Перенести в архив">' + ICONS.archive + '</button>'
            : '';
        return '<tr><td><span class="ga-file-icon">' + (file.kind === 'link' ? ICONS.link : ICONS.file) + '</span></td>'
            + '<td>' + name + mark + archivedInfo + '</td>'
            + '<td>' + kind + '</td>'
            + '<td>' + esc(formatMoment(file.at)) + '</td>'
            + '<td>' + esc(file.byName || '—') + '</td>'
            + '<td class="ga-actions-cell">' + actions + '</td></tr>';
    }

    async function openFolder(itemId) {
        let item;
        try {
            item = await itemDetails(itemId);
        } catch (error) {
            say('error', error.message);
            return;
        }
        const editable = canEditNow();
        const head = '<tr><th></th><th>Файл</th><th>Источник</th><th>Добавлен</th><th>Кто добавил</th><th></th></tr>';
        const current = (item.files || []).length
            ? '<table class="ga-table">' + head + item.files.map(file => fileRowHtml(file, false, editable, itemId)).join('') + '</table>'
            : '<div class="ga-muted">Файлов нет</div>';
        const archive = (item.archive || []).length
            ? '<table class="ga-table">' + head + item.archive.map(file => fileRowHtml(file, true, false, itemId)).join('') + '</table>'
            : '<div class="ga-muted">Архив пуст</div>';
        const actions = editable
            ? '<div class="ga-link-actions">'
                + '<button type="button" class="ga-btn" data-ga-upload="' + esc(itemId) + '">' + ICONS.upload + '<span>Загрузить файл</span></button>'
                + '<button type="button" class="ga-btn" data-ga-link-pick="' + esc(itemId) + '">' + ICONS.link + '<span>Сослаться на файл</span></button>'
                + '</div>'
            : '';
        openModal('Папка пункта: ' + item.name.trim(),
            actions
            + '<h4>Файлы</h4>' + current
            + '<h4>Архив</h4>' + archive,
            'ga-modal-folder').dataset.gaFolderItem = itemId;
    }

    async function openHistory(itemId) {
        let item;
        try {
            item = await itemDetails(itemId);
        } catch (error) {
            say('error', error.message);
            return;
        }
        const labels = item.statusLabels || STATUS_TITLES;
        const row = review => '<tr><td><span class="ga-review-status ga-status-' + esc(review.status || 'none') + '">'
            + esc(labels[review.status] || 'Нет') + '</span></td><td>' + esc(review.userName || ('ID ' + review.userId)) + '</td>'
            + '<td>' + esc(formatMoment(review.at)) + '</td></tr>';
        const reviews = item.reviews || [];
        const last = reviews[0];
        const head = '<tr><th>Статус</th><th>Проверяющий</th><th>Дата и время</th></tr>';
        openModal('История проверки: ' + item.name.trim(),
            '<h4>Последняя запись</h4>'
            + (last ? '<table class="ga-table">' + head + row(last) + '</table>' : '<div class="ga-muted">Записей нет</div>')
            + '<h4>Архив</h4>'
            + (reviews.length > 1 ? '<table class="ga-table">' + head + reviews.slice(1).map(row).join('') + '</table>' : '<div class="ga-muted">Архив пуст</div>'),
            'ga-modal-history');
    }

    async function openPicker(itemId) {
        let sources;
        try {
            sources = (await getJson('api/ga/id-files', { dialogId, userId: identity().userId })).sources || [];
        } catch (error) {
            say('error', error.message);
            return;
        }
        const body = sources.map(source => {
            const list = (source.items || []).map(entry => (
                '<div class="ga-pick-item"><div class="ga-pick-name">' + esc(entry.parentName ? entry.parentName + ' / ' + entry.name : entry.name) + '</div>'
                + entry.documents.map(document => (
                    '<button type="button" class="ga-pick-doc" data-ga-pick-source="' + esc(source.dialogId) + '"'
                    + ' data-ga-pick-item="' + esc(entry.itemId) + '" data-ga-pick-doc="' + esc(document.id) + '" data-ga-target="' + esc(itemId) + '">'
                    + '<span class="ga-file-icon">' + ICONS.file + '</span><span>' + esc(document.name) + '</span>'
                    + '<span class="ga-muted">' + esc(formatMoment(document.at)) + '</span></button>'
                )).join('')
                + '</div>'
            )).join('');
            return '<h4>' + esc(source.label) + '</h4>' + (list || '<div class="ga-muted">В чек-листе ИД нет файлов</div>');
        }).join('');
        openModal('Сослаться на файл чек-листа ИД', '<input type="search" class="ga-input ga-pick-search" placeholder="Найти файл или пункт" data-ga-pick-search>' + body, 'ga-modal-picker');
    }

    async function refreshOpenFolder() {
        const modal = doc.querySelector('.ga-modal-backdrop');
        const itemId = modal && modal.dataset.gaFolderItem;
        if (itemId) await openFolder(itemId);
    }

    // ---- architects ----

    async function addArchitect(user) {
        if (!user || !text(user.userId)) return;
        try {
            const who = identity();
            const result = await sendJson('api/ga/architects', {
                dialogId, op: 'add', userId: user.userId, userName: user.name, actingUserId: who.userId
            });
            state.architectPicker = false;
            state.data.architects = result.architects;
            await load();
            redraw();
        } catch (error) {
            global.alert(error && error.message ? error.message : 'Не удалось добавить архитектора');
        }
    }

    async function removeArchitect(userId) {
        try {
            const who = identity();
            await sendJson('api/ga/architects', { dialogId, op: 'remove', userId, actingUserId: who.userId });
            await load();
            redraw();
        } catch (error) {
            global.alert(error && error.message ? error.message : 'Не удалось убрать архитектора');
        }
    }

    // ---- events (delegated: the panel is redrawn with the checklist) ----

    let uploadTarget = { itemId: '', replaceFileId: '' };

    function pickFiles(itemId, replaceFileId) {
        const input = doc.querySelector('[data-ga-file-input]');
        if (!input) return;
        uploadTarget = { itemId, replaceFileId: replaceFileId || '' };
        input.multiple = !replaceFileId;
        input.value = '';
        input.click();
    }

    async function saveText(field, itemId, value) {
        const item = itemById(itemId);
        const key = itemId + ':' + field;
        if (!item || item[field] === value) {
            delete state.pending[key];
            return;
        }
        const op = field === 'linkText' ? 'linkText' : field === 'name' ? 'rename' : 'note';
        const body = field === 'name' ? { itemId, name: value } : { itemId, text: value };
        const result = await change(op, body);
        if (result) delete state.pending[key];
    }

    doc.addEventListener('click', async function (event) {
        const target = event.target.closest('[data-ga-status],[data-ga-set-status],[data-ga-link-edit],[data-ga-link-done],'
            + '[data-ga-upload],[data-ga-link-pick],[data-ga-folder],[data-ga-history],[data-ga-rename],[data-ga-delete],'
            + '[data-ga-add-sub],[data-ga-new-sub-save],[data-ga-new-sub-cancel],[data-ga-new-item-save],[data-ga-modal-close],'
            + '[data-ga-replace],[data-ga-archive],[data-ga-pick-doc],[data-ga-architect-add],[data-ga-architect-cancel],'
            + '[data-ga-architect-remove]');
        if (!event.target.closest('.ga-status-menu') && !(target && target.dataset.gaStatus !== undefined)) {
            closeMenu();
        }
        if (event.target.classList && event.target.classList.contains('ga-modal-backdrop')) {
            closeModal();
            return;
        }
        if (!target) return;
        const data = target.dataset;
        if (data.gaStatus !== undefined) {
            const item = itemById(data.gaStatus);
            if (!item || target.disabled) return;
            if (state.menuItemId === item.id) {
                closeMenu();
                return;
            }
            openMenu(target, item);
        } else if (data.gaSetStatus !== undefined) {
            const itemId = state.menuItemId;
            closeMenu();
            if (!itemId) return;
            const item = itemById(itemId);
            if (item && item.status !== data.gaSetStatus) {
                const result = await change('status', { itemId, status: data.gaSetStatus });
                if (result && data.gaSetStatus === 'no') {
                    focusNote(itemId);
                }
            }
        } else if (data.gaLinkEdit !== undefined) {
            state.linkEditor[data.gaLinkEdit] = true;
            focusAfterRender('[data-ga-link-text="' + data.gaLinkEdit + '"]');
            redraw();
        } else if (data.gaLinkDone !== undefined) {
            const itemId = data.gaLinkDone;
            const field = doc.querySelector('[data-ga-link-text="' + itemId + '"]');
            delete state.linkEditor[itemId];
            if (field) await saveText('linkText', itemId, field.value.trim());
            redraw();
        } else if (data.gaUpload !== undefined) {
            pickFiles(data.gaUpload, '');
        } else if (data.gaReplace !== undefined) {
            pickFiles(data.gaItem, data.gaReplace);
        } else if (data.gaArchive !== undefined) {
            if (!global.confirm('Перенести файл в архив пункта?')) return;
            await change('archiveFile', { itemId: data.gaItem, fileId: data.gaArchive });
            await refreshOpenFolder();
        } else if (data.gaLinkPick !== undefined) {
            openPicker(data.gaLinkPick);
        } else if (data.gaPickDoc !== undefined) {
            const result = await change('link', {
                itemId: data.gaTarget,
                sourceDialogId: data.gaPickSource,
                sourceItemId: data.gaPickItem,
                documentId: data.gaPickDoc
            });
            if (result) closeModal();
        } else if (data.gaFolder !== undefined) {
            openFolder(data.gaFolder);
        } else if (data.gaHistory !== undefined) {
            openHistory(data.gaHistory);
        } else if (data.gaRename !== undefined) {
            state.nameEditor = data.gaRename;
            focusAfterRender('[data-ga-name-input="' + data.gaRename + '"]');
            redraw();
        } else if (data.gaDelete !== undefined) {
            const item = itemById(data.gaDelete);
            if (!item) return;
            const sub = childrenOf(item.id).length ? ' вместе с подпунктами' : '';
            if (!global.confirm('Удалить пункт «' + item.name.trim() + '»' + sub + '?')) return;
            await change('delete', { itemId: item.id });
        } else if (data.gaAddSub !== undefined) {
            state.addingUnder = data.gaAddSub;
            focusAfterRender('[data-ga-new-sub="' + data.gaAddSub + '"]');
            redraw();
        } else if (data.gaNewSubCancel !== undefined) {
            state.addingUnder = '';
            redraw();
        } else if (data.gaNewSubSave !== undefined) {
            await addFrom('[data-ga-new-sub="' + data.gaNewSubSave + '"]', { parentId: data.gaNewSubSave });
        } else if (data.gaNewItemSave !== undefined) {
            await addFrom('[data-ga-new-item="' + data.gaNewItemSave + '"]', { groupId: data.gaNewItemSave });
        } else if (data.gaModalClose !== undefined) {
            closeModal();
        } else if (data.gaArchitectAdd !== undefined) {
            state.architectPicker = true;
            redraw();
        } else if (data.gaArchitectCancel !== undefined) {
            state.architectPicker = false;
            redraw();
        } else if (data.gaArchitectRemove !== undefined) {
            if (!global.confirm('Убрать сотрудника из архитекторов проекта?')) return;
            removeArchitect(data.gaArchitectRemove);
        }
    });

    async function addFrom(selector, body) {
        const input = doc.querySelector(selector);
        const name = input ? input.value.trim() : '';
        if (!name) {
            if (input) input.focus();
            return;
        }
        const result = await change('add', Object.assign({ name }, body));
        if (result) {
            state.addingUnder = '';
            redraw();
        }
    }

    function focusNote(itemId) {
        const field = doc.querySelector('[data-ga-note="' + itemId + '"]');
        if (field) {
            field.focus();
            field.setSelectionRange(field.value.length, field.value.length);
        }
    }

    doc.addEventListener('input', function (event) {
        const field = event.target;
        if (!field.dataset) return;
        if (field.dataset.gaPickSearch !== undefined) {
            const query = field.value.trim().toLowerCase();
            doc.querySelectorAll('.ga-pick-item').forEach(entry => {
                entry.hidden = !!query && entry.textContent.toLowerCase().indexOf(query) === -1;
            });
            return;
        }
        const itemId = field.dataset.gaNote || field.dataset.gaLinkText || field.dataset.gaNameInput;
        if (!itemId) return;
        const fieldName = field.dataset.gaNote ? 'note' : field.dataset.gaLinkText ? 'linkText' : 'name';
        state.pending[itemId + ':' + fieldName] = field.value;
        if (field.classList.contains('ga-autogrow')) grow(field);
    });

    doc.addEventListener('change', async function (event) {
        const field = event.target;
        if (!field.dataset) return;
        if (field.dataset.gaFileInput !== undefined) {
            const files = Array.from(field.files || []);
            const target = uploadTarget;
            field.value = '';
            await upload(target.itemId, files, target.replaceFileId);
            await refreshOpenFolder();
            return;
        }
        if (field.dataset.gaNote) {
            await saveText('note', field.dataset.gaNote, field.value.trim());
        } else if (field.dataset.gaLinkText) {
            await saveText('linkText', field.dataset.gaLinkText, field.value.trim());
        } else if (field.dataset.gaNameInput) {
            const itemId = field.dataset.gaNameInput;
            const value = field.value.trim();
            if (!value) return;
            state.nameEditor = '';
            await saveText('name', itemId, value);
            redraw();
        }
    });

    doc.addEventListener('keydown', function (event) {
        const field = event.target;
        if (event.key === 'Escape') {
            closeMenu();
            if (doc.querySelector('.ga-modal-backdrop')) closeModal();
        }
        if (!field || !field.dataset) return;
        if (event.key === 'Enter' && !event.shiftKey) {
            if (field.dataset.gaNewItem) {
                event.preventDefault();
                addFrom('[data-ga-new-item="' + field.dataset.gaNewItem + '"]', { groupId: field.dataset.gaNewItem });
            } else if (field.dataset.gaNewSub) {
                event.preventDefault();
                addFrom('[data-ga-new-sub="' + field.dataset.gaNewSub + '"]', { parentId: field.dataset.gaNewSub });
            } else if (field.dataset.gaNameInput) {
                event.preventDefault();
                field.blur();
            }
        }
    });

    // The menu stays next to its circle: a scroll closes it.
    doc.addEventListener('scroll', function (event) {
        if (state.menuItemId && !(event.target.closest && event.target.closest('.ga-status-menu'))) closeMenu();
    }, true);

    doc.addEventListener('focusout', function () {
        if (!state.renderPending) return;
        global.setTimeout(function () {
            if (state.renderPending && !editingInTables()) rerender(false);
        }, 0);
    });

    // ---- Save ----

    // «Сохранить и закрыть» stops when an item of ГА is «Нет» without a note.
    async function checkBeforeSave() {
        if (!visible() || !rights().edit || readOnlyWindow()) return true;
        // Typed notes are saved first.
        const pendingKeys = Object.keys(state.pending);
        for (const key of pendingKeys) {
            const [itemId, field] = key.split(':');
            if (field === 'note' || field === 'linkText') {
                await saveText(field, itemId, String(state.pending[key] || '').trim());
            }
        }
        const sid = sessionId();
        if (!sid) return true;
        let result;
        try {
            result = await getJson('api/ga/check', { dialogId, sessionId: sid, userId: identity().userId });
        } catch (error) {
            return true;
        }
        if (!result.missingNotes || !result.missingNotes.length) {
            state.stopped = false;
            return true;
        }
        state.stopped = true;
        await load();
        redraw();
        say('error', 'Заполните примечание у пунктов Граданализа со статусом «Нет»');
        const first = doc.querySelector('[data-ga-row="' + result.missingNotes[0] + '"]');
        if (first) {
            first.scrollIntoView({ block: 'center', behavior: 'smooth' });
            focusNote(result.missingNotes[0]);
        }
        return false;
    }

    // ---- lifecycle ----

    function defersOprLock() {
        return !!(state.data && state.data.rights && state.data.rights.defersOprLock && !readOnlyWindow());
    }

    function ready() {
        return Promise.race([
            firstLoad,
            new Promise(resolve => global.setTimeout(resolve, FIRST_LOAD_WAIT_MS))
        ]);
    }

    function progressCounts() {
        if (state.data && state.data.progress) return state.data.progress;
        const initial = bootstrap.gaProgress || {};
        return { done: Number(initial.done || 0), total: Number(initial.total || 0) };
    }

    function hasDraft() {
        return !!(state.data && state.data.draft);
    }

    global.addEventListener('checklist-edit-session-state', function (event) {
        const detail = event && event.detail || {};
        if (detail.status === 'active' && detail.sessionId) load();
    });
    global.addEventListener('focus', function () {
        if (isOpr()) load();
    });
    // Who holds ГА and OPR, and what others saved meanwhile.
    global.setInterval(function () {
        if (isOpr() && !doc.hidden && !state.busy && !finalizing()) load();
    }, REFRESH_MS);

    Promise.resolve(
        typeof fetchCurrentUserIfPossible === 'function' ? fetchCurrentUserIfPossible() : null
    ).catch(() => null).then(load);

    global.ChecklistGa = Object.freeze({
        load,
        ready,
        state,
        showsPanel,
        panelHtml,
        notRequiredRowsHtml,
        afterRender,
        canEditNow,
        defersOprLock,
        progressCounts,
        hasDraft,
        checkBeforeSave
    });
})(window);
