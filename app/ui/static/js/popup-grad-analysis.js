(function (global) {
    'use strict';

    // Граданализ (ГА): its own tab of the checklist window, between ИД and
    // ОПР, with the checklists to КОПР Генплан and КОПР АР, ПБ, ОДИ.
    // Everybody sees it; leaders, administrators and the architects of the
    // project change it (an architect names other architects too, with him or
    // instead of him). A change goes to the draft of the edit session of
    // the window and is kept by «Сохранить» / «Сохранить и закрыть»;
    // «Отмена» drops it. ГА has its own lock, taken by the first change, and
    // its own progress; its items in «Не требуется» are at the end of the tab.
    // A link to a file of ИД stays on the version it was made to.

    const doc = global.document;
    const popupApi = global.ChecklistPopupApi;
    const bootstrap = global.CHECKLIST_POPUP_BOOTSTRAP || {};
    const dialogId = String(bootstrap.dialogId || '');
    if (!popupApi || !dialogId) return;

    const KEY = 'ga';
    const TITLE = 'Чек-лист Граданализ';
    const REFRESH_MS = 30000;
    const STALE_MS = 10000;
    const FIRST_LOAD_WAIT_MS = 4000;
    const STATUS_TITLES = { yes: 'Да', no: 'Нет', '': 'Нет', nr: 'Не требуется' };
    // Icons of the checklist buttons (upload, folder, replace, download,
    // remove) come from the shared set; the ones the checklists have no
    // button for are drawn the same way.
    const OWN_ICONS = {
        pencil: '<path d="M4 20h4L19 9l-4-4L4 16v4Z"></path><path d="m13.5 6.5 4 4"></path>',
        link: '<path d="M10 14a4.5 4.5 0 0 0 6.4 0l3-3a4.5 4.5 0 0 0-6.4-6.4l-1.2 1.2"></path>'
            + '<path d="M14 10a4.5 4.5 0 0 0-6.4 0l-3 3a4.5 4.5 0 0 0 6.4 6.4l1.2-1.2"></path>',
        eye: '<path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12Z"></path>'
            + '<circle cx="12" cy="12" r="3"></circle>',
        file: '<path d="M14 3H6.5A1.5 1.5 0 0 0 5 4.5v15A1.5 1.5 0 0 0 6.5 21h11a1.5 1.5 0 0 0 1.5-1.5V8Z"></path>'
            + '<path d="M14 3v5h5"></path>'
    };

    function icon(name) {
        const shared = global.ChecklistActionIcons;
        if (!OWN_ICONS[name] && shared && typeof shared.svg === 'function') {
            return shared.svg(name);
        }
        return '<svg class="checklist-action-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false"'
            + ' fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">'
            + (OWN_ICONS[name] || '') + '</svg>';
    }

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

    function isTab() {
        return typeof currentChecklistKey !== 'undefined' && currentChecklistKey === KEY;
    }

    function rights() {
        return (state.data && state.data.rights) || {};
    }

    function visible() {
        return !!(state.data && state.data.visible);
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
        if (force) redraw();
        else if (JSON.stringify(result) !== before) rerender(false);
    }

    function editingInTables() {
        const active = doc.activeElement;
        return !!(active && active.closest && active.closest('.tables-grid')
            && /^(INPUT|TEXTAREA)$/.test(active.tagName));
    }

    function rerender(force) {
        if (!isTab()) return;
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

    // Files go one by one; a file of a dropped folder keeps the path inside
    // it. Returns the files that did not go.
    async function uploadFiles(itemId, files, replaceFileId) {
        const failed = [];
        if (!canEditNow() || !files || !files.length) return Array.from(files || []);
        let sid = '';
        try {
            sid = await requireEditingSession('загрузка файла', { requireCurrentLock: false });
        } catch (error) {
            say('error', error && error.message ? error.message : 'Сессия редактирования не готова');
            return Array.from(files);
        }
        const staging = global.ChecklistUploadStaging;
        const who = identity();
        let result = null;
        let lastError = '';
        state.busy += 1;
        try {
            for (let index = 0; index < files.length; index += 1) {
                const file = files[index];
                say('saving', 'Загружаем ' + file.name + '...');
                const form = new FormData();
                form.append('dialogId', dialogId);
                form.append('sessionId', sid);
                form.append('itemId', itemId);
                form.append('replaceFileId', replaceFileId || '');
                form.append('relativeFolder', staging && typeof staging.fileFolderPath === 'function'
                    ? staging.fileFolderPath(file) : '');
                form.append('actingUserId', who.userId);
                form.append('actingUserName', who.userName);
                form.append('file', file, file.name);
                try {
                    const response = await global.fetch(popupApi.url('api/ga/upload'), { method: 'POST', body: form });
                    const payload = await response.json().catch(() => ({}));
                    if (!response.ok || payload.ok === false) {
                        throw new Error(payload.error || ('HTTP ' + response.status));
                    }
                    result = payload;
                } catch (error) {
                    failed.push(file);
                    lastError = error && error.message ? error.message : '';
                }
            }
            if (result) apply(result, true);
            if (typeof updateLockNotice === 'function') updateLockNotice();
            if (failed.length) {
                say('error', 'Не загружено файлов: ' + failed.length + (lastError ? ' — ' + lastError : ''));
            }
        } finally {
            state.busy -= 1;
        }
        return failed;
    }

    // The upload area of the checklists: a click opens it, files and
    // folders are dropped on the button or into the area, «Загрузить» sends.
    function attachUploadArea(trigger, input, itemId, stateKey) {
        const staging = global.ChecklistUploadStaging;
        const toolbar = trigger.closest('.doc-actions');
        if (!staging || typeof staging.create !== 'function' || !toolbar || !input) return;
        staging.create({
            trigger,
            input,
            mount: toolbar,
            prepend: true,
            hideTriggerWhenExpanded: true,
            stateKey,
            canInteract() {
                return canEditNow();
            },
            async onConfirm(files) {
                const failed = await uploadFiles(itemId, files, '');
                await refreshOpenFolder();
                return { failedFiles: failed };
            },
            onError(error) {
                say('error', error && error.message ? error.message : 'Ошибка загрузки файлов');
            }
        });
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

    function fileHref(file, download) {
        if (file.kind === 'upload') {
            // A file of the draft opens for the session that uploaded it.
            return url('api/ga/file', {
                dialogId, fileId: file.id, userId: identity().userId, sessionId: sessionId(),
                download: download ? '1' : ''
            });
        }
        if (!file.url) return '';
        return popupApi.url(file.url) + (download ? '&download=1' : '');
    }

    function sizeText(size) {
        return typeof formatFileSize === 'function' ? formatFileSize(size || 0) : '';
    }

    function filesWord(count) {
        return typeof pluralFiles === 'function' ? pluralFiles(count) : count + ' файл.';
    }

    // ---- rendering: the panel ----

    function showsPanel() {
        return isTab() && visible();
    }

    function headerHtml() {
        const data = state.data;
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

    function counterHtml(item) {
        const active = childrenOf(item.id).filter(child => child.status !== 'nr');
        if (item.parentId || !active.length) return '';
        const done = active.filter(child => child.status === 'yes').length;
        return '<span class="subitem-counter' + (done === active.length ? ' is-complete' : '') + '"'
            + ' title="Выполнено подпунктов: ' + done + ' из ' + active.length + '">'
            + '<svg class="subitem-counter-icon" viewBox="0 0 16 16" aria-hidden="true">'
            + '<circle cx="4" cy="3.5" r="1.8"></circle><circle cx="12" cy="12.5" r="1.8"></circle>'
            + '<path d="M4 5.3v3.2a4 4 0 0 0 4 4h2.2"></path></svg>'
            + '<span>' + done + '/' + active.length + '</span></span>';
    }

    function nameCellHtml(item, editable) {
        const isSub = !!item.parentId;
        let name;
        if (editable && item.isCustom && state.nameEditor === item.id) {
            const value = state.pending[item.id + ':name'] !== undefined ? state.pending[item.id + ':name'] : item.name;
            name = '<textarea class="ga-input ga-autogrow" rows="1" data-ga-name-input="' + esc(item.id) + '">' + esc(value) + '</textarea>';
        } else if (editable && item.isCustom) {
            // As in the checklists: a click on the name edits it.
            name = '<button type="button" class="ga-name ga-name-trigger" data-ga-rename="' + esc(item.id) + '"'
                + ' title="Нажмите на описание, чтобы изменить">' + esc(item.name) + '</button>';
        } else {
            name = '<span class="ga-name">' + esc(item.name) + '</span>';
        }
        const addSub = editable && !isSub && item.status !== 'nr';
        const remove = editable && item.isCustom
            ? '<button type="button" class="subitem-add-cancel ga-delete' + (addSub ? ' has-add-trigger' : '') + '"'
                + ' data-ga-delete="' + esc(item.id) + '" title="Удалить пункт" aria-label="Удалить пункт ' + esc(item.name) + '">×</button>'
            : '';
        const trigger = addSub
            ? '<button type="button" class="subitem-add-trigger" data-ga-add-sub="' + esc(item.id) + '"'
                + ' title="Добавить подпункт" aria-label="Добавить подпункт к пункту ' + esc(item.name) + '">'
                + '<span class="subitem-add-plus" aria-hidden="true">+</span>'
                + '<span class="subitem-add-label">Добавить подпункт</span></button>'
            : '';
        return '<div class="ga-cell ga-cell-name"><div class="cell-name">'
            + (isSub ? '<span class="ga-sub-mark" aria-hidden="true">↳</span>' : '')
            + '<div class="item-name-wrap">' + name + counterHtml(item) + '</div>'
            + remove + trigger
            + '</div></div>';
    }

    // «Заменить» of an upload, or «Сослаться на новую версию» of a link
    // whose version of ИД went to the archive.
    function replaceButtonHtml(itemId, file, editable) {
        if (editable && file.kind === 'upload') {
            return '<button class="doc-file-replace checklist-action-button checklist-action-button-replace" type="button"'
                + ' data-ga-replace="' + esc(file.id) + '" data-ga-item="' + esc(itemId) + '"'
                + ' title="Заменить файл" aria-label="Заменить файл ' + esc(file.name) + '">' + icon('replace') + '</button>';
        }
        if (editable && file.kind === 'link' && file.canRelink) {
            const target = file.newerName ? ' «' + file.newerName + '»' : '';
            return '<button class="doc-file-replace checklist-action-button checklist-action-button-replace ga-relink" type="button"'
                + ' data-ga-relink="' + esc(file.id) + '" data-ga-item="' + esc(itemId) + '"'
                + ' title="' + esc('Сослаться на новую версию из ИД' + target) + '" aria-label="Сослаться на новую версию из ИД">'
                + icon('replace') + '</button>';
        }
        return '';
    }

    // «Убрать»: a link leaves the item, an upload goes to the archive of
    // the item; both stay in its folder under «Архив версий».
    function removeButtonHtml(itemId, file, editable) {
        if (!editable) return '';
        const title = file.kind === 'link' ? 'Убрать ссылку' : 'Убрать файл в архив пункта';
        return '<button class="checklist-action-button checklist-action-button-remove ga-file-remove" type="button"'
            + ' data-ga-unlink="' + esc(file.id) + '" data-ga-item="' + esc(itemId) + '"'
            + ' title="' + title + '" aria-label="' + esc(title + ' ' + file.name) + '">' + icon('remove') + '</button>';
    }

    function fileMarkHtml(file) {
        if (!file.mark) return '';
        const kind = file.state === 'deleted' ? ' is-deleted' : file.state === 'archived' ? ' is-archived' : '';
        return '<div class="ga-file-mark' + kind + '">' + esc(file.mark) + '</div>';
    }

    // A file row of the checklists: name, size, «Заменить», «Скачать»,
    // and «Убрать» for those who fill ГА.
    function fileRowHtml(itemId, file, editable) {
        const href = fileHref(file, false);
        const name = href
            ? '<a class="doc-file-link" href="' + esc(href) + '" target="_blank" rel="noopener">' + esc(file.name) + '</a>'
            : '<span class="doc-file-link is-gone">' + esc(file.name) + '</span>';
        const where = file.kind === 'link' ? (sourcePathText(file) || file.sourceItemName || '') : '';
        const meta = file.kind === 'link'
            ? '<span class="doc-file-meta ga-id-link" title="Ссылка на файл чек-листа ИД' + (where ? ' · ' + esc(where) : '') + '">ИД</span>'
            : '<span class="doc-file-meta">' + esc(sizeText(file.size)) + '</span>';
        const replace = replaceButtonHtml(itemId, file, editable) || '<span></span>';
        const download = href
            ? '<a class="doc-file-download checklist-action-button checklist-action-button-download"'
                + ' href="' + esc(fileHref(file, true)) + '" download title="Скачать" aria-label="Скачать ' + esc(file.name) + '">'
                + icon('download') + '</a>'
            : '<span></span>';
        const remove = editable ? removeButtonHtml(itemId, file, editable) : '';
        return '<div class="doc-file-block"><div class="doc-file-row' + (editable ? ' ga-file-row-editable' : '') + '">'
            + name + meta + replace + download + remove + '</div>' + fileMarkHtml(file) + '</div>';
    }

    // Files of a dropped folder: one row per top folder, as in the checklists.
    function folderRowsHtml(item) {
        const counts = new Map();
        (item.files || []).forEach(file => {
            const top = text(file.folder).split('/')[0];
            if (top) counts.set(top, (counts.get(top) || 0) + 1);
        });
        if (!counts.size) return '';
        return '<div class="doc-folders">' + Array.from(counts.entries())
            .sort((left, right) => left[0].localeCompare(right[0], 'ru'))
            .map(([folder, count]) => (
                '<button class="doc-folder-row" type="button" data-ga-folder="' + esc(item.id) + '" title="Открыть папку «' + esc(folder) + '»">'
                + '<span class="doc-folder-row-icon" aria-hidden="true">📁</span>'
                + '<span class="doc-folder-row-name">' + esc(folder) + '</span>'
                + '<span class="doc-folder-row-meta">· ' + esc(filesWord(count)) + '</span></button>'
            )).join('') + '</div>';
    }

    function linkCellHtml(item, editable) {
        const files = item.files || [];
        const rootFiles = files.filter(file => !text(file.folder));
        const editing = editable && state.linkEditor[item.id];
        let textHtml = '';
        if (editing) {
            const value = state.pending[item.id + ':linkText'] !== undefined ? state.pending[item.id + ':linkText'] : item.linkText;
            textHtml = '<textarea class="ga-input ga-autogrow" rows="1" placeholder="Нормативный документ, письмо, раздел"'
                + ' data-ga-link-text="' + esc(item.id) + '">' + esc(value) + '</textarea>'
                + '<div class="ga-link-edit-actions"><button type="button" class="add-item-btn" data-ga-link-done="' + esc(item.id) + '">Готово</button></div>';
        } else if (text(item.linkText)) {
            textHtml = '<div class="ga-link-text">' + esc(item.linkText) + '</div>';
        }
        const tools = [];
        if (editable) {
            tools.push('<button class="upload-btn doc-icon-btn checklist-action-button checklist-action-button-upload" type="button"'
                + ' data-ga-upload-trigger="' + esc(item.id) + '" title="Загрузить файлы в пункт" aria-label="Загрузить файлы в пункт">'
                + icon('upload') + '</button>');
            tools.push('<button class="doc-icon-btn checklist-action-button checklist-action-button-folder" type="button"'
                + ' data-ga-link-pick="' + esc(item.id) + '" title="Сослаться на файл чек-листа ИД" aria-label="Сослаться на файл чек-листа ИД">'
                + icon('link') + '</button>');
        }
        if (files.length || item.archiveCount) {
            tools.push('<button class="doc-btn doc-icon-btn checklist-action-button checklist-action-button-folder" type="button"'
                + ' data-ga-folder="' + esc(item.id) + '" title="Посмотреть папку" aria-label="Посмотреть папку">'
                + icon('folder') + '</button>');
        }
        if (editable && !editing) {
            tools.push('<button class="doc-icon-btn checklist-action-button checklist-action-button-folder" type="button"'
                + ' data-ga-link-edit="' + esc(item.id) + '" title="Редактировать пункт" aria-label="Редактировать пункт">'
                + icon('pencil') + '</button>');
        }
        const toolbar = tools.length
            ? '<div class="doc-actions" role="toolbar" aria-label="Действия с документами пункта">' + tools.join('') + '</div>'
            : '';
        const filesBlock = rootFiles.length
            ? '<div class="doc-files">' + rootFiles.map(file => fileRowHtml(item.id, file, editable)).join('') + '</div>'
            : '';
        const input = editable
            ? '<input type="file" data-ga-file-input="' + esc(item.id) + '" style="display:none;" multiple>'
            : '';
        const empty = !textHtml && !files.length && !editable ? '<span class="ga-placeholder">—</span>' : '';
        return '<div class="ga-cell ga-cell-link">' + textHtml + empty
            + '<div class="doc-cell">' + toolbar + filesBlock + folderRowsHtml(item) + input + '</div>'
            + '</div>';
    }

    function reviewCellHtml(item) {
        const review = item.review;
        if (!review) {
            return '<div class="ga-cell ga-cell-review"><span class="ga-placeholder">—</span></div>';
        }
        const eye = item.reviewCount > 1
            ? '<button type="button" class="checklist-action-button ga-history-button" data-ga-history="' + esc(item.id) + '"'
                + ' title="Посмотреть все изменения" aria-label="Посмотреть все изменения">' + icon('eye') + '</button>'
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
            ? '<div class="add-item-row subitem-add-row ga-add-sub">'
                + '<input class="add-item-input subitem-add-input" type="text" maxlength="1000" placeholder="Название подпункта" data-ga-new-sub="' + esc(item.id) + '">'
                + '<button class="add-item-btn" type="button" data-ga-new-sub-save="' + esc(item.id) + '">Добавить подпункт</button>'
                + '<button class="subitem-add-cancel" type="button" data-ga-new-sub-cancel title="Отмена" aria-label="Отмена">×</button></div>'
            : '';
        return row + adding;
    }

    function groupHtml(group, editable) {
        const shown = items().filter(item => item.group === group.id && item.status !== 'nr' && !item.hidden);
        const rows = shown.map(item => rowHtml(item, editable)).join('');
        const add = editable
            ? '<div class="add-item-row"><input class="add-item-input" type="text" maxlength="1000" placeholder="Новый пункт" data-ga-new-item="' + esc(group.id) + '">'
                + '<button class="add-item-btn" type="button" data-ga-new-item-save="' + esc(group.id) + '">Добавить пункт</button></div>'
            : '';
        return '<div class="ga-group" data-ga-group="' + esc(group.id) + '">'
            + '<div class="ga-group-title">' + esc(group.title) + '</div>'
            + (rows || '<div class="ga-empty">Все пункты раздела в «Не требуется»</div>')
            + add
            + '</div>';
    }

    function panelHtml() {
        if (!showsPanel()) return '';
        // Back to the tab from another checklist: ГА is read again.
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
        return '<div class="ga-panel ga-tab' + (editable ? ' is-editable' : '') + '">'
            + headerHtml()
            + blocks
            + notRequiredHtml(editable)
            + '<input type="file" hidden data-ga-replace-input>'
            + '</div>';
    }

    // «Не требуется» at the end of the tab, by sections; subitems of an item
    // go with it.
    function notRequiredHtml(editable) {
        const list = items().filter(item => item.status === 'nr' && !item.hidden);
        if (!list.length) return '';
        const groups = (state.data.groups || []).filter(group => list.some(item => item.group === group.id));
        return '<section class="ga-block ga-block-nr" data-ga-block="nr">'
            + '<div class="ga-block-title">Не требуется</div>'
            + '<div class="ga-grid-scroll"><div class="ga-grid">'
            + columnsHtml()
            + groups.map(group => (
                '<div class="ga-group" data-ga-nr-group="' + esc(group.id) + '">'
                + '<div class="ga-group-title">' + esc(group.title) + '</div>'
                + list.filter(item => item.group === group.id).map(item => rowHtml(item, editable)).join('')
                + '</div>'
            )).join('')
            + '</div></div>'
            + '</section>';
    }

    function afterRender(root) {
        const scope = root || doc;
        scope.querySelectorAll('.ga-autogrow').forEach(grow);
        scope.querySelectorAll('[data-ga-upload-trigger]').forEach(trigger => {
            const itemId = trigger.dataset.gaUploadTrigger;
            const input = scope.querySelector('[data-ga-file-input="' + itemId + '"]');
            attachUploadArea(trigger, input, itemId, 'ga:' + dialogId + ':' + itemId);
        });
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
            + '<button type="button" class="checklist-action-button" data-ga-modal-close title="Закрыть" aria-label="Закрыть">' + icon('remove') + '</button></div>'
            + '<div class="ga-modal-body">' + bodyHtml + '</div></div>';
        doc.body.appendChild(backdrop);
        return backdrop;
    }

    async function itemDetails(itemId) {
        const who = identity();
        return (await getJson('api/ga/item', { dialogId, itemId, userId: who.userId, sessionId: sessionId() })).item;
    }

    // Where a linked file lies in ИД: its item, subitem and folders; every
    // part opens its folder page of ИД.
    function sourcePathHtml(file) {
        const path = Array.isArray(file.sourcePath) ? file.sourcePath : [];
        if (!path.length) {
            return 'Ссылка на ИД' + (file.sourceItemName ? ' · ' + esc(file.sourceItemName) : '');
        }
        return '<div class="ga-source">Ссылка на ИД:</div><div class="ga-source-path">'
            + path.map(part => (
                '<a class="ga-source-link" href="' + esc(idFolderUrl(file.sourceDialogId, part.itemId, part.folder)) + '"'
                + ' target="_blank" rel="noopener" title="' + esc('Открыть папку «' + part.label + '» в чек-листе ИД') + '">'
                + esc(part.label) + '</a>'
            )).join('<span class="ga-source-sep">/</span>')
            + '</div>';
    }

    function sourcePathText(file) {
        return (Array.isArray(file.sourcePath) ? file.sourcePath : []).map(part => part.label).join(' / ');
    }

    // A file row of the folder page of the checklists: the name with
    // «Заменить» and «Скачать» next to it, then where it came from, when and
    // who added it.
    function folderFileRowHtml(file, archived, editable, itemId) {
        const href = fileHref(file, false);
        const folder = text(file.folder) ? '<span class="ga-muted">' + esc(file.folder) + ' / </span>' : '';
        const name = href
            ? '<a class="folder-document-name" href="' + esc(href) + '" target="_blank" rel="noopener noreferrer" title="Открыть файл: ' + esc(file.name) + '">' + esc(file.name) + '</a>'
            : '<span class="folder-document-name is-gone">' + esc(file.name) + '</span>';
        let replace = '';
        if (!archived && editable && file.kind === 'upload') {
            replace = '<button class="folder-replace-upload-button checklist-action-button checklist-action-button-replace" type="button"'
                + ' data-ga-replace="' + esc(file.id) + '" data-ga-item="' + esc(itemId) + '"'
                + ' title="Загрузить новую версию файла" aria-label="Заменить файл">' + icon('replace') + '</button>';
        } else if (!archived) {
            replace = replaceButtonHtml(itemId, file, editable);
        }
        const remove = !archived ? removeButtonHtml(itemId, file, editable) : '';
        const download = href
            ? '<a class="folder-download-button checklist-action-button checklist-action-button-download"'
                + ' href="' + esc(fileHref(file, true)) + '" download title="Скачать файл" aria-label="Скачать файл ' + esc(file.name) + '">'
                + icon('download') + '</a>'
            : '';
        const kind = file.kind === 'link' ? sourcePathHtml(file) : 'Загружен';
        const mark = fileMarkHtml(file);
        const archivedWhat = file.reason === 'unlinked'
            ? 'Ссылка убрана'
            : file.reason === 'replaced'
                ? (file.kind === 'link' ? 'Заменена ссылкой на новую версию' : 'Заменён')
                : 'Перенесён в архив';
        const archivedInfo = archived
            ? '<div class="ga-muted">' + archivedWhat + ' '
                + esc(formatMoment(file.archivedAt)) + (file.archivedByName ? ' · ' + esc(file.archivedByName) : '') + '</div>'
            : '';
        return '<tr><td><div class="folder-document-name-layout">' + folder + name
            + '<span class="folder-document-inline-actions">' + replace + download + remove + '</span></div>'
            + mark + archivedInfo + '</td>'
            + '<td>' + (file.kind === 'upload' ? esc(sizeText(file.size)) : '') + '</td>'
            + '<td>' + kind + '</td>'
            + '<td>' + esc(formatMoment(file.at)) + '</td>'
            + '<td>' + esc(file.byName || '—') + '</td></tr>';
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
        const head = '<tr><th>Файл</th><th>Размер</th><th>Источник</th><th>Добавлен</th><th>Кто добавил</th></tr>';
        const current = (item.files || []).length
            ? '<table class="ga-table">' + head + item.files.map(file => folderFileRowHtml(file, false, editable, itemId)).join('') + '</table>'
            : '<div class="ga-muted">Файлов нет</div>';
        const archive = (item.archive || []).length
            ? '<table class="ga-table">' + head + item.archive.map(file => folderFileRowHtml(file, true, false, itemId)).join('') + '</table>'
            : '<div class="ga-muted">Архив пуст</div>';
        const actions = editable
            ? '<div class="doc-cell ga-modal-actions"><div class="doc-actions" role="toolbar" aria-label="Действия с файлами пункта">'
                + '<button class="upload-btn doc-icon-btn checklist-action-button checklist-action-button-upload" type="button"'
                + ' data-ga-folder-upload="' + esc(itemId) + '" title="Загрузить файлы в пункт" aria-label="Загрузить файлы в пункт">' + icon('upload') + '</button>'
                + '<button class="doc-icon-btn checklist-action-button checklist-action-button-folder" type="button"'
                + ' data-ga-link-pick="' + esc(itemId) + '" title="Сослаться на файл чек-листа ИД" aria-label="Сослаться на файл чек-листа ИД">' + icon('link') + '</button>'
                + '</div><input type="file" data-ga-folder-input="' + esc(itemId) + '" style="display:none;" multiple></div>'
            : '';
        const modal = openModal('Папка пункта: ' + item.name.trim(),
            actions
            + '<h4>Файлы</h4>' + current
            + '<h4>Архив версий</h4>' + archive,
            'ga-modal-folder');
        modal.dataset.gaFolderItem = itemId;
        const trigger = modal.querySelector('[data-ga-folder-upload]');
        if (trigger) {
            attachUploadArea(trigger, modal.querySelector('[data-ga-folder-input]'), itemId, 'ga-folder:' + dialogId + ':' + itemId);
        }
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

    // Icons of the folder page of the checklists.
    const TREE_FOLDER_PATH = 'M2.5 6.5A1.5 1.5 0 0 1 4 5h5l2 2.2h9a1.5 1.5 0 0 1 1.5 1.5v9.8A1.5 1.5 0 0 1 20 20H4a1.5 1.5 0 0 1-1.5-1.5Z';
    const TREE_EYE = '<svg class="ga-tree-eye" viewBox="0 0 24 24" aria-hidden="true">'
        + '<path d="M1.5 12S5.5 4.5 12 4.5 22.5 12 22.5 12 18.5 19.5 12 19.5 1.5 12 1.5 12Z"></path>'
        + '<circle cx="12" cy="12" r="3.2"></circle></svg>';

    function treeFolderIcon(isSubitem) {
        return '<svg class="ga-tree-folder' + (isSubitem ? ' ga-tree-subitem' : '') + '" viewBox="0 0 24 24" aria-hidden="true">'
            + '<path d="' + TREE_FOLDER_PATH + '"></path>'
            + (isSubitem ? '<path d="M8 13.5l2.5 2.5L16 10.5"></path>' : '') + '</svg>';
    }

    // The folder page of an item of ИД (or of a folder inside it), read only.
    function idFolderUrl(sourceDialogId, itemId, folder) {
        const who = identity();
        return url('api/checklist/folder', {
            dialogId: sourceDialogId, checklistKey: 'id', itemId, folder: folder || '',
            readOnly: '1', openedFrom: 'ga', userId: who.userId, userName: who.userName
        });
    }

    function filesCount(count) {
        return count ? filesWord(count) : 'пусто';
    }

    function parentPath(path) {
        const index = path.lastIndexOf('/');
        return index < 0 ? '' : path.slice(0, index);
    }

    function sameFolder(left, right) {
        return String(left || '').toLocaleLowerCase('ru') === String(right || '').toLocaleLowerCase('ru');
    }

    function below(folder, path) {
        const key = String(folder || '').toLocaleLowerCase('ru');
        const base = String(path || '').toLocaleLowerCase('ru');
        return key === base || key.startsWith(base + '/');
    }

    function pickerFileHtml(ctx, document, trail) {
        const linked = ctx.linked.has(ctx.source.dialogId + '|' + document.id);
        const where = trail.concat(document.name).join(' / ');
        return '<li class="ga-tree-file">'
            + '<button type="button" class="ga-pick-doc' + (linked ? ' is-linked' : '') + '"'
            + ' data-ga-pick-source="' + esc(ctx.source.dialogId) + '" data-ga-pick-item="' + esc(ctx.itemId) + '"'
            + ' data-ga-pick-doc="' + esc(document.id) + '" data-ga-target="' + esc(ctx.targetId) + '"'
            + ' data-ga-pick-path="' + esc(where.toLocaleLowerCase('ru')) + '"'
            + ' title="' + esc(linked ? 'Ссылка на этот файл уже есть: ' + where : 'Сослаться на файл: ' + where) + '"'
            + (linked ? ' disabled' : '') + '>'
            + '<span class="ga-file-icon">' + icon('file') + '</span>'
            + '<span class="ga-pick-doc-name">' + esc(document.name) + '</span>'
            + (linked ? '<span class="ga-pick-linked">уже есть</span>' : '')
            + '<span class="ga-muted">' + esc(formatMoment(document.at)) + '</span></button></li>';
    }

    function pickerNodeHtml(options) {
        const body = (options.files ? '<ul class="ga-tree-files">' + options.files + '</ul>' : '')
            + (options.children ? '<ul class="ga-tree-children">' + options.children + '</ul>' : '');
        return '<li class="ga-tree-node" data-ga-tree-label="' + esc(options.trail.join(' / ').toLocaleLowerCase('ru')) + '">'
            + '<details class="ga-tree-details"' + (options.open ? ' open data-default-open="1"' : '') + '>'
            + '<summary class="ga-tree-summary">'
            + '<span class="ga-tree-chevron" aria-hidden="true"></span>'
            + treeFolderIcon(options.isSubitem)
            + '<span class="ga-tree-name">' + esc(options.label) + '</span>'
            + '<span class="ga-tree-meta">' + esc(filesCount(options.count)) + '</span>'
            + '<a class="ga-tree-view" href="' + esc(options.viewUrl) + '" target="_blank" rel="noopener"'
            + ' title="Посмотреть папку в чек-листе ИД">' + TREE_EYE + '<span>Посмотреть папку</span></a>'
            + '</summary>'
            + '<div class="ga-tree-body">' + (body || '<div class="ga-tree-empty">Пустая папка</div>') + '</div>'
            + '</details></li>';
    }

    // The folders inside an item: all of them, as on its folder page.
    function pickerFoldersHtml(ctx, node, parent, trail) {
        return (node.folders || [])
            .filter(path => sameFolder(parentPath(path), parent))
            .map(path => {
                const label = path.slice(path.lastIndexOf('/') + 1);
                const here = trail.concat(label);
                const files = (node.documents || []).filter(document => sameFolder(document.folder, path))
                    .map(document => pickerFileHtml(ctx, document, here)).join('');
                return pickerNodeHtml({
                    label, trail: here, isSubitem: false, open: false, files,
                    count: (node.documents || []).filter(document => below(document.folder, path)).length,
                    children: pickerFoldersHtml(ctx, node, path, here),
                    viewUrl: idFolderUrl(ctx.source.dialogId, node.itemId, path)
                });
            }).join('');
    }

    // An item of ИД: its own files, its folders, then its subitems.
    function pickerItemHtml(source, node, targetId, linked, trail, depth) {
        const here = trail.concat(node.name);
        const ctx = { source, itemId: node.itemId, targetId, linked };
        const files = (node.documents || []).filter(document => !document.folder)
            .map(document => pickerFileHtml(ctx, document, here)).join('');
        const children = pickerFoldersHtml(ctx, node, '', here)
            + (node.children || []).map(child => pickerItemHtml(source, child, targetId, linked, here, depth + 1)).join('');
        return pickerNodeHtml({
            label: node.name, trail: here, isSubitem: depth > 0, open: depth === 0, files, children,
            count: node.fileCount, viewUrl: idFolderUrl(source.dialogId, node.itemId, '')
        });
    }

    async function openPicker(itemId) {
        let sources;
        try {
            sources = (await getJson('api/ga/id-files', { dialogId, userId: identity().userId })).sources || [];
        } catch (error) {
            say('error', error.message);
            return;
        }
        const target = itemById(itemId);
        const linked = new Set(((target && target.files) || [])
            .filter(file => file.kind === 'link' && file.documentId)
            .map(file => file.sourceDialogId + '|' + file.documentId));
        const body = sources.map(source => {
            const tree = (source.tree || []).map(node => pickerItemHtml(source, node, itemId, linked, [], 0)).join('');
            return '<section class="ga-pick-source" data-ga-pick-source-block>'
                + '<h4>' + esc(source.label) + '</h4>'
                + (tree ? '<ul class="ga-tree-root">' + tree + '</ul>' : '<div class="ga-muted">В чек-листе ИД нет файлов</div>')
                + '</section>';
        }).join('');
        openModal('Сослаться на файл чек-листа ИД',
            '<input type="search" class="ga-input ga-pick-search" placeholder="Найти файл, папку или пункт" data-ga-pick-search>'
            + '<div class="ga-pick-nothing ga-muted" hidden>Ничего не найдено</div>'
            + body, 'ga-modal-picker');
    }

    // The search shows the files whose path has the text, with their
    // folders open; a folder found by its name shows all its files.
    function filterPicker(query) {
        const modal = doc.querySelector('.ga-modal-picker');
        if (!modal) return;
        const nodes = Array.from(modal.querySelectorAll('.ga-tree-node'));
        const files = Array.from(modal.querySelectorAll('.ga-tree-file'));
        if (!query) {
            nodes.forEach(node => {
                node.hidden = false;
                const details = node.querySelector(':scope > .ga-tree-details');
                if (details) details.open = !!details.dataset.defaultOpen;
            });
            files.forEach(file => { file.hidden = false; });
            modal.querySelectorAll('[data-ga-pick-source-block]').forEach(block => { block.hidden = false; });
            modal.querySelector('.ga-pick-nothing').hidden = true;
            return;
        }
        const found = new Set(nodes.filter(node => (node.dataset.gaTreeLabel || '').indexOf(query) >= 0));
        files.forEach(file => {
            const button = file.querySelector('[data-ga-pick-path]');
            const inFound = Array.from(found).some(node => node.contains(file));
            file.hidden = !(inFound || (button && button.dataset.gaPickPath.indexOf(query) >= 0));
        });
        nodes.slice().reverse().forEach(node => {
            const visible = found.has(node) || Array.from(node.querySelectorAll('.ga-tree-file')).some(file => !file.hidden);
            node.hidden = !visible;
            const details = node.querySelector(':scope > .ga-tree-details');
            if (details) details.open = visible;
        });
        let any = false;
        modal.querySelectorAll('[data-ga-pick-source-block]').forEach(block => {
            const shown = Array.from(block.querySelectorAll('.ga-tree-root > .ga-tree-node')).some(node => !node.hidden);
            block.hidden = !shown;
            any = any || shown;
        });
        modal.querySelector('.ga-pick-nothing').hidden = any;
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
        const who = identity();
        const self = text(userId) === text(who.userId) && !rights().leader;
        if (!global.confirm(self
            ? 'Убрать себя из архитекторов? После этого Граданализ этого проекта будет открыт вам только для просмотра.'
            : 'Убрать сотрудника из архитекторов проекта?')) return;
        try {
            await sendJson('api/ga/architects', { dialogId, op: 'remove', userId, actingUserId: who.userId });
            await load();
            redraw();
        } catch (error) {
            global.alert(error && error.message ? error.message : 'Не удалось убрать архитектора');
        }
    }

    // ---- events (delegated: the panel is redrawn with the checklist) ----

    let replaceTarget = { itemId: '', fileId: '' };

    // «Заменить» of the checklists: the new version takes the place of the
    // file, the old one goes to the archive of versions.
    function pickReplacement(itemId, fileId) {
        const input = doc.querySelector('[data-ga-replace-input]');
        if (!input || !canEditNow()) return;
        replaceTarget = { itemId, fileId };
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
            + '[data-ga-link-pick],[data-ga-folder],[data-ga-history],[data-ga-rename],[data-ga-delete],'
            + '[data-ga-add-sub],[data-ga-new-sub-save],[data-ga-new-sub-cancel],[data-ga-new-item-save],[data-ga-modal-close],'
            + '[data-ga-replace],[data-ga-pick-doc],[data-ga-architect-add],[data-ga-architect-cancel],'
            + '[data-ga-architect-remove],[data-ga-unlink],[data-ga-relink]');
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
        } else if (data.gaReplace !== undefined) {
            pickReplacement(data.gaItem, data.gaReplace);
        } else if (data.gaUnlink !== undefined) {
            const item = itemById(data.gaItem);
            const file = item && (item.files || []).find(entry => entry.id === data.gaUnlink);
            if (!file) return;
            const question = file.kind === 'link'
                ? 'Убрать ссылку на файл «' + file.name + '» из пункта?\n\nОна останется в папке пункта в архиве.'
                : 'Убрать файл «' + file.name + '» в архив пункта?';
            if (!global.confirm(question)) return;
            if (await change('archiveFile', { itemId: data.gaItem, fileId: data.gaUnlink })) await refreshOpenFolder();
        } else if (data.gaRelink !== undefined) {
            if (await change('relink', { itemId: data.gaItem, fileId: data.gaRelink })) await refreshOpenFolder();
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
            filterPicker(field.value.trim().toLocaleLowerCase('ru'));
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
        if (field.dataset.gaReplaceInput !== undefined) {
            const files = Array.from(field.files || []).slice(0, 1);
            const target = replaceTarget;
            field.value = '';
            if (!files.length || !target.fileId) return;
            await uploadFiles(target.itemId, files, target.fileId);
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

    // «Сохранить» stops when an item of ГА is «Нет» without a note; the
    // window goes to the tab of ГА to show it.
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
        if (!isTab() && typeof loadChecklistByKey === 'function') {
            try {
                await loadChecklistByKey(KEY);
            } catch (error) {
                console.log('ГА tab skipped:', error);
            }
        }
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

    // A window opened on the tab draws it before its session starts: the
    // controls come with the session.
    global.addEventListener('checklist-edit-session-state', function (event) {
        const detail = event && event.detail || {};
        if (detail.status === 'active' && detail.sessionId) {
            // A load still running may be of the time before the user was
            // known: the rights come with a fresh one.
            Promise.resolve(state.loading).catch(() => null)
                .then(load)
                .then(function () { rerender(false); });
        }
    });
    global.addEventListener('focus', function () {
        if (isTab()) load();
    });
    // Who holds ГА, and what others saved meanwhile.
    global.setInterval(function () {
        if (isTab() && !doc.hidden && !state.busy && !finalizing()) load();
    }, REFRESH_MS);

    Promise.resolve(
        typeof fetchCurrentUserIfPossible === 'function' ? fetchCurrentUserIfPossible() : null
    ).catch(() => null).then(load);

    global.ChecklistGa = Object.freeze({
        KEY,
        TITLE,
        load,
        ready,
        state,
        isTab,
        showsPanel,
        panelHtml,
        afterRender,
        canEditNow,
        progressCounts,
        hasDraft,
        checkBeforeSave
    });
})(window);
