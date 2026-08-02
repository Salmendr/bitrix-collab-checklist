(function (global) {
    'use strict';

    const state = {
        context: null,
        drafts: [],
        assignmentParts: [],
        userPickers: null,
        companyPicker: null,
        projectCurator: null,
        editingDraftId: '',
        busy: false,
        root: null
    };

    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function normalizeText(value) {
        return String(value == null ? '' : value).trim();
    }

    function normalizeLookupText(value) {
        return normalizeText(value).replace(/\s+/g, ' ').toLocaleLowerCase('ru-RU');
    }

    function normalizeFiles(files) {
        const result = [];
        const seen = new Set();

        (Array.isArray(files) ? files : []).forEach(function (file) {
            const documentId = normalizeText(
                file && (file.documentId || file.id)
            );
            if (!documentId || seen.has(documentId)) return;
            seen.add(documentId);
            result.push({
                documentId,
                seriesId: normalizeText(file && file.seriesId),
                fileName: normalizeText(
                    file && (file.fileName || file.name)
                ) || 'Файл',
                fileSize: Number(file && (file.fileSize || file.size) || 0)
            });
        });

        return result;
    }

    function detectBasePath() {
        const path = String(global.location && global.location.pathname || '/')
            .replace(/\/+$/, '');
        const suffixes = ['/popup', '/api/checklist/folder'];

        for (const suffix of suffixes) {
            if (path === suffix) return '';
            if (path.endsWith(suffix)) {
                return path.slice(0, -suffix.length) || '';
            }
        }
        return '';
    }

    function defaultApiUrl() {
        return (
            global.location.origin
            + detectBasePath()
            + '/api/checklist/notification-drafts'
        );
    }

    function defaultAssignmentPartsApiUrl() {
        return (
            global.location.origin
            + detectBasePath()
            + '/api/checklist/assignment-parts'
        );
    }

    function defaultBitrixUsersApiUrl() {
        return (
            global.location.origin
            + detectBasePath()
            + '/api/checklist/bitrix-users'
        );
    }

    function defaultBitrixCompaniesApiUrl() {
        return (
            global.location.origin
            + detectBasePath()
            + '/api/checklist/bitrix-companies'
        );
    }

    function defaultProjectCuratorApiUrl() {
        return (
            global.location.origin
            + detectBasePath()
            + '/api/checklist/project-curator'
        );
    }

    function ensureUserPickers() {
        const root = state.root;
        if (!root || state.userPickers) return state.userPickers;
        if (!global.ChecklistBitrixUserPicker) {
            throw new Error('ChecklistBitrixUserPicker is not initialized');
        }
        const apiUrl = function () {
            return state.context && state.context.bitrixUsersApiUrl
                ? state.context.bitrixUsersApiUrl
                : defaultBitrixUsersApiUrl();
        };
        state.userPickers = {
            sender: global.ChecklistBitrixUserPicker.create({
                input: root.querySelector('#notificationSenderLookup'),
                results: root.querySelector('#notificationSenderResults'),
                hint: root.querySelector('#notificationSenderLookupHint'),
                refreshButton: root.querySelector('#notificationSenderRefresh'),
                idInput: root.querySelector('#notificationSenderId'),
                nameInput: root.querySelector('#notificationSenderName'),
                apiUrl: apiUrl
            }),
            recipient: global.ChecklistBitrixUserPicker.create({
                input: root.querySelector('#notificationRecipientLookup'),
                results: root.querySelector('#notificationRecipientResults'),
                hint: root.querySelector('#notificationRecipientLookupHint'),
                refreshButton: root.querySelector('#notificationRecipientRefresh'),
                idInput: root.querySelector('#notificationRecipientUserId'),
                nameInput: root.querySelector('#notificationRecipientInternalName'),
                apiUrl: apiUrl
            })
        };
        return state.userPickers;
    }

    function ensureCompanyPicker() {
        const root = state.root;
        if (!root || state.companyPicker) return state.companyPicker;
        if (!global.ChecklistBitrixCompanyPicker) {
            throw new Error('ChecklistBitrixCompanyPicker is not initialized');
        }
        const apiUrl = function () {
            return state.context && state.context.bitrixCompaniesApiUrl
                ? state.context.bitrixCompaniesApiUrl
                : defaultBitrixCompaniesApiUrl();
        };
        state.companyPicker = global.ChecklistBitrixCompanyPicker.create({
            input: root.querySelector('#notificationExternalCompanyLookup'),
            results: root.querySelector('#notificationExternalCompanyResults'),
            hint: root.querySelector('#notificationExternalCompanyHint'),
            refreshButton: root.querySelector('#notificationExternalCompanyRefresh'),
            idInput: root.querySelector('#notificationRecipientCompanyId'),
            titleInput: root.querySelector('#notificationRecipientExternalName'),
            phoneInput: root.querySelector('#notificationRecipientPhone'),
            emailInput: root.querySelector('#notificationRecipientEmail'),
            contactInput: root.querySelector('#notificationRecipientContact'),
            apiUrl: apiUrl
        });
        return state.companyPicker;
    }

    async function requestJson(url, options) {
        const response = await global.fetch(url, options || {});
        const payload = await response.json().catch(function () {
            return {};
        });
        if (!response.ok || payload.ok === false) {
            const error = new Error(
                payload.error || payload.detail || ('HTTP ' + response.status)
            );
            error.status = response.status;
            error.payload = payload;
            throw error;
        }
        return payload;
    }

    function formatDate(value) {
        const raw = normalizeText(value);
        if (!raw) return 'не указан';
        const parts = raw.split('-');
        if (parts.length !== 3) return raw;
        return parts[2] + '.' + parts[1] + '.' + parts[0];
    }

    function formatRecipient(draft) {
        const recipient = draft && draft.recipient;
        if (!recipient) return 'получатель не выбран';
        if (recipient.type === 'external') {
            return recipient.name || 'внешний исполнитель';
        }
        return recipient.name
            || (recipient.bitrixUserId ? 'Bitrix ID ' + recipient.bitrixUserId : '')
            || 'внутренний сотрудник';
    }

    function ensureRoot() {
        if (state.root && global.document.body.contains(state.root)) {
            return state.root;
        }

        const root = global.document.createElement('div');
        root.id = 'notificationDraftOverlay';
        root.className = 'notification-draft-overlay';
        root.hidden = true;
        root.innerHTML = `
            <div class="notification-draft-backdrop" data-role="notification-close"></div>
            <section class="notification-draft-dialog" role="dialog" aria-modal="true" aria-labelledby="notificationDraftTitle">
                <header class="notification-draft-header">
                    <div>
                        <div class="notification-draft-kicker">Черновик оповещения</div>
                        <h2 id="notificationDraftTitle" class="notification-draft-title">Оповещение по документам</h2>
                        <div id="notificationDraftSubtitle" class="notification-draft-subtitle"></div>
                    </div>
                    <button class="notification-draft-close" type="button" data-role="notification-close" aria-label="Закрыть">×</button>
                </header>
                <div class="notification-draft-layout">
                    <form id="notificationDraftForm" class="notification-draft-form" novalidate>
                        <input type="hidden" id="notificationDraftId">
                        <input type="hidden" id="notificationDraftVersion">

                        <div class="notification-bitrix-picker-block">
                            <label class="notification-field">
                                <span>От кого</span>
                                <div class="bitrix-user-picker-search-row">
                                    <input id="notificationSenderLookup" type="search" autocomplete="off" placeholder="Начните вводить ФИО постановщика">
                                    <button id="notificationSenderRefresh" class="notification-secondary-button bitrix-user-picker-refresh" type="button">Обновить</button>
                                </div>
                                <div id="notificationSenderResults" class="bitrix-user-picker-results" hidden></div>
                                <small id="notificationSenderLookupHint" class="bitrix-user-picker-hint"></small>
                            </label>
                            <details class="bitrix-user-picker-manual">
                                <summary>Ручной ввод</summary>
                                <div class="notification-field-grid notification-field-grid--two">
                                    <label class="notification-field">
                                        <span>ФИО постановщика</span>
                                        <input id="notificationSenderName" type="text">
                                    </label>
                                    <label class="notification-field">
                                        <span>Bitrix ID постановщика</span>
                                        <input id="notificationSenderId" type="text" inputmode="numeric">
                                    </label>
                                </div>
                            </details>
                        </div>

                        <label class="notification-field">
                            <span>Тип получателя</span>
                            <select id="notificationRecipientType">
                                <option value="internal">Сотрудник Bitrix24</option>
                                <option value="external">Нет в Bitrix24</option>
                            </select>
                        </label>

                        <div id="notificationInternalRecipientFields" class="notification-bitrix-picker-block">
                            <label class="notification-field">
                                <span>Кому</span>
                                <div class="bitrix-user-picker-search-row">
                                    <input id="notificationRecipientLookup" type="search" autocomplete="off" placeholder="Начните вводить ФИО сотрудника">
                                    <button id="notificationRecipientRefresh" class="notification-secondary-button bitrix-user-picker-refresh" type="button">Обновить</button>
                                </div>
                                <div id="notificationRecipientResults" class="bitrix-user-picker-results" hidden></div>
                                <small id="notificationRecipientLookupHint" class="bitrix-user-picker-hint"></small>
                            </label>
                            <details class="bitrix-user-picker-manual">
                                <summary>Ручной ввод</summary>
                                <div class="notification-field-grid notification-field-grid--two">
                                    <label class="notification-field">
                                        <span>Bitrix ID сотрудника</span>
                                        <input id="notificationRecipientUserId" type="text" inputmode="numeric" placeholder="Например, 25">
                                    </label>
                                    <label class="notification-field">
                                        <span>ФИО сотрудника</span>
                                        <input id="notificationRecipientInternalName" type="text">
                                    </label>
                                </div>
                            </details>
                        </div>

                        <div id="notificationExternalRecipientFields" class="notification-external-fields" hidden>
                            <div class="notification-bitrix-company-picker-block">
                                <label class="notification-field">
                                    <span>Внешний исполнитель</span>
                                    <div class="bitrix-company-picker-search-row">
                                        <input id="notificationExternalCompanyLookup" type="search" autocomplete="off" placeholder="Найдите подрядчика или введите нового">
                                        <button id="notificationExternalCompanyRefresh" class="notification-secondary-button bitrix-company-picker-refresh" type="button">Обновить</button>
                                    </div>
                                    <div id="notificationExternalCompanyResults" class="bitrix-company-picker-results" hidden></div>
                                    <small id="notificationExternalCompanyHint" class="bitrix-company-picker-hint"></small>
                                </label>
                                <input id="notificationRecipientCompanyId" type="hidden">
                            </div>
                            <label class="notification-field">
                                <span>ФИО / название внешнего исполнителя</span>
                                <input id="notificationRecipientExternalName" type="text">
                            </label>
                            <div class="notification-field-grid notification-field-grid--two">
                                <label class="notification-field">
                                    <span>Телефон</span>
                                    <input id="notificationRecipientPhone" type="tel">
                                </label>
                                <label class="notification-field">
                                    <span>Email</span>
                                    <input id="notificationRecipientEmail" type="email">
                                </label>
                            </div>
                            <label class="notification-field">
                                <span>Другие контактные данные</span>
                                <input id="notificationRecipientContact" type="text">
                            </label>
                            <div class="notification-curator-block">
                                <div class="notification-curator-heading">
                                    <span>Внутренний куратор объекта</span>
                                    <button id="notificationCuratorRefresh" class="notification-secondary-button" type="button">Обновить</button>
                                </div>
                                <div id="notificationCuratorResolved" class="notification-curator-resolved"></div>
                                <small id="notificationCuratorHint" class="notification-curator-hint"></small>
                                <details class="notification-curator-manual">
                                    <summary>Ручной выбор куратора</summary>
                                    <div class="notification-field-grid notification-field-grid--two">
                                        <label class="notification-field">
                                            <span>Bitrix ID куратора</span>
                                            <input id="notificationCuratorUserId" type="text" inputmode="numeric">
                                        </label>
                                        <label class="notification-field">
                                            <span>ФИО куратора</span>
                                            <input id="notificationCuratorName" type="text">
                                        </label>
                                    </div>
                                </details>
                            </div>
                        </div>

                        <div class="notification-field-grid notification-field-grid--two">
                            <label class="notification-field notification-assignment-part-field">
                                <span>В какой части задание</span>
                                <input
                                    id="notificationAssignmentPart"
                                    type="text"
                                    list="notificationAssignmentPartOptions"
                                    autocomplete="off"
                                    placeholder="Выберите или введите новый вариант"
                                >
                                <datalist id="notificationAssignmentPartOptions"></datalist>
                                <small id="notificationAssignmentPartHint" class="notification-assignment-part-hint"></small>
                            </label>
                            <label class="notification-field">
                                <span>Крайний срок</span>
                                <input id="notificationDeadline" type="date">
                            </label>
                        </div>

                        <label class="notification-field">
                            <span>Описание задания</span>
                            <textarea id="notificationDescription" rows="3" placeholder="Дополнительные пояснения"></textarea>
                        </label>

                        <fieldset class="notification-files-fieldset">
                            <legend>Файлы текущей версии</legend>
                            <div id="notificationFilesList" class="notification-files-list"></div>
                        </fieldset>

                        <div id="notificationDraftFormState" class="notification-draft-state" aria-live="polite"></div>

                        <div class="notification-draft-form-actions">
                            <button id="notificationNewDraftBtn" class="notification-secondary-button" type="button">Новый черновик</button>
                            <button id="notificationSaveDraftBtn" class="notification-primary-button" type="submit">Сохранить черновик</button>
                        </div>
                    </form>

                    <aside class="notification-draft-sidebar">
                        <div class="notification-draft-sidebar-header">
                            <h3>Черновики пункта</h3>
                            <span id="notificationDraftCount" class="notification-draft-count">0</span>
                        </div>
                        <div id="notificationDraftCards" class="notification-draft-cards"></div>
                    </aside>
                </div>
            </section>
        `;
        global.document.body.appendChild(root);
        state.root = root;
        ensureUserPickers();
        ensureCompanyPicker();

        root.querySelectorAll('[data-role="notification-close"]').forEach(function (element) {
            element.addEventListener('click', close);
        });
        root.querySelector('#notificationRecipientType').addEventListener(
            'change',
            applyRecipientMode
        );
        root.querySelector('#notificationAssignmentPart').addEventListener(
            'input',
            updateAssignmentPartHint
        );
        root.querySelector('#notificationCuratorRefresh').addEventListener(
            'click',
            function () { loadProjectCurator(true); }
        );
        root.querySelector('#notificationNewDraftBtn').addEventListener(
            'click',
            function () { populateForm(null); }
        );
        root.querySelector('#notificationDraftForm').addEventListener(
            'submit',
            saveCurrentDraft
        );
        root.querySelector('#notificationDraftCards').addEventListener(
            'click',
            handleDraftCardClick
        );
        global.document.addEventListener('keydown', function (event) {
            if (event.key === 'Escape' && !root.hidden) close();
        });
        return root;
    }

    function renderAssignmentParts() {
        const root = ensureRoot();
        const datalist = root.querySelector('#notificationAssignmentPartOptions');
        datalist.innerHTML = state.assignmentParts.map(function (part) {
            return '<option value="' + esc(part.text || part.name || '') + '"></option>';
        }).join('');
        updateAssignmentPartHint();
    }

    function findAssignmentPartByText(value) {
        const key = normalizeLookupText(value);
        if (!key) return null;
        return state.assignmentParts.find(function (part) {
            return normalizeLookupText(part.text || part.name) === key;
        }) || null;
    }

    function updateAssignmentPartHint() {
        const root = ensureRoot();
        const input = root.querySelector('#notificationAssignmentPart');
        const hint = root.querySelector('#notificationAssignmentPartHint');
        const text = normalizeText(input.value);
        const existing = findAssignmentPartByText(text);
        if (!text) {
            hint.textContent = 'Начните ввод — приложение покажет варианты общего справочника.';
            hint.dataset.kind = '';
        } else if (existing) {
            hint.textContent = 'Выбран существующий вариант общего справочника.';
            hint.dataset.kind = 'existing';
        } else {
            hint.textContent = 'Новый вариант будет добавлен в общий справочник при сохранении черновика.';
            hint.dataset.kind = 'new';
        }
    }

    async function loadAssignmentParts() {
        const context = state.context;
        const query = new URLSearchParams({ limit: '200' });
        const payload = await requestJson(
            context.assignmentPartsApiUrl + '?' + query.toString(),
            { cache: 'no-store' }
        );
        state.assignmentParts = Array.isArray(payload.assignmentParts)
            ? payload.assignmentParts
            : [];
        renderAssignmentParts();
    }

    function renderProjectCurator() {
        const root = ensureRoot();
        const resolved = root.querySelector('#notificationCuratorResolved');
        const hint = root.querySelector('#notificationCuratorHint');
        const data = state.projectCurator || {};
        const curator = data.curator || {};
        const userId = normalizeText(curator.userId);
        const name = normalizeText(curator.name);

        if (userId) {
            resolved.textContent = (name || 'Сотрудник Bitrix24') + ' · ID ' + userId;
            resolved.dataset.kind = 'resolved';
            root.querySelector('#notificationCuratorUserId').value = userId;
            root.querySelector('#notificationCuratorName').value = name;
            hint.textContent = data.objectTitle
                ? 'Ответственный объекта «' + data.objectTitle + '».'
                : 'Куратор определён по ответственному связанного объекта.';
            hint.dataset.kind = 'success';
            return;
        }

        resolved.textContent = 'Куратор пока не определён';
        resolved.dataset.kind = 'missing';
        hint.textContent = normalizeText(data.resolutionError)
            || (data.configured
                ? 'Нажмите «Обновить» или выберите куратора вручную.'
                : 'В project context отсутствует objectItemId. Доступен ручной выбор.');
        hint.dataset.kind = data.resolutionStatus === 'error' ? 'error' : '';
    }

    async function loadProjectCurator(force) {
        const context = state.context;
        if (!context) return;
        const base = context.projectCuratorApiUrl || defaultProjectCuratorApiUrl();
        const refreshUrl = base + '/refresh';
        try {
            const payload = force
                ? await requestJson(refreshUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ dialogId: context.dialogId })
                })
                : await requestJson(
                    base + '?' + new URLSearchParams({
                        dialogId: context.dialogId,
                        refresh: 'auto'
                    }).toString(),
                    { cache: 'no-store' }
                );
            state.projectCurator = payload.projectCurator || null;
        } catch (error) {
            state.projectCurator = {
                configured: false,
                resolutionStatus: 'error',
                resolutionError: error && error.message ? error.message : 'Не удалось определить куратора',
                curator: { userId: '', name: '' }
            };
        }
        renderProjectCurator();
    }

    function applyRecipientMode() {
        const root = ensureRoot();
        const type = root.querySelector('#notificationRecipientType').value;
        root.querySelector('#notificationInternalRecipientFields').hidden = type !== 'internal';
        root.querySelector('#notificationExternalRecipientFields').hidden = type !== 'external';
    }

    function setBusy(value) {
        state.busy = !!value;
        const root = ensureRoot();
        root.classList.toggle('is-busy', state.busy);
        root.querySelectorAll('button, input, select, textarea').forEach(function (element) {
            if (state.busy) {
                element.dataset.notificationBusyDisabled = element.disabled ? '1' : '0';
                element.disabled = true;
            } else if (element.dataset.notificationBusyDisabled === '0') {
                element.disabled = false;
                delete element.dataset.notificationBusyDisabled;
            } else {
                delete element.dataset.notificationBusyDisabled;
            }
        });
    }

    function setFormState(text, kind) {
        const element = ensureRoot().querySelector('#notificationDraftFormState');
        element.textContent = normalizeText(text);
        element.dataset.kind = normalizeText(kind);
    }

    function renderFiles(selectedIds) {
        const root = ensureRoot();
        const container = root.querySelector('#notificationFilesList');
        const selected = new Set(Array.isArray(selectedIds) ? selectedIds : []);
        const files = state.context ? state.context.files : [];

        if (!files.length) {
            container.innerHTML = '<div class="notification-empty">В пункте нет текущих файлов.</div>';
            return;
        }

        container.innerHTML = files.map(function (file) {
            const checked = selected.has(file.documentId) ? 'checked' : '';
            return `
                <label class="notification-file-option">
                    <input type="checkbox" name="notificationDocument" value="${esc(file.documentId)}" ${checked}>
                    <span class="notification-file-option-name">${esc(file.fileName)}</span>
                </label>
            `;
        }).join('');
    }

    function populateForm(draft) {
        const root = ensureRoot();
        const context = state.context;
        const recipient = draft && draft.recipient || null;
        const recipientType = recipient && recipient.type === 'external'
            ? 'external'
            : 'internal';

        state.editingDraftId = draft ? normalizeText(draft.draftId) : '';
        root.querySelector('#notificationDraftId').value = state.editingDraftId;
        root.querySelector('#notificationDraftVersion').value = draft ? String(draft.version || 1) : '';
        root.querySelector('#notificationSenderName').value = normalizeText(
            draft && draft.sender && draft.sender.name
            || context.actor.name
        );
        root.querySelector('#notificationSenderId').value = normalizeText(
            draft && draft.sender && draft.sender.userId
            || context.actor.id
        );
        root.querySelector('#notificationRecipientType').value = recipientType;
        root.querySelector('#notificationRecipientUserId').value = normalizeText(
            recipient && recipient.bitrixUserId
        );
        root.querySelector('#notificationRecipientInternalName').value = normalizeText(
            recipientType === 'internal' && recipient && recipient.name
        );
        root.querySelector('#notificationCuratorUserId').value = normalizeText(
            recipientType === 'external' && recipient && recipient.curatorUserId
        );
        root.querySelector('#notificationCuratorName').value = normalizeText(
            recipientType === 'external' && recipient && recipient.curatorName
        );
        const pickers = ensureUserPickers();
        pickers.sender.setSelected({
            userId: root.querySelector('#notificationSenderId').value,
            name: root.querySelector('#notificationSenderName').value
        }, true);
        pickers.recipient.setSelected(
            recipientType === 'internal' ? {
                userId: root.querySelector('#notificationRecipientUserId').value,
                name: root.querySelector('#notificationRecipientInternalName').value
            } : null,
            true
        );
        const companyPicker = ensureCompanyPicker();
        companyPicker.setSelected(
            recipientType === 'external' ? {
                companyId: normalizeText(recipient && recipient.companyId),
                title: normalizeText(recipient && recipient.name),
                phone: normalizeText(recipient && recipient.phone),
                email: normalizeText(recipient && recipient.email),
                contactDetails: normalizeText(recipient && recipient.contactDetails),
                source: normalizeText(recipient && recipient.metadata && recipient.metadata.contractorSource),
                syncStatus: normalizeText(recipient && recipient.metadata && recipient.metadata.contractorSyncStatus),
                syncError: normalizeText(recipient && recipient.metadata && recipient.metadata.contractorSyncError)
            } : null,
            true
        );
        root.querySelector('#notificationAssignmentPart').value = normalizeText(
            draft && draft.assignmentPart && draft.assignmentPart.text
        );
        updateAssignmentPartHint();
        root.querySelector('#notificationDeadline').value = normalizeText(
            draft && draft.deadlineDate
        );
        root.querySelector('#notificationDescription').value = normalizeText(
            draft && draft.description
        );

        const selectedIds = draft
            ? (draft.files || []).map(function (file) { return normalizeText(file.documentId); })
            : context.files.map(function (file) { return file.documentId; });
        renderFiles(selectedIds);
        applyRecipientMode();
        setFormState(
            draft
                ? 'Редактируется существующий черновик.'
                : 'Создаётся новый черновик. Все текущие файлы выбраны по умолчанию.',
            ''
        );
        root.querySelector('#notificationSaveDraftBtn').textContent = draft
            ? 'Сохранить изменения'
            : 'Сохранить черновик';
    }

    function renderDraftCards() {
        const root = ensureRoot();
        const container = root.querySelector('#notificationDraftCards');
        root.querySelector('#notificationDraftCount').textContent = String(state.drafts.length);

        if (!state.drafts.length) {
            container.innerHTML = '<div class="notification-empty">Для этого пункта пока нет черновиков.</div>';
            updateBellCount(0);
            return;
        }

        container.innerHTML = state.drafts.map(function (draft) {
            const readyClass = draft.isReady ? 'is-ready' : 'is-incomplete';
            const readyText = draft.isReady ? 'Готов к отправке после commit' : 'Требует заполнения';
            const files = Array.isArray(draft.files) ? draft.files : [];
            return `
                <article class="notification-draft-card ${readyClass}" data-draft-id="${esc(draft.draftId)}">
                    <div class="notification-draft-card-status">${esc(readyText)}</div>
                    <div class="notification-draft-card-recipient">${esc(formatRecipient(draft))}</div>
                    <div class="notification-draft-card-meta">
                        <span>${esc(draft.assignmentPart && draft.assignmentPart.text || 'часть не указана')}</span>
                        <span>до ${esc(formatDate(draft.deadlineDate))}</span>
                        <span>${files.length} файл(а)</span>
                    </div>
                    <div class="notification-draft-card-actions">
                        <button type="button" class="notification-card-button" data-action="edit">Изменить</button>
                        <button type="button" class="notification-card-button notification-card-button--danger" data-action="delete">Удалить</button>
                    </div>
                </article>
            `;
        }).join('');
        updateBellCount(state.drafts.length);
    }

    function updateBellCount(count) {
        const context = state.context;
        if (!context) return;
        global.document.querySelectorAll(
            '[data-role="notify-documents"], #folderNotificationBtn'
        ).forEach(function (button) {
            const buttonItemId = String(button.dataset.itemId || context.itemId || '').trim();
            if (buttonItemId !== context.itemId) return;
            button.dataset.draftCount = String(count || 0);
            button.classList.toggle('has-notification-drafts', Number(count || 0) > 0);
        });
        if (typeof context.onChanged === 'function') {
            context.onChanged({ count: Number(count || 0), drafts: state.drafts.slice() });
        }
    }

    function buildListUrl() {
        const context = state.context;
        const query = new URLSearchParams({
            sessionId: context.sessionId,
            dialogId: context.dialogId,
            userId: context.actor.id,
            checklistKey: context.checklistKey,
            itemId: context.itemId
        });
        return context.apiUrl + '?' + query.toString();
    }

    async function loadDrafts() {
        const payload = await requestJson(buildListUrl(), { cache: 'no-store' });
        state.drafts = Array.isArray(payload.drafts) ? payload.drafts : [];
        renderDraftCards();
    }

    function collectRecipient(root) {
        const type = root.querySelector('#notificationRecipientType').value;
        if (type === 'external') {
            const name = normalizeText(root.querySelector('#notificationRecipientExternalName').value);
            const phone = normalizeText(root.querySelector('#notificationRecipientPhone').value);
            const email = normalizeText(root.querySelector('#notificationRecipientEmail').value);
            const contactDetails = normalizeText(root.querySelector('#notificationRecipientContact').value);
            if (!name && !phone && !email && !contactDetails) return null;
            const companyId = normalizeText(root.querySelector('#notificationRecipientCompanyId').value);
            const curatorUserId = normalizeText(root.querySelector('#notificationCuratorUserId').value);
            const curatorName = normalizeText(root.querySelector('#notificationCuratorName').value);
            return { type, companyId, name, phone, email, contactDetails, curatorUserId, curatorName };
        }
        const bitrixUserId = normalizeText(root.querySelector('#notificationRecipientUserId').value);
        const name = normalizeText(root.querySelector('#notificationRecipientInternalName').value);
        if (!bitrixUserId && !name) return null;
        return { type: 'internal', bitrixUserId, name };
    }

    function collectPayload() {
        const root = ensureRoot();
        const context = state.context;
        const documentIds = Array.from(
            root.querySelectorAll('input[name="notificationDocument"]:checked')
        ).map(function (element) { return element.value; });

        return {
            sessionId: context.sessionId,
            dialogId: context.dialogId,
            checklistKey: context.checklistKey,
            itemId: context.itemId,
            userId: context.actor.id,
            userName: context.actor.name,
            sender: {
                userId: normalizeText(root.querySelector('#notificationSenderId').value),
                name: normalizeText(root.querySelector('#notificationSenderName').value)
            },
            recipient: collectRecipient(root),
            assignmentPart: (function () {
                const text = normalizeText(root.querySelector('#notificationAssignmentPart').value);
                const existing = findAssignmentPartByText(text);
                return {
                    id: existing ? normalizeText(existing.assignmentPartId || existing.id) : '',
                    text
                };
            })(),
            deadlineDate: normalizeText(root.querySelector('#notificationDeadline').value),
            description: normalizeText(root.querySelector('#notificationDescription').value),
            documentIds,
            metadata: {
                source: context.source || 'popup',
                interfaceStage: '8.12'
            }
        };
    }

    async function saveCurrentDraft(event) {
        event.preventDefault();
        if (state.busy || !state.context) return;
        const root = ensureRoot();
        const payload = collectPayload();
        const draftId = normalizeText(root.querySelector('#notificationDraftId').value);
        const version = normalizeText(root.querySelector('#notificationDraftVersion').value);

        setBusy(true);
        setFormState('Сохранение черновика…', 'pending');
        try {
            let result;
            if (draftId) {
                payload.version = version ? Number(version) : undefined;
                result = await requestJson(
                    state.context.apiUrl + '/' + encodeURIComponent(draftId),
                    {
                        method: 'PATCH',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    }
                );
            } else {
                result = await requestJson(state.context.apiUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
            }
            await Promise.all([loadDrafts(), loadAssignmentParts()]);
            const saved = result.draft || null;
            populateForm(saved);
            setFormState(
                saved && saved.isReady
                    ? 'Черновик сохранён и готов к обработке после commit.'
                    : 'Черновик сохранён. Недостающие поля можно заполнить позже.',
                'success'
            );
        } catch (error) {
            if (error && error.status === 409) {
                await loadDrafts().catch(function () {});
            }
            setFormState(
                error && error.message ? error.message : 'Не удалось сохранить черновик',
                'error'
            );
        } finally {
            setBusy(false);
        }
    }

    async function deleteDraft(draft) {
        if (!draft || state.busy) return;
        if (!global.confirm('Удалить этот черновик оповещения?')) return;
        setBusy(true);
        setFormState('Удаление черновика…', 'pending');
        try {
            await requestJson(
                state.context.apiUrl + '/' + encodeURIComponent(draft.draftId),
                {
                    method: 'DELETE',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        sessionId: state.context.sessionId,
                        userId: state.context.actor.id,
                        userName: state.context.actor.name,
                        version: draft.version
                    })
                }
            );
            await loadDrafts();
            populateForm(null);
            setFormState('Черновик удалён.', 'success');
        } catch (error) {
            setFormState(
                error && error.message ? error.message : 'Не удалось удалить черновик',
                'error'
            );
        } finally {
            setBusy(false);
        }
    }

    function handleDraftCardClick(event) {
        const button = event.target.closest('[data-action]');
        const card = event.target.closest('[data-draft-id]');
        if (!button || !card) return;
        const draft = state.drafts.find(function (item) {
            return normalizeText(item.draftId) === normalizeText(card.dataset.draftId);
        });
        if (!draft) return;
        if (button.dataset.action === 'edit') {
            populateForm(draft);
            ensureRoot().querySelector('#notificationDraftForm').scrollIntoView({ behavior: 'smooth', block: 'start' });
        } else if (button.dataset.action === 'delete') {
            deleteDraft(draft);
        }
    }

    async function open(rawContext) {
        const context = rawContext || {};
        const normalized = {
            source: normalizeText(context.source) || 'popup',
            apiUrl: normalizeText(context.apiUrl) || defaultApiUrl(),
            assignmentPartsApiUrl: normalizeText(context.assignmentPartsApiUrl) || defaultAssignmentPartsApiUrl(),
            bitrixUsersApiUrl: normalizeText(context.bitrixUsersApiUrl) || defaultBitrixUsersApiUrl(),
            bitrixCompaniesApiUrl: normalizeText(context.bitrixCompaniesApiUrl) || defaultBitrixCompaniesApiUrl(),
            projectCuratorApiUrl: normalizeText(context.projectCuratorApiUrl) || defaultProjectCuratorApiUrl(),
            sessionId: normalizeText(context.sessionId),
            dialogId: normalizeText(context.dialogId),
            checklistKey: normalizeText(context.checklistKey),
            itemId: normalizeText(context.itemId),
            itemName: normalizeText(context.itemName) || 'Пункт',
            actor: {
                id: normalizeText(context.actor && (context.actor.id || context.actor.userId)),
                name: normalizeText(context.actor && (context.actor.name || context.actor.userName)) || 'Пользователь'
            },
            files: normalizeFiles(context.files),
            onChanged: typeof context.onChanged === 'function' ? context.onChanged : null
        };

        if (!normalized.sessionId) throw new Error('Нет активной edit-session');
        if (!normalized.actor.id) throw new Error('Не удалось определить текущего пользователя');
        if (!normalized.dialogId || !normalized.checklistKey || !normalized.itemId) {
            throw new Error('Не определён контекст пункта чек-листа');
        }
        if (!normalized.files.length) throw new Error('В пункте нет текущих файлов');

        state.context = normalized;
        state.drafts = [];
        state.assignmentParts = [];
        state.projectCurator = null;
        const root = ensureRoot();
        root.querySelector('#notificationDraftSubtitle').textContent = normalized.itemName;
        root.hidden = false;
        global.document.body.classList.add('notification-draft-open');
        populateForm(null);
        setBusy(true);
        setFormState('Загрузка черновиков…', 'pending');
        try {
            await Promise.all([loadDrafts(), loadAssignmentParts(), loadProjectCurator(false)]);
            setFormState('', '');
        } catch (error) {
            setFormState(
                error && error.message ? error.message : 'Не удалось загрузить черновики',
                'error'
            );
            throw error;
        } finally {
            setBusy(false);
        }
        root.querySelector('#notificationAssignmentPart').focus();
    }

    function close() {
        if (!state.root || state.root.hidden || state.busy) return;
        state.root.hidden = true;
        global.document.body.classList.remove('notification-draft-open');
    }

    global.ChecklistNotificationDraftUI = Object.freeze({
        open,
        close,
        refresh: loadDrafts
    });
})(window);
