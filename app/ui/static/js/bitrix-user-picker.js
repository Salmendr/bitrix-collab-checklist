(function (global) {
    'use strict';

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

    async function requestJson(url, options) {
        const response = await global.fetch(url, options || {});
        const payload = await response.json().catch(function () { return {}; });
        if (!response.ok || payload.ok === false) {
            const error = new Error(payload.error || payload.detail || ('HTTP ' + response.status));
            error.status = response.status;
            error.payload = payload;
            throw error;
        }
        return payload;
    }

    function create(options) {
        const config = options || {};
        const input = config.input;
        const results = config.results;
        const hint = config.hint;
        const refreshButton = config.refreshButton || null;
        const idInput = config.idInput;
        const nameInput = config.nameInput;
        if (!input || !results || !hint || !idInput || !nameInput) {
            throw new Error('Bitrix user picker elements are missing');
        }

        let selected = null;
        let timer = null;
        let requestSerial = 0;
        let firstSearch = true;
        let destroyed = false;

        function apiUrl() {
            return typeof config.apiUrl === 'function'
                ? text(config.apiUrl())
                : text(config.apiUrl);
        }

        function setHint(message, kind) {
            hint.textContent = text(message);
            hint.dataset.kind = text(kind);
        }

        function setSelected(user, silent) {
            const normalized = user && typeof user === 'object' ? {
                userId: text(user.userId || user.id),
                name: text(user.name || user.displayName),
                email: text(user.email),
                workPosition: text(user.workPosition)
            } : null;
            selected = normalized && (normalized.userId || normalized.name) ? normalized : null;
            input.value = selected ? selected.name : '';
            idInput.value = selected ? selected.userId : '';
            nameInput.value = selected ? selected.name : '';
            results.hidden = true;
            if (selected) {
                setHint(
                    'Выбран сотрудник Bitrix24 · ID ' + (selected.userId || 'не указан'),
                    'selected'
                );
            } else {
                setHint('Начните вводить ФИО сотрудника.', '');
            }
            if (!silent && typeof config.onSelect === 'function') {
                config.onSelect(selected);
            }
        }

        function getSelected() {
            const manualId = text(idInput.value);
            const manualName = text(nameInput.value);
            if (
                selected
                && selected.userId === manualId
                && selected.name === manualName
            ) {
                return Object.assign({}, selected);
            }
            if (!manualId && !manualName) return null;
            return { userId: manualId, name: manualName, email: '', workPosition: '' };
        }

        function renderUsers(users, sync) {
            const safeUsers = Array.isArray(users) ? users : [];
            if (!safeUsers.length) {
                results.innerHTML = '<div class="bitrix-user-picker-empty">Сотрудники не найдены.</div>';
                results.hidden = false;
                if (sync && sync.status === 'disabled') {
                    setHint('Webhook не настроен. Можно указать ФИО и Bitrix ID вручную.', 'warning');
                } else if (sync && sync.status === 'error') {
                    setHint('Обновить кеш не удалось. Доступен сохранённый кеш или ручной ввод.', 'warning');
                } else {
                    setHint('Нет совпадений. Можно уточнить запрос или заполнить ID и ФИО вручную.', '');
                }
                return;
            }

            results.innerHTML = safeUsers.map(function (user) {
                const meta = [text(user.workPosition), text(user.email), user.userId ? 'ID ' + user.userId : '']
                    .filter(Boolean)
                    .join(' · ');
                return `
                    <button type="button" class="bitrix-user-picker-option" data-user-id="${esc(user.userId)}">
                        <span class="bitrix-user-picker-option-name">${esc(user.name || ('ID ' + user.userId))}</span>
                        <span class="bitrix-user-picker-option-meta">${esc(meta)}</span>
                    </button>
                `;
            }).join('');
            results.hidden = false;
            setHint('Выберите сотрудника из кеша Bitrix24.', '');
            results.querySelectorAll('[data-user-id]').forEach(function (button) {
                button.addEventListener('mousedown', function (event) {
                    event.preventDefault();
                    const user = safeUsers.find(function (entry) {
                        return text(entry.userId) === text(button.dataset.userId);
                    });
                    if (user) setSelected(user);
                });
            });
        }

        async function search(query, forceRefresh) {
            if (destroyed || !apiUrl()) return;
            const serial = ++requestSerial;
            const params = new URLSearchParams({
                q: text(query),
                limit: '30'
            });
            if (forceRefresh) {
                params.set('refresh', 'force');
            } else if (firstSearch) {
                params.set('refresh', 'auto');
                firstSearch = false;
            }
            setHint('Поиск сотрудников…', 'pending');
            try {
                const payload = await requestJson(apiUrl() + '?' + params.toString(), {
                    cache: 'no-store'
                });
                if (serial !== requestSerial || destroyed) return;
                renderUsers(payload.users, payload.sync);
            } catch (error) {
                if (serial !== requestSerial || destroyed) return;
                results.hidden = true;
                setHint(
                    error && error.message
                        ? error.message
                        : 'Не удалось загрузить сотрудников',
                    'error'
                );
            }
        }

        function scheduleSearch() {
            if (timer !== null) global.clearTimeout(timer);
            timer = global.setTimeout(function () {
                search(input.value, false);
            }, 240);
        }

        input.addEventListener('focus', function () {
            search(input.value, false);
        });
        input.addEventListener('input', function () {
            selected = null;
            scheduleSearch();
        });
        input.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') results.hidden = true;
        });
        input.addEventListener('blur', function () {
            global.setTimeout(function () { results.hidden = true; }, 180);
        });
        idInput.addEventListener('input', function () { selected = null; });
        nameInput.addEventListener('input', function () { selected = null; });

        if (refreshButton) {
            refreshButton.addEventListener('click', async function () {
                if (!apiUrl()) return;
                refreshButton.disabled = true;
                setHint('Обновление кеша сотрудников…', 'pending');
                try {
                    const payload = await requestJson(apiUrl() + '/refresh', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ q: text(input.value), limit: 30 })
                    });
                    renderUsers(payload.users, payload.sync);
                } catch (error) {
                    setHint(error && error.message ? error.message : 'Ошибка обновления кеша', 'error');
                } finally {
                    refreshButton.disabled = false;
                }
            });
        }

        return Object.freeze({
            setSelected,
            getSelected,
            search: function (query) { return search(query, false); },
            refresh: function () { return search(input.value, true); },
            clear: function () { setSelected(null); },
            destroy: function () {
                destroyed = true;
                if (timer !== null) global.clearTimeout(timer);
            }
        });
    }

    global.ChecklistBitrixUserPicker = Object.freeze({ create: create });
})(window);
