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
        const titleInput = config.titleInput;
        const phoneInput = config.phoneInput;
        const emailInput = config.emailInput;
        const contactInput = config.contactInput;
        if (!input || !results || !hint || !idInput || !titleInput || !phoneInput || !emailInput || !contactInput) {
            throw new Error('Bitrix company picker elements are missing');
        }

        let selected = null;
        let selectedSnapshot = null;
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

        function normalizeCompany(company) {
            if (!company || typeof company !== 'object') return null;
            const normalized = {
                companyId: text(company.companyId || company.id),
                title: text(company.title || company.name),
                phone: text(company.phone),
                email: text(company.email),
                contactDetails: text(company.contactDetails),
                source: text(company.source),
                syncStatus: text(company.syncStatus),
                syncError: text(company.syncError)
            };
            return normalized.companyId || normalized.title ? normalized : null;
        }

        function selectedHint(company) {
            if (!company) return '';
            if (company.source === 'bitrix') {
                return 'Выбрана CRM-компания Bitrix24 · ID ' + (company.companyId || 'не указан');
            }
            if (company.syncStatus === 'pending_create' || company.syncStatus === 'pending_update') {
                return 'Подрядчик сохранён локально и ожидает синхронизации с Bitrix24.';
            }
            if (company.syncStatus === 'error') {
                return 'Подрядчик сохранён локально. Последняя синхронизация завершилась ошибкой.';
            }
            return 'Выбран локально сохранённый внешний исполнитель.';
        }

        function setSelected(company, silent) {
            selected = normalizeCompany(company);
            selectedSnapshot = selected ? Object.assign({}, selected) : null;
            input.value = selected ? selected.title : '';
            idInput.value = selected ? selected.companyId : '';
            titleInput.value = selected ? selected.title : '';
            phoneInput.value = selected ? selected.phone : '';
            emailInput.value = selected ? selected.email : '';
            contactInput.value = selected ? selected.contactDetails : '';
            results.hidden = true;
            if (selected) {
                setHint(selectedHint(selected), selected.syncStatus === 'error' ? 'warning' : 'selected');
            } else {
                setHint('Найдите сохранённого подрядчика или введите нового.', '');
            }
            if (!silent && typeof config.onSelect === 'function') {
                config.onSelect(selected);
            }
        }

        function currentValuesMatchSnapshot() {
            if (!selectedSnapshot) return false;
            return (
                text(idInput.value) === selectedSnapshot.companyId
                && text(titleInput.value) === selectedSnapshot.title
                && text(phoneInput.value) === selectedSnapshot.phone
                && text(emailInput.value) === selectedSnapshot.email
                && text(contactInput.value) === selectedSnapshot.contactDetails
            );
        }

        function clearSelectionOnManualChange() {
            if (selectedSnapshot && currentValuesMatchSnapshot()) return;
            selected = null;
            idInput.value = '';
            if (text(titleInput.value)) input.value = text(titleInput.value);
            setHint('Новый или изменённый подрядчик будет сохранён локально вместе с черновиком.', 'pending');
        }

        function getSelected() {
            if (selected && currentValuesMatchSnapshot()) {
                return Object.assign({}, selected);
            }
            const companyId = text(idInput.value);
            const title = text(titleInput.value);
            const phone = text(phoneInput.value);
            const email = text(emailInput.value);
            const contactDetails = text(contactInput.value);
            if (!companyId && !title && !phone && !email && !contactDetails) return null;
            return { companyId, title, phone, email, contactDetails };
        }

        function renderCompanies(companies, sync) {
            const safeCompanies = Array.isArray(companies) ? companies : [];
            if (!safeCompanies.length) {
                results.innerHTML = '<div class="bitrix-company-picker-empty">Подрядчики не найдены.</div>';
                results.hidden = false;
                if (sync && sync.status === 'disabled') {
                    setHint('Webhook не настроен. Можно сохранить нового подрядчика локально.', 'warning');
                } else if (sync && sync.status === 'error') {
                    setHint('Обновить CRM-кеш не удалось. Доступен сохранённый кеш и локальное добавление.', 'warning');
                } else {
                    setHint('Нет совпадений. Заполните данные нового внешнего исполнителя.', '');
                }
                return;
            }

            results.innerHTML = safeCompanies.map(function (company) {
                const meta = [
                    text(company.phone),
                    text(company.email),
                    company.source === 'bitrix' && company.companyId ? 'CRM ID ' + company.companyId : 'локально',
                    company.syncStatus === 'error' ? 'ошибка синхронизации' : ''
                ].filter(Boolean).join(' · ');
                return `
                    <button type="button" class="bitrix-company-picker-option" data-company-id="${esc(company.companyId)}">
                        <span class="bitrix-company-picker-option-name">${esc(company.title || ('Компания ' + company.companyId))}</span>
                        <span class="bitrix-company-picker-option-meta">${esc(meta)}</span>
                    </button>
                `;
            }).join('');
            results.hidden = false;
            setHint('Выберите сохранённого подрядчика или продолжите ввод нового.', '');
            results.querySelectorAll('[data-company-id]').forEach(function (button) {
                button.addEventListener('mousedown', function (event) {
                    event.preventDefault();
                    const company = safeCompanies.find(function (entry) {
                        return text(entry.companyId) === text(button.dataset.companyId);
                    });
                    if (company) setSelected(company);
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
            setHint('Поиск подрядчиков…', 'pending');
            try {
                const payload = await requestJson(apiUrl() + '?' + params.toString(), {
                    cache: 'no-store'
                });
                if (serial !== requestSerial || destroyed) return;
                renderCompanies(payload.companies, payload.sync);
            } catch (error) {
                if (serial !== requestSerial || destroyed) return;
                results.hidden = true;
                setHint(
                    error && error.message ? error.message : 'Не удалось загрузить подрядчиков',
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
            idInput.value = '';
            if (!text(titleInput.value) || text(titleInput.value) === text(selectedSnapshot && selectedSnapshot.title)) {
                titleInput.value = text(input.value);
            }
            setHint('Поиск подрядчиков…', 'pending');
            scheduleSearch();
        });
        input.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') results.hidden = true;
        });
        input.addEventListener('blur', function () {
            global.setTimeout(function () { results.hidden = true; }, 180);
        });
        [titleInput, phoneInput, emailInput, contactInput].forEach(function (element) {
            element.addEventListener('input', clearSelectionOnManualChange);
        });

        if (refreshButton) {
            refreshButton.addEventListener('click', async function () {
                if (!apiUrl()) return;
                refreshButton.disabled = true;
                setHint('Обновление кеша CRM-компаний…', 'pending');
                try {
                    const payload = await requestJson(apiUrl() + '/refresh', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ q: text(input.value), limit: 30 })
                    });
                    renderCompanies(payload.companies, payload.sync);
                } catch (error) {
                    setHint(error && error.message ? error.message : 'Ошибка обновления CRM-кеша', 'error');
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

    global.ChecklistBitrixCompanyPicker = Object.freeze({ create: create });
})(window);
