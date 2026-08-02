(function (global) {
    'use strict';

    const cache = new Map();

    function esc(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function text(value) {
        return String(value == null ? '' : value).trim();
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

    function apiUrl() {
        return global.location.origin
            + detectBasePath()
            + '/api/checklist/document-assignment-history';
    }

    function retryApiUrl() {
        return global.location.origin
            + detectBasePath()
            + '/api/checklist/notification-deliveries/retry';
    }

    function formatDateTime(value) {
        const raw = text(value);
        if (!raw) return '—';
        const date = new Date(raw);
        if (Number.isNaN(date.getTime())) return raw;
        return new Intl.DateTimeFormat('ru-RU', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        }).format(date);
    }

    function formatDate(value) {
        const raw = text(value);
        if (!raw) return '—';
        const parts = raw.split('-');
        if (parts.length !== 3) return raw;
        return parts[2] + '.' + parts[1] + '.' + parts[0];
    }

    function statusLabel(value) {
        const normalized = text(value).toLowerCase();
        const labels = {
            sent: 'Отправлено',
            succeeded: 'Выполнено',
            synced: 'Синхронизировано',
            skipped: 'Не требуется',
            pending: 'Ожидает',
            running: 'Выполняется',
            retrying: 'Повторяется',
            failed: 'Ошибка',
            blocked: 'Заблокировано',
            disabled: 'Отключено'
        };
        return labels[normalized] || normalized || '—';
    }

    function statusClass(value) {
        const normalized = text(value).toLowerCase();
        if (['sent', 'succeeded', 'synced', 'skipped'].includes(normalized)) {
            return 'assignment-history-status--success';
        }
        if (['failed', 'blocked'].includes(normalized)) {
            return 'assignment-history-status--error';
        }
        return 'assignment-history-status--pending';
    }

    function renderEntry(entry) {
        const sender = entry && entry.sender || {};
        const recipient = entry && entry.recipient || {};
        const assignmentPart = entry && entry.assignmentPart || {};
        const task = entry && entry.task || {};
        const delivery = entry && entry.delivery || {};
        const recipientSuffix = recipient.isExternal ? ' · внешний' : '';
        const taskLink = text(task.url)
            ? `<a href="${esc(task.url)}" target="_blank" rel="noopener noreferrer">Задача №${esc(task.id || '—')}</a>`
            : text(task.id)
                ? `Задача №${esc(task.id)}`
                : 'Задача не создана';

        const retryableTypes = Array.isArray(delivery.retryableTypes)
            ? delivery.retryableTypes
            : [];
        const retryAction = delivery.canRetry
            ? `
                <div class="assignment-history-retry-row">
                    <button
                        type="button"
                        class="assignment-history-retry-button"
                        data-role="assignment-history-retry"
                        data-draft-id="${esc(entry && entry.draftId)}"
                        data-delivery-types="${esc(retryableTypes.join(','))}">
                        Повторить неуспешные
                    </button>
                    <span class="assignment-history-retry-result" data-role="assignment-history-retry-result" aria-live="polite"></span>
                </div>
            `
            : '';

        return `
            <article class="assignment-history-entry" data-draft-id="${esc(entry && entry.draftId)}">
                <header class="assignment-history-entry-header">
                    <time>${esc(formatDateTime(entry && entry.createdAt))}</time>
                    <span class="assignment-history-version">Версия: ${esc(entry && entry.versionName || 'Документ')}</span>
                </header>
                <div class="assignment-history-grid">
                    <div><strong>От:</strong> ${esc(sender.name || '—')}</div>
                    <div><strong>Кому:</strong> ${esc(recipient.name || '—')}${esc(recipientSuffix)}</div>
                    <div><strong>Часть:</strong> ${esc(assignmentPart.text || '—')}</div>
                    <div><strong>Срок:</strong> ${esc(formatDate(entry && entry.deadlineDate))}</div>
                    <div class="assignment-history-task"><strong>Задача:</strong> ${taskLink}</div>
                </div>
                <div class="assignment-history-statuses" aria-label="Статусы доставки">
                    <span class="assignment-history-status ${statusClass(delivery.chatStatus)}">Чат: ${esc(statusLabel(delivery.chatStatus))}</span>
                    <span class="assignment-history-status ${statusClass(delivery.taskStatus)}">Задача: ${esc(statusLabel(delivery.taskStatus))}</span>
                    ${recipient.isExternal ? `<span class="assignment-history-status ${statusClass(delivery.crmSyncStatus)}">CRM: ${esc(statusLabel(delivery.crmSyncStatus))}</span>` : ''}
                </div>
                ${retryAction}
            </article>
        `;
    }

    function renderPayload(panel, payload) {
        const history = Array.isArray(payload && payload.history)
            ? payload.history
            : [];
        if (!history.length) {
            panel.innerHTML = '<div class="assignment-history-empty">История заданий отсутствует</div>';
            return;
        }
        panel.innerHTML = history.map(renderEntry).join('');
    }

    function setExpanded(button, panel, expanded) {
        button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        panel.hidden = !expanded;
        const icon = button.querySelector('[data-role="assignment-history-toggle-icon"]');
        if (icon) icon.textContent = expanded ? '▾' : '▸';
    }

    function buildRequestUrl(button) {
        const params = new URLSearchParams({
            dialogId: text(button.dataset.dialogId),
            checklistKey: text(button.dataset.checklistKey),
            itemId: text(button.dataset.itemId),
            seriesId: text(button.dataset.seriesId),
            limit: '10'
        });
        return apiUrl() + '?' + params.toString();
    }

    async function loadHistory(button, panel) {
        const url = buildRequestUrl(button);
        if (!cache.has(url)) {
            cache.set(url, global.fetch(url).then(async function (response) {
                const payload = await response.json().catch(function () { return {}; });
                if (!response.ok || payload.ok === false) {
                    throw new Error(payload.error || payload.detail || ('HTTP ' + response.status));
                }
                return payload;
            }).catch(function (error) {
                cache.delete(url);
                throw error;
            }));
        }
        return cache.get(url);
    }

    async function handleToggle(button) {
        const panelId = text(button.dataset.panelId);
        const panel = panelId ? global.document.getElementById(panelId) : null;
        if (!panel) return;

        const expanded = button.getAttribute('aria-expanded') === 'true';
        if (expanded) {
            setExpanded(button, panel, false);
            return;
        }

        setExpanded(button, panel, true);
        if (panel.dataset.loaded === '1') return;

        panel.innerHTML = '<div class="assignment-history-loading">Загрузка истории…</div>';
        button.disabled = true;
        try {
            const payload = await loadHistory(button, panel);
            renderPayload(panel, payload);
            panel.dataset.loaded = '1';
        } catch (error) {
            panel.innerHTML = '<div class="assignment-history-error">Не удалось загрузить историю: ' + esc(error && error.message || error) + '</div>';
        } finally {
            button.disabled = false;
        }
    }

    function parseDeliveryTypes(button) {
        return text(button && button.dataset && button.dataset.deliveryTypes)
            .split(',')
            .map(function (value) { return value.trim(); })
            .filter(Boolean);
    }

    async function postRetry(button, confirmUncertain) {
        const response = await global.fetch(retryApiUrl(), {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                draftId: text(button.dataset.draftId),
                deliveryTypes: parseDeliveryTypes(button),
                confirmed: true,
                confirmUncertain: confirmUncertain === true
            })
        });
        const payload = await response.json().catch(function () { return {}; });
        if (response.status === 409 && payload.requiresUncertainConfirmation) {
            return payload;
        }
        if (!response.ok || payload.ok === false) {
            throw new Error(payload.error || payload.warning || payload.detail || ('HTTP ' + response.status));
        }
        return payload;
    }

    async function reloadPanelForRetry(button) {
        const panel = button.closest('.assignment-history-panel');
        if (!panel || !panel.id) return;
        const toggle = global.document.querySelector(
            '[data-role="document-assignment-history-toggle"]'
            + '[data-panel-id="' + String(panel.id).replace(/"/g, '\"') + '"]'
        );
        if (!toggle) return;
        cache.clear();
        panel.dataset.loaded = '0';
        panel.innerHTML = '<div class="assignment-history-loading">Обновление истории…</div>';
        const payload = await loadHistory(toggle, panel);
        renderPayload(panel, payload);
        panel.dataset.loaded = '1';
    }

    async function handleRetry(button) {
        if (!button || button.disabled) return;
        const resultNode = button.parentElement
            ? button.parentElement.querySelector('[data-role="assignment-history-retry-result"]')
            : null;
        const confirmed = global.confirm(
            'Повторить только неуспешные операции? Уже успешные задачи, сообщения и CRM-действия повторяться не будут.'
        );
        if (!confirmed) return;

        button.disabled = true;
        button.setAttribute('aria-busy', 'true');
        if (resultNode) resultNode.textContent = 'Повтор…';
        try {
            let payload = await postRetry(button, false);
            if (payload.requiresUncertainConfirmation) {
                const warning = payload.warning
                    || 'Bitrix мог выполнить операцию, но ответ не дошёл. Повтор может создать дубль.';
                if (!global.confirm(warning + '\n\nВыполнить повтор несмотря на риск?')) {
                    if (resultNode) resultNode.textContent = 'Повтор отменён';
                    return;
                }
                payload = await postRetry(button, true);
            }
            if (resultNode) {
                resultNode.textContent = payload.status === 'nothing_to_retry'
                    ? 'Неуспешных операций больше нет'
                    : payload.allSucceeded
                        ? 'Повтор завершён успешно'
                        : 'Повтор выполнен, часть операций ещё неуспешна';
            }
            await reloadPanelForRetry(button);
        } catch (error) {
            if (resultNode) {
                resultNode.textContent = 'Ошибка: ' + text(error && error.message || error);
            }
        } finally {
            button.disabled = false;
            button.removeAttribute('aria-busy');
        }
    }

    global.document.addEventListener('click', function (event) {
        const retryButton = event.target && event.target.closest
            ? event.target.closest('[data-role="assignment-history-retry"]')
            : null;
        if (retryButton) {
            event.preventDefault();
            handleRetry(retryButton);
            return;
        }

        const button = event.target && event.target.closest
            ? event.target.closest('[data-role="document-assignment-history-toggle"]')
            : null;
        if (!button) return;
        event.preventDefault();
        handleToggle(button);
    });

    global.ChecklistDocumentAssignmentHistory = {
        clearCache: function () { cache.clear(); },
        loadHistory,
        renderPayload,
        retryFailed: handleRetry
    };
})(window);
